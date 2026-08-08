from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket

from grpc import StatusCode
from tinkoff.invest.exceptions import RequestError

from tinvest_signal_engine.adapters import ingestor_health_file
from tinvest_signal_engine.adapters.ingestor_health_file import (
    AtomicJsonIngestorHealthStore,
)
from tinvest_signal_engine.application.ingestor_health import (
    IngestorHealthTracker,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.ingestor_health import (
    INGESTOR_CONNECTING,
    INGESTOR_DNS_RESOLUTION_FAILED,
    INGESTOR_HEALTH_SCHEMA_VERSION,
    INGESTOR_RECONNECTING,
    INGESTOR_SCHEDULED_SLEEP,
    INGESTOR_STREAMING,
    INGESTOR_STREAM_STALE,
    IngestorHealthSnapshot,
    IngestorStreamState,
)
from tinvest_signal_engine.services.ingestor import _health_reason_code


UTC = timezone.utc
STARTED_AT = datetime(2026, 7, 24, 7, 0, tzinfo=UTC)


@dataclass
class _Clock:
    current: datetime

    def __call__(self) -> datetime:
        return self.current


class _MemoryStore:
    def __init__(self) -> None:
        self.items: list[IngestorHealthSnapshot] = []

    def save(self, snapshot: IngestorHealthSnapshot) -> None:
        self.items.append(snapshot)


def test_tracker_records_connect_stream_fail_reconnect_transitions() -> None:
    clock = _Clock(STARTED_AT)
    store = _MemoryStore()
    tracker = IngestorHealthTracker(
        store=store,
        clock=clock,
        stale_after_seconds=180,
    )

    connecting = tracker.connecting(
        configured_instruments=25,
        reason_code=INGESTOR_CONNECTING,
    )
    clock.current += timedelta(seconds=3)
    source_at = clock.current - timedelta(milliseconds=100)
    streaming = tracker.publish_succeeded(market_event_at=source_at)
    clock.current += timedelta(seconds=2)
    degraded = tracker.failed(
        reason_code=INGESTOR_DNS_RESOLUTION_FAILED,
    )
    reconnecting = tracker.connecting(
        configured_instruments=25,
        reason_code=INGESTOR_RECONNECTING,
    )
    clock.current += timedelta(seconds=5)
    recovered = tracker.publish_succeeded(market_event_at=clock.current)

    assert connecting.state is IngestorStreamState.CONNECTING
    assert connecting.configured_instruments == 25
    assert streaming.state is IngestorStreamState.STREAMING
    assert streaming.reason_code == INGESTOR_STREAMING
    assert streaming.last_market_event_at == source_at
    assert streaming.last_success_at == STARTED_AT + timedelta(seconds=3)
    assert degraded.state is IngestorStreamState.DEGRADED
    assert degraded.last_error_at == STARTED_AT + timedelta(seconds=5)
    assert degraded.consecutive_failures == 1
    assert reconnecting.state is IngestorStreamState.CONNECTING
    assert reconnecting.consecutive_failures == 1
    assert recovered.state is IngestorStreamState.STREAMING
    assert recovered.consecutive_failures == 0
    assert len(store.items) == 6


def test_streaming_snapshot_becomes_degraded_after_silence() -> None:
    clock = _Clock(STARTED_AT)
    store = _MemoryStore()
    tracker = IngestorHealthTracker(
        store=store,
        clock=clock,
        stale_after_seconds=180,
    )
    tracker.connecting(configured_instruments=25)
    tracker.publish_succeeded(market_event_at=STARTED_AT)

    clock.current += timedelta(seconds=180)
    assert (
        tracker.evaluate_staleness().state
        is IngestorStreamState.STREAMING
    )

    clock.current += timedelta(microseconds=1)
    stale = tracker.evaluate_staleness()

    assert stale.state is IngestorStreamState.DEGRADED
    assert stale.reason_code == INGESTOR_STREAM_STALE
    assert stale.consecutive_failures == 0
    assert store.items[-1] == stale


def test_scheduled_sleep_is_a_non_failure_connecting_state() -> None:
    clock = _Clock(STARTED_AT)
    store = _MemoryStore()
    tracker = IngestorHealthTracker(
        store=store,
        clock=clock,
        stale_after_seconds=180,
    )

    sleeping = tracker.sleeping(configured_instruments=25)

    assert sleeping.state is IngestorStreamState.CONNECTING
    assert sleeping.reason_code == INGESTOR_SCHEDULED_SLEEP
    assert sleeping.consecutive_failures == 0


def test_atomic_snapshot_contains_only_allow_list_and_no_secrets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "runtime" / "ingestor-health.json"
    store = AtomicJsonIngestorHealthStore(path)
    snapshot = IngestorHealthSnapshot.starting(
        started_at=STARTED_AT,
        stale_after_seconds=180,
    ).connecting(configured_instruments=25)

    store.save(snapshot)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": INGESTOR_HEALTH_SCHEMA_VERSION,
        "state": "connecting",
        "started_at": "2026-07-24T07:00:00Z",
        "last_market_event_at": None,
        "last_success_at": None,
        "last_error_at": None,
        "reason_code": INGESTOR_CONNECTING,
        "consecutive_failures": 0,
        "configured_instruments": 25,
        "stale_after_seconds": 180,
    }
    encoded = path.read_text(encoding="utf-8").lower()
    for forbidden in (
        "token",
        "password",
        "account",
        "invest-public-api",
        "instrument_uid",
        "figi",
    ):
        assert forbidden not in encoded


def test_file_store_replaces_complete_json_atomically(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "ingestor-health.json"
    store = AtomicJsonIngestorHealthStore(path)
    first = IngestorHealthSnapshot.starting(
        started_at=STARTED_AT,
        stale_after_seconds=180,
    )
    store.save(first)
    old_payload = path.read_text(encoding="utf-8")
    second = first.connecting(configured_instruments=25)
    original_replace = ingestor_health_file.os.replace
    observations: list[tuple[str, dict[str, object]]] = []

    def _observed_replace(source: Path, destination: Path) -> None:
        assert Path(destination) == path
        assert path.read_text(encoding="utf-8") == old_payload
        complete_new_payload = json.loads(
            Path(source).read_text(encoding="utf-8")
        )
        observations.append((complete_new_payload["state"], complete_new_payload))
        original_replace(source, destination)

    monkeypatch.setattr(
        ingestor_health_file.os,
        "replace",
        _observed_replace,
    )

    store.save(second)

    assert observations[0][0] == "connecting"
    assert json.loads(path.read_text(encoding="utf-8"))[
        "configured_instruments"
    ] == 25
    assert list(tmp_path.glob(".*.tmp")) == []


def test_dns_failure_reason_is_stable_and_does_not_include_exception_text() -> None:
    try:
        raise socket.gaierror(
            socket.EAI_NONAME,
            "invest-public-api.example.invalid secret-token",
        )
    except socket.gaierror as cause:
        failure = RuntimeError("vendor wrapper")
        failure.__cause__ = cause

    assert _health_reason_code(failure) == INGESTOR_DNS_RESOLUTION_FAILED

    grpc_failure = RequestError(
        StatusCode.UNAVAILABLE,
        "DNS resolution failed for invest-public-api.example.invalid",
        None,
    )
    assert _health_reason_code(grpc_failure) == INGESTOR_DNS_RESOLUTION_FAILED


def test_ingestor_health_path_and_stale_window_are_configurable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    configured_path = tmp_path / "shared" / "health.json"
    monkeypatch.setenv(
        "INGESTOR_HEALTH_SNAPSHOT_PATH",
        str(configured_path),
    )
    monkeypatch.setenv("INGESTOR_HEALTH_STALE_AFTER_SECONDS", "45")

    settings = RuntimeSettings.from_env(service_name="ingestor")

    assert settings.ingestor_health_snapshot_path == configured_path
    assert settings.ingestor_health_stale_after_seconds == 45
