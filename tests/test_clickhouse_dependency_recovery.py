from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from threading import Event
from urllib.error import HTTPError

import pytest

from tinvest_signal_engine.adapters.clickhouse_reference_ticks import (
    ClickHouseReferenceTickReader,
)
from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.kafka_scientific_candles import (
    ScientificCandleKafkaRuntime,
)
from tinvest_signal_engine.application.scientific_candles import (
    ScientificCandleJournalProcessor,
)
from tinvest_signal_engine.application.signal_outcomes import (
    DirectionalSignalOutcomeBatchResult,
)
from tinvest_signal_engine.services.signal_outcome_worker import run_worker_loop


NOW = datetime(2026, 7, 23, 7, 0, tzinfo=timezone.utc)


@dataclass
class _Metrics:
    rows: list[dict[str, object]] = field(default_factory=list)

    def dependency_attempted(self, **values) -> None:
        self.rows.append(values)


class _RecoveringOutcomeWorker:
    def __init__(self, stop: Event) -> None:
        self.calls = 0
        self.stop = stop

    def process_due(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            raise TransientClickHouseError(
                operation="reference_tick_select",
                reason_code="http_500",
            )
        self.stop.set()
        return DirectionalSignalOutcomeBatchResult(
            scanned=1,
            stored=1,
            pending=0,
            outcome_ids=("outcome-1",),
            reason_counts=(("available", 1),),
        )


def test_bounded_exponential_backoff_caps_delay_and_applies_jitter() -> None:
    policy = BoundedExponentialBackoff(
        base_seconds=0.5,
        maximum_seconds=2.0,
        jitter_ratio=0.2,
    )

    assert [
        policy.delay(attempt, random_value=lambda: 0.5)
        for attempt in range(1, 6)
    ] == [0.5, 1.0, 2.0, 2.0, 2.0]
    assert policy.delay(3, random_value=lambda: 0.0) == 1.6
    assert policy.delay(3, random_value=lambda: 1.0) == 2.0


def test_outcome_worker_recovers_after_clickhouse_500_without_duplicate_pass(
    caplog,
) -> None:
    stop = Event()
    worker = _RecoveringOutcomeWorker(stop)
    metrics = _Metrics()

    run_worker_loop(
        worker,  # type: ignore[arg-type]
        batch_size=100,
        poll_seconds=5,
        stop_event=stop,
        metrics=metrics,
        backoff=BoundedExponentialBackoff(
            base_seconds=0.01,
            maximum_seconds=0.04,
        ),
        now=lambda: NOW,
        random_value=lambda: 0.5,
    )

    assert worker.calls == 2
    assert [row["outcome"] for row in metrics.rows] == ["retry", "recovered"]
    assert "secret" not in caplog.text.lower()


def test_outcome_worker_shutdown_interrupts_clickhouse_backoff() -> None:
    class _StopOnWait(Event):
        def wait(self, timeout=None):
            self.set()
            return True

    class _UnavailableWorker:
        calls = 0

        def process_due(self, **_kwargs):
            self.calls += 1
            raise TransientClickHouseError(
                operation="reference_tick_select",
                reason_code="connection_unavailable",
            )

    stop = _StopOnWait()
    worker = _UnavailableWorker()

    run_worker_loop(
        worker,  # type: ignore[arg-type]
        batch_size=100,
        poll_seconds=5,
        stop_event=stop,
        backoff=BoundedExponentialBackoff(
            base_seconds=0.01,
            maximum_seconds=0.04,
        ),
        now=lambda: NOW,
        random_value=lambda: 0.5,
    )

    assert worker.calls == 1
    assert stop.is_set()


def test_outcome_reader_classifies_clickhouse_500(monkeypatch) -> None:
    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.clickhouse_reference_ticks.urlopen",
        lambda request, timeout: (_ for _ in ()).throw(
            HTTPError(
                request.full_url,
                500,
                "server error",
                {},
                BytesIO(b"secret-token-must-not-be-copied"),
            )
        ),
    )
    reader = ClickHouseReferenceTickReader(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
    )

    with pytest.raises(TransientClickHouseError) as raised:
        reader.load(
            instrument_id="SBER_TQBR",
            start_at=NOW,
            end_at=NOW,
        )

    assert raised.value.reason_code == "http_500"
    assert "secret" not in str(raised.value)


@dataclass
class _Message:
    value: object
    topic: str = "marketdata.raw"
    partition: int = 0
    offset: int = 4


@dataclass
class _Consumer:
    message: _Message
    polled: bool = False
    commits: int = 0
    closed: bool = False

    def poll(self, **_kwargs):
        if self.polled:
            raise KeyboardInterrupt
        self.polled = True
        return {"partition": [self.message]}

    def commit(self, **_kwargs):
        self.commits += 1

    def close(self):
        self.closed = True


@dataclass
class _RecoveringCandleStore:
    calls: int = 0
    rows: dict[str, object] = field(default_factory=dict)

    def persist_many(self, candles) -> None:
        self.calls += 1
        if self.calls == 1:
            raise TransientClickHouseError(
                operation="scientific_candle_insert",
                reason_code="connection_unavailable",
            )
        for candle in candles:
            self.rows[candle.source_event_id] = candle


def _raw_candle() -> dict[str, object]:
    return {
        "event_id": "candle-event-1",
        "event_type": "candle",
        "instrument_id": "uid-sber",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "lot": 10,
        "source_time": NOW.isoformat(),
        "received_at": NOW.isoformat(),
        "payload": {
            "open": {"units": 280, "nano": 0},
            "high": {"units": 281, "nano": 0},
            "low": {"units": 279, "nano": 0},
            "close": {"units": 280, "nano": 500_000_000},
            "volume": 42_000,
            "is_complete": True,
        },
    }


def test_scientific_candle_writer_recovers_without_duplicate_or_offset_recommit() -> None:
    store = _RecoveringCandleStore()
    consumer = _Consumer(_Message(_raw_candle()))

    ScientificCandleKafkaRuntime(
        consumer=consumer,
        processor=ScientificCandleJournalProcessor(store),
        backoff=BoundedExponentialBackoff(
            base_seconds=0.01,
            maximum_seconds=0.04,
        ),
        random_value=lambda: 0.5,
    ).run()

    assert store.calls == 2
    assert tuple(store.rows) == ("candle-event-1",)
    assert consumer.commits == 1
    assert consumer.closed is True
