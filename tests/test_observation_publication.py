from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from hashlib import sha256
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse

import pytest

from tinvest_signal_engine.adapters import clickhouse_detector_observations
from tinvest_signal_engine.adapters.clickhouse_detector_observations import (
    ClickHouseDetectorObservationSink,
    detector_observation_payload_fingerprint,
)
from tinvest_signal_engine.application.observation_publication import (
    DurableObservationPublisher,
    ObservationPublicationFailure,
    ObservationPublicationTask,
)
from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.services.observation_worker import (
    validate_transport_timing,
)


def _observation() -> DetectorObservation:
    return DetectorObservation(
        observation_id="00000000-0000-0000-0000-000000000111",
        source_event_id="event-111",
        observed_at=datetime(2026, 7, 14, 21, 59, 58, tzinfo=timezone.utc),
        instrument_id="SBER_TQBR",
        source_event_type="trade",
        signal_type="price_jump",
        metric_value=12.5,
        baseline_value=10.0,
        z_score=2.5,
        threshold_value=3.0,
        threshold_passed=False,
        detector_passed=False,
        signal_emitted=False,
        window_seconds=60,
        sampling_policy_version="history-v1",
        detector_config_version="detector-v7",
        expectation_catalog_version="catalog-v3",
        provenance_status="complete",
    )


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def read(self) -> bytes:
        return b""


def test_clickhouse_sink_maps_0351_columns_and_seals_flags(monkeypatch) -> None:
    requests = []
    monkeypatch.setattr(
        clickhouse_detector_observations,
        "urlopen",
        lambda request, timeout: requests.append(request) or _Response(),
    )
    sink = ClickHouseDetectorObservationSink(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="not-in-payload",
    )

    sink.persist(_observation())

    assert len(requests) == 1
    request = requests[0]
    query = parse_qs(urlparse(request.full_url).query)
    assert query == {"database": ["signal_engine"]}
    lines = request.data.decode("utf-8").splitlines()
    assert lines[0] == "INSERT INTO detector_observations FORMAT JSONEachRow"
    row = json.loads(lines[1])
    assert row["session_date"] == "2026-07-15"
    assert row["observed_at"] == "2026-07-14T21:59:58.000000Z"
    assert len(row["payload_fingerprint"]) == 64
    fingerprint = row.pop("payload_fingerprint")
    canonical = json.dumps(
        row,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    assert fingerprint == sha256(canonical).hexdigest()
    features = json.loads(row["features_json"])
    assert features == {
        "baseline_value": 10.0,
        "detector_passed": False,
        "provenance_status": "complete",
        "sampling_policy_version": "history-v1",
        "signal_emitted": False,
        "source_event_type": "trade",
        "window_seconds": 60,
        "z_score": 2.5,
    }
    assert "WHERE NOT EXISTS" not in request.data.decode("utf-8")
    assert "insert_deduplication_token" not in request.full_url
    assert "not-in-payload" not in request.data.decode("utf-8")
    assert "not-in-payload" not in request.full_url


def test_payload_fingerprint_survives_clickhouse_readback_normalization(
    monkeypatch,
) -> None:
    requests = []
    monkeypatch.setattr(
        clickhouse_detector_observations,
        "urlopen",
        lambda request, timeout: requests.append(request) or _Response(),
    )
    sink = ClickHouseDetectorObservationSink(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
    )
    sink.persist(_observation())
    row = json.loads(requests[0].data.decode("utf-8").splitlines()[1])
    expected = row.pop("payload_fingerprint")
    row["observed_at"] = "2026-07-15 00:59:58.000000+03:00"
    row["sample_weight"] = 1
    row["features_json"] = json.dumps(
        json.loads(row["features_json"]),
        ensure_ascii=False,
        indent=2,
    )

    assert detector_observation_payload_fingerprint(row) == expected


def test_clickhouse_transport_error_is_classified(monkeypatch) -> None:
    def fail(*args, **kwargs):
        raise URLError("offline")

    monkeypatch.setattr(clickhouse_detector_observations, "urlopen", fail)
    sink = ClickHouseDetectorObservationSink(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
    )

    try:
        sink.persist(_observation())
    except ObservationPublicationFailure as error:
        assert error.reason_code == "clickhouse_unavailable"
    else:
        raise AssertionError("transport failure must be classified")


def test_clickhouse_sink_batches_rows_and_retries_ambiguous_timeouts(
    monkeypatch,
) -> None:
    requests = []
    monkeypatch.setattr(
        clickhouse_detector_observations,
        "urlopen",
        lambda request, timeout: requests.append(request) or _Response(),
    )
    sink = ClickHouseDetectorObservationSink(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
        timeout_seconds=5,
    )
    second = replace(
        _observation(),
        observation_id="00000000-0000-0000-0000-000000000112",
        source_event_id="event-112",
    )

    sink.persist_many((_observation(), second))

    assert len(requests) == 1
    assert len(requests[0].data.decode("utf-8").splitlines()) == 3

    monkeypatch.setattr(
        clickhouse_detector_observations,
        "urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError()),
    )
    with pytest.raises(ObservationPublicationFailure) as failure:
        sink.persist_many((_observation(), second))
    assert failure.value.reason_code == "clickhouse_unavailable"


def test_clickhouse_timeout_must_fit_inside_claim_lease() -> None:
    validate_transport_timing(timeout_seconds=29.9, lease_seconds=30)
    with pytest.raises(ValueError, match="shorter than claim lease"):
        validate_transport_timing(timeout_seconds=30, lease_seconds=30)


@dataclass
class _Queue:
    task: ObservationPublicationTask | None
    published: int = 0
    failures: list[bool] = field(default_factory=list)

    def claim_many(self, **kwargs):
        task, self.task = self.task, None
        return (task,) if task else ()

    def mark_published(self, task, **kwargs) -> None:
        self.published += 1

    def mark_failed(self, task, **kwargs) -> None:
        self.failures.append(bool(kwargs["dead_letter"]))


@dataclass
class _Sink:
    failure: str | None = None

    def persist_many(self, observations) -> None:
        if self.failure:
            raise ObservationPublicationFailure(self.failure)


@dataclass
class _Metrics:
    outcomes: list[str] = field(default_factory=list)

    def publication_attempted(self, **kwargs) -> None:
        self.outcomes.append(str(kwargs["outcome"]))


def _worker(queue: _Queue, sink: _Sink, metrics: _Metrics):
    now = datetime(2026, 7, 14, tzinfo=timezone.utc)
    return DurableObservationPublisher(
        queue=queue,
        sink=sink,
        metrics=metrics,
        clock=lambda: now,
        lease_seconds=30,
        batch_size=250,
        maximum_attempts=3,
        retry_base_seconds=5,
        retry_maximum_seconds=60,
    )


def test_publication_worker_marks_success() -> None:
    queue = _Queue(ObservationPublicationTask(_observation(), 1))
    metrics = _Metrics()

    result = _worker(queue, _Sink(), metrics).run_once()

    assert result.outcome == "published"
    assert queue.published == 1
    assert metrics.outcomes == ["published"]


def test_publication_worker_dead_letters_at_attempt_limit() -> None:
    queue = _Queue(ObservationPublicationTask(_observation(), 3))
    metrics = _Metrics()

    result = _worker(queue, _Sink("invalid_schema"), metrics).run_once()

    assert result.outcome == "dead_letter"
    assert queue.failures == [True]
    assert metrics.outcomes == ["dead_letter"]
