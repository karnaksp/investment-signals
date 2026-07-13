from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tinvest_signal_engine.application.delivery import (
    DeliveryFailure,
    DurableDeliveryWorker,
)
from tinvest_signal_engine.application.reliable_processing import (
    BrokerEvent,
    DetectionBatch,
    ReliableEventProcessor,
    StoredEvent,
)
from tinvest_signal_engine.domain.reliable_processing import (
    DeliveryTask,
    PreparedSignal,
    SignalRecord,
)
from tinvest_signal_engine.domain.detector_observations import DetectorObservation


@dataclass
class FakeMetrics:
    events: list[str] = field(default_factory=list)
    deliveries: list[str] = field(default_factory=list)

    def event_processed(self, **kwargs) -> None:
        self.events.append(str(kwargs["outcome"]))

    def dead_lettered(self, **kwargs) -> None:
        pass

    def offset_committed(self) -> None:
        pass

    def delivery_attempted(self, **kwargs) -> None:
        self.deliveries.append(str(kwargs["outcome"]))


def _signal() -> SignalRecord:
    timestamp = datetime(2026, 7, 1, tzinfo=timezone.utc)
    return SignalRecord(
        signal_id="00000000-0000-0000-0000-000000000001",
        detected_at=timestamp,
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="price_jump",
        severity=2,
        metric_value=1.0,
        baseline_value=0.5,
        z_score=3.0,
        window_seconds=60,
        summary="signal",
        payload={"delivery_status": "delivered"},
        source_event_id="event-1",
        source_event_at=timestamp,
        signal_schema_version="1.0.0",
        expectation_catalog_version="1.0.0",
        detector_config_version="detector-1",
        delivery_config_version="delivery-1",
        cost_model_version="cost-1",
        provenance_status="complete",
    )


def _event() -> BrokerEvent:
    return BrokerEvent(
        event_id="event-1",
        event_type="trade",
        topic="marketdata.raw",
        partition_id=0,
        offset_id=42,
        payload_sha256=b"x" * 32,
        payload={"event_id": "event-1", "event_type": "trade"},
    )


def _observation() -> DetectorObservation:
    timestamp = datetime(2026, 7, 1, tzinfo=timezone.utc)
    return DetectorObservation(
        observation_id="00000000-0000-0000-0000-000000000101",
        source_event_id="event-1",
        observed_at=timestamp,
        instrument_id="SBER_TQBR",
        source_event_type="trade",
        signal_type="price_jump",
        metric_value=1.0,
        baseline_value=0.5,
        z_score=2.0,
        threshold_value=3.0,
        threshold_passed=False,
        detector_passed=False,
        signal_emitted=False,
        window_seconds=60,
        sampling_policy_version="history-v1",
        detector_config_version="detector-1",
        expectation_catalog_version="catalog-1",
        provenance_status="complete",
    )


@dataclass
class FakeDetector:
    calls: int = 0

    def detect_batch(self, payload) -> DetectionBatch:
        self.calls += 1
        return DetectionBatch(
            signals=(PreparedSignal(_signal()),),
            observations=(_observation(),),
        )


@dataclass
class FakeStore:
    stored: StoredEvent | None = None
    fail_next_persist: bool = False
    observations: tuple[DetectorObservation, ...] = ()

    def find_processed(self, event: BrokerEvent) -> StoredEvent | None:
        if self.stored is None:
            return None
        return StoredEvent(self.stored.signals, replayed=True)

    def persist_detection_once(self, event, batch) -> StoredEvent:
        if self.fail_next_persist:
            self.fail_next_persist = False
            raise RuntimeError("database unavailable")
        if self.stored is None:
            self.observations = batch.observations
            self.stored = StoredEvent(
                tuple(item.signal for item in batch.signals),
                replayed=False,
            )
        return self.stored


@dataclass
class FakePublisher:
    fail_next: bool = False
    published: list[str] = field(default_factory=list)

    def publish(self, signals) -> None:
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("broker unavailable")
        self.published.extend(signal.signal_id for signal in signals)


def test_crash_after_database_commit_replays_without_duplicate_detection() -> None:
    detector = FakeDetector()
    store = FakeStore()
    publisher = FakePublisher(fail_next=True)
    processor = ReliableEventProcessor(
        detector=detector,
        store=store,
        publisher=publisher,
        metrics=FakeMetrics(),
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        processor.process(_event())
    replayed = processor.process(_event())

    assert replayed.replayed is True
    assert detector.calls == 1
    assert publisher.published == [_signal().signal_id]
    assert len(store.stored.signals) == 1  # type: ignore[union-attr]
    assert store.observations == (_observation(),)


def test_database_rollback_allows_retry_without_losing_signal() -> None:
    detector = FakeDetector()
    store = FakeStore(fail_next_persist=True)
    publisher = FakePublisher()
    processor = ReliableEventProcessor(
        detector=detector,
        store=store,
        publisher=publisher,
        metrics=FakeMetrics(),
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        processor.process(_event())
    stored = processor.process(_event())

    assert stored.replayed is False
    assert detector.calls == 2
    assert publisher.published == [_signal().signal_id]
    assert len(stored.signals) == 1


@dataclass
class FakeQueue:
    task: DeliveryTask | None
    failures: list[tuple[bool, int]] = field(default_factory=list)
    delivered: int = 0

    def claim(self, **kwargs) -> DeliveryTask | None:
        task, self.task = self.task, None
        return task

    def mark_delivered(self, task, **kwargs) -> None:
        self.delivered += 1

    def mark_failed(self, task, **kwargs) -> None:
        self.failures.append(
            (bool(kwargs["dead_letter"]), task.attempt_count)
        )


@dataclass
class FakeSender:
    failure: str | None = None

    def send(self, task: DeliveryTask) -> None:
        if self.failure:
            raise DeliveryFailure(self.failure)


def _delivery_task(attempt_count: int) -> DeliveryTask:
    return DeliveryTask(
        outbox_id="00000000-0000-0000-0000-000000000002",
        signal_id=_signal().signal_id,
        destination_type="webhook",
        payload={},
        attempt_count=attempt_count,
    )


def _worker(queue: FakeQueue, sender: FakeSender, metrics: FakeMetrics):
    now = datetime(2026, 7, 1, tzinfo=timezone.utc)
    return DurableDeliveryWorker(
        queue=queue,
        sender=sender,
        metrics=metrics,
        clock=lambda: now,
        lease_seconds=30,
        maximum_attempts=3,
        retry_base_seconds=5,
        retry_maximum_seconds=60,
    )


def test_delivery_failure_is_retried_before_limit() -> None:
    queue = FakeQueue(_delivery_task(2))
    metrics = FakeMetrics()

    result = _worker(queue, FakeSender("timeout"), metrics).run_once()

    assert result.outcome == "retry"
    assert queue.failures == [(False, 2)]
    assert metrics.deliveries == ["retry"]


def test_delivery_failure_moves_to_dead_letter_at_limit() -> None:
    queue = FakeQueue(_delivery_task(3))
    metrics = FakeMetrics()

    result = _worker(queue, FakeSender("http_500"), metrics).run_once()

    assert result.outcome == "dead_letter"
    assert queue.failures == [(True, 3)]
    assert metrics.deliveries == ["dead_letter"]


def test_redis_runtime_uses_aof_and_noeviction() -> None:
    config = (
        Path(__file__).resolve().parents[1] / "conf" / "redis.conf"
    ).read_text(encoding="utf-8")

    assert "appendonly yes" in config
    assert "appendfsync everysec" in config
    assert "maxmemory-policy noeviction" in config
