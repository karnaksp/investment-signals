"""Idempotent event-to-signal processing use case and ports."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Protocol, Sequence

from tinvest_signal_engine.application.observability import ReliabilityMetrics
from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.domain.reliable_processing import (
    PreparedSignal,
    SignalRecord,
)


@dataclass(frozen=True)
class BrokerEvent:
    event_id: str
    event_type: str
    topic: str
    partition_id: int
    offset_id: int
    payload_sha256: bytes
    payload: dict[str, object]

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("event_id must not be empty")
        if self.partition_id < 0 or self.offset_id < 0:
            raise ValueError("broker partition and offset must be non-negative")
        if len(self.payload_sha256) != 32:
            raise ValueError("payload_sha256 must be SHA-256")


@dataclass(frozen=True)
class StoredEvent:
    signals: tuple[SignalRecord, ...]
    replayed: bool


@dataclass(frozen=True)
class DetectionBatch:
    """Application-owned unit that must be staged in one durable transaction."""

    signals: tuple[PreparedSignal, ...] = ()
    observations: tuple[DetectorObservation, ...] = ()


class BatchDetectionPort(Protocol):
    def detect_batch(self, payload: dict[str, object]) -> DetectionBatch: ...


class AtomicDetectionStore(Protocol):
    """Future persistence boundary; implementations must not dual-write."""

    def find_processed(self, event: BrokerEvent) -> StoredEvent | None: ...

    def persist_detection_once(
        self,
        event: BrokerEvent,
        batch: DetectionBatch,
    ) -> StoredEvent: ...


class DetectionPort(Protocol):
    def detect(self, payload: dict[str, object]) -> Sequence[PreparedSignal]: ...


class ReliableProcessingStore(Protocol):
    def find_processed(self, event: BrokerEvent) -> StoredEvent | None: ...

    def persist_once(
        self,
        event: BrokerEvent,
        signals: Sequence[PreparedSignal],
    ) -> StoredEvent: ...


class SignalPublisher(Protocol):
    def publish(self, signals: Sequence[SignalRecord]) -> None: ...


class ReliableEventProcessor:
    def __init__(
        self,
        *,
        detector: DetectionPort,
        store: ReliableProcessingStore,
        publisher: SignalPublisher,
        metrics: ReliabilityMetrics,
    ) -> None:
        self._detector = detector
        self._store = store
        self._publisher = publisher
        self._metrics = metrics

    def process(self, event: BrokerEvent) -> StoredEvent:
        started = monotonic()
        existing = self._store.find_processed(event)
        if existing is not None:
            self._publisher.publish(existing.signals)
            self._observe(event, existing, started)
            return existing

        prepared = tuple(self._detector.detect(event.payload))
        stored = self._store.persist_once(event, prepared)
        self._publisher.publish(stored.signals)
        self._observe(event, stored, started)
        return stored

    def _observe(
        self,
        event: BrokerEvent,
        stored: StoredEvent,
        started: float,
    ) -> None:
        self._metrics.event_processed(
            event_type=event.event_type,
            outcome="replayed" if stored.replayed else "stored",
            signal_types=tuple(signal.signal_type for signal in stored.signals),
            duration_seconds=max(0.0, monotonic() - started),
        )
