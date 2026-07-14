"""Idempotent event-to-signal processing use case and ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
class DetectorStateCheckpoint:
    """Opaque detector state owned by an adapter and durably fenced by Kafka offset."""

    instrument_id: str
    state_schema_version: str
    detector_config_version: str
    payload: bytes
    payload_sha256: bytes

    def __post_init__(self) -> None:
        if not self.instrument_id:
            raise ValueError("checkpoint instrument_id must not be empty")
        if not self.state_schema_version or not self.detector_config_version:
            raise ValueError("checkpoint versions must not be empty")
        if not self.payload:
            raise ValueError("checkpoint payload must not be empty")
        if len(self.payload_sha256) != 32:
            raise ValueError("checkpoint payload_sha256 must be SHA-256")


@dataclass(frozen=True)
class DetectorConfigAcknowledgement:
    """Runtime proof that a detector instance loaded a config revision."""

    detector_instance_id: str
    detector_config_version: str
    status: str
    loaded_at: datetime
    configured_instruments_count: int = 0
    failure_reason_code: str | None = None

    def __post_init__(self) -> None:
        if not self.detector_instance_id:
            raise ValueError("detector_instance_id must not be empty")
        if not self.detector_config_version:
            raise ValueError("detector_config_version must not be empty")
        if self.status not in {"loaded", "failed"}:
            raise ValueError("detector config acknowledgement status is invalid")
        if self.configured_instruments_count < 0:
            raise ValueError("configured_instruments_count must be non-negative")
        if self.status == "failed" and not self.failure_reason_code:
            raise ValueError("failed acknowledgement requires a reason code")
        if self.status == "loaded" and self.failure_reason_code:
            raise ValueError("loaded acknowledgement cannot include failure reason")


@dataclass(frozen=True)
class DetectionBatch:
    """Application-owned unit that must be staged in one durable transaction."""

    signals: tuple[PreparedSignal, ...] = ()
    observations: tuple[DetectorObservation, ...] = ()
    checkpoint: DetectorStateCheckpoint | None = None


class BatchDetectionPort(Protocol):
    def detect_batch(self, payload: dict[str, object]) -> DetectionBatch: ...

    def replace_state(
        self,
        checkpoints: Sequence[DetectorStateCheckpoint],
    ) -> None: ...


class DetectorConfigAcknowledgementSink(Protocol):
    def persist_detector_config_ack(
        self,
        acknowledgement: DetectorConfigAcknowledgement,
    ) -> None: ...


class AtomicDetectionStore(Protocol):
    """Persist the inbox, signals, delivery, and observation outboxes atomically."""

    def find_processed(self, event: BrokerEvent) -> StoredEvent | None: ...

    def persist_detection_once(
        self,
        event: BrokerEvent,
        batch: DetectionBatch,
    ) -> StoredEvent: ...

    def load_state_checkpoints(self) -> tuple[DetectorStateCheckpoint, ...]: ...


class SignalPublisher(Protocol):
    def publish(self, signals: Sequence[SignalRecord]) -> None: ...


class ReliableEventProcessor:
    def __init__(
        self,
        *,
        detector: BatchDetectionPort,
        store: AtomicDetectionStore,
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

        try:
            batch = self._detector.detect_batch(event.payload)
            stored = self._store.persist_detection_once(event, batch)
        except Exception:
            # Detection mutates rolling windows before the database transaction.
            # PostgreSQL is the recovery source of truth after a rollback.
            self._detector.replace_state(self._store.load_state_checkpoints())
            raise
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
