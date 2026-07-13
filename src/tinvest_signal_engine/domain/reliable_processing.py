"""Framework-independent records and invariants for reliable processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from uuid import UUID, uuid5


_OUTBOX_NAMESPACE = UUID("f5bdc8cb-3731-5755-8136-f510a6c4c2e3")
DELIVERY_DESTINATIONS = frozenset({"telegram", "webhook"})
PROVENANCE_STATUSES = frozenset({"complete", "legacy"})


class EventReplayConflict(RuntimeError):
    """An event id or broker position was reused with different content."""


@dataclass(frozen=True)
class SignalRecord:
    signal_id: str
    detected_at: datetime
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    source_event_type: str
    signal_type: str
    severity: int
    metric_value: float
    baseline_value: float
    z_score: float
    window_seconds: int
    summary: str
    payload: dict[str, object]
    source_event_id: str | None
    source_event_at: datetime | None
    signal_schema_version: str
    expectation_catalog_version: str | None
    detector_config_version: str | None
    delivery_config_version: str | None
    cost_model_version: str | None
    provenance_status: str

    def __post_init__(self) -> None:
        if self.provenance_status not in PROVENANCE_STATUSES:
            raise ValueError("unsupported signal provenance_status")
        if self.provenance_status == "complete":
            required = (
                self.source_event_id,
                self.source_event_at,
                self.expectation_catalog_version,
                self.detector_config_version,
                self.delivery_config_version,
                self.cost_model_version,
            )
            if any(value is None or value == "" for value in required):
                raise ValueError("complete signal provenance requires all versions")


@dataclass(frozen=True)
class DeliveryTarget:
    destination_type: str
    destination_key: str

    def __post_init__(self) -> None:
        if self.destination_type not in DELIVERY_DESTINATIONS:
            raise ValueError(
                f"Unsupported delivery destination: {self.destination_type!r}"
            )
        if not self.destination_key:
            raise ValueError("delivery destination key must not be empty")

    @property
    def key_hash(self) -> bytes:
        return sha256(self.destination_key.encode("utf-8")).digest()


@dataclass(frozen=True)
class PreparedSignal:
    signal: SignalRecord
    delivery_targets: tuple[DeliveryTarget, ...] = ()


@dataclass(frozen=True)
class DeliveryTask:
    outbox_id: str
    signal_id: str
    destination_type: str
    payload: dict[str, object]
    attempt_count: int

    def __post_init__(self) -> None:
        if self.destination_type not in DELIVERY_DESTINATIONS:
            raise ValueError(
                f"Unsupported delivery destination: {self.destination_type!r}"
            )
        if self.attempt_count < 1:
            raise ValueError("claimed delivery attempt_count must be positive")


@dataclass(frozen=True)
class RetryDecision:
    dead_letter: bool
    delay_seconds: int


def retry_decision(
    *,
    attempt_count: int,
    maximum_attempts: int,
    base_delay_seconds: int,
    maximum_delay_seconds: int,
) -> RetryDecision:
    if attempt_count < 1:
        raise ValueError("attempt_count must be positive")
    if maximum_attempts < 1:
        raise ValueError("maximum_attempts must be positive")
    if attempt_count >= maximum_attempts:
        return RetryDecision(dead_letter=True, delay_seconds=0)
    exponent = min(attempt_count - 1, 30)
    delay = max(1, base_delay_seconds) * (2**exponent)
    return RetryDecision(
        dead_letter=False,
        delay_seconds=min(delay, max(1, maximum_delay_seconds)),
    )


def deterministic_outbox_id(
    signal_id: str,
    destination_type: str,
    destination_key_hash: bytes,
) -> str:
    if len(destination_key_hash) != 32:
        raise ValueError("destination_key_hash must be SHA-256")
    name = f"{signal_id}\x1f{destination_type}\x1f{destination_key_hash.hex()}"
    return str(uuid5(_OUTBOX_NAMESPACE, name))
