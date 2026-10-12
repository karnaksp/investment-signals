"""Map durable delivery payloads to the application recovery guard."""

from __future__ import annotations

from datetime import datetime

from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessDecision,
)
from tinvest_signal_engine.domain.reliable_processing import DeliveryTask
from tinvest_signal_engine.serialization import parse_timestamp


class QueuedDeliveryRecoveryAdapter:
    def __init__(self, guard: DeliveryRecoveryGuard) -> None:
        self._guard = guard

    def evaluate(self, task: DeliveryTask) -> DeliveryFreshnessDecision:
        raw_source_at = task.payload.get("source_event_at")
        source_event_at: datetime | None = None
        if isinstance(raw_source_at, (str, datetime)):
            try:
                source_event_at = parse_timestamp(raw_source_at)
            except (TypeError, ValueError):
                source_event_at = None
        signal_type = str(task.payload.get("signal_type") or "unknown")
        return self._guard.evaluate(
            source_event_at=source_event_at,
            signal_type=signal_type,
        )
