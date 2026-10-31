"""Map durable delivery payloads to the application recovery guard."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Callable

from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessDecision,
)
from tinvest_signal_engine.domain.reliable_processing import DeliveryTask
from tinvest_signal_engine.domain.daily_positions import (
    REPORT_SIGNAL_TYPE, REPORT_VERSION, daily_report_freshness,
)
from tinvest_signal_engine.domain.commodity_positions import DIGEST_VERSION
from tinvest_signal_engine.serialization import parse_timestamp


class QueuedDeliveryRecoveryAdapter:
    def __init__(self, guard: DeliveryRecoveryGuard, *, clock: Callable[[], datetime] | None = None) -> None:
        self._guard = guard
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def evaluate(self, task: DeliveryTask) -> DeliveryFreshnessDecision:
        if task.payload.get("signal_type") == REPORT_SIGNAL_TYPE:
            payload = task.payload.get("payload")
            now = self._clock()
            try:
                if not isinstance(payload, dict):
                    raise ValueError("unknown daily report version")
                legacy = payload.get("daily_report_version") == REPORT_VERSION and payload.get("source_exchange") == "SHFE"
                digest = (payload.get("daily_report_version") == DIGEST_VERSION
                          and payload.get("source_exchange") == "MULTI" and payload.get("primary_source") == "SHFE")
                if not (legacy or digest):
                    raise ValueError("unknown daily report source")
                return daily_report_freshness(
                    trading_day=date.fromisoformat(payload["trading_day"]),
                    previous_day=date.fromisoformat(payload["previous_day"]),
                    generated_at=parse_timestamp(payload["report_generated_at"]), evaluated_at=now,
                )
            except (KeyError, TypeError, ValueError):
                return DeliveryFreshnessDecision(False, "daily_report_invalid", None, 21600, None, now.date().isoformat())
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
