"""Use case for suppressing stale external alerts during Kafka catch-up."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Protocol

from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessDecision,
    DeliveryFreshnessPolicy,
)


class DeliveryRecoveryMetrics(Protocol):
    def stale_delivery_suppressed(
        self,
        *,
        reason_code: str,
        signal_type: str,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliveryRecoveryGuard:
    policy: DeliveryFreshnessPolicy
    metrics: DeliveryRecoveryMetrics
    clock: Callable[[], datetime]

    def evaluate(
        self,
        *,
        source_event_at: datetime | None,
        signal_type: str,
    ) -> DeliveryFreshnessDecision:
        decision = self.policy.decide(
            source_event_at=source_event_at,
            evaluated_at=self.clock(),
        )
        if not decision.allow_external_delivery:
            self.metrics.stale_delivery_suppressed(
                reason_code=decision.reason_code,
                signal_type=signal_type,
            )
        return decision
