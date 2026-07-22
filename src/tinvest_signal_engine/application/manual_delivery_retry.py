"""Use case for an explicit, single-item delivery dead-letter retry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.reliable_processing import (
    DeadLetterDelivery,
    manual_delivery_retry_decision,
)


class DeadLetterDeliveryQueue(Protocol):
    def get_for_manual_retry(
        self, *, outbox_id: str
    ) -> DeadLetterDelivery | None: ...

    def requeue_dead_letter(
        self,
        delivery: DeadLetterDelivery,
        *,
        available_at: datetime,
    ) -> bool: ...


@dataclass(frozen=True)
class ManualDeliveryRetryResult:
    outcome: str
    delivery: DeadLetterDelivery | None
    reason_code: str


class ManualDeliveryRetry:
    """Preview or atomically requeue one transient delivery failure."""

    def __init__(self, *, queue: DeadLetterDeliveryQueue) -> None:
        self._queue = queue

    def preview(self, *, outbox_id: str) -> ManualDeliveryRetryResult:
        delivery = self._queue.get_for_manual_retry(outbox_id=outbox_id)
        if delivery is None:
            return ManualDeliveryRetryResult(
                "not_found", None, "delivery_not_found"
            )
        decision = manual_delivery_retry_decision(delivery)
        return ManualDeliveryRetryResult(
            "eligible" if decision.allowed else "ineligible",
            delivery,
            decision.reason_code,
        )

    def retry(
        self, *, outbox_id: str, available_at: datetime
    ) -> ManualDeliveryRetryResult:
        if available_at.tzinfo is None or available_at.utcoffset() is None:
            raise ValueError("available_at must be timezone-aware")
        preview = self.preview(outbox_id=outbox_id)
        if preview.outcome != "eligible" or preview.delivery is None:
            return preview
        changed = self._queue.requeue_dead_letter(
            preview.delivery,
            available_at=available_at,
        )
        if not changed:
            return ManualDeliveryRetryResult(
                "conflict",
                preview.delivery,
                "delivery_changed_before_retry",
            )
        return ManualDeliveryRetryResult(
            "requeued",
            preview.delivery,
            "manual_retry_queued",
        )
