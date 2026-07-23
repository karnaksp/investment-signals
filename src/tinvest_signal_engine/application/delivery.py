"""Durable delivery worker use case and ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Protocol

from tinvest_signal_engine.application.observability import ReliabilityMetrics
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessDecision,
)
from tinvest_signal_engine.domain.reliable_processing import (
    DeliveryTask,
    retry_decision,
)


class DeliveryFailure(RuntimeError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class DeliveryQueue(Protocol):
    def claim(
        self,
        *,
        available_at: datetime,
        lease_until: datetime,
    ) -> DeliveryTask | None: ...

    def mark_delivered(self, task: DeliveryTask, *, delivered_at: datetime) -> None: ...

    def mark_failed(
        self,
        task: DeliveryTask,
        *,
        reason_code: str,
        next_attempt_at: datetime,
        dead_letter: bool,
    ) -> None: ...


class DeliverySender(Protocol):
    def send(self, task: DeliveryTask) -> None: ...


class QueuedDeliveryRecoveryGuard(Protocol):
    def evaluate(self, task: DeliveryTask) -> DeliveryFreshnessDecision: ...


@dataclass(frozen=True)
class DeliveryRunResult:
    outcome: str
    task: DeliveryTask | None


class DurableDeliveryWorker:
    def __init__(
        self,
        *,
        queue: DeliveryQueue,
        sender: DeliverySender,
        metrics: ReliabilityMetrics,
        clock: Callable[[], datetime],
        lease_seconds: int,
        maximum_attempts: int,
        retry_base_seconds: int,
        retry_maximum_seconds: int,
        recovery_guard: QueuedDeliveryRecoveryGuard | None = None,
    ) -> None:
        self._queue = queue
        self._sender = sender
        self._metrics = metrics
        self._clock = clock
        self._lease_seconds = max(1, lease_seconds)
        self._maximum_attempts = max(1, maximum_attempts)
        self._retry_base_seconds = max(1, retry_base_seconds)
        self._retry_maximum_seconds = max(1, retry_maximum_seconds)
        self._recovery_guard = recovery_guard

    def run_once(self) -> DeliveryRunResult:
        now = self._clock()
        task = self._queue.claim(
            available_at=now,
            lease_until=now + timedelta(seconds=self._lease_seconds),
        )
        if task is None:
            return DeliveryRunResult("idle", None)
        if self._recovery_guard is not None:
            freshness = self._recovery_guard.evaluate(task)
            if not freshness.allow_external_delivery:
                self._queue.mark_failed(
                    task,
                    reason_code=freshness.reason_code,
                    next_attempt_at=now,
                    dead_letter=True,
                )
                self._metrics.delivery_attempted(
                    destination_type=task.destination_type,
                    outcome="suppressed_stale",
                    attempt_count=task.attempt_count,
                )
                return DeliveryRunResult("suppressed_stale", task)
        try:
            self._sender.send(task)
        except DeliveryFailure as failure:
            decision = retry_decision(
                attempt_count=task.attempt_count,
                maximum_attempts=self._maximum_attempts,
                base_delay_seconds=self._retry_base_seconds,
                maximum_delay_seconds=self._retry_maximum_seconds,
            )
            failed_at = self._clock()
            self._queue.mark_failed(
                task,
                reason_code=failure.reason_code,
                next_attempt_at=(failed_at + timedelta(seconds=decision.delay_seconds)),
                dead_letter=decision.dead_letter,
            )
            outcome = "dead_letter" if decision.dead_letter else "retry"
            self._metrics.delivery_attempted(
                destination_type=task.destination_type,
                outcome=outcome,
                attempt_count=task.attempt_count,
            )
            return DeliveryRunResult(outcome, task)

        self._queue.mark_delivered(task, delivered_at=self._clock())
        self._metrics.delivery_attempted(
            destination_type=task.destination_type,
            outcome="delivered",
            attempt_count=task.attempt_count,
        )
        return DeliveryRunResult("delivered", task)
