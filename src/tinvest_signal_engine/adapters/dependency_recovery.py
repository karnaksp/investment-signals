"""Shared outer-loop telemetry and interruptible dependency backoff."""

from __future__ import annotations

import logging
from random import random
from threading import Event
from typing import Callable, Protocol

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)


class DependencyRecoveryMetrics(Protocol):
    def dependency_attempted(
        self,
        *,
        worker: str,
        operation: str,
        outcome: str,
        reason_code: str,
        delay_seconds: float,
    ) -> None: ...


class NoopDependencyRecoveryMetrics:
    def dependency_attempted(
        self,
        *,
        worker: str,
        operation: str,
        outcome: str,
        reason_code: str,
        delay_seconds: float,
    ) -> None:
        del worker, operation, outcome, reason_code, delay_seconds


def wait_for_dependency(
    *,
    worker: str,
    error: TransientClickHouseError,
    consecutive_failures: int,
    stop_event: Event,
    backoff: BoundedExponentialBackoff,
    metrics: DependencyRecoveryMetrics,
    logger: logging.Logger,
    random_value: Callable[[], float] = random,
) -> bool:
    """Record a sanitized failure and wait; return true when shutdown was requested."""

    delay = backoff.delay(
        consecutive_failures,
        random_value=random_value,
    )
    metrics.dependency_attempted(
        worker=worker,
        operation=error.operation,
        outcome="retry",
        reason_code=error.reason_code,
        delay_seconds=delay,
    )
    logger.warning(
        "ClickHouse temporarily unavailable; retrying bounded operation",
        extra={
            "attempt": consecutive_failures,
            "delay_seconds": delay,
            "operation": error.operation,
            "reason_code": error.reason_code,
            "worker": worker,
        },
    )
    return stop_event.wait(delay)


def record_dependency_recovered(
    *,
    worker: str,
    operation: str,
    consecutive_failures: int,
    metrics: DependencyRecoveryMetrics,
    logger: logging.Logger,
) -> None:
    if consecutive_failures <= 0:
        return
    metrics.dependency_attempted(
        worker=worker,
        operation=operation,
        outcome="recovered",
        reason_code="recovered",
        delay_seconds=0.0,
    )
    logger.info(
        "ClickHouse dependency recovered",
        extra={
            "attempts": consecutive_failures,
            "operation": operation,
            "worker": worker,
        },
    )
