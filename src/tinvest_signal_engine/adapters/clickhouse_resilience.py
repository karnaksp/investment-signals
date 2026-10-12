"""Outer-layer recovery primitives for transient ClickHouse failures."""

from __future__ import annotations

from dataclasses import dataclass
from random import random
from typing import Callable
from urllib.error import HTTPError, URLError


RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class TransientClickHouseError(RuntimeError):
    """A dependency failure that is safe to retry without changing inputs."""

    def __init__(self, *, operation: str, reason_code: str) -> None:
        super().__init__(f"ClickHouse {operation} temporarily unavailable")
        self.operation = operation
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class BoundedExponentialBackoff:
    """Exponential delay capped at a finite value with symmetric jitter."""

    base_seconds: float = 0.5
    maximum_seconds: float = 30.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.base_seconds <= 0:
            raise ValueError("backoff base_seconds must be positive")
        if self.maximum_seconds < self.base_seconds:
            raise ValueError("backoff maximum_seconds must not be below base")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("backoff jitter_ratio must be between zero and one")

    def delay(
        self,
        consecutive_failures: int,
        *,
        random_value: Callable[[], float] = random,
    ) -> float:
        if consecutive_failures <= 0:
            raise ValueError("consecutive_failures must be positive")
        exponent = min(consecutive_failures - 1, 30)
        uncapped = self.base_seconds * (2**exponent)
        capped = min(uncapped, self.maximum_seconds)
        unit = random_value()
        if not 0 <= unit <= 1:
            raise ValueError("backoff random value must be between zero and one")
        jitter = capped * self.jitter_ratio * ((2 * unit) - 1)
        return max(0.0, min(self.maximum_seconds, capped + jitter))


def transient_clickhouse_error(
    error: BaseException,
    *,
    operation: str,
) -> TransientClickHouseError | None:
    """Classify transport/server failures without copying sensitive messages."""

    if isinstance(error, HTTPError):
        if error.code not in RETRYABLE_HTTP_STATUSES:
            return None
        return TransientClickHouseError(
            operation=operation,
            reason_code=f"http_{error.code}",
        )
    if isinstance(error, (URLError, TimeoutError, ConnectionResetError)):
        return TransientClickHouseError(
            operation=operation,
            reason_code="connection_unavailable",
        )
    return None
