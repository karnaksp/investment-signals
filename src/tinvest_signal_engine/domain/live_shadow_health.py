"""Operational health of the prospective live-shadow worker."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum


LIVE_SHADOW_HEALTH_SCHEMA_VERSION = "live-shadow-health-v1"
LIVE_SHADOW_STARTING = "live_shadow_starting"
LIVE_SHADOW_ACTIVE = "live_shadow_active"
LIVE_SHADOW_PASS_FAILED = "live_shadow_pass_failed"


class LiveShadowWorkerState(StrEnum):
    STARTING = "starting"
    ACTIVE = "active"
    DEGRADED = "degraded"


@dataclass(frozen=True, slots=True)
class LiveShadowHealthSnapshot:
    state: LiveShadowWorkerState
    started_at: datetime
    last_success_at: datetime | None
    last_error_at: datetime | None
    reason_code: str
    consecutive_failures: int
    observations_processed: int
    outcomes_processed: int
    outcomes_unavailable: int
    stale_after_seconds: int

    def __post_init__(self) -> None:
        _aware(self.started_at, "started_at")
        for name in ("last_success_at", "last_error_at"):
            value = getattr(self, name)
            if value is not None:
                _aware(value, name)
        for name in (
            "consecutive_failures",
            "observations_processed",
            "outcomes_processed",
            "outcomes_unavailable",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        expected = {
            LiveShadowWorkerState.STARTING: LIVE_SHADOW_STARTING,
            LiveShadowWorkerState.ACTIVE: LIVE_SHADOW_ACTIVE,
            LiveShadowWorkerState.DEGRADED: LIVE_SHADOW_PASS_FAILED,
        }[self.state]
        if self.reason_code != expected:
            raise ValueError("live-shadow reason code does not match state")

    @classmethod
    def starting(
        cls, *, started_at: datetime, stale_after_seconds: int
    ) -> "LiveShadowHealthSnapshot":
        return cls(
            LiveShadowWorkerState.STARTING,
            started_at,
            None,
            None,
            LIVE_SHADOW_STARTING,
            0,
            0,
            0,
            0,
            stale_after_seconds,
        )

    def succeeded(
        self,
        *,
        succeeded_at: datetime,
        observations_processed: int,
        outcomes_processed: int,
        outcomes_unavailable: int,
    ) -> "LiveShadowHealthSnapshot":
        _aware(succeeded_at, "succeeded_at")
        return replace(
            self,
            state=LiveShadowWorkerState.ACTIVE,
            last_success_at=succeeded_at,
            reason_code=LIVE_SHADOW_ACTIVE,
            consecutive_failures=0,
            observations_processed=observations_processed,
            outcomes_processed=outcomes_processed,
            outcomes_unavailable=outcomes_unavailable,
        )

    def failed(self, *, failed_at: datetime) -> "LiveShadowHealthSnapshot":
        _aware(failed_at, "failed_at")
        return replace(
            self,
            state=LiveShadowWorkerState.DEGRADED,
            last_error_at=failed_at,
            reason_code=LIVE_SHADOW_PASS_FAILED,
            consecutive_failures=self.consecutive_failures + 1,
        )


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
