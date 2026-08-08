"""Framework-free worker liveness state shared across service boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import re


_REASON_CODE = re.compile(r"^[a-z0-9][a-z0-9_]{0,95}$")


class WorkerState(str, Enum):
    STARTING = "starting"
    ACTIVE = "active"
    DEGRADED = "degraded"


@dataclass(frozen=True, slots=True)
class WorkerHealthSnapshot:
    schema_version: str
    worker_id: str
    state: WorkerState
    started_at: datetime
    last_heartbeat_at: datetime
    last_success_at: datetime | None
    last_error_at: datetime | None
    reason_code: str | None
    consecutive_failures: int
    stale_after_seconds: int

    def __post_init__(self) -> None:
        if self.schema_version != "worker-health-v1":
            raise ValueError("unsupported worker health schema")
        if not self.worker_id.strip():
            raise ValueError("worker id must not be empty")
        for value in (
            self.started_at,
            self.last_heartbeat_at,
            self.last_success_at,
            self.last_error_at,
        ):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError("worker health timestamps must be timezone-aware")
        if self.reason_code is not None and not _REASON_CODE.fullmatch(
            self.reason_code
        ):
            raise ValueError("worker health reason code is invalid")
        if self.consecutive_failures < 0:
            raise ValueError("worker health failures must not be negative")
        if self.stale_after_seconds < 1:
            raise ValueError("worker health stale threshold must be positive")
        if self.state is WorkerState.DEGRADED and self.reason_code is None:
            raise ValueError("degraded worker health requires a reason code")
        if self.state is not WorkerState.DEGRADED and self.reason_code is not None:
            raise ValueError("healthy worker state cannot retain a failure reason")
