"""Worker heartbeat use case and persistence port."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Protocol

from tinvest_signal_engine.domain.worker_health import (
    WorkerHealthSnapshot,
    WorkerState,
)


class WorkerHealthSink(Protocol):
    def persist(self, snapshot: WorkerHealthSnapshot) -> None: ...


class WorkerHealthReporter(Protocol):
    def heartbeat(self, *, force: bool = False) -> None: ...

    def succeeded(self, *, force: bool = False) -> None: ...

    def failed(self, reason_code: str) -> None: ...


class NoopWorkerHealthReporter:
    def heartbeat(self, *, force: bool = False) -> None:
        del force

    def succeeded(self, *, force: bool = False) -> None:
        del force

    def failed(self, reason_code: str) -> None:
        del reason_code


class WorkerHealthTracker:
    def __init__(
        self,
        *,
        worker_id: str,
        sink: WorkerHealthSink,
        stale_after_seconds: int = 90,
        minimum_write_interval_seconds: float = 15.0,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if minimum_write_interval_seconds < 0:
            raise ValueError("worker health write interval must not be negative")
        self._worker_id = worker_id
        self._sink = sink
        self._stale_after_seconds = stale_after_seconds
        self._minimum_write_interval_seconds = minimum_write_interval_seconds
        self._clock = clock
        now = self._now()
        self._started_at = now
        self._last_heartbeat_at = now
        self._last_success_at: datetime | None = None
        self._last_error_at: datetime | None = None
        self._last_persisted_at: datetime | None = None
        self._reason_code: str | None = None
        self._consecutive_failures = 0
        self._state = WorkerState.STARTING
        self._persist(now, force=True)

    def heartbeat(self, *, force: bool = False) -> None:
        now = self._now()
        self._last_heartbeat_at = now
        self._persist(now, force=force)

    def succeeded(self, *, force: bool = False) -> None:
        now = self._now()
        self._state = WorkerState.ACTIVE
        self._last_heartbeat_at = now
        self._last_success_at = now
        self._reason_code = None
        self._consecutive_failures = 0
        self._persist(now, force=force)

    def failed(self, reason_code: str) -> None:
        now = self._now()
        self._state = WorkerState.DEGRADED
        self._last_heartbeat_at = now
        self._last_error_at = now
        self._reason_code = reason_code
        self._consecutive_failures += 1
        self._persist(now, force=True)

    def _persist(self, now: datetime, *, force: bool) -> None:
        if not force and self._last_persisted_at is not None:
            elapsed = (now - self._last_persisted_at).total_seconds()
            if elapsed < self._minimum_write_interval_seconds:
                return
        self._sink.persist(
            WorkerHealthSnapshot(
                schema_version="worker-health-v1",
                worker_id=self._worker_id,
                state=self._state,
                started_at=self._started_at,
                last_heartbeat_at=self._last_heartbeat_at,
                last_success_at=self._last_success_at,
                last_error_at=self._last_error_at,
                reason_code=self._reason_code,
                consecutive_failures=self._consecutive_failures,
                stale_after_seconds=self._stale_after_seconds,
            )
        )
        self._last_persisted_at = now

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("worker health clock must return an aware timestamp")
        return now.astimezone(timezone.utc)
