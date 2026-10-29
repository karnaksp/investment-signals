"""Application service and persistence port for ingestor health."""

from __future__ import annotations

from datetime import datetime, timedelta
from threading import RLock
from typing import Callable, Protocol

from tinvest_signal_engine.domain.ingestor_health import (
    INGESTOR_CONNECTING,
    INGESTOR_SCHEDULED_SLEEP,
    IngestorHealthSnapshot,
)


class IngestorHealthSnapshotStore(Protocol):
    def save(self, snapshot: IngestorHealthSnapshot) -> None: ...


class IngestorHealthTracker:
    """Serialize transitions and persist heartbeat timestamps at most once a second.

    Every event still updates the in-memory snapshot used by the watchdog.
    State changes and explicit failures/configuration updates are durable
    immediately, without putting a synchronous file write on every live event.
    """

    def __init__(
        self,
        *,
        store: IngestorHealthSnapshotStore,
        clock: Callable[[], datetime],
        stale_after_seconds: int,
        initial_snapshot: IngestorHealthSnapshot | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._lock = RLock()
        self._snapshot = initial_snapshot or IngestorHealthSnapshot.starting(
            started_at=self._clock(),
            stale_after_seconds=stale_after_seconds,
        )
        self._store.save(self._snapshot)
        self._last_saved_at = self._clock()

    @property
    def snapshot(self) -> IngestorHealthSnapshot:
        with self._lock:
            return self._snapshot

    def connecting(
        self,
        *,
        configured_instruments: int,
        reason_code: str = INGESTOR_CONNECTING,
    ) -> IngestorHealthSnapshot:
        with self._lock:
            return self._save(
                self._snapshot.connecting(
                    configured_instruments=configured_instruments,
                    reason_code=reason_code,
                )
            )

    def market_event_observed(
        self,
        *,
        market_event_at: datetime,
    ) -> IngestorHealthSnapshot:
        with self._lock:
            return self._save(
                self._snapshot.market_event_observed(
                    market_event_at=market_event_at,
                ),
                heartbeat=True,
            )

    def sleeping(
        self,
        *,
        configured_instruments: int,
    ) -> IngestorHealthSnapshot:
        return self.connecting(
            configured_instruments=configured_instruments,
            reason_code=INGESTOR_SCHEDULED_SLEEP,
        )

    def publish_succeeded(
        self,
        *,
        market_event_at: datetime,
    ) -> IngestorHealthSnapshot:
        with self._lock:
            return self._save(
                self._snapshot.publish_succeeded(
                    market_event_at=market_event_at,
                    succeeded_at=self._clock(),
                ),
                heartbeat=True,
            )

    def failed(self, *, reason_code: str) -> IngestorHealthSnapshot:
        with self._lock:
            return self._save(
                self._snapshot.failed(
                    failed_at=self._clock(),
                    reason_code=reason_code,
                )
            )

    def evaluate_staleness(self) -> IngestorHealthSnapshot:
        with self._lock:
            evaluated = self._snapshot.evaluate_staleness(
                evaluated_at=self._clock(),
            )
            if evaluated == self._snapshot:
                return self._snapshot
            return self._save(evaluated)

    def _save(
        self,
        snapshot: IngestorHealthSnapshot,
        *,
        heartbeat: bool = False,
    ) -> IngestorHealthSnapshot:
        now = self._clock()
        elapsed = now - self._last_saved_at
        same_status = (
            snapshot.state == self._snapshot.state
            and snapshot.reason_code == self._snapshot.reason_code
            and snapshot.consecutive_failures == self._snapshot.consecutive_failures
            and snapshot.configured_instruments == self._snapshot.configured_instruments
        )
        if heartbeat and same_status and timedelta(0) <= elapsed < timedelta(seconds=1):
            self._snapshot = snapshot
            return snapshot
        self._store.save(snapshot)
        self._last_saved_at = now
        self._snapshot = snapshot
        return snapshot
