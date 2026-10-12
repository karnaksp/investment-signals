"""Application tracker for the live-shadow worker heartbeat."""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Protocol

from tinvest_signal_engine.domain.live_shadow_health import LiveShadowHealthSnapshot


class LiveShadowHealthSnapshotStore(Protocol):
    def save(self, snapshot: LiveShadowHealthSnapshot) -> None: ...


class LiveShadowHealthTracker:
    def __init__(
        self,
        *,
        store: LiveShadowHealthSnapshotStore,
        clock: Callable[[], datetime],
        stale_after_seconds: int,
    ) -> None:
        self._store = store
        self._clock = clock
        self._snapshot = LiveShadowHealthSnapshot.starting(
            started_at=clock(), stale_after_seconds=stale_after_seconds
        )
        self._store.save(self._snapshot)

    def succeeded(
        self,
        *,
        observations_processed: int,
        outcomes_processed: int,
        outcomes_unavailable: int,
    ) -> LiveShadowHealthSnapshot:
        self._snapshot = self._snapshot.succeeded(
            succeeded_at=self._clock(),
            observations_processed=observations_processed,
            outcomes_processed=outcomes_processed,
            outcomes_unavailable=outcomes_unavailable,
        )
        self._store.save(self._snapshot)
        return self._snapshot

    def failed(self) -> LiveShadowHealthSnapshot:
        self._snapshot = self._snapshot.failed(failed_at=self._clock())
        self._store.save(self._snapshot)
        return self._snapshot

    def heartbeat(self) -> LiveShadowHealthSnapshot:
        """Refresh liveness without erasing the last completed pass counters."""

        return self.succeeded(
            observations_processed=self._snapshot.observations_processed,
            outcomes_processed=self._snapshot.outcomes_processed,
            outcomes_unavailable=self._snapshot.outcomes_unavailable,
        )
