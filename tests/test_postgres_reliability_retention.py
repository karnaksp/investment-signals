from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.adapters.postgres_reliability import (
    PostgresObservationPublicationQueue,
)
from tinvest_signal_engine.domain.market_schedule import MarketSchedule
from tinvest_signal_engine.services.observation_worker import (
    should_purge_processed_events,
)


class _Cursor:
    def __init__(self) -> None:
        self.rowcount = 37
        self.statement = ""
        self.params: tuple[object, ...] = ()

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str, params: tuple[object, ...]) -> None:
        self.statement = statement
        self.params = params


class _Connection:
    def __init__(self) -> None:
        self.last_cursor: _Cursor | None = None

    def cursor(self) -> _Cursor:
        self.last_cursor = _Cursor()
        return self.last_cursor


def test_processed_event_purge_uses_bounded_physical_row_deletes() -> None:
    connection = _Connection()
    queue = PostgresObservationPublicationQueue(connection)  # type: ignore[arg-type]
    before = datetime(2026, 8, 1, tzinfo=timezone.utc)

    assert queue.purge_processed_events(before=before, limit=5_000) == 37

    assert connection.last_cursor is not None
    sql = connection.last_cursor.statement
    assert "snapshot_events AS MATERIALIZED" in sql
    assert "purgeable AS MATERIALIZED" in sql
    assert "SELECT inbox.ctid" in sql
    assert "LEFT JOIN snapshot_events AS snapshot" in sql
    assert "snapshot.source_event_id IS NULL" in sql
    assert "ORDER BY inbox.processed_at" in sql
    assert "target.ctid = purgeable.ctid" in sql
    assert "OFFSET 0" not in sql
    assert connection.last_cursor.params == (before, 5_000)


def test_published_observation_purge_uses_bounded_retention_index_order() -> None:
    connection = _Connection()
    queue = PostgresObservationPublicationQueue(connection)  # type: ignore[arg-type]
    before = datetime(2026, 8, 1, tzinfo=timezone.utc)

    assert queue.purge_published(before=before, limit=50_000) == 37

    assert connection.last_cursor is not None
    sql = connection.last_cursor.statement
    assert "WITH purgeable AS MATERIALIZED" in sql
    assert "status = 'published'" in sql
    assert "INTERVAL '24 hours'" in sql
    assert "ORDER BY published_at, observation_id" in sql
    assert connection.last_cursor.params == (before, 50_000)


def test_processed_event_purge_runs_only_outside_live_collection() -> None:
    schedule = MarketSchedule()

    assert not should_purge_processed_events(
        now=datetime(2026, 8, 4, 18, 0, tzinfo=timezone.utc),
        market_schedule=schedule,
    )
    assert should_purge_processed_events(
        now=datetime(2026, 8, 4, 21, 0, tzinfo=timezone.utc),
        market_schedule=schedule,
    )
    assert should_purge_processed_events(
        # Saturday noon in Moscow: no live collection to protect.
        now=datetime(2026, 8, 8, 9, 0, tzinfo=timezone.utc),
        market_schedule=schedule,
    )
