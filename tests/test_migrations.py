from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.migrations import (
    FilesystemMigrationSource,
    split_sql_statements,
)
from tinvest_signal_engine.application.migrations import MigrationRunner
from tinvest_signal_engine.domain.migrations import (
    AppliedMigration,
    Migration,
    MigrationChecksumConflict,
)


@dataclass
class FakeLedger:
    applied: dict[tuple[str, int], AppliedMigration] = field(default_factory=dict)
    prepare_count: int = 0

    def prepare(self) -> None:
        self.prepare_count += 1

    def list_applied(self, engine: str) -> tuple[AppliedMigration, ...]:
        return tuple(
            item
            for (item_engine, _), item in sorted(self.applied.items())
            if item_engine == engine
        )

    def record(
        self,
        migration: Migration,
        *,
        release_version: str,
        execution_ms: int,
    ) -> None:
        assert release_version == "1.0.0"
        assert execution_ms >= 0
        self.applied[(migration.engine, migration.version)] = AppliedMigration(
            engine=migration.engine,
            version=migration.version,
            name=migration.name,
            checksum_sha256=migration.checksum_sha256,
        )


@dataclass
class FakeExecutor:
    executed: list[int] = field(default_factory=list)

    def execute(self, migration: Migration) -> int:
        self.executed.append(migration.version)
        return 3


class StaticSource:
    def __init__(self, migrations: tuple[Migration, ...]) -> None:
        self._migrations = migrations

    def load(self) -> tuple[Migration, ...]:
        return self._migrations


def _migrations() -> tuple[Migration, ...]:
    return (
        Migration("postgresql", 100, "first", "SELECT 1;\n"),
        Migration("postgresql", 101, "second", "SELECT 2;\n"),
    )


def _runner(
    ledger: FakeLedger,
    executor: FakeExecutor,
    migrations: tuple[Migration, ...] | None = None,
) -> MigrationRunner:
    return MigrationRunner(
        source=StaticSource(migrations or _migrations()),
        ledger=ledger,
        executor=executor,
        engine="postgresql",
        release_version="1.0.0",
    )


def test_fresh_migrations_apply_in_order() -> None:
    ledger = FakeLedger()
    executor = FakeExecutor()

    result = _runner(ledger, executor).run()

    assert result.applied_versions == (100, 101)
    assert result.skipped_versions == ()
    assert executor.executed == [100, 101]
    assert ledger.prepare_count == 1


def test_replay_skips_identical_migrations() -> None:
    ledger = FakeLedger()
    first_executor = FakeExecutor()
    _runner(ledger, first_executor).run()
    replay_executor = FakeExecutor()

    result = _runner(ledger, replay_executor).run()

    assert result.applied_versions == ()
    assert result.skipped_versions == (100, 101)
    assert replay_executor.executed == []


def test_changed_applied_migration_fails_checksum_gate() -> None:
    ledger = FakeLedger()
    executor = FakeExecutor()
    _runner(ledger, executor).run()
    changed = (
        Migration("postgresql", 100, "first", "SELECT 999;\n"),
        _migrations()[1],
    )

    with pytest.raises(MigrationChecksumConflict):
        _runner(ledger, FakeExecutor(), changed).run()


def test_missing_migration_directory_cannot_succeed_silently(tmp_path: Path) -> None:
    runner = MigrationRunner(
        source=FilesystemMigrationSource(
            tmp_path / "missing",
            engine="postgresql",
        ),
        ledger=FakeLedger(),
        executor=FakeExecutor(),
        engine="postgresql",
        release_version="1.0.0",
    )

    with pytest.raises(ValueError, match="No postgresql migrations"):
        runner.run()


def test_core_migration_directories_are_utf8_and_sequential() -> None:
    root = Path(__file__).resolve().parents[1] / "sql"
    expected = {
        "postgresql": (
            root / "postgres" / "migrations",
            (100, 101, 102, 103),
        ),
        "clickhouse": (
            root / "clickhouse" / "migrations",
            (100, 101, 102, 103, 104),
        ),
    }
    for engine, (directory, versions) in expected.items():
        migrations = FilesystemMigrationSource(
            directory,
            engine=engine,
        ).load()
        assert tuple(item.version for item in migrations) == versions
        assert all(len(item.checksum_sha256) == 32 for item in migrations)


def test_clickhouse_splitter_preserves_semicolons_in_literals() -> None:
    sql = "SELECT 'a;b'; -- comment; stays\nSELECT 2;"
    assert split_sql_statements(sql) == (
        "SELECT 'a;b'",
        "-- comment; stays\nSELECT 2",
    )


def test_reliable_processing_migration_contains_inbox_and_outbox() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0103_add_reliable_processing.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS processed_events" in sql
    assert "UNIQUE (topic, partition_id, offset_id)" in sql
    assert "CREATE TABLE IF NOT EXISTS delivery_outbox" in sql
    assert "REFERENCES market_signals(signal_id) ON DELETE CASCADE" in sql
    assert "WHERE status = 'delivering'" in sql
