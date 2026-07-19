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
            (100, 101, 102, 103, 104, 105, 106, 107, 108),
        ),
        "clickhouse": (
            root / "clickhouse" / "migrations",
            (100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110),
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


def test_detector_observation_outbox_is_durable_and_payload_immutable() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0104_add_detector_observation_outbox.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS detector_observation_outbox" in sql
    assert "REFERENCES processed_events(event_id) ON DELETE RESTRICT" in sql
    assert "detector_observation_outbox_ready_idx" in sql
    assert "detector_observation_outbox_reclaim_idx" in sql
    assert "NEW.payload_json IS DISTINCT FROM OLD.payload_json" in sql
    assert "detector_observation_outbox_state_guard" in sql
    assert "detector_observation_outbox_delete_guard" in sql
    assert "past the safety window may be purged" in sql


def test_detector_state_snapshot_is_versioned_and_broker_monotonic() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0105_add_detector_state_snapshots.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE detector_state_snapshots" in sql
    assert "REFERENCES processed_events(event_id) ON DELETE RESTRICT" in sql
    assert "snapshot_sha256 BYTEA NOT NULL" in sql
    assert "NEW.offset_id <= OLD.offset_id" in sql
    assert "detector_state_snapshots_advance_guard" in sql
    assert "detector_state_snapshots_delete_guard" in sql


def test_detector_config_acknowledgement_migration_is_runtime_proof() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0106_add_detector_config_acknowledgements.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE detector_config_acknowledgements" in sql
    assert "detector_instance_id TEXT NOT NULL" in sql
    assert "detector_config_version TEXT NOT NULL" in sql
    assert "status TEXT NOT NULL CHECK (status IN ('loaded', 'failed'))" in sql
    assert "failure_reason_code TEXT" in sql
    assert "configured_instruments_count INTEGER NOT NULL DEFAULT 0" in sql
    assert "detector_config_acknowledgements_latest_idx" in sql


def test_signal_outcomes_migration_persists_automatic_verdicts() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0107_add_signal_outcomes.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS signal_outcomes" in sql
    assert "REFERENCES market_signals(signal_id) ON DELETE CASCADE" in sql
    assert "'confirmed', 'contradicted', 'insignificant', 'inconclusive'" in sql
    assert "expected_direction SMALLINT NOT NULL CHECK (expected_direction IN (-1, 1))" in sql
    assert "cost_model_version TEXT NOT NULL" in sql
    assert "policy_version TEXT NOT NULL" in sql
    assert "inverse_hypothesis_candidate BOOLEAN NOT NULL DEFAULT false" in sql
    assert "UNIQUE (signal_id, horizon_seconds, policy_version, cost_model_version)" in sql
    assert "signal_outcomes_inverse_candidate_idx" in sql


def test_core_outcomes_are_renamed_before_product_migrations() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "postgres"
        / "migrations"
        / "0108_rename_core_signal_outcomes.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "RENAME TO core_directional_signal_outcomes" in sql
    assert "core_directional_signal_outcomes_inverse_candidate_idx" in sql


def test_clickhouse_reference_ticks_migration_supports_outcome_evaluation() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "clickhouse"
        / "migrations"
        / "0105_create_market_reference_ticks.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS signal_engine.market_reference_ticks" in sql
    assert "event_at DateTime64(9, 'UTC')" in sql
    assert "event_id UUID" in sql
    assert "has_valid_book UInt8" in sql
    assert "ORDER BY (instrument_id, toDate(event_at), event_at, event_id)" in sql
    assert "TTL toDateTime(event_at) + toIntervalDay(35)" in sql


def test_scientific_candle_migration_supports_backfill_and_live_reconciliation() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "clickhouse"
        / "migrations"
        / "0108_create_scientific_candles_1m.up.sql"
    )
    sql = path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS signal_engine.scientific_candles_1m" in sql
    assert "source_kind Enum8('backfill' = 1, 'stream' = 2)" in sql
    assert "ENGINE = ReplacingMergeTree(record_version)" in sql
    assert "ORDER BY (instrument_id, trading_day, candle_at)" in sql
    assert "TTL toDateTime(candle_at) + toIntervalDay(365)" in sql


def test_scientific_observation_migrations_preserve_decisions_and_outcomes() -> None:
    root = Path(__file__).resolve().parents[1] / "sql" / "clickhouse" / "migrations"
    observations = (
        root / "0109_create_scientific_hypothesis_observations.up.sql"
    ).read_text(encoding="utf-8")
    outcomes = (
        root / "0110_create_scientific_hypothesis_outcomes.up.sql"
    ).read_text(encoding="utf-8")

    assert "'matched' = 1, 'not_matched' = 2, 'abstain' = 3" in observations
    assert "feature_max_observed_at DateTime64(6, 'UTC')" in observations
    assert "effect_unit LowCardinality(String)" in observations
    assert "claim_scope LowCardinality(String)" in observations
    assert "TTL toDateTime(observed_at) + toIntervalDay(365)" in observations
    assert "observation_id String" in observations
    assert "scientific_source_ids Array(String)" in observations
    assert "source_kind Enum8('stream' = 1, 'historical_backfill' = 2)" in observations
    assert "outcome_id String" in outcomes
    assert "available UInt8" in outcomes
    assert "outcome_policy_version LowCardinality(String)" in outcomes
    assert "source_max_observed_at DateTime64(6, 'UTC')" in outcomes
    assert "payload_fingerprint String" in outcomes
    assert "TTL toDateTime(target_at) + toIntervalDay(365)" in outcomes
