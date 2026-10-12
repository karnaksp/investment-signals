"""Migration orchestration independent from database drivers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.migrations import (
    AppliedMigration,
    Migration,
    MigrationChecksumConflict,
)


class MigrationSource(Protocol):
    def load(self) -> Sequence[Migration]: ...


class MigrationLedger(Protocol):
    def prepare(self) -> None: ...

    def list_applied(self, engine: str) -> Sequence[AppliedMigration]: ...

    def record(
        self,
        migration: Migration,
        *,
        release_version: str,
        execution_ms: int,
    ) -> None: ...


class MigrationExecutor(Protocol):
    def execute(self, migration: Migration) -> int: ...


@dataclass(frozen=True)
class MigrationResult:
    applied_versions: tuple[int, ...]
    skipped_versions: tuple[int, ...]


class MigrationRunner:
    def __init__(
        self,
        *,
        source: MigrationSource,
        ledger: MigrationLedger,
        executor: MigrationExecutor,
        engine: str,
        release_version: str,
    ) -> None:
        self._source = source
        self._ledger = ledger
        self._executor = executor
        self._engine = engine
        self._release_version = release_version

    def run(self) -> MigrationResult:
        self._ledger.prepare()
        migrations = tuple(self._source.load())
        if not migrations:
            raise ValueError(f"No {self._engine} migrations were found")
        self._validate_source(migrations)
        applied = {
            item.version: item
            for item in self._ledger.list_applied(self._engine)
        }
        applied_now: list[int] = []
        skipped: list[int] = []

        for migration in migrations:
            previous = applied.get(migration.version)
            if previous is not None:
                if (
                    previous.name != migration.name
                    or previous.checksum_sha256 != migration.checksum_sha256
                ):
                    raise MigrationChecksumConflict(
                        f"{self._engine} migration {migration.version:04d} "
                        "differs from the applied immutable migration"
                    )
                skipped.append(migration.version)
                continue

            execution_ms = self._executor.execute(migration)
            self._ledger.record(
                migration,
                release_version=self._release_version,
                execution_ms=execution_ms,
            )
            applied_now.append(migration.version)

        return MigrationResult(tuple(applied_now), tuple(skipped))

    def _validate_source(self, migrations: Sequence[Migration]) -> None:
        versions = [item.version for item in migrations]
        if any(item.engine != self._engine for item in migrations):
            raise ValueError("Migration source contains a different engine")
        if versions != sorted(versions) or len(versions) != len(set(versions)):
            raise ValueError("Migration versions must be unique and sorted")
        if versions and versions != list(range(versions[0], versions[-1] + 1)):
            raise ValueError("Migration versions must form a contiguous sequence")
