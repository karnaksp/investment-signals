"""Filesystem and database adapters for versioned migrations."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Sequence

from tinvest_signal_engine.domain.migrations import AppliedMigration, Migration


_MIGRATION_FILENAME = re.compile(
    r"^(?P<version>\d{4})_(?P<name>[a-z0-9_]+)\.up\.sql$"
)


class FilesystemMigrationSource:
    def __init__(
        self,
        directory: Path,
        *,
        engine: str,
        minimum_version: int = 100,
        maximum_version: int = 199,
    ) -> None:
        self._directory = directory
        self._engine = engine
        self._minimum_version = minimum_version
        self._maximum_version = maximum_version

    def load(self) -> Sequence[Migration]:
        migrations: list[Migration] = []
        for path in sorted(self._directory.glob("*.up.sql")):
            match = _MIGRATION_FILENAME.fullmatch(path.name)
            if match is None:
                raise ValueError(f"Invalid migration filename: {path.name}")
            version = int(match.group("version"))
            if not self._minimum_version <= version <= self._maximum_version:
                raise ValueError(
                    f"Migration {path.name} is outside the allowed range "
                    f"{self._minimum_version:04d}-{self._maximum_version:04d}"
                )
            migrations.append(
                Migration(
                    engine=self._engine,
                    version=version,
                    name=match.group("name"),
                    sql=path.read_text(encoding="utf-8"),
                )
            )
        return tuple(migrations)


class PostgresMigrationLedger:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def prepare(self) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    engine TEXT NOT NULL
                        CHECK (engine IN ('postgresql', 'clickhouse')),
                    version INTEGER NOT NULL
                        CHECK (version BETWEEN 100 AND 399),
                    name TEXT NOT NULL,
                    checksum_sha256 BYTEA NOT NULL
                        CHECK (octet_length(checksum_sha256) = 32),
                    release_version TEXT NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    execution_ms BIGINT NOT NULL CHECK (execution_ms >= 0),
                    PRIMARY KEY (engine, version)
                )
                """
            )
        self._connection.commit()

    def list_applied(self, engine: str) -> Sequence[AppliedMigration]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT engine, version, name, checksum_sha256
                FROM schema_migrations
                WHERE engine = %s
                ORDER BY version
                """,
                (engine,),
            )
            rows = cursor.fetchall()
        return tuple(
            AppliedMigration(
                engine=str(row[0]),
                version=int(row[1]),
                name=str(row[2]),
                checksum_sha256=bytes(row[3]),
            )
            for row in rows
        )

    def record(
        self,
        migration: Migration,
        *,
        release_version: str,
        execution_ms: int,
    ) -> None:
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO schema_migrations (
                        engine, version, name, checksum_sha256,
                        release_version, execution_ms
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        migration.engine,
                        migration.version,
                        migration.name,
                        migration.checksum_sha256,
                        release_version,
                        execution_ms,
                    ),
                )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise


class PostgresMigrationExecutor:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def execute(self, migration: Migration) -> int:
        started = time.monotonic()
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(migration.sql)
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return max(0, round((time.monotonic() - started) * 1000))


class ClickHouseMigrationExecutor:
    def __init__(self, client: Any, *, endpoint: str = "/") -> None:
        self._client = client
        self._endpoint = endpoint

    def execute(self, migration: Migration) -> int:
        started = time.monotonic()
        for statement in split_sql_statements(migration.sql):
            response = self._client.post(
                self._endpoint,
                content=statement.encode("utf-8"),
            )
            response.raise_for_status()
        return max(0, round((time.monotonic() - started) * 1000))


def split_sql_statements(sql: str) -> tuple[str, ...]:
    """Split migration SQL while preserving semicolons in strings/comments."""
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    in_line_comment = False
    in_block_comment = False
    index = 0
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""
        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue
        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                index += 2
                in_block_comment = False
            else:
                index += 1
            continue
        if quote is not None:
            current.append(char)
            if char == quote:
                if next_char == quote:
                    current.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char == "-" and next_char == "-":
            current.extend((char, next_char))
            in_line_comment = True
            index += 2
            continue
        if char == "/" and next_char == "*":
            current.extend((char, next_char))
            in_block_comment = True
            index += 2
            continue
        if char in {"'", '"'}:
            quote = char
            current.append(char)
            index += 1
            continue
        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return tuple(statements)
