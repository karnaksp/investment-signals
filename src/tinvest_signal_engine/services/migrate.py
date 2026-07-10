"""Composition root for core PostgreSQL and ClickHouse migrations."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import httpx
import psycopg

from ..adapters.migrations import (
    ClickHouseMigrationExecutor,
    FilesystemMigrationSource,
    PostgresMigrationExecutor,
    PostgresMigrationLedger,
)
from ..application.migrations import MigrationRunner
from ..config import RuntimeSettings
from ..logging_utils import configure_logging


logger = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply core database migrations")
    parser.add_argument(
        "--engine",
        choices=("all", "postgresql", "clickhouse"),
        default="all",
    )
    parser.add_argument("--release-version", required=True)
    parser.add_argument(
        "--migrations-root",
        type=Path,
        default=_PROJECT_ROOT / "sql",
    )
    return parser


def _run(
    *,
    settings: RuntimeSettings,
    engine: str,
    release_version: str,
    migrations_root: Path,
) -> None:
    connection = psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_database,
        user=settings.postgres_username,
        password=settings.postgres_password,
    )
    try:
        ledger = PostgresMigrationLedger(connection)
        engines = (
            ("postgresql", "postgres")
            if engine == "postgresql"
            else ("clickhouse", "clickhouse")
            if engine == "clickhouse"
            else None
        )
        selected = (engines,) if engines is not None else (
            ("postgresql", "postgres"),
            ("clickhouse", "clickhouse"),
        )
        for engine_name, directory_name in selected:
            source = FilesystemMigrationSource(
                migrations_root / directory_name / "migrations",
                engine=engine_name,
            )
            if engine_name == "postgresql":
                executor = PostgresMigrationExecutor(connection)
                result = MigrationRunner(
                    source=source,
                    ledger=ledger,
                    executor=executor,
                    engine=engine_name,
                    release_version=release_version,
                ).run()
            else:
                if not settings.clickhouse_http_url:
                    raise RuntimeError(
                        "CLICKHOUSE_HTTP_URL is required for ClickHouse migrations"
                    )
                auth = None
                if settings.clickhouse_http_username:
                    auth = (
                        settings.clickhouse_http_username,
                        settings.clickhouse_http_password or "",
                    )
                with httpx.Client(
                    base_url=settings.clickhouse_http_url,
                    auth=auth,
                    timeout=60.0,
                ) as client:
                    result = MigrationRunner(
                        source=source,
                        ledger=ledger,
                        executor=ClickHouseMigrationExecutor(client),
                        engine=engine_name,
                        release_version=release_version,
                    ).run()
            logger.info(
                "%s migrations applied=%s skipped=%s",
                engine_name,
                result.applied_versions,
                result.skipped_versions,
            )
    finally:
        connection.close()


def main() -> None:
    args = _parser().parse_args()
    settings = RuntimeSettings.from_env(service_name="migration")
    configure_logging(settings.log_level)
    _run(
        settings=settings,
        engine=args.engine,
        release_version=args.release_version,
        migrations_root=args.migrations_root,
    )


if __name__ == "__main__":
    main()
