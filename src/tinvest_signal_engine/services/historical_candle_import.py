"""Composition root for the one-time local historical candle import."""

from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys
from typing import Sequence

from tinvest_signal_engine.adapters.clickhouse_historical_candle_import import (
    ClickHouseHistoricalCandleImportDestination,
)
from tinvest_signal_engine.adapters.file_historical_candle_import import (
    AtomicFileHistoricalCandleImportProgress,
    result_payload,
)
from tinvest_signal_engine.adapters.parquet_historical_candle_import import (
    ParquetHistoricalCandleImportSource,
)
from tinvest_signal_engine.application.historical_candle_import import (
    ImportHistoricalScientificCandles,
)
from tinvest_signal_engine.config import load_instrument_configs, load_secret


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="tinvest-import-scientific-history")
    subcommands = parser.add_subparsers(dest="operation", required=True)
    run = subcommands.add_parser("run")
    run.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    run.add_argument(
        "--state-dir",
        type=Path,
        default=Path("var/research/imports/scientific-candles-v1"),
    )
    run.add_argument(
        "--instruments-config",
        type=Path,
        default=Path(os.getenv("INSTRUMENTS_CONFIG", "conf/instruments.yaml")),
    )
    run.add_argument("--batch-size", type=int, default=50_000)
    run.add_argument("--partition-group-size", type=int, default=50)
    run.add_argument("--clickhouse-timeout", type=float, default=30.0)
    run.add_argument("--tickers", default="")
    run.add_argument("--start-day", type=date.fromisoformat)
    run.add_argument("--end-day", type=date.fromisoformat)
    run.add_argument("--max-partitions", type=int)
    run.add_argument(
        "--manifest-only",
        action="store_true",
        help="import only partitions sealed by the current cache manifest",
    )
    run.add_argument("--dry-run", action="store_true")
    status = subcommands.add_parser("status")
    status.add_argument(
        "--state-dir",
        type=Path,
        default=Path("var/research/imports/scientific-candles-v1"),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    progress = AtomicFileHistoricalCandleImportProgress(args.state_dir)
    if args.operation == "status":
        _print(progress.status_payload())
        return 0
    if (args.start_day is None) != (args.end_day is None):
        raise SystemExit("--start-day and --end-day must be provided together")
    if args.start_day is not None and args.end_day < args.start_day:
        raise SystemExit("--end-day must not precede --start-day")
    try:
        if args.dry_run:
            # A dry run is intentionally read-only, including the state directory.
            # It therefore does not create the process lock used by mutating runs.
            return _run(args, progress)
        with progress.exclusive_run():
            return _run(args, progress)
    except Exception as error:
        _print(
            {
                "schema_version": 1,
                "operation": "run",
                "status": "failed",
                "reason_code": "historical_candle_import_failed",
                "error_type": type(error).__name__,
            }
        )
        print(
            f"historical candle import failed: {type(error).__name__}",
            file=sys.stderr,
        )
        return 2


def _run(
    args: argparse.Namespace,
    progress: AtomicFileHistoricalCandleImportProgress,
) -> int:
    source = ParquetHistoricalCandleImportSource(
        args.cache_dir,
        tickers=tuple(item for item in args.tickers.split(",") if item.strip()),
        start_day=args.start_day.isoformat() if args.start_day else None,
        end_day=args.end_day.isoformat() if args.end_day else None,
        max_partitions=args.max_partitions,
        manifest_only=args.manifest_only,
    )
    try:
        instruments = {
            item.ticker: item.instrument_id
            for item in load_instrument_configs(args.instruments_config)
            if item.class_code == "TQBR"
        }
        password = load_secret(
            "CLICKHOUSE_PASSWORD",
            service_name="historical_candle_import",
        )
        if password is None:
            raise ValueError(
                "CLICKHOUSE_PASSWORD or CLICKHOUSE_PASSWORD_FILE is required"
            )
        result = ImportHistoricalScientificCandles(
            source=source,
            destination=ClickHouseHistoricalCandleImportDestination(
                base_url=_required_env("CLICKHOUSE_HTTP_URL"),
                database=(os.getenv("CLICKHOUSE_DATABASE") or "signal_engine").strip(),
                username=_required_env("CLICKHOUSE_USERNAME"),
                password=password,
                timeout_seconds=args.clickhouse_timeout,
            ),
            progress=progress,
            instrument_ids=instruments,
            batch_size=args.batch_size,
            partition_group_size=args.partition_group_size,
        ).execute(dry_run=args.dry_run)
    finally:
        source.close()
    _print(result_payload(result))
    return 0


def _required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _print(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main())
