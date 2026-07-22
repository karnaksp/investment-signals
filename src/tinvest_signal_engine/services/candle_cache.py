"""Composition root for the reusable T-Invest candle cache command."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
import json
import os
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.adapters.candle_cache import (
    JsonCandleCacheManifest,
    ParquetCandlePartitionRepository,
    TInvestRestCandleHistorySource,
)
from tinvest_signal_engine.application.candle_cache import BuildReusableCandleCache
from tinvest_signal_engine.config import load_secret
from tinvest_signal_engine.domain.candle_cache import CandleCacheScope


DEFAULT_TICKERS = (
    "SBER",
    "GAZP",
    "LKOH",
    "YDEX",
    "T",
    "VTBR",
    "ROSN",
    "NVTK",
    "GMKN",
    "PLZL",
    "MOEX",
    "MGNT",
    "TRNFP",
    "CHMF",
    "NLMK",
    "TATN",
    "AFKS",
    "ALRS",
    "OZON",
    "PIKK",
    "MTSS",
    "POSI",
    "IRAO",
    "PHOR",
    "RUAL",
)
_MOSCOW = ZoneInfo("Europe/Moscow")


def _trusted_ca_from_environment() -> Path | None:
    value = os.environ.get("TINVEST_TRUSTED_CA_FILE", "").strip()
    return Path(value).expanduser() if value else None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="tinvest-cache-candles")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    parser.add_argument("--tickers", default=",".join(DEFAULT_TICKERS))
    parser.add_argument("--calendar-days", type=int, default=180)
    parser.add_argument("--start-day", type=date.fromisoformat)
    parser.add_argument("--end-day", type=date.fromisoformat)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=5)
    parser.add_argument("--request-interval", type=float, default=0.05)
    parser.add_argument(
        "--ca-cert",
        type=Path,
        default=_trusted_ca_from_environment(),
        help=(
            "trusted CA bundle (defaults to the public "
            "TINVEST_TRUSTED_CA_FILE environment setting)"
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ca_bundle_path = args.ca_cert.expanduser() if args.ca_cert is not None else None
    if ca_bundle_path is not None and not ca_bundle_path.is_file():
        raise SystemExit(f"Trusted CA bundle does not exist: {ca_bundle_path}")
    tickers = tuple(
        item.strip().upper() for item in args.tickers.split(",") if item.strip()
    )
    if args.start_day is not None or args.end_day is not None:
        if args.start_day is None or args.end_day is None:
            raise SystemExit("--start-day and --end-day must be provided together")
        start_day, end_day = args.start_day, args.end_day
    else:
        if args.calendar_days < 2:
            raise SystemExit("--calendar-days must be at least 2")
        end_day = datetime.now(_MOSCOW).date() - timedelta(days=1)
        start_day = end_day - timedelta(days=args.calendar_days - 1)
    if end_day >= datetime.now(_MOSCOW).date():
        raise SystemExit("--end-day must be a completed historical day")
    scope = CandleCacheScope(
        tickers=tickers,
        start_day=start_day,
        end_day=end_day,
    )
    token = load_secret("TINVEST_TOKEN", service_name="candle_cache") or ""
    source = TInvestRestCandleHistorySource(
        token=token,
        timeout_seconds=args.request_timeout,
        attempts=args.request_attempts,
        request_interval_seconds=args.request_interval,
        ca_bundle_path=ca_bundle_path,
    )
    repository = ParquetCandlePartitionRepository(args.cache_dir)
    try:
        receipt = BuildReusableCandleCache(
            source=source,
            repository=repository,
            manifest=JsonCandleCacheManifest(args.cache_dir),
        ).execute(scope)
    finally:
        repository.close()
        source.close()
    print(
        json.dumps(
            {
                "status": "partial" if receipt.failures else "ok",
                "cache_dir": str(args.cache_dir),
                "partitions": len(receipt.inventory.rows_by_partition),
                "skipped_existing_partitions": receipt.skipped_partitions,
                "written_partitions": receipt.written_partitions,
                "failed_partitions": len(receipt.failures),
                "content_fingerprint": receipt.inventory.dataset_fingerprint,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 2 if receipt.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
