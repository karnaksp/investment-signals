#!/usr/bin/env python3
"""Build a signal-triggered price prediction dataset from local candle cache."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    ResearchCandle,
    ResearchOrderBookSnapshot,
    build_signal_price_dataset,
    read_cache,
    read_native_signal_cache,
    read_orderbook_cache,
    write_json,
    write_table,
)


def _parse_int_list(raw: str) -> tuple[int, ...]:
    result = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not result or any(item <= 0 for item in result):
        raise ValueError("expected a comma-separated list of positive integers")
    return result


def _parse_tickers(raw: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def filter_candles_to_orderbook_dates(
    candles: Sequence[ResearchCandle],
    snapshots: Sequence[ResearchOrderBookSnapshot],
) -> tuple[ResearchCandle, ...]:
    covered_ticker_dates = {(snapshot.ticker, snapshot.at.date()) for snapshot in snapshots}
    return tuple(candle for candle in candles if (candle.ticker, candle.at.date()) in covered_ticker_dates)


def validate_orderbook_feature_requirement(
    *,
    require_orderbook_features: bool,
    orderbook_cache_dir: Path | None,
    manifest: dict[str, object],
) -> None:
    if not require_orderbook_features:
        return
    if not orderbook_cache_dir:
        raise SystemExit("--require-orderbook-features requires --orderbook-cache-dir")
    quality = manifest.get("quality")
    if not isinstance(quality, dict):
        raise SystemExit("dataset manifest has no quality section")
    orderbook_feature_rows = int(quality.get("orderbook_feature_rows") or 0)
    if orderbook_feature_rows <= 0:
        raise SystemExit(
            "No prior order-book feature rows were produced. "
            "Collect continuous order-book snapshots before training a liquidity-aware model."
        )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-build-signal-price-dataset")
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--tickers", type=_parse_tickers, help="Optional comma-separated ticker filter.")
    parser.add_argument(
        "--horizons",
        default="60,300,900,1800",
        help="Forward horizons in seconds, comma-separated.",
    )
    parser.add_argument(
        "--lookback-windows",
        default="5,15,30,60",
        help="Pre-signal windows in minutes, comma-separated.",
    )
    parser.add_argument(
        "--max-signals-per-instrument",
        type=int,
        default=10_000,
    )
    parser.add_argument(
        "--orderbook-cache-dir",
        type=Path,
        help="Optional local order-book snapshot cache from research_collect_tinvest_orderbook_snapshots.py.",
    )
    parser.add_argument(
        "--native-signal-cache-dir",
        type=Path,
        help="Optional local cache produced by research_cache_tinvest_native_signals.py.",
    )
    parser.add_argument(
        "--market-context-cache-dir",
        type=Path,
        help="Optional local IMOEX/RVI/oil/gold/yuan candle cache.",
    )
    parser.add_argument(
        "--orderbook-max-age-seconds",
        type=int,
        default=30,
        help="Use only order-book snapshots at or before the signal and no older than this many seconds.",
    )
    parser.add_argument(
        "--only-orderbook-dates",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="When order-book cache is provided, build only ticker/date partitions that have snapshots.",
    )
    parser.add_argument(
        "--require-orderbook-features",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail if an order-book cache is provided but no signal row receives prior order-book features.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("var/research/datasets/signal_price_prediction.parquet"),
    )
    parser.add_argument(
        "--manifest-output",
        type=Path,
        help="Defaults to <output>.manifest.json.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        horizons = _parse_int_list(args.horizons)
        lookbacks = _parse_int_list(args.lookback_windows)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    candles = read_cache(args.cache_dir, tickers=args.tickers)
    orderbook_snapshots = (
        read_orderbook_cache(args.orderbook_cache_dir, tickers=args.tickers)
        if args.orderbook_cache_dir
        else ()
    )
    native_signal_rows = (
        read_native_signal_cache(args.native_signal_cache_dir, tickers=args.tickers)
        if args.native_signal_cache_dir
        else ()
    )
    market_context_candles = (
        read_cache(args.market_context_cache_dir)
        if args.market_context_cache_dir
        else ()
    )
    if args.only_orderbook_dates:
        if not orderbook_snapshots:
            raise SystemExit("--only-orderbook-dates requires --orderbook-cache-dir with matching snapshots")
        candles = filter_candles_to_orderbook_dates(candles, orderbook_snapshots)
    rows, manifest = build_signal_price_dataset(
        candles,
        horizons_seconds=horizons,
        lookback_windows=lookbacks,
        max_signals_per_instrument=args.max_signals_per_instrument,
        orderbook_snapshots=orderbook_snapshots,
        orderbook_max_age_seconds=args.orderbook_max_age_seconds,
        native_signal_rows=native_signal_rows,
        market_context_candles=market_context_candles,
    )
    validate_orderbook_feature_requirement(
        require_orderbook_features=args.require_orderbook_features,
        orderbook_cache_dir=args.orderbook_cache_dir,
        manifest=manifest,
    )
    write_table(args.output, rows)
    manifest_output = args.manifest_output or args.output.with_suffix(args.output.suffix + ".manifest.json")
    manifest["output"] = str(args.output)
    manifest["cache_dir"] = str(args.cache_dir)
    if args.tickers:
        manifest["tickers"] = list(args.tickers)
    if args.orderbook_cache_dir:
        manifest["orderbook_cache_dir"] = str(args.orderbook_cache_dir)
    if args.native_signal_cache_dir:
        manifest["native_signal_cache_dir"] = str(args.native_signal_cache_dir)
    if args.market_context_cache_dir:
        manifest["market_context_cache_dir"] = str(args.market_context_cache_dir)
    write_json(manifest_output, manifest)
    print(
        json.dumps(
            {
                "status": "ok",
                "rows": manifest["quality"]["rows"],
                "signals": manifest["quality"]["signals"],
                "feature_leakage_rows": manifest["quality"]["feature_leakage_rows"],
                "orderbook_feature_rows": manifest["quality"].get("orderbook_feature_rows", 0),
                "unavailable_rows": manifest["quality"]["unavailable_rows"],
                "output": str(args.output),
                "manifest": str(manifest_output),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
