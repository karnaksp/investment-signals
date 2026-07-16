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
    build_signal_price_dataset,
    read_cache,
    write_json,
    write_table,
)


def _parse_int_list(raw: str) -> tuple[int, ...]:
    result = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not result or any(item <= 0 for item in result):
        raise ValueError("expected a comma-separated list of positive integers")
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-build-signal-price-dataset")
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
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
    candles = read_cache(args.cache_dir)
    rows, manifest = build_signal_price_dataset(
        candles,
        horizons_seconds=horizons,
        lookback_windows=lookbacks,
        max_signals_per_instrument=args.max_signals_per_instrument,
    )
    write_table(args.output, rows)
    manifest_output = args.manifest_output or args.output.with_suffix(args.output.suffix + ".manifest.json")
    manifest["output"] = str(args.output)
    manifest["cache_dir"] = str(args.cache_dir)
    write_json(manifest_output, manifest)
    print(
        json.dumps(
            {
                "status": "ok",
                "rows": manifest["quality"]["rows"],
                "signals": manifest["quality"]["signals"],
                "feature_leakage_rows": manifest["quality"]["feature_leakage_rows"],
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
