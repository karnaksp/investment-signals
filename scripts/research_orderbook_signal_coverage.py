#!/usr/bin/env python3
"""Report how much of the replayed signal set has prior order-book coverage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    ResearchCandle,
    ResearchOrderBookSnapshot,
    orderbook_signal_coverage_by_ticker_day,
    orderbook_signal_coverage_summary,
    read_cache,
    read_orderbook_cache,
    write_csv_records,
    write_json,
)


def filter_candles_to_orderbook_dates(
    candles: Sequence[ResearchCandle],
    snapshots: Sequence[ResearchOrderBookSnapshot],
) -> tuple[ResearchCandle, ...]:
    covered_ticker_dates = {(snapshot.ticker, snapshot.at.date()) for snapshot in snapshots}
    return tuple(candle for candle in candles if (candle.ticker, candle.at.date()) in covered_ticker_dates)


def _parse_int_list(raw: str) -> tuple[int, ...]:
    result = tuple(sorted({int(item.strip()) for item in raw.split(",") if item.strip()}))
    if not result or any(item <= 0 for item in result):
        raise ValueError("expected a comma-separated list of positive integers")
    return result


def _parse_tickers(raw: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def _csv_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["covered_by_type"] = json.dumps(item.get("covered_by_type", {}), sort_keys=True)
        result.append(item)
    return result


def _write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    lines = [
        "# Order-book coverage for replayed signals",
        "",
        "| Max age, seconds | Signals | Covered | Coverage | Sessions | Covered sessions | Snapshots | Nearest prior age, seconds | Nearest absolute gap, seconds |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {age} | {signals} | {covered} | {coverage:.4f} | {sessions} | {covered_sessions} | {snapshots} | {prior_age} | {abs_gap} |".format(
                age=row["max_age_seconds"],
                signals=row["signals"],
                covered=row["covered_signals"],
                coverage=float(row["coverage"]),
                sessions=row["sessions"],
                covered_sessions=row["covered_sessions"],
                snapshots=row["orderbook_snapshots"],
                prior_age=row.get("nearest_prior_orderbook_age_seconds", ""),
                abs_gap=row.get("nearest_signal_orderbook_gap_seconds", ""),
            )
        )
    if rows:
        first = rows[0]
        lines.extend(
            [
                "",
                "## Time range diagnostics",
                "",
                f"- First signal: {first.get('first_signal_at', '')}",
                f"- Last signal: {first.get('last_signal_at', '')}",
                f"- First order-book snapshot: {first.get('first_orderbook_at', '')}",
                f"- Last order-book snapshot: {first.get('last_orderbook_at', '')}",
                "",
            ]
        )
    lines.extend(
        [
            "",
            "Use this report before training liquidity-aware models. A strong research run needs dense "
            "coverage across many sessions; sparse snapshots are useful only for smoke tests.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_by_day_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    missing_rows = [row for row in rows if int(row.get("covered_signals", 0) or 0) == 0]
    partial_rows = [
        row
        for row in rows
        if int(row.get("covered_signals", 0) or 0) > 0
        and int(row.get("covered_signals", 0) or 0) < int(row.get("signals", 0) or 0)
    ]
    lines = [
        "# Order-book coverage by ticker and day",
        "",
        f"- Ticker-days with signals: {len(rows)}",
        f"- Fully missing ticker-days: {len(missing_rows)}",
        f"- Partially covered ticker-days: {len(partial_rows)}",
        "",
        "## Worst ticker-days",
        "",
        "| Ticker | Day | Signals | Covered | Missing | Coverage | Snapshots | First signal | Last signal | First snapshot | Last snapshot |",
        "|---|---|---:|---:|---:|---:|---:|---|---|---|---|",
    ]
    worst = sorted(
        rows,
        key=lambda row: (
            float(row.get("coverage", 0.0) or 0.0),
            -int(row.get("signals", 0) or 0),
            str(row.get("ticker", "")),
            str(row.get("trading_day", "")),
        ),
    )
    for row in worst[:50]:
        lines.append(
            "| {ticker} | {day} | {signals} | {covered} | {missing} | {coverage:.4f} | {snapshots} | {first_signal} | {last_signal} | {first_snapshot} | {last_snapshot} |".format(
                ticker=row.get("ticker", ""),
                day=row.get("trading_day", ""),
                signals=row.get("signals", 0),
                covered=row.get("covered_signals", 0),
                missing=row.get("missing_signals", 0),
                coverage=float(row.get("coverage", 0.0) or 0.0),
                snapshots=row.get("orderbook_snapshots", 0),
                first_signal=row.get("first_signal_at", ""),
                last_signal=row.get("last_signal_at", ""),
                first_snapshot=row.get("first_orderbook_at", ""),
                last_snapshot=row.get("last_orderbook_at", ""),
            )
        )
    lines.extend(
        [
            "",
            "Use this table to decide which ticker-days need dense order-book collection. "
            "A day with signals and zero covered rows cannot contribute to a liquidity-aware 90% claim.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-orderbook-signal-coverage")
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--tickers", type=_parse_tickers, help="Optional comma-separated ticker filter.")
    parser.add_argument("--max-age-seconds", default="5,15,30,60")
    parser.add_argument("--max-signals-per-instrument", type=int, default=10_000)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/orderbook_coverage"))
    parser.add_argument(
        "--only-orderbook-dates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Analyze only candle dates that have order-book snapshots for the same ticker.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        ages = _parse_int_list(args.max_age_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    candles = read_cache(args.cache_dir, tickers=args.tickers)
    snapshots = read_orderbook_cache(args.orderbook_cache_dir, tickers=args.tickers)
    if args.only_orderbook_dates:
        candles = filter_candles_to_orderbook_dates(candles, snapshots)
    rows = orderbook_signal_coverage_summary(
        candles,
        snapshots,
        max_age_seconds_options=ages,
        max_signals_per_instrument=args.max_signals_per_instrument,
    )
    by_day_rows = orderbook_signal_coverage_by_ticker_day(
        candles,
        snapshots,
        max_age_seconds=max(ages),
        max_signals_per_instrument=args.max_signals_per_instrument,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.output_dir / "coverage.json", {"rows": rows, "by_ticker_day": by_day_rows})
    write_csv_records(args.output_dir / "coverage.csv", _csv_rows(rows))
    write_csv_records(args.output_dir / "coverage-by-day.csv", by_day_rows)
    _write_report(args.output_dir / "coverage-report.md", rows)
    _write_by_day_report(args.output_dir / "coverage-by-day-report.md", by_day_rows)
    print(
        json.dumps(
            {
                "status": "ok",
                "output_dir": str(args.output_dir),
                "signals": rows[0]["signals"] if rows else 0,
                "orderbook_snapshots": rows[0]["orderbook_snapshots"] if rows else 0,
                "max_coverage": max((float(row["coverage"]) for row in rows), default=0.0),
                "ticker_days": len(by_day_rows),
                "missing_ticker_days": sum(1 for row in by_day_rows if int(row.get("covered_signals", 0) or 0) == 0),
                "only_orderbook_dates": args.only_orderbook_dates,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
