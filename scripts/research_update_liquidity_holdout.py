#!/usr/bin/env python3
"""Update the forward liquidity holdout used for high-confidence signal research.

The candle-only research track cannot prove a reliable 90% selected-signal
claim. This command wires the forward-data workflow into one repeatable step:

1. optionally collect fresh T-Invest order-book snapshots;
2. synchronize one-minute candles for the same dates, refreshing the latest
   order-book date so intraday partitions do not stay stale;
3. run coverage/readiness checks and train only when the holdout is ready.

Artifacts stay local and do not persist broker tokens, account identifiers,
FIGIs, or instrument UIDs.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_plan_liquidity_collection import (  # noqa: E402
    build_collection_plan,
    collection_window_preflight,
    write_report as write_collection_plan_report,
    write_schedule_files,
)
from research_price_prediction_lib import DEFAULT_RESEARCH_TICKERS, read_orderbook_cache, write_json  # noqa: E402


def _parse_tickers(raw: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def _parse_dates(raw: str) -> frozenset[date]:
    return frozenset(date.fromisoformat(item.strip()) for item in raw.split(",") if item.strip())


def _run_json_command(command: Sequence[str]) -> dict[str, Any]:
    completed = subprocess.run(  # noqa: S603 - command is built from fixed script paths and CLI args
        list(command),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Command failed with exit code {code}: {command}\n{stderr}".format(
                code=completed.returncode,
                command=" ".join(command),
                stderr=completed.stderr[-2_000:],
            )
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1]) if lines else {}


def orderbook_ticker_dates(cache_dir: Path, tickers: Sequence[str]) -> dict[str, tuple[date, ...]]:
    allowed = set(tickers)
    by_ticker: dict[str, set[date]] = defaultdict(set)
    snapshots = read_orderbook_cache(cache_dir, tickers=tickers)
    for snapshot in snapshots:
        if snapshot.ticker in allowed:
            by_ticker[snapshot.ticker].add(snapshot.at.date())
    return {ticker: tuple(sorted(days)) for ticker, days in sorted(by_ticker.items())}


def _date_span(ticker_dates: Mapping[str, Sequence[date]]) -> tuple[date, date] | None:
    days = sorted({day for dates in ticker_dates.values() for day in dates})
    if not days:
        return None
    return days[0], days[-1]


def _collect_orderbook(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_collect_tinvest_orderbook_snapshots.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.orderbook_cache_dir),
        "--tickers",
        ",".join(args.tickers),
        "--depth",
        str(args.orderbook_depth),
        "--samples",
        str(args.orderbook_samples),
        "--interval-seconds",
        str(args.orderbook_interval_seconds),
        "--request-timeout",
        str(args.request_timeout),
        "--request-attempts",
        str(args.request_attempts),
        "--flush-every-samples",
        str(args.orderbook_flush_every_samples),
    ]
    if args.ca_cert:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    return _run_json_command(command)


def _collect_signal_triggered_orderbook(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_collect_signal_triggered_orderbooks.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.orderbook_cache_dir),
        "--state-file",
        str(args.signal_triggered_state_file),
        "--tickers",
        ",".join(args.tickers),
        "--depth",
        str(args.orderbook_depth),
        "--polls",
        str(args.signal_triggered_polls),
        "--interval-seconds",
        str(args.signal_triggered_interval_seconds),
        "--max-signal-age-seconds",
        str(args.signal_triggered_max_signal_age_seconds),
        "--request-timeout",
        str(args.request_timeout),
        "--request-attempts",
        str(args.request_attempts),
    ]
    if args.signal_triggered_target_day:
        command.extend(["--target-day", args.signal_triggered_target_day.isoformat()])
    if args.ca_cert:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    return _run_json_command(command)


def _sync_candles(
    args: argparse.Namespace,
    *,
    ticker_dates: Mapping[str, Sequence[date]],
) -> dict[str, Any]:
    span = _date_span(ticker_dates)
    if span is None:
        return {"status": "skipped", "reason_code": "no_orderbook_dates"}
    start_day, end_day = span
    refresh_days = set(args.refresh_candle_days)
    if args.refresh_latest_orderbook_day:
        refresh_days.add(end_day)
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_cache_tinvest_candles.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.cache_dir),
        "--tickers",
        ",".join(args.tickers),
        "--start-day",
        start_day.isoformat(),
        "--end-day",
        end_day.isoformat(),
        "--request-timeout",
        str(args.request_timeout),
        "--request-attempts",
        str(args.request_attempts),
        "--request-interval",
        str(args.request_interval),
        "--max-workers",
        str(args.max_workers),
    ]
    if refresh_days:
        command.extend(["--refresh-days", ",".join(day.isoformat() for day in sorted(refresh_days))])
    if args.ca_cert:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    result = _run_json_command(command)
    result["refreshed_dates"] = [day.isoformat() for day in sorted(refresh_days)]
    return result


def _run_holdout(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_run_liquidity_holdout.py"),
        "--cache-dir",
        str(args.cache_dir),
        "--orderbook-cache-dir",
        str(args.orderbook_cache_dir),
        "--tickers",
        ",".join(args.tickers),
        "--max-age-seconds",
        args.max_age_seconds,
        "--preferred-max-age-seconds",
        str(args.preferred_max_age_seconds),
        "--min-covered-signals",
        str(args.min_covered_signals),
        "--min-covered-sessions",
        str(args.min_covered_sessions),
        "--min-coverage",
        str(args.min_coverage),
        "--horizons",
        args.horizons,
        "--lookback-windows",
        args.lookback_windows,
        "--max-signals-per-instrument",
        str(args.max_signals_per_instrument),
        "--output-dir",
        str(args.output_dir),
    ]
    if args.force:
        command.append("--force")
    if args.only_orderbook_dates:
        command.append("--only-orderbook-dates")
    else:
        command.append("--no-only-orderbook-dates")
    return _run_json_command(command)


def _write_collection_plan(args: argparse.Namespace, holdout: Mapping[str, Any]) -> dict[str, Any]:
    readiness = holdout.get("readiness")
    if not isinstance(readiness, Mapping):
        return {"status": "skipped", "reason_code": "holdout_readiness_missing"}
    readiness_dir = readiness.get("output_dir")
    if readiness_dir in {None, ""}:
        return {"status": "skipped", "reason_code": "holdout_readiness_output_missing"}
    readiness_path = Path(str(readiness_dir)) / "readiness.json"
    if not readiness_path.exists():
        return {"status": "skipped", "reason_code": "holdout_readiness_file_missing", "readiness_json": str(readiness_path)}
    plan_dir = args.output_dir / "collection_plan"
    plan = build_collection_plan(
        readiness_path=readiness_path,
        tickers=args.tickers,
        preferred_max_age_seconds=args.preferred_max_age_seconds,
        target_calendar_days=args.collection_plan_target_calendar_days,
        target_continuous_hours=args.collection_plan_target_continuous_hours,
        orderbook_interval_seconds=int(args.orderbook_interval_seconds),
        orderbook_flush_every_samples=int(getattr(args, "orderbook_flush_every_samples", 20)),
        signal_triggered_interval_seconds=int(args.signal_triggered_interval_seconds),
        output_dir=args.output_dir,
        schedule_dir=plan_dir,
        ca_cert=getattr(args, "ca_cert", None),
        insecure_skip_tls_verify=bool(getattr(args, "insecure_skip_tls_verify", False)),
    )
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plan_dir / "collection-plan.json"
    report_path = plan_dir / "collection-plan.md"
    write_json(plan_path, plan)
    schedule_files = write_schedule_files(plan["schedule"])
    write_collection_plan_report(report_path, plan)
    return {
        "status": plan["status"],
        "output_dir": str(plan_dir),
        "plan": str(plan_path),
        "report": str(report_path),
        "schedule_files": schedule_files,
        "missing_covered_signals": plan["missing_covered_signals"],
        "missing_covered_sessions": plan["missing_covered_sessions"],
    }


def _collection_preflight(args: argparse.Namespace) -> dict[str, Any]:
    return collection_window_preflight(
        now=datetime.now(timezone.utc),
        target_hours=args.collection_plan_target_continuous_hours,
        interval_seconds=args.orderbook_interval_seconds,
    )


def run_update(args: argparse.Namespace) -> dict[str, Any]:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    preflight = _collection_preflight(args)
    if args.preflight_only or (
        args.require_full_prior_window
        and not bool(preflight.get("can_complete_full_window_today"))
        and not args.force
    ):
        payload = {
            "schema_version": 1,
            "kind": "liquidity_holdout_update",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "preflight_only" if args.preflight_only else "preflight_blocked",
            "reason_code": preflight.get("reason_code"),
            "collection_window_preflight": preflight,
            "output_dir": str(args.output_dir),
        }
        write_json(args.output_dir / "liquidity-update-result.json", payload)
        return payload
    collected = _collect_orderbook(args) if args.collect_orderbook else {"status": "skipped"}
    signal_triggered = (
        _collect_signal_triggered_orderbook(args)
        if args.collect_signal_triggered_orderbook
        else {"status": "skipped"}
    )
    ticker_dates = orderbook_ticker_dates(args.orderbook_cache_dir, args.tickers)
    if not ticker_dates:
        payload = {
            "schema_version": 1,
            "kind": "liquidity_holdout_update",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "waiting_for_data",
            "reason_code": "no_orderbook_snapshots_for_requested_tickers",
            "collection_window_preflight": preflight,
            "orderbook_collection": collected,
            "signal_triggered_orderbook_collection": signal_triggered,
            "orderbook_ticker_dates": {},
            "output_dir": str(args.output_dir),
        }
        write_json(args.output_dir / "liquidity-update-result.json", payload)
        return payload
    candle_sync = {"status": "skipped"} if args.skip_candle_sync else _sync_candles(args, ticker_dates=ticker_dates)
    holdout = {"status": "skipped"} if args.skip_holdout_run else _run_holdout(args)
    collection_plan = (
        {"status": "skipped", "reason_code": "holdout_run_skipped"}
        if args.skip_holdout_run
        else _write_collection_plan(args, holdout)
    )
    payload = {
        "schema_version": 1,
        "kind": "liquidity_holdout_update",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": holdout.get("status", "ok") if not args.skip_holdout_run else "ok",
        "collection_window_preflight": preflight,
        "orderbook_collection": collected,
        "signal_triggered_orderbook_collection": signal_triggered,
        "orderbook_ticker_dates": {
            ticker: [day.isoformat() for day in days] for ticker, days in ticker_dates.items()
        },
        "candle_sync": candle_sync,
        "holdout": holdout,
        "collection_plan": collection_plan,
        "output_dir": str(args.output_dir),
    }
    write_json(args.output_dir / "liquidity-update-result.json", payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-update-liquidity-holdout")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--tickers", type=_parse_tickers, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--collect-orderbook", action="store_true")
    parser.add_argument("--collect-signal-triggered-orderbook", action="store_true")
    parser.add_argument("--orderbook-depth", type=int, default=10)
    parser.add_argument("--orderbook-samples", type=int, default=1)
    parser.add_argument("--orderbook-interval-seconds", type=float, default=60.0)
    parser.add_argument("--orderbook-flush-every-samples", type=int, default=20)
    parser.add_argument(
        "--signal-triggered-state-file",
        type=Path,
        default=Path("var/research/tinvest_orderbooks/signal-triggered-state.json"),
    )
    parser.add_argument("--signal-triggered-polls", type=int, default=1)
    parser.add_argument("--signal-triggered-interval-seconds", type=float, default=60.0)
    parser.add_argument("--signal-triggered-max-signal-age-seconds", type=int, default=180)
    parser.add_argument("--signal-triggered-target-day", type=date.fromisoformat)
    parser.add_argument("--refresh-latest-orderbook-day", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--refresh-candle-days", type=_parse_dates, default=frozenset())
    parser.add_argument("--skip-candle-sync", action="store_true")
    parser.add_argument("--skip-holdout-run", action="store_true")
    parser.add_argument("--max-age-seconds", default="5,15,30,60")
    parser.add_argument("--preferred-max-age-seconds", type=int, default=30)
    parser.add_argument("--min-covered-signals", type=int, default=300)
    parser.add_argument("--min-covered-sessions", type=int, default=30)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--horizons", default="60,300,900,1800")
    parser.add_argument("--lookback-windows", default="5,15,30,60")
    parser.add_argument("--max-signals-per-instrument", type=int, default=10_000)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--request-interval", type=float, default=0.05)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current"))
    parser.add_argument("--only-orderbook-dates", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--collection-plan-target-calendar-days", type=int, default=45)
    parser.add_argument("--collection-plan-target-continuous-hours", type=float, default=8.0)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--require-full-prior-window", action="store_true")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    result = run_update(parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
