#!/usr/bin/env python3
"""Run bounded order-book collection cycles until microstructure evidence is ready."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import DEFAULT_RESEARCH_TICKERS, write_json  # noqa: E402
from research_signal_90_status import build_signal_90_status, write_report as write_status_report  # noqa: E402


Runner = Callable[[Sequence[str]], Mapping[str, Any]]


def _parse_tickers(raw: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


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


def build_update_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_update_liquidity_holdout.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.cache_dir),
        "--orderbook-cache-dir",
        str(args.orderbook_cache_dir),
        "--collect-orderbook",
        "--collect-signal-triggered-orderbook",
        "--tickers",
        ",".join(args.tickers),
        "--orderbook-depth",
        str(args.orderbook_depth),
        "--orderbook-samples",
        str(args.orderbook_samples),
        "--orderbook-interval-seconds",
        str(args.orderbook_interval_seconds),
        "--orderbook-flush-every-samples",
        str(args.orderbook_flush_every_samples),
        "--signal-triggered-polls",
        str(args.signal_triggered_polls),
        "--signal-triggered-interval-seconds",
        str(args.signal_triggered_interval_seconds),
        "--signal-triggered-max-signal-age-seconds",
        str(args.signal_triggered_max_signal_age_seconds),
        "--preferred-max-age-seconds",
        str(args.preferred_max_age_seconds),
        "--min-covered-signals",
        str(args.min_covered_signals),
        "--min-covered-sessions",
        str(args.min_covered_sessions),
        "--min-coverage",
        str(args.min_coverage),
        "--output-dir",
        str(args.output_dir),
        "--collection-plan-target-continuous-hours",
        str(args.collection_plan_target_continuous_hours),
    ]
    if args.skip_candle_sync:
        command.append("--skip-candle-sync")
    if args.skip_holdout_run:
        command.append("--skip-holdout-run")
    if args.force:
        command.append("--force")
    if args.require_full_prior_window:
        command.append("--require-full-prior-window")
    if args.only_orderbook_dates:
        command.append("--only-orderbook-dates")
    else:
        command.append("--no-only-orderbook-dates")
    if args.ca_cert:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    return command


def should_stop(status: Mapping[str, Any]) -> bool:
    microstructure = status.get("microstructure")
    if isinstance(microstructure, Mapping) and bool(microstructure.get("ready")):
        return True
    return bool(status.get("product_claim_allowed"))


def _collection_plan_path(output_dir: Path) -> Path:
    return output_dir / "collection_plan" / "collection-plan.json"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _coverage_progress(output_dir: Path, *, preferred_max_age_seconds: int) -> dict[str, Any]:
    coverage = _load_json(output_dir / "coverage" / "coverage.json")
    rows = coverage.get("rows") if isinstance(coverage.get("rows"), list) else []
    by_day = coverage.get("by_ticker_day") if isinstance(coverage.get("by_ticker_day"), list) else []
    if not rows and not by_day:
        return {
            "status": "missing",
            "reason_code": "coverage_json_missing_or_empty",
            "coverage_json": str(output_dir / "coverage" / "coverage.json"),
        }
    preferred = [
        row
        for row in rows
        if int(row.get("max_age_seconds", 0) or 0) <= preferred_max_age_seconds
    ]
    candidates = preferred or rows
    best = max(
        candidates,
        key=lambda row: (
            float(row.get("coverage", 0.0) or 0.0),
            int(row.get("covered_signals", 0) or 0),
            int(row.get("covered_sessions", 0) or 0),
        ),
    ) if candidates else {}
    missing_days = [
        row for row in by_day if int(row.get("covered_signals", 0) or 0) == 0
    ]
    partial_days = [
        row
        for row in by_day
        if int(row.get("covered_signals", 0) or 0) > 0
        and int(row.get("covered_signals", 0) or 0) < int(row.get("signals", 0) or 0)
    ]
    return {
        "status": "ok",
        "preferred_max_age_seconds": preferred_max_age_seconds,
        "best_window": dict(best),
        "by_ticker_day_available": bool(by_day),
        "ticker_days": len(by_day),
        "missing_ticker_days": len(missing_days) if by_day else None,
        "partial_ticker_days": len(partial_days) if by_day else None,
        "covered_signals": int(best.get("covered_signals", 0) or 0),
        "signals": int(best.get("signals", 0) or 0),
        "covered_sessions": int(best.get("covered_sessions", 0) or 0),
        "coverage": float(best.get("coverage", 0.0) or 0.0),
        "nearest_prior_orderbook_age_seconds": best.get("nearest_prior_orderbook_age_seconds", ""),
        "nearest_signal_orderbook_gap_seconds": best.get("nearest_signal_orderbook_gap_seconds", ""),
        "worst_ticker_days": [
            {
                "ticker": row.get("ticker"),
                "trading_day": row.get("trading_day"),
                "signals": row.get("signals"),
                "covered_signals": row.get("covered_signals"),
                "coverage": row.get("coverage"),
                "first_signal_at": row.get("first_signal_at"),
                "last_signal_at": row.get("last_signal_at"),
                "first_orderbook_at": row.get("first_orderbook_at"),
                "last_orderbook_at": row.get("last_orderbook_at"),
            }
            for row in sorted(
                by_day,
                key=lambda item: (
                    float(item.get("coverage", 0.0) or 0.0),
                    -int(item.get("signals", 0) or 0),
                    str(item.get("ticker", "")),
                    str(item.get("trading_day", "")),
                ),
            )[:10]
        ],
    }


def _coverage_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    def int_value(payload: Mapping[str, Any], key: str) -> int:
        try:
            return int(payload.get(key, 0) or 0)
        except (TypeError, ValueError):
            return 0

    def float_value(payload: Mapping[str, Any], key: str) -> float:
        try:
            return float(payload.get(key, 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    covered_delta = int_value(after, "covered_signals") - int_value(before, "covered_signals")
    sessions_delta = int_value(after, "covered_sessions") - int_value(before, "covered_sessions")
    coverage_delta = float_value(after, "coverage") - float_value(before, "coverage")
    return {
        "covered_signals_delta": covered_delta,
        "covered_sessions_delta": sessions_delta,
        "coverage_delta": coverage_delta,
        "improved_prior_coverage": bool(covered_delta > 0 or sessions_delta > 0 or coverage_delta > 0),
        "reason_code": "prior_coverage_improved" if covered_delta > 0 else "no_prior_coverage_improvement",
    }


def write_loop_report(path: Path, payload: Mapping[str, Any]) -> None:
    lines = [
        "# Microstructure collection loop",
        "",
        f"- Status: `{payload.get('status')}`",
        f"- Iterations: {len(payload.get('iterations', []))}",
        "",
        "## Iterations",
        "",
        "| Iteration | Update | Signal 90 status | Covered signals | Δ covered | Coverage | Δ coverage | Covered sessions | Δ sessions | Missing ticker-days | Nearest prior age, seconds | Missing reasons |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in payload.get("iterations", []):
        if not isinstance(item, Mapping):
            continue
        coverage = item.get("coverage_progress") if isinstance(item.get("coverage_progress"), Mapping) else {}
        delta = item.get("coverage_delta") if isinstance(item.get("coverage_delta"), Mapping) else {}
        lines.append(
            "| {iteration} | {update} | {status} | {covered} | {delta_covered} | {coverage:.4f} | {delta_coverage:.4f} | {sessions} | {delta_sessions} | {missing_days} | {prior_age} | {reasons} |".format(
                iteration=item.get("iteration", ""),
                update=item.get("update_status", ""),
                status=item.get("signal_90_status", ""),
                covered=coverage.get("covered_signals", 0),
                delta_covered=delta.get("covered_signals_delta", ""),
                coverage=float(coverage.get("coverage", 0.0) or 0.0),
                delta_coverage=float(delta.get("coverage_delta", 0.0) or 0.0),
                sessions=coverage.get("covered_sessions", 0),
                delta_sessions=delta.get("covered_sessions_delta", ""),
                missing_days=coverage.get("missing_ticker_days", ""),
                prior_age=coverage.get("nearest_prior_orderbook_age_seconds", ""),
                reasons=", ".join(item.get("missing_reasons", [])),
            )
        )
    final_coverage = payload.get("final_coverage_progress")
    if isinstance(final_coverage, Mapping) and final_coverage:
        lines.extend(
            [
                "",
                "## Final coverage",
                "",
                f"- Covered signals: {final_coverage.get('covered_signals')}",
                f"- Total signals: {final_coverage.get('signals')}",
                f"- Coverage: {float(final_coverage.get('coverage', 0.0) or 0.0):.4f}",
                f"- Covered sessions: {final_coverage.get('covered_sessions')}",
                f"- Ticker-day detail available: `{final_coverage.get('by_ticker_day_available')}`",
                f"- Ticker-days: {final_coverage.get('ticker_days')}",
                f"- Missing ticker-days: {final_coverage.get('missing_ticker_days')}",
                f"- Nearest prior age, seconds: {final_coverage.get('nearest_prior_orderbook_age_seconds')}",
                "",
            ]
        )
    if isinstance(final_coverage, Mapping) and final_coverage.get("worst_ticker_days"):
        lines.extend(
            [
                "",
                "## Worst ticker-days",
                "",
                "| Ticker | Day | Signals | Covered | Coverage | First signal | Last signal | First snapshot | Last snapshot |",
                "|---|---|---:|---:|---:|---|---|---|---|",
            ]
        )
        for row in final_coverage.get("worst_ticker_days", []):
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "| {ticker} | {day} | {signals} | {covered} | {coverage:.4f} | {first_signal} | {last_signal} | {first_snapshot} | {last_snapshot} |".format(
                    ticker=row.get("ticker", ""),
                    day=row.get("trading_day", ""),
                    signals=row.get("signals", 0),
                    covered=row.get("covered_signals", 0),
                    coverage=float(row.get("coverage", 0.0) or 0.0),
                    first_signal=row.get("first_signal_at", ""),
                    last_signal=row.get("last_signal_at", ""),
                    first_snapshot=row.get("first_orderbook_at", ""),
                    last_snapshot=row.get("last_orderbook_at", ""),
                )
            )
    lines.extend(
        [
            "",
            "A useful collection cycle must increase covered signals with prior order-book snapshots. "
            "More snapshots alone are not enough when they are not close to signal timestamps.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run_collection_loop(args: argparse.Namespace, *, runner: Runner = _run_json_command) -> dict[str, Any]:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.status_output_dir.mkdir(parents=True, exist_ok=True)
    command = build_update_command(args)
    iterations: list[dict[str, Any]] = []
    final_status: dict[str, Any] = {}

    if args.dry_run:
        coverage_progress = _coverage_progress(args.output_dir, preferred_max_age_seconds=args.preferred_max_age_seconds)
        final_status = build_signal_90_status(
            run_dir=args.run_dir,
            collection_plan_path=_collection_plan_path(args.output_dir),
        )
        write_status_report(args.status_output_dir / "signal-90-status.md", final_status)
        write_json(args.status_output_dir / "signal-90-status.json", final_status)
        payload = {
            "schema_version": 1,
            "kind": "microstructure_collection_loop",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "dry_run",
            "iterations": [],
            "recommended_command": command,
            "final_coverage_progress": coverage_progress,
            "final_signal_90_status": final_status,
        }
        write_json(args.output_dir / "microstructure-collection-loop.json", payload)
        write_loop_report(args.output_dir / "microstructure-collection-loop.md", payload)
        return payload

    for index in range(args.max_iterations):
        before_coverage = _coverage_progress(args.output_dir, preferred_max_age_seconds=args.preferred_max_age_seconds)
        update_result = dict(runner(command))
        plan_path = _collection_plan_path(args.output_dir)
        coverage_progress = _coverage_progress(args.output_dir, preferred_max_age_seconds=args.preferred_max_age_seconds)
        coverage_delta = _coverage_delta(before_coverage, coverage_progress)
        final_status = build_signal_90_status(run_dir=args.run_dir, collection_plan_path=plan_path)
        write_status_report(args.status_output_dir / "signal-90-status.md", final_status)
        write_json(args.status_output_dir / "signal-90-status.json", final_status)
        iteration = {
            "iteration": index + 1,
            "update_status": update_result.get("status"),
            "signal_90_status": final_status.get("status"),
            "microstructure": final_status.get("microstructure"),
            "coverage_progress": coverage_progress,
            "coverage_delta": coverage_delta,
            "missing_reasons": final_status.get("missing_reasons", []),
        }
        iterations.append(iteration)
        if should_stop(final_status):
            break

    payload = {
        "schema_version": 1,
        "kind": "microstructure_collection_loop",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "ready" if should_stop(final_status) else "collect_more_data",
        "iterations": iterations,
        "recommended_command": command,
        "final_coverage_progress": iterations[-1]["coverage_progress"] if iterations else _coverage_progress(
            args.output_dir,
            preferred_max_age_seconds=args.preferred_max_age_seconds,
        ),
        "final_signal_90_status": final_status,
    }
    write_json(args.output_dir / "microstructure-collection-loop.json", payload)
    write_loop_report(args.output_dir / "microstructure-collection-loop.md", payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-collect-until-microstructure-ready")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--run-dir", type=Path, default=Path("var/research/runs/fe7da78bab3fd474"))
    parser.add_argument("--status-output-dir", type=Path, default=Path("var/research/signal_90_status/current"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current"))
    parser.add_argument("--tickers", type=_parse_tickers, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--max-iterations", type=int, default=1)
    parser.add_argument("--orderbook-depth", type=int, default=10)
    parser.add_argument("--orderbook-samples", type=int, default=1920)
    parser.add_argument("--orderbook-interval-seconds", type=float, default=15.0)
    parser.add_argument("--orderbook-flush-every-samples", type=int, default=20)
    parser.add_argument("--signal-triggered-polls", type=int, default=1920)
    parser.add_argument("--signal-triggered-interval-seconds", type=float, default=15.0)
    parser.add_argument("--signal-triggered-max-signal-age-seconds", type=int, default=180)
    parser.add_argument("--preferred-max-age-seconds", type=int, default=30)
    parser.add_argument("--min-covered-signals", type=int, default=300)
    parser.add_argument("--min-covered-sessions", type=int, default=30)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--collection-plan-target-continuous-hours", type=float, default=8.0)
    parser.add_argument("--only-orderbook-dates", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--skip-candle-sync", action="store_true")
    parser.add_argument("--skip-holdout-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--require-full-prior-window", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    result = run_collection_loop(parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
