#!/usr/bin/env python3
"""Run the liquidity-aware research flow only after holdout readiness is proven."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent


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
    if not lines:
        return {}
    return json.loads(lines[-1])


def choose_ready_window(readiness_rows: Sequence[Mapping[str, Any]]) -> int | None:
    ready = [row for row in readiness_rows if row.get("ready")]
    if not ready:
        return None
    return int(min(ready, key=lambda row: int(row["max_age_seconds"]))["max_age_seconds"])


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    coverage_dir = args.output_dir / "coverage"
    readiness_dir = args.output_dir / "readiness"
    dataset_path = args.output_dir / "signal_price_prediction_liquidity.parquet"
    runs_dir = args.output_dir / "runs"

    coverage_command = [
        sys.executable,
        str(SCRIPT_DIR / "research_orderbook_signal_coverage.py"),
        "--cache-dir",
        str(args.cache_dir),
        "--orderbook-cache-dir",
        str(args.orderbook_cache_dir),
        "--max-age-seconds",
        args.max_age_seconds,
        "--max-signals-per-instrument",
        str(args.max_signals_per_instrument),
        "--output-dir",
        str(coverage_dir),
    ]
    if args.tickers:
        coverage_command.extend(["--tickers", ",".join(args.tickers)])
    coverage_result = _run_json_command(coverage_command)

    readiness_command = [
        sys.executable,
        str(SCRIPT_DIR / "research_holdout_readiness.py"),
        "--coverage-json",
        str(coverage_dir / "coverage.json"),
        "--min-covered-signals",
        str(args.min_covered_signals),
        "--min-covered-sessions",
        str(args.min_covered_sessions),
        "--min-coverage",
        str(args.min_coverage),
        "--preferred-max-age-seconds",
        str(args.preferred_max_age_seconds),
        "--output-dir",
        str(readiness_dir),
    ]
    readiness_result = _run_json_command(readiness_command)
    readiness_payload = _load_json(readiness_dir / "readiness.json")
    selected_age = choose_ready_window(readiness_payload.get("rows", []))

    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "liquidity_holdout_research_pipeline",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "coverage": coverage_result,
        "readiness": readiness_result,
        "selected_orderbook_max_age_seconds": selected_age,
        "output_dir": str(args.output_dir),
    }
    if selected_age is None and not args.force:
        payload["status"] = "waiting_for_data"
        payload["reason_code"] = "holdout_not_ready"
        _write_json(args.output_dir / "pipeline-result.json", payload)
        return payload

    training_age = selected_age or args.preferred_max_age_seconds
    build_command = [
        sys.executable,
        str(SCRIPT_DIR / "research_build_signal_price_dataset.py"),
        "--cache-dir",
        str(args.cache_dir),
        "--orderbook-cache-dir",
        str(args.orderbook_cache_dir),
        "--orderbook-max-age-seconds",
        str(training_age),
        "--require-orderbook-features",
        "--horizons",
        args.horizons,
        "--lookback-windows",
        args.lookback_windows,
        "--max-signals-per-instrument",
        str(args.max_signals_per_instrument),
        "--output",
        str(dataset_path),
    ]
    if args.only_orderbook_dates:
        build_command.append("--only-orderbook-dates")
    if args.tickers:
        build_command.extend(["--tickers", ",".join(args.tickers)])
    build_result = _run_json_command(build_command)

    train_result = _run_json_command(
        [
            sys.executable,
            str(SCRIPT_DIR / "research_train_price_models.py"),
            "--dataset",
            str(dataset_path),
            "--output-dir",
            str(runs_dir),
        ]
    )
    mine_result = _run_json_command(
        [
            sys.executable,
            str(SCRIPT_DIR / "research_mine_price_patterns.py"),
            "--dataset",
            str(dataset_path),
            "--run-dir",
            str(runs_dir / str(train_result["run_id"])),
        ]
    )
    payload.update(
        {
            "status": "ok",
            "dataset": build_result,
            "training": train_result,
            "patterns": mine_result,
        }
    )
    _write_json(args.output_dir / "pipeline-result.json", payload)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-run-liquidity-holdout")
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--tickers", type=_parse_tickers)
    parser.add_argument("--max-age-seconds", default="5,15,30,60")
    parser.add_argument("--preferred-max-age-seconds", type=int, default=30)
    parser.add_argument("--min-covered-signals", type=int, default=300)
    parser.add_argument("--min-covered-sessions", type=int, default=30)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--horizons", default="60,300,900,1800")
    parser.add_argument("--lookback-windows", default="5,15,30,60")
    parser.add_argument("--max-signals-per-instrument", type=int, default=10_000)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current"))
    parser.add_argument(
        "--only-orderbook-dates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Build the liquidity dataset only from ticker/date partitions that have order-book snapshots.",
    )
    parser.add_argument("--force", action="store_true", help="Build/train even when readiness is false.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    result = run_pipeline(parse_args(argv))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
