#!/usr/bin/env python3
"""Refresh all 90% objective reports using the latest available research run."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_BASELINE_RUN_DIR = Path("var/research/runs/fe7da78bab3fd474")
DEFAULT_BASELINE_DATASET = Path("var/research/datasets/signal_price_prediction.parquet")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _run(command: Sequence[str]) -> dict[str, Any]:
    completed = subprocess.run(  # noqa: S603 - commands are built from fixed local scripts
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
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return {"stdout_tail": lines[-1]}


def resolve_current_run(
    *,
    holdout_dir: Path,
    fallback_run_dir: Path = DEFAULT_BASELINE_RUN_DIR,
    fallback_dataset: Path = DEFAULT_BASELINE_DATASET,
) -> dict[str, Any]:
    pipeline = _read_json(holdout_dir / "pipeline-result.json")
    training = pipeline.get("training") if isinstance(pipeline.get("training"), Mapping) else {}
    run_id = training.get("run_id")
    candidate_run_dir = holdout_dir / "runs" / str(run_id) if run_id else Path()
    candidate_dataset = holdout_dir / "signal_price_prediction_liquidity.parquet"
    if pipeline.get("status") == "ok" and run_id and candidate_run_dir.exists() and candidate_dataset.exists():
        return {
            "source": "liquidity_holdout",
            "run_dir": str(candidate_run_dir),
            "dataset": str(candidate_dataset),
            "run_id": str(run_id),
            "pipeline_status": pipeline.get("status"),
        }
    return {
        "source": "baseline",
        "run_dir": str(fallback_run_dir),
        "dataset": str(fallback_dataset),
        "run_id": fallback_run_dir.name,
        "pipeline_status": pipeline.get("status", "missing"),
        "fallback_reason": (
            "liquidity_run_not_ready"
            if pipeline
            else "pipeline_result_missing"
        ),
    }


def refresh_reports(args: argparse.Namespace) -> dict[str, Any]:
    current = resolve_current_run(
        holdout_dir=args.holdout_dir,
        fallback_run_dir=args.fallback_run_dir,
        fallback_dataset=args.fallback_dataset,
    )
    run_dir = Path(str(current["run_dir"]))
    dataset = Path(str(current["dataset"]))
    collection_plan = args.holdout_dir / "collection_plan" / "collection-plan.json"
    schedule_status_dir = args.holdout_dir / "collection_plan"
    schedule_status = schedule_status_dir / "schedule-status.json"
    commands = [
        [
            sys.executable,
            "scripts/research_signal_90_status.py",
            "--run-dir",
            str(run_dir),
            "--collection-plan",
            str(collection_plan),
            "--output-dir",
            str(args.signal_status_dir),
        ],
        [
            sys.executable,
            "scripts/research_audit_90_goal_readiness.py",
            "--run-dir",
            str(run_dir),
            "--signal-status",
            str(args.signal_status_dir / "signal-90-status.json"),
            "--collection-plan",
            str(collection_plan),
            "--output-dir",
            str(args.goal_audit_dir),
        ],
        [
            sys.executable,
            "scripts/research_mine_false_positive_guards.py",
            "--decision-audit",
            str(run_dir / "decision-audit.csv"),
            "--output-dir",
            str(run_dir),
        ],
        [
            sys.executable,
            "scripts/research_report_90_selection.py",
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(args.selection_dir),
        ],
        [
            sys.executable,
            "scripts/research_audit_90_gap.py",
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(args.gap_audit_dir),
        ],
        [
            "uv",
            "run",
            "--extra",
            "research",
            "python",
            "scripts/research_audit_90_feature_coverage.py",
            "--dataset",
            str(dataset),
            "--decision-audit",
            str(run_dir / "decision-audit.csv"),
            "--threshold-report",
            str(run_dir / "confidence-threshold-report.csv"),
            "--precision-scout",
            str(run_dir / "precision-scout-candidates.csv"),
            "--output-dir",
            str(args.feature_coverage_dir),
        ],
        [
            sys.executable,
            "scripts/research_collection_schedule_status.py",
            "--collection-plan",
            str(collection_plan),
            "--output-dir",
            str(schedule_status_dir),
        ],
        [
            sys.executable,
            "scripts/research_liquidity_collection_live_status.py",
            "--collection-plan",
            str(collection_plan),
            "--schedule-status",
            str(schedule_status),
            "--orderbook-cache-dir",
            str(args.orderbook_cache_dir),
            "--output-dir",
            str(args.holdout_dir / "live_status"),
        ],
        [
            sys.executable,
            "scripts/research_collection_watchdog.py",
            "--live-status",
            str(args.holdout_dir / "live_status" / "live-status.json"),
            "--schedule-status",
            str(schedule_status),
            "--output-dir",
            str(args.holdout_dir / "watchdog"),
        ],
        [
            sys.executable,
            "scripts/research_microstructure_progress.py",
            "--coverage-json",
            str(args.holdout_dir / "coverage" / "coverage.json"),
            "--readiness-json",
            str(args.holdout_dir / "readiness" / "readiness.json"),
            "--live-status",
            str(args.holdout_dir / "live_status" / "live-status.json"),
            "--watchdog",
            str(args.holdout_dir / "watchdog" / "collection-watchdog.json"),
            "--output-dir",
            str(args.holdout_dir / "progress"),
        ],
        [
            sys.executable,
            "scripts/research_plan_90_next_actions.py",
            "--gap-audit",
            str(args.gap_audit_dir / "gap-to-90.json"),
            "--feature-coverage",
            str(args.feature_coverage_dir / "feature-coverage.json"),
            "--live-status",
            str(args.holdout_dir / "live_status" / "live-status.json"),
            "--output-dir",
            str(args.next_actions_dir),
        ],
        [
            sys.executable,
            "scripts/research_audit_90_objective_contract.py",
            "--selection-report",
            str(args.selection_dir / "selection-90-report.json"),
            "--signal-status",
            str(args.signal_status_dir / "signal-90-status.json"),
            "--goal-audit",
            str(args.goal_audit_dir / "goal-90-audit.json"),
            "--schedule-status",
            str(schedule_status),
            "--feature-coverage",
            str(args.feature_coverage_dir / "feature-coverage.json"),
            "--gap-audit",
            str(args.gap_audit_dir / "gap-to-90.json"),
            "--output-dir",
            str(args.objective_contract_dir),
        ],
        [
            sys.executable,
            "scripts/research_microstructure_daily_summary.py",
            "--coverage-json",
            str(args.holdout_dir / "coverage" / "coverage.json"),
            "--readiness-json",
            str(args.holdout_dir / "readiness" / "readiness.json"),
            "--signal-status",
            str(args.signal_status_dir / "signal-90-status.json"),
            "--collection-plan",
            str(collection_plan),
            "--output-dir",
            str(args.holdout_dir / "daily_summary"),
        ],
    ]
    results = [_run(command) for command in commands]
    payload = {
        "schema_version": 1,
        "kind": "refresh_90_reports",
        "current_run": current,
        "commands": [" ".join(command) for command in commands],
        "results": results,
        "status": "ok",
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "refresh-90-reports.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-refresh-90-reports")
    parser.add_argument("--holdout-dir", type=Path, default=Path("var/research/liquidity_holdout/current"))
    parser.add_argument("--fallback-run-dir", type=Path, default=DEFAULT_BASELINE_RUN_DIR)
    parser.add_argument("--fallback-dataset", type=Path, default=DEFAULT_BASELINE_DATASET)
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--signal-status-dir", type=Path, default=Path("var/research/signal_90_status/current"))
    parser.add_argument("--goal-audit-dir", type=Path, default=Path("var/research/goal_90_audit/current"))
    parser.add_argument("--selection-dir", type=Path, default=Path("var/research/selection_90/current"))
    parser.add_argument("--gap-audit-dir", type=Path, default=Path("var/research/gap_90/current"))
    parser.add_argument("--feature-coverage-dir", type=Path, default=Path("var/research/objective_90_features/current"))
    parser.add_argument("--objective-contract-dir", type=Path, default=Path("var/research/objective_90_contract/current"))
    parser.add_argument("--next-actions-dir", type=Path, default=Path("var/research/next_actions_90/current"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/refresh_90/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    result = refresh_reports(parse_args(argv))
    print(
        json.dumps(
            {"status": result["status"], "current_run": result["current_run"]},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
