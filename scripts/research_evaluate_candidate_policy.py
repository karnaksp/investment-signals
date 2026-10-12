#!/usr/bin/env python3
"""Evaluate an exported research candidate policy on a separate run."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


def stable_candidate_id(scope: object, rule: object) -> str:
    payload = json.dumps(
        {
            "schema": "signal_candidate_v1",
            "scope": str(scope),
            "rule": str(rule),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _read_frontier(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _run_metadata(run_dir: Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {}
    path = run_dir / "model-results.json"
    if not path.exists():
        return {}
    payload = _read_json(path)
    return {
        "dataset": payload.get("dataset"),
        "dataset_fingerprint": payload.get("dataset_fingerprint"),
        "dataset_rows": payload.get("dataset_rows"),
        "validation_sessions": payload.get("validation_sessions"),
    }


def _float_or_none(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: object) -> int:
    numeric = _float_or_none(value)
    return int(numeric) if numeric is not None else 0


def _wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float | None:
    if total <= 0:
        return None
    phat = successes / total
    denominator = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    margin = z * ((phat * (1 - phat) + z * z / (4 * total)) / total) ** 0.5
    return (centre - margin) / denominator


def _frontier_by_candidate_id(rows: Sequence[Mapping[str, str]]) -> dict[str, Mapping[str, str]]:
    result: dict[str, Mapping[str, str]] = {}
    for row in rows:
        candidate_id = stable_candidate_id(row.get("scope", ""), row.get("rule", ""))
        previous = result.get(candidate_id)
        if previous is None or _int_or_zero(row.get("selected_rows")) > _int_or_zero(previous.get("selected_rows")):
            result[candidate_id] = row
    return result


def evaluate_candidate_policy(
    *,
    policy_path: Path,
    frontier_path: Path,
    run_dir: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    policy = _read_json(policy_path)
    frontier_rows = _read_frontier(frontier_path)
    frontier = _frontier_by_candidate_id(frontier_rows)
    metadata = _run_metadata(run_dir or frontier_path.parent)
    current_fingerprint = metadata.get("dataset_fingerprint")

    evaluations: list[dict[str, Any]] = []
    for rule in policy.get("rules", []):
        if not isinstance(rule, dict) or rule.get("status") != "shadow":
            continue
        candidate_id = str(rule.get("candidate_id", ""))
        source_fingerprints = set(rule.get("source_dataset_fingerprints") or [])
        independent = bool(current_fingerprint and current_fingerprint not in source_fingerprints)
        row = frontier.get(candidate_id)
        if row is None:
            evaluations.append(
                {
                    "candidate_id": candidate_id,
                    "status": "not_observed",
                    "independent_dataset": independent,
                    "product_claim_allowed": False,
                }
            )
            continue
        selected_rows = _int_or_zero(row.get("selected_rows"))
        successes = _int_or_zero(row.get("success_count"))
        success_rate = successes / selected_rows if selected_rows else None
        lower = _wilson_lower_bound(successes, selected_rows)
        mean_result = _float_or_none(row.get("mean_selected_result_bps"))
        passed_shadow_gate = bool(
            independent
            and selected_rows >= 300
            and _int_or_zero(row.get("sessions")) >= 30
            and success_rate is not None
            and success_rate >= 0.90
            and lower is not None
            and lower >= 0.75
            and mean_result is not None
            and mean_result > 0
        )
        evaluations.append(
            {
                "candidate_id": candidate_id,
                "status": "evaluated",
                "independent_dataset": independent,
                "selected_rows": selected_rows,
                "sessions": _int_or_zero(row.get("sessions")),
                "success_count": successes,
                "success_rate": success_rate,
                "wilson_lower_95": lower,
                "mean_selected_result_bps": mean_result,
                "passed_shadow_gate": passed_shadow_gate,
                "product_claim_allowed": False,
                "rule": row.get("rule", rule.get("rule", "")),
            }
        )

    passed = [row for row in evaluations if row.get("passed_shadow_gate")]
    return {
        "schema_version": 1,
        "kind": "research_candidate_policy_evaluation",
        "generated_at": generated_at,
        "policy": str(policy_path),
        "frontier": str(frontier_path),
        "run_dir": str(run_dir or frontier_path.parent),
        "dataset_fingerprint": current_fingerprint,
        "policy_status": policy.get("status", "unknown"),
        "evaluated_rules": len(evaluations),
        "passed_shadow_gate": len(passed),
        "product_claim_allowed": False,
        "status": "passed_shadow" if passed else "no_shadow_candidate_passed",
        "evaluations": evaluations,
    }


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    if not fieldnames:
        fieldnames = ["candidate_id", "status"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-evaluate-candidate-policy")
    parser.add_argument("--policy", type=Path, default=Path("var/research/candidate-decision-policy.json"))
    parser.add_argument("--frontier", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--output-json", type=Path, default=Path("var/research/candidate-policy-evaluation.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("var/research/candidate-policy-evaluation.csv"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = evaluate_candidate_policy(policy_path=args.policy, frontier_path=args.frontier, run_dir=args.run_dir)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with args.output_json.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    _write_csv(args.output_csv, result.get("evaluations", []))
    print(
        json.dumps(
            {
                "status": result["status"],
                "evaluated_rules": result["evaluated_rules"],
                "passed_shadow_gate": result["passed_shadow_gate"],
                "output_json": str(args.output_json),
                "output_csv": str(args.output_csv),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
