#!/usr/bin/env python3
"""Apply a research candidate policy to decision-audit rows.

This is the final research-only three-way decision layer for accepted shadow
candidates. It never authorizes a product claim: matched rows can become
admin-only shadow up/down decisions, and every other row remains a skip.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_extract_candidate_audit_rows import parse_candidate_rule


RU_LABELS = {
    "up": "ожидается рост",
    "down": "ожидается снижение",
    "skip": "пропустить, недостаточно уверенности",
}

OUTPUT_FIELDS = (
    "row_id",
    "ticker",
    "signal_type",
    "source_event_at",
    "trading_day",
    "horizon_seconds",
    "product_decision",
    "product_label_ru",
    "shadow_candidate_id",
    "shadow_rule",
    "shadow_decision",
    "shadow_admin_only",
    "confidence",
    "frontier_decision",
    "frontier_decision_relation",
    "frontier_success",
    "frontier_result_bps",
    "reason_code",
    "product_claim_allowed",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _audit_field_for_rule_key(key: str) -> str:
    if key == "decision":
        return "frontier_decision"
    if key == "decision_relation":
        return "frontier_decision_relation"
    return key


def _row_matches_rule(row: Mapping[str, Any], predicates: Mapping[str, str]) -> bool:
    for key, expected in predicates.items():
        if str(row.get(_audit_field_for_rule_key(key), "")) != expected:
            return False
    return True


def _current_dataset_fingerprint(run_dir: Path | None) -> str:
    if run_dir is None:
        return ""
    payload = _read_json(run_dir / "model-results.json")
    return str(payload.get("dataset_fingerprint") or "")


def _shadow_rules(
    policy: Mapping[str, Any],
    *,
    current_dataset_fingerprint: str = "",
) -> tuple[list[dict[str, Any]], int]:
    if str(policy.get("status", "disabled")) != "shadow":
        return [], 0
    result: list[dict[str, Any]] = []
    blocked_same_dataset = 0
    for rule in policy.get("rules", []):
        if not isinstance(rule, dict) or rule.get("status") != "shadow":
            continue
        source_fingerprints = {str(item) for item in rule.get("source_dataset_fingerprints") or []}
        if current_dataset_fingerprint and current_dataset_fingerprint in source_fingerprints:
            blocked_same_dataset += 1
            continue
        decision = str(rule.get("shadow_decision") or "").strip()
        if decision not in {"up", "down"}:
            predicates = parse_candidate_rule(rule.get("rule", ""))
            decision = predicates.get("decision") or predicates.get("frontier_decision") or ""
        if decision not in {"up", "down"}:
            continue
        item = dict(rule)
        item["_predicates"] = parse_candidate_rule(rule.get("rule", ""))
        item["_shadow_decision"] = decision
        result.append(item)
    return result, blocked_same_dataset


def _skip_row(row: Mapping[str, Any], reason_code: str) -> dict[str, Any]:
    return {
        "row_id": row.get("row_id", ""),
        "ticker": row.get("ticker", ""),
        "signal_type": row.get("signal_type", ""),
        "source_event_at": row.get("source_event_at", ""),
        "trading_day": row.get("trading_day", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "product_decision": "skip",
        "product_label_ru": RU_LABELS["skip"],
        "shadow_candidate_id": "",
        "shadow_rule": "",
        "shadow_decision": "",
        "shadow_admin_only": True,
        "confidence": row.get("frontier_confidence", row.get("max_confidence", "")),
        "frontier_decision": row.get("frontier_decision", ""),
        "frontier_decision_relation": row.get("frontier_decision_relation", ""),
        "frontier_success": row.get("frontier_success", ""),
        "frontier_result_bps": row.get("frontier_result_bps", ""),
        "reason_code": reason_code,
        "product_claim_allowed": False,
    }


def apply_candidate_policy_rows(
    *,
    audit_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    current_dataset_fingerprint: str = "",
) -> list[dict[str, Any]]:
    rules, blocked_same_dataset = _shadow_rules(
        policy,
        current_dataset_fingerprint=current_dataset_fingerprint,
    )
    if not rules:
        reason = (
            "shadow_policy_not_independent"
            if blocked_same_dataset
            else "no_shadow_candidate_policy"
        )
        return [_skip_row(row, reason) for row in audit_rows]

    result: list[dict[str, Any]] = []
    for row in audit_rows:
        match = next(
            (rule for rule in rules if _row_matches_rule(row, rule["_predicates"])),
            None,
        )
        if match is None:
            result.append(_skip_row(row, "no_shadow_rule_match"))
            continue
        decision = str(match["_shadow_decision"])
        result.append(
            {
                "row_id": row.get("row_id", ""),
                "ticker": row.get("ticker", ""),
                "signal_type": row.get("signal_type", ""),
                "source_event_at": row.get("source_event_at", ""),
                "trading_day": row.get("trading_day", ""),
                "horizon_seconds": row.get("horizon_seconds", ""),
                "product_decision": decision,
                "product_label_ru": RU_LABELS[decision],
                "shadow_candidate_id": match.get("candidate_id", ""),
                "shadow_rule": match.get("rule", ""),
                "shadow_decision": decision,
                "shadow_admin_only": True,
                "confidence": row.get("frontier_confidence", row.get("max_confidence", "")),
                "frontier_decision": row.get("frontier_decision", ""),
                "frontier_decision_relation": row.get("frontier_decision_relation", ""),
                "frontier_success": row.get("frontier_success", ""),
                "frontier_result_bps": row.get("frontier_result_bps", ""),
                "reason_code": "matched_shadow_candidate",
                "product_claim_allowed": False,
            }
        )
    return result


def summarize_rows(rows: Sequence[Mapping[str, Any]], *, policy: Mapping[str, Any]) -> dict[str, Any]:
    decisions = Counter(str(row.get("product_decision", "")) for row in rows)
    reasons = Counter(str(row.get("reason_code", "")) for row in rows)
    return {
        "schema_version": 1,
        "kind": "research_candidate_policy_application",
        "policy_status": policy.get("status", "missing"),
        "dataset_independence_enforced": True,
        "rows": len(rows),
        "decision_counts": dict(sorted(decisions.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "shadow_decisions": decisions.get("up", 0) + decisions.get("down", 0),
        "product_claim_allowed": False,
    }


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    lines = [
        "# Применение политики кандидатов",
        "",
        f"- Статус политики: `{summary.get('policy_status')}`",
        f"- Строк: {summary.get('rows')}",
        f"- Shadow-решений рост/снижение: {summary.get('shadow_decisions')}",
        f"- Product claim allowed: `{summary.get('product_claim_allowed')}`",
        "",
        "## Решения",
        "",
    ]
    for key, value in dict(summary.get("decision_counts", {})).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Причины", ""])
    for key, value in dict(summary.get("reason_counts", {})).items():
        lines.append(f"- `{key}`: {value}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def apply_candidate_policy(
    *,
    audit_path: Path,
    policy_path: Path,
    output_dir: Path,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    policy = _read_json(policy_path)
    effective_run_dir = run_dir or audit_path.parent
    dataset_fingerprint = _current_dataset_fingerprint(effective_run_dir)
    rows = apply_candidate_policy_rows(
        audit_rows=_read_csv(audit_path),
        policy=policy,
        current_dataset_fingerprint=dataset_fingerprint,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "candidate-policy-decisions.csv", rows)
    summary = summarize_rows(rows, policy=policy) | {
        "run_dir": str(effective_run_dir),
        "dataset_fingerprint": dataset_fingerprint,
    }
    (output_dir / "candidate-policy-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_report(output_dir / "candidate-policy-report.md", summary)
    return summary | {"output_dir": str(output_dir)}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-apply-candidate-policy")
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = apply_candidate_policy(
        audit_path=args.audit,
        policy_path=args.policy,
        output_dir=args.output_dir,
        run_dir=args.run_dir,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
