#!/usr/bin/env python3
"""Export product-safe up/down/skip decisions from research audit rows."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


RU_LABELS = {
    "up": "ожидается рост",
    "down": "ожидается снижение",
    "skip": "пропустить, недостаточно уверенности",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float_or_zero(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _reliability_by_band(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("scope")) == "confidence_band" and row.get("rule"):
            result[str(row["rule"])] = dict(row)
    return result


def _skip_row(row: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "row_id": row.get("row_id", ""),
        "ticker": row.get("ticker", ""),
        "signal_type": row.get("signal_type", ""),
        "source_event_at": row.get("source_event_at", ""),
        "horizon_seconds": row.get("horizon_seconds", ""),
        "product_decision": "skip",
        "product_label_ru": RU_LABELS["skip"],
        "display_tier": "skip",
        "confidence": row.get("max_confidence", ""),
        "research_frontier_decision": row.get("frontier_decision", ""),
        "research_frontier_relation": row.get("frontier_decision_relation", ""),
        "reason_code": reason,
        "product_claim_allowed": False,
    }


def export_safe_triage_rows(
    *,
    audit_rows: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    reliability_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    policy_status = str(policy.get("status", "disabled"))
    selected_threshold = policy.get("selected_threshold")
    product_claim_allowed = bool(policy.get("product_claim_allowed"))
    reliability = _reliability_by_band(reliability_rows)
    if policy_status != "shadow" or selected_threshold in {None, ""}:
        return [_skip_row(row, "policy_not_enabled") for row in audit_rows]

    threshold = _float_or_zero(selected_threshold)
    result: list[dict[str, Any]] = []
    for row in audit_rows:
        confidence = _float_or_zero(row.get("max_confidence"))
        if confidence < threshold:
            result.append(_skip_row(row, "below_policy_threshold"))
            continue
        band = str(row.get("confidence_band", "skip"))
        band_evidence = reliability.get(band, {})
        safe_action = str(band_evidence.get("safe_runtime_action", "skip"))
        if safe_action == "skip":
            result.append(_skip_row(row, "confidence_band_not_validated"))
            continue
        direction = str(row.get("frontier_decision", "skip"))
        if direction not in {"up", "down"}:
            result.append(_skip_row(row, "no_direction"))
            continue
        result.append(
            {
                "row_id": row.get("row_id", ""),
                "ticker": row.get("ticker", ""),
                "signal_type": row.get("signal_type", ""),
                "source_event_at": row.get("source_event_at", ""),
                "horizon_seconds": row.get("horizon_seconds", ""),
                "product_decision": direction,
                "product_label_ru": RU_LABELS[direction],
                "display_tier": band,
                "confidence": row.get("max_confidence", ""),
                "research_frontier_decision": row.get("frontier_decision", ""),
                "research_frontier_relation": row.get("frontier_decision_relation", ""),
                "reason_code": "validated_shadow_policy",
                "product_claim_allowed": product_claim_allowed,
            }
        )
    return result


def write_safe_triage_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "row_id",
        "ticker",
        "signal_type",
        "source_event_at",
        "horizon_seconds",
        "product_decision",
        "product_label_ru",
        "display_tier",
        "confidence",
        "research_frontier_decision",
        "research_frontier_relation",
        "reason_code",
        "product_claim_allowed",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def summarize_safe_triage_rows(rows: Sequence[Mapping[str, Any]], *, policy: Mapping[str, Any]) -> dict[str, Any]:
    decisions = Counter(str(row.get("product_decision", "")) for row in rows)
    tiers = Counter(str(row.get("display_tier", "")) for row in rows)
    reasons = Counter(str(row.get("reason_code", "")) for row in rows)
    return {
        "schema_version": 1,
        "kind": "safe_triage_decision_export",
        "policy_status": policy.get("status", "missing"),
        "policy_reason_code": policy.get("reason_code"),
        "selected_threshold": policy.get("selected_threshold"),
        "product_claim_allowed": bool(policy.get("product_claim_allowed")),
        "rows": len(rows),
        "decision_counts": dict(sorted(decisions.items())),
        "display_tier_counts": dict(sorted(tiers.items())),
        "reason_counts": dict(sorted(reasons.items())),
    }


def write_safe_triage_report(path: Path, summary: Mapping[str, Any]) -> None:
    lines = [
        "# Safe triage decision export",
        "",
        f"- Policy status: `{summary.get('policy_status')}`",
        f"- Policy reason: `{summary.get('policy_reason_code')}`",
        f"- Selected threshold: {summary.get('selected_threshold')}",
        f"- Product claim allowed: `{summary.get('product_claim_allowed')}`",
        f"- Rows: {summary.get('rows')}",
        "",
        "## Product decisions",
        "",
    ]
    for key, value in dict(summary.get("decision_counts", {})).items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Reasons", ""])
    for key, value in dict(summary.get("reason_counts", {})).items():
        lines.append(f"- `{key}`: {value}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def export_safe_triage(
    *,
    audit_path: Path,
    policy_path: Path,
    reliability_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    policy = _read_json(policy_path)
    rows = export_safe_triage_rows(
        audit_rows=_read_csv(audit_path),
        policy=policy,
        reliability_rows=_read_csv(reliability_path),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_safe_triage_csv(output_dir / "safe-triage-decisions.csv", rows)
    summary = summarize_safe_triage_rows(rows, policy=policy)
    (output_dir / "safe-triage-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_safe_triage_report(output_dir / "safe-triage-report.md", summary)
    return summary | {"output_dir": str(output_dir)}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-export-safe-triage-decisions")
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--reliability", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = export_safe_triage(
        audit_path=args.audit,
        policy_path=args.policy,
        reliability_path=args.reliability,
        output_dir=args.output_dir,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
