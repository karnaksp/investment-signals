#!/usr/bin/env python3
"""Extract row-level audit evidence for candidate watchlist rules."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


OUTPUT_FIELDS = (
    "candidate_id",
    "candidate_scope",
    "candidate_rule",
    "candidate_status",
    "rank",
    "row_id",
    "ticker",
    "source_event_at",
    "trading_day",
    "horizon_seconds",
    "signal_type",
    "original_direction",
    "session_bucket",
    "volatility_bucket",
    "signal_count_bucket",
    "combo_key_300s",
    "recent_signal_count_60s",
    "recent_signal_count_300s",
    "recent_signal_count_900s",
    "up_confidence",
    "down_confidence",
    "frontier_decision",
    "frontier_decision_relation",
    "frontier_success",
    "frontier_confidence",
    "frontier_result_bps",
    "policy_decision",
    "policy_success",
    "policy_result_bps",
    "confidence_band",
    "forward_return_bps",
)


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


def parse_candidate_rule(rule: object) -> dict[str, str]:
    text = str(rule or "").strip()
    if not text or text == "all":
        return {}
    result: dict[str, str] = {}
    for part in text.split("|"):
        key, separator, value = part.strip().partition("=")
        if not separator:
            raise ValueError(f"invalid candidate rule part: {part!r}")
        result[key.strip()] = value.strip()
    return result


def _audit_field_for_rule_key(key: str) -> str:
    if key == "decision":
        return "frontier_decision"
    if key == "decision_relation":
        return "frontier_decision_relation"
    return key


def _row_matches_rule(row: Mapping[str, Any], predicates: Mapping[str, str]) -> bool:
    for key, expected in predicates.items():
        actual = row.get(_audit_field_for_rule_key(key))
        if str(actual) != expected:
            return False
    return True


def _float_or_zero(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[float, str]:
    return (
        _float_or_zero(row.get("frontier_confidence")),
        str(row.get("row_id", "")),
    )


def extract_candidate_audit_rows(
    *,
    watchlist_path: Path,
    audit_path: Path,
) -> list[dict[str, Any]]:
    watchlist_rows = _read_csv(watchlist_path)
    audit_rows = _read_csv(audit_path)
    result: list[dict[str, Any]] = []

    for candidate in watchlist_rows:
        scope = str(candidate.get("scope", ""))
        rule = str(candidate.get("rule", ""))
        if not scope and not rule and not candidate.get("candidate_id"):
            continue
        predicates = parse_candidate_rule(rule)
        candidate_id = str(candidate.get("candidate_id") or stable_candidate_id(scope, rule))
        selected_rows = _int_or_zero(candidate.get("selected_rows"))
        if selected_rows <= 0:
            continue
        matches = [row for row in audit_rows if _row_matches_rule(row, predicates)]
        selected = sorted(matches, key=_candidate_sort_key, reverse=True)
        selected = selected[:selected_rows]
        for rank, row in enumerate(selected, start=1):
            result.append(
                {
                    "candidate_id": candidate_id,
                    "candidate_scope": scope,
                    "candidate_rule": rule,
                    "candidate_status": candidate.get("status", ""),
                    "rank": rank,
                    "row_id": row.get("row_id", ""),
                    "ticker": row.get("ticker", ""),
                    "source_event_at": row.get("source_event_at", ""),
                    "trading_day": row.get("trading_day", ""),
                    "horizon_seconds": row.get("horizon_seconds", ""),
                    "signal_type": row.get("signal_type", ""),
                    "original_direction": row.get("original_direction", ""),
                    "session_bucket": row.get("session_bucket", ""),
                    "volatility_bucket": row.get("volatility_bucket", ""),
                    "signal_count_bucket": row.get("signal_count_bucket", ""),
                    "combo_key_300s": row.get("combo_key_300s", ""),
                    "recent_signal_count_60s": row.get("recent_signal_count_60s", ""),
                    "recent_signal_count_300s": row.get("recent_signal_count_300s", ""),
                    "recent_signal_count_900s": row.get("recent_signal_count_900s", ""),
                    "up_confidence": row.get("up_confidence", ""),
                    "down_confidence": row.get("down_confidence", ""),
                    "frontier_decision": row.get("frontier_decision", ""),
                    "frontier_decision_relation": row.get("frontier_decision_relation", ""),
                    "frontier_success": row.get("frontier_success", ""),
                    "frontier_confidence": row.get("frontier_confidence", ""),
                    "frontier_result_bps": row.get("frontier_result_bps", ""),
                    "policy_decision": row.get("decision", ""),
                    "policy_success": row.get("success", ""),
                    "policy_result_bps": row.get("decision_result_bps", ""),
                    "confidence_band": row.get("confidence_band", ""),
                    "forward_return_bps": row.get("forward_return_bps", ""),
                }
            )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-extract-candidate-audit-rows")
    parser.add_argument("--watchlist", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = extract_candidate_audit_rows(watchlist_path=args.watchlist, audit_path=args.audit)
    _write_csv(args.output, rows)
    print(json.dumps({"output": str(args.output), "rows": len(rows)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
