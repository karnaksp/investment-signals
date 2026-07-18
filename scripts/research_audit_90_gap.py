#!/usr/bin/env python3
"""Audit how far current selective candidates are from the 90% objective."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


TARGET_SUCCESS_RATE = 0.90
TARGET_LOWER_BOUND = 0.75
MIN_ROWS = 300
MIN_SESSIONS = 30


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


def _int_or_zero(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _ceil_successes(rows: int, rate: float = TARGET_SUCCESS_RATE) -> int:
    return math.ceil(rows * rate) if rows > 0 else 0


def _missing_successes(successes: int, rows: int) -> int:
    return max(0, _ceil_successes(rows) - successes)


def _additional_successes_at_min_rows(successes: int, rows: int) -> dict[str, Any]:
    denominator = max(rows, MIN_ROWS)
    required_successes = _ceil_successes(denominator)
    missing_rows = max(0, MIN_ROWS - rows)
    additional_successes = max(0, required_successes - successes)
    return {
        "missing_rows_to_minimum": missing_rows,
        "additional_successes_needed_at_min_rows": additional_successes,
        "allowed_future_failures_at_min_rows": max(0, missing_rows - additional_successes),
        "required_future_success_rate_at_min_rows": (
            additional_successes / missing_rows
            if missing_rows
            else 0.0
        ),
        "can_reach_90_at_min_rows": additional_successes <= missing_rows,
    }


def _gap_row(
    *,
    source: str,
    rule: str,
    rows: int,
    sessions: int,
    successes: int,
    success_rate: float,
    lower_bound: float,
    mean_result_bps: float,
) -> dict[str, Any]:
    future = _additional_successes_at_min_rows(successes, rows)
    blockers: list[str] = []
    if rows < MIN_ROWS:
        blockers.append("sample_size")
    if sessions < MIN_SESSIONS:
        blockers.append("trading_days")
    if success_rate < TARGET_SUCCESS_RATE:
        blockers.append("success_rate")
    if lower_bound < TARGET_LOWER_BOUND:
        blockers.append("lower_bound")
    if mean_result_bps <= 0:
        blockers.append("positive_after_costs")
    if not bool(future["can_reach_90_at_min_rows"]):
        blockers.append("cannot_reach_90_at_min_rows")
    accepted = not blockers
    return {
        "source": source,
        "rule": rule,
        "rows": rows,
        "sessions": sessions,
        "success_count": successes,
        "success_rate": success_rate,
        "wilson_lower_95": lower_bound,
        "mean_result_bps": mean_result_bps,
        "success_rate_gap_to_90": max(0.0, TARGET_SUCCESS_RATE - success_rate),
        "lower_bound_gap_to_75": max(0.0, TARGET_LOWER_BOUND - lower_bound),
        "missing_successes_to_90_current_rows": _missing_successes(successes, rows),
        **future,
        "accepted_shadow": accepted,
        "product_claim_allowed": False,
        "blockers": ",".join(blockers) if blockers else "",
        "next_action": _next_action(blockers, future, mean_result_bps),
    }


def _next_action(blockers: Sequence[str], future: Mapping[str, Any], mean_result_bps: float) -> str:
    blocker_set = set(blockers)
    if not blockers:
        return "shadow_validate"
    if "cannot_reach_90_at_min_rows" in blocker_set:
        return "retire_or_redefine_rule"
    if mean_result_bps <= 0:
        return "reject_until_positive_after_costs"
    required_future = _float_or_zero(future.get("required_future_success_rate_at_min_rows"))
    if required_future >= 0.95:
        return "needs_new_features_not_more_thresholding"
    if {"sample_size", "trading_days"} & blocker_set:
        return "forward_holdout"
    return "refine_features"


def _threshold_gap_rows(rows: Sequence[Mapping[str, str]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        selected = _int_or_zero(row.get("selected_rows"))
        if selected <= 0:
            continue
        result.append(
            _gap_row(
                source="confidence_threshold",
                rule=f"threshold>={_float_or_zero(row.get('threshold')):.2f}",
                rows=selected,
                sessions=_int_or_zero(row.get("sessions")),
                successes=_int_or_zero(row.get("success_count")),
                success_rate=_float_or_zero(row.get("success_rate")),
                lower_bound=_float_or_zero(row.get("wilson_lower_95")),
                mean_result_bps=_float_or_zero(row.get("mean_selected_result_bps")),
            )
        )
    return result


def _precision_gap_rows(rows: Sequence[Mapping[str, str]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        selected = _int_or_zero(row.get("evaluation_rows"))
        if selected <= 0:
            continue
        result.append(
            _gap_row(
                source="precision_scout",
                rule=str(row.get("rule", "")),
                rows=selected,
                sessions=_int_or_zero(row.get("evaluation_sessions")),
                successes=_int_or_zero(row.get("evaluation_success_count")),
                success_rate=_float_or_zero(row.get("evaluation_success_rate")),
                lower_bound=_float_or_zero(row.get("evaluation_wilson_lower_95")),
                mean_result_bps=_float_or_zero(row.get("evaluation_mean_result_bps")),
            )
        )
    return result


def _guard_gap_rows(rows: Sequence[Mapping[str, str]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        selected = _int_or_zero(row.get("kept_rows"))
        if selected <= 0:
            continue
        result.append(
            _gap_row(
                source="false_positive_guard",
                rule=str(row.get("guard", "")),
                rows=selected,
                sessions=_int_or_zero(row.get("kept_sessions")),
                successes=_int_or_zero(row.get("kept_success_count")),
                success_rate=_float_or_zero(row.get("kept_success_rate")),
                lower_bound=_float_or_zero(row.get("kept_wilson_lower_95")),
                mean_result_bps=_float_or_zero(row.get("kept_mean_result_bps")),
            )
        )
    return result


def build_gap_report(run_dir: Path) -> dict[str, Any]:
    rows = (
        _threshold_gap_rows(_read_csv(run_dir / "confidence-threshold-report.csv"))
        + _precision_gap_rows(_read_csv(run_dir / "precision-scout-candidates.csv"))
        + _guard_gap_rows(_read_csv(run_dir / "false-positive-guards.csv"))
    )
    rows.sort(
        key=lambda row: (
            bool(row["accepted_shadow"]),
            float(row["success_rate"]),
            float(row["wilson_lower_95"]),
            int(row["rows"]),
        ),
        reverse=True,
    )
    best_by_success = max(rows, key=lambda row: (float(row["success_rate"]), int(row["rows"])), default=None)
    blocker_counts = Counter()
    action_counts = Counter()
    for row in rows:
        for blocker in str(row.get("blockers", "")).split(","):
            if blocker:
                blocker_counts[blocker] += 1
        action_counts[str(row.get("next_action", ""))] += 1
    return {
        "schema_version": 1,
        "kind": "gap_to_90_audit",
        "run_dir": str(run_dir),
        "target": {
            "success_rate": TARGET_SUCCESS_RATE,
            "minimum_rows": MIN_ROWS,
            "minimum_sessions": MIN_SESSIONS,
            "minimum_lower_bound": TARGET_LOWER_BOUND,
        },
        "summary": {
            "candidate_rows": len(rows),
            "accepted_shadow": sum(1 for row in rows if row["accepted_shadow"]),
            "best_success_rate": best_by_success.get("success_rate") if best_by_success else None,
            "best_rule": best_by_success.get("rule") if best_by_success else None,
            "best_source": best_by_success.get("source") if best_by_success else None,
            "blocker_counts": dict(sorted(blocker_counts.items())),
            "next_action_counts": dict(sorted(action_counts.items())),
        },
        "rows": rows[:500],
        "status": "not_ready" if not any(row["accepted_shadow"] for row in rows) else "shadow_ready",
    }


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = [
        "source",
        "rule",
        "rows",
        "sessions",
        "success_count",
        "success_rate",
        "wilson_lower_95",
        "mean_result_bps",
        "success_rate_gap_to_90",
        "lower_bound_gap_to_75",
        "missing_successes_to_90_current_rows",
        "missing_rows_to_minimum",
        "additional_successes_needed_at_min_rows",
        "allowed_future_failures_at_min_rows",
        "required_future_success_rate_at_min_rows",
        "can_reach_90_at_min_rows",
        "accepted_shadow",
        "product_claim_allowed",
        "blockers",
        "next_action",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _pct(value: object) -> str:
    return f"{_float_or_zero(value) * 100:.2f}%"


def write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    summary = report.get("summary") if isinstance(report.get("summary"), Mapping) else {}
    lines = [
        "# Разрыв до цели 90%",
        "",
        f"- Статус: `{report.get('status')}`",
        f"- Проверено кандидатов: {summary.get('candidate_rows')}",
        f"- Кандидатов для теневого режима: {summary.get('accepted_shadow')}",
        f"- Лучшая доля успеха: {_pct(summary.get('best_success_rate'))}",
        f"- Лучшее правило: `{summary.get('best_rule')}`",
        f"- Источник лучшего правила: `{summary.get('best_source')}`",
        f"- Причины блокировки: `{json.dumps(summary.get('blocker_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        f"- Следующие действия: `{json.dumps(summary.get('next_action_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        "",
        "## Ближайшие кандидаты",
        "",
        "| Источник | Правило | Строк | Доля успеха | Разрыв до 90% | Нижняя граница | Разрыв нижней границы | Не хватает успехов | Следующее действие |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in report.get("rows", [])[:10]:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {source} | `{rule}` | {rows} | {success_rate} | {success_gap} | {lower} | {lower_gap} | {missing} | `{action}` |".format(
                source=row.get("source"),
                rule=row.get("rule"),
                rows=row.get("rows"),
                success_rate=_pct(row.get("success_rate")),
                success_gap=_pct(row.get("success_rate_gap_to_90")),
                lower=_pct(row.get("wilson_lower_95")),
                lower_gap=_pct(row.get("lower_bound_gap_to_75")),
                missing=row.get("missing_successes_to_90_current_rows"),
                action=row.get("next_action"),
            )
        )
    lines.extend(
        [
            "",
            "## Вывод",
            "",
            "Если лучший кандидат остаётся далеко от 90%, дальнейшее подкручивание порога не решает задачу. Нужно добавлять новые признаки или переносить кандидата в будущую проверку, если ему статистически ещё возможно добрать доказательства.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_gap_report(run_dir: Path, output_dir: Path) -> dict[str, Any]:
    report = build_gap_report(run_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "gap-to-90.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_csv(output_dir / "gap-to-90.csv", report["rows"])
    write_markdown(output_dir / "gap-to-90.md", report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-audit-90-gap")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/gap_90/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = write_gap_report(args.run_dir, args.output_dir)
    print(
        json.dumps(
            {
                "status": report["status"],
                "candidate_rows": report["summary"]["candidate_rows"],
                "accepted_shadow": report["summary"]["accepted_shadow"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
