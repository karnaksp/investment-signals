#!/usr/bin/env python3
"""Build the next research action plan for the selective 90% objective."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _int(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _need_microstructure(feature_coverage: Mapping[str, Any]) -> bool:
    coverage = feature_coverage.get("microstructure_value_coverage")
    if not isinstance(coverage, Mapping):
        return True
    return not bool(coverage.get("ready"))


def _candidate_priority(row: Mapping[str, Any]) -> tuple[object, ...]:
    action = str(row.get("next_action", ""))
    action_rank = {
        "needs_new_features_not_more_thresholding": 4,
        "forward_holdout": 3,
        "refine_features": 2,
        "reject_until_positive_after_costs": 1,
        "retire_or_redefine_rule": 0,
    }.get(action, 0)
    return (
        action_rank,
        _float(row.get("success_rate")),
        _float(row.get("wilson_lower_95")),
        _int(row.get("rows")),
    )


def build_next_action_plan(
    *,
    gap_audit: Mapping[str, Any],
    feature_coverage: Mapping[str, Any],
    live_status: Mapping[str, Any],
) -> dict[str, Any]:
    gap_rows = gap_audit.get("rows") if isinstance(gap_audit.get("rows"), list) else []
    ranked_rows = sorted(
        [dict(row) for row in gap_rows if isinstance(row, Mapping)],
        key=_candidate_priority,
        reverse=True,
    )
    action_counts = Counter(str(row.get("next_action", "")) for row in ranked_rows)
    blockers = Counter()
    for row in ranked_rows:
        for blocker in str(row.get("blockers", "")).split(","):
            if blocker:
                blockers[blocker] += 1
    microstructure_needed = _need_microstructure(feature_coverage)
    collection_status = str(live_status.get("status", "unknown"))
    top_new_feature_candidates = [
        row
        for row in ranked_rows
        if str(row.get("next_action")) == "needs_new_features_not_more_thresholding"
    ][:10]
    top_retire_candidates = [
        row
        for row in ranked_rows
        if str(row.get("next_action")) == "retire_or_redefine_rule"
    ][:10]
    next_actions: list[dict[str, Any]] = []
    if microstructure_needed:
        next_actions.append(
            {
                "priority": 1,
                "action": "collect_microstructure_holdout",
                "reason": "Текущие свечные признаки не отделяют 90% случаи; нужны спред, глубина и дисбаланс стакана до сигнала.",
                "status": collection_status,
                "done_when": "Есть минимум 300 строк сигналов с реальными orderbook_spread_bps, orderbook_total_qty и orderbook_imbalance_ratio.",
            }
        )
    if top_new_feature_candidates:
        next_actions.append(
            {
                "priority": 2,
                "action": "retest_near_candidates_with_microstructure",
                "reason": "Есть кандидаты, которые не проходят 90%, но gap-аудит считает, что им нужны новые признаки, а не дальнейшее подкручивание порогов.",
                "candidate_count": len(top_new_feature_candidates),
                "done_when": "После добавления стакана кандидат проходит 300 случаев, 30 дней, 90% успешности и нижнюю границу 75%.",
            }
        )
    if top_retire_candidates:
        next_actions.append(
            {
                "priority": 3,
                "action": "retire_or_redefine_weak_rules",
                "reason": "Большинство текущих правил математически нельзя довести до 90% на минимальных 300 случаях.",
                "candidate_count": len(top_retire_candidates),
                "done_when": "Слабые правила не попадают в продуктовые формулировки и остаются только в исследовательском архиве.",
            }
        )
    if not next_actions:
        next_actions.append(
            {
                "priority": 1,
                "action": "inspect_gap_audit_inputs",
                "reason": "Нет кандидатов или статусов, достаточных для построения плана.",
                "done_when": "gap-to-90.json содержит кандидатов с next_action.",
            }
        )
    return {
        "schema_version": 1,
        "kind": "selective_90_next_action_plan",
        "status": "waiting_for_microstructure" if microstructure_needed else "ready_to_retrain_with_microstructure",
        "collection_status": collection_status,
        "microstructure_needed": microstructure_needed,
        "summary": {
            "gap_candidates": len(ranked_rows),
            "action_counts": dict(sorted(action_counts.items())),
            "blocker_counts": dict(sorted(blockers.items())),
            "new_feature_candidates": len(top_new_feature_candidates),
            "retire_candidates_sampled": len(top_retire_candidates),
        },
        "next_actions": next_actions,
        "top_new_feature_candidates": top_new_feature_candidates,
        "top_retire_candidates": top_retire_candidates,
    }


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    fields = [
        "source",
        "rule",
        "rows",
        "sessions",
        "success_rate",
        "wilson_lower_95",
        "success_rate_gap_to_90",
        "missing_successes_to_90_current_rows",
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
    return f"{_float(value) * 100:.2f}%"


def write_markdown(path: Path, plan: Mapping[str, Any]) -> None:
    summary = plan.get("summary") if isinstance(plan.get("summary"), Mapping) else {}
    lines = [
        "# Следующие действия для цели 90%",
        "",
        f"- Статус: `{plan.get('status')}`",
        f"- Статус сбора стакана: `{plan.get('collection_status')}`",
        f"- Стакан нужен: {'да' if plan.get('microstructure_needed') else 'нет'}",
        f"- Кандидатов в gap-аудите: {summary.get('gap_candidates')}",
        f"- Действия по кандидатам: `{json.dumps(summary.get('action_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        f"- Блокирующие причины: `{json.dumps(summary.get('blocker_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        "",
        "## План",
        "",
    ]
    for item in plan.get("next_actions", []):
        if not isinstance(item, Mapping):
            continue
        lines.extend(
            [
                f"### {item.get('priority')}. `{item.get('action')}`",
                "",
                f"- Причина: {item.get('reason')}",
                f"- Готово, когда: {item.get('done_when')}",
                "",
            ]
        )
    lines.extend(
        [
            "## Кандидаты, которым нужны новые признаки",
            "",
            "| Источник | Правило | Строк | Доля успеха | Разрыв до 90% | Нижняя граница |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in plan.get("top_new_feature_candidates", [])[:10]:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {source} | `{rule}` | {rows} | {success_rate} | {gap} | {lower} |".format(
                source=row.get("source"),
                rule=row.get("rule"),
                rows=row.get("rows"),
                success_rate=_pct(row.get("success_rate")),
                gap=_pct(row.get("success_rate_gap_to_90")),
                lower=_pct(row.get("wilson_lower_95")),
            )
        )
    lines.extend(
        [
            "",
            "## Вывод",
            "",
            "Основной следующий шаг — дождаться и проверить данные стакана. Если после добавления стакана ближайшие кандидаты не приблизятся к 90%, эти правила нужно списывать или менять гипотезу, а не показывать пользователю направление.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_plan(
    *,
    gap_audit_path: Path,
    feature_coverage_path: Path,
    live_status_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    plan = build_next_action_plan(
        gap_audit=_read_json(gap_audit_path),
        feature_coverage=_read_json(feature_coverage_path),
        live_status=_read_json(live_status_path),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "next-actions-90.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "next-actions-90.md", plan)
    write_csv(output_dir / "new-feature-candidates.csv", plan["top_new_feature_candidates"])
    return plan


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-plan-90-next-actions")
    parser.add_argument("--gap-audit", type=Path, default=Path("var/research/gap_90/current/gap-to-90.json"))
    parser.add_argument("--feature-coverage", type=Path, default=Path("var/research/objective_90_features/current/feature-coverage.json"))
    parser.add_argument("--live-status", type=Path, default=Path("var/research/liquidity_holdout/current/live_status/live-status.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/next_actions_90/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    plan = write_plan(
        gap_audit_path=args.gap_audit,
        feature_coverage_path=args.feature_coverage,
        live_status_path=args.live_status,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": plan["status"],
                "microstructure_needed": plan["microstructure_needed"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
