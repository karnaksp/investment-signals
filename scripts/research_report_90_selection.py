#!/usr/bin/env python3
"""Write a concise report about selective 90% signal readiness."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


MIN_ROWS = 300
MIN_SESSIONS = 30
TARGET_SUCCESS_RATE = 0.90
TARGET_LOWER_BOUND = 0.75

RU_BAND_LABELS = {
    "skip": "пропустить, недостаточно уверенности",
    "weak_observation": "слабое наблюдение",
    "working_hypothesis": "рабочая гипотеза",
    "strong_signal": "сильный сигнал",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _int_value(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float_value(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _bool_value(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _pct(value: object) -> str:
    return f"{_float_value(value) * 100:.2f}%"


def _successes_needed_for_90(selected_rows: int, success_count: int) -> int:
    if selected_rows <= 0:
        return 0
    return max(0, math.ceil(TARGET_SUCCESS_RATE * selected_rows) - success_count)


def _threshold_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        eligible = _int_value(row.get("eligible_rows"))
        selected = _int_value(row.get("selected_rows"))
        skipped = _int_value(row.get("skipped_rows"))
        success_count = _int_value(row.get("success_count"))
        sessions = _int_value(row.get("sessions"))
        success_rate = _float_value(row.get("success_rate"))
        lower = _float_value(row.get("wilson_lower_95"))
        up_decisions = _int_value(row.get("up_decisions"))
        down_decisions = _int_value(row.get("down_decisions"))
        inverse_decisions = _int_value(row.get("inverse_decisions"))
        result.append(
            {
                "threshold": _float_value(row.get("threshold")),
                "eligible_rows": eligible,
                "selected_rows": selected,
                "skipped_rows": skipped,
                "up_decisions": up_decisions,
                "down_decisions": down_decisions,
                "inverse_decisions": inverse_decisions,
                "selected_share": selected / eligible if eligible else 0.0,
                "skipped_share": skipped / eligible if eligible else 0.0,
                "success_count": success_count,
                "success_rate": success_rate,
                "wilson_lower_95": lower,
                "sessions": sessions,
                "mean_selected_result_bps": _float_value(row.get("mean_selected_result_bps")),
                "missing_successes_to_90_current_rows": (
                    _successes_needed_for_90(selected, success_count) if selected > 0 else None
                ),
                "missing_rows_to_minimum": max(0, MIN_ROWS - selected),
                "accepted_for_research": _bool_value(row.get("accepted_research")),
                "passes_observed_90": success_rate >= TARGET_SUCCESS_RATE,
                "passes_product_lower_bound": lower >= TARGET_LOWER_BOUND,
                "passes_sample_gate": selected >= MIN_ROWS and sessions >= MIN_SESSIONS,
            }
        )
    return result


def _best_by(rows: Sequence[Mapping[str, Any]], key: str) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _int_value(row.get("selected_rows")) > 0]
    if not candidates:
        return None
    return max(candidates, key=lambda row: (_float_value(row.get(key)), _int_value(row.get("selected_rows"))))


def _top_precision_candidates(rows: Sequence[Mapping[str, Any]], limit: int = 10) -> list[dict[str, Any]]:
    candidates = [dict(row) for row in rows if _int_value(row.get("evaluation_rows")) > 0]
    candidates.sort(
        key=lambda row: (
            _bool_value(row.get("can_reach_90pct_at_min_rows")),
            _float_value(row.get("evaluation_mean_result_bps")) > 0,
            _float_value(row.get("evaluation_success_rate")),
            _float_value(row.get("evaluation_wilson_lower_95")),
            _int_value(row.get("evaluation_rows")),
        ),
        reverse=True,
    )
    return candidates[:limit]


def _inverse_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    inverse_rows = [
        row
        for row in rows
        if "inverse" in str(row.get("rule", "")).lower()
        or _int_value(row.get("inverse_decisions")) > 0
    ]
    best = None
    if inverse_rows:
        best = max(
            inverse_rows,
            key=lambda row: (
                _float_value(row.get("success_rate")),
                _float_value(row.get("wilson_lower_95")),
                _float_value(row.get("mean_selected_result_bps")),
                _int_value(row.get("selected_rows")),
            ),
        )
    return {
        "rows": len(inverse_rows),
        "best": dict(best) if best else None,
    }


def _guard_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(str(row.get("status", "")) for row in rows if row.get("status"))
    guard_term_counts = Counter(str(row.get("guard_terms", "")) for row in rows if row.get("guard_terms"))
    candidates = [dict(row) for row in rows if _int_value(row.get("kept_rows")) > 0]
    candidates.sort(
        key=lambda row: (
            _bool_value(row.get("accepted_shadow")),
            _float_value(row.get("precision_gain")),
            _float_value(row.get("kept_success_rate")),
            _float_value(row.get("kept_wilson_lower_95")),
            _float_value(row.get("removed_failure_share")),
            _int_value(row.get("kept_rows")),
        ),
        reverse=True,
    )
    return {
        "rows": len(rows),
        "accepted_shadow": sum(1 for row in rows if _bool_value(row.get("accepted_shadow"))),
        "status_counts": dict(sorted(status_counts.items())),
        "guard_term_counts": dict(sorted(guard_term_counts.items())),
        "best_precision_gain": max((_float_value(row.get("precision_gain")) for row in rows), default=0.0),
        "best_success_rate_after_guard": max((_float_value(row.get("kept_success_rate")) for row in rows), default=0.0),
        "top_guards": candidates[:5],
    }


def _confidence_band_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("scope")) != "confidence_band":
            continue
        rule = str(row.get("rule", ""))
        selected = _int_value(row.get("selected_rows"))
        success_count = _int_value(row.get("success_count"))
        sessions = _int_value(row.get("sessions"))
        success_rate = _float_value(row.get("observed_success_rate"))
        lower = _float_value(row.get("wilson_lower_95"))
        product_allowed = _bool_value(row.get("product_90_allowed"))
        shadow_allowed = _bool_value(row.get("shadow_allowed"))
        result.append(
            {
                "band": rule,
                "label_ru": RU_BAND_LABELS.get(rule, rule),
                "min_confidence": _float_value(row.get("min_confidence")),
                "max_confidence": _float_value(row.get("max_confidence")),
                "selected_rows": selected,
                "sessions": sessions,
                "success_count": success_count,
                "observed_success_rate": success_rate,
                "wilson_lower_95": lower,
                "mean_model_confidence": _float_value(row.get("mean_model_confidence")),
                "mean_result_bps": _float_value(row.get("mean_result_bps")),
                "shadow_allowed": shadow_allowed,
                "product_90_allowed": product_allowed,
                "safe_runtime_action": row.get("safe_runtime_action", "skip"),
                "missing_successes_to_90_current_rows": (
                    _successes_needed_for_90(selected, success_count) if selected > 0 else None
                ),
                "passes_sample_gate": selected >= MIN_ROWS and sessions >= MIN_SESSIONS,
                "passes_observed_90": success_rate >= TARGET_SUCCESS_RATE,
                "passes_product_lower_bound": lower >= TARGET_LOWER_BOUND,
            }
        )
    return result


def build_selection_report(run_dir: Path) -> dict[str, Any]:
    model_results = _read_json(run_dir / "model-results.json")
    policy = _read_json(run_dir / "decision-policy.json")
    threshold_source = _read_csv(run_dir / "confidence-threshold-report.csv")
    threshold_rows = _threshold_rows(threshold_source)
    confidence_band_rows = _confidence_band_rows(_read_csv(run_dir / "confidence-reliability-report.csv"))
    precision_rows = _read_csv(run_dir / "precision-scout-candidates.csv")
    frontier_rows = _read_csv(run_dir / "selective-frontier.csv")
    guard_rows = _read_csv(run_dir / "false-positive-guards.csv")
    viability = Counter(str(row.get("proof_viability", "")) for row in precision_rows if row.get("proof_viability"))
    next_actions = Counter(str(row.get("proof_next_action", "")) for row in precision_rows if row.get("proof_next_action"))
    best_success = _best_by(threshold_rows, "success_rate")
    best_lower = _best_by(threshold_rows, "wilson_lower_95")
    accepted_thresholds = [row for row in threshold_rows if row["accepted_for_research"]]
    return {
        "schema_version": 1,
        "kind": "selection_90_report",
        "run_dir": str(run_dir),
        "run_id": model_results.get("run_id", run_dir.name),
        "dataset_rows": model_results.get("dataset_rows"),
        "validation_sessions": model_results.get("validation_sessions"),
        "policy_status": policy.get("status", "missing"),
        "policy_reason_code": policy.get("reason_code"),
        "product_claim_allowed": bool(policy.get("product_claim_allowed")),
        "target": {
            "success_rate": TARGET_SUCCESS_RATE,
            "minimum_rows": MIN_ROWS,
            "minimum_sessions": MIN_SESSIONS,
            "minimum_lower_bound": TARGET_LOWER_BOUND,
        },
        "threshold_rows": threshold_rows,
        "confidence_band_rows": confidence_band_rows,
        "best_observed_success_threshold": best_success,
        "best_lower_bound_threshold": best_lower,
        "accepted_threshold_count": len(accepted_thresholds),
        "precision_scout": {
            "rows": len(precision_rows),
            "viability_counts": dict(sorted(viability.items())),
            "next_action_counts": dict(sorted(next_actions.items())),
            "top_candidates": _top_precision_candidates(precision_rows),
        },
        "false_positive_guards": _guard_summary(guard_rows),
        "inverse_hypotheses": _inverse_summary(frontier_rows),
        "conclusion": (
            "ready_for_shadow"
            if accepted_thresholds and bool(policy.get("product_claim_allowed"))
            else "not_ready_keep_default_skip"
        ),
    }


def write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    best_success = report.get("best_observed_success_threshold")
    best_lower = report.get("best_lower_bound_threshold")
    inverse = report.get("inverse_hypotheses") if isinstance(report.get("inverse_hypotheses"), Mapping) else {}
    lines = [
        "# Отбор сигналов для цели 90%",
        "",
        f"- Запуск исследования: `{report.get('run_id')}`",
        f"- Строк в наборе данных: {report.get('dataset_rows')}",
        f"- Торговых дней в проверке: {report.get('validation_sessions')}",
        f"- Политика решений: `{report.get('policy_status')}` / `{report.get('policy_reason_code')}`",
        f"- Продуктовый вывод разрешён: {'да' if report.get('product_claim_allowed') else 'нет'}",
        f"- Итог: `{report.get('conclusion')}`",
        "",
        "## Что показывать пользователю сейчас",
        "",
        "| Решение | Статус | Почему |",
        "|---|---|---|",
        "| «ожидается рост» | нельзя показывать как торговый сигнал | нет проверенного порога уверенности и нет правила, прошедшего 300 случаев / 30 торговых дней / нижнюю границу надёжности |",
        "| «ожидается снижение» | нельзя показывать как торговый сигнал | обратные гипотезы найдены, но они не прошли доказательный порог |",
        "| «пропустить, недостаточно уверенности» | безопасное действие по умолчанию | текущие свечные признаки не отделяют сильные случаи от слабых достаточно надёжно |",
        "",
        "## Точность против количества сигналов",
        "",
        "| Порог | Осталось | Рост | Снижение | Обратных | Пропущено | Успешных | Доля успеха | Нижняя граница | Торговых дней | Не хватает успехов до 90% |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report.get("threshold_rows", []):
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {threshold:.2f} | {selected} | {up} | {down} | {inverse} | {skipped} ({skipped_pct}) | {successes} | {success_rate} | {lower} | {sessions} | {missing} |".format(
                threshold=float(row.get("threshold", 0.0) or 0.0),
                selected=row.get("selected_rows"),
                up=row.get("up_decisions"),
                down=row.get("down_decisions"),
                inverse=row.get("inverse_decisions"),
                skipped=row.get("skipped_rows"),
                skipped_pct=_pct(row.get("skipped_share")),
                successes=row.get("success_count"),
                success_rate=_pct(row.get("success_rate")),
                lower=_pct(row.get("wilson_lower_95")),
                sessions=row.get("sessions"),
                missing=(
                    row.get("missing_successes_to_90_current_rows")
                    if row.get("missing_successes_to_90_current_rows") is not None
                    else "—"
                ),
            )
        )
    lines.extend(["", "## Лучшие найденные пороги", ""])
    if isinstance(best_success, Mapping):
        lines.extend(
            [
                f"- Максимальная наблюдаемая доля успеха: {_pct(best_success.get('success_rate'))} при пороге {best_success.get('threshold')}.",
                f"- Осталось сигналов: {best_success.get('selected_rows')}; пропущено: {best_success.get('skipped_rows')} ({_pct(best_success.get('skipped_share'))}).",
                f"- Нижняя граница надёжности: {_pct(best_success.get('wilson_lower_95'))}.",
            ]
        )
    if isinstance(best_lower, Mapping):
        lines.extend(
            [
                f"- Лучшая нижняя граница надёжности: {_pct(best_lower.get('wilson_lower_95'))} при пороге {best_lower.get('threshold')}.",
                f"- Наблюдаемая доля успеха там: {_pct(best_lower.get('success_rate'))}.",
            ]
        )
    lines.extend(
        [
            "",
            "## Продуктовые диапазоны уверенности",
            "",
            "| Диапазон | Как показывать | Сигналов | Успешных | Доля успеха | Нижняя граница | Безопасное действие |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in report.get("confidence_band_rows", []):
        if not isinstance(row, Mapping):
            continue
        max_conf = float(row.get("max_confidence", 0.0) or 0.0)
        range_text = (
            f"{float(row.get('min_confidence', 0.0) or 0.0):.0%}+"
            if max_conf > 1.0
            else f"{float(row.get('min_confidence', 0.0) or 0.0):.0%}–{max_conf:.0%}"
        )
        lines.append(
            "| {range_text} | {label} | {selected} | {successes} | {success_rate} | {lower} | `{action}` |".format(
                range_text=range_text,
                label=row.get("label_ru"),
                selected=row.get("selected_rows"),
                successes=row.get("success_count"),
                success_rate=_pct(row.get("observed_success_rate")),
                lower=_pct(row.get("wilson_lower_95")),
                action=row.get("safe_runtime_action"),
            )
        )
    lines.extend(["", "## Редкие правила", ""])
    scout = report.get("precision_scout") if isinstance(report.get("precision_scout"), Mapping) else {}
    lines.append(f"- Проверено кандидатов: {scout.get('rows')}")
    lines.append(f"- Жизнеспособность: `{json.dumps(scout.get('viability_counts', {}), ensure_ascii=False, sort_keys=True)}`")
    lines.append(f"- Следующие действия: `{json.dumps(scout.get('next_action_counts', {}), ensure_ascii=False, sort_keys=True)}`")
    lines.extend(["", "### Лучшие кандидаты", ""])
    for row in scout.get("top_candidates", [])[:5]:
        if not isinstance(row, Mapping):
            continue
        lines.extend(
            [
                f"- `{row.get('rule')}`",
                f"  - проверочных случаев: {row.get('evaluation_rows')}; доля успеха: {_pct(row.get('evaluation_success_rate'))}; нижняя граница: {_pct(row.get('evaluation_wilson_lower_95'))}; результат после издержек: {row.get('evaluation_mean_result_bps')} базисных пунктов;",
                f"  - статус: `{row.get('status')}`; доказуемость: `{row.get('proof_viability')}`; нужно будущих успехов к 300 случаям: {row.get('additional_successes_needed_for_90pct_at_min_rows')}.",
            ]
        )
    lines.extend(["", "## Обратные гипотезы", ""])
    lines.append(f"- Найдено строк с обратной гипотезой: {inverse.get('rows', 0)}")
    best_inverse = inverse.get("best") if isinstance(inverse.get("best"), Mapping) else None
    if best_inverse:
        lines.extend(
            [
                f"- Лучшая обратная строка: `{best_inverse.get('rule')}`",
                f"- Осталось сигналов: {best_inverse.get('selected_rows')}; доля успеха: {_pct(best_inverse.get('success_rate'))}; нижняя граница: {_pct(best_inverse.get('wilson_lower_95'))}; результат после издержек: {best_inverse.get('mean_selected_result_bps')} базисных пунктов.",
            ]
        )
    lines.extend(
        [
            "",
            "## Исключение ложных срабатываний",
            "",
        ]
    )
    guard_summary = report.get("false_positive_guards") if isinstance(report.get("false_positive_guards"), Mapping) else {}
    lines.append(f"- Проверено исключающих условий: {guard_summary.get('rows', 0)}")
    lines.append(f"- Условий, прошедших теневой порог: {guard_summary.get('accepted_shadow', 0)}")
    lines.append(f"- Лучший прирост доли успеха: {_pct(guard_summary.get('best_precision_gain'))}")
    lines.append(f"- Лучшая доля успеха после исключения: {_pct(guard_summary.get('best_success_rate_after_guard'))}")
    lines.append(f"- Статусы: `{json.dumps(guard_summary.get('status_counts', {}), ensure_ascii=False, sort_keys=True)}`")
    lines.append(f"- Число условий в исключении: `{json.dumps(guard_summary.get('guard_term_counts', {}), ensure_ascii=False, sort_keys=True)}`")
    top_guards = guard_summary.get("top_guards", []) if isinstance(guard_summary.get("top_guards"), list) else []
    if top_guards:
        lines.extend(
            [
                "",
                "| Порог | Что исключить | Осталось | Доля успеха | Нижняя граница | Прирост | Статус |",
                "|---:|---|---:|---:|---:|---:|---|",
            ]
        )
        for row in top_guards:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "| {threshold:.2f} | `{guard}` | {kept_rows} | {success_rate} | {lower} | {gain} | `{status}` |".format(
                    threshold=_float_value(row.get("threshold")),
                    guard=row.get("guard"),
                    kept_rows=row.get("kept_rows"),
                    success_rate=_pct(row.get("kept_success_rate")),
                    lower=_pct(row.get("kept_wilson_lower_95")),
                    gain=_pct(row.get("precision_gain")),
                    status=row.get("status"),
                )
            )
    lines.extend(
        [
            "",
            "## Вывод",
            "",
            "Сейчас нельзя показывать пользователю «ожидается рост» или «ожидается снижение» как доказанный торговый сигнал. Безопасное действие — «пропустить, недостаточно уверенности». Немедленное продолжение исследования использует уже доступные межрыночные показатели и разметку первого достижения ценовой границы. Стакан накапливается только фоном и не задерживает выпуск продукта.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_selection_report(run_dir: Path, output_dir: Path) -> dict[str, Any]:
    report = build_selection_report(run_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "selection-90-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "selection-90-report.md", report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-report-90-selection")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/selection_90/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = write_selection_report(args.run_dir, args.output_dir)
    print(
        json.dumps(
            {
                "conclusion": report["conclusion"],
                "product_claim_allowed": report["product_claim_allowed"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
