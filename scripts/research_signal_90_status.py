#!/usr/bin/env python3
"""Summarize whether the research evidence supports a 90% selected-signal claim."""

from __future__ import annotations

import argparse
import csv
import json
import shlex
from pathlib import Path
from typing import Any, Mapping, Sequence

MIN_MICROSTRUCTURE_ROWS = 300
MIN_MICROSTRUCTURE_SESSIONS = 30


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _non_empty_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if any(value not in {None, ""} for value in row.values())]


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


def _bool_value(value: object) -> bool:
    return str(value).lower() in {"1", "true", "yes"}


def _best_threshold(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _int_or_zero(row.get("selected_rows")) > 0]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("accepted_research")),
            _float_or_zero(row.get("success_rate")),
            _float_or_zero(row.get("wilson_lower_95")),
            _float_or_zero(row.get("mean_selected_result_bps")),
            _int_or_zero(row.get("selected_rows")),
        ),
    )


def _best_state(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _int_or_zero(row.get("evaluation_rows")) > 0]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("accepted_shadow")),
            _float_or_zero(row.get("evaluation_success_rate")),
            _float_or_zero(row.get("evaluation_wilson_lower_95")),
            _float_or_zero(row.get("evaluation_mean_result_bps")),
            _int_or_zero(row.get("evaluation_rows")),
        ),
    )


def _best_product_relevant_state(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _int_or_zero(row.get("evaluation_rows")) > 0]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("accepted_shadow")),
            _bool_value(row.get("can_reach_90pct_at_min_rows")),
            _float_or_zero(row.get("evaluation_mean_result_bps")) > 0,
            str(row.get("status")) == "watch_only",
            _bool_value(row.get("discovery_gate_passed")),
            _float_or_zero(row.get("evaluation_success_rate")),
            _float_or_zero(row.get("evaluation_wilson_lower_95")),
            _int_or_zero(row.get("evaluation_sessions")),
            _int_or_zero(row.get("evaluation_rows")),
            _float_or_zero(row.get("evaluation_mean_result_bps")),
        ),
    )


def _best_reliability_band(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        dict(row)
        for row in rows
        if _int_or_zero(row.get("selected_rows")) > 0
        and str(row.get("scope")) == "confidence_band"
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("product_90_allowed")),
            _bool_value(row.get("shadow_allowed")),
            _float_or_zero(row.get("wilson_lower_95")),
            _float_or_zero(row.get("observed_success_rate")),
            _int_or_zero(row.get("selected_rows")),
        ),
    )


def _microstructure_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_rows: int = MIN_MICROSTRUCTURE_ROWS,
    min_sessions: int = MIN_MICROSTRUCTURE_SESSIONS,
) -> dict[str, Any]:
    non_empty = _non_empty_rows(rows)
    usable = [
        row
        for row in non_empty
        if str(row.get("spread_bucket", "")) not in {"", "missing"}
        and str(row.get("depth_bucket", "")) not in {"", "missing"}
        and str(row.get("imbalance_bucket", "")) not in {"", "missing"}
    ]
    return {
        "audit_rows": len(non_empty),
        "usable_rows": len(usable),
        "usable_sessions": len({str(row.get("trading_day", "")) for row in usable if row.get("trading_day")}),
        "usable_tickers": len({str(row.get("ticker", "")) for row in usable if row.get("ticker")}),
        "required_usable_rows": min_rows,
        "required_usable_sessions": min_sessions,
        "missing_usable_rows": max(0, min_rows - len(usable)),
        "missing_usable_sessions": max(
            0,
            min_sessions - len({str(row.get("trading_day", "")) for row in usable if row.get("trading_day")}),
        ),
        "ready": bool(
            len(usable) >= min_rows
            and len({str(row.get("trading_day", "")) for row in usable if row.get("trading_day")}) >= min_sessions
        ),
    }


def build_signal_90_status(
    *,
    run_dir: Path,
    collection_plan_path: Path | None = None,
) -> dict[str, Any]:
    model_results = _read_json(run_dir / "model-results.json")
    policy = _read_json(run_dir / "decision-policy.json")
    thresholds = _non_empty_rows(_read_csv(run_dir / "confidence-threshold-report.csv"))
    reliability = _non_empty_rows(_read_csv(run_dir / "confidence-reliability-report.csv"))
    audit_rows = _read_csv(run_dir / "decision-audit.csv")
    watchlist = _non_empty_rows(_read_csv(run_dir / "candidate-watchlist.csv"))
    directional_states = _non_empty_rows(_read_csv(run_dir / "directional-state-candidates.csv"))
    selective_rules = _non_empty_rows(_read_csv(run_dir / "selective-rule-candidates.csv"))
    precision_scout = _non_empty_rows(_read_csv(run_dir / "precision-scout-candidates.csv"))
    collection_plan = _read_json(collection_plan_path) if collection_plan_path else {}

    accepted_thresholds = [row for row in thresholds if _bool_value(row.get("accepted_research"))]
    accepted_reliability = [row for row in reliability if _bool_value(row.get("product_90_allowed"))]
    accepted_states = [
        row
        for row in directional_states + selective_rules + precision_scout
        if _bool_value(row.get("accepted_shadow"))
    ]
    best_threshold = _best_threshold(thresholds)
    best_reliability = _best_reliability_band(reliability)
    best_state = _best_state(directional_states + selective_rules + precision_scout)
    best_precision_scout = _best_product_relevant_state(precision_scout)
    microstructure = _microstructure_summary(audit_rows)
    product_claim_allowed = (
        bool(policy.get("product_claim_allowed"))
        and bool(accepted_thresholds or accepted_states)
        and bool(accepted_reliability)
    )
    missing_reasons: list[str] = []
    if not product_claim_allowed:
        missing_reasons.append("no_product_claim_policy")
    if not accepted_thresholds:
        missing_reasons.append("no_confidence_threshold_passed_gate")
    if not accepted_reliability:
        missing_reasons.append("no_calibrated_90_confidence_band")
    if not accepted_states:
        missing_reasons.append("no_market_state_passed_gate")
    if collection_plan and collection_plan.get("status") != "ready":
        missing_reasons.append("liquidity_holdout_not_ready")
    if int(microstructure["usable_rows"]) == 0:
        missing_reasons.append("no_microstructure_validation_rows")
    if not microstructure["ready"]:
        missing_reasons.append("microstructure_validation_not_ready")

    return {
        "schema_version": 1,
        "kind": "signal_90_research_status",
        "status": "ready_for_product_claim" if product_claim_allowed else "not_ready",
        "product_claim_allowed": product_claim_allowed,
        "run_dir": str(run_dir),
        "run_id": model_results.get("run_id", run_dir.name),
        "dataset_rows": model_results.get("dataset_rows"),
        "validation_sessions": model_results.get("validation_sessions"),
        "policy_status": policy.get("status", "missing"),
        "policy_reason_code": policy.get("reason_code"),
        "default_action": policy.get("default_action"),
        "accepted_threshold_count": len(accepted_thresholds),
        "accepted_reliability_band_count": len(accepted_reliability),
        "watchlist_count": len(watchlist),
        "accepted_market_state_count": len(accepted_states),
        "selective_rule_count": len(selective_rules),
        "precision_scout_count": len(precision_scout),
        "accepted_precision_scout_count": sum(1 for row in precision_scout if _bool_value(row.get("accepted_shadow"))),
        "precision_scout_summary": model_results.get("precision_scout_summary", {}),
        "best_threshold": best_threshold,
        "best_reliability_band": best_reliability,
        "best_market_state": best_state,
        "best_precision_scout": best_precision_scout,
        "microstructure": microstructure,
        "liquidity": {
            "status": collection_plan.get("status", "missing") if collection_plan else "missing",
            "missing_covered_signals": collection_plan.get("missing_covered_signals"),
            "missing_covered_sessions": collection_plan.get("missing_covered_sessions"),
            "recommended_additional_market_sessions": collection_plan.get("recommended_additional_market_sessions"),
            "collection_window_preflight": collection_plan.get("collection_window_preflight"),
            "recommended_command": collection_plan.get("recommended_command"),
        },
        "missing_reasons": missing_reasons,
    }


def _shell_command(command: object) -> str | None:
    if not isinstance(command, list) or not command:
        return None
    return " ".join(shlex.quote(str(item)) for item in command)


def _pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.2f}%"
    except (TypeError, ValueError):
        return "н/д"


def _ru_bool(value: object) -> str:
    return "да" if _bool_value(value) else "нет"


def write_report(path: Path, status: Mapping[str, Any]) -> None:
    threshold = status.get("best_threshold") if isinstance(status.get("best_threshold"), Mapping) else None
    reliability = (
        status.get("best_reliability_band") if isinstance(status.get("best_reliability_band"), Mapping) else None
    )
    state = status.get("best_market_state") if isinstance(status.get("best_market_state"), Mapping) else None
    precision = status.get("best_precision_scout") if isinstance(status.get("best_precision_scout"), Mapping) else None
    precision_summary = (
        status.get("precision_scout_summary")
        if isinstance(status.get("precision_scout_summary"), Mapping)
        else {}
    )
    microstructure = status.get("microstructure") if isinstance(status.get("microstructure"), Mapping) else {}
    liquidity = status.get("liquidity") if isinstance(status.get("liquidity"), Mapping) else {}
    lines = [
        "# Статус исследования цели 90%",
        "",
        f"- Статус: `{status['status']}`",
        f"- Можно заявлять продуктовый результат: {_ru_bool(status.get('product_claim_allowed'))}",
        f"- Запуск исследования: `{status.get('run_id')}`",
        f"- Строк в наборе данных: {status.get('dataset_rows')}",
        f"- Торговых дней в проверке: {status.get('validation_sessions')}",
        f"- Политика решений: `{status.get('policy_status')}` / `{status.get('policy_reason_code')}`",
        f"- Действие по умолчанию: `{status.get('default_action')}`",
        "",
        "## Порог уверенности",
        "",
    ]
    if threshold:
        lines.extend(
            [
                f"- Лучший порог: {threshold.get('threshold')}",
                f"- Отобрано случаев: {threshold.get('selected_rows')}",
                f"- Доля успешных случаев: {_pct(threshold.get('success_rate'))}",
                f"- Нижняя 95% граница надёжности: {_pct(threshold.get('wilson_lower_95'))}",
                f"- Принято исследованием: {_ru_bool(threshold.get('accepted_research'))}",
                "",
            ]
        )
    else:
        lines.extend(["Строки с порогами не найдены.", ""])
    lines.extend(["## Надёжность уверенности", ""])
    if reliability:
        lines.extend(
            [
                f"- Лучший диапазон: `{reliability.get('rule')}`",
                f"- Номинальное действие: `{reliability.get('nominal_action')}`",
                f"- Безопасное действие в продукте: `{reliability.get('safe_runtime_action')}`",
                f"- Отобрано случаев: {reliability.get('selected_rows')}",
                f"- Наблюдаемая доля успешных случаев: {_pct(reliability.get('observed_success_rate'))}",
                f"- Средняя уверенность модели: {_pct(reliability.get('mean_model_confidence'))}",
                f"- Нижняя 95% граница надёжности: {_pct(reliability.get('wilson_lower_95'))}",
                f"- Можно включать продуктовый режим 90%: {_ru_bool(reliability.get('product_90_allowed'))}",
                "",
            ]
        )
    else:
        lines.extend(["Строки с надёжностью уверенности не найдены.", ""])
    lines.extend(["## Рыночное состояние", ""])
    if state:
        lines.extend(
            [
                f"- Лучшее правило: `{state.get('rule')}`",
                f"- Случаев в проверке: {state.get('evaluation_rows')}",
                f"- Доля успешных случаев: {_pct(state.get('evaluation_success_rate'))}",
                f"- Нижняя 95% граница надёжности: {_pct(state.get('evaluation_wilson_lower_95'))}",
                f"- Причины блокировки: `{state.get('blocking_reasons')}`",
                "",
            ]
        )
    else:
        lines.extend(["Строки с рыночными состояниями не найдены.", ""])
    lines.extend(["## Поиск редких точных правил", ""])
    if precision_summary:
        lines.extend(
            [
                f"- Кандидатов: {precision_summary.get('candidate_rows')}",
                f"- Только наблюдать: {precision_summary.get('watch_only')}",
                f"- Кандидатов с положительным результатом после издержек: {precision_summary.get('positive_result_rows')}",
                f"- Теоретически могут дойти до 90% на 300 случаях: {precision_summary.get('can_reach_90pct_at_min_rows')}",
                f"- Жизнеспособность доказательства: `{json.dumps(precision_summary.get('proof_viability_counts'), ensure_ascii=False, sort_keys=True)}`",
                f"- Следующее действие: `{json.dumps(precision_summary.get('next_action_counts'), ensure_ascii=False, sort_keys=True)}`",
                "",
            ]
        )
    if precision:
        lines.extend(
            [
                f"- Лучшее продуктово-значимое правило: `{precision.get('rule')}`",
                f"- Основное направление: `{precision.get('dominant_decision')}`",
                f"- Основная гипотеза: `{precision.get('dominant_relation')}`",
                f"- Случаев в проверке: {precision.get('evaluation_rows')}",
                f"- Доля успешных случаев: {_pct(precision.get('evaluation_success_rate'))}",
                f"- Нижняя 95% граница надёжности: {_pct(precision.get('evaluation_wilson_lower_95'))}",
                f"- Средний результат после издержек: {precision.get('evaluation_mean_result_bps')} базисных пунктов",
                f"- Сколько текущих случаев не хватает до 90%: {precision.get('current_successes_needed_for_90pct')}",
                f"- Сколько дополнительных успехов нужно к 300 случаям: {precision.get('additional_successes_needed_for_90pct_at_min_rows')}",
                f"- Сколько будущих ошибок допустимо к 300 случаям: {precision.get('allowed_future_failures_for_90pct_at_min_rows')}",
                f"- Какая доля будущих успехов нужна: {_pct(precision.get('required_future_success_rate_for_90pct_at_min_rows'))}",
                f"- Может дойти до 90% на 300 случаях: {_ru_bool(precision.get('can_reach_90pct_at_min_rows'))}",
                f"- Жизнеспособность доказательства: `{precision.get('proof_viability')}`",
                f"- Статус: `{precision.get('status')}`",
                f"- Причины блокировки: `{precision.get('blocking_reasons')}`",
                "",
            ]
        )
    else:
        lines.extend(["Строки редких точных правил не найдены.", ""])
    lines.extend(
        [
            "## Покрытие данными стакана",
            "",
            f"- Строк в аудите: {microstructure.get('audit_rows')}",
            f"- Пригодных строк стакана: {microstructure.get('usable_rows')}",
            f"- Требуется пригодных строк: {microstructure.get('required_usable_rows')}",
            f"- Не хватает пригодных строк: {microstructure.get('missing_usable_rows')}",
            f"- Пригодных торговых дней: {microstructure.get('usable_sessions')}",
            f"- Требуется торговых дней: {microstructure.get('required_usable_sessions')}",
            f"- Не хватает торговых дней: {microstructure.get('missing_usable_sessions')}",
            f"- Инструментов с пригодными данными: {microstructure.get('usable_tickers')}",
            f"- Готово: {_ru_bool(microstructure.get('ready'))}",
            "",
        ]
    )
    lines.extend(
        [
            "## Отложенная проверка ликвидности",
            "",
            f"- Статус: `{liquidity.get('status')}`",
            f"- Не хватает покрытых сигналов: {liquidity.get('missing_covered_signals')}",
            f"- Не хватает торговых дней: {liquidity.get('missing_covered_sessions')}",
            f"- Рекомендуется дополнительно торговых дней: {liquidity.get('recommended_additional_market_sessions')}",
            "",
        ]
    )
    preflight = (
        liquidity.get("collection_window_preflight")
        if isinstance(liquidity.get("collection_window_preflight"), Mapping)
        else {}
    )
    if preflight:
        lines.extend(
            [
                "### Окно сбора",
                "",
                f"- Проверка окна: `{preflight.get('status')}`",
                f"- Причина: `{preflight.get('reason_code')}`",
                f"- Следующий рекомендуемый старт: {preflight.get('recommended_start_moscow')}",
                f"- Рекомендуемое окончание: {preflight.get('recommended_end_moscow')}",
                f"- Последний допустимый старт сегодня: {preflight.get('latest_full_start_moscow')}",
                f"- Можно успеть сегодня: {_ru_bool(preflight.get('can_complete_full_window_today'))}",
                "",
            ]
        )
    command = _shell_command(liquidity.get("recommended_command"))
    if command:
        lines.extend(
            [
                "### Команда добора данных",
                "",
                "```bash",
                command,
                "```",
                "",
            ]
        )
    lines.extend(["## Чего не хватает", ""])
    lines.extend(f"- `{reason}`" for reason in status.get("missing_reasons", []))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-signal-90-status")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--collection-plan", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/signal_90_status/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    status = build_signal_90_status(run_dir=args.run_dir, collection_plan_path=args.collection_plan)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "signal-90-status.json"
    report_path = args.output_dir / "signal-90-status.md"
    json_path.write_text(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_report(report_path, status)
    print(json.dumps({"status": status["status"], "output_dir": str(args.output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
