#!/usr/bin/env python3
"""Audit whether current research evidence can support a 90% signal goal."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


MIN_ACCEPTED_ROWS = 300
MIN_ACCEPTED_SESSIONS = 30
TARGET_SUCCESS_RATE = 0.90
TARGET_PRODUCT_LOWER_BOUND = 0.90


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


def _bool_value(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


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


def _decision_count(summary: Mapping[str, Any], decision: str) -> int:
    counts = summary.get("decision_counts")
    if not isinstance(counts, Mapping):
        return 0
    return _int_or_zero(counts.get(decision))


def _best_threshold(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [dict(row) for row in rows if _int_or_zero(row.get("selected_rows")) > 0]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("accepted_research")),
            _float_or_zero(row.get("wilson_lower_95")),
            _float_or_zero(row.get("success_rate")),
            _int_or_zero(row.get("selected_rows")),
        ),
    )


def _best_reliability(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        dict(row)
        for row in rows
        if str(row.get("scope")) == "confidence_band" and _int_or_zero(row.get("selected_rows")) > 0
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            _bool_value(row.get("product_90_allowed")),
            _float_or_zero(row.get("wilson_lower_95")),
            _float_or_zero(row.get("observed_success_rate")),
            _int_or_zero(row.get("selected_rows")),
        ),
    )


def _check(
    check_id: str,
    title: str,
    passed: bool,
    *,
    required: str,
    observed: object,
    evidence: str,
    blocking: bool = True,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "title": title,
        "status": "passed" if passed else "failed",
        "blocking": blocking,
        "required": required,
        "observed": observed,
        "evidence": evidence,
    }


def build_goal_90_audit(
    *,
    run_dir: Path,
    signal_status_path: Path | None = None,
    collection_plan_path: Path | None = None,
) -> dict[str, Any]:
    model_results = _read_json(run_dir / "model-results.json")
    policy = _read_json(run_dir / "decision-policy.json")
    thresholds = _non_empty_rows(_read_csv(run_dir / "confidence-threshold-report.csv"))
    reliability = _non_empty_rows(_read_csv(run_dir / "confidence-reliability-report.csv"))
    directional_states = _non_empty_rows(_read_csv(run_dir / "directional-state-candidates.csv"))
    honest_states = _non_empty_rows(
        _read_csv(run_dir / "honest-market-states" / "honest-market-state-candidates.csv")
    )
    selective_rules = _non_empty_rows(_read_csv(run_dir / "selective-rule-candidates.csv"))
    precision_scout = _non_empty_rows(_read_csv(run_dir / "precision-scout-candidates.csv"))
    safe_triage = _read_json(run_dir / "safe-triage" / "safe-triage-summary.json")
    signal_status = _read_json(signal_status_path) if signal_status_path else {}
    collection_plan = _read_json(collection_plan_path) if collection_plan_path else {}

    accepted_thresholds = [row for row in thresholds if _bool_value(row.get("accepted_research"))]
    accepted_reliability = [row for row in reliability if _bool_value(row.get("product_90_allowed"))]
    accepted_states = [row for row in directional_states if _bool_value(row.get("accepted_shadow"))]
    accepted_honest_states = [row for row in honest_states if _bool_value(row.get("accepted_shadow"))]
    accepted_selective_rules = [row for row in selective_rules if _bool_value(row.get("accepted_shadow"))]
    accepted_precision_scout = [row for row in precision_scout if _bool_value(row.get("accepted_shadow"))]
    inverse_states = [
        row
        for row in directional_states + honest_states + selective_rules + precision_scout
        if "inverse" in str(row.get("relation", "")).lower()
        or "inverse" in str(row.get("decision_relation", "")).lower()
        or "inverse" in str(row.get("rule", "")).lower()
    ]
    best_threshold = signal_status.get("best_threshold") if isinstance(signal_status.get("best_threshold"), Mapping) else _best_threshold(thresholds)
    best_reliability = (
        signal_status.get("best_reliability_band")
        if isinstance(signal_status.get("best_reliability_band"), Mapping)
        else _best_reliability(reliability)
    )
    microstructure = signal_status.get("microstructure") if isinstance(signal_status.get("microstructure"), Mapping) else {}

    safe_rows = _int_or_zero(safe_triage.get("rows"))
    safe_up_down = _decision_count(safe_triage, "up") + _decision_count(safe_triage, "down")
    safe_skip = _decision_count(safe_triage, "skip")
    policy_claim = bool(policy.get("product_claim_allowed"))
    signal_claim = bool(signal_status.get("product_claim_allowed"))
    product_claim_allowed = policy_claim and signal_claim

    threshold_rows = _int_or_zero(best_threshold.get("selected_rows")) if isinstance(best_threshold, Mapping) else 0
    threshold_sessions = _int_or_zero(model_results.get("validation_sessions") or signal_status.get("validation_sessions"))
    threshold_success = _float_or_zero(best_threshold.get("success_rate")) if isinstance(best_threshold, Mapping) else 0.0
    threshold_lower = _float_or_zero(best_threshold.get("wilson_lower_95")) if isinstance(best_threshold, Mapping) else 0.0

    reliability_rows = _int_or_zero(best_reliability.get("selected_rows")) if isinstance(best_reliability, Mapping) else 0
    reliability_success = _float_or_zero(best_reliability.get("observed_success_rate")) if isinstance(best_reliability, Mapping) else 0.0
    reliability_lower = _float_or_zero(best_reliability.get("wilson_lower_95")) if isinstance(best_reliability, Mapping) else 0.0

    checks = [
        _check(
            "three_way_decision_export",
            "Три решения для интерфейса",
            bool(safe_triage) and safe_rows > 0 and safe_skip + safe_up_down == safe_rows,
            required="Каждая строка получает одно из решений: рост, снижение или пропуск.",
            observed={"rows": safe_rows, "skip": safe_skip, "directional": safe_up_down},
            evidence="safe-triage/safe-triage-summary.json",
            blocking=False,
        ),
        _check(
            "safe_default_skip",
            "Безопасное действие по умолчанию",
            (
                safe_rows > 0
                and (
                    (not product_claim_allowed and safe_skip == safe_rows and safe_up_down == 0)
                    or (product_claim_allowed and safe_up_down > 0)
                )
            ),
            required="Пока политика не доказана, все строки должны быть пропущены; после доказательства допустимы редкие направления.",
            observed={"rows": safe_rows, "skip": safe_skip, "directional": safe_up_down},
            evidence="safe-triage/safe-triage-summary.json",
            blocking=True,
        ),
        _check(
            "confidence_threshold_table",
            "Таблица порогов уверенности",
            bool(thresholds),
            required="Нужны строки проверки разных порогов уверенности.",
            observed={"rows": len(thresholds)},
            evidence="confidence-threshold-report.csv",
            blocking=True,
        ),
        _check(
            "accepted_confidence_threshold",
            "Принятый порог уверенности",
            bool(accepted_thresholds),
            required="Хотя бы один порог должен пройти исследовательские ограничения.",
            observed={
                "accepted_thresholds": len(accepted_thresholds),
                "best_selected_rows": threshold_rows,
                "best_success_rate": threshold_success,
                "best_wilson_lower_95": threshold_lower,
            },
            evidence="confidence-threshold-report.csv",
            blocking=True,
        ),
        _check(
            "sample_size_gate",
            "Минимум наблюдений и торговых дней",
            bool(accepted_thresholds) and threshold_rows >= MIN_ACCEPTED_ROWS and threshold_sessions >= MIN_ACCEPTED_SESSIONS,
            required=f"Не меньше {MIN_ACCEPTED_ROWS} строк и {MIN_ACCEPTED_SESSIONS} торговых дней для принятого правила.",
            observed={"selected_rows": threshold_rows, "validation_sessions": threshold_sessions},
            evidence="model-results.json + confidence-threshold-report.csv",
            blocking=True,
        ),
        _check(
            "observed_90_gate",
            "Наблюдаемая доля успешных случаев",
            bool(accepted_thresholds) and threshold_success >= TARGET_SUCCESS_RATE,
            required=f"Наблюдаемая успешность должна быть не ниже {TARGET_SUCCESS_RATE:.0%}.",
            observed={"success_rate": threshold_success},
            evidence="confidence-threshold-report.csv",
            blocking=True,
        ),
        _check(
            "product_reliability_lower_bound",
            "Нижняя граница надёжности",
            bool(accepted_reliability) and reliability_lower >= TARGET_PRODUCT_LOWER_BOUND,
            required=f"Нижняя 95% граница должна быть не ниже {TARGET_PRODUCT_LOWER_BOUND:.0%}.",
            observed={
                "accepted_bands": len(accepted_reliability),
                "best_rows": reliability_rows,
                "best_success_rate": reliability_success,
                "best_wilson_lower_95": reliability_lower,
            },
            evidence="confidence-reliability-report.csv",
            blocking=True,
        ),
        _check(
            "market_state_search",
            "Поиск рыночных состояний",
            bool(directional_states or honest_states or selective_rules or precision_scout),
            required="Нужен перебор условий, которые отделяют сильные случаи от шума.",
            observed={
                "directional_state_rows": len(directional_states),
                "honest_market_state_rows": len(honest_states),
                "selective_rule_rows": len(selective_rules),
                "precision_scout_rows": len(precision_scout),
                "accepted_directional_states": len(accepted_states),
                "accepted_honest_market_states": len(accepted_honest_states),
                "accepted_selective_rules": len(accepted_selective_rules),
                "accepted_precision_scout": len(accepted_precision_scout),
            },
            evidence="directional-state-candidates.csv + honest-market-state-candidates.csv + selective-rule-candidates.csv + precision-scout-candidates.csv",
            blocking=True,
        ),
        _check(
            "accepted_market_state",
            "Принятое рыночное состояние",
            bool(accepted_states or accepted_honest_states or accepted_selective_rules or accepted_precision_scout),
            required="Хотя бы одно состояние должно пройти проверку на отложенной части данных.",
            observed={
                "accepted_directional_states": len(accepted_states),
                "accepted_honest_market_states": len(accepted_honest_states),
                "accepted_selective_rules": len(accepted_selective_rules),
                "accepted_precision_scout": len(accepted_precision_scout),
            },
            evidence="directional-state-candidates.csv + honest-market-state-candidates.csv + selective-rule-candidates.csv + precision-scout-candidates.csv",
            blocking=True,
        ),
        _check(
            "inverse_hypothesis_search",
            "Проверка обратной гипотезы",
            bool(inverse_states),
            required="Нужно проверять случаи, где после сигнала устойчивее работает обратное направление.",
            observed={"inverse_rows": len(inverse_states)},
            evidence="directional-state-candidates.csv",
            blocking=False,
        ),
        _check(
            "microstructure_gate",
            "Проверка по стакану",
            bool(microstructure.get("ready")),
            required="Нужны валидные строки со спредом, глубиной и дисбалансом стакана.",
            observed=microstructure or {"ready": False},
            evidence=str(signal_status_path) if signal_status_path else "signal-90-status.json",
            blocking=True,
        ),
        _check(
            "liquidity_holdout",
            "Отложенная проверка ликвидности",
            collection_plan.get("status") == "ready",
            required="План сбора стакана должен быть закрыт, а не находиться в режиме добора данных.",
            observed={
                "status": collection_plan.get("status", "missing"),
                "missing_covered_signals": collection_plan.get("missing_covered_signals"),
                "missing_covered_sessions": collection_plan.get("missing_covered_sessions"),
            },
            evidence=str(collection_plan_path) if collection_plan_path else "collection-plan.json",
            blocking=True,
        ),
        _check(
            "product_claim_policy",
            "Разрешение продуктового вывода",
            product_claim_allowed,
            required="И исследовательский статус, и политика решений должны разрешить продуктовый вывод.",
            observed={
                "decision_policy_product_claim_allowed": policy_claim,
                "signal_status_product_claim_allowed": signal_claim,
                "policy_status": policy.get("status", "missing"),
                "signal_status": signal_status.get("status", "missing"),
            },
            evidence="decision-policy.json + signal-90-status.json",
            blocking=True,
        ),
    ]

    blocking_failures = [check for check in checks if check["blocking"] and check["status"] != "passed"]
    status = "ready_for_shadow_candidate" if not blocking_failures else "not_ready"
    return {
        "schema_version": 1,
        "kind": "goal_90_readiness_audit",
        "status": status,
        "ready": not blocking_failures,
        "run_dir": str(run_dir),
        "run_id": model_results.get("run_id", run_dir.name),
        "product_claim_allowed": product_claim_allowed,
        "summary": {
            "checks": len(checks),
            "passed": sum(1 for check in checks if check["status"] == "passed"),
            "failed": sum(1 for check in checks if check["status"] != "passed"),
            "blocking_failed": len(blocking_failures),
        },
        "blocking_failures": [check["id"] for check in blocking_failures],
        "checks": checks,
    }


def write_goal_90_audit_report(path: Path, audit: Mapping[str, Any]) -> None:
    lines = [
        "# Аудит готовности цели 90%",
        "",
        f"- Статус: `{audit.get('status')}`",
        f"- Продуктовый вывод разрешён: `{audit.get('product_claim_allowed')}`",
        f"- Запуск исследования: `{audit.get('run_id')}`",
        f"- Проверок пройдено: {dict(audit.get('summary', {})).get('passed')} из {dict(audit.get('summary', {})).get('checks')}",
        f"- Блокирующих провалов: {dict(audit.get('summary', {})).get('blocking_failed')}",
        "",
        "## Проверки",
        "",
    ]
    for check in audit.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        marker = "✅" if check.get("status") == "passed" else "❌"
        blocking = "блокирует" if check.get("blocking") else "не блокирует"
        lines.extend(
            [
                f"### {marker} {check.get('title')}",
                "",
                f"- Код: `{check.get('id')}`",
                f"- Статус: `{check.get('status')}` ({blocking})",
                f"- Требование: {check.get('required')}",
                f"- Факт: `{json.dumps(check.get('observed'), ensure_ascii=False, sort_keys=True)}`",
                f"- Артефакт: `{check.get('evidence')}`",
                "",
            ]
        )
    if audit.get("blocking_failures"):
        lines.extend(["## Что мешает включить 90% в продукт", ""])
        lines.extend(f"- `{item}`" for item in audit.get("blocking_failures", []))
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_goal_90_audit(output_dir: Path, audit: Mapping[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "goal-90-audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_goal_90_audit_report(output_dir / "goal-90-audit.md", audit)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-audit-90-goal-readiness")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--signal-status", type=Path)
    parser.add_argument("--collection-plan", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/goal_90_audit/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    audit = build_goal_90_audit(
        run_dir=args.run_dir,
        signal_status_path=args.signal_status,
        collection_plan_path=args.collection_plan,
    )
    write_goal_90_audit(args.output_dir, audit)
    print(
        json.dumps(
            {
                "status": audit["status"],
                "ready": audit["ready"],
                "blocking_failures": audit["blocking_failures"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
