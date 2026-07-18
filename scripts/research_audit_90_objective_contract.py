#!/usr/bin/env python3
"""Audit whether research artifacts implement the selective 90% objective."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


REQUIRED_BANDS = {
    "skip": "пропустить, недостаточно уверенности",
    "weak_observation": "слабое наблюдение",
    "working_hypothesis": "рабочая гипотеза",
    "strong_signal": "сильный сигнал",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _check(
    check_id: str,
    title: str,
    passed: bool,
    *,
    requirement: str,
    evidence: str,
    observed: object,
    blocks_product_claim: bool,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "title": title,
        "status": "passed" if passed else "failed",
        "blocks_product_claim": blocks_product_claim,
        "requirement": requirement,
        "evidence": evidence,
        "observed": observed,
    }


def _goal_check(goal_audit: Mapping[str, Any], check_id: str) -> dict[str, Any] | None:
    checks = goal_audit.get("checks")
    if not isinstance(checks, list):
        return None
    for item in checks:
        if isinstance(item, Mapping) and item.get("id") == check_id:
            return dict(item)
    return None


def _status(goal_audit: Mapping[str, Any], check_id: str) -> str:
    check = _goal_check(goal_audit, check_id)
    return str(check.get("status", "missing")) if check else "missing"


def _confidence_bands(selection_report: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = selection_report.get("confidence_band_rows")
    if not isinstance(rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            result[str(row.get("band", ""))] = dict(row)
    return result


def build_objective_contract_audit(
    *,
    selection_report: Mapping[str, Any],
    signal_status: Mapping[str, Any],
    goal_audit: Mapping[str, Any],
    schedule_status: Mapping[str, Any],
    feature_coverage: Mapping[str, Any] | None = None,
    gap_audit: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    target = selection_report.get("target") if isinstance(selection_report.get("target"), Mapping) else {}
    threshold_rows = selection_report.get("threshold_rows") if isinstance(selection_report.get("threshold_rows"), list) else []
    bands = _confidence_bands(selection_report)
    missing_bands = sorted(set(REQUIRED_BANDS) - set(bands))
    band_labels_ok = all(
        str(bands.get(band, {}).get("label_ru")) == label
        for band, label in REQUIRED_BANDS.items()
        if band in bands
    )
    unsafe_unproven_bands = [
        band
        for band, row in bands.items()
        if not _bool(row.get("product_90_allowed")) and str(row.get("safe_runtime_action")) != "skip"
    ]
    gap_summary = gap_audit.get("summary") if isinstance(gap_audit, Mapping) and isinstance(gap_audit.get("summary"), Mapping) else {}
    gap_target = gap_audit.get("target") if isinstance(gap_audit, Mapping) and isinstance(gap_audit.get("target"), Mapping) else {}
    gap_rows = gap_audit.get("rows") if isinstance(gap_audit, Mapping) and isinstance(gap_audit.get("rows"), list) else []
    gap_product_safe = bool(gap_audit) and (
        _int(gap_summary.get("accepted_shadow")) > 0
        or (
            str(gap_audit.get("status")) == "not_ready"
            and _int(gap_summary.get("candidate_rows")) > 0
            and _float(gap_summary.get("best_success_rate")) < 0.90
        )
    )
    checks = [
        _check(
            "default_skip_not_all_signals",
            "Система не заставляет отвечать на каждый сигнал",
            _status(goal_audit, "safe_default_skip") == "passed",
            requirement="Пока нет доказанного правила, каждое событие должно получать безопасное решение «пропустить».",
            evidence="goal-90-audit.json + safe-triage-summary.json",
            observed=_goal_check(goal_audit, "safe_default_skip") or {},
            blocks_product_claim=True,
        ),
        _check(
            "three_decisions_supported",
            "Поддержаны три решения",
            _status(goal_audit, "three_way_decision_export") == "passed",
            requirement="Каждая строка должна сводиться к росту, снижению или пропуску.",
            evidence="goal-90-audit.json + safe-triage-decisions.csv",
            observed=_goal_check(goal_audit, "three_way_decision_export") or {},
            blocks_product_claim=False,
        ),
        _check(
            "confidence_threshold_curve",
            "Есть таблица «точность против количества»",
            len(threshold_rows) > 0,
            requirement="Нужны разные пороги уверенности с количеством оставшихся сигналов, успешными исходами и долей успеха.",
            evidence="selection-90-report.json",
            observed={"threshold_rows": len(threshold_rows)},
            blocks_product_claim=True,
        ),
        _check(
            "product_confidence_bands",
            "Есть продуктовые диапазоны уверенности",
            not missing_bands and band_labels_ok and not unsafe_unproven_bands,
            requirement="Диапазоны 0–60%, 60–75%, 75–90%, 90%+ должны быть явно заданы и оставаться безопасными до проверки.",
            evidence="selection-90-report.json + confidence-reliability-report.csv",
            observed={
                "bands": sorted(bands),
                "missing_bands": missing_bands,
                "labels_ok": band_labels_ok,
                "unsafe_unproven_bands": unsafe_unproven_bands,
            },
            blocks_product_claim=True,
        ),
        _check(
            "minimum_evidence_gate",
            "Включён минимум 300 случаев и 30 торговых дней",
            _int(target.get("minimum_rows")) >= 300 and _int(target.get("minimum_sessions")) >= 30,
            requirement="Продуктовый вывод не должен приниматься на малой выборке.",
            evidence="selection-90-report.json",
            observed=target,
            blocks_product_claim=True,
        ),
        _check(
            "lower_bound_gate",
            "Включена нижняя граница надёжности",
            _float(target.get("minimum_lower_bound")) >= 0.75,
            requirement="Нижняя граница оценки должна быть высокой, а не только средняя доля успеха.",
            evidence="selection-90-report.json",
            observed=target,
            blocks_product_claim=True,
        ),
        _check(
            "market_state_search",
            "Ищутся состояния рынка вокруг сигнала",
            _status(goal_audit, "market_state_search") == "passed",
            requirement="Нужно искать комбинации предсигнального движения, серий сигналов, объёма, диапазона, сессии, волатильности и ликвидности.",
            evidence="goal-90-audit.json + directional-state-candidates.csv + precision-scout-candidates.csv",
            observed=_goal_check(goal_audit, "market_state_search") or {},
            blocks_product_claim=True,
        ),
        _check(
            "objective_feature_coverage",
            "Покрыты признаки из цели",
            bool(feature_coverage) and bool(feature_coverage.get("ready")),
            requirement="Набор данных и аудит решений должны содержать окна 5/15/30/60 минут, серии сигналов, объём, диапазон, сессию, волатильность, ликвидность, тренд, консолидацию, отклонение от обычного поведения и обратную гипотезу.",
            evidence="feature-coverage.json",
            observed=feature_coverage or {"ready": False},
            blocks_product_claim=True,
        ),
        _check(
            "inverse_hypothesis_search",
            "Ищутся обратные гипотезы",
            _status(goal_audit, "inverse_hypothesis_search") == "passed"
            and _int(dict(selection_report.get("inverse_hypotheses", {})).get("rows")) > 0,
            requirement="Нужно отдельно проверять, не является ли часть сигналов признаком истощения и отката.",
            evidence="goal-90-audit.json + selection-90-report.json",
            observed={
                "goal_check": _goal_check(goal_audit, "inverse_hypothesis_search") or {},
                "inverse_hypotheses": selection_report.get("inverse_hypotheses", {}),
            },
            blocks_product_claim=False,
        ),
        _check(
            "microstructure_collection_scheduled",
            "Сбор стакана подготовлен",
            str(schedule_status.get("status")) == "ready_loaded",
            requirement="Для дальнейшего отделения сильных случаев нужен плотный сбор спреда, глубины и дисбаланса стакана до сигнала.",
            evidence="schedule-status.json",
            observed=schedule_status,
            blocks_product_claim=True,
        ),
        _check(
            "microstructure_value_status_tracked",
            "Отслеживается заполненность значений стакана",
            bool(feature_coverage)
            and isinstance(feature_coverage.get("microstructure_value_coverage"), Mapping)
            and str(feature_coverage.get("value_status", "")) in {
                "microstructure_values_ready",
                "waiting_for_microstructure_values",
            },
            requirement="Нужно отличать наличие колонок стакана от фактического наличия значений спреда, глубины и дисбаланса до сигнала.",
            evidence="feature-coverage.json",
            observed={
                "value_status": dict(feature_coverage or {}).get("value_status"),
                "microstructure_value_coverage": dict(feature_coverage or {}).get("microstructure_value_coverage"),
            },
            blocks_product_claim=False,
        ),
        _check(
            "gap_to_90_audit",
            "Разрыв до 90% явно посчитан",
            gap_product_safe
            and _int(gap_target.get("minimum_rows")) >= 300
            and _int(gap_target.get("minimum_sessions")) >= 30
            and _float(gap_target.get("minimum_lower_bound")) >= 0.75,
            requirement="Нужно явно показывать, насколько текущие пороги, редкие правила и исключения далеки от 90%, и не разрешать продуктовый вывод, если лучший кандидат ниже цели.",
            evidence="gap-to-90.json/md",
            observed={
                "status": dict(gap_audit or {}).get("status"),
                "summary": gap_summary,
                "target": gap_target,
                "sample_rows": len(gap_rows),
            },
            blocks_product_claim=True,
        ),
        _check(
            "product_claim_still_blocked_without_evidence",
            "Продуктовый вывод заблокирован без доказательства",
            not bool(selection_report.get("product_claim_allowed"))
            and not bool(signal_status.get("product_claim_allowed")),
            requirement="Если 90% не доказано, система не должна разрешать клиентский вывод «рост» или «снижение».",
            evidence="selection-90-report.json + signal-90-status.json",
            observed={
                "selection_product_claim_allowed": selection_report.get("product_claim_allowed"),
                "signal_product_claim_allowed": signal_status.get("product_claim_allowed"),
            },
            blocks_product_claim=True,
        ),
    ]
    failed = [check for check in checks if check["status"] != "passed"]
    product_blockers = [check["id"] for check in checks if check["blocks_product_claim"] and check["status"] != "passed"]
    evidence_ready = bool(signal_status.get("product_claim_allowed")) and not product_blockers
    mechanism_ready = not failed
    return {
        "schema_version": 1,
        "kind": "objective_90_contract_audit",
        "status": (
            "ready_for_product_claim"
            if evidence_ready
            else "mechanism_ready_waiting_for_evidence"
            if mechanism_ready
            else "mechanism_incomplete"
        ),
        "mechanism_ready": mechanism_ready,
        "evidence_ready": evidence_ready,
        "product_claim_allowed": evidence_ready,
        "checks": checks,
        "summary": {
            "checks": len(checks),
            "passed": sum(1 for check in checks if check["status"] == "passed"),
            "failed": len(failed),
            "product_blockers": product_blockers,
        },
    }


def write_markdown(path: Path, audit: Mapping[str, Any]) -> None:
    lines = [
        "# Контракт цели 90%",
        "",
        f"- Статус: `{audit.get('status')}`",
        f"- Механизм готов: {'да' if audit.get('mechanism_ready') else 'нет'}",
        f"- Доказательства готовы: {'да' if audit.get('evidence_ready') else 'нет'}",
        f"- Продуктовый вывод разрешён: {'да' if audit.get('product_claim_allowed') else 'нет'}",
        f"- Проверок пройдено: {dict(audit.get('summary', {})).get('passed')} из {dict(audit.get('summary', {})).get('checks')}",
        "",
        "## Проверки",
        "",
    ]
    for check in audit.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        marker = "✅" if check.get("status") == "passed" else "❌"
        lines.extend(
            [
                f"### {marker} {check.get('title')}",
                "",
                f"- Код: `{check.get('id')}`",
                f"- Статус: `{check.get('status')}`",
                f"- Требование: {check.get('requirement')}",
                f"- Артефакт: `{check.get('evidence')}`",
                f"- Факт: `{json.dumps(check.get('observed'), ensure_ascii=False, sort_keys=True)}`",
                "",
            ]
        )
    blockers = dict(audit.get("summary", {})).get("product_blockers")
    if blockers:
        lines.extend(["## Что блокирует продуктовый вывод", ""])
        lines.extend(f"- `{item}`" for item in blockers)
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_audit(
    *,
    selection_report_path: Path,
    signal_status_path: Path,
    goal_audit_path: Path,
    schedule_status_path: Path,
    feature_coverage_path: Path,
    gap_audit_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    audit = build_objective_contract_audit(
        selection_report=_read_json(selection_report_path),
        signal_status=_read_json(signal_status_path),
        goal_audit=_read_json(goal_audit_path),
        schedule_status=_read_json(schedule_status_path),
        feature_coverage=_read_json(feature_coverage_path),
        gap_audit=_read_json(gap_audit_path),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "objective-90-contract.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "objective-90-contract.md", audit)
    return audit


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-audit-90-objective-contract")
    parser.add_argument("--selection-report", type=Path, default=Path("var/research/selection_90/current/selection-90-report.json"))
    parser.add_argument("--signal-status", type=Path, default=Path("var/research/signal_90_status/current/signal-90-status.json"))
    parser.add_argument("--goal-audit", type=Path, default=Path("var/research/goal_90_audit/current/goal-90-audit.json"))
    parser.add_argument("--schedule-status", type=Path, default=Path("var/research/liquidity_holdout/current/collection_plan/schedule-status.json"))
    parser.add_argument("--feature-coverage", type=Path, default=Path("var/research/objective_90_features/current/feature-coverage.json"))
    parser.add_argument("--gap-audit", type=Path, default=Path("var/research/gap_90/current/gap-to-90.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/objective_90_contract/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    audit = write_audit(
        selection_report_path=args.selection_report,
        signal_status_path=args.signal_status,
        goal_audit_path=args.goal_audit,
        schedule_status_path=args.schedule_status,
        feature_coverage_path=args.feature_coverage,
        gap_audit_path=args.gap_audit,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": audit["status"],
                "mechanism_ready": audit["mechanism_ready"],
                "evidence_ready": audit["evidence_ready"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
