#!/usr/bin/env python3
"""Audit whether the 90% research dataset covers the requested market-state features."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


FeatureSpec = dict[str, Any]

MIN_MICROSTRUCTURE_VALUE_ROWS = 300


FEATURE_SPECS: list[FeatureSpec] = [
    {
        "id": "pre_signal_windows",
        "title": "Окна до сигнала 5/15/30/60 минут",
        "required_groups": [
            [f"pre_return_bps_{window}m", f"pre_volatility_bps_{window}m", f"pre_range_bps_{window}m"]
            for window in (5, 15, 30, 60)
        ],
    },
    {
        "id": "signal_series",
        "title": "Серия похожих сигналов",
        "required_any": [
            "recent_signal_count_60s",
            "recent_signal_count_300s",
            "recent_signal_count_900s",
            "recent_same_family_count_300s",
            "combo_key_300s",
        ],
        "min_present": 3,
    },
    {
        "id": "volume_and_range",
        "title": "Объём, диапазон и форма свечи",
        "required_any": [
            "volume_z_score",
            "range_z_score",
            "event_volume_ratio",
            "event_range_ratio",
            "candle_range_bps",
            "event_body_to_range",
            "event_close_to_direction",
            "event_reversal_pressure",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "pre_volume_change_60m",
        ],
        "min_present": 4,
    },
    {
        "id": "session_position",
        "title": "Положение внутри торговой сессии",
        "required_any": ["session_bucket"],
        "min_present": 1,
    },
    {
        "id": "volatility_regime",
        "title": "Режим волатильности дня и инструмента",
        "required_any": ["volatility_bucket", "day_volatility_bps", "day_volatility_quantile", "ticker_volatility_quantile"],
        "min_present": 2,
    },
    {
        "id": "liquidity_or_noise",
        "title": "Ликвидность или шумность инструмента",
        "required_any": [
            "liquidity_bucket",
            "day_volume_quantile",
            "ticker_volume_quantile",
            "ticker_mean_daily_volume",
            "orderbook_spread_bps",
            "orderbook_total_qty",
        ],
        "min_present": 3,
    },
    {
        "id": "trend_relation",
        "title": "Сигнал по движению или против движения",
        "required_any": [
            "pre_trend_bucket",
            "pre_trend_strength_bucket",
            "event_trend_relation",
            "decision_trend_relation",
            "pre_directional_return_bps_60m",
            "signal_directional_vs_market_bps_60m",
            "market_alignment_bucket",
            "relative_market_bucket",
        ],
        "min_present": 4,
    },
    {
        "id": "consolidation",
        "title": "Консолидация перед сигналом",
        "required_any": [
            "consolidation_bucket",
            "pre_consolidation_score_5m",
            "pre_consolidation_score_15m",
            "pre_consolidation_score_30m",
            "pre_consolidation_score_60m",
        ],
        "min_present": 3,
    },
    {
        "id": "instrument_abnormality",
        "title": "Отклонение от обычного поведения инструмента",
        "required_any": [
            "z_score",
            "event_strength_to_volatility",
            "baseline_volatility_bps",
            "event_to_pre_volatility_60m",
            "event_to_pre_range_60m",
            "baseline_move_bps",
        ],
        "min_present": 4,
    },
    {
        "id": "inverse_hypothesis",
        "title": "Обратная гипотеза и откат",
        "required_any": [
            "decision_relation",
            "frontier_decision_relation",
            "inverse_decisions",
            "reverse_directional_bps",
        ],
        "min_present": 2,
    },
    {
        "id": "leakage_guard",
        "title": "Защита от заглядывания в будущее",
        "required_any": ["feature_max_observed_at", "feature_leakage_flag"],
        "min_present": 2,
    },
]


def _csv_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return set(next(reader, []))


def _parquet_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        import duckdb  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return set()
    connection = duckdb.connect()
    try:
        rows = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    finally:
        connection.close()
    return {str(row[0]) for row in rows}


def _parquet_value_profile(path: Path, columns: set[str]) -> dict[str, Any]:
    if not path.exists() or not columns:
        return {"row_count": 0, "columns": {}}
    try:
        import duckdb  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        return {"row_count": 0, "columns": {}, "error": "duckdb_not_installed"}
    profile_columns = sorted(
        set().union(
            *(set(spec.get("required_any", [])) for spec in FEATURE_SPECS),
            *(set().union(*(set(group) for group in spec.get("required_groups", []))) for spec in FEATURE_SPECS),
            {"orderbook_available", "orderbook_spread_bps", "orderbook_total_qty", "orderbook_imbalance_ratio"},
        )
        & columns
    )
    connection = duckdb.connect()
    try:
        row_count = int(connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
        column_profiles: dict[str, Any] = {}
        for column in profile_columns:
            quoted = '"' + column.replace('"', '""') + '"'
            non_null = int(
                connection.execute(
                    f"SELECT count(*) FROM read_parquet(?) WHERE {quoted} IS NOT NULL",
                    [str(path)],
                ).fetchone()[0]
            )
            item: dict[str, Any] = {
                "non_null_rows": non_null,
                "non_null_share": non_null / row_count if row_count else 0.0,
            }
            if column == "orderbook_available":
                true_rows = int(
                    connection.execute(
                        f"SELECT count(*) FROM read_parquet(?) WHERE {quoted} = true",
                        [str(path)],
                    ).fetchone()[0]
                )
                item["true_rows"] = true_rows
                item["true_share"] = true_rows / row_count if row_count else 0.0
            column_profiles[column] = item
    finally:
        connection.close()
    return {"row_count": row_count, "columns": column_profiles}


def _present(columns: set[str], names: Sequence[str]) -> list[str]:
    return [name for name in names if name in columns]


def _evaluate_spec(columns: set[str], spec: Mapping[str, Any]) -> dict[str, Any]:
    required_groups = spec.get("required_groups")
    if isinstance(required_groups, list) and required_groups:
        groups: list[dict[str, Any]] = []
        passed = True
        for group in required_groups:
            names = [str(item) for item in group] if isinstance(group, list) else []
            present = _present(columns, names)
            group_passed = len(present) == len(names)
            passed = passed and group_passed
            groups.append({"required": names, "present": present, "passed": group_passed})
        return {
            "id": spec["id"],
            "title": spec["title"],
            "status": "passed" if passed else "failed",
            "mode": "all_groups",
            "groups": groups,
        }
    required_any = [str(item) for item in spec.get("required_any", [])]
    min_present = int(spec.get("min_present", len(required_any)))
    present = _present(columns, required_any)
    passed = len(present) >= min_present
    return {
        "id": spec["id"],
        "title": spec["title"],
        "status": "passed" if passed else "failed",
        "mode": "minimum_present",
        "required_any": required_any,
        "min_present": min_present,
        "present": present,
        "missing": [name for name in required_any if name not in columns],
    }


def build_feature_coverage_audit(
    *,
    dataset_columns: set[str],
    decision_audit_columns: set[str],
    threshold_columns: set[str],
    precision_scout_columns: set[str],
    value_profile: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    all_columns = set().union(dataset_columns, decision_audit_columns, threshold_columns, precision_scout_columns)
    checks = [_evaluate_spec(all_columns, spec) for spec in FEATURE_SPECS]
    failed = [check for check in checks if check["status"] != "passed"]
    value_profile = dict(value_profile or {})
    value_columns = value_profile.get("columns") if isinstance(value_profile.get("columns"), Mapping) else {}
    orderbook_profile = value_columns.get("orderbook_available") if isinstance(value_columns.get("orderbook_available"), Mapping) else {}
    orderbook_true_rows = int(orderbook_profile.get("true_rows", 0) or 0)
    return {
        "schema_version": 1,
        "kind": "objective_90_feature_coverage_audit",
        "status": "ready" if not failed else "missing_features",
        "ready": not failed,
        "value_status": (
            "microstructure_values_ready"
            if orderbook_true_rows >= MIN_MICROSTRUCTURE_VALUE_ROWS
            else "waiting_for_microstructure_values"
        ),
        "value_profile": value_profile,
        "microstructure_value_coverage": {
            "required_orderbook_rows": MIN_MICROSTRUCTURE_VALUE_ROWS,
            "orderbook_available_rows": orderbook_true_rows,
            "missing_orderbook_rows": max(0, MIN_MICROSTRUCTURE_VALUE_ROWS - orderbook_true_rows),
            "ready": orderbook_true_rows >= MIN_MICROSTRUCTURE_VALUE_ROWS,
        },
        "column_sources": {
            "dataset_columns": len(dataset_columns),
            "decision_audit_columns": len(decision_audit_columns),
            "threshold_columns": len(threshold_columns),
            "precision_scout_columns": len(precision_scout_columns),
            "total_unique_columns": len(all_columns),
        },
        "checks": checks,
        "summary": {
            "checks": len(checks),
            "passed": sum(1 for check in checks if check["status"] == "passed"),
            "failed": len(failed),
            "failed_ids": [check["id"] for check in failed],
        },
    }


def write_markdown(path: Path, audit: Mapping[str, Any]) -> None:
    lines = [
        "# Покрытие признаков для цели 90%",
        "",
        f"- Статус: `{audit.get('status')}`",
        f"- Статус значений: `{audit.get('value_status')}`",
        f"- Готово: {'да' if audit.get('ready') else 'нет'}",
        f"- Проверок пройдено: {dict(audit.get('summary', {})).get('passed')} из {dict(audit.get('summary', {})).get('checks')}",
        f"- Источники колонок: `{json.dumps(audit.get('column_sources'), ensure_ascii=False, sort_keys=True)}`",
        f"- Покрытие стакана значениями: `{json.dumps(audit.get('microstructure_value_coverage'), ensure_ascii=False, sort_keys=True)}`",
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
            ]
        )
        if check.get("mode") == "all_groups":
            for group in check.get("groups", []):
                if isinstance(group, Mapping):
                    lines.append(
                        f"- Группа: требуется `{group.get('required')}`, найдено `{group.get('present')}`, статус `{group.get('passed')}`"
                    )
        else:
            lines.extend(
                [
                    f"- Требуется минимум: {check.get('min_present')}",
                    f"- Найдено: `{check.get('present')}`",
                    f"- Не найдено: `{check.get('missing')}`",
                ]
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_feature_coverage_audit(
    *,
    dataset: Path,
    decision_audit: Path,
    threshold_report: Path,
    precision_scout: Path,
    output_dir: Path,
) -> dict[str, Any]:
    dataset_columns = _parquet_columns(dataset)
    audit = build_feature_coverage_audit(
        dataset_columns=dataset_columns,
        decision_audit_columns=_csv_columns(decision_audit),
        threshold_columns=_csv_columns(threshold_report),
        precision_scout_columns=_csv_columns(precision_scout),
        value_profile=_parquet_value_profile(dataset, dataset_columns),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "feature-coverage.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "feature-coverage.md", audit)
    return audit


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-audit-90-feature-coverage")
    parser.add_argument("--dataset", type=Path, default=Path("var/research/datasets/signal_price_prediction.parquet"))
    parser.add_argument("--decision-audit", type=Path, default=Path("var/research/runs/fe7da78bab3fd474/decision-audit.csv"))
    parser.add_argument("--threshold-report", type=Path, default=Path("var/research/runs/fe7da78bab3fd474/confidence-threshold-report.csv"))
    parser.add_argument("--precision-scout", type=Path, default=Path("var/research/runs/fe7da78bab3fd474/precision-scout-candidates.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/objective_90_features/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    audit = write_feature_coverage_audit(
        dataset=args.dataset,
        decision_audit=args.decision_audit,
        threshold_report=args.threshold_report,
        precision_scout=args.precision_scout,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {"status": audit["status"], "ready": audit["ready"], "output_dir": str(args.output_dir)},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
