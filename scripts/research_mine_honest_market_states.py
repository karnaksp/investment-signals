#!/usr/bin/env python3
"""Mine honest direct/inverse market states from the signal price dataset.

This script deliberately does not depend on model probabilities. It searches
for interpretable market states on an earlier discovery slice, then evaluates
the same state/action rules on a later chronological slice. A candidate action
is either direct (follow the original signal direction) or inverse (fade it).
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import float_or_none, read_table, wilson_lower_bound, write_csv_records  # noqa: E402


STATE_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("action_signal_horizon", ("candidate_action", "signal_type", "horizon_seconds")),
    (
        "action_signal_session_volatility_horizon",
        ("candidate_action", "signal_type", "session_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "action_signal_combo_horizon",
        ("candidate_action", "signal_type", "combo_key_300s", "horizon_seconds"),
    ),
    (
        "action_combo_event_shape_horizon",
        (
            "candidate_action",
            "combo_key_300s",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "action_signal_event_shape_horizon",
        (
            "candidate_action",
            "signal_type",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "action_signal_trend_shape_horizon",
        (
            "candidate_action",
            "signal_type",
            "pre_trend_bucket",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "action_signal_consolidation_volatility_horizon",
        ("candidate_action", "signal_type", "consolidation_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "action_signal_cluster_volatility_horizon",
        ("candidate_action", "signal_type", "signal_count_bucket", "volatility_bucket", "horizon_seconds"),
    ),
)


def _float(value: object, default: float = 0.0) -> float:
    parsed = float_or_none(value)
    return default if parsed is None else parsed


def _boolish(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _signed_bucket(value: float | None, *, flat_bps: float = 10.0) -> str:
    if value is None:
        return "unknown"
    if abs(value) < flat_bps:
        return "flat"
    return "up" if value > 0 else "down"


def _quantile_bucket(value: object) -> str:
    parsed = float_or_none(value)
    if parsed is None:
        return "unknown"
    if parsed < 0.33:
        return "low"
    if parsed < 0.66:
        return "medium"
    return "high"


def _consolidation_bucket(row: Mapping[str, Any]) -> str:
    for column in ("pre_consolidation_score_60m", "pre_consolidation_score_30m", "pre_consolidation_score_15m"):
        value = float_or_none(row.get(column))
        if value is None:
            continue
        if value < 0.15:
            return "compressed"
        if value < 0.35:
            return "mixed"
        return "directional"
    return "unknown"


def _pre_trend_bucket(row: Mapping[str, Any]) -> str:
    for column in ("pre_return_bps_60m", "pre_return_bps_30m", "pre_return_bps_15m", "pre_return_bps_5m"):
        value = float_or_none(row.get(column))
        if value is not None:
            return _signed_bucket(value)
    return "unknown"


def _event_close_quality_bucket(row: Mapping[str, Any]) -> str:
    value = float_or_none(row.get("event_close_to_direction"))
    if value is None:
        return "unknown"
    if value < 0.35:
        return "weak_close"
    if value < 0.70:
        return "mixed_close"
    return "strong_close"


def _event_reversal_pressure_bucket(row: Mapping[str, Any]) -> str:
    value = float_or_none(row.get("event_reversal_pressure"))
    if value is None:
        return "unknown"
    if value < 0.35:
        return "low_reversal_pressure"
    if value < 0.70:
        return "medium_reversal_pressure"
    return "high_reversal_pressure"


def _signal_count_bucket(row: Mapping[str, Any]) -> str:
    count = int(_float(row.get("recent_signal_count_300s"), 0.0))
    if count <= 0:
        return "single"
    if count <= 3:
        return "cluster_2_3"
    return "cluster_4_plus"


def chronological_split(
    rows: Sequence[Mapping[str, Any]],
    *,
    discovery_fraction: float = 0.70,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    days = sorted({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")})
    if len(days) < 2:
        return [dict(row) for row in rows], []
    split_index = max(1, min(len(days) - 1, int(len(days) * discovery_fraction)))
    discovery_days = set(days[:split_index])
    return (
        [dict(row) for row in rows if str(row.get("trading_day", "")) in discovery_days],
        [dict(row) for row in rows if str(row.get("trading_day", "")) not in discovery_days],
    )


def action_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if not _boolish(row.get("forward_available")):
            continue
        direct_result = float_or_none(row.get("cost_adjusted_directional_bps"))
        inverse_result = float_or_none(row.get("reverse_directional_bps"))
        if direct_result is None or inverse_result is None:
            continue
        base = {
            "row_id": row.get("row_id"),
            "ticker": row.get("ticker"),
            "signal_type": row.get("signal_type"),
            "trading_day": row.get("trading_day"),
            "horizon_seconds": str(row.get("horizon_seconds")),
            "session_bucket": str(row.get("session_bucket")),
            "combo_key_300s": str(row.get("combo_key_300s") or "none"),
            "volatility_bucket": _quantile_bucket(row.get("day_volatility_quantile")),
            "liquidity_bucket": _quantile_bucket(row.get("ticker_volume_quantile")),
            "consolidation_bucket": _consolidation_bucket(row),
            "pre_trend_bucket": _pre_trend_bucket(row),
            "event_close_quality_bucket": _event_close_quality_bucket(row),
            "event_reversal_pressure_bucket": _event_reversal_pressure_bucket(row),
            "signal_count_bucket": _signal_count_bucket(row),
        }
        result.append(
            {
                **base,
                "candidate_action": "direct",
                "success": int(direct_result > 0.0),
                "result_bps": direct_result,
            }
        )
        result.append(
            {
                **base,
                "candidate_action": "inverse",
                "success": int(inverse_result > 0.0),
                "result_bps": inverse_result,
            }
        )
    return result


def _rule_text(columns: Sequence[str], key: Sequence[str]) -> str:
    return " | ".join(f"{column}={value}" for column, value in zip(columns, key))


def _rule_matches(row: Mapping[str, Any], columns: Sequence[str], key: Sequence[str]) -> bool:
    return tuple(str(row.get(column, "")) for column in columns) == tuple(key)


def metric_row(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successes = sum(int(row.get("success", 0)) for row in rows)
    result_values = [_float(row.get("result_bps")) for row in rows]
    return {
        "rows": len(rows),
        "sessions": len({str(row.get("trading_day", "")) for row in rows}),
        "tickers": len({str(row.get("ticker", "")) for row in rows}),
        "success_count": successes,
        "success_rate": successes / len(rows) if rows else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, len(rows)) or 0.0,
        "mean_result_bps": statistics.fmean(result_values) if result_values else 0.0,
        "max_day_share": max_day_share(rows),
    }


def max_day_share(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("trading_day", ""))] += 1
    return max(counts.values()) / len(rows)


def mine_states(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_discovery_rows: int = 50,
    accepted_min_rows: int = 300,
    accepted_min_sessions: int = 30,
    accepted_min_success_rate: float = 0.90,
    accepted_min_lower_bound: float = 0.75,
) -> list[dict[str, Any]]:
    discovery_rows, evaluation_rows = chronological_split(action_rows(rows))
    result: list[dict[str, Any]] = []
    for group_set, columns in STATE_GROUPS:
        groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        evaluation_groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in discovery_rows:
            groups[tuple(str(row.get(column, "")) for column in columns)].append(row)
        for row in evaluation_rows:
            evaluation_groups[tuple(str(row.get(column, "")) for column in columns)].append(row)
        for key, discovery_group in groups.items():
            if len(discovery_group) < min_discovery_rows:
                continue
            rule = _rule_text(columns, key)
            evaluation_group = evaluation_groups.get(key, [])
            discovery = metric_row(discovery_group)
            evaluation = metric_row(evaluation_group)
            accepted = bool(
                evaluation["rows"] >= accepted_min_rows
                and evaluation["sessions"] >= accepted_min_sessions
                and evaluation["success_rate"] >= accepted_min_success_rate
                and evaluation["wilson_lower_95"] >= accepted_min_lower_bound
                and evaluation["mean_result_bps"] > 0.0
                and evaluation["max_day_share"] <= 0.20
            )
            result.append(
                {
                    "group_set": group_set,
                    "rule": rule,
                    "discovery_rows": discovery["rows"],
                    "discovery_sessions": discovery["sessions"],
                    "discovery_success_count": discovery["success_count"],
                    "discovery_success_rate": discovery["success_rate"],
                    "discovery_wilson_lower_95": discovery["wilson_lower_95"],
                    "discovery_mean_result_bps": discovery["mean_result_bps"],
                    "evaluation_rows": evaluation["rows"],
                    "evaluation_sessions": evaluation["sessions"],
                    "evaluation_tickers": evaluation["tickers"],
                    "evaluation_success_count": evaluation["success_count"],
                    "evaluation_success_rate": evaluation["success_rate"],
                    "evaluation_wilson_lower_95": evaluation["wilson_lower_95"],
                    "evaluation_mean_result_bps": evaluation["mean_result_bps"],
                    "evaluation_max_day_share": evaluation["max_day_share"],
                    "accepted_shadow": accepted,
                }
            )
    return sorted(
        result,
        key=lambda item: (
            bool(item["accepted_shadow"]),
            float(item["evaluation_wilson_lower_95"]),
            float(item["evaluation_success_rate"]),
            float(item["evaluation_rows"]),
        ),
        reverse=True,
    )


def write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    accepted = [row for row in rows if row.get("accepted_shadow") is True]
    lines = [
        "# Honest market-state mining",
        "",
        f"- Проверенных правил: {len(rows)}",
        f"- Правил, прошедших shadow-gate: {len(accepted)}",
        "",
        "Правила ищутся на раннем периоде и проверяются на позднем периоде. "
        "Вероятность модели не используется; проверяются только прямое и обратное действие.",
        "",
        "## Лучшие проверочные состояния",
        "",
        "| Правило | строк | дней | успех | нижняя граница | средний результат, б.п. | принято |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows[:30]:
        lines.append(
            "| {rule} | {n} | {sessions} | {rate:.1%} | {lower:.1%} | {mean:.2f} | {accepted} |".format(
                rule=row["rule"],
                n=int(row["evaluation_rows"]),
                sessions=int(row["evaluation_sessions"]),
                rate=float(row["evaluation_success_rate"]),
                lower=float(row["evaluation_wilson_lower_95"]),
                mean=float(row["evaluation_mean_result_bps"]),
                accepted="да" if row.get("accepted_shadow") is True else "нет",
            )
        )
    lines.extend(
        [
            "",
            "Если верхние строки не проходят gate, текущих свечных признаков недостаточно "
            "для честного заявления о 90% даже на малом отборе.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-mine-honest-market-states")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-discovery-rows", type=int, default=50)
    parser.add_argument("--accepted-min-rows", type=int, default=300)
    parser.add_argument("--accepted-min-sessions", type=int, default=30)
    parser.add_argument("--accepted-min-success-rate", type=float, default=0.90)
    parser.add_argument("--accepted-min-lower-bound", type=float, default=0.75)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = read_table(args.dataset)
    candidates = mine_states(
        rows,
        min_discovery_rows=args.min_discovery_rows,
        accepted_min_rows=args.accepted_min_rows,
        accepted_min_sessions=args.accepted_min_sessions,
        accepted_min_success_rate=args.accepted_min_success_rate,
        accepted_min_lower_bound=args.accepted_min_lower_bound,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv_records(args.output_dir / "honest-market-state-candidates.csv", candidates)
    write_report(args.output_dir / "honest-market-state-report.md", candidates)
    print(
        json.dumps(
            {
                "status": "ok",
                "output_dir": str(args.output_dir),
                "candidate_rows": len(candidates),
                "accepted_shadow": sum(1 for row in candidates if row["accepted_shadow"]),
                "best_success_rate": candidates[0]["evaluation_success_rate"] if candidates else 0.0,
                "best_wilson_lower_95": candidates[0]["evaluation_wilson_lower_95"] if candidates else 0.0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
