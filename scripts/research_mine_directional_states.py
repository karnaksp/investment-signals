#!/usr/bin/env python3
"""Mine out-of-sample directional market states from decision audit rows."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence


STATE_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("decision_signal_horizon", ("frontier_decision", "signal_type", "horizon_seconds")),
    (
        "decision_relation_signal_horizon",
        ("frontier_decision", "frontier_decision_relation", "signal_type", "horizon_seconds"),
    ),
    (
        "decision_signal_session_volatility_horizon",
        ("frontier_decision", "signal_type", "session_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "decision_relation_session_volatility_horizon",
        (
            "frontier_decision",
            "frontier_decision_relation",
            "signal_type",
            "session_bucket",
            "volatility_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_signal_cluster_volatility_horizon",
        ("frontier_decision", "signal_type", "signal_count_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "decision_signal_combo_horizon",
        ("frontier_decision", "signal_type", "combo_key_300s", "horizon_seconds"),
    ),
    (
        "decision_session_combo_volatility_horizon",
        ("frontier_decision", "session_bucket", "combo_key_300s", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "decision_signal_consolidation_liquidity_horizon",
        (
            "frontier_decision",
            "signal_type",
            "consolidation_bucket",
            "liquidity_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_relation_consolidation_liquidity_horizon",
        (
            "frontier_decision",
            "frontier_decision_relation",
            "consolidation_bucket",
            "liquidity_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_signal_event_shape_horizon",
        (
            "frontier_decision",
            "signal_type",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_relation_event_shape_horizon",
        (
            "frontier_decision",
            "frontier_decision_relation",
            "event_close_quality_bucket",
            "event_reversal_pressure_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_signal_microstructure_horizon",
        (
            "frontier_decision",
            "signal_type",
            "spread_bucket",
            "depth_bucket",
            "imbalance_bucket",
            "horizon_seconds",
        ),
    ),
    (
        "decision_relation_microstructure_horizon",
        (
            "frontier_decision",
            "frontier_decision_relation",
            "spread_bucket",
            "depth_bucket",
            "imbalance_bucket",
            "horizon_seconds",
        ),
    ),
)

DEFAULT_CONFIDENCE_THRESHOLDS = (
    0.0,
    0.05,
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
    0.60,
    0.75,
    0.85,
    0.90,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
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


def wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float | None:
    if total <= 0:
        return None
    phat = successes / total
    denominator = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    margin = z * ((phat * (1 - phat) + z * z / (4 * total)) / total) ** 0.5
    return (centre - margin) / denominator


def chronological_day_split(
    rows: Sequence[Mapping[str, Any]],
    *,
    discovery_fraction: float = 0.50,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    days = sorted({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")})
    if not days:
        return [dict(row) for row in rows], []
    split_index = max(1, min(len(days) - 1, int(len(days) * discovery_fraction)))
    discovery_days = set(days[:split_index])
    discovery = [dict(row) for row in rows if str(row.get("trading_day", "")) in discovery_days]
    evaluation = [dict(row) for row in rows if str(row.get("trading_day", "")) not in discovery_days]
    return discovery, evaluation


def _rule_text(columns: Sequence[str], key: Sequence[str]) -> str:
    return " | ".join(f"{column}={value}" for column, value in zip(columns, key))


def _rule_matches(row: Mapping[str, Any], columns: Sequence[str], key: Sequence[str]) -> bool:
    return tuple(str(row.get(column, "")) for column in columns) == tuple(key)


def _eligible_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        decision = str(row.get("frontier_decision", ""))
        if decision not in {"up", "down"}:
            continue
        if row.get("frontier_success") in {None, ""}:
            continue
        result.append(dict(row))
    return result


def _select_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str],
    key: Sequence[str],
    min_confidence: float,
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if _rule_matches(row, columns, key)
        and _float_or_zero(row.get("frontier_confidence")) >= min_confidence
    ]


def _max_group_share(rows: Sequence[Mapping[str, Any]], column: str) -> float:
    if not rows:
        return 0.0
    counts = Counter(str(row.get(column, "")) for row in rows)
    return max(counts.values()) / len(rows)


def metric_row(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successes = sum(_int_or_zero(row.get("frontier_success")) for row in rows)
    results = [_float_or_zero(row.get("frontier_result_bps")) for row in rows]
    confidences = [_float_or_zero(row.get("frontier_confidence")) for row in rows]
    return {
        "rows": len(rows),
        "sessions": len({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")}),
        "tickers": len({str(row.get("ticker", "")) for row in rows if row.get("ticker")}),
        "success_count": successes,
        "success_rate": successes / len(rows) if rows else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, len(rows)) or 0.0,
        "mean_result_bps": statistics.fmean(results) if results else 0.0,
        "min_confidence": min(confidences) if confidences else 0.0,
        "max_confidence": max(confidences) if confidences else 0.0,
        "max_day_share": _max_group_share(rows, "trading_day"),
        "max_ticker_share": _max_group_share(rows, "ticker"),
        "inverse_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "inverse"),
        "direct_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "direct"),
        "neutral_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "neutral"),
    }


def _accepted(metric: Mapping[str, Any], *, min_rows: int, min_sessions: int) -> bool:
    return bool(
        int(metric["rows"]) >= min_rows
        and int(metric["sessions"]) >= min_sessions
        and float(metric["success_rate"]) >= 0.90
        and float(metric["wilson_lower_95"]) >= 0.75
        and float(metric["mean_result_bps"]) > 0
        and float(metric["max_day_share"]) <= 0.20
        and float(metric["max_ticker_share"]) <= 0.25
    )


def temporal_metric(
    rows: Sequence[Mapping[str, Any]],
    *,
    blocks: int = 5,
    min_blocks_with_selected: int = 3,
    min_block_success_rate: float = 0.75,
) -> dict[str, Any]:
    days = sorted({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")})
    if not days:
        return {
            "temporal_blocks": 0,
            "temporal_blocks_with_selected": 0,
            "temporal_weak_blocks": 0,
            "temporal_min_success_rate": 0.0,
            "temporal_min_mean_result_bps": 0.0,
            "temporal_supported": False,
        }
    block_count = min(max(1, blocks), len(days))
    metrics: list[dict[str, Any]] = []
    for block_index in range(block_count):
        start = block_index * len(days) // block_count
        end = (block_index + 1) * len(days) // block_count
        block_days = set(days[start:end])
        selected = [row for row in rows if str(row.get("trading_day", "")) in block_days]
        if selected:
            metrics.append(metric_row(selected))
    success_rates = [float(metric["success_rate"]) for metric in metrics]
    mean_results = [float(metric["mean_result_bps"]) for metric in metrics]
    weak_blocks = sum(
        1
        for metric in metrics
        if float(metric["success_rate"]) < min_block_success_rate
        or float(metric["mean_result_bps"]) <= 0
    )
    return {
        "temporal_blocks": block_count,
        "temporal_blocks_with_selected": len(metrics),
        "temporal_weak_blocks": weak_blocks,
        "temporal_min_success_rate": min(success_rates) if success_rates else 0.0,
        "temporal_min_mean_result_bps": min(mean_results) if mean_results else 0.0,
        "temporal_supported": bool(
            len(metrics) >= min_blocks_with_selected
            and weak_blocks == 0
        ),
    }


def _blocking_reasons(metric: Mapping[str, Any], *, min_rows: int, min_sessions: int) -> str:
    reasons: list[str] = []
    if int(metric["rows"]) < min_rows:
        reasons.append("sample_size")
    if int(metric["sessions"]) < min_sessions:
        reasons.append("trading_days")
    if float(metric["success_rate"]) < 0.90:
        reasons.append("success_rate")
    if float(metric["wilson_lower_95"]) < 0.75:
        reasons.append("reliability_bound")
    if float(metric["mean_result_bps"]) <= 0:
        reasons.append("positive_result")
    if float(metric["max_day_share"]) > 0.20:
        reasons.append("day_concentration")
    if float(metric["max_ticker_share"]) > 0.25:
        reasons.append("ticker_concentration")
    return ",".join(reasons)


def mine_directional_state_candidates(
    rows: Sequence[Mapping[str, Any]],
    *,
    confidence_thresholds: Sequence[float] = DEFAULT_CONFIDENCE_THRESHOLDS,
    discovery_fraction: float = 0.50,
    min_discovery_rows: int = 50,
    min_discovery_success_rate: float = 0.70,
    accepted_min_rows: int = 300,
    accepted_min_sessions: int = 30,
) -> list[dict[str, Any]]:
    eligible = _eligible_rows(rows)
    discovery_rows, evaluation_rows = chronological_day_split(
        eligible,
        discovery_fraction=discovery_fraction,
    )
    result: list[dict[str, Any]] = []
    for group_set, columns in STATE_GROUPS:
        grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        evaluation_grouped: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in discovery_rows:
            key = tuple(str(row.get(column, "")) for column in columns)
            if any(not value or value == "missing" for value in key):
                continue
            grouped[key].append(row)
        for row in evaluation_rows:
            key = tuple(str(row.get(column, "")) for column in columns)
            if any(not value or value == "missing" for value in key):
                continue
            evaluation_grouped[key].append(row)
        for key, discovery_group in grouped.items():
            emitted_signatures: set[tuple[int, int, int, int]] = set()
            for threshold in confidence_thresholds:
                selected_discovery = [
                    row
                    for row in discovery_group
                    if _float_or_zero(row.get("frontier_confidence")) >= threshold
                ]
                if len(selected_discovery) < min_discovery_rows:
                    continue
                discovery_metric = metric_row(selected_discovery)
                selected_evaluation = [
                    dict(row)
                    for row in evaluation_grouped.get(key, [])
                    if _float_or_zero(row.get("frontier_confidence")) >= threshold
                ]
                evaluation_metric = metric_row(selected_evaluation)
                time_metric = temporal_metric(selected_evaluation)
                signature = (
                    int(discovery_metric["rows"]),
                    int(discovery_metric["success_count"]),
                    int(evaluation_metric["rows"]),
                    int(evaluation_metric["success_count"]),
                )
                if signature in emitted_signatures:
                    continue
                emitted_signatures.add(signature)
                accepted = _accepted(
                    evaluation_metric,
                    min_rows=accepted_min_rows,
                    min_sessions=accepted_min_sessions,
                )
                accepted = accepted and bool(time_metric["temporal_supported"])
                reasons = _blocking_reasons(
                    evaluation_metric,
                    min_rows=accepted_min_rows,
                    min_sessions=accepted_min_sessions,
                )
                if not bool(time_metric["temporal_supported"]) and int(evaluation_metric["rows"]) > 0:
                    reasons = ",".join(item for item in (reasons, "temporal_instability") if item)
                result.append(
                    {
                        "group_set": group_set,
                        "rule": _rule_text(columns, key),
                        "min_confidence": threshold,
                        "discovery_rows": discovery_metric["rows"],
                        "discovery_sessions": discovery_metric["sessions"],
                        "discovery_success_count": discovery_metric["success_count"],
                        "discovery_success_rate": discovery_metric["success_rate"],
                        "discovery_wilson_lower_95": discovery_metric["wilson_lower_95"],
                        "discovery_mean_result_bps": discovery_metric["mean_result_bps"],
                        "discovery_promising": bool(
                            float(discovery_metric["success_rate"]) >= min_discovery_success_rate
                            and float(discovery_metric["mean_result_bps"]) > 0
                        ),
                        "evaluation_rows": evaluation_metric["rows"],
                        "evaluation_sessions": evaluation_metric["sessions"],
                        "evaluation_tickers": evaluation_metric["tickers"],
                        "evaluation_success_count": evaluation_metric["success_count"],
                        "evaluation_success_rate": evaluation_metric["success_rate"],
                        "evaluation_wilson_lower_95": evaluation_metric["wilson_lower_95"],
                        "evaluation_mean_result_bps": evaluation_metric["mean_result_bps"],
                        "evaluation_max_day_share": evaluation_metric["max_day_share"],
                        "evaluation_max_ticker_share": evaluation_metric["max_ticker_share"],
                        "evaluation_direct_rows": evaluation_metric["direct_rows"],
                        "evaluation_inverse_rows": evaluation_metric["inverse_rows"],
                        "evaluation_neutral_rows": evaluation_metric["neutral_rows"],
                        **time_metric,
                        "accepted_shadow": accepted,
                        "product_claim_allowed": False,
                        "blocking_reasons": reasons,
                    }
                )
    return sorted(
        result,
        key=lambda row: (
            bool(row["accepted_shadow"]),
            bool(row["discovery_promising"]),
            float(row["evaluation_success_rate"]),
            float(row["evaluation_wilson_lower_95"]),
            float(row["evaluation_mean_result_bps"]),
            int(row["evaluation_rows"]),
        ),
        reverse=True,
    )


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "group_set",
        "rule",
        "min_confidence",
        "discovery_rows",
        "discovery_sessions",
        "discovery_success_count",
        "discovery_success_rate",
        "discovery_wilson_lower_95",
        "discovery_mean_result_bps",
        "discovery_promising",
        "evaluation_rows",
        "evaluation_sessions",
        "evaluation_tickers",
        "evaluation_success_count",
        "evaluation_success_rate",
        "evaluation_wilson_lower_95",
        "evaluation_mean_result_bps",
        "evaluation_max_day_share",
        "evaluation_max_ticker_share",
        "evaluation_direct_rows",
        "evaluation_inverse_rows",
        "evaluation_neutral_rows",
        "temporal_blocks",
        "temporal_blocks_with_selected",
        "temporal_weak_blocks",
        "temporal_min_success_rate",
        "temporal_min_mean_result_bps",
        "temporal_supported",
        "accepted_shadow",
        "product_claim_allowed",
        "blocking_reasons",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    accepted = [row for row in rows if row.get("accepted_shadow")]
    lines = [
        "# Directional state mining",
        "",
        f"- Accepted shadow states: {len(accepted)}",
        f"- Candidate rows evaluated: {len(rows)}",
        "",
        "## Top evaluated states",
        "",
        "| Rule | min confidence | eval rows | eval sessions | success rate | Wilson lower 95% | mean result bps | inverse rows | temporal supported | weak blocks | blocking reasons |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for row in rows[:30]:
        lines.append(
            "| {rule} | {confidence:.2f} | {rows} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {inverse} | {temporal} | {weak} | {reasons} |".format(
                rule=row["rule"],
                confidence=float(row["min_confidence"]),
                rows=row["evaluation_rows"],
                sessions=row["evaluation_sessions"],
                rate=float(row["evaluation_success_rate"]),
                lower=float(row["evaluation_wilson_lower_95"]),
                mean=float(row["evaluation_mean_result_bps"]),
                inverse=row["evaluation_inverse_rows"],
                temporal=row.get("temporal_supported", ""),
                weak=row.get("temporal_weak_blocks", ""),
                reasons=row["blocking_reasons"],
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Rules are discovered on the earlier part of `decision-audit.csv` and evaluated on the later part. "
            "A rule is allowed only for shadow research when the later part has at least 300 rows, at least "
            "30 trading days, at least 90% observed success, Wilson lower bound at least 75%, positive mean "
            "result after costs, and no single day or ticker concentration.",
            "",
            "Rows with `frontier_decision_relation = inverse` are explicit reverse-signal hypotheses: the "
            "model is not saying that the original signal continues, but that this state may precede a move "
            "in the opposite direction.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-mine-directional-states")
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-discovery-rows", type=int, default=50)
    parser.add_argument("--accepted-min-rows", type=int, default=300)
    parser.add_argument("--accepted-min-sessions", type=int, default=30)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = _read_csv(args.audit)
    candidates = mine_directional_state_candidates(
        rows,
        min_discovery_rows=args.min_discovery_rows,
        accepted_min_rows=args.accepted_min_rows,
        accepted_min_sessions=args.accepted_min_sessions,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "directional-state-candidates.csv"
    report_path = args.output_dir / "directional-state-report.md"
    write_csv(csv_path, candidates)
    write_report(report_path, candidates)
    print(
        json.dumps(
            {
                "status": "ok",
                "audit": str(args.audit),
                "output": str(csv_path),
                "report": str(report_path),
                "candidate_rows": len(candidates),
                "accepted_shadow": sum(1 for row in candidates if row["accepted_shadow"]),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
