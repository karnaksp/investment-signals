#!/usr/bin/env python3
"""Mine selective conjunctive rules for rare high-confidence signal states."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


CATEGORICAL_FIELDS = (
    "frontier_decision",
    "frontier_decision_relation",
    "signal_type",
    "horizon_seconds",
    "session_bucket",
    "volatility_bucket",
    "consolidation_bucket",
    "liquidity_bucket",
    "pre_trend_bucket",
    "pre_trend_strength_bucket",
    "event_trend_relation",
    "decision_trend_relation",
    "event_close_quality_bucket",
    "event_reversal_pressure_bucket",
    "market_alignment_bucket",
    "relative_market_bucket",
    "signal_count_bucket",
    "combo_key_300s",
    "spread_bucket",
    "depth_bucket",
    "imbalance_bucket",
)

NUMERIC_MIN_THRESHOLDS: Mapping[str, tuple[float, ...]] = {
    "frontier_confidence": (0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.75, 0.85, 0.90),
    "max_confidence": (0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.75, 0.85, 0.90),
    "recent_signal_count_60s": (1, 2, 3),
    "recent_signal_count_300s": (1, 2, 3),
    "recent_signal_count_900s": (1, 2, 3, 5),
    "event_to_pre_volatility_60m": (1.5, 2.0, 3.0, 5.0),
    "event_to_pre_range_60m": (1.5, 2.0, 3.0, 5.0),
    "event_body_to_range": (0.25, 0.50, 0.75),
    "event_upper_wick_to_range": (0.25, 0.50, 0.75),
    "event_lower_wick_to_range": (0.25, 0.50, 0.75),
    "event_close_to_direction": (0.25, 0.50, 0.75),
    "event_reversal_pressure": (0.25, 0.50, 0.75, 0.90),
    "pre_abs_return_bps_60m": (10.0, 25.0, 50.0, 100.0),
    "pre_directional_return_bps_60m": (10.0, 25.0, 50.0, 100.0),
    "pre_return_to_volatility_60m": (0.75, 1.5, 2.5, 4.0),
    "signal_vs_market_bps_60m": (10.0, 25.0, 50.0, 100.0),
    "orderbook_spread_bps": (5.0, 10.0, 20.0, 50.0),
    "orderbook_total_qty": (1_000.0, 5_000.0, 10_000.0, 50_000.0),
}

QUANTILE_BUCKETS = ("day_volatility_quantile", "ticker_volatility_quantile")

ATOM_FIELD_ALIASES = {
    "frontier_confidence": "model_confidence",
    "max_confidence": "model_confidence",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: object) -> float:
    return _float_or_none(value) or 0.0


def _int_or_zero(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float:
    if total <= 0:
        return 0.0
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
    return (
        [dict(row) for row in rows if str(row.get("trading_day", "")) in discovery_days],
        [dict(row) for row in rows if str(row.get("trading_day", "")) not in discovery_days],
    )


def row_atoms(row: Mapping[str, Any]) -> frozenset[str]:
    atoms: set[str] = set()
    for field in CATEGORICAL_FIELDS:
        value = str(row.get(field, "")).strip()
        if value and value != "missing":
            atoms.add(f"{field}={value}")
    for field, thresholds in NUMERIC_MIN_THRESHOLDS.items():
        value = _float_or_none(row.get(field))
        if value is None:
            continue
        for threshold in thresholds:
            if value >= threshold:
                atoms.add(f"{field}>={threshold:g}")
    for field in QUANTILE_BUCKETS:
        value = _float_or_none(row.get(field))
        if value is None:
            continue
        if value <= 0.25:
            atoms.add(f"{field}=low")
        if value >= 0.75:
            atoms.add(f"{field}=high")
    return frozenset(atoms)


def eligible_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("frontier_decision", "")) not in {"up", "down"}:
            continue
        if row.get("frontier_success") in {None, ""}:
            continue
        result.append(dict(row))
    return result


def _max_share(rows: Sequence[Mapping[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    counts = Counter(str(row.get(field, "")) for row in rows)
    return max(counts.values()) / len(rows)


def metric_row(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successes = sum(_int_or_zero(row.get("frontier_success")) for row in rows)
    result_values = [_float_or_zero(row.get("frontier_result_bps")) for row in rows]
    confidence_values = [_float_or_zero(row.get("frontier_confidence")) for row in rows]
    return {
        "rows": len(rows),
        "sessions": len({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")}),
        "tickers": len({str(row.get("ticker", "")) for row in rows if row.get("ticker")}),
        "success_count": successes,
        "success_rate": successes / len(rows) if rows else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, len(rows)),
        "mean_result_bps": statistics.fmean(result_values) if result_values else 0.0,
        "mean_confidence": statistics.fmean(confidence_values) if confidence_values else 0.0,
        "max_day_share": _max_share(rows, "trading_day"),
        "max_ticker_share": _max_share(rows, "ticker"),
        "inverse_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "inverse"),
        "direct_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "direct"),
        "neutral_rows": sum(1 for row in rows if row.get("frontier_decision_relation") == "neutral"),
    }


@dataclass(frozen=True)
class PreparedRows:
    rows: Sequence[Mapping[str, Any]]
    success: tuple[int, ...]
    result_bps: tuple[float, ...]
    confidence: tuple[float, ...]
    decision: tuple[str, ...]
    trading_day: tuple[str, ...]
    ticker: tuple[str, ...]
    relation: tuple[str, ...]


def prepare_rows(rows: Sequence[Mapping[str, Any]]) -> PreparedRows:
    return PreparedRows(
        rows=rows,
        success=tuple(_int_or_zero(row.get("frontier_success")) for row in rows),
        result_bps=tuple(_float_or_zero(row.get("frontier_result_bps")) for row in rows),
        confidence=tuple(_float_or_zero(row.get("frontier_confidence")) for row in rows),
        decision=tuple(str(row.get("frontier_decision", "")) for row in rows),
        trading_day=tuple(str(row.get("trading_day", "")) for row in rows),
        ticker=tuple(str(row.get("ticker", "")) for row in rows),
        relation=tuple(str(row.get("frontier_decision_relation", "")) for row in rows),
    )


def _max_index_share(values: Sequence[str], indices: frozenset[int]) -> float:
    if not indices:
        return 0.0
    counts = Counter(values[index] for index in indices)
    return max(counts.values()) / len(indices)


def metric_indices(prepared: PreparedRows, indices: frozenset[int]) -> dict[str, Any]:
    if not indices:
        return {
            "rows": 0,
            "sessions": 0,
            "tickers": 0,
            "success_count": 0,
            "success_rate": 0.0,
            "wilson_lower_95": 0.0,
            "mean_result_bps": 0.0,
            "mean_confidence": 0.0,
            "max_day_share": 0.0,
            "max_ticker_share": 0.0,
            "up_rows": 0,
            "down_rows": 0,
            "dominant_decision": "",
            "dominant_decision_share": 0.0,
            "inverse_rows": 0,
            "direct_rows": 0,
            "neutral_rows": 0,
            "dominant_relation": "",
            "dominant_relation_share": 0.0,
        }
    successes = sum(prepared.success[index] for index in indices)
    decision_counts = Counter(prepared.decision[index] for index in indices)
    relation_counts = Counter(prepared.relation[index] for index in indices)
    dominant_decision, dominant_decision_count = decision_counts.most_common(1)[0]
    dominant_relation, dominant_relation_count = relation_counts.most_common(1)[0]
    return {
        "rows": len(indices),
        "sessions": len({prepared.trading_day[index] for index in indices if prepared.trading_day[index]}),
        "tickers": len({prepared.ticker[index] for index in indices if prepared.ticker[index]}),
        "success_count": successes,
        "success_rate": successes / len(indices),
        "wilson_lower_95": wilson_lower_bound(successes, len(indices)),
        "mean_result_bps": statistics.fmean(prepared.result_bps[index] for index in indices),
        "mean_confidence": statistics.fmean(prepared.confidence[index] for index in indices),
        "max_day_share": _max_index_share(prepared.trading_day, indices),
        "max_ticker_share": _max_index_share(prepared.ticker, indices),
        "up_rows": decision_counts.get("up", 0),
        "down_rows": decision_counts.get("down", 0),
        "dominant_decision": dominant_decision,
        "dominant_decision_share": dominant_decision_count / len(indices),
        "inverse_rows": sum(1 for index in indices if prepared.relation[index] == "inverse"),
        "direct_rows": sum(1 for index in indices if prepared.relation[index] == "direct"),
        "neutral_rows": sum(1 for index in indices if prepared.relation[index] == "neutral"),
        "dominant_relation": dominant_relation,
        "dominant_relation_share": dominant_relation_count / len(indices),
    }


def accepted(metric: Mapping[str, Any], *, min_rows: int, min_sessions: int) -> bool:
    return bool(
        int(metric["rows"]) >= min_rows
        and int(metric["sessions"]) >= min_sessions
        and float(metric["success_rate"]) >= 0.90
        and float(metric["wilson_lower_95"]) >= 0.75
        and float(metric["mean_result_bps"]) > 0
        and float(metric["max_day_share"]) <= 0.20
        and float(metric["max_ticker_share"]) <= 0.25
    )


def temporal_rule_metric(
    prepared: PreparedRows,
    indices: frozenset[int],
    *,
    blocks: int = 5,
    min_blocks_with_selected: int = 3,
    min_block_success_rate: float = 0.75,
    require_positive_block_result: bool = True,
) -> dict[str, Any]:
    days = sorted({day for day in prepared.trading_day if day})
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
    block_metrics: list[dict[str, Any]] = []
    for block_index in range(block_count):
        start = block_index * len(days) // block_count
        end = (block_index + 1) * len(days) // block_count
        block_days = set(days[start:end])
        block_indices = frozenset(
            index
            for index in indices
            if prepared.trading_day[index] in block_days
        )
        if block_indices:
            block_metrics.append(metric_indices(prepared, block_indices))
    success_rates = [float(metric["success_rate"]) for metric in block_metrics]
    mean_results = [float(metric["mean_result_bps"]) for metric in block_metrics]
    weak_blocks = sum(
        1
        for metric in block_metrics
        if float(metric["success_rate"]) < min_block_success_rate
        or (require_positive_block_result and float(metric["mean_result_bps"]) <= 0)
    )
    return {
        "temporal_blocks": block_count,
        "temporal_blocks_with_selected": len(block_metrics),
        "temporal_weak_blocks": weak_blocks,
        "temporal_min_success_rate": min(success_rates) if success_rates else 0.0,
        "temporal_min_mean_result_bps": min(mean_results) if mean_results else 0.0,
        "temporal_supported": bool(
            len(block_metrics) >= min_blocks_with_selected
            and weak_blocks == 0
        ),
    }


def blocking_reasons(metric: Mapping[str, Any], *, min_rows: int, min_sessions: int) -> str:
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


def _signature(rule: Sequence[str]) -> str:
    raw = "\n".join(rule)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _support_id(indices: frozenset[int]) -> str:
    raw = ",".join(str(index) for index in sorted(indices))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _atom_field(atom: str) -> str:
    for separator in (">=", "<=", "=", ">", "<"):
        if separator in atom:
            return atom.split(separator, 1)[0]
    return atom


def _canonical_atom(atom: str) -> str:
    for separator in (">=", "<=", "=", ">", "<"):
        if separator not in atom:
            continue
        field, value = atom.split(separator, 1)
        canonical_field = ATOM_FIELD_ALIASES.get(field, field)
        return f"{canonical_field}{separator}{value}"
    return atom


def _has_redundant_field(rule: Sequence[str], atom: str) -> bool:
    existing = {_atom_field(item) for item in rule}
    return _atom_field(atom) in existing


def _additional_successes_needed(successes: int, rows: int, *, min_rows: int, target_rate: float = 0.90) -> int:
    denominator = max(rows, min_rows)
    required_successes = math.ceil(target_rate * denominator)
    return max(0, required_successes - successes)


def _success_deficit_at_current_rows(successes: int, rows: int, *, target_rate: float = 0.90) -> int:
    if rows <= 0:
        return 0
    return max(0, math.ceil(target_rate * rows) - successes)


def _future_success_requirement(
    successes: int,
    rows: int,
    *,
    min_rows: int,
    target_rate: float = 0.90,
) -> dict[str, float | int]:
    missing_rows = max(0, min_rows - rows)
    additional_successes = _additional_successes_needed(successes, rows, min_rows=min_rows, target_rate=target_rate)
    allowed_future_failures = max(0, missing_rows - additional_successes)
    required_future_success_rate = additional_successes / missing_rows if missing_rows else 0.0
    return {
        "current_successes_needed_for_90pct": _success_deficit_at_current_rows(
            successes,
            rows,
            target_rate=target_rate,
        ),
        "additional_successes_needed_for_90pct_at_min_rows": additional_successes,
        "allowed_future_failures_for_90pct_at_min_rows": allowed_future_failures,
        "required_future_success_rate_for_90pct_at_min_rows": required_future_success_rate,
        "can_reach_90pct_at_min_rows": additional_successes <= missing_rows,
    }


def _proof_viability(requirement: Mapping[str, object]) -> tuple[str, str]:
    if not bool(requirement.get("can_reach_90pct_at_min_rows")):
        return (
            "impossible_at_min_rows",
            "Even perfect future outcomes cannot reach 90% by the minimum-row gate.",
        )
    required_future_rate = float(requirement.get("required_future_success_rate_for_90pct_at_min_rows") or 0.0)
    if required_future_rate > 0.95:
        return (
            "near_perfect_forward_validation_required",
            "The candidate can reach 90% only with more than 95% future success.",
        )
    if required_future_rate > 0.90:
        return (
            "severe_forward_validation_required",
            "The candidate can reach 90%, but only with more than 90% future success.",
        )
    return (
        "forward_validation_possible",
        "The candidate can still reach 90% at the minimum-row gate without a near-perfect future run.",
    )


def _build_atom_index(row_atoms_by_index: Sequence[frozenset[str]]) -> dict[str, frozenset[int]]:
    index: dict[str, set[int]] = defaultdict(set)
    for row_index, atoms in enumerate(row_atoms_by_index):
        for atom in atoms:
            index[_canonical_atom(atom)].add(row_index)
    return {atom: frozenset(indices) for atom, indices in index.items()}


def _rule_rows(
    rows: Sequence[Mapping[str, Any]],
    row_atoms_by_index: Sequence[frozenset[str]],
    rule: frozenset[str],
) -> list[dict[str, Any]]:
    return [dict(row) for row, atoms in zip(rows, row_atoms_by_index) if rule.issubset(atoms)]


def _support_for_rule(atom_index: Mapping[str, frozenset[int]], rule: frozenset[str]) -> frozenset[int]:
    support: frozenset[int] | None = None
    for atom in rule:
        atom_support = atom_index.get(atom, frozenset())
        support = atom_support if support is None else support & atom_support
        if not support:
            return frozenset()
    return support or frozenset()


def _rows_by_indices(rows: Sequence[Mapping[str, Any]], indices: frozenset[int]) -> list[dict[str, Any]]:
    return [dict(rows[index]) for index in sorted(indices)]


def _candidate_rules(
    *,
    atom_index: Mapping[str, frozenset[int]],
    row_count: int,
    min_rows: int,
    max_terms: int,
    max_frequent_per_size: int,
) -> list[frozenset[str]]:
    frequent = [
        frozenset((atom,))
        for atom, indices in atom_index.items()
        if len(indices) >= min_rows
    ]
    by_size: dict[int, list[frozenset[str]]] = {1: sorted(frequent, key=lambda item: tuple(item))}
    all_rules: list[frozenset[str]] = list(by_size[1])
    support_cache: dict[frozenset[str], frozenset[int]] = {
        rule: atom_index[next(iter(rule))] for rule in by_size[1]
    }
    for size in range(2, max_terms + 1):
        previous = by_size.get(size - 1, [])
        generated: dict[frozenset[str], frozenset[int]] = {}
        for left_index, left in enumerate(previous):
            for right in previous[left_index + 1 :]:
                merged = frozenset(left | right)
                if len(merged) != size or merged in generated:
                    continue
                parents_exist = all(
                    frozenset(atom for atom in merged if atom != removed) in support_cache
                    for removed in merged
                )
                if not parents_exist:
                    continue
                support: frozenset[int] | None = None
                for atom in merged:
                    support = atom_index[atom] if support is None else support & atom_index[atom]
                    if len(support) < min_rows:
                        break
                if support is None or len(support) < min_rows:
                    continue
                support_cache[merged] = support
                generated[merged] = support
        ranked = sorted(
            generated,
            key=lambda rule: (len(generated[rule]), tuple(rule)),
            reverse=True,
        )
        by_size[size] = ranked[:max_frequent_per_size]
        all_rules.extend(by_size[size])
        if not by_size[size] or row_count < min_rows:
            break
    return all_rules


def mine_selective_rules(
    rows: Sequence[Mapping[str, Any]],
    *,
    discovery_fraction: float = 0.50,
    min_discovery_rows: int = 50,
    min_discovery_success_rate: float = 0.55,
    max_terms: int = 2,
    max_frequent_per_size: int = 3_000,
    top_n: int = 500,
    accepted_min_rows: int = 300,
    accepted_min_sessions: int = 30,
) -> list[dict[str, Any]]:
    eligible = eligible_rows(rows)
    discovery_rows, evaluation_rows = chronological_day_split(
        eligible,
        discovery_fraction=discovery_fraction,
    )
    discovery_atoms = [row_atoms(row) for row in discovery_rows]
    atom_index = _build_atom_index(discovery_atoms)
    discovery_prepared = prepare_rows(discovery_rows)
    evaluation_prepared = prepare_rows(evaluation_rows)
    evaluation_atom_index = _build_atom_index([row_atoms(row) for row in evaluation_rows])
    raw_rules = _candidate_rules(
        atom_index=atom_index,
        row_count=len(discovery_rows),
        min_rows=min_discovery_rows,
        max_terms=max_terms,
        max_frequent_per_size=max_frequent_per_size,
    )

    discovered: list[tuple[float, frozenset[str], dict[str, Any]]] = []
    for rule in raw_rules:
        discovery_metric = metric_indices(discovery_prepared, _support_for_rule(atom_index, rule))
        if float(discovery_metric["success_rate"]) < min_discovery_success_rate:
            continue
        score = (
            float(discovery_metric["success_rate"])
            + float(discovery_metric["wilson_lower_95"])
            + min(0.20, int(discovery_metric["rows"]) / 10_000)
        )
        discovered.append((score, rule, discovery_metric))

    result: list[dict[str, Any]] = []
    for _, rule, discovery_metric in sorted(discovered, key=lambda item: item[0], reverse=True)[:top_n]:
        evaluation_support = _support_for_rule(evaluation_atom_index, rule)
        evaluation_metric = metric_indices(evaluation_prepared, evaluation_support)
        temporal_metric = temporal_rule_metric(evaluation_prepared, evaluation_support)
        aggregate_accepted = accepted(
            evaluation_metric,
            min_rows=accepted_min_rows,
            min_sessions=accepted_min_sessions,
        )
        is_accepted = aggregate_accepted and bool(temporal_metric["temporal_supported"])
        reasons = blocking_reasons(
            evaluation_metric,
            min_rows=accepted_min_rows,
            min_sessions=accepted_min_sessions,
        )
        if aggregate_accepted and not bool(temporal_metric["temporal_supported"]):
            reasons = ",".join(item for item in (reasons, "temporal_instability") if item)
        ordered_rule = tuple(sorted(rule))
        result.append(
            {
                "rule_id": _signature(ordered_rule),
                "terms": len(ordered_rule),
                "rule": " | ".join(ordered_rule),
                "discovery_rows": discovery_metric["rows"],
                "discovery_sessions": discovery_metric["sessions"],
                "discovery_success_count": discovery_metric["success_count"],
                "discovery_success_rate": discovery_metric["success_rate"],
                "discovery_wilson_lower_95": discovery_metric["wilson_lower_95"],
                "discovery_mean_result_bps": discovery_metric["mean_result_bps"],
                "evaluation_rows": evaluation_metric["rows"],
                "evaluation_sessions": evaluation_metric["sessions"],
                "evaluation_tickers": evaluation_metric["tickers"],
                "evaluation_success_count": evaluation_metric["success_count"],
                "evaluation_success_rate": evaluation_metric["success_rate"],
                "evaluation_wilson_lower_95": evaluation_metric["wilson_lower_95"],
                "evaluation_mean_result_bps": evaluation_metric["mean_result_bps"],
                "evaluation_mean_confidence": evaluation_metric["mean_confidence"],
                "evaluation_max_day_share": evaluation_metric["max_day_share"],
                "evaluation_max_ticker_share": evaluation_metric["max_ticker_share"],
                "evaluation_direct_rows": evaluation_metric["direct_rows"],
                "evaluation_inverse_rows": evaluation_metric["inverse_rows"],
                "evaluation_neutral_rows": evaluation_metric["neutral_rows"],
                **temporal_metric,
                "accepted_shadow": is_accepted,
                "product_claim_allowed": False,
                "blocking_reasons": reasons,
            }
        )
    return sorted(
        result,
        key=lambda row: (
            bool(row["accepted_shadow"]),
            float(row["evaluation_success_rate"]),
            float(row["evaluation_wilson_lower_95"]),
            float(row["evaluation_mean_result_bps"]),
            int(row["evaluation_rows"]),
        ),
        reverse=True,
    )


def _scout_score(metric: Mapping[str, Any]) -> float:
    rows = int(metric["rows"])
    return (
        float(metric["success_rate"]) * 3.0
        + float(metric["wilson_lower_95"]) * 2.0
        + min(0.50, rows / 2_000)
        + max(-0.50, min(0.50, float(metric["mean_result_bps"]) / 100))
        - max(0.0, float(metric["max_day_share"]) - 0.20)
        - max(0.0, float(metric["max_ticker_share"]) - 0.25)
    )


def _precision_scout_preference(row: Mapping[str, Any]) -> tuple[object, ...]:
    positive_result = float(row.get("evaluation_mean_result_bps") or 0.0) > 0
    can_reach_90 = _boolish(row.get("can_reach_90pct_at_min_rows"))
    return (
        bool(row.get("accepted_shadow")),
        can_reach_90,
        positive_result,
        str(row.get("status")) == "watch_only",
        bool(row.get("discovery_gate_passed")),
        float(row.get("evaluation_success_rate") or 0.0),
        float(row.get("evaluation_wilson_lower_95") or 0.0),
        int(row.get("evaluation_sessions") or 0),
        int(row.get("evaluation_rows") or 0),
        -int(row.get("terms") or 0),
        float(row.get("evaluation_mean_result_bps") or 0.0),
    )


def _deduplicate_precision_scout_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    best_by_support: dict[tuple[object, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("_evaluation_support_id"),
            row.get("dominant_decision"),
            row.get("dominant_relation"),
        )
        current = best_by_support.get(key)
        candidate = dict(row)
        if current is None or _precision_scout_preference(candidate) > _precision_scout_preference(current):
            best_by_support[key] = candidate
    return list(best_by_support.values())


def _boolish(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _proof_next_action(row: Mapping[str, Any]) -> str:
    if _boolish(row.get("accepted_shadow")):
        return "shadow_validate"
    if not _boolish(row.get("can_reach_90pct_at_min_rows")):
        return "retire_for_90pct_min_row_gate"
    if float(row.get("evaluation_mean_result_bps") or 0.0) <= 0:
        return "reject_until_positive_after_costs"
    if str(row.get("status")) == "watch_only":
        return "forward_holdout_candidate"
    if str(row.get("proof_viability")) in {
        "severe_forward_validation_required",
        "near_perfect_forward_validation_required",
    }:
        return "collect_or_refine_features"
    return "research_backlog"


def _candidate_status(
    *,
    accepted_shadow: bool,
    discovery_gate_passed: bool,
    evaluation_rows: int,
    proof_viability: str,
) -> str:
    if accepted_shadow:
        return "shadow"
    if not discovery_gate_passed:
        return "discovery_weak"
    if not evaluation_rows:
        return "no_later_evidence"
    if proof_viability == "impossible_at_min_rows":
        return "retired_90_impossible"
    return "watch_only"


def summarize_precision_scout_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    viability_counts = Counter(str(row.get("proof_viability", "")) for row in rows)
    status_counts = Counter(str(row.get("status", "")) for row in rows)
    next_action_counts = Counter(str(row.get("proof_next_action", "")) for row in rows)
    return {
        "candidate_rows": len(rows),
        "accepted_shadow": sum(1 for row in rows if _boolish(row.get("accepted_shadow"))),
        "watch_only": sum(1 for row in rows if str(row.get("status")) == "watch_only"),
        "positive_result_rows": sum(1 for row in rows if float(row.get("evaluation_mean_result_bps") or 0.0) > 0),
        "can_reach_90pct_at_min_rows": sum(1 for row in rows if _boolish(row.get("can_reach_90pct_at_min_rows"))),
        "proof_viability_counts": dict(sorted(viability_counts.items())),
        "status_counts": dict(sorted(status_counts.items())),
        "next_action_counts": dict(sorted(next_action_counts.items())),
        "best_evaluation_success_rate": (
            max(float(row.get("evaluation_success_rate") or 0.0) for row in rows)
            if rows
            else None
        ),
        "best_evaluation_wilson_lower_95": (
            max(float(row.get("evaluation_wilson_lower_95") or 0.0) for row in rows)
            if rows
            else None
        ),
    }


def mine_precision_scout_rules(
    rows: Sequence[Mapping[str, Any]],
    *,
    discovery_fraction: float = 0.50,
    min_discovery_rows: int = 20,
    min_discovery_sessions: int = 5,
    min_discovery_success_rate: float = 0.65,
    expansion_min_success_rate: float = 0.15,
    max_terms: int = 4,
    beam_width: int = 250,
    top_n: int = 500,
    accepted_min_rows: int = 300,
    accepted_min_sessions: int = 30,
) -> list[dict[str, Any]]:
    """Search rare strict rules for a three-way decision policy.

    This is intentionally a scout, not a product policy. It can surface rare
    states that look promising on early days, but acceptance is based only on
    later days and still requires enough rows, enough sessions, 90% observed
    success, a strong lower reliability bound, positive result after costs,
    and no day/ticker concentration.
    """

    eligible = eligible_rows(rows)
    discovery_rows, evaluation_rows = chronological_day_split(
        eligible,
        discovery_fraction=discovery_fraction,
    )
    discovery_atoms = [row_atoms(row) for row in discovery_rows]
    evaluation_atoms = [row_atoms(row) for row in evaluation_rows]
    atom_index = _build_atom_index(discovery_atoms)
    evaluation_atom_index = _build_atom_index(evaluation_atoms)
    discovery_prepared = prepare_rows(discovery_rows)
    evaluation_prepared = prepare_rows(evaluation_rows)

    frequent_atoms = sorted(
        atom
        for atom, indices in atom_index.items()
        if len(indices) >= min_discovery_rows
    )
    if not frequent_atoms:
        return []

    discovered: dict[frozenset[str], dict[str, Any]] = {}
    current_level: list[tuple[float, frozenset[str], frozenset[int], dict[str, Any]]] = []

    for atom in frequent_atoms:
        rule = frozenset((atom,))
        indices = atom_index[atom]
        metric = metric_indices(discovery_prepared, indices)
        if (
            int(metric["rows"]) >= min_discovery_rows
            and int(metric["sessions"]) >= min_discovery_sessions
            and float(metric["success_rate"]) >= expansion_min_success_rate
        ):
            discovered[rule] = metric
            current_level.append((_scout_score(metric), rule, indices, metric))

    current_level = sorted(current_level, key=lambda item: item[0], reverse=True)[:beam_width]

    for _depth in range(2, max_terms + 1):
        generated: dict[frozenset[str], tuple[frozenset[int], dict[str, Any]]] = {}
        for _score, rule, indices, _metric in current_level:
            last_atom = max(rule)
            for atom in frequent_atoms:
                if atom <= last_atom or atom in rule or _has_redundant_field(tuple(rule), atom):
                    continue
                support = indices & atom_index.get(atom, frozenset())
                if len(support) < min_discovery_rows:
                    continue
                candidate = frozenset(set(rule) | {atom})
                if candidate in generated:
                    continue
                metric = metric_indices(discovery_prepared, support)
                if (
                    int(metric["sessions"]) < min_discovery_sessions
                    or float(metric["success_rate"]) < expansion_min_success_rate
                ):
                    continue
                generated[candidate] = (support, metric)
                discovered[candidate] = metric

        current_level = sorted(
            (
                (_scout_score(metric), rule, support, metric)
                for rule, (support, metric) in generated.items()
            ),
            key=lambda item: item[0],
            reverse=True,
        )[:beam_width]
        if not current_level:
            break

    ranked_discovery = sorted(
        discovered.items(),
        key=lambda item: _scout_score(item[1]),
        reverse=True,
    )[:top_n]

    result: list[dict[str, Any]] = []
    for rule, discovery_metric in ranked_discovery:
        discovery_support = _support_for_rule(atom_index, rule)
        evaluation_support = _support_for_rule(evaluation_atom_index, rule)
        evaluation_metric = metric_indices(evaluation_prepared, evaluation_support)
        temporal_metric = temporal_rule_metric(evaluation_prepared, evaluation_support)
        discovery_gate_passed = bool(
            int(discovery_metric["rows"]) >= min_discovery_rows
            and int(discovery_metric["sessions"]) >= min_discovery_sessions
            and float(discovery_metric["success_rate"]) >= min_discovery_success_rate
        )
        evaluation_accepted = accepted(
            evaluation_metric,
            min_rows=accepted_min_rows,
            min_sessions=accepted_min_sessions,
        )
        is_accepted = (
            discovery_gate_passed
            and evaluation_accepted
            and bool(temporal_metric["temporal_supported"])
        )
        ordered_rule = tuple(sorted(rule))
        successes = int(evaluation_metric["success_count"])
        evaluation_rows = int(evaluation_metric["rows"])
        evaluation_sessions = int(evaluation_metric["sessions"])
        future_requirement = _future_success_requirement(
            successes,
            evaluation_rows,
            min_rows=accepted_min_rows,
        )
        proof_viability, proof_viability_reason = _proof_viability(future_requirement)
        reason_items = [
            item
            for item in blocking_reasons(
                evaluation_metric,
                min_rows=accepted_min_rows,
                min_sessions=accepted_min_sessions,
            ).split(",")
            if item
        ]
        if not discovery_gate_passed:
            reason_items.append("discovery_success_rate")
        if evaluation_accepted and not bool(temporal_metric["temporal_supported"]):
            reason_items.append("temporal_instability")
        missing_reasons = ",".join(dict.fromkeys(reason_items))
        status = _candidate_status(
            accepted_shadow=is_accepted,
            discovery_gate_passed=discovery_gate_passed,
            evaluation_rows=evaluation_rows,
            proof_viability=proof_viability,
        )
        result.append(
            {
                "rule_id": _signature(ordered_rule),
                "_discovery_support_id": _support_id(discovery_support),
                "_evaluation_support_id": _support_id(evaluation_support),
                "terms": len(ordered_rule),
                "rule": " | ".join(ordered_rule),
                "discovery_rows": discovery_metric["rows"],
                "discovery_sessions": discovery_metric["sessions"],
                "discovery_success_count": discovery_metric["success_count"],
                "discovery_success_rate": discovery_metric["success_rate"],
                "discovery_wilson_lower_95": discovery_metric["wilson_lower_95"],
                "discovery_mean_result_bps": discovery_metric["mean_result_bps"],
                "discovery_gate_passed": discovery_gate_passed,
                "evaluation_rows": evaluation_rows,
                "evaluation_sessions": evaluation_sessions,
                "evaluation_tickers": evaluation_metric["tickers"],
                "evaluation_success_count": successes,
                "evaluation_success_rate": evaluation_metric["success_rate"],
                "evaluation_wilson_lower_95": evaluation_metric["wilson_lower_95"],
                "evaluation_mean_result_bps": evaluation_metric["mean_result_bps"],
                "evaluation_mean_confidence": evaluation_metric["mean_confidence"],
                "evaluation_max_day_share": evaluation_metric["max_day_share"],
                "evaluation_max_ticker_share": evaluation_metric["max_ticker_share"],
                "evaluation_up_rows": evaluation_metric["up_rows"],
                "evaluation_down_rows": evaluation_metric["down_rows"],
                "dominant_decision": evaluation_metric["dominant_decision"],
                "dominant_decision_share": evaluation_metric["dominant_decision_share"],
                "evaluation_direct_rows": evaluation_metric["direct_rows"],
                "evaluation_inverse_rows": evaluation_metric["inverse_rows"],
                "evaluation_neutral_rows": evaluation_metric["neutral_rows"],
                "dominant_relation": evaluation_metric["dominant_relation"],
                "dominant_relation_share": evaluation_metric["dominant_relation_share"],
                **temporal_metric,
                "missing_rows_to_shadow_gate": max(0, accepted_min_rows - evaluation_rows),
                "missing_sessions_to_shadow_gate": max(0, accepted_min_sessions - evaluation_sessions),
                **future_requirement,
                "proof_viability": proof_viability,
                "proof_viability_reason": proof_viability_reason,
                "accepted_shadow": is_accepted,
                "product_claim_allowed": False,
                "status": status,
                "blocking_reasons": missing_reasons,
            }
        )
        result[-1]["proof_next_action"] = _proof_next_action(result[-1])
    return sorted(
        _deduplicate_precision_scout_rows(result),
        key=_precision_scout_preference,
        reverse=True,
    )


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "rule_id",
        "terms",
        "rule",
        "discovery_rows",
        "discovery_sessions",
        "discovery_success_count",
        "discovery_success_rate",
        "discovery_wilson_lower_95",
        "discovery_mean_result_bps",
        "evaluation_rows",
        "evaluation_sessions",
        "evaluation_tickers",
        "evaluation_success_count",
        "evaluation_success_rate",
        "evaluation_wilson_lower_95",
        "evaluation_mean_result_bps",
        "evaluation_mean_confidence",
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


def write_precision_scout_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "rule_id",
        "_discovery_support_id",
        "_evaluation_support_id",
        "terms",
        "rule",
        "discovery_rows",
        "discovery_sessions",
        "discovery_success_count",
        "discovery_success_rate",
        "discovery_wilson_lower_95",
        "discovery_mean_result_bps",
        "discovery_gate_passed",
        "evaluation_rows",
        "evaluation_sessions",
        "evaluation_tickers",
        "evaluation_success_count",
        "evaluation_success_rate",
        "evaluation_wilson_lower_95",
        "evaluation_mean_result_bps",
        "evaluation_mean_confidence",
        "evaluation_max_day_share",
        "evaluation_max_ticker_share",
        "evaluation_up_rows",
        "evaluation_down_rows",
        "dominant_decision",
        "dominant_decision_share",
        "evaluation_direct_rows",
        "evaluation_inverse_rows",
        "evaluation_neutral_rows",
        "dominant_relation",
        "dominant_relation_share",
        "temporal_blocks",
        "temporal_blocks_with_selected",
        "temporal_weak_blocks",
        "temporal_min_success_rate",
        "temporal_min_mean_result_bps",
        "temporal_supported",
        "missing_rows_to_shadow_gate",
        "missing_sessions_to_shadow_gate",
        "current_successes_needed_for_90pct",
        "additional_successes_needed_for_90pct_at_min_rows",
        "allowed_future_failures_for_90pct_at_min_rows",
        "required_future_success_rate_for_90pct_at_min_rows",
        "can_reach_90pct_at_min_rows",
        "proof_viability",
        "proof_viability_reason",
        "proof_next_action",
        "accepted_shadow",
        "product_claim_allowed",
        "status",
        "blocking_reasons",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    accepted_rows = [row for row in rows if row.get("accepted_shadow")]
    lines = [
        "# Selective rule mining",
        "",
        f"- Accepted shadow rules: {len(accepted_rows)}",
        f"- Candidate rules evaluated: {len(rows)}",
        "",
        "## Top evaluated rules",
        "",
        "| Rule | Terms | Eval rows | Eval sessions | Success rate | Wilson lower 95% | Mean result bps | Temporal supported | Weak blocks | Blocking reasons |",
        "|---|---:|---:|---:|---:|---:|---:|---|---:|---|",
    ]
    for row in rows[:30]:
        lines.append(
            "| {rule} | {terms} | {rows} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {temporal} | {weak} | {reasons} |".format(
                rule=str(row["rule"]).replace("|", "\\|"),
                terms=row["terms"],
                rows=row["evaluation_rows"],
                sessions=row["evaluation_sessions"],
                rate=float(row["evaluation_success_rate"]),
                lower=float(row["evaluation_wilson_lower_95"]),
                mean=float(row["evaluation_mean_result_bps"]),
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
            "This report searches conjunctive rules discovered on the earlier trading days "
            "and evaluates them on later trading days. It is intentionally strict: a rule "
            "can become a shadow candidate only after it has enough later observations, "
            "enough trading days, at least 90% observed success, a strong lower reliability "
            "bound, positive result after costs, no day or ticker concentration, and no weak "
            "later time block.",
            "",
            "A high discovery score with a weak evaluation score is treated as overfitting, "
            "not as product evidence.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def write_precision_scout_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    accepted_rows = [row for row in rows if row.get("accepted_shadow")]
    watch_rows = [row for row in rows if row.get("status") == "watch_only"]
    summary = summarize_precision_scout_rows(rows)
    lines = [
        "# Precision scout rule mining",
        "",
        f"- Accepted shadow rules: {len(accepted_rows)}",
        f"- Watch-only rules: {len(watch_rows)}",
        f"- Candidate rules evaluated: {len(rows)}",
        f"- Positive result rows: {summary['positive_result_rows']}",
        f"- Can reach 90% at 300 rows: {summary['can_reach_90pct_at_min_rows']}",
        "",
        "## Proof viability summary",
        "",
        "| Category | Count |",
        "|---|---:|",
    ]
    for category, count in dict(summary["proof_viability_counts"]).items():
        lines.append(f"| {category} | {count} |")
    lines.extend(
        [
            "",
            "## Next action summary",
            "",
            "| Action | Count |",
            "|---|---:|",
        ]
    )
    for action, count in dict(summary["next_action_counts"]).items():
        lines.append(f"| {action} | {count} |")
    lines.extend(
        [
            "",
        "## Top later-day rules",
        "",
            "| Rule | Direction | Hypothesis | Terms | Eval rows | Eval sessions | Success rate | Wilson lower 95% | Mean result bps | Temporal supported | Weak blocks | Missing rows | Missing days | Extra successes needed | Allowed future failures | Required future success | Proof viability | Next action | Status | Blocking reasons |",
            "|---|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|---|",
        ]
    )
    for row in rows[:30]:
        lines.append(
            "| {rule} | {direction} | {relation} | {terms} | {rows} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {temporal} | {weak} | {missing_rows} | {missing_days} | {extra_successes} | {allowed_failures} | {future_rate:.4f} | {viability} | {next_action} | {status} | {reasons} |".format(
                rule=str(row["rule"]).replace("|", "\\|"),
                direction=row.get("dominant_decision", ""),
                relation=row.get("dominant_relation", ""),
                terms=row["terms"],
                rows=row["evaluation_rows"],
                sessions=row["evaluation_sessions"],
                rate=float(row["evaluation_success_rate"]),
                lower=float(row["evaluation_wilson_lower_95"]),
                mean=float(row["evaluation_mean_result_bps"]),
                temporal=row.get("temporal_supported", ""),
                weak=row.get("temporal_weak_blocks", ""),
                missing_rows=row["missing_rows_to_shadow_gate"],
                missing_days=row["missing_sessions_to_shadow_gate"],
                extra_successes=row["additional_successes_needed_for_90pct_at_min_rows"],
                allowed_failures=row["allowed_future_failures_for_90pct_at_min_rows"],
                future_rate=float(row["required_future_success_rate_for_90pct_at_min_rows"]),
                viability=row["proof_viability"],
                next_action=row["proof_next_action"],
                status=row["status"],
                reasons=row["blocking_reasons"],
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This scout deliberately searches narrower multi-condition states than the "
            "standard selective rule report. It is designed to answer whether a rare "
            "90% subset may exist. It does not authorize a product claim: all rules "
            "are rechecked on later trading days and must still pass the same sample "
            "size, trading-day, lower-bound, result, and concentration gates.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-mine-selective-rules")
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-discovery-rows", type=int, default=50)
    parser.add_argument("--min-discovery-success-rate", type=float, default=0.55)
    parser.add_argument("--max-terms", type=int, default=2)
    parser.add_argument("--max-frequent-per-size", type=int, default=3_000)
    parser.add_argument("--top-n", type=int, default=500)
    parser.add_argument("--accepted-min-rows", type=int, default=300)
    parser.add_argument("--accepted-min-sessions", type=int, default=30)
    parser.add_argument(
        "--precision-scout",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run the stricter beam-search scout instead of the standard frequent-rule miner.",
    )
    parser.add_argument("--beam-width", type=int, default=250)
    parser.add_argument("--expansion-min-success-rate", type=float, default=0.15)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = _read_csv(args.audit)
    if args.precision_scout:
        candidates = mine_precision_scout_rules(
            rows,
            min_discovery_rows=args.min_discovery_rows,
            min_discovery_success_rate=args.min_discovery_success_rate,
            expansion_min_success_rate=args.expansion_min_success_rate,
            max_terms=args.max_terms,
            beam_width=args.beam_width,
            top_n=args.top_n,
            accepted_min_rows=args.accepted_min_rows,
            accepted_min_sessions=args.accepted_min_sessions,
        )
    else:
        candidates = mine_selective_rules(
            rows,
            min_discovery_rows=args.min_discovery_rows,
            min_discovery_success_rate=args.min_discovery_success_rate,
            max_terms=args.max_terms,
            max_frequent_per_size=args.max_frequent_per_size,
            top_n=args.top_n,
            accepted_min_rows=args.accepted_min_rows,
            accepted_min_sessions=args.accepted_min_sessions,
        )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / (
        "precision-scout-candidates.csv" if args.precision_scout else "selective-rule-candidates.csv"
    )
    report_path = args.output_dir / (
        "precision-scout-report.md" if args.precision_scout else "selective-rule-report.md"
    )
    if args.precision_scout:
        write_precision_scout_csv(csv_path, candidates)
        write_precision_scout_report(report_path, candidates)
    else:
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
                "mode": "precision_scout" if args.precision_scout else "standard",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
