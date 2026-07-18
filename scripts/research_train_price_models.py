#!/usr/bin/env python3
"""Train and evaluate offline signal-triggered price prediction baselines."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    MATERIALITY_BPS,
    ROUND_TRIP_COST_BPS,
    bayesian_score_summary,
    chronological_split,
    dataset_feature_columns,
    event_study_summary,
    fingerprint_records,
    float_or_none,
    read_table,
    render_markdown_report,
    wilson_lower_bound,
    write_csv_records,
    write_json,
)
from research_export_safe_triage_decisions import (  # noqa: E402
    export_safe_triage_rows,
    summarize_safe_triage_rows,
    write_safe_triage_csv,
    write_safe_triage_report,
)
from research_confidence_band_audit import (  # noqa: E402
    build_confidence_band_audit,
    write_csv as write_confidence_band_audit_csv,
    write_report as write_confidence_band_audit_report,
)
from research_mine_selective_rules import (  # noqa: E402
    mine_precision_scout_rules,
    mine_selective_rules,
    summarize_precision_scout_rows,
    write_precision_scout_csv,
    write_precision_scout_report,
    write_csv as write_selective_rules_csv,
    write_report as write_selective_rules_report,
)
from research_mine_false_positive_guards import (  # noqa: E402
    mine_false_positive_guards,
    summarize_guards as summarize_false_positive_guards,
    write_csv as write_false_positive_guard_csv,
    write_report as write_false_positive_guard_report,
)
from research_mine_directional_states import (  # noqa: E402
    mine_directional_state_candidates,
    write_csv as write_directional_state_csv,
    write_report as write_directional_state_report,
)
from research_mine_honest_market_states import (  # noqa: E402
    mine_states as mine_honest_market_states,
    write_report as write_honest_market_state_report,
)
from research_report_90_selection import write_selection_report  # noqa: E402


CONFIDENCE_THRESHOLDS = (0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.75, 0.85, 0.90, 0.92, 0.95)
SELECTIVE_FRONTIER_COUNTS = (20, 50, 100, 300, 1_000, 3_000, 10_000)
CALIBRATION_BINS = 20
POLICY_DECISION_THRESHOLD = 0.60
TARGET_SUCCESS_RATE = 0.90
PRODUCT_CLAIM_MIN_WILSON_LOWER = 0.90
SHADOW_MIN_WILSON_LOWER = 0.75
HIGH_CONFIDENCE_SLICE_THRESHOLDS = (0.30, 0.40, 0.50, 0.60, 0.75, 0.85, 0.90)
BAYESIAN_STATE_GROUPS = (
    ("signal_horizon", ("signal_type", "horizon_seconds")),
    ("signal_session_horizon", ("signal_type", "session_bucket", "horizon_seconds")),
    ("signal_volatility_horizon", ("signal_type", "volatility_bucket", "horizon_seconds")),
    ("signal_combo_horizon", ("signal_type", "combo_key_300s", "horizon_seconds")),
    (
        "signal_session_volatility_horizon",
        ("signal_type", "session_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "signal_cluster_volatility_horizon",
        ("signal_type", "signal_count_bucket", "volatility_bucket", "horizon_seconds"),
    ),
    (
        "signal_consolidation_liquidity_horizon",
        ("signal_type", "consolidation_bucket", "liquidity_bucket", "horizon_seconds"),
    ),
    (
        "signal_trend_context_horizon",
        ("signal_type", "pre_trend_bucket", "pre_trend_strength_bucket", "horizon_seconds"),
    ),
    (
        "signal_trend_market_context_horizon",
        ("signal_type", "pre_trend_bucket", "market_alignment_bucket", "horizon_seconds"),
    ),
    (
        "signal_microstructure_horizon",
        ("signal_type", "spread_bucket", "depth_bucket", "imbalance_bucket", "horizon_seconds"),
    ),
)


def _target_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("meta_label")) in {"0", "1"}]


def _validation_sessions(rows: Sequence[Mapping[str, Any]]) -> int:
    return len({str(row["trading_day"]) for row in rows})


def _naive_positive_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    selected = _target_rows(rows)
    if not selected:
        return None
    return sum(1 for row in selected if str(row["meta_label"]) == "1") / len(selected)


def _feature_dicts(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[int], list[float], list[str]]:
    numeric, categorical = dataset_feature_columns(rows)
    features: list[dict[str, Any]] = []
    labels: list[int] = []
    returns: list[float] = []
    ids: list[str] = []
    for row in _target_rows(rows):
        item: dict[str, Any] = {}
        for column in numeric:
            item[column] = float_or_none(row.get(column)) or 0.0
        for column in categorical:
            item[column] = str(row.get(column, ""))
        features.append(item)
        labels.append(int(str(row["meta_label"])))
        returns.append(float_or_none(row.get("cost_adjusted_directional_bps")) or 0.0)
        ids.append(str(row["row_id"]))
    return features, labels, returns, ids


def _directional_target_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        forward_return = float_or_none(row.get("forward_return_bps"))
        if forward_return is None:
            continue
        item = dict(row)
        item["_forward_return_bps"] = forward_return
        item["_up_target"] = int(forward_return - ROUND_TRIP_COST_BPS >= MATERIALITY_BPS)
        item["_down_target"] = int(-forward_return - ROUND_TRIP_COST_BPS >= MATERIALITY_BPS)
        result.append(item)
    return result


def _directional_feature_dicts(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[int], list[int], list[float], list[str]]:
    numeric, categorical = dataset_feature_columns(rows)
    features: list[dict[str, Any]] = []
    up_labels: list[int] = []
    down_labels: list[int] = []
    returns: list[float] = []
    ids: list[str] = []
    for row in _directional_target_rows(rows):
        item: dict[str, Any] = {}
        for column in numeric:
            item[column] = float_or_none(row.get(column)) or 0.0
        for column in categorical:
            item[column] = str(row.get(column, ""))
        features.append(item)
        up_labels.append(int(row["_up_target"]))
        down_labels.append(int(row["_down_target"]))
        returns.append(float(row["_forward_return_bps"]))
        ids.append(str(row["row_id"]))
    return features, up_labels, down_labels, returns, ids


def _classification_metrics(y_true: Sequence[int], probabilities: Sequence[float]) -> dict[str, Any]:
    if not y_true:
        return {"status": "no_validation_rows"}
    predictions = [1 if score >= 0.5 else 0 for score in probabilities]
    tp = sum(1 for y, p in zip(y_true, predictions) if y == 1 and p == 1)
    tn = sum(1 for y, p in zip(y_true, predictions) if y == 0 and p == 0)
    fp = sum(1 for y, p in zip(y_true, predictions) if y == 0 and p == 1)
    fn = sum(1 for y, p in zip(y_true, predictions) if y == 1 and p == 0)
    brier = statistics.fmean((y - score) ** 2 for y, score in zip(y_true, probabilities))
    return {
        "accuracy": (tp + tn) / len(y_true),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "brier_score": brier,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def _fit_lightgbm_probabilities(
    train_x: Sequence[Mapping[str, Any]],
    train_y: Sequence[int],
    valid_x: Sequence[Mapping[str, Any]],
    *,
    model_name: str,
) -> tuple[str, list[float]]:
    if len(set(train_y)) < 2 or not valid_x:
        return "insufficient_classes_or_rows", []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError:
        return "not_available_install_research_extra", []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LGBMClassifier(
        n_estimators=250,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260716,
        class_weight="balanced",
        verbose=-1,
    )
    model.fit(x_train, train_y)
    probabilities = [float(item) for item in model.predict_proba(x_valid)[:, 1]]
    return f"{model_name}_ok", probabilities


def _calibration_bins(
    probabilities: Sequence[float],
    labels: Sequence[int],
    *,
    bins: int = CALIBRATION_BINS,
) -> list[dict[str, Any]]:
    pairs = sorted((float(probability), int(label)) for probability, label in zip(probabilities, labels))
    if not pairs:
        return []
    result: list[dict[str, Any]] = []
    for index in range(bins):
        start = index * len(pairs) // bins
        end = (index + 1) * len(pairs) // bins
        group = pairs[start:end]
        if not group:
            continue
        positives = sum(label for _, label in group)
        # Laplace smoothing avoids impossible 0%/100% estimates on tiny bins.
        observed_rate = (positives + 1) / (len(group) + 2)
        result.append(
            {
                "min_probability": group[0][0],
                "max_probability": group[-1][0],
                "n": len(group),
                "positives": positives,
                "observed_rate": observed_rate,
            }
        )
    monotone = 0.0
    for row in result:
        monotone = max(monotone, float(row["observed_rate"]))
        row["calibrated_rate"] = monotone
    return result


def _apply_calibration(probability: float, bins: Sequence[Mapping[str, Any]]) -> float:
    if not bins:
        return float(probability)
    for row in bins:
        if probability <= float(row["max_probability"]):
            return float(row["calibrated_rate"])
    return float(bins[-1]["calibrated_rate"])


def _decision_relation(row: Mapping[str, Any], decision: str) -> str:
    original_direction = int(float(row.get("direction") or 0))
    decision_direction = 1 if decision == "up" else -1 if decision == "down" else 0
    if original_direction == 0 or decision_direction == 0:
        return "neutral"
    return "direct" if original_direction == decision_direction else "inverse"


def _decision_for_threshold(row: Mapping[str, Any], threshold: float) -> tuple[str, int, float, float, str]:
    up_probability = float(row.get("_up_probability", 0.0))
    down_probability = float(row.get("_down_probability", 0.0))
    forward_return = float(row.get("_forward_return_bps", 0.0))
    if up_probability < threshold and down_probability < threshold:
        return "skip", 0, max(up_probability, down_probability), 0.0, "neutral"
    if up_probability >= down_probability:
        result_bps = forward_return - ROUND_TRIP_COST_BPS
        return "up", int(row.get("_up_target", 0)), up_probability, result_bps, _decision_relation(row, "up")
    result_bps = -forward_return - ROUND_TRIP_COST_BPS
    return "down", int(row.get("_down_target", 0)), down_probability, result_bps, _decision_relation(row, "down")


def _forced_directional_decision(row: Mapping[str, Any]) -> tuple[str, int, float, float, str]:
    up_probability = float(row.get("_up_probability", 0.0))
    down_probability = float(row.get("_down_probability", 0.0))
    forward_return = float(row.get("_forward_return_bps", 0.0))
    if up_probability >= down_probability:
        result_bps = forward_return - ROUND_TRIP_COST_BPS
        return "up", int(row.get("_up_target", 0)), up_probability, result_bps, _decision_relation(row, "up")
    result_bps = -forward_return - ROUND_TRIP_COST_BPS
    return "down", int(row.get("_down_target", 0)), down_probability, result_bps, _decision_relation(row, "down")


def _frontier_summary_row(
    *,
    scope: str,
    rule: str,
    population: int,
    selected: Sequence[tuple[Mapping[str, Any], str, int, float, float, str]],
    accepted_min_n: int,
    accepted_min_sessions: int,
    accepted_min_lower_bound: float,
    accepted_min_success_rate: float,
) -> dict[str, Any]:
    successes = sum(item[2] for item in selected)
    selected_trading_days = sorted({str(item[0].get("trading_day")) for item in selected if item[0].get("trading_day")})
    sessions = len(selected_trading_days)
    success_rate = successes / len(selected) if selected else None
    lower = wilson_lower_bound(successes, len(selected))
    mean_result = statistics.fmean(item[4] for item in selected) if selected else None
    min_confidence = min((item[3] for item in selected), default=None)
    max_confidence = max((item[3] for item in selected), default=None)
    return {
        "scope": scope,
        "rule": rule,
        "group_population": population,
        "selected_rows": len(selected),
        "sessions": sessions,
        "selected_trading_days": "|".join(selected_trading_days),
        "min_confidence": min_confidence,
        "max_confidence": max_confidence,
        "up_decisions": sum(1 for _, decision, *_ in selected if decision == "up"),
        "down_decisions": sum(1 for _, decision, *_ in selected if decision == "down"),
        "direct_decisions": sum(1 for *_, relation in selected if relation == "direct"),
        "inverse_decisions": sum(1 for *_, relation in selected if relation == "inverse"),
        "neutral_decisions": sum(1 for *_, relation in selected if relation == "neutral"),
        "success_count": successes,
        "success_rate": success_rate,
        "wilson_lower_95": lower,
        "mean_selected_result_bps": mean_result,
        "target_success_rate": accepted_min_success_rate,
        "observed_90_success": bool(success_rate is not None and success_rate >= TARGET_SUCCESS_RATE),
        "reliable_90_success": bool(lower is not None and lower >= PRODUCT_CLAIM_MIN_WILSON_LOWER),
        "accepted_research": bool(
            len(selected) >= accepted_min_n
            and sessions >= accepted_min_sessions
            and success_rate is not None
            and success_rate >= accepted_min_success_rate
            and lower is not None
            and lower >= accepted_min_lower_bound
            and mean_result is not None
            and mean_result > 0
        ),
    }


def selective_frontier_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    counts: Sequence[int] = SELECTIVE_FRONTIER_COUNTS,
    min_report_n: int = 20,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    accepted_min_lower_bound: float = SHADOW_MIN_WILSON_LOWER,
    accepted_min_success_rate: float = TARGET_SUCCESS_RATE,
) -> list[dict[str, Any]]:
    enriched: list[tuple[Mapping[str, Any], str, int, float, float, str]] = []
    for row in scored_rows:
        decision, success, probability, result_bps, relation = _forced_directional_decision(row)
        enriched.append((row, decision, success, probability, result_bps, relation))

    group_sets = [
        ("all", ()),
        ("decision_horizon", ("decision", "horizon_seconds")),
        ("decision_signal_horizon", ("decision", "signal_type", "horizon_seconds")),
        ("decision_relation_signal_horizon", ("decision_relation", "decision", "signal_type", "horizon_seconds")),
        (
            "decision_signal_session_volatility_horizon",
            ("decision", "signal_type", "session_bucket", "volatility_bucket", "horizon_seconds"),
        ),
        (
            "decision_signal_cluster_volatility_horizon",
            ("decision", "signal_type", "signal_count_bucket", "volatility_bucket", "horizon_seconds"),
        ),
        (
            "decision_signal_consolidation_liquidity_horizon",
            ("decision", "signal_type", "consolidation_bucket", "liquidity_bucket", "horizon_seconds"),
        ),
        (
            "decision_signal_microstructure_horizon",
            ("decision", "signal_type", "spread_bucket", "depth_bucket", "imbalance_bucket", "horizon_seconds"),
        ),
    ]

    result: list[dict[str, Any]] = []
    for scope, keys in group_sets:
        grouped: dict[tuple[str, ...], list[tuple[Mapping[str, Any], str, int, float, float, str]]] = {}
        for item in enriched:
            row, decision, _, _, _, relation = item
            if not keys:
                group_key: tuple[str, ...] = ()
            else:
                group_key = tuple(_slice_value(row, decision, relation, key) for key in keys)
            if any(value == "unknown" for value in group_key):
                continue
            grouped.setdefault(group_key, []).append(item)

        for group_key, items in grouped.items():
            if len(items) < min_report_n:
                continue
            sorted_items = sorted(
                items,
                key=lambda item: (
                    item[3],
                    str(item[0].get("row_id", "")),
                ),
                reverse=True,
            )
            rule = "all" if not keys else " | ".join(f"{key}={value}" for key, value in zip(keys, group_key))
            emitted_counts: set[int] = set()
            for count in counts:
                if count > len(sorted_items):
                    continue
                emitted_counts.add(int(count))
                result.append(
                    _frontier_summary_row(
                        scope=scope,
                        rule=rule,
                        population=len(items),
                        selected=sorted_items[: int(count)],
                        accepted_min_n=accepted_min_n,
                        accepted_min_sessions=accepted_min_sessions,
                        accepted_min_lower_bound=accepted_min_lower_bound,
                        accepted_min_success_rate=accepted_min_success_rate,
                    )
                )
            if len(sorted_items) >= accepted_min_n and accepted_min_n not in emitted_counts:
                result.append(
                    _frontier_summary_row(
                        scope=scope,
                        rule=rule,
                        population=len(items),
                        selected=sorted_items[:accepted_min_n],
                        accepted_min_n=accepted_min_n,
                        accepted_min_sessions=accepted_min_sessions,
                        accepted_min_lower_bound=accepted_min_lower_bound,
                        accepted_min_success_rate=accepted_min_success_rate,
                    )
                )

    return sorted(
        result,
        key=lambda row: (
            bool(row["accepted_research"]),
            float(row["success_rate"] or 0.0),
            float(row["wilson_lower_95"] or 0.0),
            float(row["mean_selected_result_bps"] or -1_000_000.0),
            int(row["selected_rows"]),
        ),
        reverse=True,
    )


def _minimum_successes_for_wilson(total: int, target_lower_bound: float) -> int | None:
    if total <= 0:
        return None
    for successes in range(total + 1):
        lower = wilson_lower_bound(successes, total)
        if lower is not None and lower >= target_lower_bound:
            return successes
    return None


def stable_candidate_id(scope: object, rule: object) -> str:
    payload = json.dumps(
        {
            "schema": "signal_candidate_v1",
            "scope": str(scope),
            "rule": str(rule),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def candidate_watchlist_rows(
    frontier_rows: Sequence[Mapping[str, Any]],
    *,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    shadow_min_lower_bound: float = SHADOW_MIN_WILSON_LOWER,
    product_claim_min_lower_bound: float = PRODUCT_CLAIM_MIN_WILSON_LOWER,
    target_success_rate: float = TARGET_SUCCESS_RATE,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    min_successes_for_shadow = _minimum_successes_for_wilson(accepted_min_n, shadow_min_lower_bound)
    min_successes_for_product = _minimum_successes_for_wilson(accepted_min_n, product_claim_min_lower_bound)
    for row in frontier_rows:
        selected_rows = int(row.get("selected_rows") or 0)
        sessions = int(row.get("sessions") or 0)
        successes = int(row.get("success_count") or 0)
        success_rate = float_or_none(row.get("success_rate"))
        lower = float_or_none(row.get("wilson_lower_95"))
        mean_result = float_or_none(row.get("mean_selected_result_bps"))
        if success_rate is None or lower is None or mean_result is None:
            continue
        if success_rate < target_success_rate or mean_result <= 0:
            continue
        if row.get("accepted_research"):
            continue
        missing_rows = max(0, accepted_min_n - selected_rows)
        missing_sessions = max(0, accepted_min_sessions - sessions)
        required_successes_at_min_n = math.ceil(target_success_rate * accepted_min_n)
        additional_successes_for_rate = max(0, required_successes_at_min_n - successes)
        additional_successes_for_shadow = (
            max(0, min_successes_for_shadow - successes)
            if min_successes_for_shadow is not None
            else None
        )
        additional_successes_for_product = (
            max(0, min_successes_for_product - successes)
            if min_successes_for_product is not None
            else None
        )
        missing_reasons = []
        if missing_rows:
            missing_reasons.append("sample_size")
        if missing_sessions:
            missing_reasons.append("trading_days")
        if lower < shadow_min_lower_bound:
            missing_reasons.append("shadow_reliability_bound")
        if lower < product_claim_min_lower_bound:
            missing_reasons.append("product_reliability_bound")
        if mean_result <= 0:
            missing_reasons.append("positive_result")
        result.append(
            {
                "candidate_id": stable_candidate_id(row.get("scope", ""), row.get("rule", "")),
                "source_report": "selective_frontier",
                "scope": row.get("scope", ""),
                "rule": row.get("rule", ""),
                "group_population": row.get("group_population", ""),
                "selected_rows": selected_rows,
                "sessions": sessions,
                "selected_trading_days": row.get("selected_trading_days", ""),
                "success_count": successes,
                "success_rate": success_rate,
                "wilson_lower_95": lower,
                "mean_selected_result_bps": mean_result,
                "min_confidence": row.get("min_confidence"),
                "max_confidence": row.get("max_confidence"),
                "up_decisions": row.get("up_decisions", 0),
                "down_decisions": row.get("down_decisions", 0),
                "direct_decisions": row.get("direct_decisions", 0),
                "inverse_decisions": row.get("inverse_decisions", 0),
                "neutral_decisions": row.get("neutral_decisions", 0),
                "missing_rows_to_shadow_gate": missing_rows,
                "missing_sessions_to_shadow_gate": missing_sessions,
                "additional_successes_needed_for_90pct_at_300": additional_successes_for_rate,
                "additional_successes_needed_for_shadow_lower_bound_at_300": additional_successes_for_shadow,
                "additional_successes_needed_for_product_lower_bound_at_300": additional_successes_for_product,
                "shadow_lower_bound_target": shadow_min_lower_bound,
                "product_lower_bound_target": product_claim_min_lower_bound,
                "status": "watch_only",
                "missing_reasons": ",".join(missing_reasons),
                "next_action": "collect_forward_holdout_until_minimum_rows_and_days",
                "product_claim_allowed": False,
            }
        )
    return sorted(
        result,
        key=lambda item: (
            float(item["success_rate"]),
            float(item["wilson_lower_95"]),
            float(item["mean_selected_result_bps"]),
            int(item["selected_rows"]),
        ),
        reverse=True,
    )


def confidence_threshold_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Sequence[float] = CONFIDENCE_THRESHOLDS,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    accepted_min_lower_bound: float = 0.75,
    accepted_min_success_rate: float = TARGET_SUCCESS_RATE,
) -> list[dict[str, Any]]:
    eligible = len(scored_rows)
    result: list[dict[str, Any]] = []
    for threshold in thresholds:
        decisions = [_decision_for_threshold(row, float(threshold)) for row in scored_rows]
        selected = [
            (decision, success, probability, result_bps, relation, row)
            for (decision, success, probability, result_bps, relation), row in zip(decisions, scored_rows)
            if decision != "skip"
        ]
        successes = sum(success for _, success, _, _, _, _ in selected)
        sessions = len({str(row["trading_day"]) for *_, row in selected})
        lower = wilson_lower_bound(successes, len(selected))
        success_rate = successes / len(selected) if selected else None
        mean_result = statistics.fmean(result_bps for *_, result_bps, _, _ in selected) if selected else None
        result.append(
            {
                "threshold": float(threshold),
                "eligible_rows": eligible,
                "selected_rows": len(selected),
                "skipped_rows": eligible - len(selected),
                "up_decisions": sum(1 for decision, *_ in selected if decision == "up"),
                "down_decisions": sum(1 for decision, *_ in selected if decision == "down"),
                "direct_decisions": sum(1 for *_, relation, _ in selected if relation == "direct"),
                "inverse_decisions": sum(1 for *_, relation, _ in selected if relation == "inverse"),
                "neutral_decisions": sum(1 for *_, relation, _ in selected if relation == "neutral"),
                "success_count": successes,
                "success_rate": success_rate,
                "wilson_lower_95": lower,
                "sessions": sessions,
                "coverage": len(selected) / eligible if eligible else None,
                "mean_selected_result_bps": mean_result,
                "target_success_rate": accepted_min_success_rate,
                "observed_90_success": bool(success_rate is not None and success_rate >= TARGET_SUCCESS_RATE),
                "reliable_90_success": bool(lower is not None and lower >= PRODUCT_CLAIM_MIN_WILSON_LOWER),
                "accepted_research": bool(
                    len(selected) >= accepted_min_n
                    and sessions >= accepted_min_sessions
                    and success_rate is not None
                    and success_rate >= accepted_min_success_rate
                    and lower is not None
                    and lower >= accepted_min_lower_bound
                    and mean_result is not None
                    and mean_result > 0
                ),
            }
        )
    return result


def _confidence_band(confidence: float) -> str:
    if confidence < 0.60:
        return "skip"
    if confidence < 0.75:
        return "weak_observation"
    if confidence < 0.90:
        return "working_hypothesis"
    return "strong_signal"


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


def _liquidity_bucket(row: Mapping[str, Any]) -> str:
    value = float_or_none(row.get("ticker_volume_quantile"))
    if value is None:
        value = float_or_none(row.get("day_volume_quantile"))
    if value is None:
        return "unknown"
    if value < 0.33:
        return "noisy"
    if value < 0.66:
        return "medium"
    return "liquid"


def _first_float(row: Mapping[str, Any], columns: Sequence[str]) -> float | None:
    for column in columns:
        value = float_or_none(row.get(column))
        if value is not None:
            return value
    return None


def _signed_bucket(value: float | None, *, flat_bps: float = 10.0) -> str:
    if value is None:
        return "unknown"
    if abs(value) < flat_bps:
        return "flat"
    return "up" if value > 0 else "down"


def _pre_trend_bucket(row: Mapping[str, Any]) -> str:
    return _signed_bucket(
        _first_float(
            row,
            (
                "pre_return_bps_60m",
                "pre_return_bps_30m",
                "pre_return_bps_15m",
                "pre_return_bps_5m",
            ),
        )
    )


def _pre_trend_strength_bucket(row: Mapping[str, Any]) -> str:
    trend = _pre_trend_bucket(row)
    if trend in {"unknown", "flat"}:
        return trend
    value = _first_float(
        row,
        (
            "pre_return_to_volatility_60m",
            "pre_return_to_volatility_30m",
            "pre_return_to_volatility_15m",
            "pre_return_to_volatility_5m",
        ),
    )
    if value is None:
        return "unknown"
    if value < 0.75:
        return "weak"
    if value < 1.50:
        return "medium"
    return "strong"


def _relation_to_pre_trend(row: Mapping[str, Any], direction: str) -> str:
    trend = _pre_trend_bucket(row)
    if trend == "unknown":
        return "unknown"
    if trend == "flat":
        return "flat_pretrend"
    if direction not in {"up", "down"}:
        return "non_directional"
    return "with_pretrend" if direction == trend else "against_pretrend"


def _event_direction(row: Mapping[str, Any]) -> str:
    original = str(row.get("direction", ""))
    if original in {"1", "1.0"}:
        return "up"
    if original in {"-1", "-1.0"}:
        return "down"
    event_move = float_or_none(row.get("event_move_bps"))
    return _signed_bucket(event_move)


def _event_trend_relation(row: Mapping[str, Any]) -> str:
    return _relation_to_pre_trend(row, _event_direction(row))


def _decision_trend_relation(row: Mapping[str, Any], decision: str) -> str:
    return _relation_to_pre_trend(row, decision)


def _market_alignment_bucket(row: Mapping[str, Any], decision: str) -> str:
    market_return = _first_float(
        row,
        (
            "market_return_bps_60m",
            "market_return_bps_30m",
            "market_return_bps_15m",
            "market_return_bps_5m",
        ),
    )
    market_direction = _signed_bucket(market_return, flat_bps=5.0)
    if market_direction == "unknown":
        return "unknown"
    if market_direction == "flat":
        return "flat_market"
    if decision not in {"up", "down"}:
        event_direction = _event_direction(row)
        if event_direction not in {"up", "down"}:
            return "non_directional"
        decision = event_direction
    return "with_market" if decision == market_direction else "against_market"


def _relative_market_bucket(row: Mapping[str, Any]) -> str:
    return _signed_bucket(
        _first_float(
            row,
            (
                "signal_vs_market_bps_60m",
                "signal_vs_market_bps_30m",
                "signal_vs_market_bps_15m",
                "signal_vs_market_bps_5m",
            ),
        ),
        flat_bps=5.0,
    )


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


def _has_orderbook(row: Mapping[str, Any]) -> bool:
    return str(row.get("orderbook_available", "")).lower() in {"1", "true", "yes"}


def _spread_bucket(row: Mapping[str, Any]) -> str:
    if not _has_orderbook(row):
        return "missing"
    value = float_or_none(row.get("orderbook_spread_bps"))
    if value is None:
        return "missing"
    if value <= 10.0:
        return "tight"
    if value <= 30.0:
        return "normal"
    return "wide"


def _depth_bucket(row: Mapping[str, Any]) -> str:
    if not _has_orderbook(row):
        return "missing"
    value = float_or_none(row.get("orderbook_total_qty"))
    if value is None:
        return "missing"
    if value < 1_000.0:
        return "shallow"
    if value < 10_000.0:
        return "medium"
    return "deep"


def _imbalance_bucket(row: Mapping[str, Any]) -> str:
    if not _has_orderbook(row):
        return "missing"
    value = float_or_none(row.get("orderbook_imbalance_ratio"))
    if value is None:
        return "missing"
    if value < 0.40:
        return "ask_heavy"
    if value > 0.60:
        return "bid_heavy"
    return "balanced"


def decision_audit_rows(scored_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in scored_rows:
        decision, success, probability, result_bps, relation = _decision_for_threshold(
            row,
            POLICY_DECISION_THRESHOLD,
        )
        frontier_decision, frontier_success, frontier_probability, frontier_result_bps, frontier_relation = (
            _forced_directional_decision(row)
        )
        confidence = max(float(row.get("_up_probability", 0.0)), float(row.get("_down_probability", 0.0)))
        result.append(
            {
                "row_id": row.get("row_id"),
                "ticker": row.get("ticker"),
                "signal_type": row.get("signal_type"),
                "source_event_at": row.get("source_event_at"),
                "trading_day": row.get("trading_day"),
                "horizon_seconds": row.get("horizon_seconds"),
                "original_direction": row.get("direction"),
                "session_bucket": row.get("session_bucket"),
                "volatility_bucket": _volatility_bucket(row),
                "consolidation_bucket": _consolidation_bucket(row),
                "liquidity_bucket": _liquidity_bucket(row),
                "pre_trend_bucket": _pre_trend_bucket(row),
                "pre_trend_strength_bucket": _pre_trend_strength_bucket(row),
                "event_trend_relation": _event_trend_relation(row),
                "decision_trend_relation": _decision_trend_relation(row, frontier_decision),
                "event_close_quality_bucket": _event_close_quality_bucket(row),
                "event_reversal_pressure_bucket": _event_reversal_pressure_bucket(row),
                "market_alignment_bucket": _market_alignment_bucket(row, frontier_decision),
                "relative_market_bucket": _relative_market_bucket(row),
                "spread_bucket": _spread_bucket(row),
                "depth_bucket": _depth_bucket(row),
                "imbalance_bucket": _imbalance_bucket(row),
                "signal_count_bucket": _signal_count_bucket(row),
                "combo_key_300s": row.get("combo_key_300s"),
                "orderbook_available": row.get("orderbook_available"),
                "orderbook_age_seconds": row.get("orderbook_age_seconds"),
                "orderbook_spread_bps": row.get("orderbook_spread_bps"),
                "orderbook_total_qty": row.get("orderbook_total_qty"),
                "orderbook_imbalance_ratio": row.get("orderbook_imbalance_ratio"),
                "recent_signal_count_60s": row.get("recent_signal_count_60s"),
                "recent_signal_count_300s": row.get("recent_signal_count_300s"),
                "recent_signal_count_900s": row.get("recent_signal_count_900s"),
                "day_volatility_quantile": row.get("day_volatility_quantile"),
                "ticker_volatility_quantile": row.get("ticker_volatility_quantile"),
                "event_to_pre_volatility_60m": row.get("event_to_pre_volatility_60m"),
                "event_to_pre_range_60m": row.get("event_to_pre_range_60m"),
                "event_body_bps": row.get("event_body_bps"),
                "event_upper_wick_bps": row.get("event_upper_wick_bps"),
                "event_lower_wick_bps": row.get("event_lower_wick_bps"),
                "event_body_to_range": row.get("event_body_to_range"),
                "event_upper_wick_to_range": row.get("event_upper_wick_to_range"),
                "event_lower_wick_to_range": row.get("event_lower_wick_to_range"),
                "event_close_to_direction": row.get("event_close_to_direction"),
                "event_reversal_pressure": row.get("event_reversal_pressure"),
                "up_confidence": float(row.get("_up_probability", 0.0)),
                "down_confidence": float(row.get("_down_probability", 0.0)),
                "max_confidence": confidence,
                "confidence_band": _confidence_band(confidence),
                "decision": decision,
                "decision_relation": relation,
                "success": success,
                "decision_result_bps": result_bps,
                "frontier_decision": frontier_decision,
                "frontier_decision_relation": frontier_relation,
                "frontier_success": frontier_success,
                "frontier_confidence": frontier_probability,
                "frontier_result_bps": frontier_result_bps,
                "forward_return_bps": row.get("_forward_return_bps"),
            }
        )
    return result


def confidence_reliability_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    shadow_min_lower_bound: float = SHADOW_MIN_WILSON_LOWER,
    product_claim_min_lower_bound: float = PRODUCT_CLAIM_MIN_WILSON_LOWER,
    target_success_rate: float = TARGET_SUCCESS_RATE,
) -> list[dict[str, Any]]:
    """Compare model confidence bands with realized directional success.

    This is a calibration guard for the product wording. A row in the
    90%+ confidence band is not allowed to become a product claim unless the
    realized validation success and lower reliability bound also support it.
    """

    bands = [
        ("skip", 0.0, 0.60, "skip"),
        ("weak_observation", 0.60, 0.75, "observe"),
        ("working_hypothesis", 0.75, 0.90, "shadow"),
        ("strong_signal", 0.90, 1.01, "candidate"),
    ]
    forced: list[tuple[Mapping[str, Any], str, int, float, float, str]] = [
        (row, *_forced_directional_decision(row))
        for row in scored_rows
    ]

    result: list[dict[str, Any]] = []
    for scope, groups in (
        (
            "confidence_band",
            [
                (name, lo, hi, action, [item for item in forced if lo <= item[3] < hi])
                for name, lo, hi, action in bands
            ],
        ),
        (
            "decision_confidence_band",
            [
                (
                    f"decision={decision} | confidence_band={name}",
                    lo,
                    hi,
                    action,
                    [item for item in forced if item[1] == decision and lo <= item[3] < hi],
                )
                for name, lo, hi, action in bands
                for decision in ("up", "down")
            ],
        ),
    ):
        for rule, lo, hi, action, items in groups:
            successes = sum(item[2] for item in items)
            sessions = len({str(item[0].get("trading_day")) for item in items if item[0].get("trading_day")})
            observed = successes / len(items) if items else None
            lower = wilson_lower_bound(successes, len(items))
            mean_confidence = statistics.fmean(item[3] for item in items) if items else None
            mean_result = statistics.fmean(item[4] for item in items) if items else None
            shadow_allowed = bool(
                len(items) >= accepted_min_n
                and sessions >= accepted_min_sessions
                and observed is not None
                and observed >= target_success_rate
                and lower is not None
                and lower >= shadow_min_lower_bound
                and mean_result is not None
                and mean_result > 0
            )
            product_allowed = bool(
                shadow_allowed
                and lower is not None
                and lower >= product_claim_min_lower_bound
            )
            result.append(
                {
                    "scope": scope,
                    "rule": rule,
                    "nominal_action": action,
                    "min_confidence": lo,
                    "max_confidence": hi,
                    "selected_rows": len(items),
                    "sessions": sessions,
                    "success_count": successes,
                    "observed_success_rate": observed,
                    "wilson_lower_95": lower,
                    "mean_model_confidence": mean_confidence,
                    "confidence_minus_observed": (
                        mean_confidence - observed
                        if mean_confidence is not None and observed is not None
                        else None
                    ),
                    "mean_result_bps": mean_result,
                    "target_success_rate": target_success_rate,
                    "shadow_allowed": shadow_allowed,
                    "product_90_allowed": product_allowed,
                    "safe_runtime_action": action if shadow_allowed else "skip",
                }
            )
    return result


def temporal_stability_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Sequence[float] = CONFIDENCE_THRESHOLDS,
    blocks: int = 5,
) -> list[dict[str, Any]]:
    days = sorted({str(row.get("trading_day")) for row in scored_rows if row.get("trading_day")})
    if not days:
        return []
    block_count = min(max(1, blocks), len(days))
    result: list[dict[str, Any]] = []
    for block_index in range(block_count):
        start = block_index * len(days) // block_count
        end = (block_index + 1) * len(days) // block_count
        block_days = set(days[start:end])
        block_rows = [row for row in scored_rows if str(row.get("trading_day")) in block_days]
        if not block_rows:
            continue
        for threshold_row in confidence_threshold_rows(block_rows, thresholds=thresholds):
            selected_rows = int(threshold_row.get("selected_rows") or 0)
            success_rate = threshold_row.get("success_rate")
            lower = threshold_row.get("wilson_lower_95")
            result.append(
                {
                    "threshold": threshold_row["threshold"],
                    "block_index": block_index + 1,
                    "block_count": block_count,
                    "first_day": days[start],
                    "last_day": days[end - 1],
                    "eligible_rows": threshold_row["eligible_rows"],
                    "selected_rows": selected_rows,
                    "skipped_rows": threshold_row["skipped_rows"],
                    "success_count": threshold_row["success_count"],
                    "success_rate": success_rate,
                    "wilson_lower_95": lower,
                    "mean_selected_result_bps": threshold_row["mean_selected_result_bps"],
                    "observed_90_success": bool(success_rate is not None and success_rate >= TARGET_SUCCESS_RATE),
                    "reliable_90_success": bool(lower is not None and lower >= PRODUCT_CLAIM_MIN_WILSON_LOWER),
                    "has_selected_signals": selected_rows > 0,
                }
            )
    return result


def temporal_stability_summary_rows(
    temporal_rows: Sequence[Mapping[str, Any]],
    *,
    min_blocks_with_selected: int = 3,
    min_block_success_rate: float = TARGET_SUCCESS_RATE,
    min_block_lower_bound: float = SHADOW_MIN_WILSON_LOWER,
) -> list[dict[str, Any]]:
    grouped: dict[float, list[Mapping[str, Any]]] = {}
    for row in temporal_rows:
        grouped.setdefault(float(row.get("threshold") or 0.0), []).append(row)
    result: list[dict[str, Any]] = []
    for threshold, rows in sorted(grouped.items()):
        selected_blocks = [row for row in rows if int(row.get("selected_rows") or 0) > 0]
        success_rates = [
            float(row["success_rate"])
            for row in selected_blocks
            if row.get("success_rate") not in {None, ""}
        ]
        lower_bounds = [
            float(row["wilson_lower_95"])
            for row in selected_blocks
            if row.get("wilson_lower_95") not in {None, ""}
        ]
        min_success_rate = min(success_rates) if success_rates else None
        min_lower_bound = min(lower_bounds) if lower_bounds else None
        weak_blocks = sum(
            1
            for row in selected_blocks
            if row.get("success_rate") in {None, ""}
            or float(row.get("success_rate") or 0.0) < min_block_success_rate
            or row.get("wilson_lower_95") in {None, ""}
            or float(row.get("wilson_lower_95") or 0.0) < min_block_lower_bound
        )
        result.append(
            {
                "threshold": threshold,
                "blocks": len(rows),
                "blocks_with_selected": len(selected_blocks),
                "min_success_rate": min_success_rate,
                "min_wilson_lower_95": min_lower_bound,
                "min_selected_rows": min((int(row.get("selected_rows") or 0) for row in selected_blocks), default=0),
                "weak_blocks": weak_blocks,
                "min_blocks_with_selected": min_blocks_with_selected,
                "min_block_success_rate": min_block_success_rate,
                "min_block_lower_bound": min_block_lower_bound,
                "temporal_supported": bool(
                    len(selected_blocks) >= min_blocks_with_selected
                    and weak_blocks == 0
                    and min_success_rate is not None
                    and min_success_rate >= min_block_success_rate
                    and min_lower_bound is not None
                    and min_lower_bound >= min_block_lower_bound
                ),
            }
        )
    return result


def _bayesian_state_key(row: Mapping[str, Any], keys: Sequence[str]) -> tuple[str, ...] | None:
    values = tuple(_slice_value(row, "skip", "neutral", key) for key in keys)
    if any(value == "unknown" for value in values):
        return None
    return values


def train_bayesian_state_candidates(
    train_rows: Sequence[Mapping[str, Any]],
    *,
    min_train_rows: int = 100,
    alpha: float = 1.0,
    beta: float = 1.0,
) -> list[dict[str, Any]]:
    stats: dict[tuple[str, tuple[str, ...], str], dict[str, Any]] = {}
    for row in _directional_target_rows(train_rows):
        for group_name, keys in BAYESIAN_STATE_GROUPS:
            key = _bayesian_state_key(row, keys)
            if key is None:
                continue
            for decision, target_field in (("up", "_up_target"), ("down", "_down_target")):
                stat_key = (group_name, key, decision)
                item = stats.setdefault(
                    stat_key,
                    {
                        "group_set": group_name,
                        "state_key": key,
                        "decision": decision,
                        "train_rows": 0,
                        "success_count": 0,
                    },
                )
                item["train_rows"] += 1
                item["success_count"] += int(row[target_field])
    result: list[dict[str, Any]] = []
    for item in stats.values():
        train_n = int(item["train_rows"])
        if train_n < min_train_rows:
            continue
        successes = int(item["success_count"])
        posterior_mean = (successes + alpha) / (train_n + alpha + beta)
        result.append(
            {
                "group_set": item["group_set"],
                "state_key": "|".join(item["state_key"]),
                "decision": item["decision"],
                "train_rows": train_n,
                "success_count": successes,
                "train_success_rate": successes / train_n,
                "posterior_mean": posterior_mean,
                "wilson_lower_95": wilson_lower_bound(successes, train_n),
            }
        )
    return sorted(
        result,
        key=lambda row: (
            float(row["posterior_mean"]),
            float(row["wilson_lower_95"] or 0.0),
            int(row["train_rows"]),
        ),
        reverse=True,
    )


def score_bayesian_state_rows(
    validation_rows: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {
        (str(row["group_set"]), tuple(str(row["state_key"]).split("|")), str(row["decision"])): row
        for row in candidates
    }
    scored: list[dict[str, Any]] = []
    for index, row in enumerate(_directional_target_rows(validation_rows)):
        best: Mapping[str, Any] | None = None
        best_decision = "skip"
        for group_name, keys in BAYESIAN_STATE_GROUPS:
            state_key = _bayesian_state_key(row, keys)
            if state_key is None:
                continue
            for decision in ("up", "down"):
                candidate = by_key.get((group_name, state_key, decision))
                if candidate is None:
                    continue
                if best is None or float(candidate["posterior_mean"]) > float(best["posterior_mean"]):
                    best = candidate
                    best_decision = decision
        scored_row = dict(row)
        scored_row["row_id"] = row.get("row_id") or f"bayesian-{index}"
        scored_row["_up_probability"] = float(best["posterior_mean"]) if best_decision == "up" and best else 0.0
        scored_row["_down_probability"] = float(best["posterior_mean"]) if best_decision == "down" and best else 0.0
        scored_row["_bayesian_group_set"] = best.get("group_set") if best else ""
        scored_row["_bayesian_state_key"] = best.get("state_key") if best else ""
        scored_row["_bayesian_train_rows"] = best.get("train_rows") if best else 0
        scored.append(scored_row)
    return scored


def run_bayesian_state_triage(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    candidates = train_bayesian_state_candidates(train_rows)
    if not candidates:
        return {"model": "bayesian_state_triage", "status": "no_state_candidates"}, [], [], []
    scored_rows = score_bayesian_state_rows(validation_rows, candidates)
    threshold_rows = confidence_threshold_rows(scored_rows)
    stability_rows = temporal_stability_summary_rows(temporal_stability_rows(scored_rows))
    best = max(
        threshold_rows,
        key=lambda item: (
            bool(item["accepted_research"]),
            float(item["wilson_lower_95"] or -1.0),
            float(item["success_rate"] or -1.0),
            int(item["selected_rows"]),
        ),
    )
    return {
        "model": "bayesian_state_triage",
        "status": "ok",
        "n": len(scored_rows),
        "state_candidates": len(candidates),
        "best_threshold": best["threshold"],
        "best_selected_rows": best["selected_rows"],
        "best_success_rate": best["success_rate"],
        "best_wilson_lower_95": best["wilson_lower_95"],
        "accepted_thresholds": sum(1 for item in threshold_rows if item["accepted_research"]),
        "temporal_supported_thresholds": sum(1 for item in stability_rows if item["temporal_supported"]),
    }, threshold_rows, stability_rows, candidates[:200]


def _volatility_bucket(row: Mapping[str, Any]) -> str:
    explicit = row.get("_volatility_bucket")
    if explicit not in {None, ""}:
        return str(explicit)
    value = float_or_none(row.get("day_volatility_quantile"))
    if value is None:
        value = float_or_none(row.get("ticker_volatility_quantile"))
    if value is None:
        return "unknown"
    if value < 0.33:
        return "low"
    if value < 0.66:
        return "medium"
    return "high"


def _signal_count_bucket(row: Mapping[str, Any]) -> str:
    count = int(float(row.get("recent_signal_count_300s") or row.get("recent_signal_count_900s") or 0))
    if count <= 1:
        return "single"
    if count <= 3:
        return "cluster_2_3"
    return "cluster_4_plus"


def _slice_value(row: Mapping[str, Any], decision: str, relation: str, key: str) -> str:
    if key == "decision":
        return decision
    if key == "decision_relation":
        return relation
    if key == "volatility_bucket":
        return _volatility_bucket(row)
    if key == "consolidation_bucket":
        return _consolidation_bucket(row)
    if key == "liquidity_bucket":
        return _liquidity_bucket(row)
    if key == "pre_trend_bucket":
        return _pre_trend_bucket(row)
    if key == "pre_trend_strength_bucket":
        return _pre_trend_strength_bucket(row)
    if key == "event_trend_relation":
        return _event_trend_relation(row)
    if key == "decision_trend_relation":
        return _decision_trend_relation(row, decision)
    if key == "market_alignment_bucket":
        return _market_alignment_bucket(row, decision)
    if key == "relative_market_bucket":
        return _relative_market_bucket(row)
    if key == "spread_bucket":
        value = _spread_bucket(row)
        return "unknown" if value == "missing" else value
    if key == "depth_bucket":
        value = _depth_bucket(row)
        return "unknown" if value == "missing" else value
    if key == "imbalance_bucket":
        value = _imbalance_bucket(row)
        return "unknown" if value == "missing" else value
    if key == "signal_count_bucket":
        return _signal_count_bucket(row)
    value = row.get(key)
    return "unknown" if value in {None, ""} else str(value)


def high_confidence_slice_rows(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Sequence[float] = HIGH_CONFIDENCE_SLICE_THRESHOLDS,
    min_n: int = 100,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    target_success_rate: float = TARGET_SUCCESS_RATE,
    shadow_min_lower_bound: float = SHADOW_MIN_WILSON_LOWER,
) -> list[dict[str, Any]]:
    group_sets = [
        ("decision_horizon", ("decision", "horizon_seconds")),
        ("decision_signal_horizon", ("decision", "signal_type", "horizon_seconds")),
        ("decision_signal_session_horizon", ("decision", "signal_type", "session_bucket", "horizon_seconds")),
        (
            "decision_signal_session_volatility_horizon",
            ("decision", "signal_type", "session_bucket", "volatility_bucket", "horizon_seconds"),
        ),
        ("decision_combo_horizon", ("decision", "combo_key_300s", "horizon_seconds")),
        ("decision_relation_signal_horizon", ("decision_relation", "decision", "signal_type", "horizon_seconds")),
        (
            "decision_signal_cluster_volatility_horizon",
            ("decision", "signal_type", "signal_count_bucket", "volatility_bucket", "horizon_seconds"),
        ),
        (
            "decision_signal_consolidation_liquidity_horizon",
            ("decision", "signal_type", "consolidation_bucket", "liquidity_bucket", "horizon_seconds"),
        ),
        (
            "decision_signal_trend_horizon",
            (
                "decision",
                "signal_type",
                "pre_trend_bucket",
                "pre_trend_strength_bucket",
                "decision_trend_relation",
                "horizon_seconds",
            ),
        ),
        (
            "decision_signal_trend_market_horizon",
            (
                "decision",
                "signal_type",
                "pre_trend_bucket",
                "market_alignment_bucket",
                "relative_market_bucket",
                "horizon_seconds",
            ),
        ),
        (
            "decision_signal_microstructure_horizon",
            ("decision", "signal_type", "spread_bucket", "depth_bucket", "imbalance_bucket", "horizon_seconds"),
        ),
    ]
    candidates: list[dict[str, Any]] = []
    for threshold in thresholds:
        selected: list[tuple[Mapping[str, Any], str, int, float, float, str]] = []
        for row in scored_rows:
            decision, success, probability, result_bps, relation = _decision_for_threshold(row, float(threshold))
            if decision == "skip":
                continue
            selected.append((row, decision, success, probability, result_bps, relation))
        for group_name, keys in group_sets:
            grouped: dict[tuple[str, ...], list[tuple[Mapping[str, Any], str, int, float, float, str]]] = {}
            for item in selected:
                row, decision, _, _, _, relation = item
                group_key = tuple(_slice_value(row, decision, relation, key) for key in keys)
                if any(value == "unknown" for value in group_key):
                    continue
                grouped.setdefault(group_key, []).append(item)
            for group_key, items in grouped.items():
                if len(items) < min_n:
                    continue
                successes = sum(item[2] for item in items)
                sessions = len({str(item[0].get("trading_day")) for item in items})
                success_rate = successes / len(items)
                lower = wilson_lower_bound(successes, len(items)) or 0.0
                mean_result = statistics.fmean(item[4] for item in items)
                observed_90 = success_rate >= target_success_rate
                reliable_90 = lower >= PRODUCT_CLAIM_MIN_WILSON_LOWER
                accepted_shadow = bool(
                    len(items) >= accepted_min_n
                    and sessions >= accepted_min_sessions
                    and observed_90
                    and lower >= shadow_min_lower_bound
                    and mean_result > 0
                )
                rule = " | ".join(f"{key}={value}" for key, value in zip(keys, group_key))
                candidates.append(
                    {
                        "group_set": group_name,
                        "rule": rule,
                        "threshold": float(threshold),
                        "selected_rows": len(items),
                        "sessions": sessions,
                        "success_count": successes,
                        "success_rate": success_rate,
                        "wilson_lower_95": lower,
                        "mean_result_bps": mean_result,
                        "mean_confidence": statistics.fmean(item[3] for item in items),
                        "up_decisions": sum(1 for _, decision, *_ in items if decision == "up"),
                        "down_decisions": sum(1 for _, decision, *_ in items if decision == "down"),
                        "direct_decisions": sum(1 for *_, relation in items if relation == "direct"),
                        "inverse_decisions": sum(1 for *_, relation in items if relation == "inverse"),
                        "neutral_decisions": sum(1 for *_, relation in items if relation == "neutral"),
                        "observed_90_success": observed_90,
                        "reliable_90_success": reliable_90,
                        "accepted_shadow": accepted_shadow,
                        "product_claim_allowed": reliable_90 and accepted_shadow,
                    }
                )
    return sorted(
        candidates,
        key=lambda item: (
            bool(item["product_claim_allowed"]),
            bool(item["accepted_shadow"]),
            float(item["wilson_lower_95"]),
            float(item["success_rate"]),
            int(item["selected_rows"]),
        ),
        reverse=True,
    )


def run_directional_triage(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    fit_rows, calibration_rows = chronological_split(train_rows, train_fraction=0.80)
    if not calibration_rows:
        fit_rows, calibration_rows = list(train_rows), list(train_rows)
    fit_x, fit_up, fit_down, _, _ = _directional_feature_dicts(fit_rows)
    calibration_x, calibration_up, calibration_down, _, _ = _directional_feature_dicts(calibration_rows)
    valid_x, valid_up, valid_down, valid_returns, valid_ids = _directional_feature_dicts(validation_rows)
    if not valid_x:
        return {"model": "lightgbm_directional_triage", "status": "no_validation_rows"}, [], [], [], [], [], [], []
    up_status, calibration_up_raw = _fit_lightgbm_probabilities(
        fit_x,
        fit_up,
        calibration_x,
        model_name="up_calibration",
    )
    _, up_raw_probabilities = _fit_lightgbm_probabilities(
        fit_x,
        fit_up,
        valid_x,
        model_name="up",
    )
    down_status, calibration_down_raw = _fit_lightgbm_probabilities(
        fit_x,
        fit_down,
        calibration_x,
        model_name="down_calibration",
    )
    _, down_raw_probabilities = _fit_lightgbm_probabilities(
        fit_x,
        fit_down,
        valid_x,
        model_name="down",
    )
    if not up_raw_probabilities or not down_raw_probabilities:
        return {
            "model": "lightgbm_directional_triage",
            "status": f"{up_status};{down_status}",
            "n": len(valid_x),
        }, [], [], [], [], [], [], []
    up_bins = _calibration_bins(calibration_up_raw, calibration_up)
    down_bins = _calibration_bins(calibration_down_raw, calibration_down)
    up_probabilities = [_apply_calibration(probability, up_bins) for probability in up_raw_probabilities]
    down_probabilities = [_apply_calibration(probability, down_bins) for probability in down_raw_probabilities]
    target_rows = _directional_target_rows(validation_rows)
    scored_rows: list[dict[str, Any]] = []
    for row, row_id, forward_return, up_target, down_target, up_probability, down_probability in zip(
        target_rows,
        valid_ids,
        valid_returns,
        valid_up,
        valid_down,
        up_probabilities,
        down_probabilities,
    ):
        scored = dict(row)
        scored["row_id"] = row_id
        scored["_forward_return_bps"] = forward_return
        scored["_up_target"] = up_target
        scored["_down_target"] = down_target
        scored["_up_probability"] = up_probability
        scored["_down_probability"] = down_probability
        scored_rows.append(scored)
    threshold_rows = confidence_threshold_rows(scored_rows)
    audit_rows = decision_audit_rows(scored_rows)
    reliability_rows = confidence_reliability_rows(scored_rows)
    frontier_rows = selective_frontier_rows(scored_rows)
    slice_rows = high_confidence_slice_rows(scored_rows)
    stability_rows = temporal_stability_rows(scored_rows)
    stability_summary = temporal_stability_summary_rows(stability_rows)
    best = max(
        threshold_rows,
        key=lambda item: (
            bool(item["accepted_research"]),
            float(item["wilson_lower_95"] or -1.0),
            float(item["success_rate"] or -1.0),
            int(item["selected_rows"]),
        ),
    )
    return {
        "model": "lightgbm_directional_triage",
        "status": "ok",
        "n": len(scored_rows),
        "best_threshold": best["threshold"],
        "best_selected_rows": best["selected_rows"],
        "best_success_rate": best["success_rate"],
        "best_wilson_lower_95": best["wilson_lower_95"],
        "accepted_thresholds": sum(1 for item in threshold_rows if item["accepted_research"]),
        "accepted_selective_frontier_rows": sum(1 for item in frontier_rows if item["accepted_research"]),
        "accepted_high_confidence_slices": sum(1 for item in slice_rows if item["accepted_shadow"]),
        "temporal_stability_rows": len(stability_rows),
        "temporal_supported_thresholds": sum(1 for item in stability_summary if item["temporal_supported"]),
        "calibration_rows": len(calibration_x),
        "calibration_bins": len(up_bins),
        "confidence_reliability_rows": len(reliability_rows),
        "product_90_calibrated_bands": sum(1 for item in reliability_rows if item["product_90_allowed"]),
    }, threshold_rows, audit_rows, reliability_rows, frontier_rows, slice_rows, stability_rows, stability_summary


def run_logistic_regression(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    valid_x, valid_y, _, _ = _feature_dicts(validation_rows)
    if len(set(train_y)) < 2 or not valid_x:
        return {"model": "logistic_regression", "status": "insufficient_classes_or_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from sklearn.linear_model import LogisticRegression  # type: ignore
    except ImportError:
        return {"model": "logistic_regression", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=20260716)
    model.fit(x_train, train_y)
    probabilities = [float(item) for item in model.predict_proba(x_valid)[:, 1]]
    metrics = _classification_metrics(valid_y, probabilities)
    coefficients = model.coef_[0]
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "logistic_regression",
                "feature": str(name),
                "importance": float(abs(value)),
                "signed_value": float(value),
            }
            for name, value in zip(names, coefficients)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "logistic_regression", "status": "ok", "n": len(valid_y), **metrics}, importance


def run_lightgbm_classifier(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    valid_x, valid_y, _, _ = _feature_dicts(validation_rows)
    if len(set(train_y)) < 2 or not valid_x:
        return {"model": "lightgbm_classifier", "status": "insufficient_classes_or_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError:
        return {"model": "lightgbm_classifier", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LGBMClassifier(
        n_estimators=250,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260716,
        class_weight="balanced",
        verbose=-1,
    )
    model.fit(x_train, train_y)
    probabilities = [float(item) for item in model.predict_proba(x_valid)[:, 1]]
    metrics = _classification_metrics(valid_y, probabilities)
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "lightgbm_classifier",
                "feature": str(name),
                "importance": float(value),
                "signed_value": float(value),
            }
            for name, value in zip(names, model.feature_importances_)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "lightgbm_classifier", "status": "ok", "n": len(valid_y), **metrics}, importance


def run_lightgbm_regressor(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, _, train_y, _ = _feature_dicts(train_rows)
    valid_x, _, valid_y, _ = _feature_dicts(validation_rows)
    if len(train_y) < 50 or not valid_x:
        return {"model": "lightgbm_regressor", "status": "insufficient_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMRegressor  # type: ignore
    except ImportError:
        return {"model": "lightgbm_regressor", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LGBMRegressor(
        n_estimators=250,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260716,
        verbose=-1,
    )
    model.fit(x_train, train_y)
    predictions = [float(item) for item in model.predict(x_valid)]
    mae = statistics.fmean(abs(y - p) for y, p in zip(valid_y, predictions))
    rmse = math.sqrt(statistics.fmean((y - p) ** 2 for y, p in zip(valid_y, predictions)))
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "lightgbm_regressor",
                "feature": str(name),
                "importance": float(value),
                "signed_value": float(value),
            }
            for name, value in zip(names, model.feature_importances_)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "lightgbm_regressor", "status": "ok", "n": len(valid_y), "mae_bps": mae, "rmse_bps": rmse}, importance


def univariate_feature_importance(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    numeric, _ = dataset_feature_columns(rows)
    selected = _target_rows(rows)
    result: list[dict[str, Any]] = []
    for column in numeric:
        positive = [float_or_none(row.get(column)) for row in selected if str(row["meta_label"]) == "1"]
        negative = [float_or_none(row.get(column)) for row in selected if str(row["meta_label"]) == "0"]
        pos_values = [item for item in positive if item is not None]
        neg_values = [item for item in negative if item is not None]
        if not pos_values or not neg_values:
            continue
        diff = statistics.fmean(pos_values) - statistics.fmean(neg_values)
        result.append(
            {
                "model": "univariate_screen",
                "feature": column,
                "importance": abs(diff),
                "signed_value": diff,
            }
        )
    return sorted(result, key=lambda item: item["importance"], reverse=True)


def build_leaderboard(
    model_results: Sequence[Mapping[str, Any]],
    event_study: Sequence[Mapping[str, Any]],
    *,
    validation_sessions: int,
    naive_positive_rate: float | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in model_results:
        if item.get("status") != "ok":
            rows.append(
                {
                    "model": item["model"],
                    "status": item.get("status"),
                    "n": item.get("n", 0),
                    "score": "",
                    "accepted": False,
                }
            )
            continue
        if item["model"].endswith("_regressor"):
            score = -item.get("mae_bps", 0)
            accepted = False
        elif item["model"].endswith("_triage"):
            score = item.get("best_success_rate")
            accepted = bool(
                validation_sessions >= 30
                and int(item.get("n", 0)) >= 300
                and score is not None
                and float(score) >= TARGET_SUCCESS_RATE
                and float(item.get("best_wilson_lower_95") or 0.0) >= SHADOW_MIN_WILSON_LOWER
                and int(item.get("accepted_thresholds", 0) or 0) > 0
            )
        else:
            score = item.get("precision") or item.get("accuracy")
            accepted = bool(
                validation_sessions >= 30
                and int(item.get("n", 0)) >= 300
                and score is not None
                and naive_positive_rate is not None
                and float(score) > naive_positive_rate
                and float(score) >= 0.75
            )
        rows.append(
            {
                "model": item["model"],
                "status": "ok",
                "n": item.get("n", 0),
                "score": score,
                "accepted": accepted,
            }
        )
    for item in event_study:
        score = item.get("mean_cost_adjusted_directional_bps")
        rows.append(
            {
                "model": "event_study_baseline",
                "status": "ok",
                "signal_type": item.get("signal_type"),
                "horizon_seconds": item.get("horizon_seconds"),
                "n": item.get("n", 0),
                "score": score,
                "accepted": bool(
                    validation_sessions >= 30
                    and int(item.get("n", 0)) >= 300
                    and score is not None
                    and float(score) > 0
                ),
            }
        )
    return sorted(rows, key=lambda item: (bool(item["accepted"]), float(item["score"] or -999999)), reverse=True)


def build_decision_policy(
    confidence_thresholds: Sequence[Mapping[str, Any]],
    temporal_summary: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    temporal_by_threshold = {
        float(row.get("threshold") or 0.0): row
        for row in temporal_summary
    }
    aggregate_accepted = [row for row in confidence_thresholds if row.get("accepted_research")]
    accepted = [
        row
        for row in aggregate_accepted
        if not temporal_summary
        or bool(temporal_by_threshold.get(float(row.get("threshold") or 0.0), {}).get("temporal_supported"))
    ]
    tiers = [
        {"name": "skip", "min_confidence": 0.0, "max_confidence": 0.60, "action": "skip"},
        {"name": "weak_observation", "min_confidence": 0.60, "max_confidence": 0.75, "action": "observe"},
        {"name": "working_hypothesis", "min_confidence": 0.75, "max_confidence": 0.90, "action": "shadow"},
        {"name": "strong_signal", "min_confidence": 0.90, "max_confidence": 1.01, "action": "candidate"},
    ]
    if not accepted:
        reason = (
            "no_temporally_stable_confidence_threshold_passed_research_gate"
            if aggregate_accepted and temporal_summary
            else "no_confidence_threshold_passed_research_gate"
        )
        return {
            "schema_version": 1,
            "kind": "signal_decision_policy",
            "status": "disabled",
            "default_action": "skip",
            "reason_code": reason,
            "selected_threshold": None,
            "selected_threshold_evidence": None,
            "selected_threshold_temporal_evidence": None,
            "tiers": tiers,
            "product_claim_allowed": False,
        }
    selected = max(
        accepted,
        key=lambda row: (
            float(row.get("wilson_lower_95") or 0.0),
            float(row.get("success_rate") or 0.0),
            int(row.get("selected_rows") or 0),
        ),
    )
    return {
        "schema_version": 1,
        "kind": "signal_decision_policy",
        "status": "shadow",
        "default_action": "skip",
        "reason_code": "confidence_threshold_passed_research_gate",
        "selected_threshold": float(selected["threshold"]),
        "selected_threshold_evidence": dict(selected),
        "selected_threshold_temporal_evidence": dict(
            temporal_by_threshold.get(float(selected.get("threshold") or 0.0), {})
        ) or None,
        "tiers": tiers,
        "product_claim_allowed": False,
    }


def render_decision_policy_report(policy: Mapping[str, Any]) -> str:
    lines = [
        "# Signal decision policy",
        "",
        f"- Status: {policy.get('status')}",
        f"- Default action: {policy.get('default_action')}",
        f"- Reason: {policy.get('reason_code')}",
        f"- Selected threshold: {policy.get('selected_threshold')}",
        f"- Product claim allowed: {policy.get('product_claim_allowed')}",
        "",
        "## Confidence tiers",
        "",
        "| Tier | Range | Action |",
        "|---|---:|---|",
    ]
    for tier in policy.get("tiers", []):
        lines.append(
            "| {name} | {lo:.2f}–{hi:.2f} | {action} |".format(
                name=tier["name"],
                lo=float(tier["min_confidence"]),
                hi=float(tier["max_confidence"]),
                action=tier["action"],
            )
        )
    evidence = policy.get("selected_threshold_evidence")
    if evidence:
        lines.extend(
            [
                "",
                "## Selected threshold evidence",
                "",
                f"- Selected rows: {evidence.get('selected_rows')}",
                f"- Success rate: {evidence.get('success_rate')}",
                f"- Wilson lower 95%: {evidence.get('wilson_lower_95')}",
                f"- Mean selected result bps: {evidence.get('mean_selected_result_bps')}",
            ]
        )
    temporal = policy.get("selected_threshold_temporal_evidence")
    if temporal:
        lines.extend(
            [
                "",
                "## Temporal stability evidence",
                "",
                f"- Blocks with selected signals: {temporal.get('blocks_with_selected')} / {temporal.get('blocks')}",
                f"- Minimum block success rate: {temporal.get('min_success_rate')}",
                f"- Minimum block Wilson lower 95%: {temporal.get('min_wilson_lower_95')}",
                f"- Weak blocks: {temporal.get('weak_blocks')}",
            ]
        )
    return "\n".join(lines) + "\n"


def run_research(dataset: Path, output_dir: Path) -> dict[str, Any]:
    rows = read_table(dataset)
    train_rows, validation_rows = chronological_split(rows)
    validation_sessions = _validation_sessions(validation_rows)
    naive_positive_rate = _naive_positive_rate(validation_rows)
    event_study = event_study_summary(validation_rows, split="validation")
    bayesian = bayesian_score_summary(train_rows, split="train") + bayesian_score_summary(validation_rows, split="validation")
    model_results: list[dict[str, Any]] = []
    feature_importance = univariate_feature_importance(train_rows)
    for runner in (run_logistic_regression, run_lightgbm_classifier, run_lightgbm_regressor):
        result, importance = runner(train_rows, validation_rows)
        model_results.append(result)
        feature_importance.extend(importance)
    (
        triage_result,
        confidence_thresholds,
        decision_audit,
        confidence_reliability,
        selective_frontier,
        high_confidence_slices,
        temporal_stability,
        temporal_stability_summary,
    ) = run_directional_triage(
        train_rows,
        validation_rows,
    )
    candidate_watchlist = candidate_watchlist_rows(selective_frontier)
    model_results.append(triage_result)
    bayesian_result, bayesian_thresholds, bayesian_temporal_summary, bayesian_candidates = run_bayesian_state_triage(
        train_rows,
        validation_rows,
    )
    model_results.append(bayesian_result)
    leaderboard = build_leaderboard(
        model_results,
        event_study,
        validation_sessions=validation_sessions,
        naive_positive_rate=naive_positive_rate,
    )
    decision_policy = build_decision_policy(confidence_thresholds, temporal_stability_summary)
    confidence_band_audit = build_confidence_band_audit(decision_audit)
    safe_triage_decisions = export_safe_triage_rows(
        audit_rows=decision_audit,
        policy=decision_policy,
        reliability_rows=confidence_reliability,
    )
    safe_triage_summary = summarize_safe_triage_rows(safe_triage_decisions, policy=decision_policy)
    selective_rule_candidates = mine_selective_rules(
        decision_audit,
        min_discovery_rows=50,
        min_discovery_success_rate=0.35,
        max_terms=2,
        top_n=500,
    )
    precision_scout_candidates = mine_precision_scout_rules(
        decision_audit,
        min_discovery_rows=20,
        min_discovery_success_rate=0.65,
        expansion_min_success_rate=0.15,
        max_terms=4,
        beam_width=250,
        top_n=500,
    )
    false_positive_guards = mine_false_positive_guards(
        decision_audit,
        top_n=200,
    )
    directional_state_candidates = mine_directional_state_candidates(
        decision_audit,
        min_discovery_rows=50,
        accepted_min_rows=300,
        accepted_min_sessions=30,
    )
    selective_rule_summary = {
        "candidate_rows": len(selective_rule_candidates),
        "accepted_shadow": sum(1 for row in selective_rule_candidates if row["accepted_shadow"]),
        "best_evaluation_success_rate": (
            max(float(row.get("evaluation_success_rate") or 0.0) for row in selective_rule_candidates)
            if selective_rule_candidates
            else None
        ),
        "best_evaluation_wilson_lower_95": (
            max(float(row.get("evaluation_wilson_lower_95") or 0.0) for row in selective_rule_candidates)
            if selective_rule_candidates
            else None
        ),
    }
    precision_scout_summary = summarize_precision_scout_rows(precision_scout_candidates)
    false_positive_guard_summary = summarize_false_positive_guards(false_positive_guards)
    directional_state_summary = {
        "candidate_rows": len(directional_state_candidates),
        "accepted_shadow": sum(1 for row in directional_state_candidates if row["accepted_shadow"]),
        "inverse_rows": sum(1 for row in directional_state_candidates if int(row.get("evaluation_inverse_rows") or 0) > 0),
        "temporal_supported": sum(1 for row in directional_state_candidates if row.get("temporal_supported")),
        "best_evaluation_success_rate": (
            max(float(row.get("evaluation_success_rate") or 0.0) for row in directional_state_candidates)
            if directional_state_candidates
            else None
        ),
    }
    honest_market_state_candidates = mine_honest_market_states(
        rows,
        min_discovery_rows=50,
        accepted_min_rows=300,
        accepted_min_sessions=30,
    )
    honest_market_state_summary = {
        "candidate_rows": len(honest_market_state_candidates),
        "accepted_shadow": sum(1 for row in honest_market_state_candidates if row["accepted_shadow"]),
        "best_evaluation_success_rate": (
            max(float(row.get("evaluation_success_rate") or 0.0) for row in honest_market_state_candidates)
            if honest_market_state_candidates
            else None
        ),
        "best_evaluation_wilson_lower_95": (
            max(float(row.get("evaluation_wilson_lower_95") or 0.0) for row in honest_market_state_candidates)
            if honest_market_state_candidates
            else None
        ),
    }
    payload = {
        "schema_version": 1,
        "kind": "signal_price_prediction_research_run",
        "dataset": str(dataset),
        "dataset_fingerprint": fingerprint_records(rows),
        "dataset_rows": len(rows),
        "train_rows": len(train_rows),
        "validation_rows": len(validation_rows),
        "validation_sessions": validation_sessions,
        "naive_positive_rate": naive_positive_rate,
        "event_study": event_study,
        "bayesian_score": bayesian,
        "models": model_results,
        "confidence_thresholds": confidence_thresholds,
        "decision_audit_rows": len(decision_audit),
        "confidence_reliability": confidence_reliability,
        "selective_frontier": selective_frontier,
        "candidate_watchlist": candidate_watchlist,
        "high_confidence_slices": high_confidence_slices,
        "temporal_stability": temporal_stability,
        "temporal_stability_summary": temporal_stability_summary,
        "bayesian_state_thresholds": bayesian_thresholds,
        "bayesian_state_temporal_summary": bayesian_temporal_summary,
        "bayesian_state_candidates": bayesian_candidates,
        "decision_policy": decision_policy,
        "confidence_band_audit": confidence_band_audit,
        "safe_triage_summary": safe_triage_summary,
        "selective_rule_candidates": selective_rule_candidates,
        "selective_rule_summary": selective_rule_summary,
        "precision_scout_candidates": precision_scout_candidates,
        "precision_scout_summary": precision_scout_summary,
        "false_positive_guards": false_positive_guards,
        "false_positive_guard_summary": false_positive_guard_summary,
        "directional_state_candidates": directional_state_candidates,
        "directional_state_summary": directional_state_summary,
        "honest_market_state_candidates": honest_market_state_candidates,
        "honest_market_state_summary": honest_market_state_summary,
        "leaderboard": leaderboard,
    }
    run_id = hashlib.sha256(
        json.dumps(
            {
                "dataset": str(dataset),
                "fingerprint": payload["dataset_fingerprint"],
                "models": [item["model"] for item in model_results],
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "model-results.json", payload)
    write_json(run_dir / "dataset-manifest.json", {"dataset": str(dataset), "fingerprint": payload["dataset_fingerprint"]})
    write_csv_records(run_dir / "leaderboard.csv", leaderboard or [{"model": "", "status": "", "n": "", "score": "", "accepted": ""}])
    write_csv_records(
        run_dir / "feature-importance.csv",
        feature_importance or [{"model": "", "feature": "", "importance": "", "signed_value": ""}],
    )
    write_csv_records(
        run_dir / "slice-report.csv",
        event_study or [{"model": "event_study_baseline", "split": "validation", "n": 0}],
    )
    write_csv_records(
        run_dir / "confidence-threshold-report.csv",
        confidence_thresholds
        or [
            {
                "threshold": "",
                "eligible_rows": "",
                "selected_rows": "",
                "skipped_rows": "",
                "up_decisions": "",
                "down_decisions": "",
                "direct_decisions": "",
                "inverse_decisions": "",
                "neutral_decisions": "",
                "success_count": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "accepted_research": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "decision-audit.csv",
        decision_audit
        or [
            {
                "row_id": "",
                "ticker": "",
                "signal_type": "",
                "source_event_at": "",
                "trading_day": "",
                "horizon_seconds": "",
                "session_bucket": "",
                "volatility_bucket": "",
                "consolidation_bucket": "",
                "liquidity_bucket": "",
                "spread_bucket": "",
                "depth_bucket": "",
                "imbalance_bucket": "",
                "orderbook_available": "",
                "orderbook_age_seconds": "",
                "orderbook_spread_bps": "",
                "orderbook_total_qty": "",
                "orderbook_imbalance_ratio": "",
                "signal_count_bucket": "",
                "combo_key_300s": "",
                "decision": "",
                "max_confidence": "",
                "confidence_band": "",
                "frontier_decision": "",
                "frontier_confidence": "",
                "frontier_success": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "confidence-reliability-report.csv",
        confidence_reliability
        or [
            {
                "scope": "",
                "rule": "",
                "nominal_action": "",
                "min_confidence": "",
                "max_confidence": "",
                "selected_rows": "",
                "sessions": "",
                "success_count": "",
                "observed_success_rate": "",
                "wilson_lower_95": "",
                "mean_model_confidence": "",
                "confidence_minus_observed": "",
                "mean_result_bps": "",
                "target_success_rate": "",
                "shadow_allowed": "",
                "product_90_allowed": "",
                "safe_runtime_action": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "selective-frontier.csv",
        selective_frontier
        or [
            {
                "scope": "",
                "rule": "",
                "group_population": "",
                "selected_rows": "",
                "sessions": "",
                "min_confidence": "",
                "max_confidence": "",
                "up_decisions": "",
                "down_decisions": "",
                "direct_decisions": "",
                "inverse_decisions": "",
                "neutral_decisions": "",
                "success_count": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "mean_selected_result_bps": "",
                "observed_90_success": "",
                "reliable_90_success": "",
                "accepted_research": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "candidate-watchlist.csv",
        candidate_watchlist
        or [
            {
                "candidate_id": "",
                "source_report": "",
                "scope": "",
                "rule": "",
                "selected_rows": "",
                "sessions": "",
                "selected_trading_days": "",
                "success_count": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "mean_selected_result_bps": "",
                "missing_rows_to_shadow_gate": "",
                "missing_sessions_to_shadow_gate": "",
                "additional_successes_needed_for_90pct_at_300": "",
                "additional_successes_needed_for_shadow_lower_bound_at_300": "",
                "additional_successes_needed_for_product_lower_bound_at_300": "",
                "status": "",
                "missing_reasons": "",
                "next_action": "",
                "product_claim_allowed": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "high-confidence-slices.csv",
        high_confidence_slices
        or [
            {
                "group_set": "",
                "rule": "",
                "threshold": "",
                "selected_rows": "",
                "sessions": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "mean_result_bps": "",
                "observed_90_success": "",
                "reliable_90_success": "",
                "accepted_shadow": "",
                "product_claim_allowed": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "temporal-stability-report.csv",
        temporal_stability
        or [
            {
                "threshold": "",
                "block_index": "",
                "block_count": "",
                "first_day": "",
                "last_day": "",
                "eligible_rows": "",
                "selected_rows": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "observed_90_success": "",
                "reliable_90_success": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "temporal-stability-summary.csv",
        temporal_stability_summary
        or [
            {
                "threshold": "",
                "blocks": "",
                "blocks_with_selected": "",
                "min_success_rate": "",
                "min_wilson_lower_95": "",
                "weak_blocks": "",
                "temporal_supported": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "bayesian-state-threshold-report.csv",
        bayesian_thresholds
        or [
            {
                "threshold": "",
                "eligible_rows": "",
                "selected_rows": "",
                "success_rate": "",
                "wilson_lower_95": "",
                "accepted_research": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "bayesian-state-temporal-summary.csv",
        bayesian_temporal_summary
        or [
            {
                "threshold": "",
                "blocks": "",
                "blocks_with_selected": "",
                "min_success_rate": "",
                "min_wilson_lower_95": "",
                "temporal_supported": "",
            }
        ],
    )
    write_csv_records(
        run_dir / "bayesian-state-candidates.csv",
        bayesian_candidates
        or [
            {
                "group_set": "",
                "state_key": "",
                "decision": "",
                "train_rows": "",
                "success_count": "",
                "train_success_rate": "",
                "posterior_mean": "",
                "wilson_lower_95": "",
            }
        ],
    )
    write_json(run_dir / "decision-policy.json", decision_policy)
    (run_dir / "decision-policy.md").write_text(render_decision_policy_report(decision_policy), encoding="utf-8")
    write_confidence_band_audit_csv(run_dir / "confidence-band-audit.csv", confidence_band_audit)
    write_confidence_band_audit_report(run_dir / "confidence-band-audit.md", confidence_band_audit)
    write_selective_rules_csv(run_dir / "selective-rule-candidates.csv", selective_rule_candidates)
    write_selective_rules_report(run_dir / "selective-rule-report.md", selective_rule_candidates)
    write_precision_scout_csv(run_dir / "precision-scout-candidates.csv", precision_scout_candidates)
    write_precision_scout_report(run_dir / "precision-scout-report.md", precision_scout_candidates)
    write_false_positive_guard_csv(run_dir / "false-positive-guards.csv", false_positive_guards)
    write_false_positive_guard_report(run_dir / "false-positive-guards.md", false_positive_guard_summary)
    write_directional_state_csv(run_dir / "directional-state-candidates.csv", directional_state_candidates)
    write_directional_state_report(run_dir / "directional-state-report.md", directional_state_candidates)
    honest_market_state_dir = run_dir / "honest-market-states"
    honest_market_state_dir.mkdir(parents=True, exist_ok=True)
    write_csv_records(
        honest_market_state_dir / "honest-market-state-candidates.csv",
        honest_market_state_candidates
        or [
            {
                "group_set": "",
                "rule": "",
                "evaluation_rows": "",
                "evaluation_sessions": "",
                "evaluation_success_rate": "",
                "evaluation_wilson_lower_95": "",
                "accepted_shadow": "",
            }
        ],
    )
    write_honest_market_state_report(
        honest_market_state_dir / "honest-market-state-report.md",
        honest_market_state_candidates,
    )
    safe_triage_dir = run_dir / "safe-triage"
    safe_triage_dir.mkdir(parents=True, exist_ok=True)
    write_safe_triage_csv(safe_triage_dir / "safe-triage-decisions.csv", safe_triage_decisions)
    write_json(safe_triage_dir / "safe-triage-summary.json", safe_triage_summary)
    write_safe_triage_report(safe_triage_dir / "safe-triage-report.md", safe_triage_summary)
    (run_dir / "report.md").write_text(render_markdown_report(payload), encoding="utf-8")
    write_selection_report(run_dir, run_dir)
    payload["run_id"] = run_id
    payload["run_dir"] = str(run_dir)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-train-price-models")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/runs"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_research(args.dataset, args.output_dir)
    print(
        json.dumps(
            {
                "status": "ok",
                "run_id": result["run_id"],
                "run_dir": result["run_dir"],
                "dataset_rows": result["dataset_rows"],
                "validation_sessions": result["validation_sessions"],
                "accepted_candidates": sum(1 for item in result["leaderboard"] if item.get("accepted")),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
