#!/usr/bin/env python3
"""Research target-before-risk and non-loss morning-retracement outcomes.

The original discovery pipeline predicts whether a retracement level is ever
touched before the morning deadline.  A live recommendation, however, follows
an entry/target/stop/break-even policy.  This script labels the exact competing
outcome first and fits separate target and non-loss probabilities.
"""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
import math
import statistics
import sys
import warnings
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    read_cache,
    read_table,
    write_json,
    write_table,
)
from tinvest_signal_engine.application.morning_retracement_research import (  # noqa: E402
    BuildMorningRetracementResearch,
    MorningRetracementExample,
    MorningRetracementResearchPolicy,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (  # noqa: E402
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (  # noqa: E402
    TradePolicy,
    simulate_trade,
)


TARGET_THRESHOLDS = tuple(value / 100 for value in range(50, 100, 2))
NON_LOSS_THRESHOLDS = tuple(value / 100 for value in range(50, 100, 2))
MOSCOW = ZoneInfo("Europe/Moscow")
MINIMUM_CURRENT_RETRACEMENT_FRACTION = 0.10
MINIMUM_RELATIVE_VOLUME = 0.50
MAXIMUM_RELATIVE_VOLUME = 10.0
MINIMUM_ACTIVE_MINUTE_RATIO = 0.50


@dataclass(frozen=True, slots=True)
class CompetingLabel:
    target_hit: bool
    non_loss: bool
    net_result_bps: float
    exit_reason: str
    minutes_to_target: float | None


@dataclass(slots=True)
class FittedOutcomeModels:
    name: str
    vectorizer: Any
    target_model: Any
    non_loss_model: Any


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/morning_retracement_competing"),
    )
    parser.add_argument("--round-trip-cost-bps", type=float, default=10.0)
    parser.add_argument("--minimum-events", type=int, default=300)
    parser.add_argument("--minimum-days", type=int, default=30)
    parser.add_argument(
        "--start-day",
        type=date.fromisoformat,
        default=date(2025, 1, 27),
        help="First eligible market regime day (morning equity session resumed).",
    )
    parser.add_argument("--end-day", type=date.fromisoformat)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    candles = read_cache(
        args.cache_dir,
        start_day=args.start_day,
        end_day=args.end_day,
    )
    examples = tuple(
        item
        for item in BuildMorningRetracementResearch(
            MorningRetracementResearchPolicy(
                round_trip_cost_bps=args.round_trip_cost_bps
            )
        ).execute(candles)
        if item.label_available
    )
    days = sorted({item.trading_day for item in examples})
    training_days = frozenset(days[: max(1, int(len(days) * 0.60))])
    evaluation_days = frozenset(day for day in days if day not in training_days)
    training = tuple(item for item in examples if item.trading_day in training_days)
    evaluation = tuple(item for item in examples if item.trading_day in evaluation_days)

    policies = (
        *(
            TradePolicy(
                0.25,
                0.40,
                0.15,
                11 * 60,
                args.round_trip_cost_bps,
                break_even_target_progress_fraction=progress,
            )
            for progress in (0.50, 0.67, 0.80, 0.90)
        ),
        TradePolicy(0.25, 0.40, 0.05, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.25, 0.40, 0.10, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.25, 0.25, 0.15, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.25, 0.40, 0.15, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.50, 0.40, 0.15, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.50, 0.40, 0.25, 11 * 60, args.round_trip_cost_bps),
        TradePolicy(0.50, 0.40, 0.33, 11 * 60, args.round_trip_cost_bps),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    all_results: list[dict[str, Any]] = []
    best: dict[str, Any] | None = None
    best_by_model: dict[str, dict[str, Any]] = {}
    for policy in policies:
        train_rows = _labeled_rows(training, policy)
        evaluation_rows = _labeled_rows(evaluation, policy)
        training_ticker_allowlist = _training_ticker_allowlist(train_rows)
        doubled_slippage_labels = _robust_labels(
            evaluation,
            policy=TradePolicy(
                target_fraction=policy.target_fraction,
                stop_extension_fraction=policy.stop_extension_fraction,
                break_even_trigger_fraction=(
                    policy.break_even_trigger_fraction
                ),
                deadline_local_minute=policy.deadline_local_minute,
                round_trip_cost_bps=policy.round_trip_cost_bps,
                doubled_slippage_bps=args.round_trip_cost_bps,
                break_even_target_progress_fraction=(
                    policy.break_even_target_progress_fraction
                ),
            ),
        )
        for family in ("morning", "morning_ticker"):
            for model_name in (
                "dual_logistic",
                "dual_tree_depth_3",
                "dual_lightgbm",
            ):
                models = _fit_models(
                    train_rows,
                    family=family,
                    model_name=model_name,
                )
                target_probabilities, non_loss_probabilities = _predict(
                    models,
                    evaluation_rows,
                    family=family,
                )
                training_target_probabilities, training_non_loss_probabilities = (
                    _predict(models, train_rows, family=family)
                )
                scored_name = (
                    f"{policy.key}-{family.replace('_', '-')}-{model_name}"
                )
                write_json(
                    args.output_dir / "models" / f"{scored_name}.json",
                    _model_diagnostics(models),
                )
                write_table(
                    args.output_dir
                    / f"scored-{scored_name}.parquet",
                    _scored_rows(
                        evaluation_rows,
                        target_probabilities,
                        non_loss_probabilities,
                        partition="evaluation",
                    ),
                )
                write_table(
                    args.output_dir / f"training-{scored_name}.parquet",
                    _scored_rows(
                        train_rows,
                        training_target_probabilities,
                        training_non_loss_probabilities,
                        partition="training",
                    ),
                )
                for instrument_filter, allowed_tickers in (
                    ("all_instruments", None),
                    (
                        "training_ticker_allowlist",
                        training_ticker_allowlist,
                    ),
                ):
                    frontier = _threshold_frontier(
                        evaluation_rows,
                        target_probabilities,
                        non_loss_probabilities,
                        minimum_events=args.minimum_events,
                        minimum_days=args.minimum_days,
                        doubled_slippage_labels=doubled_slippage_labels,
                        allowed_tickers=allowed_tickers,
                    )
                    for row in frontier:
                        result = {
                            "policy_key": policy.key,
                            "target_fraction": policy.target_fraction,
                            "stop_extension_fraction": (
                                policy.stop_extension_fraction
                            ),
                            "break_even_trigger_fraction": (
                                policy.break_even_trigger_fraction
                            ),
                            "break_even_target_progress_fraction": (
                                policy.break_even_target_progress_fraction
                            ),
                            "deadline_local_minute": (
                                policy.deadline_local_minute
                            ),
                            "model_name": models.name,
                            "feature_family": family,
                            "instrument_filter": instrument_filter,
                            "enabled_tickers": (
                                "|".join(sorted(allowed_tickers))
                                if allowed_tickers is not None
                                else ""
                            ),
                            **row,
                        }
                        all_results.append(result)
                        if best is None or _rank(result) > _rank(best):
                            best = result
                        model_key = f"{models.name}:{family}"
                        current_model_best = best_by_model.get(model_key)
                        if (
                            current_model_best is None
                            or _rank(result) > _rank(current_model_best)
                        ):
                            best_by_model[model_key] = result
                print(
                    json.dumps(
                        {
                            "progress": "model_evaluated",
                            "policy_key": policy.key,
                            "feature_family": family,
                            "model_name": models.name,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
    if best is None:
        raise SystemExit("No competing-outcome candidate produced a trade.")

    write_table(args.output_dir / "frontier.parquet", all_results)
    runtime_selected = max(
        (
            row
            for row in all_results
            if row["model_name"] == "dual_logistic_competing_outcomes"
        ),
        key=_runtime_rank,
    )
    write_json(
        args.output_dir / "selected.json",
        {
            "schema": "morning-retracement-competing-research-v1",
            "training_start": min(training_days).isoformat(),
            "training_end": max(training_days).isoformat(),
            "evaluation_start": min(evaluation_days).isoformat(),
            "evaluation_end": max(evaluation_days).isoformat(),
            "training_days": len(training_days),
            "evaluation_days": len(evaluation_days),
            "examples": len(examples),
            "episodes": len({item.episode_id for item in examples}),
            "market_regime_start": args.start_day.isoformat(),
            "market_regime_end": (
                args.end_day.isoformat() if args.end_day is not None else None
            ),
            "selected": best,
            "runtime_selected": runtime_selected,
            "compact_rule_within_control_tolerance": (
                _within_control_tolerance(runtime_selected, best)
            ),
            "selected_by_model": best_by_model,
        },
    )
    runtime_artifact = _runtime_artifact(
        output_dir=args.output_dir,
        selected=runtime_selected,
        control=best,
        start_day=args.start_day,
        end_day=args.end_day,
        training_days=len(training_days),
        evaluation_days=len(evaluation_days),
        episodes=len({item.episode_id for item in examples}),
    )
    write_json(args.output_dir / "runtime-policy.json", runtime_artifact)
    print(json.dumps(best, ensure_ascii=False, sort_keys=True))
    return 0


def _within_control_tolerance(
    candidate: Mapping[str, Any],
    control: Mapping[str, Any],
) -> bool:
    return (
        float(control["target_hit_rate"]) - float(candidate["target_hit_rate"])
        <= 0.03
        and float(control["non_loss_rate"]) - float(candidate["non_loss_rate"])
        <= 0.03
    )


def _runtime_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    high_precision = (
        float(row["target_hit_rate"]) >= 0.90
        and float(row["non_loss_rate"]) >= 0.95
        and float(row["doubled_slippage_target_rate"]) >= 0.90
        and float(row["doubled_slippage_non_loss_rate"]) >= 0.95
        and float(row["median_net_bps"]) > 0.0
        and float(row["doubled_slippage_median_net_bps"]) > 0.0
        and int(row["tickers"]) >= 3
    )
    return (
        int(high_precision),
        int(row["trades"]) if high_precision else 0,
        min(
            float(row["target_wilson_lower"]),
            float(row["non_loss_wilson_lower"]),
        ),
        *_rank(row),
    )


def _runtime_artifact(
    *,
    output_dir: Path,
    selected: Mapping[str, Any],
    control: Mapping[str, Any],
    start_day: date,
    end_day: date | None,
    training_days: int,
    evaluation_days: int,
    episodes: int,
) -> dict[str, Any]:
    scored_name = (
        f"{selected['policy_key']}-"
        f"{str(selected['feature_family']).replace('_', '-')}-dual_logistic"
    )
    diagnostics = json.loads(
        (output_dir / "models" / f"{scored_name}.json").read_text(
            encoding="utf-8"
        )
    )
    scored_rows = read_table(output_dir / f"scored-{scored_name}.parquet")
    selected_rows = _select_runtime_rows(
        scored_rows,
        target_threshold=float(selected["target_probability_threshold"]),
        non_loss_threshold=float(
            selected["non_loss_probability_threshold"]
        ),
        enabled_tickers=_enabled_tickers(selected),
    )
    exact_selected = dict(selected)
    exact_selected["target_day_bootstrap_lower"] = _day_bootstrap_lower(
        [
            (str(row["trading_day"]), float(row["target_hit"]))
            for row in selected_rows
        ],
        samples=5_000,
    )
    exact_selected["non_loss_day_bootstrap_lower"] = _day_bootstrap_lower(
        [
            (str(row["trading_day"]), float(row["non_loss"]))
            for row in selected_rows
        ],
        samples=5_000,
    )
    selected = exact_selected
    target_model = _linear_model_artifact(diagnostics, outcome="target")
    non_loss_model = _linear_model_artifact(
        diagnostics,
        outcome="non_loss",
    )
    expected_hit_window = _runtime_hit_window(selected_rows)
    ticker_slices = _runtime_ticker_slices(selected_rows)
    compact_rule_passed = _within_control_tolerance(selected, control)
    gates = _runtime_gates(selected, compact_rule_passed=compact_rule_passed)
    product_claim_allowed = all(bool(item["passed"]) for item in gates)
    run_identity = {
        "market_regime_start": start_day.isoformat(),
        "market_regime_end": end_day.isoformat() if end_day else None,
        "selected": dict(selected),
        "target_model_fingerprint": target_model["fingerprint"],
        "non_loss_model_fingerprint": non_loss_model["fingerprint"],
    }
    run_id = sha256(
        json.dumps(
            run_identity,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    recommendation = {
        "status": "validated" if product_claim_allowed else "observed",
        "validated": product_claim_allowed,
        "target_fraction": float(selected["target_fraction"]),
        "target_probability": float(selected["target_hit_rate"]),
        "target_probability_lower": float(
            selected["target_day_bootstrap_lower"]
        ),
        "non_loss_probability": float(selected["non_loss_rate"]),
        "non_loss_probability_lower": float(
            selected["non_loss_day_bootstrap_lower"]
        ),
        "sample_count": int(selected["trades"]),
        "trading_days": int(selected["trading_days"]),
        "disclaimer_code": "historical_observation_not_guarantee",
    }
    operational_filter = {
        "filter_id": "causal-reversal-and-liquidity-v1",
        "require_volume_baseline": True,
        "minimum_current_retracement_fraction": (
            MINIMUM_CURRENT_RETRACEMENT_FRACTION
        ),
        "minimum_relative_volume": MINIMUM_RELATIVE_VOLUME,
        "maximum_relative_volume": MAXIMUM_RELATIVE_VOLUME,
        "minimum_active_minute_ratio": MINIMUM_ACTIVE_MINUTE_RATIO,
        "enabled_tickers": [
            item for item in sorted(_enabled_tickers(selected))
        ],
    }
    artifact: dict[str, Any] = {
        "artifact_schema": "morning-retracement-competing-runtime-v1",
        "run_id": run_id,
        "policy_version": "morning-retracement-competing-v2.3.0:runtime-v1",
        "product_hypothesis_version": "2.3.0",
        "hypothesis_version": "morning-retracement-competing-v2.3.0",
        "research_only": not product_claim_allowed,
        "product_claim_allowed": product_claim_allowed,
        "independent_holdout": False,
        "recommendation": recommendation,
        "target_fraction": float(selected["target_fraction"]),
        "expected_direction": "opposite_to_running_morning_extreme",
        "feature_family": str(selected["feature_family"]),
        "model_name": "dual_logistic_competing_outcomes",
        "probability_threshold": float(
            selected["target_probability_threshold"]
        ),
        "non_loss_probability_threshold": float(
            selected["non_loss_probability_threshold"]
        ),
        "stop_extension_fraction": float(
            selected["stop_extension_fraction"]
        ),
        "break_even_trigger_fraction": float(
            selected["break_even_trigger_fraction"]
        ),
        "break_even_target_progress_fraction": (
            float(selected["break_even_target_progress_fraction"])
            if selected["break_even_target_progress_fraction"] is not None
            else None
        ),
        "deadline_local_minute": int(selected["deadline_local_minute"]),
        "round_trip_cost_bps": 10.0,
        "operational_filter": operational_filter,
        "evaluation": {
            key: value
            for key, value in selected.items()
            if key
            not in {
                "model_name",
                "feature_family",
                "policy_key",
            }
        },
        "evaluation_doubled_slippage": {
            "sample_count": int(
                selected["doubled_slippage_available_count"]
            ),
            "target_probability": float(
                selected["doubled_slippage_target_rate"]
            ),
            "non_loss_probability": float(
                selected["doubled_slippage_non_loss_rate"]
            ),
            "median_net_bps": float(
                selected["doubled_slippage_median_net_bps"]
            ),
        },
        "compact_rule_control": {
            "within_three_percentage_points": compact_rule_passed,
            "control_model_name": str(control["model_name"]),
            "control_feature_family": str(control["feature_family"]),
            "control_target_probability": float(
                control["target_hit_rate"]
            ),
            "control_non_loss_probability": float(
                control["non_loss_rate"]
            ),
        },
        "expected_hit_window": expected_hit_window,
        "ticker_slices": ticker_slices,
        "runtime_model": target_model,
        "runtime_target_model": target_model,
        "runtime_non_loss_model": non_loss_model,
        "model_explanation": _model_explanation(diagnostics),
        "gates": gates,
        "dataset": {
            "market_regime_start": start_day.isoformat(),
            "market_regime_end": end_day.isoformat() if end_day else None,
            "training_days": training_days,
            "evaluation_days": evaluation_days,
            "episodes": episodes,
        },
        "scientific_sources": [
            "https://arxiv.org/abs/1707.03498",
            "https://arxiv.org/abs/2003.10502",
            "https://doi.org/10.1016/j.cor.2004.06.001",
        ],
    }
    return artifact


def _select_runtime_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    target_threshold: float,
    non_loss_threshold: float,
    enabled_tickers: frozenset[str],
) -> tuple[Mapping[str, Any], ...]:
    selected: dict[str, Mapping[str, Any]] = {}
    for row in sorted(
        rows,
        key=lambda item: (str(item["observed_at"]), str(item["ticker"])),
    ):
        episode_id = str(row["episode_id"])
        if episode_id in selected:
            continue
        if (
            float(row["target_probability"]) < target_threshold
            or float(row["non_loss_probability"]) < non_loss_threshold
            or (
                enabled_tickers
                and str(row["ticker"]) not in enabled_tickers
            )
            or float(row["current_retracement_fraction"])
            < MINIMUM_CURRENT_RETRACEMENT_FRACTION
            or float(row["morning_volume_baseline_available"]) < 1.0
            or not (
                MINIMUM_RELATIVE_VOLUME
                <= float(row["morning_relative_volume"])
                <= MAXIMUM_RELATIVE_VOLUME
            )
            or float(row["morning_active_minute_ratio"])
            < MINIMUM_ACTIVE_MINUTE_RATIO
        ):
            continue
        selected[episode_id] = row
    return tuple(selected.values())


def _enabled_tickers(selected: Mapping[str, Any]) -> frozenset[str]:
    raw = selected.get("enabled_tickers", "")
    if isinstance(raw, str):
        return frozenset(item for item in raw.split("|") if item)
    if isinstance(raw, Sequence):
        return frozenset(str(item) for item in raw if str(item))
    return frozenset()


def _linear_model_artifact(
    diagnostics: Mapping[str, Any],
    *,
    outcome: str,
) -> dict[str, Any]:
    values = diagnostics[outcome]
    payload: dict[str, Any] = {
        "schema": "linear-probability-model-v1",
        "link": "logit",
        "positive_class": 1,
        "feature_names": [
            str(item) for item in diagnostics["feature_names"]
        ],
        "coefficients": [float(item) for item in values["coefficients"]],
        "intercept": float(values["intercept"]),
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    payload["fingerprint"] = "sha256:" + sha256(encoded).hexdigest()
    return payload


def _runtime_hit_window(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, float]:
    values = sorted(
        float(row["minutes_to_target"])
        for row in rows
        if row.get("minutes_to_target") is not None
    )
    if not values:
        return {
            "p25_minutes": 15.0,
            "median_minutes": 30.0,
            "p75_minutes": 60.0,
        }
    return {
        "p25_minutes": _percentile(values, 0.25),
        "median_minutes": _percentile(values, 0.50),
        "p75_minutes": _percentile(values, 0.75),
    }


def _percentile(values: Sequence[float], quantile: float) -> float:
    if not values:
        raise ValueError("percentile requires values")
    position = quantile * (len(values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(values[lower])
    weight = position - lower
    return float(values[lower] * (1.0 - weight) + values[upper] * weight)


def _runtime_ticker_slices(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["ticker"]), []).append(row)
    return [
        {
            "ticker": ticker,
            "sample_count": len(values),
            "target_probability": statistics.fmean(
                float(value["target_hit"]) for value in values
            ),
            "non_loss_probability": statistics.fmean(
                float(value["non_loss"]) for value in values
            ),
            "median_net_bps": statistics.median(
                float(value["net_result_bps"]) for value in values
            ),
        }
        for ticker, values in sorted(grouped.items())
    ]


def _model_explanation(diagnostics: Mapping[str, Any]) -> str:
    names = [str(item) for item in diagnostics["feature_names"]]
    lines: list[str] = []
    for outcome in ("target", "non_loss"):
        coefficients = [
            float(item) for item in diagnostics[outcome]["coefficients"]
        ]
        strongest = sorted(
            zip(names, coefficients),
            key=lambda item: abs(item[1]),
            reverse=True,
        )[:10]
        lines.append(
            outcome
            + ": "
            + ", ".join(f"{name}={value:+.4f}" for name, value in strongest)
        )
    return "\n".join(lines)


def _runtime_gates(
    selected: Mapping[str, Any],
    *,
    compact_rule_passed: bool,
) -> list[dict[str, Any]]:
    checks = (
        ("minimum_episodes_300", int(selected["trades"]) >= 300),
        ("minimum_trading_days_30", int(selected["trading_days"]) >= 30),
        ("target_rate_90pct", float(selected["target_hit_rate"]) >= 0.90),
        (
            "target_day_bootstrap_lower_80pct",
            float(selected["target_day_bootstrap_lower"]) >= 0.80,
        ),
        ("non_loss_rate_95pct", float(selected["non_loss_rate"]) >= 0.95),
        (
            "non_loss_day_bootstrap_lower_90pct",
            float(selected["non_loss_day_bootstrap_lower"]) >= 0.90,
        ),
        ("positive_median_net", float(selected["median_net_bps"]) > 0.0),
        ("multiple_instruments", int(selected["tickers"]) >= 3),
        (
            "instrument_share_at_most_50pct",
            float(selected["maximum_instrument_share"]) <= 0.50,
        ),
        (
            "doubled_slippage_target_rate_90pct",
            float(selected["doubled_slippage_target_rate"]) >= 0.90,
        ),
        (
            "doubled_slippage_non_loss_rate_95pct",
            float(selected["doubled_slippage_non_loss_rate"]) >= 0.95,
        ),
        (
            "doubled_slippage_positive_median",
            float(selected["doubled_slippage_median_net_bps"]) > 0.0,
        ),
        ("compact_rule_within_3pp", compact_rule_passed),
    )
    return [{"gate": name, "passed": passed} for name, passed in checks]


def _labeled_rows(
    examples: Sequence[MorningRetracementExample],
    policy: TradePolicy,
) -> tuple[tuple[MorningRetracementExample, CompetingLabel], ...]:
    result: list[tuple[MorningRetracementExample, CompetingLabel]] = []
    for item in examples:
        if item.snapshot.excursion_bps < 4.0 * policy.round_trip_cost_bps:
            continue
        target = item.snapshot.target_price(policy.target_fraction)
        remaining_move_bps = (
            int(item.snapshot.direction)
            * (target / item.snapshot.current_price - 1.0)
            * 10_000.0
        )
        if remaining_move_bps < 2.0 * policy.round_trip_cost_bps:
            continue
        future = tuple(
            row
            for row in item.future_candles
            if _local_minute(row) <= policy.deadline_local_minute
        )
        simulation = simulate_trade(item.snapshot, future, policy)
        if simulation.net_result_bps is None:
            continue
        result.append(
            (
                item,
                CompetingLabel(
                    target_hit=simulation.target_hit,
                    non_loss=simulation.non_loss,
                    net_result_bps=float(simulation.net_result_bps),
                    exit_reason=simulation.exit_reason.value,
                    minutes_to_target=(
                        (simulation.exit_at - simulation.entry_at).total_seconds()
                        / 60.0
                        if simulation.target_hit
                        and simulation.exit_at is not None
                        and simulation.entry_at is not None
                        else None
                    ),
                ),
            )
        )
    return tuple(result)


def _training_ticker_allowlist(
    rows: Sequence[tuple[MorningRetracementExample, CompetingLabel]],
) -> frozenset[str]:
    selected: dict[str, tuple[MorningRetracementExample, CompetingLabel]] = {}
    for item, label in sorted(
        rows,
        key=lambda value: (
            value[0].snapshot.observed_at,
            value[0].snapshot.ticker,
        ),
    ):
        if item.episode_id in selected:
            continue
        if not _passes_operational_filter(item.feature_values("morning")):
            continue
        selected[item.episode_id] = (item, label)
    grouped: dict[str, list[CompetingLabel]] = {}
    for item, label in selected.values():
        grouped.setdefault(item.snapshot.ticker, []).append(label)
    qualified = {
        ticker
        for ticker, labels in grouped.items()
        if len(labels) >= 20
        and statistics.fmean(float(item.target_hit) for item in labels) >= 0.85
        and statistics.fmean(float(item.non_loss) for item in labels) >= 0.90
        and statistics.median(item.net_result_bps for item in labels) > 0.0
    }
    if len(qualified) >= 3:
        return frozenset(qualified)
    ranked = sorted(
        (
            (
                min(
                    statistics.fmean(
                        float(item.target_hit) for item in labels
                    ),
                    statistics.fmean(
                        float(item.non_loss) for item in labels
                    ),
                ),
                statistics.median(item.net_result_bps for item in labels),
                len(labels),
                ticker,
            )
            for ticker, labels in grouped.items()
            if len(labels) >= 20
            and statistics.median(item.net_result_bps for item in labels) > 0.0
        ),
        reverse=True,
    )
    return frozenset(item[3] for item in ranked[:8])


def _passes_operational_filter(features: Mapping[str, Any]) -> bool:
    return (
        float(features["current_retracement_fraction"])
        >= MINIMUM_CURRENT_RETRACEMENT_FRACTION
        and float(features["morning_volume_baseline_available"]) >= 1.0
        and MINIMUM_RELATIVE_VOLUME
        <= float(features["morning_relative_volume"])
        <= MAXIMUM_RELATIVE_VOLUME
        and float(features["morning_active_minute_ratio"])
        >= MINIMUM_ACTIVE_MINUTE_RATIO
    )


def _feature_mapping(
    item: MorningRetracementExample,
    *,
    family: str,
) -> dict[str, float | str]:
    result: dict[str, float | str] = dict(item.feature_values("morning"))
    if family == "morning_ticker":
        result["ticker"] = item.snapshot.ticker
    return result


def _fit_models(
    rows: Sequence[tuple[MorningRetracementExample, CompetingLabel]],
    *,
    family: str,
    model_name: str,
) -> FittedOutcomeModels:
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from sklearn.linear_model import LogisticRegression  # type: ignore
    except ImportError as exc:
        raise SystemExit("Run: uv sync --extra research") from exc

    vectorizer = DictVectorizer(sparse=True)
    x = _int32_sparse(
        vectorizer.fit_transform(
            [_feature_mapping(item, family=family) for item, _ in rows]
        )
    )
    counts = Counter(item.episode_id for item, _ in rows)
    weights = [1.0 / counts[item.episode_id] for item, _ in rows]
    target_y = [int(label.target_hit) for _, label in rows]
    non_loss_y = [int(label.non_loss) for _, label in rows]
    if model_name == "dual_logistic":
        target_model = LogisticRegression(
            max_iter=3000,
            C=0.25,
            random_state=20260729,
            solver="liblinear",
        )
        non_loss_model = LogisticRegression(
            max_iter=3000,
            C=0.25,
            random_state=20260729,
            solver="liblinear",
        )
        fitted_name = "dual_logistic_competing_outcomes"
    elif model_name == "dual_tree_depth_3":
        try:
            from sklearn.tree import DecisionTreeClassifier  # type: ignore
        except ImportError as exc:
            raise SystemExit("Run: uv sync --extra research") from exc
        parameters = {
            "max_depth": 3,
            "min_samples_leaf": max(30, len(rows) // 100),
            "random_state": 20260729,
        }
        target_model = DecisionTreeClassifier(**parameters)
        non_loss_model = DecisionTreeClassifier(**parameters)
        fitted_name = "dual_tree_depth_3_competing_outcomes"
    elif model_name == "dual_lightgbm":
        try:
            from lightgbm import LGBMClassifier  # type: ignore
        except ImportError as exc:
            raise SystemExit("Run: uv sync --extra research") from exc
        parameters = {
            "n_estimators": 200,
            "learning_rate": 0.03,
            "num_leaves": 7,
            "max_depth": 3,
            "min_child_samples": 75,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "reg_lambda": 2.0,
            "random_state": 20260729,
            "verbose": -1,
        }
        target_model = LGBMClassifier(**parameters)
        non_loss_model = LGBMClassifier(**parameters)
        fitted_name = "dual_lightgbm_competing_outcomes"
    else:
        raise ValueError(f"unknown model {model_name}")
    target_model.fit(x, target_y, sample_weight=weights)
    non_loss_model.fit(x, non_loss_y, sample_weight=weights)
    return FittedOutcomeModels(
        name=fitted_name,
        vectorizer=vectorizer,
        target_model=target_model,
        non_loss_model=non_loss_model,
    )


def _predict(
    models: FittedOutcomeModels,
    rows: Sequence[tuple[MorningRetracementExample, CompetingLabel]],
    *,
    family: str,
) -> tuple[list[float], list[float]]:
    x = _int32_sparse(
        models.vectorizer.transform(
            [_feature_mapping(item, family=family) for item, _ in rows]
        )
    )
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names",
            category=UserWarning,
        )
        target = models.target_model.predict_proba(x)[:, 1]
        non_loss = models.non_loss_model.predict_proba(x)[:, 1]
    return (
        [float(value) for value in target],
        [float(value) for value in non_loss],
    )


def _model_diagnostics(models: FittedOutcomeModels) -> dict[str, Any]:
    names = [str(item) for item in models.vectorizer.get_feature_names_out()]
    result: dict[str, Any] = {
        "model_name": models.name,
        "feature_names": names,
    }
    if models.name == "dual_logistic_competing_outcomes":
        result["target"] = {
            "intercept": float(models.target_model.intercept_[0]),
            "coefficients": [
                float(value) for value in models.target_model.coef_[0]
            ],
        }
        result["non_loss"] = {
            "intercept": float(models.non_loss_model.intercept_[0]),
            "coefficients": [
                float(value) for value in models.non_loss_model.coef_[0]
            ],
        }
    elif models.name == "dual_tree_depth_3_competing_outcomes":
        from sklearn.tree import export_text  # type: ignore

        result["target_rule"] = export_text(
            models.target_model,
            feature_names=names,
            decimals=4,
        )
        result["non_loss_rule"] = export_text(
            models.non_loss_model,
            feature_names=names,
            decimals=4,
        )
    else:
        result["target_feature_importance"] = {
            name: int(value)
            for name, value in zip(
                names,
                models.target_model.feature_importances_,
            )
        }
        result["non_loss_feature_importance"] = {
            name: int(value)
            for name, value in zip(
                names,
                models.non_loss_model.feature_importances_,
            )
        }
    return result


def _threshold_frontier(
    rows: Sequence[tuple[MorningRetracementExample, CompetingLabel]],
    target_probabilities: Sequence[float],
    non_loss_probabilities: Sequence[float],
    *,
    minimum_events: int,
    minimum_days: int,
    doubled_slippage_labels: Mapping[str, CompetingLabel],
    allowed_tickers: frozenset[str] | None,
) -> list[dict[str, Any]]:
    import numpy as np

    result: list[dict[str, Any]] = []
    ordered = sorted(
        zip(rows, target_probabilities, non_loss_probabilities),
        key=lambda value: (
            value[0][0].snapshot.observed_at,
            value[0][0].snapshot.ticker,
        ),
    )
    items = [value[0][0] for value in ordered]
    labels = [value[0][1] for value in ordered]
    target_values = np.asarray([value[1] for value in ordered], dtype=float)
    non_loss_values = np.asarray([value[2] for value in ordered], dtype=float)
    episode_codes_by_id: dict[str, int] = {}
    episode_codes = np.empty(len(ordered), dtype=np.int32)
    for position, item in enumerate(items):
        episode_codes[position] = episode_codes_by_id.setdefault(
            item.episode_id,
            len(episode_codes_by_id),
        )
    target_hits_array = np.asarray(
        [label.target_hit for label in labels],
        dtype=np.int8,
    )
    non_losses_array = np.asarray(
        [label.non_loss for label in labels],
        dtype=np.int8,
    )
    net_results_array = np.asarray(
        [label.net_result_bps for label in labels],
        dtype=float,
    )
    trading_days_array = np.asarray(
        [item.trading_day for item in items],
        dtype=object,
    )
    tickers_array = np.asarray(
        [item.snapshot.ticker for item in items],
        dtype=object,
    )
    morning_features = [item.feature_values("morning") for item in items]
    operational_filter_mask = np.asarray(
        [
            _passes_operational_filter(features)
            and (
                allowed_tickers is None
                or item.snapshot.ticker in allowed_tickers
            )
            for item, features in zip(items, morning_features)
        ],
        dtype=bool,
    )
    doubled = [doubled_slippage_labels.get(item.row_id) for item in items]
    doubled_available_array = np.asarray(
        [item is not None for item in doubled],
        dtype=bool,
    )
    doubled_target_array = np.asarray(
        [bool(item is not None and item.target_hit) for item in doubled],
        dtype=np.int8,
    )
    doubled_non_loss_array = np.asarray(
        [bool(item is not None and item.non_loss) for item in doubled],
        dtype=np.int8,
    )
    doubled_net_array = np.asarray(
        [
            item.net_result_bps if item is not None else math.nan
            for item in doubled
        ],
        dtype=float,
    )
    for target_threshold in TARGET_THRESHOLDS:
        for non_loss_threshold in NON_LOSS_THRESHOLDS:
            eligible_indices = np.flatnonzero(
                operational_filter_mask
                & (target_values >= target_threshold)
                & (non_loss_values >= non_loss_threshold)
            )
            if eligible_indices.size == 0:
                continue
            _, first_positions = np.unique(
                episode_codes[eligible_indices],
                return_index=True,
            )
            selected_indices = eligible_indices[first_positions]
            target_hits = int(target_hits_array[selected_indices].sum())
            non_losses = int(non_losses_array[selected_indices].sum())
            count = int(selected_indices.size)
            selected_days = trading_days_array[selected_indices]
            selected_tickers = tickers_array[selected_indices]
            days = len(set(selected_days.tolist()))
            tickers = len(set(selected_tickers.tolist()))
            ticker_counts = Counter(selected_tickers.tolist())
            doubled_available_mask = doubled_available_array[selected_indices]
            doubled_available_count = int(doubled_available_mask.sum())
            doubled_nets = doubled_net_array[selected_indices][
                doubled_available_mask
            ]
            result.append(
                {
                    "target_probability_threshold": target_threshold,
                    "non_loss_probability_threshold": non_loss_threshold,
                    "trades": count,
                    "trading_days": days,
                    "tickers": tickers,
                    "target_hits": target_hits,
                    "target_hit_rate": target_hits / count,
                    "target_wilson_lower": _wilson_lower(target_hits, count),
                    "target_day_bootstrap_lower": 0.0,
                    "non_loss_count": non_losses,
                    "non_loss_rate": non_losses / count,
                    "non_loss_wilson_lower": _wilson_lower(non_losses, count),
                    "non_loss_day_bootstrap_lower": 0.0,
                    "median_net_bps": float(
                        np.median(net_results_array[selected_indices])
                    ),
                    "mean_net_bps": float(
                        np.mean(net_results_array[selected_indices])
                    ),
                    "maximum_instrument_share": max(ticker_counts.values()) / count,
                    "doubled_slippage_available_count": doubled_available_count,
                    "doubled_slippage_target_rate": (
                        float(doubled_target_array[selected_indices].sum()) / count
                    ),
                    "doubled_slippage_non_loss_rate": (
                        float(doubled_non_loss_array[selected_indices].sum()) / count
                    ),
                    "doubled_slippage_median_net_bps": (
                        float(np.median(doubled_nets))
                        if doubled_available_count
                        else -1_000_000_000.0
                    ),
                    "minimum_sample_met": (
                        count >= minimum_events and days >= minimum_days
                    ),
                    "minimum_current_retracement_fraction": (
                        MINIMUM_CURRENT_RETRACEMENT_FRACTION
                    ),
                    "minimum_relative_volume": MINIMUM_RELATIVE_VOLUME,
                    "maximum_relative_volume": MAXIMUM_RELATIVE_VOLUME,
                    "minimum_active_minute_ratio": MINIMUM_ACTIVE_MINUTE_RATIO,
                    "require_volume_baseline": True,
                    "_selected_indices": selected_indices,
                }
            )
    bootstrap_candidates = sorted(
        (
            row
            for row in result
            if bool(row["minimum_sample_met"])
            and float(row["median_net_bps"]) > 0.0
        ),
        key=lambda row: (
            min(
                float(row["target_wilson_lower"]),
                float(row["non_loss_wilson_lower"]),
            ),
            float(row["non_loss_rate"]),
            float(row["target_hit_rate"]),
        ),
        reverse=True,
    )[:5]
    for row in bootstrap_candidates:
        selected_indices = row["_selected_indices"]
        row["target_day_bootstrap_lower"] = _day_bootstrap_lower(
            [
                (
                    items[int(position)].trading_day,
                    float(labels[int(position)].target_hit),
                )
                for position in selected_indices
            ]
        )
        row["non_loss_day_bootstrap_lower"] = _day_bootstrap_lower(
            [
                (
                    items[int(position)].trading_day,
                    float(labels[int(position)].non_loss),
                )
                for position in selected_indices
            ]
        )
    for row in result:
        row.pop("_selected_indices", None)
    return result


def _scored_rows(
    rows: Sequence[tuple[MorningRetracementExample, CompetingLabel]],
    target_probabilities: Sequence[float],
    non_loss_probabilities: Sequence[float],
    *,
    partition: str,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for (item, label), target_probability, non_loss_probability in zip(
        rows,
        target_probabilities,
        non_loss_probabilities,
    ):
        features = item.feature_values("morning")
        result.append(
            {
                "episode_id": item.episode_id,
                "dataset_partition": partition,
                "trading_day": item.trading_day.isoformat(),
                "ticker": item.snapshot.ticker,
                "observed_at": item.snapshot.observed_at.isoformat(),
                "target_hit": label.target_hit,
                "non_loss": label.non_loss,
                "net_result_bps": label.net_result_bps,
                "exit_reason": label.exit_reason,
                "minutes_to_target": label.minutes_to_target,
                "target_probability": target_probability,
                "non_loss_probability": non_loss_probability,
                "excursion_bps": item.snapshot.excursion_bps,
                **features,
            }
        )
    return result


def _robust_labels(
    examples: Sequence[MorningRetracementExample],
    *,
    policy: TradePolicy,
) -> dict[str, CompetingLabel]:
    result: dict[str, CompetingLabel] = {}
    for item in examples:
        future = tuple(
            row
            for row in item.future_candles
            if _local_minute(row) <= policy.deadline_local_minute
        )
        simulation = simulate_trade(item.snapshot, future, policy)
        if simulation.net_result_bps is None:
            continue
        result[item.row_id] = CompetingLabel(
            target_hit=simulation.target_hit,
            non_loss=simulation.non_loss,
            net_result_bps=float(simulation.net_result_bps),
            exit_reason=simulation.exit_reason.value,
            minutes_to_target=(
                (simulation.exit_at - simulation.entry_at).total_seconds() / 60.0
                if simulation.target_hit
                and simulation.exit_at is not None
                and simulation.entry_at is not None
                else None
            ),
        )
    return result


def _rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    gates = (
        bool(row["minimum_sample_met"])
        and float(row["target_hit_rate"]) >= 0.90
        and float(row["target_day_bootstrap_lower"]) >= 0.80
        and float(row["non_loss_rate"]) >= 0.95
        and float(row["non_loss_day_bootstrap_lower"]) >= 0.90
        and float(row["median_net_bps"]) > 0.0
        and int(row["tickers"]) >= 3
        and float(row["maximum_instrument_share"]) <= 0.50
        and float(row["doubled_slippage_target_rate"]) >= 0.90
        and float(row["doubled_slippage_non_loss_rate"]) >= 0.95
        and float(row["doubled_slippage_median_net_bps"]) > 0.0
    )
    return (
        int(gates),
        int(row["minimum_sample_met"]),
        min(
            float(row["target_day_bootstrap_lower"]),
            float(row["non_loss_day_bootstrap_lower"]),
        ),
        min(
            float(row["target_wilson_lower"]),
            float(row["non_loss_wilson_lower"]),
        ),
        float(row["non_loss_rate"]),
        float(row["target_hit_rate"]),
        float(row["median_net_bps"]),
        int(row["trades"]),
    )


def _wilson_lower(successes: int, total: int) -> float:
    if total <= 0:
        return 0.0
    z = 1.959963984540054
    rate = successes / total
    denominator = 1.0 + z * z / total
    centre = rate + z * z / (2.0 * total)
    margin = z * math.sqrt(
        (rate * (1.0 - rate) + z * z / (4.0 * total)) / total
    )
    return (centre - margin) / denominator


def _day_bootstrap_lower(
    values: Sequence[tuple[Any, float]],
    *,
    samples: int = 500,
) -> float:
    import random

    grouped: dict[Any, list[float]] = {}
    for day, value in values:
        grouped.setdefault(day, []).append(value)
    days = sorted(grouped)
    if not days:
        return 0.0
    randomizer = random.Random(20260729)
    estimates: list[float] = []
    for _ in range(samples):
        sampled = [randomizer.choice(days) for _ in days]
        flattened = [value for day in sampled for value in grouped[day]]
        estimates.append(statistics.fmean(flattened))
    estimates.sort()
    return estimates[int(0.025 * (len(estimates) - 1))]


def _local_minute(candle: HistoricalCandle) -> int:
    local = candle.at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute


def _int32_sparse(matrix: Any) -> Any:
    """Normalize SciPy sparse indices for scikit-learn/liblinear."""

    import numpy as np

    matrix.indices = matrix.indices.astype(np.int32, copy=False)
    matrix.indptr = matrix.indptr.astype(np.int32, copy=False)
    return matrix


if __name__ == "__main__":
    raise SystemExit(main())
