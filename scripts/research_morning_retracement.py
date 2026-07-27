#!/usr/bin/env python3
"""Discover causal morning retracement conditions and safety-first policies."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
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
    PreviousSignalEvent,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (  # noqa: E402
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (  # noqa: E402
    TradePolicy,
    simulate_trade,
)


MOSCOW = ZoneInfo("Europe/Moscow")
TARGET_FRACTIONS = (0.25, 0.50, 0.75, 1.0)
STOP_FRACTIONS = (0.15, 0.25, 0.40)
BREAK_EVEN_FRACTIONS = (0.15, 0.25, 0.33)
DEADLINE_MINUTES = (9 * 60 + 30, 10 * 60, 10 * 60 + 30, 11 * 60)
PROBABILITY_THRESHOLDS = (
    0.50,
    0.60,
    0.70,
    0.75,
    0.80,
    0.85,
    0.90,
    0.92,
    0.95,
    0.97,
    0.99,
)


@dataclass(slots=True)
class FittedCandidate:
    target_fraction: float
    feature_family: str
    model_name: str
    vectorizer: Any
    model: Any
    validation_probabilities: list[float]
    validation_brier: float
    explanation: str
    threshold: float | None = None
    threshold_metrics: dict[str, Any] | None = None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-morning-retracement")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    parser.add_argument("--tickers", type=_parse_tickers)
    parser.add_argument(
        "--signal-events",
        "--previous-signals",
        dest="signal_events",
        type=Path,
        help="Optional JSONL or Parquet signal events from current and prior sessions.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/morning_retracement_runs"),
    )
    parser.add_argument("--round-trip-cost-bps", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--analytical-floor-bps", type=float, default=10.0)
    parser.add_argument("--minimum-validation-events", type=int, default=20)
    parser.add_argument(
        "--independent-holdout",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Mark holdout as unopened for this hypothesis version. "
            "Use only for a genuinely new chronological period."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if min(
        args.round_trip_cost_bps,
        args.slippage_bps,
        args.analytical_floor_bps,
    ) < 0.0:
        raise SystemExit("Costs and analytical floor must not be negative.")
    candles = tuple(
        HistoricalCandle(
            ticker=item.ticker,
            at=item.at,
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            volume=item.volume,
            complete=item.complete,
        )
        for item in read_cache(args.cache_dir, tickers=args.tickers)
    )
    if not candles:
        raise SystemExit("Candle cache is empty for the selected universe.")
    previous_signals = _read_previous_signals(args.signal_events)
    policy = MorningRetracementResearchPolicy(
        analytical_floor_bps=args.analytical_floor_bps,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    examples = BuildMorningRetracementResearch(policy).execute(
        candles,
        previous_signals=previous_signals,
    )
    labeled = tuple(item for item in examples if item.label_available)
    days = sorted({item.trading_day for item in labeled})
    if len(days) < 5:
        raise SystemExit(
            "At least five complete trading days are required for a 60/20/20 split."
        )
    train_days, validation_days, holdout_days = _split_days(days)
    train = tuple(item for item in labeled if item.trading_day in train_days)
    validation = tuple(
        item for item in labeled if item.trading_day in validation_days
    )
    holdout = tuple(item for item in labeled if item.trading_day in holdout_days)

    run_id = _run_id(
        candles=candles,
        policy=policy,
        tickers=args.tickers,
        previous_signal_count=len(previous_signals),
        independent_holdout=args.independent_holdout,
    )
    run_dir = args.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    dataset_rows = [_dataset_row(item, policy) for item in labeled]
    write_table(run_dir / "morning-retracement-dataset.parquet", dataset_rows)
    candidates, leaderboard = _fit_candidates(
        train=train,
        validation=validation,
        minimum_validation_events=args.minimum_validation_events,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    _write_csv(run_dir / "model-leaderboard.csv", leaderboard)
    best_by_target = _best_candidate_by_target(candidates)
    policy_rows = _evaluate_policy_frontier(
        best_by_target=best_by_target,
        validation=validation,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    _write_csv(run_dir / "policy-frontier.csv", policy_rows)
    if not policy_rows:
        raise SystemExit(
            "No validation policy had an eligible next-minute entry. "
            "Inspect model-leaderboard.csv and the dataset."
        )
    selected_validation = max(policy_rows, key=_policy_rank)
    selected_candidate = best_by_target[
        float(selected_validation["target_fraction"])
    ]
    selected_policy = _policy_from_row(
        selected_validation,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    locked_threshold = float(selected_validation["probability_threshold"])

    holdout_probabilities, final_explanation = _refit_for_holdout(
        selected_candidate,
        train + validation,
        holdout,
    )
    holdout_metrics = _evaluate_locked_policy(
        examples=holdout,
        probabilities=holdout_probabilities,
        threshold=locked_threshold,
        policy=selected_policy,
    )
    matched_controls = _matched_control_analysis(
        examples=holdout,
        probabilities=holdout_probabilities,
        threshold=locked_threshold,
        policy=selected_policy,
    )
    stress_policy = TradePolicy(
        target_fraction=selected_policy.target_fraction,
        stop_extension_fraction=selected_policy.stop_extension_fraction,
        break_even_trigger_fraction=selected_policy.break_even_trigger_fraction,
        deadline_local_minute=selected_policy.deadline_local_minute,
        round_trip_cost_bps=selected_policy.round_trip_cost_bps,
        doubled_slippage_bps=args.slippage_bps,
    )
    holdout_stress = _evaluate_locked_policy(
        examples=holdout,
        probabilities=holdout_probabilities,
        threshold=locked_threshold,
        policy=stress_policy,
    )
    previous_session_comparison = _previous_session_holdout_comparison(
        selected_candidate=selected_candidate,
        candidates=candidates,
        training=train + validation,
        holdout=holdout,
        selected_policy=selected_policy,
    )
    gates = _product_gates(
        holdout_metrics,
        holdout_stress,
        matched_control_lift_lower=float(
            matched_controls["target_rate_lift_day_bootstrap_lower"]
        ),
        previous_session_incremental_value=bool(
            previous_session_comparison["passed"]
        ),
        independent_holdout=args.independent_holdout,
    )
    selected_artifact = {
        "artifact_schema": "morning-retracement-candidate-v1",
        "run_id": run_id,
        "research_only": True,
        "independent_holdout": args.independent_holdout,
        "product_claim_allowed": all(item["passed"] for item in gates),
        "hypothesis_version": policy.version,
        "target_fraction": selected_policy.target_fraction,
        "expected_direction": "opposite_to_running_morning_extreme",
        "feature_family": selected_candidate.feature_family,
        "model_name": selected_candidate.model_name,
        "probability_threshold": locked_threshold,
        "stop_extension_fraction": selected_policy.stop_extension_fraction,
        "break_even_trigger_fraction": (
            selected_policy.break_even_trigger_fraction
        ),
        "deadline_local_minute": selected_policy.deadline_local_minute,
        "round_trip_cost_bps": selected_policy.round_trip_cost_bps,
        "validation": selected_validation,
        "holdout": holdout_metrics,
        "holdout_doubled_slippage": holdout_stress,
        "matched_controls": matched_controls,
        "previous_session_holdout_comparison": previous_session_comparison,
        "expected_hit_window": _expected_hit_window(
            validation,
            selected_candidate.validation_probabilities,
            locked_threshold,
            selected_policy.target_fraction,
        ),
        "model_explanation": final_explanation,
        "gates": gates,
        "scientific_sources": [
            "https://arxiv.org/abs/1707.03498",
            "https://arxiv.org/abs/2003.10502",
            "https://doi.org/10.1016/j.cor.2004.06.001",
        ],
    }
    write_json(run_dir / "selected-policy.json", selected_artifact)
    manifest = {
        "artifact_schema": "morning-retracement-dataset-manifest-v1",
        "run_id": run_id,
        "policy_version": policy.version,
        "cache_dir": str(args.cache_dir),
        "tickers": sorted({item.snapshot.ticker for item in labeled}),
        "rows": len(labeled),
        "episodes": len({item.episode_id for item in labeled}),
        "trading_days": len(days),
        "feature_leakage_rows": sum(
            any(
                feature.observed_at > item.feature_cutoff_at
                for feature in item.features
            )
            for item in labeled
        ),
        "split": {
            "train_days": len(train_days),
            "validation_days": len(validation_days),
            "holdout_days": len(holdout_days),
            "train_start": min(train_days).isoformat(),
            "train_end": max(train_days).isoformat(),
            "validation_start": min(validation_days).isoformat(),
            "validation_end": max(validation_days).isoformat(),
            "holdout_start": min(holdout_days).isoformat(),
            "holdout_end": max(holdout_days).isoformat(),
        },
        "previous_signal_events": len(previous_signals),
        "independent_holdout": args.independent_holdout,
        "dataset_fingerprint": _fingerprint_rows(dataset_rows),
    }
    write_json(run_dir / "dataset-manifest.json", manifest)
    (run_dir / "report.md").write_text(
        _render_report(
            manifest=manifest,
            leaderboard=leaderboard,
            selected=selected_artifact,
        ),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "run_id": run_id,
                "rows": len(labeled),
                "episodes": manifest["episodes"],
                "trading_days": len(days),
                "product_claim_allowed": selected_artifact[
                    "product_claim_allowed"
                ],
                "output": str(run_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _parse_tickers(raw: str) -> tuple[str, ...]:
    result = tuple(
        sorted({item.strip().upper() for item in raw.split(",") if item.strip()})
    )
    if not result:
        raise argparse.ArgumentTypeError("ticker list must not be empty")
    return result


def _read_previous_signals(path: Path | None) -> tuple[PreviousSignalEvent, ...]:
    if path is None:
        return ()
    if path.suffix == ".parquet":
        rows: Sequence[Mapping[str, Any]] = read_table(path)
    else:
        rows = tuple(
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    result: list[PreviousSignalEvent] = []
    for row in rows:
        event_at = _timestamp(
            row.get("source_event_at") or row.get("event_at")
        )
        ready_raw = row.get("outcome_ready_at")
        confirmed_raw = row.get("outcome_confirmed")
        result.append(
            PreviousSignalEvent(
                ticker=str(row["ticker"]).upper(),
                event_at=event_at,
                signal_type=str(row.get("signal_type") or "unknown"),
                direction=int(row.get("direction") or 0),
                outcome_ready_at=(
                    _timestamp(ready_raw) if ready_raw not in (None, "") else None
                ),
                outcome_confirmed=(
                    bool(confirmed_raw)
                    if confirmed_raw not in (None, "")
                    else None
                ),
            )
        )
    return tuple(result)


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("signal timestamp must be timezone-aware")
    return parsed


def _split_days(
    days: list[Any],
) -> tuple[frozenset[Any], frozenset[Any], frozenset[Any]]:
    train_end = max(1, int(len(days) * 0.60))
    validation_end = max(train_end + 1, int(len(days) * 0.80))
    validation_end = min(validation_end, len(days) - 1)
    return (
        frozenset(days[:train_end]),
        frozenset(days[train_end:validation_end]),
        frozenset(days[validation_end:]),
    )


def _feature_mapping(
    item: MorningRetracementExample, family: str
) -> dict[str, Any]:
    if family == "morning":
        features = item.feature_values("morning")
    elif family == "previous_session":
        features = item.feature_values("previous_session")
    elif family == "combined":
        features = item.feature_values()
    else:
        raise ValueError(f"unknown feature family {family}")
    result: dict[str, Any] = dict(features)
    result["ticker"] = item.snapshot.ticker
    return result


def _fit_candidates(
    *,
    train: tuple[MorningRetracementExample, ...],
    validation: tuple[MorningRetracementExample, ...],
    minimum_validation_events: int,
    round_trip_cost_bps: float,
) -> tuple[list[FittedCandidate], list[dict[str, Any]]]:
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from sklearn.linear_model import LogisticRegression  # type: ignore
        from sklearn.metrics import brier_score_loss  # type: ignore
        from sklearn.tree import DecisionTreeClassifier, export_text  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            'Research dependencies are missing. Run: uv sync --extra research'
        ) from exc

    candidates: list[FittedCandidate] = []
    leaderboard: list[dict[str, Any]] = []
    specifications = [
        ("morning", "logistic_regression"),
        ("previous_session", "logistic_regression"),
        ("combined", "logistic_regression"),
        ("combined", "shallow_decision_tree"),
        ("combined", "lightgbm"),
    ]
    for fraction in TARGET_FRACTIONS:
        train_y = [int(item.label_for(fraction).reached) for item in train]
        valid_y = [int(item.label_for(fraction).reached) for item in validation]
        for family, model_name in specifications:
            vectorizer = DictVectorizer(sparse=True)
            train_x = _int32_sparse(
                vectorizer.fit_transform(
                    [_feature_mapping(item, family) for item in train]
                )
            )
            valid_x = _int32_sparse(
                vectorizer.transform(
                    [_feature_mapping(item, family) for item in validation]
                )
            )
            if len(set(train_y)) < 2:
                leaderboard.append(
                    {
                        "target_fraction": fraction,
                        "feature_family": family,
                        "model_name": model_name,
                        "status": "insufficient_classes",
                    }
                )
                continue
            explanation = ""
            if model_name == "logistic_regression":
                model = LogisticRegression(
                    max_iter=2000,
                    class_weight="balanced",
                    random_state=20260728,
                    solver="liblinear",
                )
            elif model_name == "shallow_decision_tree":
                model = DecisionTreeClassifier(
                    max_depth=3,
                    min_samples_leaf=max(20, len(train) // 100),
                    class_weight="balanced",
                    random_state=20260728,
                )
            else:
                try:
                    from lightgbm import LGBMClassifier  # type: ignore
                except ImportError:
                    leaderboard.append(
                        {
                            "target_fraction": fraction,
                            "feature_family": family,
                            "model_name": model_name,
                            "status": "lightgbm_not_installed",
                        }
                    )
                    continue
                model = LGBMClassifier(
                    n_estimators=250,
                    learning_rate=0.03,
                    num_leaves=15,
                    min_child_samples=max(20, len(train) // 100),
                    subsample=0.8,
                    colsample_bytree=0.8,
                    class_weight="balanced",
                    random_state=20260728,
                    verbose=-1,
                )
            model.fit(train_x, train_y)
            probabilities = [
                float(item) for item in model.predict_proba(valid_x)[:, 1]
            ]
            if model_name == "shallow_decision_tree":
                explanation = export_text(
                    model,
                    feature_names=list(vectorizer.get_feature_names_out()),
                )
            elif model_name == "logistic_regression":
                names = list(vectorizer.get_feature_names_out())
                coefficients = list(model.coef_[0])
                strongest = sorted(
                    zip(names, coefficients),
                    key=lambda pair: abs(pair[1]),
                    reverse=True,
                )[:12]
                explanation = "\n".join(
                    f"{name}: {coefficient:+.6f}"
                    for name, coefficient in strongest
                )
            candidate = FittedCandidate(
                target_fraction=fraction,
                feature_family=family,
                model_name=model_name,
                vectorizer=vectorizer,
                model=model,
                validation_probabilities=probabilities,
                validation_brier=float(
                    brier_score_loss(valid_y, probabilities)
                ),
                explanation=explanation,
            )
            threshold, threshold_metrics = _select_probability_threshold(
                validation,
                probabilities,
                target_fraction=fraction,
                minimum_events=minimum_validation_events,
                round_trip_cost_bps=round_trip_cost_bps,
            )
            candidate.threshold = threshold
            candidate.threshold_metrics = threshold_metrics
            candidates.append(candidate)
            leaderboard.append(
                {
                    "target_fraction": fraction,
                    "feature_family": family,
                    "model_name": model_name,
                    "status": "ok",
                    "validation_brier": candidate.validation_brier,
                    "probability_threshold": threshold,
                    **(threshold_metrics or {}),
                }
            )
    return candidates, leaderboard


def _select_probability_threshold(
    examples: tuple[MorningRetracementExample, ...],
    probabilities: Sequence[float],
    *,
    target_fraction: float,
    minimum_events: int,
    round_trip_cost_bps: float,
) -> tuple[float | None, dict[str, Any] | None]:
    rows: list[tuple[float, dict[str, Any]]] = []
    for threshold in PROBABILITY_THRESHOLDS:
        selected = _select_first_episode_signal(
            examples,
            probabilities,
            threshold,
            target_fraction,
            round_trip_cost_bps,
        )
        if not selected:
            continue
        successes = sum(
            item.label_for(target_fraction).reached for item, _ in selected
        )
        count = len(selected)
        rate = successes / count
        metrics = {
            "selected_events": count,
            "target_hits": successes,
            "target_hit_rate": rate,
            "target_wilson_lower": _wilson_lower(successes, count),
            "selected_days": len({item.trading_day for item, _ in selected}),
            "selected_tickers": len(
                {item.snapshot.ticker for item, _ in selected}
            ),
        }
        score = (
            float(count >= minimum_events),
            metrics["target_wilson_lower"],
            rate,
            math.log1p(count),
        )
        rows.append((threshold, {"score": score, **metrics}))
    if not rows:
        return None, None
    threshold, metrics = max(rows, key=lambda item: item[1]["score"])
    metrics.pop("score", None)
    return threshold, metrics


def _best_candidate_by_target(
    candidates: Sequence[FittedCandidate],
) -> dict[float, FittedCandidate]:
    result: dict[float, FittedCandidate] = {}
    for fraction in TARGET_FRACTIONS:
        eligible = [
            item
            for item in candidates
            if item.target_fraction == fraction
            and item.threshold is not None
            and item.threshold_metrics is not None
        ]
        if not eligible:
            continue
        result[fraction] = max(
            eligible,
            key=lambda item: (
                item.threshold_metrics["target_wilson_lower"],
                item.threshold_metrics["target_hit_rate"],
                item.threshold_metrics["selected_events"],
                -item.validation_brier,
            ),
        )
    if not result:
        raise SystemExit("No model produced a selectable validation threshold.")
    return result


def _evaluate_policy_frontier(
    *,
    best_by_target: Mapping[float, FittedCandidate],
    validation: tuple[MorningRetracementExample, ...],
    round_trip_cost_bps: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fraction, candidate in best_by_target.items():
        assert candidate.threshold is not None
        selected = _select_first_episode_signal(
            validation,
            candidate.validation_probabilities,
            candidate.threshold,
            fraction,
            round_trip_cost_bps,
        )
        selected_examples = tuple(item for item, _ in selected)
        for stop_fraction in STOP_FRACTIONS:
            for break_even_fraction in BREAK_EVEN_FRACTIONS:
                for deadline in DEADLINE_MINUTES:
                    policy = TradePolicy(
                        target_fraction=fraction,
                        stop_extension_fraction=stop_fraction,
                        break_even_trigger_fraction=break_even_fraction,
                        deadline_local_minute=deadline,
                        round_trip_cost_bps=round_trip_cost_bps,
                    )
                    metrics = _simulate_examples(selected_examples, policy)
                    if metrics["trades"] == 0:
                        continue
                    rows.append(
                        {
                            "target_fraction": fraction,
                            "feature_family": candidate.feature_family,
                            "model_name": candidate.model_name,
                            "probability_threshold": candidate.threshold,
                            "stop_extension_fraction": stop_fraction,
                            "break_even_trigger_fraction": break_even_fraction,
                            "deadline_local_minute": deadline,
                            **metrics,
                        }
                    )
    return sorted(rows, key=_policy_rank, reverse=True)


def _select_first_episode_signal(
    examples: Sequence[MorningRetracementExample],
    probabilities: Sequence[float],
    threshold: float,
    target_fraction: float,
    round_trip_cost_bps: float,
) -> list[tuple[MorningRetracementExample, float]]:
    selected: dict[str, tuple[MorningRetracementExample, float]] = {}
    minimum_excursion = 4.0 * round_trip_cost_bps
    for item, probability in sorted(
        zip(examples, probabilities),
        key=lambda pair: (
            pair[0].snapshot.observed_at,
            pair[0].snapshot.ticker,
        ),
    ):
        if item.episode_id in selected or probability < threshold:
            continue
        if item.snapshot.excursion_bps < minimum_excursion:
            continue
        if not _entry_still_valid(
            item,
            target_fraction,
            round_trip_cost_bps=round_trip_cost_bps,
        ):
            continue
        selected[item.episode_id] = (item, float(probability))
    return list(selected.values())


def _entry_still_valid(
    item: MorningRetracementExample,
    target_fraction: float,
    *,
    round_trip_cost_bps: float,
) -> bool:
    target = item.snapshot.target_price(target_fraction)
    remaining_gross_bps = (
        int(item.snapshot.direction)
        * (target / item.snapshot.current_price - 1.0)
        * 10_000.0
    )
    # A target that merely covers modeled costs has no safety margin for the
    # next-minute entry or worse execution.  Discovery therefore requires a
    # remaining gross move of at least twice the full round-trip estimate.
    return remaining_gross_bps >= 2.0 * round_trip_cost_bps


def _simulate_examples(
    examples: Sequence[MorningRetracementExample],
    policy: TradePolicy,
) -> dict[str, Any]:
    simulations = []
    selected_examples = []
    for item in examples:
        if _local_minute(item.snapshot.observed_at) >= policy.deadline_local_minute:
            continue
        future = tuple(
            row
            for row in item.future_candles
            if _local_minute(row.at) <= policy.deadline_local_minute
        )
        simulation = simulate_trade(item.snapshot, future, policy)
        if simulation.net_result_bps is None:
            continue
        simulations.append(simulation)
        selected_examples.append(item)
    count = len(simulations)
    if not count:
        return {
            "trades": 0,
            "target_hits": 0,
            "target_hit_rate": 0.0,
            "target_wilson_lower": 0.0,
            "target_day_bootstrap_lower": 0.0,
            "target_day_bootstrap_upper": 0.0,
            "non_loss_count": 0,
            "non_loss_rate": 0.0,
            "non_loss_wilson_lower": 0.0,
            "non_loss_day_bootstrap_lower": 0.0,
            "non_loss_day_bootstrap_upper": 0.0,
            "median_net_bps": None,
            "mean_net_bps": None,
            "trading_days": 0,
            "tickers": 0,
            "maximum_instrument_share": 1.0,
        }
    target_hits = sum(item.target_hit for item in simulations)
    non_losses = sum(item.non_loss for item in simulations)
    net = [float(item.net_result_bps) for item in simulations]
    ticker_counts = Counter(item.snapshot.ticker for item in selected_examples)
    target_interval = _clustered_day_bootstrap_interval(
        selected_examples,
        [float(item.target_hit) for item in simulations],
    )
    non_loss_interval = _clustered_day_bootstrap_interval(
        selected_examples,
        [float(item.non_loss) for item in simulations],
    )
    return {
        "trades": count,
        "target_hits": target_hits,
        "target_hit_rate": target_hits / count,
        "target_wilson_lower": _wilson_lower(target_hits, count),
        "target_day_bootstrap_lower": target_interval[0],
        "target_day_bootstrap_upper": target_interval[1],
        "non_loss_count": non_losses,
        "non_loss_rate": non_losses / count,
        "non_loss_wilson_lower": _wilson_lower(non_losses, count),
        "non_loss_day_bootstrap_lower": non_loss_interval[0],
        "non_loss_day_bootstrap_upper": non_loss_interval[1],
        "median_net_bps": statistics.median(net),
        "mean_net_bps": statistics.fmean(net),
        "trading_days": len({item.trading_day for item in selected_examples}),
        "tickers": len(ticker_counts),
        "maximum_instrument_share": max(ticker_counts.values()) / count,
        "exit_target": sum(
            item.exit_reason.value == "target" for item in simulations
        ),
        "exit_break_even": sum(
            item.exit_reason.value == "break_even" for item in simulations
        ),
        "exit_initial_stop": sum(
            item.exit_reason.value == "initial_stop" for item in simulations
        ),
        "exit_deadline": sum(
            item.exit_reason.value == "deadline" for item in simulations
        ),
    }


def _matched_control_analysis(
    *,
    examples: Sequence[MorningRetracementExample],
    probabilities: Sequence[float],
    threshold: float,
    policy: TradePolicy,
    controls_per_event: int = 5,
) -> dict[str, Any]:
    """Compare selected events with causal, similarly situated deviations."""

    selected_pairs = _select_first_episode_signal(
        examples,
        probabilities,
        threshold,
        policy.target_fraction,
        policy.round_trip_cost_bps,
    )
    selected_ids = {item.episode_id for item, _ in selected_pairs}
    pool = [
        item
        for item, probability in zip(examples, probabilities)
        if item.episode_id not in selected_ids
        and probability < threshold
        and _local_minute(item.snapshot.observed_at) < policy.deadline_local_minute
        and item.snapshot.excursion_bps >= 4.0 * policy.round_trip_cost_bps
        and _entry_still_valid(
            item,
            policy.target_fraction,
            round_trip_cost_bps=policy.round_trip_cost_bps,
        )
    ]
    used_control_episodes: set[str] = set()
    matched_selected: list[MorningRetracementExample] = []
    matched_controls: list[MorningRetracementExample] = []
    for selected, _ in sorted(
        selected_pairs,
        key=lambda pair: (
            pair[0].snapshot.observed_at,
            pair[0].snapshot.ticker,
        ),
    ):
        selected_volatility = selected.feature_values("morning").get(
            "morning_realized_volatility_bps",
            0.0,
        )
        ranked: list[tuple[float, MorningRetracementExample]] = []
        for control in pool:
            if control.episode_id in used_control_episodes:
                continue
            if control.snapshot.ticker != selected.snapshot.ticker:
                continue
            if control.snapshot.direction is not selected.snapshot.direction:
                continue
            minute_distance = abs(
                _local_minute(control.snapshot.observed_at)
                - _local_minute(selected.snapshot.observed_at)
            )
            if minute_distance > 15:
                continue
            excursion_ratio = (
                control.snapshot.excursion_bps / selected.snapshot.excursion_bps
            )
            if not 0.5 <= excursion_ratio <= 2.0:
                continue
            control_volatility = control.feature_values("morning").get(
                "morning_realized_volatility_bps",
                0.0,
            )
            volatility_ratio = (
                (control_volatility + 1.0) / (selected_volatility + 1.0)
            )
            if not 0.5 <= volatility_ratio <= 2.0:
                continue
            distance = (
                minute_distance / 15.0
                + abs(math.log(excursion_ratio))
                + abs(math.log(volatility_ratio))
            )
            ranked.append((distance, control))
        chosen: list[MorningRetracementExample] = []
        chosen_episodes: set[str] = set()
        for _, control in sorted(
            ranked,
            key=lambda pair: (
                pair[0],
                pair[1].trading_day,
                pair[1].episode_id,
            ),
        ):
            if control.episode_id in chosen_episodes:
                continue
            chosen.append(control)
            chosen_episodes.add(control.episode_id)
            used_control_episodes.add(control.episode_id)
            if len(chosen) >= controls_per_event:
                break
        if chosen:
            matched_selected.append(selected)
            matched_controls.extend(chosen)
    selected_metrics = _simulate_examples(matched_selected, policy)
    control_metrics = _simulate_examples(matched_controls, policy)
    selected_target_values = [
        float(item.label_for(policy.target_fraction).reached)
        for item in matched_selected
    ]
    control_target_values = [
        float(item.label_for(policy.target_fraction).reached)
        for item in matched_controls
    ]
    selected_rate = (
        statistics.fmean(selected_target_values)
        if selected_target_values
        else 0.0
    )
    control_rate = (
        statistics.fmean(control_target_values)
        if control_target_values
        else 0.0
    )
    lift_interval = _clustered_day_bootstrap_difference(
        matched_selected,
        selected_target_values,
        matched_controls,
        control_target_values,
    )
    return {
        "matching_rule": (
            "same_ticker_direction_time_15m_excursion_0.5x_2x_"
            "observed_volatility_0.5x_2x"
        ),
        "requested_controls_per_event": controls_per_event,
        "selected_events": len(selected_pairs),
        "matched_selected_events": len(matched_selected),
        "matched_controls": len(matched_controls),
        "coverage": (
            len(matched_selected) / len(selected_pairs)
            if selected_pairs
            else 0.0
        ),
        "selected_target_rate": selected_rate,
        "control_target_rate": control_rate,
        "target_rate_lift": selected_rate - control_rate,
        "target_rate_lift_day_bootstrap_lower": lift_interval[0],
        "target_rate_lift_day_bootstrap_upper": lift_interval[1],
        "selected_non_loss_rate": float(selected_metrics["non_loss_rate"]),
        "control_non_loss_rate": float(control_metrics["non_loss_rate"]),
        "selected_mean_net_bps": selected_metrics["mean_net_bps"],
        "control_mean_net_bps": control_metrics["mean_net_bps"],
    }


def _policy_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    validation_gate = (
        int(row.get("trades", 0)) >= 20
        and float(row.get("median_net_bps") or 0.0) > 0.0
    )
    return (
        int(validation_gate),
        float(row.get("non_loss_wilson_lower") or 0.0),
        float(row.get("target_wilson_lower") or 0.0),
        float(row.get("median_net_bps") or -1e9),
        int(row.get("trades", 0)),
    )


def _policy_from_row(
    row: Mapping[str, Any], *, round_trip_cost_bps: float
) -> TradePolicy:
    return TradePolicy(
        target_fraction=float(row["target_fraction"]),
        stop_extension_fraction=float(row["stop_extension_fraction"]),
        break_even_trigger_fraction=float(
            row["break_even_trigger_fraction"]
        ),
        deadline_local_minute=int(row["deadline_local_minute"]),
        round_trip_cost_bps=round_trip_cost_bps,
    )


def _refit_for_holdout(
    candidate: FittedCandidate,
    training: tuple[MorningRetracementExample, ...],
    holdout: tuple[MorningRetracementExample, ...],
) -> tuple[list[float], str]:
    from sklearn.feature_extraction import DictVectorizer  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.tree import DecisionTreeClassifier, export_text  # type: ignore

    vectorizer = DictVectorizer(sparse=True)
    train_x = _int32_sparse(
        vectorizer.fit_transform(
            [_feature_mapping(item, candidate.feature_family) for item in training]
        )
    )
    holdout_x = _int32_sparse(
        vectorizer.transform(
            [_feature_mapping(item, candidate.feature_family) for item in holdout]
        )
    )
    train_y = [
        int(item.label_for(candidate.target_fraction).reached)
        for item in training
    ]
    if candidate.model_name == "logistic_regression":
        model = LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
            random_state=20260728,
            solver="liblinear",
        )
    elif candidate.model_name == "shallow_decision_tree":
        model = DecisionTreeClassifier(
            max_depth=3,
            min_samples_leaf=max(20, len(training) // 100),
            class_weight="balanced",
            random_state=20260728,
        )
    else:
        from lightgbm import LGBMClassifier  # type: ignore

        model = LGBMClassifier(
            n_estimators=250,
            learning_rate=0.03,
            num_leaves=15,
            min_child_samples=max(20, len(training) // 100),
            subsample=0.8,
            colsample_bytree=0.8,
            class_weight="balanced",
            random_state=20260728,
            verbose=-1,
        )
    model.fit(train_x, train_y)
    probabilities = [
        float(item) for item in model.predict_proba(holdout_x)[:, 1]
    ]
    if candidate.model_name == "shallow_decision_tree":
        explanation = export_text(
            model,
            feature_names=list(vectorizer.get_feature_names_out()),
        )
    elif candidate.model_name == "logistic_regression":
        strongest = sorted(
            zip(vectorizer.get_feature_names_out(), model.coef_[0]),
            key=lambda pair: abs(pair[1]),
            reverse=True,
        )[:12]
        explanation = "\n".join(
            f"{name}: {coefficient:+.6f}"
            for name, coefficient in strongest
        )
    else:
        strongest = sorted(
            zip(vectorizer.get_feature_names_out(), model.feature_importances_),
            key=lambda pair: pair[1],
            reverse=True,
        )[:12]
        explanation = "\n".join(
            f"{name}: {int(importance)}"
            for name, importance in strongest
        )
    return probabilities, explanation


def _evaluate_locked_policy(
    *,
    examples: tuple[MorningRetracementExample, ...],
    probabilities: Sequence[float],
    threshold: float,
    policy: TradePolicy,
) -> dict[str, Any]:
    selected = _select_first_episode_signal(
        examples,
        probabilities,
        threshold,
        policy.target_fraction,
        policy.round_trip_cost_bps,
    )
    return {
        "probability_threshold": threshold,
        **_simulate_examples(tuple(item for item, _ in selected), policy),
    }


def _previous_session_holdout_comparison(
    *,
    selected_candidate: FittedCandidate,
    candidates: Sequence[FittedCandidate],
    training: tuple[MorningRetracementExample, ...],
    holdout: tuple[MorningRetracementExample, ...],
    selected_policy: TradePolicy,
) -> dict[str, Any]:
    if selected_candidate.feature_family == "morning":
        return {
            "status": "previous_session_not_selected",
            "passed": True,
            "selected_feature_family": selected_candidate.feature_family,
        }
    morning = [
        item
        for item in candidates
        if item.target_fraction == selected_candidate.target_fraction
        and item.feature_family == "morning"
        and item.threshold is not None
        and item.threshold_metrics is not None
    ]
    if not morning:
        return {
            "status": "morning_comparator_unavailable",
            "passed": False,
            "selected_feature_family": selected_candidate.feature_family,
        }
    same_model = [
        item for item in morning if item.model_name == selected_candidate.model_name
    ]
    comparator = max(
        same_model or morning,
        key=lambda item: (
            item.threshold_metrics["target_wilson_lower"],
            item.threshold_metrics["selected_events"],
        ),
    )
    probabilities, _ = _refit_for_holdout(comparator, training, holdout)
    assert comparator.threshold is not None
    metrics = _evaluate_locked_policy(
        examples=holdout,
        probabilities=probabilities,
        threshold=comparator.threshold,
        policy=selected_policy,
    )
    # The previous-session feature family is retained only when the final
    # sample improves target precision without reducing the conservative
    # non-loss lower bound.
    selected_probabilities, _ = _refit_for_holdout(
        selected_candidate, training, holdout
    )
    assert selected_candidate.threshold is not None
    selected_metrics = _evaluate_locked_policy(
        examples=holdout,
        probabilities=selected_probabilities,
        threshold=selected_candidate.threshold,
        policy=selected_policy,
    )
    passed = (
        float(selected_metrics["target_hit_rate"])
        > float(metrics["target_hit_rate"])
        and float(selected_metrics["non_loss_wilson_lower"])
        >= float(metrics["non_loss_wilson_lower"])
    )
    return {
        "status": "compared_on_holdout",
        "passed": passed,
        "selected_feature_family": selected_candidate.feature_family,
        "selected": selected_metrics,
        "morning_only": {
            "model_name": comparator.model_name,
            "probability_threshold": comparator.threshold,
            **metrics,
        },
    }


def _product_gates(
    holdout: Mapping[str, Any],
    stress: Mapping[str, Any],
    *,
    matched_control_lift_lower: float = 1.0,
    previous_session_incremental_value: bool = True,
    independent_holdout: bool = True,
) -> list[dict[str, Any]]:
    definitions = (
        ("minimum_episodes_300", int(holdout.get("trades", 0)) >= 300),
        ("minimum_trading_days_30", int(holdout.get("trading_days", 0)) >= 30),
        ("target_rate_90pct", float(holdout.get("target_hit_rate", 0.0)) >= 0.90),
        (
            "target_wilson_lower_80pct",
            float(
                holdout.get(
                    "target_day_bootstrap_lower",
                    holdout.get("target_wilson_lower", 0.0),
                )
            )
            >= 0.80,
        ),
        ("non_loss_rate_95pct", float(holdout.get("non_loss_rate", 0.0)) >= 0.95),
        (
            "non_loss_wilson_lower_90pct",
            float(
                holdout.get(
                    "non_loss_day_bootstrap_lower",
                    holdout.get("non_loss_wilson_lower", 0.0),
                )
            )
            >= 0.90,
        ),
        ("positive_median_net", float(holdout.get("median_net_bps") or 0.0) > 0.0),
        ("multiple_instruments", int(holdout.get("tickers", 0)) >= 3),
        (
            "instrument_share_at_most_50pct",
            float(holdout.get("maximum_instrument_share", 1.0)) <= 0.50,
        ),
        (
            "doubled_slippage_positive_median",
            float(stress.get("median_net_bps") or 0.0) > 0.0,
        ),
        (
            "matched_control_target_lift_lower_above_zero",
            matched_control_lift_lower > 0.0,
        ),
        (
            "previous_session_incremental_value_when_used",
            previous_session_incremental_value,
        ),
        ("independent_holdout_not_previously_opened", independent_holdout),
    )
    return [{"gate": name, "passed": passed} for name, passed in definitions]


def _expected_hit_window(
    validation: tuple[MorningRetracementExample, ...],
    probabilities: Sequence[float],
    threshold: float,
    target_fraction: float,
) -> dict[str, float | None]:
    selected = _select_first_episode_signal(
        validation,
        probabilities,
        threshold,
        target_fraction,
        10.0,
    )
    values = sorted(
        float(label.minutes_to_target)
        for item, _ in selected
        if (
            label := item.label_for(target_fraction)
        ).minutes_to_target
        is not None
    )
    if not values:
        return {"p25_minutes": None, "median_minutes": None, "p75_minutes": None}
    return {
        "p25_minutes": _percentile(values, 0.25),
        "median_minutes": _percentile(values, 0.50),
        "p75_minutes": _percentile(values, 0.75),
    }


def _dataset_row(
    item: MorningRetracementExample,
    policy: MorningRetracementResearchPolicy,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "row_id": item.row_id,
        "episode_id": item.episode_id,
        "ticker": item.snapshot.ticker,
        "trading_day": item.trading_day.isoformat(),
        "observed_at": item.snapshot.observed_at.isoformat(),
        "feature_cutoff_at": item.feature_cutoff_at.isoformat(),
        "previous_close": item.snapshot.previous_close,
        "current_price": item.snapshot.current_price,
        "running_extreme": item.snapshot.running_extreme,
        "extreme_at": item.snapshot.extreme_at.isoformat(),
        "expected_direction": int(item.snapshot.direction),
        "excursion_bps": item.snapshot.excursion_bps,
        "tradable_excursion": (
            item.snapshot.excursion_bps >= policy.tradable_excursion_bps
        ),
        "maximum_retracement_fraction": item.maximum_retracement_fraction,
        "maximum_adverse_extension_fraction": (
            item.maximum_adverse_extension_fraction
        ),
        "label_available": item.label_available,
    }
    for feature in item.features:
        row[feature.name] = feature.value
    for label in item.labels:
        suffix = int(label.fraction * 100)
        row[f"target_r{suffix}_price"] = label.target_price
        row[f"target_r{suffix}_reached"] = label.reached
        row[f"target_r{suffix}_first_at"] = (
            label.first_reached_at.isoformat()
            if label.first_reached_at
            else None
        )
        row[f"target_r{suffix}_minutes"] = label.minutes_to_target
    return row


def _run_id(
    *,
    candles: Sequence[HistoricalCandle],
    policy: MorningRetracementResearchPolicy,
    tickers: tuple[str, ...] | None,
    previous_signal_count: int,
    independent_holdout: bool,
) -> str:
    payload = {
        "version": policy.version,
        "rows": len(candles),
        "first": min(item.at for item in candles).isoformat(),
        "last": max(item.at for item in candles).isoformat(),
        "tickers": tickers,
        "previous_signal_count": previous_signal_count,
        "independent_holdout": independent_holdout,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()[:16]


def _fingerprint_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
                separators=(",", ":"),
            ).encode()
        )
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def _int32_sparse(matrix: Any) -> Any:
    """Map scipy sparse indices to sklearn's portable 32-bit contract."""

    if hasattr(matrix, "indices") and hasattr(matrix, "indptr"):
        matrix.indices = matrix.indices.astype("int32", copy=False)
        matrix.indptr = matrix.indptr.astype("int32", copy=False)
    return matrix


def _wilson_lower(successes: int, count: int, z: float = 1.95996398454) -> float:
    if count <= 0:
        return 0.0
    rate = successes / count
    denominator = 1.0 + z * z / count
    centre = rate + z * z / (2.0 * count)
    margin = z * math.sqrt(
        (rate * (1.0 - rate) + z * z / (4.0 * count)) / count
    )
    return (centre - margin) / denominator


def _clustered_day_bootstrap_interval(
    examples: Sequence[MorningRetracementExample],
    values: Sequence[float],
    *,
    repetitions: int = 2000,
    seed: int = 20260728,
) -> tuple[float, float]:
    if len(examples) != len(values):
        raise ValueError("cluster bootstrap inputs must have equal length")
    grouped: defaultdict[Any, list[float]] = defaultdict(list)
    for example, value in zip(examples, values):
        grouped[example.trading_day].append(float(value))
    days = sorted(grouped)
    if not days:
        return (0.0, 0.0)
    if len(days) == 1:
        point = statistics.fmean(grouped[days[0]])
        return (point, point)
    rng = random.Random(seed)
    estimates: list[float] = []
    for _ in range(repetitions):
        sampled: list[float] = []
        for _ in days:
            sampled.extend(grouped[rng.choice(days)])
        estimates.append(statistics.fmean(sampled))
    estimates.sort()
    return (
        _percentile(estimates, 0.025),
        _percentile(estimates, 0.975),
    )


def _clustered_day_bootstrap_difference(
    selected_examples: Sequence[MorningRetracementExample],
    selected_values: Sequence[float],
    control_examples: Sequence[MorningRetracementExample],
    control_values: Sequence[float],
    *,
    repetitions: int = 2000,
    seed: int = 20260728,
) -> tuple[float, float]:
    if len(selected_examples) != len(selected_values):
        raise ValueError("selected cluster inputs must have equal length")
    if len(control_examples) != len(control_values):
        raise ValueError("control cluster inputs must have equal length")
    selected_grouped: defaultdict[Any, list[float]] = defaultdict(list)
    control_grouped: defaultdict[Any, list[float]] = defaultdict(list)
    for example, value in zip(selected_examples, selected_values):
        selected_grouped[example.trading_day].append(float(value))
    for example, value in zip(control_examples, control_values):
        control_grouped[example.trading_day].append(float(value))
    selected_days = sorted(selected_grouped)
    control_days = sorted(control_grouped)
    if not selected_days or not control_days:
        return (0.0, 0.0)
    rng = random.Random(seed)
    differences: list[float] = []
    for _ in range(repetitions):
        selected_sample: list[float] = []
        control_sample: list[float] = []
        for _ in selected_days:
            selected_sample.extend(selected_grouped[rng.choice(selected_days)])
        for _ in control_days:
            control_sample.extend(control_grouped[rng.choice(control_days)])
        differences.append(
            statistics.fmean(selected_sample) - statistics.fmean(control_sample)
        )
    differences.sort()
    return (
        _percentile(differences, 0.025),
        _percentile(differences, 0.975),
    )


def _percentile(values: Sequence[float], quantile: float) -> float:
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight


def _local_minute(at: datetime) -> int:
    local = at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _render_report(
    *,
    manifest: Mapping[str, Any],
    leaderboard: Sequence[Mapping[str, Any]],
    selected: Mapping[str, Any],
) -> str:
    holdout = selected["holdout"]
    validation = selected["validation"]
    gates = selected["gates"]
    failed = [item["gate"] for item in gates if not item["passed"]]
    previous_comparison = selected["previous_session_holdout_comparison"]
    family_labels = {
        "morning": "только текущее утро",
        "previous_session": "только предыдущая сессия",
        "combined": "текущее утро и предыдущая сессия",
    }
    model_labels = {
        "logistic_regression": "логистическая модель",
        "shallow_decision_tree": "неглубокое дерево решений",
        "lightgbm": "градиентное усиление деревьев",
    }
    gate_labels = {
        "minimum_episodes_300": "не менее 300 независимых эпизодов",
        "minimum_trading_days_30": "не менее 30 торговых дней",
        "target_rate_90pct": "вероятность достижения цели не ниже 90%",
        "target_wilson_lower_80pct": (
            "нижняя доверительная граница вероятности цели не ниже 80%"
        ),
        "non_loss_rate_95pct": (
            "вероятность результата без чистого убытка не ниже 95%"
        ),
        "non_loss_wilson_lower_90pct": (
            "нижняя доверительная граница отсутствия убытка не ниже 90%"
        ),
        "positive_median_net": (
            "положительная медиана результата после издержек"
        ),
        "doubled_slippage_positive_median": (
            "положительная медиана при удвоенном проскальзывании"
        ),
        "multiple_instruments": "результат устойчив на нескольких инструментах",
        "instrument_share_at_most_50pct": (
            "ни один инструмент не составляет больше половины выборки"
        ),
        "matched_control_target_lift_lower_above_zero": (
            "нижняя доверительная граница преимущества над контрольными "
            "отклонениями выше нуля"
        ),
        "previous_session_incremental_value_when_used": (
            "предыдущая сессия даёт дополнительную прогнозную ценность"
        ),
        "independent_holdout_not_previously_opened": (
            "итоговый период ранее не использовался при изменении правила"
        ),
    }
    comparison = [
        item
        for item in leaderboard
        if item.get("status") == "ok"
        and float(item["target_fraction"]) == float(selected["target_fraction"])
    ]
    comparison.sort(
        key=lambda item: float(item.get("target_wilson_lower") or 0.0),
        reverse=True,
    )
    lines = [
        "# Исследование утреннего возврата",
        "",
        "## Вывод",
        "",
        (
            "Кандидат **не разрешён для продуктового утверждения**."
            if failed
            else "Кандидат прошёл формальные условия итоговой проверки."
        ),
        "",
        f"- Строк снимков: {manifest['rows']}.",
        f"- Независимых эпизодов: {manifest['episodes']}.",
        f"- Торговых дней: {manifest['trading_days']}.",
        f"- Утечек будущих признаков: {manifest['feature_leakage_rows']}.",
        f"- Выбранный уровень возврата: {float(selected['target_fraction']):.0%}.",
        (
            "- Источник условий: "
            f"{family_labels.get(str(selected['feature_family']), selected['feature_family'])}."
        ),
        (
            "- Модель: "
            f"{model_labels.get(str(selected['model_name']), selected['model_name'])}."
        ),
        f"- Порог вероятности: {float(selected['probability_threshold']):.0%}.",
        "",
        "## Проверочная и итоговая выборки",
        "",
        (
            "- На проверочной части: "
            f"{int(validation['trades'])} сделок, цель "
            f"{float(validation['target_hit_rate']):.1%}, без чистого убытка "
            f"{float(validation['non_loss_rate']):.1%}, медиана "
            f"{float(validation['median_net_bps']):.2f} б. п."
        ),
        (
            "- На закрытой итоговой части: "
            f"{int(holdout['trades'])} сделок, цель "
            f"{float(holdout['target_hit_rate']):.1%}, нижняя граница "
            "с группировкой по торговым дням "
            f"{float(holdout['target_day_bootstrap_lower']):.1%}; "
            "без чистого убытка "
            f"{float(holdout['non_loss_rate']):.1%}, нижняя граница "
            "с группировкой по торговым дням "
            f"{float(holdout['non_loss_day_bootstrap_lower']):.1%}; медиана "
            f"{float(holdout['median_net_bps'] or 0.0):.2f} б. п."
        ),
        "",
        "## Роль предыдущей сессии",
        "",
        (
            "Модели ниже сравнивались на одинаковых торговых днях. Признаки "
            "предыдущей сессии считаются полезными только если совместная модель "
            "устойчиво превосходит утреннюю на итоговой проверке."
        ),
        "",
    ]
    for item in comparison[:8]:
        lines.append(
            "- "
            f"{family_labels.get(str(item['feature_family']), item['feature_family'])}, "
            f"{model_labels.get(str(item['model_name']), item['model_name'])}: "
            "выбранных событий "
            f"{int(item.get('selected_events') or 0)}, точность цели "
            f"{float(item.get('target_hit_rate') or 0.0):.1%}, нижняя граница "
            f"{float(item.get('target_wilson_lower') or 0.0):.1%}."
        )
    if previous_comparison["status"] == "compared_on_holdout":
        selected_previous = previous_comparison["selected"]
        morning_only = previous_comparison["morning_only"]
        lines.extend(
            [
                "",
                (
                    "- Итоговая проверка выбранных признаков: цель "
                    f"{float(selected_previous['target_hit_rate']):.1%}, "
                    "нижняя граница отсутствия убытка "
                    f"{float(selected_previous['non_loss_wilson_lower']):.1%}."
                ),
                (
                    "- Та же схема только с утренними признаками: цель "
                    f"{float(morning_only['target_hit_rate']):.1%}, "
                    "нижняя граница отсутствия убытка "
                    f"{float(morning_only['non_loss_wilson_lower']):.1%}."
                ),
                (
                    "- Дополнительная ценность предыдущей сессии: "
                    + (
                        "подтверждена."
                        if previous_comparison["passed"]
                        else "не подтверждена; такие признаки нельзя переносить в рабочую модель."
                    )
                ),
            ]
        )
    matched = selected["matched_controls"]
    lines.extend(
        [
            "",
            "## Сравнение с контрольными отклонениями",
            "",
            (
                "Для каждого выбранного события искались отклонения другого дня "
                "того же инструмента и направления, близкого времени, размера "
                "отклонения и наблюдаемой волатильности."
            ),
            "",
            (
                f"- Сопоставлено {int(matched['matched_selected_events'])} из "
                f"{int(matched['selected_events'])} выбранных событий; "
                f"контрольных событий: {int(matched['matched_controls'])}."
            ),
            (
                f"- Достижение цели: выбранные события "
                f"{float(matched['selected_target_rate']):.1%}, контрольные "
                f"{float(matched['control_target_rate']):.1%}, разница "
                f"{float(matched['target_rate_lift']):+.1%}; доверительный "
                "промежуток с группировкой по дням "
                f"от {float(matched['target_rate_lift_day_bootstrap_lower']):+.1%} "
                f"до {float(matched['target_rate_lift_day_bootstrap_upper']):+.1%}."
            ),
            (
                f"- Результат без чистого убытка: выбранные события "
                f"{float(matched['selected_non_loss_rate']):.1%}, контрольные "
                f"{float(matched['control_non_loss_rate']):.1%}."
            ),
            (
                f"- Средний результат после издержек: выбранные события "
                f"{float(matched['selected_mean_net_bps'] or 0.0):+.2f} б. п., "
                f"контрольные {float(matched['control_mean_net_bps'] or 0.0):+.2f} б. п."
            ),
        ]
    )
    lines.extend(
        [
            "",
            "## Условия сделки",
            "",
            f"- Ограничение убытка: {float(selected['stop_extension_fraction']):.0%} исходного отклонения за экстремумом.",
            f"- Защита безубытка: после {float(selected['break_even_trigger_fraction']):.0%} движения в пользу позиции.",
            f"- Крайнее время выхода: {int(selected['deadline_local_minute']) // 60:02d}:{int(selected['deadline_local_minute']) % 60:02d}.",
            "",
            "## Наиболее значимые условия модели",
            "",
        ]
    )
    explanation_lines = [
        item
        for item in str(selected.get("model_explanation") or "").splitlines()
        if item.strip()
    ]
    lines.extend(
        [
            f"- {_translate_feature_line(item)}"
            for item in explanation_lines[:12]
        ]
        or ["- Объяснение модели недоступно."]
    )
    lines.extend(
        [
            "",
            "## Непройденные условия",
            "",
        ]
    )
    lines.extend(
        [f"- {gate_labels.get(str(item), str(item))}." for item in failed]
        or ["- Все формальные условия пройдены."]
    )
    lines.extend(
        [
            "",
            "Расчётный безубыток не является гарантией исполнения: разрыв цены "
            "и недостаток ликвидности могут привести к худшему результату.",
            "",
        ]
    )
    return "\n".join(lines)


def _translate_feature_line(line: str) -> str:
    name, separator, value = line.partition(":")
    labels = {
        "current_retracement_fraction": "уже пройденная доля возврата",
        "decision_local_minute": "время принятия решения",
        "excursion_bps": "размер утреннего отклонения",
        "minutes_since_extreme": "минуты после последнего экстремума",
        "morning_return_5m_bps": "движение за последние 5 минут",
        "morning_return_15m_bps": "движение за последние 15 минут",
        "morning_acceleration_bps": "ускорение утреннего движения",
        "morning_realized_volatility_bps": "утренняя фактическая волатильность",
        "morning_range_bps": "утренний ценовой диапазон",
        "morning_close_to_vwap_bps": "отклонение от средневзвешенной цены",
        "morning_log_cumulative_volume": "накопленный утренний объём",
        "morning_directional_streak": "серия однонаправленных свечей",
        "prior_return_day_bps": "движение предыдущей сессии",
        "prior_return_15m_bps": "последние 15 минут предыдущей сессии",
        "prior_return_30m_bps": "последние 30 минут предыдущей сессии",
        "prior_return_60m_bps": "последние 60 минут предыдущей сессии",
        "prior_close_position": "положение вчерашнего закрытия в диапазоне",
        "prior_close_to_vwap_bps": "вчерашнее закрытие относительно средневзвешенной цены",
        "prior_range_bps": "диапазон предыдущей сессии",
        "prior_realized_volatility_bps": "волатильность предыдущей сессии",
        "prior_log_total_volume": "объём предыдущей сессии",
        "prior_closing_volume_ratio": "ускорение объёма перед закрытием",
        "morning_continues_prior_direction": "утро продолжает вчерашнее направление",
        "morning_opposes_prior_direction": "утро направлено против вчерашнего движения",
        "expected_direction": "направление предполагаемого возврата",
        "tradable_excursion": "достаточность полного отклонения для издержек",
    }
    if name.startswith("ticker="):
        label = f"инструмент {name.removeprefix('ticker=')}"
    else:
        label = labels.get(name, name.replace("_", " "))
    return f"{label}:{value}" if separator else label


if __name__ == "__main__":
    raise SystemExit(main())
