"""Use cases for selecting, then independently checking, a meta-policy.

The meta-policy never changes the preregistered hypothesis direction.  It may
only publish the sealed rule or abstain.  Model and threshold selection uses
the validation partition before the holdout is inspected.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import json
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.hypothesis_evidence import (
    DatasetPartition,
    day_block_bootstrap_interval,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.selective_hypothesis_policy import (
    CalibrationBin,
    SelectiveExample,
    SelectiveMetrics,
    SelectiveModelKind,
    SelectivePolicyResult,
    SelectiveResearchDecision,
    SelectiveResearchPolicy,
    TuneCandidate,
)


FeatureRow = tuple[float, ...]


class FittedProbabilityEstimator(Protocol):
    def predict_probabilities(self, rows: Sequence[FeatureRow]) -> tuple[float, ...]: ...


class ProbabilityEstimatorFactory(Protocol):
    def available_model_kinds(self) -> tuple[SelectiveModelKind, ...]: ...

    def fit(
        self,
        *,
        model_kind: SelectiveModelKind,
        feature_names: tuple[str, ...],
        rows: Sequence[FeatureRow],
        labels: Sequence[int],
        seed: int,
    ) -> FittedProbabilityEstimator | None: ...


class SelectiveResearchArtifactPort(Protocol):
    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None: ...

    def persist(self, result: "SelectivePortfolioResult") -> str: ...


@dataclass(frozen=True)
class SelectivePortfolioResult:
    run_id: str
    input_fingerprint: str
    policy_fingerprint: str
    results: tuple[SelectivePolicyResult, ...]
    examples: int


@dataclass(frozen=True)
class SelectivePortfolioExecution:
    run_id: str
    reused: bool
    artifact_uri: str
    result: SelectivePortfolioResult | None


class ResearchSelectiveHypothesisPolicy:
    """Select on validation and open the holdout only for the final gate."""

    def __init__(
        self,
        *,
        estimator_factory: ProbabilityEstimatorFactory,
        policy: SelectiveResearchPolicy = SelectiveResearchPolicy(),
    ) -> None:
        self._estimators = estimator_factory
        self._policy = policy

    def execute(self, examples: Sequence[SelectiveExample]) -> SelectivePolicyResult:
        ordered = tuple(
            sorted(examples, key=lambda item: (item.trading_day, item.observed_at, item.observation_id))
        )
        identity, feature_names, cost_model = _validate_study(ordered)
        split = chronological_split_60_20_20(tuple(item.trading_day for item in ordered))
        partitions = {
            partition: tuple(
                item
                for item in ordered
                if split.partition_for(item.trading_day) is partition
            )
            for partition in DatasetPartition
        }
        train = partitions[DatasetPartition.TRAIN]
        tune = partitions[DatasetPartition.VALIDATION]
        holdout = partitions[DatasetPartition.HOLDOUT]
        train_rate = sum(item.useful for item in train) / len(train) if train else 0.5
        tune_rule = _metrics(
            tune,
            tuple(train_rate for _ in tune),
            threshold=0.0,
            calibration_bins=self._policy.calibration_bins,
        )
        holdout_rule = _metrics(
            holdout,
            tuple(train_rate for _ in holdout),
            threshold=0.0,
            calibration_bins=self._policy.calibration_bins,
        )
        rule_candidate = TuneCandidate(
            model_kind=SelectiveModelKind.SEALED_RULE,
            probability_threshold=0.0,
            metrics=tune_rule,
            lift_over_sealed_rule_bps=0.0,
            eligible=True,
            reason_codes=(),
        )
        minimum_reasons = _minimum_sample_reasons(train, tune, holdout, self._policy)
        if minimum_reasons:
            return SelectivePolicyResult(
                hypothesis_id=identity[0],
                hypothesis_version=identity[1],
                horizon_seconds=identity[2],
                feature_names=feature_names,
                cost_model_version=cost_model,
                train_examples=len(train),
                tune_examples=len(tune),
                holdout_examples=len(holdout),
                tune_candidates=(rule_candidate,),
                tune_selected_model=SelectiveModelKind.SEALED_RULE,
                tune_selected_threshold=0.0,
                holdout_rule_metrics=holdout_rule,
                holdout_selected_metrics=holdout_rule,
                holdout_lift_over_rule_bps=0.0,
                holdout_lift_interval=None,
                decision=SelectiveResearchDecision.INSUFFICIENT_DATA,
                deployment_model=SelectiveModelKind.SEALED_RULE,
                claim_allowed=False,
                hypothesis_changed=False,
                reason_codes=minimum_reasons,
            )

        train_rows = _matrix(train, feature_names)
        train_labels = tuple(int(item.useful) for item in train)
        tune_rows = _matrix(tune, feature_names)
        candidates = [rule_candidate]
        fitted: dict[SelectiveModelKind, FittedProbabilityEstimator] = {}
        tune_predictions: dict[SelectiveModelKind, tuple[float, ...]] = {}
        for model_kind in self._estimators.available_model_kinds():
            if model_kind is SelectiveModelKind.SEALED_RULE:
                continue
            estimator = self._estimators.fit(
                model_kind=model_kind,
                feature_names=feature_names,
                rows=train_rows,
                labels=train_labels,
                seed=_study_seed(identity, self._policy.bootstrap_seed),
            )
            if estimator is None:
                continue
            probabilities = _checked_probabilities(
                estimator.predict_probabilities(tune_rows), len(tune)
            )
            fitted[model_kind] = estimator
            tune_predictions[model_kind] = probabilities
            for threshold in self._policy.probability_thresholds:
                metrics = _metrics(
                    tune,
                    probabilities,
                    threshold=threshold,
                    calibration_bins=self._policy.calibration_bins,
                )
                lift = _mean_lift(metrics, tune_rule)
                reasons = _candidate_reasons(metrics, lift, tune_rule, self._policy)
                candidates.append(
                    TuneCandidate(
                        model_kind=model_kind,
                        probability_threshold=threshold,
                        metrics=metrics,
                        lift_over_sealed_rule_bps=lift,
                        eligible=not reasons,
                        reason_codes=reasons,
                    )
                )

        eligible_models = [
            item
            for item in candidates
            if item.model_kind is not SelectiveModelKind.SEALED_RULE and item.eligible
        ]
        selected = (
            sorted(
                eligible_models,
                key=lambda item: (
                    -(item.lift_over_sealed_rule_bps or 0.0),
                    item.metrics.brier_score,
                    -item.metrics.coverage,
                    item.model_kind.value,
                    item.probability_threshold,
                ),
            )[0]
            if eligible_models
            else rule_candidate
        )
        if selected.model_kind is SelectiveModelKind.SEALED_RULE:
            return SelectivePolicyResult(
                hypothesis_id=identity[0],
                hypothesis_version=identity[1],
                horizon_seconds=identity[2],
                feature_names=feature_names,
                cost_model_version=cost_model,
                train_examples=len(train),
                tune_examples=len(tune),
                holdout_examples=len(holdout),
                tune_candidates=tuple(candidates),
                tune_selected_model=selected.model_kind,
                tune_selected_threshold=selected.probability_threshold,
                holdout_rule_metrics=holdout_rule,
                holdout_selected_metrics=holdout_rule,
                holdout_lift_over_rule_bps=0.0,
                holdout_lift_interval=None,
                decision=SelectiveResearchDecision.NO_IMPROVEMENT,
                deployment_model=SelectiveModelKind.SEALED_RULE,
                claim_allowed=False,
                hypothesis_changed=False,
                reason_codes=("no_tune_model_improved_sealed_rule",),
            )

        holdout_probabilities = _checked_probabilities(
            fitted[selected.model_kind].predict_probabilities(_matrix(holdout, feature_names)),
            len(holdout),
        )
        holdout_selected = _metrics(
            holdout,
            holdout_probabilities,
            threshold=selected.probability_threshold,
            calibration_bins=self._policy.calibration_bins,
        )
        holdout_lift = _mean_lift(holdout_selected, holdout_rule)
        interval = _selected_day_lift_interval(
            holdout,
            holdout_probabilities,
            selected.probability_threshold,
            samples=self._policy.bootstrap_samples,
            seed=_study_seed(identity, self._policy.bootstrap_seed + 1),
        )
        reasons = list(
            _candidate_reasons(
                holdout_selected,
                holdout_lift,
                holdout_rule,
                self._policy,
            )
        )
        if interval is None or interval.lower <= 0.0:
            reasons.append("holdout_lift_lower_bound_not_positive")
        improved = not reasons
        return SelectivePolicyResult(
            hypothesis_id=identity[0],
            hypothesis_version=identity[1],
            horizon_seconds=identity[2],
            feature_names=feature_names,
            cost_model_version=cost_model,
            train_examples=len(train),
            tune_examples=len(tune),
            holdout_examples=len(holdout),
            tune_candidates=tuple(candidates),
            tune_selected_model=selected.model_kind,
            tune_selected_threshold=selected.probability_threshold,
            holdout_rule_metrics=holdout_rule,
            holdout_selected_metrics=holdout_selected,
            holdout_lift_over_rule_bps=holdout_lift,
            holdout_lift_interval=interval,
            decision=(
                SelectiveResearchDecision.IMPROVED
                if improved
                else SelectiveResearchDecision.NO_IMPROVEMENT
            ),
            deployment_model=(
                selected.model_kind if improved else SelectiveModelKind.SEALED_RULE
            ),
            claim_allowed=improved,
            hypothesis_changed=False,
            reason_codes=tuple(dict.fromkeys(reasons)),
        )


def _validate_study(
    examples: Sequence[SelectiveExample],
) -> tuple[tuple[str, str, int], tuple[str, ...], str]:
    if not examples:
        raise ValueError("selective research requires examples")
    identities = {
        (item.hypothesis_id, item.hypothesis_version, item.horizon_seconds)
        for item in examples
    }
    if len(identities) != 1:
        raise ValueError("one selective study must contain one hypothesis version and horizon")
    observation_ids = tuple(item.observation_id for item in examples)
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError("selective observation ids must be unique")
    schemas = {tuple(name for name, _ in item.feature_values) for item in examples}
    if len(schemas) != 1:
        raise ValueError("all selective examples must share one feature schema")
    cost_models = {item.cost_model_version for item in examples}
    if len(cost_models) != 1:
        raise ValueError("one selective study must use one cost model version")
    return next(iter(identities)), next(iter(schemas)), next(iter(cost_models))


def _minimum_sample_reasons(
    train: Sequence[SelectiveExample],
    tune: Sequence[SelectiveExample],
    holdout: Sequence[SelectiveExample],
    policy: SelectiveResearchPolicy,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if len(train) < policy.minimum_train_examples:
        reasons.append("minimum_train_examples_not_met")
    if len(tune) < policy.minimum_tune_examples:
        reasons.append("minimum_tune_examples_not_met")
    if len(holdout) < policy.minimum_holdout_examples:
        reasons.append("minimum_holdout_examples_not_met")
    if len({item.trading_day for item in holdout}) < 5:
        reasons.append("minimum_holdout_days_not_met")
    if len({item.useful for item in train}) < 2:
        reasons.append("train_requires_both_labels")
    return tuple(reasons)


def _matrix(
    examples: Sequence[SelectiveExample], feature_names: tuple[str, ...]
) -> tuple[FeatureRow, ...]:
    return tuple(
        tuple(dict(item.feature_values)[name] for name in feature_names) for item in examples
    )


def _checked_probabilities(
    probabilities: Sequence[float], expected: int
) -> tuple[float, ...]:
    checked = tuple(float(value) for value in probabilities)
    if len(checked) != expected:
        raise ValueError("estimator returned the wrong probability count")
    if any(value < 0.0 or value > 1.0 for value in checked):
        raise ValueError("estimator probability must be between zero and one")
    return checked


def _metrics(
    examples: Sequence[SelectiveExample],
    probabilities: Sequence[float],
    *,
    threshold: float,
    calibration_bins: int,
) -> SelectiveMetrics:
    probabilities = _checked_probabilities(probabilities, len(examples))
    if not examples:
        return SelectiveMetrics(
            observations=0,
            acted_observations=0,
            coverage=0.0,
            abstention_rate=1.0,
            useful_rate_when_acted=None,
            mean_cost_adjusted_result_bps=None,
            brier_score=0.0,
            expected_calibration_error=0.0,
            calibration=(),
        )
    labels = tuple(float(item.useful) for item in examples)
    acted = tuple(
        item
        for item, probability in zip(examples, probabilities, strict=True)
        if probability >= threshold
    )
    coverage = len(acted) / len(examples)
    calibration = _calibration(probabilities, labels, calibration_bins)
    expected_error = sum(
        item.observations
        / len(examples)
        * abs((item.mean_probability or 0.0) - (item.observed_useful_rate or 0.0))
        for item in calibration
    )
    return SelectiveMetrics(
        observations=len(examples),
        acted_observations=len(acted),
        coverage=coverage,
        abstention_rate=1.0 - coverage,
        useful_rate_when_acted=(
            sum(item.useful for item in acted) / len(acted) if acted else None
        ),
        mean_cost_adjusted_result_bps=(
            sum(item.cost_adjusted_result_bps for item in acted) / len(acted)
            if acted
            else None
        ),
        brier_score=sum(
            (probability - label) ** 2
            for probability, label in zip(probabilities, labels, strict=True)
        )
        / len(examples),
        expected_calibration_error=expected_error,
        calibration=calibration,
    )


def _calibration(
    probabilities: Sequence[float], labels: Sequence[float], bins: int
) -> tuple[CalibrationBin, ...]:
    grouped: list[list[tuple[float, float]]] = [[] for _ in range(bins)]
    for probability, label in zip(probabilities, labels, strict=True):
        index = min(int(probability * bins), bins - 1)
        grouped[index].append((probability, label))
    result: list[CalibrationBin] = []
    for index, values in enumerate(grouped):
        lower = index / bins
        upper = (index + 1) / bins
        result.append(
            CalibrationBin(
                lower_probability=lower,
                upper_probability=upper,
                observations=len(values),
                mean_probability=(
                    sum(item[0] for item in values) / len(values) if values else None
                ),
                observed_useful_rate=(
                    sum(item[1] for item in values) / len(values) if values else None
                ),
            )
        )
    return tuple(result)


def _mean_lift(
    selected: SelectiveMetrics, rule: SelectiveMetrics
) -> float | None:
    if (
        selected.mean_cost_adjusted_result_bps is None
        or rule.mean_cost_adjusted_result_bps is None
    ):
        return None
    return (
        selected.mean_cost_adjusted_result_bps
        - rule.mean_cost_adjusted_result_bps
    )


def _candidate_reasons(
    metrics: SelectiveMetrics,
    lift: float | None,
    rule: SelectiveMetrics,
    policy: SelectiveResearchPolicy,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if metrics.acted_observations < policy.minimum_acted_examples:
        reasons.append("minimum_acted_examples_not_met")
    if metrics.coverage < policy.minimum_coverage:
        reasons.append("minimum_coverage_not_met")
    if lift is None or lift < policy.minimum_lift_bps:
        reasons.append("minimum_cost_adjusted_lift_not_met")
    if metrics.brier_score > rule.brier_score + 1e-12:
        reasons.append("brier_score_not_improved")
    return tuple(reasons)


def _selected_day_lift_interval(
    examples: Sequence[SelectiveExample],
    probabilities: Sequence[float],
    threshold: float,
    *,
    samples: int,
    seed: int,
):
    all_by_day: dict[date, list[float]] = defaultdict(list)
    selected_by_day: dict[date, list[float]] = defaultdict(list)
    for example, probability in zip(examples, probabilities, strict=True):
        all_by_day[example.trading_day].append(example.cost_adjusted_result_bps)
        if probability >= threshold:
            selected_by_day[example.trading_day].append(example.cost_adjusted_result_bps)
    differences = {
        day: (
            sum(selected_by_day[day]) / len(selected_by_day[day])
            - sum(values) / len(values)
        ,)
        for day, values in all_by_day.items()
        if selected_by_day[day]
    }
    if len(differences) < 5:
        return None
    return day_block_bootstrap_interval(differences, samples=samples, seed=seed)


def _study_seed(identity: tuple[str, str, int], base: int) -> int:
    digest = sha256("\0".join(map(str, identity)).encode("utf-8")).hexdigest()
    return base + int(digest[:8], 16)


def selective_policy_fingerprint(policy: SelectiveResearchPolicy) -> str:
    payload = {
        name: getattr(policy, name)
        for name in policy.__dataclass_fields__
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"sha256:{sha256(encoded.encode('utf-8')).hexdigest()}"
