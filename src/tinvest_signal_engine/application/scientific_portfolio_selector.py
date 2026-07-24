"""Evaluate an explainable, abstaining portfolio of scientific hypotheses.

The use case is deliberately self-contained and deterministic.  It uses only
sealed, mature examples, tunes on later trading days, and opens the final
holdout only after model and confidence-threshold selection are frozen.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date
from hashlib import sha256
import json
from math import exp, sqrt
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.scientific_portfolio_selector import (
    PortfolioAction,
    PortfolioCalibrationBin,
    PortfolioDecision,
    PortfolioModelEvaluation,
    PortfolioModelExplanation,
    PortfolioSelectorExample,
    PortfolioSelectorMetrics,
    PortfolioSelectorModel,
    PortfolioSelectorState,
    PortfolioTemporalSplit,
    PortfolioWalkForwardFold,
    ScientificPortfolioSelectorResult,
)


_ACTIONS = tuple(PortfolioAction)
_ACTION_INDEX = {action: index for index, action in enumerate(_ACTIONS)}
_MODELS = tuple(PortfolioSelectorModel)
_EPSILON = 1e-12


@dataclass(frozen=True, slots=True)
class ScientificPortfolioSelectorPolicy:
    version: str = "scientific-portfolio-selector-v1.0.0"
    minimum_train_examples: int = 120
    minimum_validation_examples: int = 40
    minimum_holdout_examples: int = 40
    minimum_total_trading_days: int = 30
    minimum_acted_examples: int = 20
    minimum_coverage: float = 0.20
    gap_trading_days: int = 1
    confidence_thresholds: tuple[float, ...] = (0.40, 0.50, 0.60, 0.70, 0.80)
    minimum_accuracy_lift: float = 0.02
    logistic_complexity_premium: float = 0.01
    walk_forward_folds: int = 4
    minimum_positive_walk_forward_folds: int = 3
    calibration_bins: int = 5
    bayesian_prior: float = 1.0
    logistic_l2: float = 0.05
    logistic_learning_rate: float = 0.08
    logistic_iterations: int = 500

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("portfolio selector policy version is required")
        counts = (
            self.minimum_train_examples,
            self.minimum_validation_examples,
            self.minimum_holdout_examples,
            self.minimum_total_trading_days,
            self.minimum_acted_examples,
            self.gap_trading_days,
            self.walk_forward_folds,
            self.minimum_positive_walk_forward_folds,
            self.calibration_bins,
            self.logistic_iterations,
        )
        if any(item <= 0 for item in counts):
            raise ValueError("portfolio selector policy counts must be positive")
        if self.minimum_positive_walk_forward_folds > self.walk_forward_folds:
            raise ValueError("required positive folds exceed configured folds")
        if not 0.0 <= self.minimum_coverage <= 1.0:
            raise ValueError("minimum coverage must be in [0, 1]")
        if self.minimum_accuracy_lift < 0.0 or self.logistic_complexity_premium < 0.0:
            raise ValueError("model improvement gates must not be negative")
        if self.bayesian_prior <= 0.0:
            raise ValueError("Bayesian prior must be positive")
        if self.logistic_l2 < 0.0 or self.logistic_learning_rate <= 0.0:
            raise ValueError("logistic training parameters are invalid")
        if (
            not self.confidence_thresholds
            or tuple(sorted(set(self.confidence_thresholds)))
            != self.confidence_thresholds
            or any(not 0.0 <= item <= 1.0 for item in self.confidence_thresholds)
        ):
            raise ValueError("confidence thresholds must be sorted and unique")


class FittedPortfolioModel(Protocol):
    model_kind: PortfolioSelectorModel

    def probabilities(
        self, examples: Sequence[PortfolioSelectorExample]
    ) -> tuple[tuple[float, ...], ...]: ...

    def explanation(self) -> PortfolioModelExplanation: ...

    def decision_explanation(
        self, example: PortfolioSelectorExample, action: PortfolioAction
    ) -> tuple[tuple[str, float], ...]: ...


@dataclass(frozen=True, slots=True)
class _Prediction:
    example: PortfolioSelectorExample
    probabilities: tuple[float, ...]
    raw_action: PortfolioAction
    raw_confidence: float
    calibrated_confidence: float
    action: PortfolioAction


@dataclass(frozen=True, slots=True)
class _ConfidenceCalibrator:
    bins: int
    observed_accuracy: tuple[float | None, ...]

    def apply(self, confidence: float) -> float:
        index = min(int(confidence * self.bins), self.bins - 1)
        observed = self.observed_accuracy[index]
        return confidence if observed is None else observed


@dataclass(frozen=True, slots=True)
class _FixedRuleModel:
    model_kind: PortfolioSelectorModel = PortfolioSelectorModel.FIXED_RULE

    def probabilities(
        self, examples: Sequence[PortfolioSelectorExample]
    ) -> tuple[tuple[float, ...], ...]:
        return tuple(_one_hot(item.sealed_action) for item in examples)

    def explanation(self) -> PortfolioModelExplanation:
        return PortfolioModelExplanation(
            model_kind=self.model_kind,
            terms=(("all", "sealed_scientific_rule", 1.0),),
        )

    def decision_explanation(
        self, example: PortfolioSelectorExample, action: PortfolioAction
    ) -> tuple[tuple[str, float], ...]:
        del action
        return ((f"sealed_rule:{example.sealed_action.value}", 1.0),)


@dataclass(frozen=True, slots=True)
class _BayesianFrequencyModel:
    probabilities_by_key: dict[tuple[str, PortfolioAction], tuple[float, ...]]
    fallback_probabilities: tuple[float, ...]
    model_kind: PortfolioSelectorModel = PortfolioSelectorModel.BAYESIAN_FREQUENCY

    @classmethod
    def fit(
        cls,
        examples: Sequence[PortfolioSelectorExample],
        *,
        prior: float,
    ) -> "_BayesianFrequencyModel":
        grouped: dict[tuple[str, PortfolioAction], list[int]] = defaultdict(
            lambda: [0 for _ in _ACTIONS]
        )
        fallback = [0 for _ in _ACTIONS]
        for item in examples:
            grouped[(item.probability_stratum, item.sealed_action)][
                _ACTION_INDEX[item.target_action]
            ] += 1
            fallback[_ACTION_INDEX[item.target_action]] += 1
        return cls(
            probabilities_by_key={
                key: _smoothed_distribution(counts, prior)
                for key, counts in sorted(
                    grouped.items(), key=lambda item: (item[0][0], item[0][1].value)
                )
            },
            fallback_probabilities=_smoothed_distribution(fallback, prior),
        )

    def probabilities(
        self, examples: Sequence[PortfolioSelectorExample]
    ) -> tuple[tuple[float, ...], ...]:
        return tuple(
            self.probabilities_by_key.get(
                (item.probability_stratum, item.sealed_action),
                self.fallback_probabilities,
            )
            for item in examples
        )

    def explanation(self) -> PortfolioModelExplanation:
        terms: list[tuple[str, str, float]] = []
        for (stratum, sealed), values in sorted(
            self.probabilities_by_key.items(),
            key=lambda item: (item[0][0], item[0][1].value),
        ):
            for action, value in zip(_ACTIONS, values, strict=True):
                terms.append(
                    (
                        action.value,
                        f"posterior:{stratum}:sealed={sealed.value}",
                        value,
                    )
                )
        return PortfolioModelExplanation(model_kind=self.model_kind, terms=tuple(terms))

    def decision_explanation(
        self, example: PortfolioSelectorExample, action: PortfolioAction
    ) -> tuple[tuple[str, float], ...]:
        values = self.probabilities_by_key.get(
            (example.probability_stratum, example.sealed_action),
            self.fallback_probabilities,
        )
        return tuple(
            (f"posterior:{candidate.value}", value)
            for candidate, value in sorted(
                zip(_ACTIONS, values, strict=True),
                key=lambda item: (-item[1], item[0].value),
            )
        )[:3] + ((f"selected:{action.value}", values[_ACTION_INDEX[action]]),)


@dataclass(frozen=True, slots=True)
class _RegularizedLogisticModel:
    input_feature_names: tuple[str, ...]
    expanded_feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    weights: tuple[tuple[float, ...], ...]
    model_kind: PortfolioSelectorModel = PortfolioSelectorModel.REGULARIZED_LOGISTIC

    @classmethod
    def fit(
        cls,
        examples: Sequence[PortfolioSelectorExample],
        feature_names: tuple[str, ...],
        *,
        l2: float,
        learning_rate: float,
        iterations: int,
    ) -> "_RegularizedLogisticModel":
        raw = tuple(_raw_logistic_row(item, feature_names) for item in examples)
        numeric_width = len(feature_names)
        means = tuple(
            sum(row[index] for row in raw) / len(raw)
            for index in range(numeric_width)
        )
        scales = tuple(
            max(
                sqrt(
                    sum((row[index] - means[index]) ** 2 for row in raw) / len(raw)
                ),
                1e-9,
            )
            for index in range(numeric_width)
        )
        rows = tuple(_standardize_row(row, means, scales) for row in raw)
        width = len(rows[0]) + 1
        weights = [[0.0 for _ in range(width)] for _ in _ACTIONS]
        for iteration in range(iterations):
            gradients = [[0.0 for _ in range(width)] for _ in _ACTIONS]
            for row, example in zip(rows, examples, strict=True):
                vector = (1.0,) + row
                probabilities = _softmax(
                    tuple(
                        sum(weight * value for weight, value in zip(action_weights, vector))
                        for action_weights in weights
                    )
                )
                target_index = _ACTION_INDEX[example.target_action]
                for action_index in range(len(_ACTIONS)):
                    error = probabilities[action_index] - float(
                        action_index == target_index
                    )
                    for feature_index, value in enumerate(vector):
                        gradients[action_index][feature_index] += error * value
            rate = learning_rate / sqrt(iteration + 1.0)
            sample_count = float(len(rows))
            for action_index in range(len(_ACTIONS)):
                for feature_index in range(width):
                    penalty = (
                        0.0
                        if feature_index == 0
                        else l2 * weights[action_index][feature_index]
                    )
                    weights[action_index][feature_index] -= rate * (
                        gradients[action_index][feature_index] / sample_count + penalty
                    )
        expanded = feature_names + tuple(
            f"sealed_action={action.value}" for action in _ACTIONS
        )
        return cls(
            input_feature_names=feature_names,
            expanded_feature_names=expanded,
            means=means,
            scales=scales,
            weights=tuple(tuple(item) for item in weights),
        )

    def _vector(self, example: PortfolioSelectorExample) -> tuple[float, ...]:
        raw = _raw_logistic_row(example, self.input_feature_names)
        return (1.0,) + _standardize_row(raw, self.means, self.scales)

    def probabilities(
        self, examples: Sequence[PortfolioSelectorExample]
    ) -> tuple[tuple[float, ...], ...]:
        return tuple(
            _softmax(
                tuple(
                    sum(weight * value for weight, value in zip(action_weights, self._vector(item)))
                    for action_weights in self.weights
                )
            )
            for item in examples
        )

    def explanation(self) -> PortfolioModelExplanation:
        names = ("intercept",) + self.expanded_feature_names
        return PortfolioModelExplanation(
            model_kind=self.model_kind,
            terms=tuple(
                (action.value, name, value)
                for action, action_weights in zip(_ACTIONS, self.weights, strict=True)
                for name, value in zip(names, action_weights, strict=True)
            ),
        )

    def decision_explanation(
        self, example: PortfolioSelectorExample, action: PortfolioAction
    ) -> tuple[tuple[str, float], ...]:
        vector = self._vector(example)
        weights = self.weights[_ACTION_INDEX[action]]
        names = ("intercept",) + self.expanded_feature_names
        contributions = tuple(
            (name, weight * value)
            for name, weight, value in zip(names, weights, vector, strict=True)
        )
        return tuple(
            sorted(contributions, key=lambda item: (-abs(item[1]), item[0]))[:5]
        )


class EvaluateScientificPortfolioSelector:
    """Select the simplest stable portfolio policy without weakening evidence."""

    def __init__(
        self,
        policy: ScientificPortfolioSelectorPolicy = ScientificPortfolioSelectorPolicy(),
    ) -> None:
        self._policy = policy

    def execute(
        self, examples: Sequence[PortfolioSelectorExample]
    ) -> ScientificPortfolioSelectorResult:
        ordered = tuple(
            sorted(
                examples,
                key=lambda item: (item.trading_day, item.observed_at, item.event_id),
            )
        )
        feature_names, cost_model_version = _validate_examples(ordered)
        input_fingerprint = _input_fingerprint(ordered)
        policy_fingerprint = _sha(asdict(self._policy))
        run_id = _sha(
            {
                "input_fingerprint": input_fingerprint,
                "policy_fingerprint": policy_fingerprint,
            }
        )
        split = _temporal_split(
            tuple(sorted({item.trading_day for item in ordered})),
            self._policy.gap_trading_days,
        )
        train = _on_days(ordered, split.train_days)
        validation = _on_days(ordered, split.validation_days)
        holdout = _on_days(ordered, split.holdout_days)
        blockers = _minimum_data_reasons(
            train=train,
            validation=validation,
            holdout=holdout,
            total_days=len({item.trading_day for item in ordered}),
            policy=self._policy,
        )
        if blockers:
            return _blocked_result(
                run_id=run_id,
                input_fingerprint=input_fingerprint,
                policy_fingerprint=policy_fingerprint,
                split=split,
                feature_names=feature_names,
                cost_model_version=cost_model_version,
                examples=ordered,
                validation=validation,
                holdout=holdout,
                blockers=blockers,
                policy=self._policy,
            )

        calibration_days, selection_days = _split_validation_days(split.validation_days)
        calibration = _on_days(ordered, calibration_days)
        selection = _on_days(ordered, selection_days)
        fixed_model = _FixedRuleModel()
        bayesian_model = _BayesianFrequencyModel.fit(
            train, prior=self._policy.bayesian_prior
        )
        logistic_model = _RegularizedLogisticModel.fit(
            train,
            feature_names,
            l2=self._policy.logistic_l2,
            learning_rate=self._policy.logistic_learning_rate,
            iterations=self._policy.logistic_iterations,
        )
        models: tuple[FittedPortfolioModel, ...] = (
            fixed_model,
            bayesian_model,
            logistic_model,
        )
        fixed_validation = _metrics(
            _predict(fixed_model, selection, threshold=0.0),
            bins=self._policy.calibration_bins,
        )
        fixed_holdout = _metrics(
            _predict(fixed_model, holdout, threshold=0.0),
            bins=self._policy.calibration_bins,
        )
        evaluations: list[PortfolioModelEvaluation] = []
        validation_accuracy_by_model: dict[PortfolioSelectorModel, float] = {}
        frozen: dict[
            PortfolioSelectorModel, tuple[FittedPortfolioModel, _ConfidenceCalibrator, float]
        ] = {}

        for model in models:
            if model.model_kind is PortfolioSelectorModel.FIXED_RULE:
                calibrator = _identity_calibrator(self._policy.calibration_bins)
                threshold = 0.0
            else:
                calibrator = _fit_calibrator(
                    model.probabilities(calibration),
                    calibration,
                    bins=self._policy.calibration_bins,
                )
                threshold = _select_threshold(
                    model=model,
                    examples=selection,
                    calibrator=calibrator,
                    policy=self._policy,
                )
            validation_predictions = _predict(
                model, selection, threshold=threshold, calibrator=calibrator
            )
            holdout_predictions = _predict(
                model, holdout, threshold=threshold, calibrator=calibrator
            )
            validation_metrics = _metrics(
                validation_predictions, bins=self._policy.calibration_bins
            )
            holdout_metrics = _metrics(
                holdout_predictions, bins=self._policy.calibration_bins
            )
            validation_lift = _accuracy_lift(validation_metrics, fixed_validation)
            holdout_lift = _accuracy_lift(holdout_metrics, fixed_holdout)
            folds = _walk_forward(
                model_kind=model.model_kind,
                ordered=ordered,
                pre_holdout_days=tuple(
                    day for day in sorted({item.trading_day for item in ordered})
                    if day < min(split.holdout_days)
                ),
                feature_names=feature_names,
                policy=self._policy,
                threshold=threshold,
            )
            positive_folds = sum(item.positive for item in folds)
            reasons = _eligibility_reasons(
                model_kind=model.model_kind,
                validation_metrics=validation_metrics,
                validation_lift=validation_lift,
                positive_folds=positive_folds,
                total_folds=len(folds),
                simpler_validation_accuracy=validation_accuracy_by_model,
                policy=self._policy,
            )
            evaluations.append(
                PortfolioModelEvaluation(
                    model_kind=model.model_kind,
                    validation_metrics=validation_metrics,
                    holdout_metrics=holdout_metrics,
                    selected_confidence_threshold=threshold,
                    validation_accuracy_lift=validation_lift,
                    holdout_accuracy_lift=holdout_lift,
                    positive_walk_forward_folds=positive_folds,
                    total_walk_forward_folds=len(folds),
                    eligible=not reasons,
                    reason_codes=reasons,
                    walk_forward=folds,
                    explanation=model.explanation(),
                )
            )
            validation_accuracy_by_model[model.model_kind] = (
                validation_metrics.accuracy_when_acted or 0.0
            )
            frozen[model.model_kind] = (model, calibrator, threshold)

        selected_evaluation = next(
            (
                item
                for item in reversed(evaluations)
                if item.eligible
            ),
            evaluations[0],
        )
        tuned_model = selected_evaluation.model_kind
        final_holdout_reasons = (
            _holdout_gate_reasons(selected_evaluation, self._policy)
            if tuned_model is not PortfolioSelectorModel.FIXED_RULE
            else ()
        )
        if tuned_model is PortfolioSelectorModel.FIXED_RULE:
            selected_model = PortfolioSelectorModel.FIXED_RULE
            state = PortfolioSelectorState.NO_STABLE_IMPROVEMENT
            reason_codes = ("no_candidate_passed_stable_improvement_gate",)
        elif final_holdout_reasons:
            selected_model = PortfolioSelectorModel.FIXED_RULE
            state = PortfolioSelectorState.NO_STABLE_IMPROVEMENT
            reason_codes = (
                "tuned_candidate_failed_independent_holdout",
                *final_holdout_reasons,
            )
        else:
            selected_model = tuned_model
            state = PortfolioSelectorState.READY
            reason_codes = ("stable_improvement_confirmed_on_holdout",)
        model, calibrator, threshold = frozen[selected_model]
        decisions = _decisions(
            model,
            _predict(model, holdout, threshold=threshold, calibrator=calibrator),
        )
        return ScientificPortfolioSelectorResult(
            run_id=run_id,
            input_fingerprint=input_fingerprint,
            policy_fingerprint=policy_fingerprint,
            state=state,
            split=split,
            feature_names=feature_names,
            cost_model_version=cost_model_version,
            examples=len(ordered),
            trading_days=len({item.trading_day for item in ordered}),
            evaluations=tuple(evaluations),
            selected_model=selected_model,
            holdout_decisions=decisions,
            reason_codes=reason_codes,
        )


def _validate_examples(
    examples: Sequence[PortfolioSelectorExample],
) -> tuple[tuple[str, ...], str]:
    if not examples:
        raise ValueError("portfolio selector requires examples")
    event_ids = tuple(item.event_id for item in examples)
    if len(event_ids) != len(set(event_ids)):
        raise ValueError("portfolio selector event ids must be unique")
    schemas = {
        tuple(name for name, _ in item.feature_values)
        for item in examples
    }
    if len(schemas) != 1:
        raise ValueError("portfolio selector feature schema must be stable")
    cost_models = {item.cost_model_version for item in examples}
    if len(cost_models) != 1:
        raise ValueError("portfolio selector requires one cost model version")
    return next(iter(schemas)), next(iter(cost_models))


def _temporal_split(days: tuple[date, ...], gap: int) -> PortfolioTemporalSplit:
    if not days:
        raise ValueError("portfolio selector requires trading days")
    train_boundary = max(1, int(len(days) * 0.60))
    validation_boundary = max(train_boundary + 1, int(len(days) * 0.80))
    validation_boundary = min(validation_boundary, len(days))
    first_gap_start = max(0, train_boundary - gap)
    second_gap_start = max(train_boundary, validation_boundary - gap)
    return PortfolioTemporalSplit(
        train_days=days[:first_gap_start],
        validation_days=days[train_boundary:second_gap_start],
        holdout_days=days[validation_boundary:],
        embargo_days=days[first_gap_start:train_boundary]
        + days[second_gap_start:validation_boundary],
        gap_trading_days=gap,
    )


def _split_validation_days(days: tuple[date, ...]) -> tuple[tuple[date, ...], tuple[date, ...]]:
    boundary = max(1, len(days) // 2)
    return days[:boundary], days[boundary:]


def _on_days(
    examples: Sequence[PortfolioSelectorExample], days: Sequence[date]
) -> tuple[PortfolioSelectorExample, ...]:
    day_set = set(days)
    return tuple(item for item in examples if item.trading_day in day_set)


def _minimum_data_reasons(
    *,
    train: Sequence[PortfolioSelectorExample],
    validation: Sequence[PortfolioSelectorExample],
    holdout: Sequence[PortfolioSelectorExample],
    total_days: int,
    policy: ScientificPortfolioSelectorPolicy,
) -> tuple[str, ...]:
    reasons = []
    if len(train) < policy.minimum_train_examples:
        reasons.append("insufficient_train_examples")
    if len(validation) < policy.minimum_validation_examples:
        reasons.append("insufficient_validation_examples")
    if len(holdout) < policy.minimum_holdout_examples:
        reasons.append("insufficient_holdout_examples")
    if total_days < policy.minimum_total_trading_days:
        reasons.append("insufficient_trading_days")
    if len({item.trading_day for item in validation}) < 2:
        reasons.append("validation_cannot_separate_calibration_and_selection")
    target_actions = {item.target_action for item in train}
    if len(target_actions) < 2:
        reasons.append("insufficient_target_action_diversity")
    return tuple(reasons)


def _blocked_result(
    *,
    run_id: str,
    input_fingerprint: str,
    policy_fingerprint: str,
    split: PortfolioTemporalSplit,
    feature_names: tuple[str, ...],
    cost_model_version: str,
    examples: Sequence[PortfolioSelectorExample],
    validation: Sequence[PortfolioSelectorExample],
    holdout: Sequence[PortfolioSelectorExample],
    blockers: tuple[str, ...],
    policy: ScientificPortfolioSelectorPolicy,
) -> ScientificPortfolioSelectorResult:
    fixed = _FixedRuleModel()
    empty_metrics = _metrics((), bins=policy.calibration_bins)
    validation_metrics = (
        _metrics(_predict(fixed, validation, threshold=0.0), bins=policy.calibration_bins)
        if validation else empty_metrics
    )
    holdout_predictions = (
        _predict(fixed, holdout, threshold=0.0) if holdout else ()
    )
    holdout_metrics = (
        _metrics(holdout_predictions, bins=policy.calibration_bins)
        if holdout else empty_metrics
    )
    evaluations = (
        PortfolioModelEvaluation(
            model_kind=PortfolioSelectorModel.FIXED_RULE,
            validation_metrics=validation_metrics,
            holdout_metrics=holdout_metrics,
            selected_confidence_threshold=0.0,
            validation_accuracy_lift=0.0,
            holdout_accuracy_lift=0.0,
            positive_walk_forward_folds=0,
            total_walk_forward_folds=0,
            eligible=True,
            reason_codes=(),
            walk_forward=(),
            explanation=fixed.explanation(),
        ),
        *tuple(
            PortfolioModelEvaluation(
                model_kind=model_kind,
                validation_metrics=empty_metrics,
                holdout_metrics=empty_metrics,
                selected_confidence_threshold=policy.confidence_thresholds[-1],
                validation_accuracy_lift=None,
                holdout_accuracy_lift=None,
                positive_walk_forward_folds=0,
                total_walk_forward_folds=0,
                eligible=False,
                reason_codes=blockers,
                walk_forward=(),
                explanation=PortfolioModelExplanation(
                    model_kind=model_kind,
                    terms=(),
                ),
            )
            for model_kind in _MODELS[1:]
        ),
    )
    return ScientificPortfolioSelectorResult(
        run_id=run_id,
        input_fingerprint=input_fingerprint,
        policy_fingerprint=policy_fingerprint,
        state=PortfolioSelectorState.BLOCKED_BY_DATA,
        split=split,
        feature_names=feature_names,
        cost_model_version=cost_model_version,
        examples=len(examples),
        trading_days=len({item.trading_day for item in examples}),
        evaluations=evaluations,
        selected_model=PortfolioSelectorModel.FIXED_RULE,
        holdout_decisions=_decisions(fixed, holdout_predictions),
        reason_codes=blockers,
    )


def _raw_logistic_row(
    example: PortfolioSelectorExample, feature_names: tuple[str, ...]
) -> tuple[float, ...]:
    values = dict(example.feature_values)
    return tuple(values[name] for name in feature_names) + tuple(
        float(example.sealed_action is action) for action in _ACTIONS
    )


def _standardize_row(
    row: tuple[float, ...],
    means: tuple[float, ...],
    scales: tuple[float, ...],
) -> tuple[float, ...]:
    numeric_width = len(means)
    return tuple(
        (row[index] - means[index]) / scales[index]
        for index in range(numeric_width)
    ) + row[numeric_width:]


def _softmax(scores: tuple[float, ...]) -> tuple[float, ...]:
    maximum = max(scores)
    numerators = tuple(exp(item - maximum) for item in scores)
    denominator = sum(numerators)
    return tuple(item / denominator for item in numerators)


def _one_hot(action: PortfolioAction) -> tuple[float, ...]:
    return tuple(float(candidate is action) for candidate in _ACTIONS)


def _smoothed_distribution(counts: Sequence[int], prior: float) -> tuple[float, ...]:
    denominator = sum(counts) + prior * len(_ACTIONS)
    return tuple((count + prior) / denominator for count in counts)


def _fit_calibrator(
    probabilities: Sequence[tuple[float, ...]],
    examples: Sequence[PortfolioSelectorExample],
    *,
    bins: int,
) -> _ConfidenceCalibrator:
    correct: list[list[bool]] = [[] for _ in range(bins)]
    for values, example in zip(probabilities, examples, strict=True):
        action, confidence = _argmax(values)
        index = min(int(confidence * bins), bins - 1)
        correct[index].append(action is example.target_action)
    return _ConfidenceCalibrator(
        bins=bins,
        observed_accuracy=tuple(
            (sum(items) + 1.0) / (len(items) + 2.0) if items else None
            for items in correct
        ),
    )


def _identity_calibrator(bins: int) -> _ConfidenceCalibrator:
    return _ConfidenceCalibrator(bins=bins, observed_accuracy=(None,) * bins)


def _argmax(values: tuple[float, ...]) -> tuple[PortfolioAction, float]:
    index = max(range(len(values)), key=lambda item: (values[item], -item))
    return _ACTIONS[index], values[index]


def _predict(
    model: FittedPortfolioModel,
    examples: Sequence[PortfolioSelectorExample],
    *,
    threshold: float,
    calibrator: _ConfidenceCalibrator | None = None,
) -> tuple[_Prediction, ...]:
    active_calibrator = calibrator or _identity_calibrator(5)
    predictions = []
    for example, values in zip(
        examples, model.probabilities(examples), strict=True
    ):
        raw_action, raw_confidence = _argmax(values)
        calibrated = active_calibrator.apply(raw_confidence)
        action = (
            raw_action
            if raw_action is not PortfolioAction.ABSTAIN
            and calibrated + _EPSILON >= threshold
            else PortfolioAction.ABSTAIN
        )
        predictions.append(
            _Prediction(
                example=example,
                probabilities=values,
                raw_action=raw_action,
                raw_confidence=raw_confidence,
                calibrated_confidence=calibrated,
                action=action,
            )
        )
    return tuple(predictions)


def _metrics(
    predictions: Sequence[_Prediction], *, bins: int
) -> PortfolioSelectorMetrics:
    observations = len(predictions)
    acted = tuple(
        item for item in predictions if item.action is not PortfolioAction.ABSTAIN
    )
    correct = sum(item.action is item.example.target_action for item in acted)
    action_counts = tuple(
        (action, sum(item.action is action for item in predictions))
        for action in _ACTIONS
    )
    if observations:
        brier = sum(
            sum(
                (
                    probability
                    - float(action is item.example.target_action)
                )
                ** 2
                for action, probability in zip(
                    _ACTIONS, item.probabilities, strict=True
                )
            )
            / 2.0
            for item in predictions
        ) / observations
    else:
        brier = 0.0
    calibration_bins = []
    calibration_error = 0.0
    for index in range(bins):
        lower = index / bins
        upper = (index + 1) / bins
        items = tuple(
            item
            for item in predictions
            if min(int(item.calibrated_confidence * bins), bins - 1) == index
        )
        if items:
            mean_confidence = sum(
                item.calibrated_confidence for item in items
            ) / len(items)
            observed_accuracy = sum(
                item.raw_action is item.example.target_action for item in items
            ) / len(items)
            calibration_error += (
                len(items)
                / max(observations, 1)
                * abs(mean_confidence - observed_accuracy)
            )
        else:
            mean_confidence = None
            observed_accuracy = None
        calibration_bins.append(
            PortfolioCalibrationBin(
                lower_confidence=lower,
                upper_confidence=upper,
                observations=len(items),
                mean_confidence=mean_confidence,
                observed_accuracy=observed_accuracy,
            )
        )
    coverage = len(acted) / observations if observations else 0.0
    return PortfolioSelectorMetrics(
        observations=observations,
        acted_observations=len(acted),
        correct_acted_observations=correct,
        accuracy_when_acted=correct / len(acted) if acted else None,
        coverage=coverage,
        abstention_rate=1.0 - coverage if observations else 1.0,
        multiclass_brier_score=min(max(brier, 0.0), 1.0),
        expected_calibration_error=min(max(calibration_error, 0.0), 1.0),
        action_counts=action_counts,
        calibration=tuple(calibration_bins),
    )


def _select_threshold(
    *,
    model: FittedPortfolioModel,
    examples: Sequence[PortfolioSelectorExample],
    calibrator: _ConfidenceCalibrator,
    policy: ScientificPortfolioSelectorPolicy,
) -> float:
    candidates = []
    for threshold in policy.confidence_thresholds:
        metrics = _metrics(
            _predict(model, examples, threshold=threshold, calibrator=calibrator),
            bins=policy.calibration_bins,
        )
        eligible = (
            metrics.acted_observations >= policy.minimum_acted_examples
            and metrics.coverage >= policy.minimum_coverage
            and metrics.accuracy_when_acted is not None
        )
        candidates.append((eligible, metrics.accuracy_when_acted or 0.0, metrics.coverage, -threshold, threshold))
    return max(candidates)[-1]


def _accuracy_lift(
    candidate: PortfolioSelectorMetrics, baseline: PortfolioSelectorMetrics
) -> float | None:
    if (
        candidate.accuracy_when_acted is None
        or baseline.accuracy_when_acted is None
    ):
        return None
    return candidate.accuracy_when_acted - baseline.accuracy_when_acted


def _walk_forward(
    *,
    model_kind: PortfolioSelectorModel,
    ordered: Sequence[PortfolioSelectorExample],
    pre_holdout_days: tuple[date, ...],
    feature_names: tuple[str, ...],
    policy: ScientificPortfolioSelectorPolicy,
    threshold: float,
) -> tuple[PortfolioWalkForwardFold, ...]:
    if len(pre_holdout_days) < policy.walk_forward_folds + policy.gap_trading_days + 2:
        return ()
    initial = max(2, len(pre_holdout_days) // 2)
    remaining = len(pre_holdout_days) - initial
    block = max(1, remaining // policy.walk_forward_folds)
    folds = []
    for fold_index in range(policy.walk_forward_folds):
        evaluation_start_index = initial + fold_index * block
        if evaluation_start_index >= len(pre_holdout_days):
            break
        evaluation_end = (
            len(pre_holdout_days)
            if fold_index == policy.walk_forward_folds - 1
            else min(len(pre_holdout_days), evaluation_start_index + block)
        )
        train_end_index = evaluation_start_index - policy.gap_trading_days
        if train_end_index <= 0:
            continue
        train_days = pre_holdout_days[:train_end_index]
        evaluation_days = pre_holdout_days[evaluation_start_index:evaluation_end]
        train = _on_days(ordered, train_days)
        evaluation = _on_days(ordered, evaluation_days)
        if not train or not evaluation:
            continue
        model = _fit_model(model_kind, train, feature_names, policy)
        model_metrics = _metrics(
            _predict(model, evaluation, threshold=threshold),
            bins=policy.calibration_bins,
        )
        fixed_metrics = _metrics(
            _predict(_FixedRuleModel(), evaluation, threshold=0.0),
            bins=policy.calibration_bins,
        )
        improvement = _accuracy_lift(model_metrics, fixed_metrics)
        folds.append(
            PortfolioWalkForwardFold(
                model_kind=model_kind,
                train_end=train_days[-1],
                evaluation_start=evaluation_days[0],
                train_days=len(train_days),
                evaluation_days=len(evaluation_days),
                gap_days=policy.gap_trading_days,
                model_accuracy=model_metrics.accuracy_when_acted,
                fixed_rule_accuracy=fixed_metrics.accuracy_when_acted,
                coverage=model_metrics.coverage,
                improvement=improvement,
                positive=(
                    improvement is not None
                    and improvement >= policy.minimum_accuracy_lift
                    and model_metrics.coverage >= policy.minimum_coverage
                ),
            )
        )
    return tuple(folds)


def _fit_model(
    model_kind: PortfolioSelectorModel,
    examples: Sequence[PortfolioSelectorExample],
    feature_names: tuple[str, ...],
    policy: ScientificPortfolioSelectorPolicy,
) -> FittedPortfolioModel:
    if model_kind is PortfolioSelectorModel.FIXED_RULE:
        return _FixedRuleModel()
    if model_kind is PortfolioSelectorModel.BAYESIAN_FREQUENCY:
        return _BayesianFrequencyModel.fit(
            examples, prior=policy.bayesian_prior
        )
    return _RegularizedLogisticModel.fit(
        examples,
        feature_names,
        l2=policy.logistic_l2,
        learning_rate=policy.logistic_learning_rate,
        iterations=policy.logistic_iterations,
    )


def _eligibility_reasons(
    *,
    model_kind: PortfolioSelectorModel,
    validation_metrics: PortfolioSelectorMetrics,
    validation_lift: float | None,
    positive_folds: int,
    total_folds: int,
    simpler_validation_accuracy: dict[PortfolioSelectorModel, float],
    policy: ScientificPortfolioSelectorPolicy,
) -> tuple[str, ...]:
    if model_kind is PortfolioSelectorModel.FIXED_RULE:
        return ()
    reasons = []
    if validation_metrics.acted_observations < policy.minimum_acted_examples:
        reasons.append("insufficient_validation_actions")
    if validation_metrics.coverage < policy.minimum_coverage:
        reasons.append("validation_coverage_below_gate")
    if validation_lift is None or validation_lift < policy.minimum_accuracy_lift:
        reasons.append("validation_accuracy_lift_below_gate")
    required_folds = min(
        policy.minimum_positive_walk_forward_folds,
        total_folds,
    )
    if total_folds == 0 or positive_folds < required_folds:
        reasons.append("walk_forward_improvement_not_stable")
    if model_kind is PortfolioSelectorModel.REGULARIZED_LOGISTIC:
        simpler = max(simpler_validation_accuracy.values(), default=0.0)
        complex_accuracy = validation_metrics.accuracy_when_acted or 0.0
        if complex_accuracy < simpler + policy.logistic_complexity_premium:
            reasons.append("complex_model_does_not_beat_simpler_model")
    return tuple(dict.fromkeys(reasons))


def _holdout_gate_reasons(
    evaluation: PortfolioModelEvaluation,
    policy: ScientificPortfolioSelectorPolicy,
) -> tuple[str, ...]:
    reasons = []
    if evaluation.holdout_metrics.acted_observations < policy.minimum_acted_examples:
        reasons.append("insufficient_holdout_actions")
    if evaluation.holdout_metrics.coverage < policy.minimum_coverage:
        reasons.append("holdout_coverage_below_gate")
    if (
        evaluation.holdout_accuracy_lift is None
        or evaluation.holdout_accuracy_lift < policy.minimum_accuracy_lift
    ):
        reasons.append("holdout_accuracy_lift_below_gate")
    return tuple(reasons)


def _decisions(
    model: FittedPortfolioModel,
    predictions: Sequence[_Prediction],
) -> tuple[PortfolioDecision, ...]:
    return tuple(
        PortfolioDecision(
            event_id=item.example.event_id,
            model_kind=model.model_kind,
            action=item.action,
            confidence=item.calibrated_confidence,
            reason_codes=(
                ("confidence_gate_passed",)
                if item.action is not PortfolioAction.ABSTAIN
                else ("abstained_by_confidence_or_model",)
            ),
            explanation=model.decision_explanation(item.example, item.raw_action),
        )
        for item in predictions
    )


def _input_fingerprint(examples: Sequence[PortfolioSelectorExample]) -> str:
    return _sha(
        [
            {
                "event_id": item.event_id,
                "instrument_id": item.instrument_id,
                "source_study_ids": item.source_study_ids,
                "source_artifact_fingerprints": item.source_artifact_fingerprints,
                "trading_day": item.trading_day.isoformat(),
                "observed_at": item.observed_at.isoformat(),
                "feature_max_observed_at": item.feature_max_observed_at.isoformat(),
                "label_observed_at": item.label_observed_at.isoformat(),
                "horizon_seconds": item.horizon_seconds,
                "sealed_action": item.sealed_action.value,
                "target_action": item.target_action.value,
                "probability_stratum": item.probability_stratum,
                "feature_values": item.feature_values,
                "cost_model_version": item.cost_model_version,
            }
            for item in examples
        ]
    )


def _sha(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"
