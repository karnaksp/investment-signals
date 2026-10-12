"""Reproducible shadow comparison over already sealed scientific outcomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from math import ceil
from typing import Protocol, Sequence

from tinvest_signal_engine.domain.hypothesis_evidence import (
    DatasetPartition,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.scientific_model_shadow import (
    SealedShadowDataset,
    ShadowCalibrationBin,
    ShadowModelEvaluation,
    ShadowModelExample,
    ShadowModelKind,
    ShadowModelMetrics,
    ShadowPortfolioResult,
    ShadowResultState,
    ShadowSelectionState,
    ShadowStudyResult,
)


FeatureRow = tuple[float, ...]


class ShadowExampleSourcePort(Protocol):
    def load(self) -> SealedShadowDataset: ...


class FittedShadowEstimator(Protocol):
    def predict_probabilities(
        self, rows: Sequence[FeatureRow]
    ) -> tuple[float, ...]: ...


class ShadowEstimatorFactoryPort(Protocol):
    def fit(
        self,
        *,
        model_kind: ShadowModelKind,
        feature_names: tuple[str, ...],
        rows: Sequence[FeatureRow],
        labels: Sequence[int],
        seed: int,
    ) -> FittedShadowEstimator | None: ...


class ShadowArtifactPort(Protocol):
    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None: ...

    def persist(self, result: ShadowPortfolioResult) -> str: ...


@dataclass(frozen=True, slots=True)
class ShadowComparisonPolicy:
    version: str = "scientific-model-shadow-v1.0.0"
    required_study_ids: tuple[str, ...] = (
        "H1",
        "H2",
        "H3V2",
        "H4V2",
        "H5",
        "H6",
        "H7V3",
        "H12",
        "H15V2",
        "H16",
        "H17",
        "C1",
        "C2",
        "C3",
        "C4",
    )
    minimum_train_examples: int = 100
    minimum_validation_examples: int = 50
    minimum_holdout_examples: int = 50
    minimum_total_trading_days: int = 30
    minimum_holdout_trading_days: int = 5
    action_probability_threshold: float = 0.60
    candidate_action_thresholds: tuple[float, ...] = (
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
    )
    minimum_model_coverage: float = 0.10
    minimum_complexity_useful_rate_improvement: float = 0.02
    stability_blocks: int = 5
    minimum_positive_stability_fraction: float = 0.80
    calibration_bins: int = 10
    seed: int = 20260722

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("shadow policy version must not be empty")
        if not self.required_study_ids or any(
            not item.strip() for item in self.required_study_ids
        ):
            raise ValueError("shadow policy requires study identities")
        if len(set(self.required_study_ids)) != len(self.required_study_ids):
            raise ValueError("shadow required study identities must be unique")
        counts = (
            self.minimum_train_examples,
            self.minimum_validation_examples,
            self.minimum_holdout_examples,
            self.minimum_total_trading_days,
            self.minimum_holdout_trading_days,
            self.stability_blocks,
            self.calibration_bins,
        )
        if any(value <= 0 for value in counts):
            raise ValueError("shadow sample requirements must be positive")
        if not 0.0 <= self.action_probability_threshold <= 1.0:
            raise ValueError("shadow action threshold must be in [0, 1]")
        if (
            not self.candidate_action_thresholds
            or tuple(sorted(set(self.candidate_action_thresholds)))
            != self.candidate_action_thresholds
            or any(
                not self.action_probability_threshold <= value <= 1.0
                for value in self.candidate_action_thresholds
            )
        ):
            raise ValueError(
                "shadow candidate thresholds must be sorted, unique and no lower "
                "than the minimum action threshold"
            )
        if not 0.0 < self.minimum_model_coverage <= 1.0:
            raise ValueError("shadow minimum model coverage must be in (0, 1]")
        if not 0.0 <= self.minimum_complexity_useful_rate_improvement <= 1.0:
            raise ValueError("shadow complexity improvement must be in [0, 1]")
        if not 0.0 < self.minimum_positive_stability_fraction <= 1.0:
            raise ValueError(
                "shadow minimum positive stability fraction must be in (0, 1]"
            )


@dataclass(frozen=True, slots=True)
class ShadowPortfolioExecution:
    run_id: str
    reused: bool
    artifact_uri: str
    result: ShadowPortfolioResult | None


@dataclass(frozen=True, slots=True)
class _CalibrationMap:
    bins: int
    values: tuple[float, ...]

    def apply(self, probabilities: Sequence[float]) -> tuple[float, ...]:
        return tuple(
            self.values[min(int(value * self.bins), self.bins - 1)]
            for value in probabilities
        )


class RunScientificModelShadowComparison:
    """Compare models in shadow without changing the sealed evidence gate."""

    def __init__(
        self,
        *,
        source: ShadowExampleSourcePort,
        estimators: ShadowEstimatorFactoryPort,
        artifacts: ShadowArtifactPort,
        policy: ShadowComparisonPolicy = ShadowComparisonPolicy(),
    ) -> None:
        self._source = source
        self._estimators = estimators
        self._artifacts = artifacts
        self._policy = policy

    def execute(self) -> ShadowPortfolioExecution:
        dataset = self._source.load()
        input_fingerprint = shadow_input_fingerprint(dataset)
        policy_fingerprint = shadow_policy_fingerprint(self._policy)
        run_id = _fingerprint(
            {
                "input_fingerprint": input_fingerprint,
                "policy_fingerprint": policy_fingerprint,
                "workflow": "scientific-model-shadow-comparison-v1",
            }
        )
        completed = self._artifacts.completed_uri(run_id, input_fingerprint)
        if completed is not None:
            return ShadowPortfolioExecution(run_id, True, completed, None)

        grouped: defaultdict[tuple[str, str, int], list[ShadowModelExample]] = (
            defaultdict(list)
        )
        for example in dataset.examples:
            grouped[example.scope.key].append(example)
        results = tuple(
            self._study(scope, tuple(grouped.get(scope.key, ())))
            for scope in dataset.scopes
        )
        available_ids = {scope.study_id for scope in dataset.scopes}
        missing = tuple(
            item
            for item in self._policy.required_study_ids
            if item not in available_ids
        )
        state = (
            ShadowResultState.READY
            if not missing
            and all(item.state is ShadowResultState.READY for item in results)
            else ShadowResultState.BLOCKED_BY_DATA
        )
        result = ShadowPortfolioResult(
            run_id=run_id,
            input_fingerprint=input_fingerprint,
            policy_fingerprint=policy_fingerprint,
            state=state,
            results=results,
            missing_study_ids=missing,
        )
        return ShadowPortfolioExecution(
            run_id=run_id,
            reused=False,
            artifact_uri=self._artifacts.persist(result),
            result=result,
        )

    def _study(
        self,
        scope,
        examples: tuple[ShadowModelExample, ...],
    ) -> ShadowStudyResult:
        ordered = tuple(
            sorted(
                examples,
                key=lambda item: (
                    item.trading_day,
                    item.observed_at,
                    item.observation_id,
                ),
            )
        )
        schemas = {
            tuple(name for name, _ in example.feature_values) for example in ordered
        }
        feature_names = next(iter(schemas), ()) if len(schemas) == 1 else ()
        days = tuple(sorted({item.trading_day for item in ordered}))
        if len(days) >= 5:
            split = chronological_split_60_20_20(days)
            partitions = {
                partition: tuple(
                    item
                    for item in ordered
                    if split.partition_for(item.trading_day) is partition
                )
                for partition in DatasetPartition
            }
            train = partitions[DatasetPartition.TRAIN]
            validation = partitions[DatasetPartition.VALIDATION]
            holdout = partitions[DatasetPartition.HOLDOUT]
        else:
            # Preserve an honest blocked artifact instead of inventing a split.
            train, validation, holdout = ordered, (), ()
        reasons = self._blocked_reasons(ordered, train, validation, holdout, schemas)
        if reasons:
            models = tuple(
                ShadowModelEvaluation(
                    model_kind=kind,
                    state=ShadowResultState.BLOCKED_BY_DATA,
                    metrics=None,
                    reason_codes=reasons,
                )
                for kind in ShadowModelKind
            )
            return _study_result(
                scope,
                feature_names,
                train,
                validation,
                holdout,
                models,
                ShadowResultState.BLOCKED_BY_DATA,
                reasons,
            )

        labels = tuple(int(item.useful) for item in train)
        train_rows = _matrix(train, feature_names)
        validation_rows = _matrix(validation, feature_names)
        holdout_rows = _matrix(holdout, feature_names)
        seed = _study_seed(scope.key, self._policy.seed)
        # Beta(1, 1) smoothing prevents small samples from receiving a
        # misleading probability of exactly zero or one.
        train_rate = (sum(labels) + 1.0) / (len(labels) + 2.0)
        models: list[ShadowModelEvaluation] = []
        for model_kind in ShadowModelKind:
            if model_kind is ShadowModelKind.SCIENTIFIC_RULE:
                # Every row in this dataset is an event already matched by the
                # pre-registered scientific rule.  The rule is therefore the
                # mandatory deterministic reference and acts on every row.
                validation_probabilities = tuple(1.0 for _ in validation)
                holdout_probabilities = tuple(1.0 for _ in holdout)
                threshold = 0.5
            elif model_kind is ShadowModelKind.BASE_RATE:
                validation_raw = tuple(train_rate for _ in validation)
                holdout_raw = tuple(train_rate for _ in holdout)
            else:
                estimator = self._estimators.fit(
                    model_kind=model_kind,
                    feature_names=feature_names,
                    rows=train_rows,
                    labels=labels,
                    seed=seed,
                )
                if estimator is None:
                    models.append(
                        ShadowModelEvaluation(
                            model_kind=model_kind,
                            state=ShadowResultState.BLOCKED_BY_DATA,
                            metrics=None,
                            reason_codes=("model_dependency_or_fit_unavailable",),
                        )
                    )
                    continue
                validation_raw = _checked_probabilities(
                    estimator.predict_probabilities(validation_rows), len(validation)
                )
                holdout_raw = _checked_probabilities(
                    estimator.predict_probabilities(holdout_rows), len(holdout)
                )
            if model_kind is not ShadowModelKind.SCIENTIFIC_RULE:
                calibrator = _fit_calibrator(
                    validation_raw,
                    tuple(int(item.useful) for item in validation),
                    self._policy.calibration_bins,
                )
                validation_probabilities = calibrator.apply(validation_raw)
                holdout_probabilities = calibrator.apply(holdout_raw)
                threshold = _select_action_threshold(
                    validation,
                    validation_probabilities,
                    thresholds=self._policy.candidate_action_thresholds,
                    minimum_coverage=self._policy.minimum_model_coverage,
                    calibration_bins=self._policy.calibration_bins,
                )
            validation_metrics = _metrics(
                validation,
                validation_probabilities,
                threshold=threshold,
                calibration_bins=self._policy.calibration_bins,
            )
            holdout_metrics = _metrics(
                holdout,
                holdout_probabilities,
                threshold=threshold,
                calibration_bins=self._policy.calibration_bins,
            )
            positive_blocks, total_blocks = _temporal_stability(
                holdout,
                holdout_probabilities,
                threshold=threshold,
                blocks=self._policy.stability_blocks,
            )
            models.append(
                ShadowModelEvaluation(
                    model_kind=model_kind,
                    state=ShadowResultState.READY,
                    metrics=holdout_metrics,
                    reason_codes=(),
                    validation_metrics=validation_metrics,
                    action_probability_threshold=threshold,
                    holdout_positive_stability_blocks=positive_blocks,
                    holdout_total_stability_blocks=total_blocks,
                )
            )
        study_state = (
            ShadowResultState.READY
            if all(item.state is ShadowResultState.READY for item in models)
            else ShadowResultState.BLOCKED_BY_DATA
        )
        study_reasons = (
            ()
            if study_state is ShadowResultState.READY
            else ("one_or_more_models_blocked_by_data",)
        )
        selection_state, selected_model_kind, selection_reasons = _select_model(
            models,
            minimum_coverage=self._policy.minimum_model_coverage,
            minimum_complexity_improvement=(
                self._policy.minimum_complexity_useful_rate_improvement
            ),
            minimum_positive_stability_fraction=(
                self._policy.minimum_positive_stability_fraction
            ),
        )
        return _study_result(
            scope,
            feature_names,
            train,
            validation,
            holdout,
            tuple(models),
            study_state,
            study_reasons,
            selection_state=selection_state,
            selected_model_kind=selected_model_kind,
            selection_reason_codes=selection_reasons,
        )

    def _blocked_reasons(
        self,
        ordered: Sequence[ShadowModelExample],
        train: Sequence[ShadowModelExample],
        validation: Sequence[ShadowModelExample],
        holdout: Sequence[ShadowModelExample],
        schemas: set[tuple[str, ...]],
    ) -> tuple[str, ...]:
        reasons: list[str] = []
        if not ordered:
            reasons.append("no_sealed_examples")
        if len(schemas) > 1:
            reasons.append("feature_schema_drift")
        if len(train) < self._policy.minimum_train_examples:
            reasons.append("minimum_train_examples_not_met")
        if len(validation) < self._policy.minimum_validation_examples:
            reasons.append("minimum_validation_examples_not_met")
        if len(holdout) < self._policy.minimum_holdout_examples:
            reasons.append("minimum_holdout_examples_not_met")
        if (
            len({item.trading_day for item in ordered})
            < self._policy.minimum_total_trading_days
        ):
            reasons.append("minimum_total_trading_days_not_met")
        if (
            len({item.trading_day for item in holdout})
            < self._policy.minimum_holdout_trading_days
        ):
            reasons.append("minimum_holdout_trading_days_not_met")
        if len({item.useful for item in train}) < 2:
            reasons.append("train_requires_both_labels")
        return tuple(reasons)


def _study_result(
    scope,
    feature_names: tuple[str, ...],
    train: Sequence[ShadowModelExample],
    validation: Sequence[ShadowModelExample],
    holdout: Sequence[ShadowModelExample],
    models: tuple[ShadowModelEvaluation, ...],
    state: ShadowResultState,
    reasons: tuple[str, ...],
    *,
    selection_state: ShadowSelectionState = ShadowSelectionState.ABSTAIN,
    selected_model_kind: ShadowModelKind | None = None,
    selection_reason_codes: tuple[str, ...] = (),
) -> ShadowStudyResult:
    if selection_state is ShadowSelectionState.ABSTAIN and not selection_reason_codes:
        selection_reason_codes = reasons or ("model_comparison_blocked",)
    return ShadowStudyResult(
        scope=scope,
        state=state,
        train_examples=len(train),
        validation_examples=len(validation),
        holdout_examples=len(holdout),
        train_days=len({item.trading_day for item in train}),
        validation_days=len({item.trading_day for item in validation}),
        holdout_days=len({item.trading_day for item in holdout}),
        feature_names=feature_names,
        models=models,
        reason_codes=reasons,
        selection_state=selection_state,
        selected_model_kind=selected_model_kind,
        selection_reason_codes=selection_reason_codes,
    )


def _select_action_threshold(
    examples: Sequence[ShadowModelExample],
    probabilities: Sequence[float],
    *,
    thresholds: Sequence[float],
    minimum_coverage: float,
    calibration_bins: int,
) -> float:
    """Choose abstention on validation only; the holdout remains unopened."""

    candidates: list[tuple[float, ShadowModelMetrics]] = []
    for threshold in thresholds:
        metrics = _metrics(
            examples,
            probabilities,
            threshold=threshold,
            calibration_bins=calibration_bins,
        )
        if metrics.coverage >= minimum_coverage and metrics.acted_observations:
            candidates.append((threshold, metrics))
    if not candidates:
        return float(thresholds[0])
    return max(
        candidates,
        key=lambda item: (
            item[1].useful_rate_when_acted or 0.0,
            item[1].mean_effect_when_acted or float("-inf"),
            item[1].coverage,
            -item[0],
        ),
    )[0]


def _select_model(
    models: Sequence[ShadowModelEvaluation],
    *,
    minimum_coverage: float,
    minimum_complexity_improvement: float,
    minimum_positive_stability_fraction: float,
) -> tuple[ShadowSelectionState, ShadowModelKind | None, tuple[str, ...]]:
    """Keep the least complex stable model unless complexity earns its place."""

    ordered = (
        ShadowModelKind.SCIENTIFIC_RULE,
        ShadowModelKind.BASE_RATE,
        ShadowModelKind.LOGISTIC_REGRESSION,
        ShadowModelKind.GRADIENT_BOOSTING,
    )
    by_kind = {model.model_kind: model for model in models}
    stable: list[ShadowModelEvaluation] = []
    for index, kind in enumerate(ordered):
        candidate = by_kind.get(kind)
        if candidate is None or not _is_stable_candidate(
            candidate,
            minimum_coverage=minimum_coverage,
            minimum_positive_stability_fraction=(minimum_positive_stability_fraction),
        ):
            continue
        simpler_ready = tuple(
            by_kind[simpler]
            for simpler in ordered[:index]
            if simpler in by_kind
            and by_kind[simpler].state is ShadowResultState.READY
            and by_kind[simpler].metrics is not None
            and by_kind[simpler].validation_metrics is not None
            and by_kind[simpler].metrics.acted_observations > 0
            and by_kind[simpler].validation_metrics.acted_observations > 0
        )
        if simpler_ready and not all(
            _earns_additional_complexity(
                candidate,
                reference,
                minimum_improvement=minimum_complexity_improvement,
            )
            for reference in simpler_ready
        ):
            continue
        stable.append(candidate)
    if not stable:
        return (
            ShadowSelectionState.ABSTAIN,
            None,
            ("no_model_stable_on_validation_and_holdout",),
        )

    selected = stable[0]
    complexity_won = selected.model_kind is not ShadowModelKind.SCIENTIFIC_RULE
    for candidate in stable[1:]:
        if _earns_additional_complexity(
            candidate,
            selected,
            minimum_improvement=minimum_complexity_improvement,
        ):
            selected = candidate
            complexity_won = True
    return (
        ShadowSelectionState.SELECTED,
        selected.model_kind,
        (
            "complexity_selected_after_stable_improvement"
            if complexity_won
            else "simplest_stable_candidate_selected",
        ),
    )


def _is_stable_candidate(
    model: ShadowModelEvaluation,
    *,
    minimum_coverage: float,
    minimum_positive_stability_fraction: float,
) -> bool:
    if (
        model.state is not ShadowResultState.READY
        or model.metrics is None
        or model.validation_metrics is None
        or model.holdout_positive_stability_blocks is None
        or model.holdout_total_stability_blocks is None
        or model.holdout_total_stability_blocks == 0
        or model.holdout_positive_stability_blocks
        < ceil(
            model.holdout_total_stability_blocks * minimum_positive_stability_fraction
        )
    ):
        return False
    for metrics in (model.validation_metrics, model.metrics):
        if (
            metrics.coverage < minimum_coverage
            or not metrics.acted_observations
            or metrics.useful_rate_when_acted is None
            or metrics.useful_rate_when_acted < 0.5
            or metrics.mean_effect_when_acted is None
            or metrics.mean_effect_when_acted <= 0.0
        ):
            return False
    return True


def _temporal_stability(
    examples: Sequence[ShadowModelExample],
    probabilities: Sequence[float],
    *,
    threshold: float,
    blocks: int,
) -> tuple[int, int]:
    """Measure late-period stability without using it to fit or recalibrate."""

    checked = _checked_probabilities(probabilities, len(examples))
    days = tuple(sorted({item.trading_day for item in examples}))
    total_blocks = min(blocks, len(days))
    if not total_blocks:
        return 0, 0
    day_block = {
        day: min(index * total_blocks // len(days), total_blocks - 1)
        for index, day in enumerate(days)
    }
    selected: list[list[ShadowModelExample]] = [[] for _ in range(total_blocks)]
    for example, probability in zip(examples, checked, strict=True):
        if probability >= threshold:
            selected[day_block[example.trading_day]].append(example)
    positive = sum(
        bool(rows)
        and sum(item.useful for item in rows) / len(rows) >= 0.5
        and sum(item.effect_value for item in rows) / len(rows) > 0.0
        for rows in selected
    )
    return positive, total_blocks


def _earns_additional_complexity(
    candidate: ShadowModelEvaluation,
    reference: ShadowModelEvaluation,
    *,
    minimum_improvement: float,
) -> bool:
    if (
        candidate.metrics is None
        or candidate.validation_metrics is None
        or reference.metrics is None
        or reference.validation_metrics is None
    ):
        return False
    for candidate_metrics, reference_metrics in (
        (candidate.validation_metrics, reference.validation_metrics),
        (candidate.metrics, reference.metrics),
    ):
        if (
            candidate_metrics.useful_rate_when_acted is None
            or reference_metrics.useful_rate_when_acted is None
            or candidate_metrics.mean_effect_when_acted is None
            or reference_metrics.mean_effect_when_acted is None
            or candidate_metrics.useful_rate_when_acted
            < reference_metrics.useful_rate_when_acted + minimum_improvement
            or candidate_metrics.mean_effect_when_acted
            < reference_metrics.mean_effect_when_acted
            or candidate_metrics.brier_score > reference_metrics.brier_score
        ):
            return False
    return True


def _matrix(
    examples: Sequence[ShadowModelExample], feature_names: tuple[str, ...]
) -> tuple[FeatureRow, ...]:
    return tuple(
        tuple(dict(item.feature_values)[name] for name in feature_names)
        for item in examples
    )


def _checked_probabilities(values: Sequence[float], expected: int) -> tuple[float, ...]:
    result = tuple(float(value) for value in values)
    if len(result) != expected:
        raise ValueError("shadow estimator returned wrong probability count")
    if any(value < 0.0 or value > 1.0 for value in result):
        raise ValueError("shadow estimator returned invalid probability")
    return result


def _fit_calibrator(
    probabilities: Sequence[float], labels: Sequence[int], bins: int
) -> _CalibrationMap:
    values = _checked_probabilities(probabilities, len(labels))
    if not values:
        raise ValueError("shadow calibration requires validation observations")
    grouped: list[list[int]] = [[] for _ in range(bins)]
    for probability, label in zip(values, labels, strict=True):
        grouped[min(int(probability * bins), bins - 1)].append(label)
    fallback = (sum(labels) + 1.0) / (len(labels) + 2.0)
    return _CalibrationMap(
        bins=bins,
        values=tuple(
            (sum(group) + 1.0) / (len(group) + 2.0) if group else fallback
            for group in grouped
        ),
    )


def _metrics(
    examples: Sequence[ShadowModelExample],
    probabilities: Sequence[float],
    *,
    threshold: float,
    calibration_bins: int,
) -> ShadowModelMetrics:
    checked = _checked_probabilities(probabilities, len(examples))
    labels = tuple(float(item.useful) for item in examples)
    acted = tuple(
        item
        for item, probability in zip(examples, checked, strict=True)
        if probability >= threshold
    )
    calibration = _calibration(checked, labels, calibration_bins)
    expected_error = sum(
        item.observations
        / len(examples)
        * abs((item.mean_probability or 0.0) - (item.observed_useful_rate or 0.0))
        for item in calibration
    )
    return ShadowModelMetrics(
        observations=len(examples),
        acted_observations=len(acted),
        accuracy=sum(
            int((probability >= 0.5) == item.useful)
            for item, probability in zip(examples, checked, strict=True)
        )
        / len(examples),
        coverage=len(acted) / len(examples),
        abstention_rate=1.0 - len(acted) / len(examples),
        useful_rate_when_acted=(
            sum(item.useful for item in acted) / len(acted) if acted else None
        ),
        mean_effect_when_acted=(
            sum(item.effect_value for item in acted) / len(acted) if acted else None
        ),
        brier_score=sum(
            (probability - label) ** 2
            for probability, label in zip(checked, labels, strict=True)
        )
        / len(examples),
        expected_calibration_error=expected_error,
        calibration=calibration,
    )


def _calibration(
    probabilities: Sequence[float], labels: Sequence[float], bins: int
) -> tuple[ShadowCalibrationBin, ...]:
    grouped: list[list[tuple[float, float]]] = [[] for _ in range(bins)]
    for probability, label in zip(probabilities, labels, strict=True):
        grouped[min(int(probability * bins), bins - 1)].append((probability, label))
    return tuple(
        ShadowCalibrationBin(
            lower_probability=index / bins,
            upper_probability=(index + 1) / bins,
            observations=len(values),
            mean_probability=(
                sum(value[0] for value in values) / len(values) if values else None
            ),
            observed_useful_rate=(
                sum(value[1] for value in values) / len(values) if values else None
            ),
        )
        for index, values in enumerate(grouped)
    )


def shadow_input_fingerprint(dataset: SealedShadowDataset) -> str:
    return _fingerprint(
        {
            "dataset_fingerprint": dataset.dataset_fingerprint,
            "source_artifact_fingerprints": dataset.source_artifact_fingerprints,
            "scopes": [asdict(scope) for scope in dataset.scopes],
            "examples": [asdict(example) for example in dataset.examples],
        }
    )


def shadow_policy_fingerprint(policy: ShadowComparisonPolicy) -> str:
    return _fingerprint(asdict(policy))


def _study_seed(identity: tuple[str, str, int], seed: int) -> int:
    digest = sha256(
        json.dumps((identity, seed), separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return int(digest[:8], 16)


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value,
        default=_json_default,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _json_default(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[no-any-return,union-attr]
    if hasattr(value, "value"):
        return value.value  # type: ignore[no-any-return,union-attr]
    raise TypeError(f"unsupported fingerprint value: {type(value)!r}")
