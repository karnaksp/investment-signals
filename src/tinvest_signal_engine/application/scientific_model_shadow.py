"""Reproducible shadow comparison over already sealed scientific outcomes."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
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
            self.calibration_bins,
        )
        if any(value <= 0 for value in counts):
            raise ValueError("shadow sample requirements must be positive")
        if not 0.0 <= self.action_probability_threshold <= 1.0:
            raise ValueError("shadow action threshold must be in [0, 1]")


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
        train_rate = (sum(labels) + 1.0) / (len(labels) + 2.0)
        models: list[ShadowModelEvaluation] = []
        for model_kind in ShadowModelKind:
            if model_kind is ShadowModelKind.BASE_RATE:
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
            calibrator = _fit_calibrator(
                validation_raw,
                tuple(int(item.useful) for item in validation),
                self._policy.calibration_bins,
            )
            probabilities = calibrator.apply(holdout_raw)
            models.append(
                ShadowModelEvaluation(
                    model_kind=model_kind,
                    state=ShadowResultState.READY,
                    metrics=_metrics(
                        holdout,
                        probabilities,
                        threshold=self._policy.action_probability_threshold,
                        calibration_bins=self._policy.calibration_bins,
                    ),
                    reason_codes=(),
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
        return _study_result(
            scope,
            feature_names,
            train,
            validation,
            holdout,
            tuple(models),
            study_state,
            study_reasons,
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
) -> ShadowStudyResult:
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
    )


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
