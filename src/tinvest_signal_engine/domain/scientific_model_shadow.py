"""Pure contracts for a non-causal shadow comparison of sealed models.

The comparison may rank abstention policies, but it cannot replace or relax the
causal evidence gate used by the scientific hypothesis portfolio.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from math import isfinite


class ShadowStudyKind(str, Enum):
    HYPOTHESIS = "hypothesis"
    COMBINATION = "combination"


class ShadowModelKind(str, Enum):
    SCIENTIFIC_RULE = "scientific_rule"
    BASE_RATE = "base_rate"
    LOGISTIC_REGRESSION = "logistic_regression"
    GRADIENT_BOOSTING = "gradient_boosting"


class ShadowResultState(str, Enum):
    READY = "ready"
    BLOCKED_BY_DATA = "blocked_by_data"


class ShadowSelectionState(str, Enum):
    SELECTED = "selected"
    ABSTAIN = "abstain"


@dataclass(frozen=True, slots=True)
class ShadowStudyScope:
    study_id: str
    study_version: str
    study_kind: ShadowStudyKind
    horizon_seconds: int
    effect_unit: str
    cost_model_version: str
    costs_applied: bool

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.study_id,
                self.study_version,
                self.effect_unit,
                self.cost_model_version,
            )
        ):
            raise ValueError("shadow study scope identity must not be empty")
        if self.horizon_seconds <= 0:
            raise ValueError("shadow study horizon must be positive")

    @property
    def key(self) -> tuple[str, str, int]:
        return self.study_id, self.study_version, self.horizon_seconds


@dataclass(frozen=True, slots=True)
class ShadowModelExample:
    scope: ShadowStudyScope
    observation_id: str
    instrument_id: str
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    feature_values: tuple[tuple[str, float], ...]
    effect_value: float

    def __post_init__(self) -> None:
        if not self.observation_id.strip() or not self.instrument_id.strip():
            raise ValueError("shadow example identity must not be empty")
        for name, value in (
            ("observed_at", self.observed_at),
            ("feature_max_observed_at", self.feature_max_observed_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("shadow feature uses future data")
        if not self.feature_values:
            raise ValueError("shadow feature values must not be empty")
        names = tuple(name for name, _ in self.feature_values)
        if names != tuple(sorted(names)) or len(names) != len(set(names)):
            raise ValueError("shadow feature names must be sorted and unique")
        if any(not name.strip() for name in names):
            raise ValueError("shadow feature names must not be empty")
        if any(not isfinite(value) for _, value in self.feature_values):
            raise ValueError("shadow feature values must be finite")
        if not isfinite(self.effect_value):
            raise ValueError("shadow effect must be finite")

    @property
    def useful(self) -> bool:
        return self.effect_value > 0.0


@dataclass(frozen=True, slots=True)
class SealedShadowDataset:
    dataset_fingerprint: str
    source_artifact_fingerprints: tuple[str, ...]
    scopes: tuple[ShadowStudyScope, ...]
    examples: tuple[ShadowModelExample, ...]

    def __post_init__(self) -> None:
        fingerprints = (self.dataset_fingerprint, *self.source_artifact_fingerprints)
        if any(not value.startswith("sha256:") for value in fingerprints):
            raise ValueError("shadow dataset fingerprints must use sha256")
        if not self.source_artifact_fingerprints:
            raise ValueError("shadow dataset requires sealed source artifacts")
        if len(set(self.source_artifact_fingerprints)) != len(
            self.source_artifact_fingerprints
        ):
            raise ValueError("shadow source fingerprints must be unique")
        keys = tuple(scope.key for scope in self.scopes)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("shadow scopes must be sorted and unique")
        allowed = set(keys)
        if any(example.scope.key not in allowed for example in self.examples):
            raise ValueError("shadow example is outside the sealed scope")
        identities = tuple(example.observation_id for example in self.examples)
        if len(identities) != len(set(identities)):
            raise ValueError("shadow observation identities must be unique")


@dataclass(frozen=True, slots=True)
class ShadowCalibrationBin:
    lower_probability: float
    upper_probability: float
    observations: int
    mean_probability: float | None
    observed_useful_rate: float | None

    def __post_init__(self) -> None:
        if not 0.0 <= self.lower_probability <= self.upper_probability <= 1.0:
            raise ValueError("shadow calibration bounds must be in [0, 1]")
        if self.observations < 0:
            raise ValueError("shadow calibration count must not be negative")
        values = (self.mean_probability, self.observed_useful_rate)
        if self.observations == 0 and any(value is not None for value in values):
            raise ValueError("empty shadow calibration bin must not carry values")
        if self.observations > 0 and any(value is None for value in values):
            raise ValueError("non-empty shadow calibration bin requires values")


@dataclass(frozen=True, slots=True)
class ShadowModelMetrics:
    observations: int
    acted_observations: int
    accuracy: float
    coverage: float
    abstention_rate: float
    useful_rate_when_acted: float | None
    mean_effect_when_acted: float | None
    brier_score: float
    expected_calibration_error: float
    calibration: tuple[ShadowCalibrationBin, ...]

    def __post_init__(self) -> None:
        if (
            self.observations < 0
            or not 0 <= self.acted_observations <= self.observations
        ):
            raise ValueError("shadow metric counts are invalid")
        probabilities = (
            self.accuracy,
            self.coverage,
            self.abstention_rate,
            self.brier_score,
            self.expected_calibration_error,
        )
        if any(not 0.0 <= value <= 1.0 for value in probabilities):
            raise ValueError("shadow probability metrics must be in [0, 1]")
        if abs(self.coverage + self.abstention_rate - 1.0) > 1e-12:
            raise ValueError("shadow coverage and abstention must sum to one")
        selected = (self.useful_rate_when_acted, self.mean_effect_when_acted)
        if self.acted_observations == 0 and any(
            value is not None for value in selected
        ):
            raise ValueError("empty shadow selection must not carry selected metrics")
        if self.acted_observations > 0 and any(value is None for value in selected):
            raise ValueError("shadow selection requires selected metrics")


@dataclass(frozen=True, slots=True)
class ShadowModelEvaluation:
    model_kind: ShadowModelKind
    state: ShadowResultState
    metrics: ShadowModelMetrics | None
    reason_codes: tuple[str, ...]
    validation_metrics: ShadowModelMetrics | None = None
    action_probability_threshold: float | None = None
    holdout_positive_stability_blocks: int | None = None
    holdout_total_stability_blocks: int | None = None

    def __post_init__(self) -> None:
        if self.state is ShadowResultState.READY and self.metrics is None:
            raise ValueError("ready shadow model requires metrics")
        if self.state is ShadowResultState.BLOCKED_BY_DATA and not self.reason_codes:
            raise ValueError("blocked shadow model requires a reason")
        if self.state is ShadowResultState.READY and self.validation_metrics is None:
            raise ValueError("ready shadow model requires validation metrics")
        if (
            self.state is ShadowResultState.READY
            and self.action_probability_threshold is None
        ):
            raise ValueError("ready shadow model requires an action threshold")
        if (
            self.action_probability_threshold is not None
            and not 0.0 <= self.action_probability_threshold <= 1.0
        ):
            raise ValueError("shadow action threshold must be in [0, 1]")
        stability = (
            self.holdout_positive_stability_blocks,
            self.holdout_total_stability_blocks,
        )
        if self.state is ShadowResultState.READY and any(
            value is None for value in stability
        ):
            raise ValueError("ready shadow model requires temporal stability")
        if self.state is ShadowResultState.READY and not (
            0
            <= (self.holdout_positive_stability_blocks or 0)
            <= (self.holdout_total_stability_blocks or 0)
        ):
            raise ValueError("shadow temporal stability counts are invalid")


@dataclass(frozen=True, slots=True)
class ShadowStudyResult:
    scope: ShadowStudyScope
    state: ShadowResultState
    train_examples: int
    validation_examples: int
    holdout_examples: int
    train_days: int
    validation_days: int
    holdout_days: int
    feature_names: tuple[str, ...]
    models: tuple[ShadowModelEvaluation, ...]
    reason_codes: tuple[str, ...]
    selection_state: ShadowSelectionState = ShadowSelectionState.ABSTAIN
    selected_model_kind: ShadowModelKind | None = None
    selection_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            self.selection_state is ShadowSelectionState.SELECTED
            and self.selected_model_kind is None
        ):
            raise ValueError("selected shadow study requires a model")
        if (
            self.selection_state is ShadowSelectionState.ABSTAIN
            and self.selected_model_kind is not None
        ):
            raise ValueError("abstaining shadow study cannot select a model")
        if (
            self.selection_state is ShadowSelectionState.ABSTAIN
            and not self.selection_reason_codes
        ):
            raise ValueError("abstaining shadow study requires a reason")


@dataclass(frozen=True, slots=True)
class ShadowPortfolioResult:
    run_id: str
    input_fingerprint: str
    policy_fingerprint: str
    state: ShadowResultState
    results: tuple[ShadowStudyResult, ...]
    missing_study_ids: tuple[str, ...]
    causal_evidence_gate_unchanged: bool = True
    claim_allowed: bool = False

    def __post_init__(self) -> None:
        for value in (self.run_id, self.input_fingerprint, self.policy_fingerprint):
            if not value.startswith("sha256:"):
                raise ValueError("shadow result fingerprints must use sha256")
        if not self.causal_evidence_gate_unchanged or self.claim_allowed:
            raise ValueError("shadow comparison must not change causal product claims")
