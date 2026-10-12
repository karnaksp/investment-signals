"""Pure contracts for an explainable selective scientific portfolio.

The selector is a shadow meta-policy.  It chooses among already sealed
scientific actions, but it cannot change a hypothesis, bypass its evidence
gate, or turn an abstention into a product claim.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from math import isfinite


class PortfolioAction(str, Enum):
    UP = "up"
    DOWN = "down"
    RISK = "risk"
    ABSTAIN = "abstain"


class PortfolioSelectorModel(str, Enum):
    FIXED_RULE = "fixed_rule"
    BAYESIAN_FREQUENCY = "bayesian_frequency"
    REGULARIZED_LOGISTIC = "regularized_logistic"


class PortfolioSelectorState(str, Enum):
    READY = "ready"
    BLOCKED_BY_DATA = "blocked_by_data"
    NO_STABLE_IMPROVEMENT = "no_stable_improvement"


@dataclass(frozen=True, slots=True)
class PortfolioSelectorExample:
    event_id: str
    instrument_id: str
    source_study_ids: tuple[str, ...]
    source_artifact_fingerprints: tuple[str, ...]
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    label_observed_at: datetime
    horizon_seconds: int
    sealed_action: PortfolioAction
    target_action: PortfolioAction
    probability_stratum: str
    feature_values: tuple[tuple[str, float], ...]
    cost_model_version: str

    def __post_init__(self) -> None:
        identities = (
            self.event_id,
            self.instrument_id,
            self.probability_stratum,
            self.cost_model_version,
        )
        if any(not item.strip() for item in identities):
            raise ValueError("portfolio selector identity must not be empty")
        if (
            not self.source_study_ids
            or any(not item.strip() for item in self.source_study_ids)
            or len(set(self.source_study_ids)) != len(self.source_study_ids)
        ):
            raise ValueError("portfolio selector requires unique scientific sources")
        if (
            not self.source_artifact_fingerprints
            or any(
                not item.startswith("sha256:")
                for item in self.source_artifact_fingerprints
            )
            or len(set(self.source_artifact_fingerprints))
            != len(self.source_artifact_fingerprints)
        ):
            raise ValueError("portfolio selector requires sealed source artifacts")
        if self.horizon_seconds <= 0:
            raise ValueError("portfolio selector horizon must be positive")
        for field, value in (
            ("observed_at", self.observed_at),
            ("feature_max_observed_at", self.feature_max_observed_at),
            ("label_observed_at", self.label_observed_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field} must be timezone-aware")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("portfolio selector feature uses future data")
        if self.label_observed_at < self.observed_at + timedelta(
            seconds=self.horizon_seconds
        ):
            raise ValueError("portfolio selector label is not mature")
        if not self.feature_values:
            raise ValueError("portfolio selector features must not be empty")
        names = tuple(name for name, _ in self.feature_values)
        if (
            names != tuple(sorted(names))
            or len(names) != len(set(names))
            or any(not name.strip() for name in names)
        ):
            raise ValueError("portfolio selector feature names must be sorted and unique")
        if any(not isfinite(value) for _, value in self.feature_values):
            raise ValueError("portfolio selector features must be finite")


@dataclass(frozen=True, slots=True)
class PortfolioCalibrationBin:
    lower_confidence: float
    upper_confidence: float
    observations: int
    mean_confidence: float | None
    observed_accuracy: float | None

    def __post_init__(self) -> None:
        if not 0.0 <= self.lower_confidence <= self.upper_confidence <= 1.0:
            raise ValueError("calibration bounds must be in [0, 1]")
        if self.observations < 0:
            raise ValueError("calibration observations must not be negative")
        values = self.mean_confidence, self.observed_accuracy
        if self.observations == 0 and any(item is not None for item in values):
            raise ValueError("empty calibration bin must not carry estimates")
        if self.observations > 0 and any(item is None for item in values):
            raise ValueError("non-empty calibration bin requires estimates")


@dataclass(frozen=True, slots=True)
class PortfolioSelectorMetrics:
    observations: int
    acted_observations: int
    correct_acted_observations: int
    accuracy_when_acted: float | None
    coverage: float
    abstention_rate: float
    multiclass_brier_score: float
    expected_calibration_error: float
    action_counts: tuple[tuple[PortfolioAction, int], ...]
    calibration: tuple[PortfolioCalibrationBin, ...]

    def __post_init__(self) -> None:
        if not (
            0
            <= self.correct_acted_observations
            <= self.acted_observations
            <= self.observations
        ):
            raise ValueError("portfolio selector metric counts are invalid")
        if not 0.0 <= self.coverage <= 1.0:
            raise ValueError("portfolio selector coverage must be in [0, 1]")
        if not 0.0 <= self.abstention_rate <= 1.0:
            raise ValueError("portfolio selector abstention must be in [0, 1]")
        if abs(self.coverage + self.abstention_rate - 1.0) > 1e-12:
            raise ValueError("portfolio selector coverage and abstention must sum to one")
        if not 0.0 <= self.multiclass_brier_score <= 1.0:
            raise ValueError("portfolio selector Brier score must be in [0, 1]")
        if not 0.0 <= self.expected_calibration_error <= 1.0:
            raise ValueError("portfolio selector calibration error must be in [0, 1]")
        if self.acted_observations == 0 and self.accuracy_when_acted is not None:
            raise ValueError("empty action sample must not carry accuracy")
        if self.acted_observations > 0 and self.accuracy_when_acted is None:
            raise ValueError("acted sample requires accuracy")
        if self.accuracy_when_acted is not None and not (
            0.0 <= self.accuracy_when_acted <= 1.0
        ):
            raise ValueError("portfolio selector accuracy must be in [0, 1]")
        actions = tuple(action for action, _ in self.action_counts)
        if actions != tuple(PortfolioAction) or any(
            count < 0 for _, count in self.action_counts
        ):
            raise ValueError("portfolio selector action counts are incomplete")
        if sum(count for _, count in self.action_counts) != self.observations:
            raise ValueError("portfolio selector action counts must cover observations")


@dataclass(frozen=True, slots=True)
class PortfolioTemporalSplit:
    train_days: tuple[date, ...]
    validation_days: tuple[date, ...]
    holdout_days: tuple[date, ...]
    embargo_days: tuple[date, ...]
    gap_trading_days: int

    def __post_init__(self) -> None:
        if self.gap_trading_days <= 0:
            raise ValueError("portfolio selector time gap must be positive")
        groups = (
            self.train_days,
            self.validation_days,
            self.holdout_days,
            self.embargo_days,
        )
        if any(days != tuple(sorted(set(days))) for days in groups):
            raise ValueError("portfolio selector split days must be sorted and unique")
        flattened = tuple(day for days in groups for day in days)
        if len(flattened) != len(set(flattened)):
            raise ValueError("portfolio selector split days must be disjoint")
        if self.train_days and self.validation_days and not (
            max(self.train_days) < min(self.validation_days)
        ):
            raise ValueError("validation must follow training")
        if self.validation_days and self.holdout_days and not (
            max(self.validation_days) < min(self.holdout_days)
        ):
            raise ValueError("holdout must follow validation")


@dataclass(frozen=True, slots=True)
class PortfolioWalkForwardFold:
    model_kind: PortfolioSelectorModel
    train_end: date
    evaluation_start: date
    train_days: int
    evaluation_days: int
    gap_days: int
    model_accuracy: float | None
    fixed_rule_accuracy: float | None
    coverage: float
    improvement: float | None
    positive: bool

    def __post_init__(self) -> None:
        if self.train_end >= self.evaluation_start:
            raise ValueError("walk-forward training must precede evaluation")
        if min(self.train_days, self.evaluation_days, self.gap_days) <= 0:
            raise ValueError("walk-forward day counts must be positive")
        for value in (
            self.model_accuracy,
            self.fixed_rule_accuracy,
            self.coverage,
        ):
            if value is not None and not 0.0 <= value <= 1.0:
                raise ValueError("walk-forward rates must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class PortfolioModelExplanation:
    model_kind: PortfolioSelectorModel
    terms: tuple[tuple[str, str, float], ...]

    def __post_init__(self) -> None:
        if any(
            not action.strip() or not feature.strip() or not isfinite(value)
            for action, feature, value in self.terms
        ):
            raise ValueError("portfolio model explanation terms are invalid")


@dataclass(frozen=True, slots=True)
class PortfolioModelEvaluation:
    model_kind: PortfolioSelectorModel
    validation_metrics: PortfolioSelectorMetrics
    holdout_metrics: PortfolioSelectorMetrics
    selected_confidence_threshold: float
    validation_accuracy_lift: float | None
    holdout_accuracy_lift: float | None
    positive_walk_forward_folds: int
    total_walk_forward_folds: int
    eligible: bool
    reason_codes: tuple[str, ...]
    walk_forward: tuple[PortfolioWalkForwardFold, ...]
    explanation: PortfolioModelExplanation

    def __post_init__(self) -> None:
        if not 0.0 <= self.selected_confidence_threshold <= 1.0:
            raise ValueError("portfolio selector threshold must be in [0, 1]")
        if not (
            0
            <= self.positive_walk_forward_folds
            <= self.total_walk_forward_folds
        ):
            raise ValueError("portfolio selector fold counts are invalid")
        if self.total_walk_forward_folds != len(self.walk_forward):
            raise ValueError("portfolio selector fold count must match details")
        if self.eligible and self.reason_codes:
            raise ValueError("eligible portfolio model cannot carry blockers")
        if not self.eligible and not self.reason_codes:
            raise ValueError("ineligible portfolio model requires blockers")


@dataclass(frozen=True, slots=True)
class PortfolioDecision:
    event_id: str
    model_kind: PortfolioSelectorModel
    action: PortfolioAction
    confidence: float
    reason_codes: tuple[str, ...]
    explanation: tuple[tuple[str, float], ...]

    def __post_init__(self) -> None:
        if not self.event_id.strip() or not self.reason_codes:
            raise ValueError("portfolio decision identity and reason are required")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("portfolio decision confidence must be in [0, 1]")
        if any(not name.strip() or not isfinite(value) for name, value in self.explanation):
            raise ValueError("portfolio decision explanation is invalid")


@dataclass(frozen=True, slots=True)
class ScientificPortfolioSelectorResult:
    run_id: str
    input_fingerprint: str
    policy_fingerprint: str
    state: PortfolioSelectorState
    split: PortfolioTemporalSplit
    feature_names: tuple[str, ...]
    cost_model_version: str
    examples: int
    trading_days: int
    evaluations: tuple[PortfolioModelEvaluation, ...]
    selected_model: PortfolioSelectorModel
    holdout_decisions: tuple[PortfolioDecision, ...]
    reason_codes: tuple[str, ...]
    causal_evidence_gate_unchanged: bool = True
    claim_allowed: bool = False

    def __post_init__(self) -> None:
        fingerprints = self.input_fingerprint, self.policy_fingerprint
        if (
            not self.run_id.startswith("sha256:")
            or any(not item.startswith("sha256:") for item in fingerprints)
        ):
            raise ValueError("portfolio selector identities must use sha256")
        if self.examples < 0 or self.trading_days < 0:
            raise ValueError("portfolio selector counts must not be negative")
        if not self.feature_names or any(not item.strip() for item in self.feature_names):
            raise ValueError("portfolio selector feature schema is required")
        if not self.cost_model_version.strip():
            raise ValueError("portfolio selector cost model is required")
        if tuple(item.model_kind for item in self.evaluations) != tuple(
            PortfolioSelectorModel
        ):
            raise ValueError("portfolio selector must evaluate all models")
        if self.state is not PortfolioSelectorState.READY and (
            self.selected_model is not PortfolioSelectorModel.FIXED_RULE
        ):
            raise ValueError("failed selector must retain the fixed scientific rule")
        if not self.causal_evidence_gate_unchanged or self.claim_allowed:
            raise ValueError("portfolio selector must remain shadow-only")
