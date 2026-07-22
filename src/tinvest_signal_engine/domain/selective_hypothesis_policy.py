"""Pure domain contracts for selective scientific-hypothesis decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from math import isfinite

from tinvest_signal_engine.domain.hypothesis_evidence import ConfidenceInterval


class SelectiveModelKind(str, Enum):
    SEALED_RULE = "sealed_rule"
    SMOOTHED_PROBABILITY = "smoothed_probability"
    LOGISTIC_REGRESSION = "logistic_regression"
    GRADIENT_BOOSTED_TREES = "gradient_boosted_trees"


class SelectiveResearchDecision(str, Enum):
    IMPROVED = "improved"
    NO_IMPROVEMENT = "no_improvement"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass(frozen=True)
class SelectiveExample:
    """One causal meta-label example; hypothesis direction remains sealed."""

    hypothesis_id: str
    hypothesis_version: str
    observation_id: str
    instrument_id: str
    horizon_seconds: int
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    feature_values: tuple[tuple[str, float], ...]
    cost_adjusted_result_bps: float
    cost_model_version: str
    probability_stratum: str = "all"

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.hypothesis_id,
                self.hypothesis_version,
                self.observation_id,
                self.instrument_id,
                self.cost_model_version,
                self.probability_stratum,
            )
        ):
            raise ValueError("selective example identity must not be empty")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("selective feature uses future data")
        if not self.feature_values:
            raise ValueError("feature_values must not be empty")
        names = tuple(name for name, _ in self.feature_values)
        if names != tuple(sorted(names)) or len(names) != len(set(names)):
            raise ValueError("feature names must be sorted and unique")
        if not all(isfinite(value) for _, value in self.feature_values):
            raise ValueError("feature values must be finite")
        if not isfinite(self.cost_adjusted_result_bps):
            raise ValueError("cost_adjusted_result_bps must be finite")

    @property
    def useful(self) -> bool:
        return self.cost_adjusted_result_bps > 0.0


@dataclass(frozen=True)
class ProbabilityPrediction:
    observation_id: str
    useful_probability: float

    def __post_init__(self) -> None:
        if not self.observation_id.strip():
            raise ValueError("prediction observation_id must not be empty")
        if not 0.0 <= self.useful_probability <= 1.0:
            raise ValueError("useful_probability must be between zero and one")


@dataclass(frozen=True)
class CalibrationBin:
    lower_probability: float
    upper_probability: float
    observations: int
    mean_probability: float | None
    observed_useful_rate: float | None

    def __post_init__(self) -> None:
        if not 0.0 <= self.lower_probability <= self.upper_probability <= 1.0:
            raise ValueError("calibration bounds must be between zero and one")
        if self.observations < 0:
            raise ValueError("calibration observations must not be negative")
        if self.observations == 0:
            if self.mean_probability is not None or self.observed_useful_rate is not None:
                raise ValueError("empty calibration bins must not carry estimates")
        elif self.mean_probability is None or self.observed_useful_rate is None:
            raise ValueError("non-empty calibration bins require estimates")


@dataclass(frozen=True)
class SelectiveMetrics:
    observations: int
    acted_observations: int
    coverage: float
    abstention_rate: float
    useful_rate_when_acted: float | None
    mean_cost_adjusted_result_bps: float | None
    brier_score: float
    expected_calibration_error: float
    calibration: tuple[CalibrationBin, ...]
    coverage_day_interval: ConfidenceInterval | None = None
    useful_rate_day_interval: ConfidenceInterval | None = None
    mean_cost_adjusted_result_day_interval: ConfidenceInterval | None = None

    def __post_init__(self) -> None:
        if self.observations < 0 or self.acted_observations < 0:
            raise ValueError("observation counts must not be negative")
        if self.acted_observations > self.observations:
            raise ValueError("acted observations cannot exceed observations")
        if not 0.0 <= self.coverage <= 1.0:
            raise ValueError("coverage must be between zero and one")
        if not 0.0 <= self.abstention_rate <= 1.0:
            raise ValueError("abstention_rate must be between zero and one")
        if abs(self.coverage + self.abstention_rate - 1.0) > 1e-12:
            raise ValueError("coverage and abstention_rate must sum to one")
        if not 0.0 <= self.brier_score <= 1.0:
            raise ValueError("brier_score must be between zero and one")
        if not 0.0 <= self.expected_calibration_error <= 1.0:
            raise ValueError("expected calibration error must be between zero and one")
        if self.acted_observations == 0 and (
            self.useful_rate_when_acted is not None
            or self.mean_cost_adjusted_result_bps is not None
        ):
            raise ValueError("empty selected sample must not carry acted metrics")
        if self.acted_observations > 0 and (
            self.useful_rate_when_acted is None
            or self.mean_cost_adjusted_result_bps is None
        ):
            raise ValueError("selected sample requires acted metrics")
        if self.observations == 0 and self.coverage_day_interval is not None:
            raise ValueError("empty sample must not carry a coverage interval")
        if self.acted_observations == 0 and (
            self.useful_rate_day_interval is not None
            or self.mean_cost_adjusted_result_day_interval is not None
        ):
            raise ValueError("empty selected sample must not carry selected intervals")


@dataclass(frozen=True)
class TuneCandidate:
    model_kind: SelectiveModelKind
    probability_threshold: float
    metrics: SelectiveMetrics
    lift_over_sealed_rule_bps: float | None
    eligible: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class SelectivePolicyResult:
    hypothesis_id: str
    hypothesis_version: str
    horizon_seconds: int
    feature_names: tuple[str, ...]
    cost_model_version: str
    train_examples: int
    tune_examples: int
    holdout_examples: int
    tune_candidates: tuple[TuneCandidate, ...]
    tune_selected_model: SelectiveModelKind
    tune_selected_threshold: float
    holdout_rule_metrics: SelectiveMetrics
    holdout_selected_metrics: SelectiveMetrics
    holdout_lift_over_rule_bps: float | None
    holdout_lift_interval: ConfidenceInterval | None
    decision: SelectiveResearchDecision
    deployment_model: SelectiveModelKind
    claim_allowed: bool
    hypothesis_changed: bool
    reason_codes: tuple[str, ...]
    total_examples: int = 0
    total_trading_days: int = 0
    complex_model_gate_passed: bool = False

    def __post_init__(self) -> None:
        if min(self.train_examples, self.tune_examples, self.holdout_examples) < 0:
            raise ValueError("partition example counts must not be negative")
        if self.total_examples < 0 or self.total_trading_days < 0:
            raise ValueError("total sample counts must not be negative")
        if self.hypothesis_changed:
            raise ValueError("selective research must not change the sealed hypothesis")
        if self.claim_allowed and self.decision is not SelectiveResearchDecision.IMPROVED:
            raise ValueError("claim is allowed only for independently improved policy")
        if (
            self.decision is not SelectiveResearchDecision.IMPROVED
            and self.deployment_model is not SelectiveModelKind.SEALED_RULE
        ):
            raise ValueError("failed research must retain the sealed rule")


@dataclass(frozen=True)
class SelectiveResearchPolicy:
    version: str = "selective-meta-policy-v2.0.0"
    probability_thresholds: tuple[float, ...] = (
        0.50,
        0.55,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
    )
    minimum_train_examples: int = 100
    minimum_tune_examples: int = 50
    minimum_holdout_examples: int = 50
    minimum_acted_examples: int = 30
    minimum_coverage: float = 0.10
    minimum_lift_bps: float = 1.0
    bootstrap_samples: int = 2_000
    bootstrap_seed: int = 41
    calibration_bins: int = 10
    minimum_complex_examples: int = 3_000
    minimum_complex_trading_days: int = 30
    minimum_stable_blocks: int = 4

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        if not self.probability_thresholds or any(
            value < 0.0 or value > 1.0 for value in self.probability_thresholds
        ):
            raise ValueError("probability thresholds must be between zero and one")
        if tuple(sorted(set(self.probability_thresholds))) != self.probability_thresholds:
            raise ValueError("probability thresholds must be sorted and unique")
        if min(
            self.minimum_train_examples,
            self.minimum_tune_examples,
            self.minimum_holdout_examples,
            self.minimum_acted_examples,
            self.bootstrap_samples,
            self.calibration_bins,
            self.minimum_complex_examples,
            self.minimum_complex_trading_days,
            self.minimum_stable_blocks,
        ) <= 0:
            raise ValueError("sample and calculation counts must be positive")
        if self.minimum_stable_blocks > 5:
            raise ValueError("minimum_stable_blocks must not exceed five")
        if not 0.0 < self.minimum_coverage <= 1.0:
            raise ValueError("minimum_coverage must be in (0, 1]")
        if self.minimum_lift_bps < 0.0:
            raise ValueError("minimum_lift_bps must not be negative")
