"""Causal, research-only decisions over sealed scientific evidence.

The policy is deliberately narrower than a trading strategy.  Direction may
come only from the preregistered C5 agreement of H11V2 and H12V2.  H16V2 and
H17V2 are non-directional risk evidence: they can suppress a decision, never
create or reverse one.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from hashlib import sha256
import json
from math import isfinite

from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    ConfidenceInterval,
    DatasetPartition,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
    ScientificCombinationObservation,
    combination_formula_fingerprint,
)


class CausalSelectiveDecision(str, Enum):
    EXPECTED_UP = "expected_up"
    EXPECTED_DOWN = "expected_down"
    ABSTAIN = "abstain"


class CausalSelectiveReason(str, Enum):
    ELIGIBLE_C5_AGREEMENT = "eligible_c5_agreement"
    C5_MISSING = "c5_missing"
    C5_NOT_MATCHED = "c5_not_matched"
    C5_COMPONENT_MISSING = "c5_component_missing"
    C5_COMPONENT_ABSTAINED = "c5_component_abstained"
    DIRECTION_CONFLICT = "direction_conflict"
    DIRECTION_UNAVAILABLE = "direction_unavailable"
    RISK_EVIDENCE_MISSING = "risk_evidence_missing"
    RISK_EVIDENCE_STALE = "risk_evidence_stale"
    RISK_EVIDENCE_UNAVAILABLE = "risk_evidence_unavailable"
    H16V2_ELEVATED_RISK = "h16v2_elevated_risk"
    H17V2_ELEVATED_RISK = "h17v2_elevated_risk"
    INSUFFICIENT_TRAINING_SAMPLE = "insufficient_training_sample"
    CONFIDENCE_LOWER_BOUND_TOO_LOW = "confidence_lower_bound_too_low"
    TRAINING_NET_RESULT_NOT_POSITIVE = "training_net_result_not_positive"


@dataclass(frozen=True, slots=True)
class CausalSelectivePolicy:
    version: str = "causal-selective-c5-risk-v1.0.0"
    minimum_training_examples: int = 30
    minimum_confidence_lower_bound: float = 0.55
    minimum_mean_cost_adjusted_return_bps: float = 0.0
    maximum_risk_evidence_age_seconds: int = 1800
    success_threshold_bps: float = 0.0

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("selective policy version must not be empty")
        if self.minimum_training_examples <= 0:
            raise ValueError("minimum training examples must be positive")
        if not 0.0 <= self.minimum_confidence_lower_bound <= 1.0:
            raise ValueError("confidence lower bound must be in [0, 1]")
        if self.maximum_risk_evidence_age_seconds < 0:
            raise ValueError("risk evidence age must not be negative")
        values = (
            self.minimum_mean_cost_adjusted_return_bps,
            self.success_threshold_bps,
        )
        if any(not isfinite(value) for value in values):
            raise ValueError("selective policy thresholds must be finite")


def causal_selective_policy_fingerprint(
    policy: CausalSelectivePolicy,
) -> str:
    """Bind thresholds to the exact scientific formulas they govern."""

    encoded = json.dumps(
        {
            "implementation": "causal-selective-scientific-policy-v1",
            "policy": asdict(policy),
            "direction_source": {
                "combination_id": ScientificCombinationId.C5.value,
                "combination_version": ScientificCombinationId.C5.version,
                "formula_fingerprint": combination_formula_fingerprint(
                    ScientificCombinationId.C5
                ),
                "components": (
                    (
                        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2.value,
                        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2.version,
                    ),
                    (
                        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2.value,
                        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2.version,
                    ),
                ),
            },
            "risk_filters": (
                (
                    ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2.value,
                    ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2.version,
                ),
                (
                    ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2.value,
                    ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2.version,
                ),
            ),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return f"sha256:{sha256(encoded.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True, slots=True)
class CausalSelectiveContext:
    episode_id: str
    instrument_id: str
    primary_scope: str
    trading_day: date
    observed_at: datetime
    horizon_seconds: int

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (self.episode_id, self.instrument_id, self.primary_scope)
        ):
            raise ValueError("selective context identity must not be empty")
        _require_aware(self.observed_at, "observed_at")
        if self.horizon_seconds <= 0:
            raise ValueError("selective horizon must be positive")


@dataclass(frozen=True, slots=True)
class CausalSelectiveOutcome:
    episode_id: str
    target_at: datetime
    available: bool
    net_directional_return_bps: float | None
    cost_model_version: str

    def __post_init__(self) -> None:
        if not self.episode_id.strip() or not self.cost_model_version.strip():
            raise ValueError("selective outcome identity must not be empty")
        _require_aware(self.target_at, "target_at")
        if self.available != (self.net_directional_return_bps is not None):
            raise ValueError("available outcome must carry a net result")
        if (
            self.net_directional_return_bps is not None
            and not isfinite(self.net_directional_return_bps)
        ):
            raise ValueError("net directional return must be finite")


@dataclass(frozen=True, slots=True)
class CausalSelectiveEpisode:
    context: CausalSelectiveContext
    c5: ScientificCombinationObservation | None
    h16v2: ProspectiveFeature | None
    h17v2: ProspectiveFeature | None
    outcome: CausalSelectiveOutcome | None

    def __post_init__(self) -> None:
        context = self.context
        if self.c5 is not None:
            if self.c5.combination_id is not ScientificCombinationId.C5:
                raise ValueError("directional evidence must be C5")
            if (
                self.c5.observation_id != context.episode_id
                or self.c5.primary_scope != context.primary_scope
                or self.c5.trading_day != context.trading_day
                or self.c5.observed_at != context.observed_at
                or self.c5.horizon_seconds != context.horizon_seconds
            ):
                raise ValueError("C5 evidence does not match selective context")
        self._validate_risk(
            self.h16v2,
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
        )
        self._validate_risk(
            self.h17v2,
            ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2,
        )
        if self.outcome is not None:
            if self.outcome.episode_id != context.episode_id:
                raise ValueError("selective outcome does not match its episode")
            expected_target = context.observed_at + timedelta(
                seconds=context.horizon_seconds
            )
            if self.outcome.target_at != expected_target:
                raise ValueError("selective outcome target does not match the horizon")

    def _validate_risk(
        self,
        feature: ProspectiveFeature | None,
        expected: ProspectiveHypothesis,
    ) -> None:
        if feature is None:
            return
        context = self.context
        if feature.hypothesis is not expected:
            raise ValueError("unexpected risk hypothesis")
        if feature.ticker != context.instrument_id:
            raise ValueError("risk evidence instrument does not match C5")
        if feature.trading_day != context.trading_day:
            raise ValueError("risk evidence must belong to the same trading day")
        if (
            feature.observed_at > context.observed_at
            or feature.feature_max_observed_at > context.observed_at
        ):
            raise ValueError("selective risk evidence uses future data")
        if feature.expected_direction != 0:
            raise ValueError("risk evidence must not invent a direction")


@dataclass(frozen=True, slots=True)
class CausalTrainingConfidence:
    horizon_seconds: int
    examples: int
    successes: int
    success_interval: ConfidenceInterval
    mean_cost_adjusted_return_bps: float
    trained_until: datetime
    cost_model_version: str

    def __post_init__(self) -> None:
        if self.horizon_seconds <= 0 or self.examples <= 0:
            raise ValueError("training confidence requires positive sample counts")
        if not 0 <= self.successes <= self.examples:
            raise ValueError("training successes must fit inside the sample")
        _require_aware(self.trained_until, "trained_until")
        if not self.cost_model_version.strip():
            raise ValueError("training confidence requires a cost model")
        if not isfinite(self.mean_cost_adjusted_return_bps):
            raise ValueError("training mean net result must be finite")


@dataclass(frozen=True, slots=True)
class CausalSelectiveDecisionRecord:
    episode_id: str
    trading_day: date
    observed_at: datetime
    horizon_seconds: int
    decision: CausalSelectiveDecision
    reason_codes: tuple[CausalSelectiveReason, ...]
    policy_fingerprint: str
    confidence: CausalTrainingConfidence | None
    source_observation_ids: tuple[str, ...]
    risk_elevated: bool

    def __post_init__(self) -> None:
        if not self.episode_id.strip():
            raise ValueError("decision episode id must not be empty")
        _require_aware(self.observed_at, "observed_at")
        if self.horizon_seconds <= 0:
            raise ValueError("decision horizon must be positive")
        if not self.reason_codes:
            raise ValueError("every selective decision requires a reason code")
        if self.reason_codes != tuple(dict.fromkeys(self.reason_codes)):
            raise ValueError("selective reason codes must be unique and ordered")
        if not self.policy_fingerprint.startswith("sha256:"):
            raise ValueError("selective policy fingerprint must use sha256")
        if self.decision is not CausalSelectiveDecision.ABSTAIN and (
            self.reason_codes != (CausalSelectiveReason.ELIGIBLE_C5_AGREEMENT,)
            or self.confidence is None
            or self.risk_elevated
        ):
            raise ValueError("acted decisions require eligible C5 evidence")
        if self.decision is CausalSelectiveDecision.ABSTAIN and (
            CausalSelectiveReason.ELIGIBLE_C5_AGREEMENT in self.reason_codes
        ):
            raise ValueError("abstention cannot be marked eligible")


@dataclass(frozen=True, slots=True)
class CausalSelectivePartitionMetrics:
    partition: DatasetPartition
    observations: int
    acted_observations: int
    resolved_acted_outcomes: int
    correct_acted_outcomes: int
    coverage: float
    selective_accuracy: float | None
    mean_cost_adjusted_return_bps: float | None

    def __post_init__(self) -> None:
        counts = (
            self.observations,
            self.acted_observations,
            self.resolved_acted_outcomes,
            self.correct_acted_outcomes,
        )
        if any(value < 0 for value in counts):
            raise ValueError("selective metrics counts must not be negative")
        if not (
            self.correct_acted_outcomes
            <= self.resolved_acted_outcomes
            <= self.acted_observations
            <= self.observations
        ):
            raise ValueError("selective metrics counts are inconsistent")
        if not 0.0 <= self.coverage <= 1.0:
            raise ValueError("selective coverage must be in [0, 1]")
        expected_coverage = (
            self.acted_observations / self.observations
            if self.observations
            else 0.0
        )
        if abs(self.coverage - expected_coverage) > 1e-12:
            raise ValueError("selective coverage does not match counts")
        if self.resolved_acted_outcomes == 0:
            if (
                self.selective_accuracy is not None
                or self.mean_cost_adjusted_return_bps is not None
            ):
                raise ValueError("unresolved acted sample cannot carry performance")
        elif (
            self.selective_accuracy is None
            or self.mean_cost_adjusted_return_bps is None
        ):
            raise ValueError("resolved acted sample requires performance")
        elif not 0.0 <= self.selective_accuracy <= 1.0:
            raise ValueError("selective accuracy must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class CausalSelectiveReport:
    policy_version: str
    policy_fingerprint: str
    dataset_fingerprint: str
    report_fingerprint: str
    split: ChronologicalSplit
    cost_model_version: str | None
    decisions: tuple[CausalSelectiveDecisionRecord, ...]
    metrics: tuple[CausalSelectivePartitionMetrics, ...]
    product_claim_allowed: bool = False
    automatic_execution_allowed: bool = False

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("selective report policy version is required")
        for value in (
            self.policy_fingerprint,
            self.dataset_fingerprint,
            self.report_fingerprint,
        ):
            if not value.startswith("sha256:"):
                raise ValueError("selective report fingerprints must use sha256")
        if self.product_claim_allowed or self.automatic_execution_allowed:
            raise ValueError("research policy cannot claim or execute")
        if self.cost_model_version is not None and not self.cost_model_version.strip():
            raise ValueError("selective report cost model must not be blank")
        if tuple(item.partition for item in self.metrics) != tuple(DatasetPartition):
            raise ValueError("report must contain train, validation, and holdout metrics")


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
