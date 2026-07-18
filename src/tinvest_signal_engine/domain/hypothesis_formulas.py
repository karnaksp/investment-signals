"""Safe deterministic executable contracts for preregistered hypotheses H1-H7."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from typing import Iterable

from tinvest_signal_engine.domain.trading_phases import TradingPhase


class HypothesisId(str, Enum):
    H1 = "H1"
    H2 = "H2"
    H3 = "H3"
    H4 = "H4"
    H5 = "H5"
    H6 = "H6"
    H7 = "H7"


class FeatureName(str, Enum):
    PREVIOUS_CLOSE = "previous_close"
    EVENT_PRICE = "event_price"
    MORNING_DEVIATION_Z = "morning_deviation_z"
    CUMULATIVE_RELATIVE_VOLUME = "cumulative_relative_volume"
    RANGE_PERCENTILE = "range_percentile"
    FIVE_MINUTE_RETURN_BPS = "five_minute_return_bps"
    FIVE_MINUTE_MOVE_PERCENTILE = "five_minute_move_percentile"
    RELATIVE_VOLUME_PERCENTILE = "relative_volume_percentile"
    ILLIQUIDITY_PERCENTILE = "illiquidity_percentile"
    MARKET_ALIGNMENT = "market_alignment"
    SAME_PHASE_MEAN_RETURN_BPS_20D = "same_phase_mean_return_bps_20d"
    SAME_PHASE_HISTORY_DAYS = "same_phase_history_days"
    OPENING_BASKET_RETURN_BPS = "opening_basket_return_bps"
    PHASE_VOLUME_PERCENTILE = "phase_volume_percentile"
    PHASE_HISTORY_DAYS = "phase_history_days"


class ExpectedEffect(str, Enum):
    REVERSAL = "reversal"
    CONTINUATION = "continuation"
    PHASE_REPEAT = "phase_repeat"
    MARKET_CONTINUATION = "market_continuation"
    ACTIVITY_UPLIFT = "activity_uplift"


class OutcomeAnchor(str, Enum):
    EVENT_TIME = "event_time"
    MAIN_SESSION_OPEN = "main_session_open"
    PRE_CLOSE_START = "pre_close_start"


class ObservationVerdict(str, Enum):
    MATCHED = "matched"
    NOT_MATCHED = "not_matched"
    ABSTAIN = "abstain"


class ObservationReason(str, Enum):
    CONDITIONS_MATCHED = "conditions_matched"
    CONDITIONS_NOT_MET = "conditions_not_met"
    OUTSIDE_PHASE = "outside_phase"
    MISSING_FEATURE = "missing_feature"
    FUTURE_FEATURE = "future_feature"
    TRADING_GAP = "trading_gap"
    MARKET_MOVE_CONFIRMS_EVENT = "market_move_confirms_event"
    INSUFFICIENT_HISTORY = "insufficient_history"
    DIRECTION_UNAVAILABLE = "direction_unavailable"


@dataclass(frozen=True, slots=True)
class ObservedFeature:
    name: FeatureName
    value: float
    observed_at: datetime
    window_start: datetime
    window_end: datetime

    def __post_init__(self) -> None:
        for value in (self.observed_at, self.window_start, self.window_end):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("feature timestamps must be timezone-aware")
        if self.window_start > self.window_end:
            raise ValueError("feature window_start must not be after window_end")

    def is_available_at(self, cutoff: datetime) -> bool:
        return self.observed_at <= cutoff and self.window_end <= cutoff


@dataclass(frozen=True, slots=True)
class HypothesisFeatureSet:
    values: tuple[ObservedFeature, ...]

    @classmethod
    def from_iterable(cls, values: Iterable[ObservedFeature]) -> "HypothesisFeatureSet":
        return cls(tuple(values))

    def __post_init__(self) -> None:
        names = tuple(item.name for item in self.values)
        if len(set(names)) != len(names):
            raise ValueError("feature set contains duplicate names")

    def get(self, name: FeatureName) -> ObservedFeature | None:
        return next((item for item in self.values if item.name is name), None)

    def unavailable_after(self, cutoff: datetime) -> tuple[FeatureName, ...]:
        return tuple(
            item.name for item in self.values if not item.is_available_at(cutoff)
        )


@dataclass(frozen=True, slots=True)
class HypothesisEvent:
    ticker: str
    event_at: datetime
    phase: TradingPhase
    has_trading_gap: bool = False

    def __post_init__(self) -> None:
        if not self.ticker.strip():
            raise ValueError("event ticker must not be empty")
        if self.event_at.tzinfo is None or self.event_at.utcoffset() is None:
            raise ValueError("event timestamp must be timezone-aware")


@dataclass(frozen=True, slots=True)
class HypothesisRule:
    hypothesis_id: HypothesisId
    version: str
    allowed_phases: tuple[TradingPhase, ...]
    required_features: tuple[FeatureName, ...]
    expected_effect: ExpectedEffect
    outcome_anchor: OutcomeAnchor
    horizons_seconds: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class HypothesisObservation:
    observation_id: str
    hypothesis_id: HypothesisId
    hypothesis_version: str
    ticker: str
    event_at: datetime
    phase: TradingPhase
    verdict: ObservationVerdict
    reason: ObservationReason
    expected_effect: ExpectedEffect
    expected_direction: int
    outcome_anchor: OutcomeAnchor
    horizons_seconds: tuple[int, ...]
    feature_cutoff_at: datetime


MORNING_PHASES = (TradingPhase.MORNING_LOW_LIQUIDITY,)
CONTINUOUS_PHASES = (
    TradingPhase.MAIN_OPENING,
    TradingPhase.MAIN_CONTINUOUS,
    TradingPhase.PRE_CLOSE,
)
ALL_RESEARCH_PHASES = MORNING_PHASES + CONTINUOUS_PHASES


HYPOTHESIS_RULES_V1 = (
    HypothesisRule(HypothesisId.H1, "1.0.0", MORNING_PHASES, (
        FeatureName.PREVIOUS_CLOSE, FeatureName.EVENT_PRICE,
        FeatureName.MORNING_DEVIATION_Z, FeatureName.CUMULATIVE_RELATIVE_VOLUME,
    ), ExpectedEffect.REVERSAL, OutcomeAnchor.MAIN_SESSION_OPEN, (1800, 3600)),
    HypothesisRule(HypothesisId.H2, "1.0.0", MORNING_PHASES, (
        FeatureName.PREVIOUS_CLOSE, FeatureName.EVENT_PRICE,
        FeatureName.MORNING_DEVIATION_Z, FeatureName.CUMULATIVE_RELATIVE_VOLUME,
        FeatureName.RANGE_PERCENTILE,
    ), ExpectedEffect.CONTINUATION, OutcomeAnchor.MAIN_SESSION_OPEN, (900, 1800, 3600)),
    HypothesisRule(HypothesisId.H3, "1.0.0", CONTINUOUS_PHASES, (
        FeatureName.FIVE_MINUTE_RETURN_BPS, FeatureName.FIVE_MINUTE_MOVE_PERCENTILE,
        FeatureName.RELATIVE_VOLUME_PERCENTILE, FeatureName.ILLIQUIDITY_PERCENTILE,
        FeatureName.MARKET_ALIGNMENT,
    ), ExpectedEffect.REVERSAL, OutcomeAnchor.EVENT_TIME, (300, 900, 1800)),
    HypothesisRule(HypothesisId.H4, "1.0.0", CONTINUOUS_PHASES, (
        FeatureName.FIVE_MINUTE_RETURN_BPS, FeatureName.FIVE_MINUTE_MOVE_PERCENTILE,
        FeatureName.RELATIVE_VOLUME_PERCENTILE, FeatureName.RANGE_PERCENTILE,
    ), ExpectedEffect.CONTINUATION, OutcomeAnchor.EVENT_TIME, (300, 900, 1800)),
    HypothesisRule(HypothesisId.H5, "1.0.0", ALL_RESEARCH_PHASES, (
        FeatureName.SAME_PHASE_MEAN_RETURN_BPS_20D,
        FeatureName.SAME_PHASE_HISTORY_DAYS,
    ), ExpectedEffect.PHASE_REPEAT, OutcomeAnchor.EVENT_TIME, (1800,)),
    HypothesisRule(HypothesisId.H6, "1.0.0", (
        TradingPhase.MAIN_CONTINUOUS, TradingPhase.PRE_CLOSE,
    ), (FeatureName.OPENING_BASKET_RETURN_BPS,), ExpectedEffect.MARKET_CONTINUATION,
        OutcomeAnchor.PRE_CLOSE_START, (1800,)),
    HypothesisRule(HypothesisId.H7, "1.0.0", ALL_RESEARCH_PHASES, (
        FeatureName.PHASE_VOLUME_PERCENTILE, FeatureName.PHASE_HISTORY_DAYS,
    ), ExpectedEffect.ACTIVITY_UPLIFT, OutcomeAnchor.EVENT_TIME, (900, 1800, 3600)),
)


def default_rule(hypothesis_id: HypothesisId) -> HypothesisRule:
    return next(rule for rule in HYPOTHESIS_RULES_V1 if rule.hypothesis_id is hypothesis_id)


def evaluate_hypothesis_rule(
    rule: HypothesisRule,
    event: HypothesisEvent,
    features: HypothesisFeatureSet,
) -> HypothesisObservation:
    """Evaluate only information sealed at ``event.event_at``.

    Thresholds are the preregistered first executable version.  A threshold
    change must create a new rule version instead of mutating this tuple.
    """

    reason = ObservationReason.CONDITIONS_MATCHED
    verdict = ObservationVerdict.MATCHED
    expected_direction = 0
    if event.phase not in rule.allowed_phases:
        verdict, reason = ObservationVerdict.ABSTAIN, ObservationReason.OUTSIDE_PHASE
    elif event.has_trading_gap:
        verdict, reason = ObservationVerdict.ABSTAIN, ObservationReason.TRADING_GAP
    elif features.unavailable_after(event.event_at):
        verdict, reason = ObservationVerdict.ABSTAIN, ObservationReason.FUTURE_FEATURE
    elif any(features.get(name) is None for name in rule.required_features):
        verdict, reason = ObservationVerdict.ABSTAIN, ObservationReason.MISSING_FEATURE
    else:
        verdict, reason, expected_direction = _evaluate_conditions(rule, features)
    observation_key = "|".join((
        rule.hypothesis_id.value,
        rule.version,
        event.ticker,
        event.event_at.isoformat(),
    ))
    return HypothesisObservation(
        observation_id=f"sha256:{sha256(observation_key.encode('utf-8')).hexdigest()}",
        hypothesis_id=rule.hypothesis_id,
        hypothesis_version=rule.version,
        ticker=event.ticker,
        event_at=event.event_at,
        phase=event.phase,
        verdict=verdict,
        reason=reason,
        expected_effect=rule.expected_effect,
        expected_direction=expected_direction if verdict is ObservationVerdict.MATCHED else 0,
        outcome_anchor=rule.outcome_anchor,
        horizons_seconds=rule.horizons_seconds,
        feature_cutoff_at=event.event_at,
    )


def _value(features: HypothesisFeatureSet, name: FeatureName) -> float:
    feature = features.get(name)
    if feature is None:
        raise ValueError(f"feature {name.value} is required")
    return feature.value


def _direction(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _evaluate_conditions(
    rule: HypothesisRule,
    features: HypothesisFeatureSet,
) -> tuple[ObservationVerdict, ObservationReason, int]:
    hypothesis_id = rule.hypothesis_id
    if hypothesis_id in (HypothesisId.H1, HypothesisId.H2):
        deviation = _value(features, FeatureName.EVENT_PRICE) - _value(features, FeatureName.PREVIOUS_CLOSE)
        direction = _direction(deviation)
        if direction == 0:
            return ObservationVerdict.ABSTAIN, ObservationReason.DIRECTION_UNAVAILABLE, 0
        deviation_z = abs(_value(features, FeatureName.MORNING_DEVIATION_Z))
        relative_volume = _value(features, FeatureName.CUMULATIVE_RELATIVE_VOLUME)
        if hypothesis_id is HypothesisId.H1:
            matched = deviation_z >= 2.0 and relative_volume <= 0.8
            expected = -direction
        else:
            matched = (
                deviation_z >= 2.0
                and relative_volume >= 1.5
                and _value(features, FeatureName.RANGE_PERCENTILE) >= 0.90
            )
            expected = direction
    elif hypothesis_id is HypothesisId.H3:
        direction = _direction(_value(features, FeatureName.FIVE_MINUTE_RETURN_BPS))
        if direction == 0:
            return ObservationVerdict.ABSTAIN, ObservationReason.DIRECTION_UNAVAILABLE, 0
        if _value(features, FeatureName.MARKET_ALIGNMENT) >= 0.5:
            return ObservationVerdict.ABSTAIN, ObservationReason.MARKET_MOVE_CONFIRMS_EVENT, 0
        matched = (
            _value(features, FeatureName.FIVE_MINUTE_MOVE_PERCENTILE) >= 0.99
            and _value(features, FeatureName.RELATIVE_VOLUME_PERCENTILE) < 0.50
            and _value(features, FeatureName.ILLIQUIDITY_PERCENTILE) >= 0.75
        )
        expected = -direction
    elif hypothesis_id is HypothesisId.H4:
        direction = _direction(_value(features, FeatureName.FIVE_MINUTE_RETURN_BPS))
        if direction == 0:
            return ObservationVerdict.ABSTAIN, ObservationReason.DIRECTION_UNAVAILABLE, 0
        matched = (
            _value(features, FeatureName.FIVE_MINUTE_MOVE_PERCENTILE) >= 0.99
            and _value(features, FeatureName.RELATIVE_VOLUME_PERCENTILE) >= 0.90
            and _value(features, FeatureName.RANGE_PERCENTILE) >= 0.90
        )
        expected = direction
    elif hypothesis_id is HypothesisId.H5:
        if _value(features, FeatureName.SAME_PHASE_HISTORY_DAYS) < 20:
            return ObservationVerdict.ABSTAIN, ObservationReason.INSUFFICIENT_HISTORY, 0
        expected = _direction(_value(features, FeatureName.SAME_PHASE_MEAN_RETURN_BPS_20D))
        if expected == 0:
            return ObservationVerdict.ABSTAIN, ObservationReason.DIRECTION_UNAVAILABLE, 0
        matched = True
    elif hypothesis_id is HypothesisId.H6:
        expected = _direction(_value(features, FeatureName.OPENING_BASKET_RETURN_BPS))
        if expected == 0:
            return ObservationVerdict.ABSTAIN, ObservationReason.DIRECTION_UNAVAILABLE, 0
        matched = abs(_value(features, FeatureName.OPENING_BASKET_RETURN_BPS)) >= 5.0
    else:
        if _value(features, FeatureName.PHASE_HISTORY_DAYS) < 20:
            return ObservationVerdict.ABSTAIN, ObservationReason.INSUFFICIENT_HISTORY, 0
        matched = _value(features, FeatureName.PHASE_VOLUME_PERCENTILE) >= 0.90
        expected = 0
    if matched:
        return ObservationVerdict.MATCHED, ObservationReason.CONDITIONS_MATCHED, expected
    return ObservationVerdict.NOT_MATCHED, ObservationReason.CONDITIONS_NOT_MET, 0
