"""Framework-independent signal self-evaluation rules."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick


DIRECTIONAL_VERDICTS = frozenset(
    {"confirmed", "contradicted", "insignificant", "inconclusive"}
)


class SignalOutcomeConflict(RuntimeError):
    """An immutable signal outcome key was reused with different content."""


@dataclass(frozen=True)
class DirectionalOutcomePolicy:
    min_move_bps: Decimal | float
    volatility_multiplier: Decimal | float
    round_trip_cost_bps: Decimal | float
    horizon_seconds: int = 300
    anchor_max_age_seconds: int = 5
    forward_grace_seconds: int = 30
    baseline_volatility_window_seconds: int = 60
    policy_version: str = "directional-outcome-v1"
    cost_model_version: str = "study-round-trip-cost-v1"

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be empty")
        if not self.cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.anchor_max_age_seconds < 0:
            raise ValueError("anchor_max_age_seconds must be non-negative")
        if self.forward_grace_seconds < 0:
            raise ValueError("forward_grace_seconds must be non-negative")
        if self.baseline_volatility_window_seconds <= 0:
            raise ValueError("baseline_volatility_window_seconds must be positive")
        if _as_decimal(self.min_move_bps) < 0:
            raise ValueError("min_move_bps must be non-negative")
        if _as_decimal(self.volatility_multiplier) < 0:
            raise ValueError("volatility_multiplier must be non-negative")
        if _as_decimal(self.round_trip_cost_bps) < 0:
            raise ValueError("round_trip_cost_bps must be non-negative")

    def materiality_bps(self, realized_volatility_bps: Decimal) -> Decimal:
        if realized_volatility_bps < 0:
            raise ValueError("realized_volatility_bps must be non-negative")
        return max(
            _as_decimal(self.min_move_bps),
            _as_decimal(self.volatility_multiplier) * realized_volatility_bps,
        )


@dataclass(frozen=True)
class DirectionalReturnAssessment:
    """Verdict for one signed directional return at a predeclared horizon."""

    verdict: str
    gross_expected_bps: Decimal
    net_expected_bps: Decimal
    net_reverse_bps: Decimal
    materiality_bps: Decimal
    inverse_hypothesis_candidate: bool

    def __post_init__(self) -> None:
        if self.verdict not in DIRECTIONAL_VERDICTS - {"inconclusive"}:
            raise ValueError("unsupported directional return verdict")
        if self.materiality_bps < 0:
            raise ValueError("directional return materiality must be non-negative")


@dataclass(frozen=True)
class DirectionalOutcomeAssessment:
    """Float-facing assessment used by historical studies and reports."""

    verdict: str
    net_expected_bps: float
    net_reverse_bps: float
    materiality_bps: float
    inverse_hypothesis_candidate: bool

    def __post_init__(self) -> None:
        if self.verdict not in DIRECTIONAL_VERDICTS - {"inconclusive"}:
            raise ValueError("unsupported directional outcome verdict")


@dataclass(frozen=True)
class DirectionalSignalOutcome:
    signal_id: str
    instrument_id: str
    signal_type: str
    source_event_at: datetime
    horizon_seconds: int
    verdict: str
    reason_code: str
    expected_direction: int
    anchor_price: Decimal | None
    forward_price: Decimal | None
    raw_return_bps: Decimal | None
    net_expected_bps: Decimal | None
    net_reverse_bps: Decimal | None
    materiality_bps: Decimal
    cost_model_version: str
    policy_version: str
    inverse_hypothesis_candidate: bool

    def __post_init__(self) -> None:
        if self.verdict not in DIRECTIONAL_VERDICTS:
            raise ValueError("unsupported directional outcome verdict")
        if self.expected_direction not in {-1, 1}:
            raise ValueError("expected_direction must be -1 or 1")
        if self.source_event_at.tzinfo is None:
            raise ValueError("source_event_at must be timezone-aware")
        if not self.reason_code.strip():
            raise ValueError("reason_code must not be empty")


def reference_price(tick: ReferenceTick) -> Decimal | None:
    """Return the canonical price for outcome evaluation.

    Priority matches the product contract: valid book midpoint, then last price,
    then trade price.
    """

    if tick.has_valid_book:
        return (tick.bid_price + tick.ask_price) / Decimal(2)
    if tick.has_last_price:
        return tick.last_price
    if tick.has_trade:
        return tick.trade_price
    return None


def classify_directional_outcome(
    *,
    gross_expected_bps: float,
    baseline_sigma_bps: float,
    horizon_seconds: int,
    policy: DirectionalOutcomePolicy,
) -> DirectionalOutcomeAssessment:
    """Classify a directional hypothesis with horizon-scaled volatility."""

    if not math.isfinite(gross_expected_bps):
        raise ValueError("gross_expected_bps must be finite")
    if not math.isfinite(baseline_sigma_bps) or baseline_sigma_bps < 0:
        raise ValueError("baseline sigma must be finite and non-negative")
    if horizon_seconds <= 0:
        raise ValueError("horizon must be positive")
    scaled_sigma = baseline_sigma_bps * math.sqrt(
        horizon_seconds / policy.baseline_volatility_window_seconds
    )
    assessment = classify_directional_return(
        gross_expected_bps=_as_decimal(gross_expected_bps),
        realized_volatility_bps=_as_decimal(scaled_sigma),
        policy=policy,
    )
    return DirectionalOutcomeAssessment(
        verdict=assessment.verdict,
        net_expected_bps=float(assessment.net_expected_bps),
        net_reverse_bps=float(assessment.net_reverse_bps),
        materiality_bps=float(assessment.materiality_bps),
        inverse_hypothesis_candidate=assessment.inverse_hypothesis_candidate,
    )


def classify_directional_return(
    *,
    gross_expected_bps: Decimal,
    realized_volatility_bps: Decimal,
    policy: DirectionalOutcomePolicy,
) -> DirectionalReturnAssessment:
    """Classify a signed directional return against cost and materiality.

    ``gross_expected_bps`` is already signed to the signal expectation:
    positive means the market moved in the expected direction, negative means it
    moved against the signal.  The reverse hypothesis is evaluated explicitly so
    contradicted outcomes can be accumulated as inverse-hypothesis candidates.
    """

    materiality_bps = policy.materiality_bps(realized_volatility_bps)
    cost_bps = _as_decimal(policy.round_trip_cost_bps)
    net_expected_bps = gross_expected_bps - cost_bps
    net_reverse_bps = -gross_expected_bps - cost_bps
    if net_expected_bps >= materiality_bps:
        verdict = "confirmed"
        inverse_candidate = False
    elif net_reverse_bps >= materiality_bps:
        verdict = "contradicted"
        inverse_candidate = True
    else:
        verdict = "insignificant"
        inverse_candidate = False
    return DirectionalReturnAssessment(
        verdict=verdict,
        gross_expected_bps=gross_expected_bps,
        net_expected_bps=net_expected_bps,
        net_reverse_bps=net_reverse_bps,
        materiality_bps=materiality_bps,
        inverse_hypothesis_candidate=inverse_candidate,
    )


def evaluate_directional_outcome(
    *,
    signal_id: str,
    instrument_id: str,
    signal_type: str,
    source_event_at: datetime,
    expected_direction: int,
    anchor_tick: ReferenceTick | None,
    forward_tick: ReferenceTick | None,
    realized_volatility_bps: Decimal,
    policy: DirectionalOutcomePolicy,
) -> DirectionalSignalOutcome:
    if expected_direction not in {-1, 1}:
        raise ValueError("expected_direction must be -1 or 1")
    if source_event_at.tzinfo is None:
        raise ValueError("source_event_at must be timezone-aware")
    materiality_bps = policy.materiality_bps(realized_volatility_bps)

    if anchor_tick is None:
        return _inconclusive(
            signal_id,
            instrument_id,
            signal_type,
            source_event_at,
            expected_direction,
            materiality_bps,
            policy,
            "anchor_price_unavailable",
        )
    if anchor_tick.event_at > source_event_at:
        raise ValueError("anchor_tick must not be after source_event_at")
    anchor_age = source_event_at - anchor_tick.event_at
    if anchor_age.total_seconds() > policy.anchor_max_age_seconds:
        return _inconclusive(
            signal_id,
            instrument_id,
            signal_type,
            source_event_at,
            expected_direction,
            materiality_bps,
            policy,
            "anchor_price_stale",
        )
    anchor_price = reference_price(anchor_tick)
    if anchor_price is None or anchor_price <= 0:
        return _inconclusive(
            signal_id,
            instrument_id,
            signal_type,
            source_event_at,
            expected_direction,
            materiality_bps,
            policy,
            "anchor_price_unavailable",
        )
    if forward_tick is None:
        return _inconclusive(
            signal_id,
            instrument_id,
            signal_type,
            source_event_at,
            expected_direction,
            materiality_bps,
            policy,
            "forward_price_unavailable",
            anchor_price=anchor_price,
        )
    if forward_tick.event_at <= source_event_at:
        raise ValueError("forward_tick must be after source_event_at")
    forward_price = reference_price(forward_tick)
    if forward_price is None or forward_price <= 0:
        return _inconclusive(
            signal_id,
            instrument_id,
            signal_type,
            source_event_at,
            expected_direction,
            materiality_bps,
            policy,
            "forward_price_unavailable",
            anchor_price=anchor_price,
        )

    raw_return_bps = (forward_price - anchor_price) / anchor_price * Decimal(10_000)
    signed_return_bps = raw_return_bps * Decimal(expected_direction)
    assessment = classify_directional_return(
        gross_expected_bps=signed_return_bps,
        realized_volatility_bps=realized_volatility_bps,
        policy=policy,
    )

    return DirectionalSignalOutcome(
        signal_id=signal_id,
        instrument_id=instrument_id,
        signal_type=signal_type,
        source_event_at=source_event_at,
        horizon_seconds=policy.horizon_seconds,
        verdict=assessment.verdict,
        reason_code=assessment.verdict,
        expected_direction=expected_direction,
        anchor_price=anchor_price,
        forward_price=forward_price,
        raw_return_bps=raw_return_bps,
        net_expected_bps=assessment.net_expected_bps,
        net_reverse_bps=assessment.net_reverse_bps,
        materiality_bps=assessment.materiality_bps,
        cost_model_version=policy.cost_model_version,
        policy_version=policy.policy_version,
        inverse_hypothesis_candidate=assessment.inverse_hypothesis_candidate,
    )


def _inconclusive(
    signal_id: str,
    instrument_id: str,
    signal_type: str,
    source_event_at: datetime,
    expected_direction: int,
    materiality_bps: Decimal,
    policy: DirectionalOutcomePolicy,
    reason_code: str,
    *,
    anchor_price: Decimal | None = None,
) -> DirectionalSignalOutcome:
    return DirectionalSignalOutcome(
        signal_id=signal_id,
        instrument_id=instrument_id,
        signal_type=signal_type,
        source_event_at=source_event_at,
        horizon_seconds=policy.horizon_seconds,
        verdict="inconclusive",
        reason_code=reason_code,
        expected_direction=expected_direction,
        anchor_price=anchor_price,
        forward_price=None,
        raw_return_bps=None,
        net_expected_bps=None,
        net_reverse_bps=None,
        materiality_bps=materiality_bps,
        cost_model_version=policy.cost_model_version,
        policy_version=policy.policy_version,
        inverse_hypothesis_candidate=False,
    )


def _as_decimal(value: Decimal | float) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if not math.isfinite(value):
        raise ValueError("numeric value must be finite")
    return Decimal(str(value))
