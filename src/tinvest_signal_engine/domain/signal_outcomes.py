"""Framework-independent signal self-evaluation rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick


DIRECTIONAL_VERDICTS = frozenset(
    {"confirmed", "contradicted", "insignificant", "inconclusive"}
)


@dataclass(frozen=True)
class DirectionalOutcomePolicy:
    policy_version: str
    cost_model_version: str
    horizon_seconds: int
    anchor_max_age_seconds: int
    forward_grace_seconds: int
    min_move_bps: Decimal
    volatility_multiplier: Decimal
    round_trip_cost_bps: Decimal

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
        if self.min_move_bps < 0:
            raise ValueError("min_move_bps must be non-negative")
        if self.volatility_multiplier < 0:
            raise ValueError("volatility_multiplier must be non-negative")
        if self.round_trip_cost_bps < 0:
            raise ValueError("round_trip_cost_bps must be non-negative")

    def materiality_bps(self, realized_volatility_bps: Decimal) -> Decimal:
        if realized_volatility_bps < 0:
            raise ValueError("realized_volatility_bps must be non-negative")
        return max(
            self.min_move_bps,
            self.volatility_multiplier * realized_volatility_bps,
        )


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
    net_expected_bps = signed_return_bps - policy.round_trip_cost_bps
    net_reverse_bps = -signed_return_bps - policy.round_trip_cost_bps
    if net_expected_bps >= materiality_bps:
        verdict = "confirmed"
        inverse_candidate = False
    elif net_reverse_bps >= materiality_bps:
        verdict = "contradicted"
        inverse_candidate = True
    else:
        verdict = "insignificant"
        inverse_candidate = False

    return DirectionalSignalOutcome(
        signal_id=signal_id,
        instrument_id=instrument_id,
        signal_type=signal_type,
        source_event_at=source_event_at,
        horizon_seconds=policy.horizon_seconds,
        verdict=verdict,
        reason_code=verdict,
        expected_direction=expected_direction,
        anchor_price=anchor_price,
        forward_price=forward_price,
        raw_return_bps=raw_return_bps,
        net_expected_bps=net_expected_bps,
        net_reverse_bps=net_reverse_bps,
        materiality_bps=materiality_bps,
        cost_model_version=policy.cost_model_version,
        policy_version=policy.policy_version,
        inverse_hypothesis_candidate=inverse_candidate,
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
