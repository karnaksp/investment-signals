from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick
from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    classify_directional_outcome,
    classify_directional_return,
    evaluate_directional_outcome,
    reference_price,
)


def _policy(**overrides: object) -> DirectionalOutcomePolicy:
    values = {
        "policy_version": "directional-outcome-v1",
        "cost_model_version": "cost-v1",
        "horizon_seconds": 300,
        "anchor_max_age_seconds": 5,
        "forward_grace_seconds": 30,
        "min_move_bps": Decimal("10"),
        "volatility_multiplier": Decimal("0"),
        "round_trip_cost_bps": Decimal("4"),
    }
    values.update(overrides)
    return DirectionalOutcomePolicy(**values)


def _tick(
    *,
    event_at: datetime,
    bid: str = "99",
    ask: str = "101",
    last: str = "0",
    trade: str = "0",
    has_book: bool = True,
    has_last: bool = False,
    has_trade: bool = False,
) -> ReferenceTick:
    return ReferenceTick(
        instrument_id="SBER",
        event_at=event_at,
        received_at=event_at,
        event_id=uuid4(),
        source_kind="orderbook" if has_book else "last_price" if has_last else "trade",
        bid_price=Decimal(bid),
        ask_price=Decimal(ask),
        last_price=Decimal(last),
        trade_price=Decimal(trade),
        bid_quantity=10,
        ask_quantity=12,
        has_valid_book=has_book,
        has_last_price=has_last,
        has_trade=has_trade,
    )


def test_directional_return_subtracts_cost_and_marks_inverse_candidate() -> None:
    policy = _policy()

    insignificant = classify_directional_return(
        gross_expected_bps=Decimal("13.999"),
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )
    confirmed = classify_directional_return(
        gross_expected_bps=Decimal("14"),
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )
    contradicted = classify_directional_return(
        gross_expected_bps=Decimal("-14"),
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )

    assert insignificant.verdict == "insignificant"
    assert insignificant.net_expected_bps == Decimal("9.999")
    assert insignificant.net_reverse_bps == Decimal("-17.999")
    assert insignificant.inverse_hypothesis_candidate is False
    assert confirmed.verdict == "confirmed"
    assert confirmed.net_expected_bps == Decimal("10")
    assert confirmed.inverse_hypothesis_candidate is False
    assert contradicted.verdict == "contradicted"
    assert contradicted.net_reverse_bps == Decimal("10")
    assert contradicted.inverse_hypothesis_candidate is True


def test_directional_return_materiality_scales_from_realized_volatility() -> None:
    policy = _policy(
        min_move_bps=Decimal("1"),
        volatility_multiplier=Decimal("2"),
        round_trip_cost_bps=Decimal("0"),
    )

    assessment = classify_directional_return(
        gross_expected_bps=Decimal("20"),
        realized_volatility_bps=Decimal("9"),
        policy=policy,
    )

    assert assessment.verdict == "confirmed"
    assert assessment.materiality_bps == Decimal("18")


def test_directional_outcome_wrapper_scales_volatility_by_horizon() -> None:
    policy = DirectionalOutcomePolicy(
        min_move_bps=1.0,
        volatility_multiplier=2.0,
        round_trip_cost_bps=0.0,
        baseline_volatility_window_seconds=60,
    )

    assessment = classify_directional_outcome(
        gross_expected_bps=20.0,
        baseline_sigma_bps=3.0,
        horizon_seconds=9 * 60,
        policy=policy,
    )

    assert assessment.verdict == "confirmed"
    assert assessment.materiality_bps == 18.0


def test_reference_price_uses_book_midpoint_then_last_then_trade() -> None:
    at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)

    assert reference_price(_tick(event_at=at, bid="99", ask="101")) == Decimal("100")
    assert reference_price(
        _tick(event_at=at, has_book=False, has_last=True, last="102")
    ) == Decimal("102")
    assert reference_price(
        _tick(event_at=at, has_book=False, has_trade=True, trade="103")
    ) == Decimal("103")


def test_directional_outcome_rejects_future_anchor_and_old_anchor() -> None:
    source_event_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    policy = _policy()
    forward = _tick(event_at=source_event_at + timedelta(seconds=300), bid="101", ask="103")

    with pytest.raises(ValueError, match="anchor_tick"):
        evaluate_directional_outcome(
            signal_id="signal-1",
            instrument_id="SBER",
            signal_type="price_jump",
            source_event_at=source_event_at,
            expected_direction=1,
            anchor_tick=_tick(event_at=source_event_at + timedelta(seconds=1)),
            forward_tick=forward,
            realized_volatility_bps=Decimal("1"),
            policy=policy,
        )

    stale = evaluate_directional_outcome(
        signal_id="signal-1",
        instrument_id="SBER",
        signal_type="price_jump",
        source_event_at=source_event_at,
        expected_direction=1,
        anchor_tick=_tick(event_at=source_event_at - timedelta(seconds=6)),
        forward_tick=forward,
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )

    assert stale.verdict == "inconclusive"
    assert stale.reason_code == "anchor_price_stale"


def test_directional_outcome_evaluates_prices_and_contradictions() -> None:
    source_event_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    policy = _policy(round_trip_cost_bps=Decimal("1"), min_move_bps=Decimal("5"))

    confirmed = evaluate_directional_outcome(
        signal_id="signal-1",
        instrument_id="SBER",
        signal_type="price_jump",
        source_event_at=source_event_at,
        expected_direction=1,
        anchor_tick=_tick(event_at=source_event_at, bid="99", ask="101"),
        forward_tick=_tick(
            event_at=source_event_at + timedelta(seconds=300), bid="101", ask="103"
        ),
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )
    contradicted = evaluate_directional_outcome(
        signal_id="signal-2",
        instrument_id="SBER",
        signal_type="price_jump",
        source_event_at=source_event_at,
        expected_direction=1,
        anchor_tick=_tick(event_at=source_event_at, bid="99", ask="101"),
        forward_tick=_tick(
            event_at=source_event_at + timedelta(seconds=300), bid="97", ask="99"
        ),
        realized_volatility_bps=Decimal("1"),
        policy=policy,
    )

    assert confirmed.verdict == "confirmed"
    assert confirmed.raw_return_bps == Decimal("200")
    assert confirmed.net_expected_bps == Decimal("199")
    assert confirmed.inverse_hypothesis_candidate is False
    assert contradicted.verdict == "contradicted"
    assert contradicted.net_reverse_bps == Decimal("199")
    assert contradicted.inverse_hypothesis_candidate is True


def test_directional_outcome_rejects_invalid_policy_and_inputs() -> None:
    with pytest.raises(ValueError, match="policy_version"):
        _policy(policy_version="")
    with pytest.raises(ValueError, match="non-negative"):
        _policy(min_move_bps=Decimal("-1"))
    policy = _policy()
    with pytest.raises(ValueError, match="realized_volatility"):
        policy.materiality_bps(Decimal("-0.1"))
    with pytest.raises(ValueError, match="expected_direction"):
        evaluate_directional_outcome(
            signal_id="signal-1",
            instrument_id="SBER",
            signal_type="price_jump",
            source_event_at=datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc),
            expected_direction=0,
            anchor_tick=None,
            forward_tick=None,
            realized_volatility_bps=Decimal("1"),
            policy=policy,
        )
