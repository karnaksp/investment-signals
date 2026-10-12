from tinvest_signal_engine.domain.indicators import (
    classify_futures_positioning,
    detect_confirmed_impulse,
    detect_liquidity_vacuum,
)


def test_confirmed_impulse_requires_aligned_delta_and_activity() -> None:
    decision = detect_confirmed_impulse(
        signed_move_bps=18.0,
        signed_delta_qty=25.0,
        volume_active=True,
        trade_rate_active=False,
        orderbook_imbalance_ratio=0.62,
        min_move_bps=10.0,
        min_abs_delta_qty=5.0,
    )

    assert decision is not None
    assert decision.direction == "long"
    assert decision.orderbook_confirmed is True
    assert decision.score == 6

    assert (
        detect_confirmed_impulse(
            signed_move_bps=18.0,
            signed_delta_qty=-25.0,
            volume_active=True,
            trade_rate_active=False,
            orderbook_imbalance_ratio=0.62,
            min_move_bps=10.0,
            min_abs_delta_qty=5.0,
        )
        is None
    )


def test_liquidity_vacuum_requires_depth_loss_and_wide_spread() -> None:
    decision = detect_liquidity_vacuum(
        current_bid_qty=20.0,
        current_ask_qty=30.0,
        reference_bid_qty=100.0,
        reference_ask_qty=100.0,
        spread_z_score=4.0,
        min_spread_z_score=2.5,
        min_depth_drop_ratio=0.45,
        min_reference_qty=10.0,
    )

    assert decision is not None
    assert decision.depth_drop_ratio == 0.75
    assert decision.weaker_side == "bid"


def test_futures_positioning_classifies_all_four_regimes() -> None:
    assert (
        classify_futures_positioning(
            open_interest_delta=10,
            signed_price_move_bps=8,
            min_price_move_bps=5,
        ).regime
        == "long_build"
    )
    assert (
        classify_futures_positioning(
            open_interest_delta=10,
            signed_price_move_bps=-8,
            min_price_move_bps=5,
        ).regime
        == "short_build"
    )
    assert (
        classify_futures_positioning(
            open_interest_delta=-10,
            signed_price_move_bps=8,
            min_price_move_bps=5,
        ).regime
        == "short_covering"
    )
    assert (
        classify_futures_positioning(
            open_interest_delta=-10,
            signed_price_move_bps=-8,
            min_price_move_bps=5,
        ).regime
        == "long_liquidation"
    )
