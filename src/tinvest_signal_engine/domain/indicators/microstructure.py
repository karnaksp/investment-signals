"""Deterministic microstructure classifications without infrastructure dependencies."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfirmedImpulse:
    direction: str
    score: int
    max_score: int
    activity_sources: tuple[str, ...]
    orderbook_confirmed: bool


@dataclass(frozen=True)
class LiquidityVacuum:
    depth_drop_ratio: float
    bid_drop_ratio: float
    ask_drop_ratio: float
    weaker_side: str


@dataclass(frozen=True)
class FuturesPositioning:
    regime: str
    direction: str


def detect_confirmed_impulse(
    *,
    signed_move_bps: float,
    signed_delta_qty: float,
    volume_active: bool,
    trade_rate_active: bool,
    orderbook_imbalance_ratio: float | None,
    min_move_bps: float,
    min_abs_delta_qty: float,
    long_book_threshold: float = 0.55,
    short_book_threshold: float = 0.45,
) -> ConfirmedImpulse | None:
    """Require aligned price, aggressor delta, and at least one activity spike."""

    if abs(signed_move_bps) < max(0.0, min_move_bps):
        return None
    if not (volume_active or trade_rate_active):
        return None
    direction = "long" if signed_move_bps > 0 else "short"
    delta_aligned = (
        signed_delta_qty >= min_abs_delta_qty
        if direction == "long"
        else signed_delta_qty <= -min_abs_delta_qty
    )
    if not delta_aligned:
        return None
    activity_sources = tuple(
        name
        for name, active in (
            ("volume_spike", volume_active),
            ("trade_rate_spike", trade_rate_active),
        )
        if active
    )
    book_confirmed = False
    if orderbook_imbalance_ratio is not None:
        book_confirmed = (
            orderbook_imbalance_ratio >= long_book_threshold
            if direction == "long"
            else orderbook_imbalance_ratio <= short_book_threshold
        )
    return ConfirmedImpulse(
        direction=direction,
        score=2 + 2 + len(activity_sources) + int(book_confirmed),
        max_score=7,
        activity_sources=activity_sources,
        orderbook_confirmed=book_confirmed,
    )


def detect_liquidity_vacuum(
    *,
    current_bid_qty: float,
    current_ask_qty: float,
    reference_bid_qty: float,
    reference_ask_qty: float,
    spread_z_score: float,
    min_spread_z_score: float,
    min_depth_drop_ratio: float,
    min_reference_qty: float,
) -> LiquidityVacuum | None:
    """Detect a broad depth collapse that coincides with an abnormal spread."""

    reference_total = reference_bid_qty + reference_ask_qty
    current_total = current_bid_qty + current_ask_qty
    if reference_total < max(0.0, min_reference_qty):
        return None
    depth_drop_ratio = 1.0 - (current_total / reference_total)
    if depth_drop_ratio < min_depth_drop_ratio:
        return None
    if spread_z_score < min_spread_z_score:
        return None
    bid_drop = _drop_ratio(reference_bid_qty, current_bid_qty)
    ask_drop = _drop_ratio(reference_ask_qty, current_ask_qty)
    weaker_side = (
        "bid" if bid_drop > ask_drop else "ask" if ask_drop > bid_drop else "both"
    )
    return LiquidityVacuum(
        depth_drop_ratio=depth_drop_ratio,
        bid_drop_ratio=bid_drop,
        ask_drop_ratio=ask_drop,
        weaker_side=weaker_side,
    )


def classify_futures_positioning(
    *,
    open_interest_delta: float,
    signed_price_move_bps: float,
    min_price_move_bps: float,
) -> FuturesPositioning | None:
    """Classify the common price/open-interest positioning regimes."""

    if open_interest_delta == 0 or abs(signed_price_move_bps) < min_price_move_bps:
        return None
    if open_interest_delta > 0 and signed_price_move_bps > 0:
        return FuturesPositioning("long_build", "long")
    if open_interest_delta > 0 and signed_price_move_bps < 0:
        return FuturesPositioning("short_build", "short")
    if open_interest_delta < 0 and signed_price_move_bps > 0:
        return FuturesPositioning("short_covering", "long")
    return FuturesPositioning("long_liquidation", "short")


def _drop_ratio(reference: float, current: float) -> float:
    if reference <= 0:
        return 0.0
    return 1.0 - (current / reference)
