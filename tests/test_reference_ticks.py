from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from tinvest_signal_engine.application.reference_ticks import (
    NormalizedMarketEvent,
    reference_tick_from_event,
)


EVENT_ID = "fd56ea27-aeb3-47f1-b038-182f747f5aa2"
NOW = datetime(2026, 7, 10, 9, 30, tzinfo=timezone.utc)


def _event(event_type: str, payload: dict[str, object]) -> NormalizedMarketEvent:
    return NormalizedMarketEvent(
        event_id=EVENT_ID,
        event_type=event_type,
        instrument_id="SBER_TQBR",
        source_time=NOW,
        received_at=NOW,
        payload=payload,
    )


def test_trade_maps_to_reference_tick_without_float_rounding() -> None:
    tick = reference_tick_from_event(
        _event("trade", {"price": {"units": 312, "nano": 123_456_789}})
    )

    assert tick is not None
    assert tick.event_id == UUID(EVENT_ID)
    assert tick.trade_price == Decimal("312.123456789")
    assert tick.has_trade is True
    assert tick.has_valid_book is False


def test_orderbook_selects_best_valid_levels() -> None:
    tick = reference_tick_from_event(
        _event(
            "orderbook",
            {
                "bids": [
                    {"price": 100, "quantity": 10},
                    {"price": 101, "quantity": 4},
                ],
                "asks": [
                    {"price": 103, "quantity": 3},
                    {"price": 102, "quantity": 8},
                ],
            },
        )
    )

    assert tick is not None
    assert tick.bid_price == Decimal("101")
    assert tick.ask_price == Decimal("102")
    assert tick.bid_quantity == 4
    assert tick.ask_quantity == 8
    assert tick.has_valid_book is True


def test_crossed_book_and_unrelated_event_do_not_create_ticks() -> None:
    crossed = _event(
        "orderbook",
        {
            "bids": [{"price": 103, "quantity": 1}],
            "asks": [{"price": 102, "quantity": 1}],
        },
    )

    assert reference_tick_from_event(crossed) is None
    assert reference_tick_from_event(_event("candle", {"close": 100})) is None
