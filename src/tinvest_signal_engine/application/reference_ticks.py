"""Extract and persist reference prices from normalized market events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, Protocol, Sequence
from uuid import UUID

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick


@dataclass(frozen=True)
class NormalizedMarketEvent:
    event_id: str
    event_type: str
    instrument_id: str
    source_time: datetime
    received_at: datetime
    payload: Mapping[str, object]


class ReferenceTickStore(Protocol):
    def persist(self, tick: ReferenceTick) -> None: ...

    def persist_many(self, ticks: tuple[ReferenceTick, ...]) -> None: ...


class ReferenceTickProcessor:
    def __init__(self, store: ReferenceTickStore) -> None:
        self._store = store

    def process(self, event: NormalizedMarketEvent) -> bool:
        tick = reference_tick_from_event(event)
        if tick is None:
            return False
        self._store.persist(tick)
        return True

    def process_many(self, events: tuple[NormalizedMarketEvent, ...]) -> int:
        ticks = tuple(
            tick
            for event in events
            if (tick := reference_tick_from_event(event)) is not None
        )
        if ticks:
            self._store.persist_many(ticks)
        return len(ticks)


def reference_tick_from_event(
    event: NormalizedMarketEvent,
) -> ReferenceTick | None:
    if event.event_type == "trade":
        price = _quotation(event.payload.get("price"))
        if price is None or price <= 0:
            return None
        return _tick(event, source_kind="trade", trade_price=price, has_trade=True)

    if event.event_type == "last_price":
        price = _quotation(event.payload.get("price"))
        if price is None or price <= 0:
            return None
        return _tick(
            event,
            source_kind="last_price",
            last_price=price,
            has_last_price=True,
        )

    if event.event_type != "orderbook":
        return None

    bids = _levels(event.payload.get("bids"))
    asks = _levels(event.payload.get("asks"))
    if not bids or not asks:
        return None
    bid_price, bid_quantity = max(bids, key=lambda item: item[0])
    ask_price, ask_quantity = min(asks, key=lambda item: item[0])
    if ask_price < bid_price:
        return None
    return _tick(
        event,
        source_kind="orderbook",
        bid_price=bid_price,
        ask_price=ask_price,
        bid_quantity=bid_quantity,
        ask_quantity=ask_quantity,
        has_valid_book=True,
    )


def _tick(
    event: NormalizedMarketEvent,
    *,
    source_kind: str,
    bid_price: Decimal = Decimal(0),
    ask_price: Decimal = Decimal(0),
    last_price: Decimal = Decimal(0),
    trade_price: Decimal = Decimal(0),
    bid_quantity: int = 0,
    ask_quantity: int = 0,
    has_valid_book: bool = False,
    has_last_price: bool = False,
    has_trade: bool = False,
) -> ReferenceTick:
    return ReferenceTick(
        instrument_id=event.instrument_id,
        event_at=event.source_time,
        received_at=event.received_at,
        event_id=UUID(event.event_id),
        source_kind=source_kind,
        bid_price=bid_price,
        ask_price=ask_price,
        last_price=last_price,
        trade_price=trade_price,
        bid_quantity=bid_quantity,
        ask_quantity=ask_quantity,
        has_valid_book=has_valid_book,
        has_last_price=has_last_price,
        has_trade=has_trade,
    )


def _levels(value: object) -> tuple[tuple[Decimal, int], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    levels: list[tuple[Decimal, int]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        price = _quotation(raw.get("price"))
        try:
            quantity = int(raw.get("quantity", 0))
        except (TypeError, ValueError):
            continue
        if price is not None and price > 0 and quantity >= 0:
            levels.append((price, quantity))
    return tuple(levels)


def _quotation(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Mapping):
        try:
            units = Decimal(str(value.get("units", 0)))
            nano = Decimal(str(value.get("nano", 0)))
            return units + nano / Decimal(1_000_000_000)
        except (InvalidOperation, TypeError, ValueError):
            return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
