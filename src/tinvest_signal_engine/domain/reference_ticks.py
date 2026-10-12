"""Domain model for prices used by signal outcome evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID


@dataclass(frozen=True)
class ReferenceTick:
    instrument_id: str
    event_at: datetime
    received_at: datetime
    event_id: UUID
    source_kind: str
    bid_price: Decimal = Decimal(0)
    ask_price: Decimal = Decimal(0)
    last_price: Decimal = Decimal(0)
    trade_price: Decimal = Decimal(0)
    bid_quantity: int = 0
    ask_quantity: int = 0
    has_valid_book: bool = False
    has_last_price: bool = False
    has_trade: bool = False

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if self.event_at.tzinfo is None or self.event_at.utcoffset() is None:
            raise ValueError("event_at must be timezone-aware")
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        if self.source_kind not in {"orderbook", "last_price", "trade"}:
            raise ValueError("unsupported reference tick source_kind")
        if self.bid_quantity < 0 or self.ask_quantity < 0:
            raise ValueError("book quantities must be non-negative")
        if self.has_valid_book:
            if self.bid_price <= 0 or self.ask_price <= 0:
                raise ValueError("valid book prices must be positive")
            if self.ask_price < self.bid_price:
                raise ValueError("valid book must not be crossed")
        if self.has_last_price and self.last_price <= 0:
            raise ValueError("valid last price must be positive")
        if self.has_trade and self.trade_price <= 0:
            raise ValueError("valid trade price must be positive")
