"""Immutable one-minute candles used by prospective scientific models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
import json


@dataclass(frozen=True, slots=True)
class ScientificCandle:
    instrument_id: str
    ticker: str
    exchange: str
    trading_day: date
    candle_at: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    complete: bool
    source_kind: str
    source_at: datetime
    received_at: datetime
    source_event_id: str
    payload_fingerprint: str
    has_gap: bool = False
    schema_version: str = "scientific-candle-v1"

    def __post_init__(self) -> None:
        if not all(
            value.strip()
            for value in (
                self.instrument_id,
                self.ticker,
                self.exchange,
                self.source_event_id,
                self.schema_version,
            )
        ):
            raise ValueError("candle identity and schema values are required")
        if self.source_kind not in {"backfill", "stream"}:
            raise ValueError("unsupported candle source kind")
        for name, value in (
            ("candle_at", self.candle_at),
            ("source_at", self.source_at),
            ("received_at", self.received_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        prices = (
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
        )
        if any(value <= 0 for value in prices):
            raise ValueError("candle prices must be positive")
        if self.low_price > min(self.open_price, self.close_price):
            raise ValueError("candle low is above open or close")
        if self.high_price < max(self.open_price, self.close_price):
            raise ValueError("candle high is below open or close")
        if self.volume < 0:
            raise ValueError("candle volume must be non-negative")
        expected = scientific_candle_fingerprint(
            instrument_id=self.instrument_id,
            ticker=self.ticker,
            exchange=self.exchange,
            candle_at=self.candle_at,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            volume=self.volume,
            complete=self.complete,
            source_kind=self.source_kind,
            source_at=self.source_at,
            source_event_id=self.source_event_id,
            has_gap=self.has_gap,
            schema_version=self.schema_version,
        )
        if self.payload_fingerprint != expected:
            raise ValueError("candle payload fingerprint does not match content")


def scientific_candle_fingerprint(
    *,
    instrument_id: str,
    ticker: str,
    exchange: str,
    candle_at: datetime,
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    volume: int,
    complete: bool,
    source_kind: str,
    source_at: datetime,
    source_event_id: str,
    has_gap: bool,
    schema_version: str,
) -> str:
    payload = {
        "candle_at": candle_at.isoformat(),
        "close_price": str(close_price),
        "complete": complete,
        "exchange": exchange,
        "has_gap": has_gap,
        "high_price": str(high_price),
        "instrument_id": instrument_id,
        "low_price": str(low_price),
        "open_price": str(open_price),
        "schema_version": schema_version,
        "source_at": source_at.isoformat(),
        "source_event_id": source_event_id,
        "source_kind": source_kind,
        "ticker": ticker,
        "volume": volume,
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()
