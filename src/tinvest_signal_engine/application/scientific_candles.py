"""Map normalized candle events into the durable scientific candle journal."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, Protocol
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.scientific_candles import (
    ScientificCandle,
    scientific_candle_fingerprint,
)


MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True, slots=True)
class NormalizedCandleEvent:
    event_id: str
    event_type: str
    instrument_id: str
    ticker: str
    class_code: str
    source_time: datetime
    received_at: datetime
    payload: Mapping[str, object]


class ScientificCandleStore(Protocol):
    def persist_many(self, candles: tuple[ScientificCandle, ...]) -> None: ...


class ScientificCandleJournalProcessor:
    def __init__(self, store: ScientificCandleStore) -> None:
        self._store = store

    def process_many(self, events: tuple[NormalizedCandleEvent, ...]) -> int:
        candles = tuple(
            candle
            for event in events
            if (candle := scientific_candle_from_event(event)) is not None
        )
        if candles:
            self._store.persist_many(candles)
        return len(candles)


def scientific_candle_from_event(
    event: NormalizedCandleEvent,
) -> ScientificCandle | None:
    if event.event_type != "candle":
        return None
    if event.source_time.tzinfo is None or event.source_time.utcoffset() is None:
        raise ValueError("source_time must be timezone-aware")
    if event.received_at.tzinfo is None or event.received_at.utcoffset() is None:
        raise ValueError("received_at must be timezone-aware")
    open_price = _quotation(event.payload.get("open"))
    high_price = _quotation(event.payload.get("high"))
    low_price = _quotation(event.payload.get("low"))
    close_price = _quotation(event.payload.get("close"))
    if None in {open_price, high_price, low_price, close_price}:
        return None
    try:
        volume = int(event.payload.get("volume", 0))
    except (TypeError, ValueError):
        return None
    source_at = _timestamp(event.payload.get("last_trade_ts")) or event.source_time
    exchange = event.class_code.strip() or "TQBR"
    complete = bool(event.payload.get("is_complete", True))
    fingerprint = scientific_candle_fingerprint(
        instrument_id=event.instrument_id,
        ticker=event.ticker,
        exchange=exchange,
        candle_at=event.source_time,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        complete=complete,
        source_kind="stream",
        source_at=source_at,
        source_event_id=event.event_id,
        has_gap=False,
        schema_version="scientific-candle-v1",
    )
    return ScientificCandle(
        instrument_id=event.instrument_id,
        ticker=event.ticker,
        exchange=exchange,
        trading_day=event.source_time.astimezone(MOSCOW).date(),
        candle_at=event.source_time,
        open_price=open_price,
        high_price=high_price,
        low_price=low_price,
        close_price=close_price,
        volume=volume,
        complete=complete,
        source_kind="stream",
        source_at=source_at,
        received_at=event.received_at,
        source_event_id=event.event_id,
        payload_fingerprint=fingerprint,
    )


def _quotation(value: object) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        if isinstance(value, Mapping):
            units = Decimal(str(value.get("units", 0)))
            nano = Decimal(str(value.get("nano", 0)))
            return units + nano / Decimal(1_000_000_000)
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str) or not value.strip():
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else None
