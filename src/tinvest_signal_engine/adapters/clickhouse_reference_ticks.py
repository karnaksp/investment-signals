"""Idempotent ClickHouse persistence for reference ticks."""

from __future__ import annotations

from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick


INSERT_REFERENCE_TICK_SQL = """
INSERT INTO market_reference_ticks
(
    instrument_id, event_at, received_at, event_id, source_kind,
    bid_price, ask_price, last_price, trade_price,
    bid_quantity, ask_quantity,
    has_valid_book, has_last_price, has_trade
)
SELECT
    {instrument_id:String},
    parseDateTime64BestEffort({event_at:String}, 9, 'UTC'),
    parseDateTime64BestEffort({received_at:String}, 9, 'UTC'),
    toUUID({event_id:String}),
    {source_kind:String},
    toDecimal128({bid_price:String}, 9),
    toDecimal128({ask_price:String}, 9),
    toDecimal128({last_price:String}, 9),
    toDecimal128({trade_price:String}, 9),
    {bid_quantity:UInt64},
    {ask_quantity:UInt64},
    {has_valid_book:UInt8},
    {has_last_price:UInt8},
    {has_trade:UInt8}
WHERE NOT EXISTS
(
    SELECT 1
    FROM market_reference_ticks
    WHERE event_id = toUUID({event_id:String})
)
""".strip()


class ClickHouseReferenceTickStore:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def persist(self, tick: ReferenceTick) -> None:
        parameters = _parameters(tick)
        query = {"database": self._database}
        query.update({f"param_{key}": value for key, value in parameters.items()})
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=INSERT_REFERENCE_TICK_SQL.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                response.read()
        except HTTPError as error:
            raise RuntimeError(
                f"ClickHouse reference tick insert failed with status {error.code}"
            ) from error
        except URLError as error:
            raise RuntimeError(
                "ClickHouse reference tick insert connection failed"
            ) from error


def _parameters(tick: ReferenceTick) -> Mapping[str, str]:
    return {
        "instrument_id": tick.instrument_id,
        "event_at": tick.event_at.isoformat(),
        "received_at": tick.received_at.isoformat(),
        "event_id": str(tick.event_id),
        "source_kind": tick.source_kind,
        "bid_price": str(tick.bid_price),
        "ask_price": str(tick.ask_price),
        "last_price": str(tick.last_price),
        "trade_price": str(tick.trade_price),
        "bid_quantity": str(tick.bid_quantity),
        "ask_quantity": str(tick.ask_quantity),
        "has_valid_book": "1" if tick.has_valid_book else "0",
        "has_last_price": "1" if tick.has_last_price else "0",
        "has_trade": "1" if tick.has_trade else "0",
    }
