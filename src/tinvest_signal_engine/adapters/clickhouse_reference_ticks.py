"""Idempotent ClickHouse persistence for reference ticks."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Mapping
from uuid import UUID
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    transient_clickhouse_error,
)
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

BATCH_INSERT_REFERENCE_TICKS_SQL = """
INSERT INTO market_reference_ticks
(
    instrument_id, event_at, received_at, event_id, source_kind,
    bid_price, ask_price, last_price, trade_price,
    bid_quantity, ask_quantity,
    has_valid_book, has_last_price, has_trade
)
SELECT
    incoming.instrument_id,
    incoming.event_at,
    incoming.received_at,
    incoming.event_id,
    incoming.source_kind,
    incoming.bid_price,
    incoming.ask_price,
    incoming.last_price,
    incoming.trade_price,
    incoming.bid_quantity,
    incoming.ask_quantity,
    incoming.has_valid_book,
    incoming.has_last_price,
    incoming.has_trade
FROM input(
    'instrument_id String,
     event_at DateTime64(9, \\'UTC\\'),
     received_at DateTime64(9, \\'UTC\\'),
     event_id UUID,
     source_kind String,
     bid_price Decimal(18, 9),
     ask_price Decimal(18, 9),
     last_price Decimal(18, 9),
     trade_price Decimal(18, 9),
     bid_quantity UInt64,
     ask_quantity UInt64,
     has_valid_book UInt8,
     has_last_price UInt8,
     has_trade UInt8'
) AS incoming
LEFT ANTI JOIN
(
    SELECT instrument_id, event_at, event_id
    FROM market_reference_ticks
    PREWHERE instrument_id IN {batch_instruments:Array(String)}
      AND toDate(event_at) = {batch_day:Date}
    WHERE event_at >= parseDateTime64BestEffort({batch_start:String}, 9, 'UTC')
      AND event_at <= parseDateTime64BestEffort({batch_end:String}, 9, 'UTC')
) AS stored
ON stored.instrument_id = incoming.instrument_id
   AND stored.event_at = incoming.event_at
   AND stored.event_id = incoming.event_id
SETTINGS
    async_insert = 1,
    wait_for_async_insert = 1,
    async_insert_busy_timeout_ms = 2000,
    async_insert_max_data_size = 10485760,
    max_execution_time = 15,
    timeout_before_checking_execution_speed = 0,
    max_rows_to_read = 5000000,
    max_bytes_to_read = 536870912,
    max_memory_usage = 536870912
FORMAT JSONEachRow
""".strip()


SELECT_REFERENCE_TICKS_SQL = """
SELECT DISTINCT
    instrument_id,
    formatDateTime(event_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS event_at,
    formatDateTime(received_at, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS received_at,
    toString(event_id) AS event_id,
    source_kind,
    toString(bid_price) AS bid_price,
    toString(ask_price) AS ask_price,
    toString(last_price) AS last_price,
    toString(trade_price) AS trade_price,
    toUInt64(bid_quantity) AS bid_quantity,
    toUInt64(ask_quantity) AS ask_quantity,
    toUInt8(has_valid_book) AS has_valid_book,
    toUInt8(has_last_price) AS has_last_price,
    toUInt8(has_trade) AS has_trade
FROM market_reference_ticks
WHERE instrument_id = {instrument_id:String}
  AND toDate(event_at) BETWEEN
      toDate(parseDateTime64BestEffort({start_at:String}, 9, 'UTC'))
      AND toDate(parseDateTime64BestEffort({end_at:String}, 9, 'UTC'))
  AND event_at >= parseDateTime64BestEffort({start_at:String}, 9, 'UTC')
  AND event_at <= parseDateTime64BestEffort({end_at:String}, 9, 'UTC')
ORDER BY event_at ASC, event_id ASC
LIMIT {limit:UInt32}
SETTINGS
    max_execution_time = 15,
    timeout_before_checking_execution_speed = 0,
    max_rows_to_read = 1000000
FORMAT JSONEachRow
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
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as error:
            transient = transient_clickhouse_error(
                error,
                operation="reference_tick_insert",
            )
            if transient is not None:
                raise transient from error
            raise RuntimeError(
                f"ClickHouse reference tick insert failed with status {error.code}"
            ) from error

    def persist_many(self, ticks: tuple[ReferenceTick, ...]) -> None:
        if not ticks:
            return
        unique = _unique_ticks(ticks)
        by_day: dict[date, list[ReferenceTick]] = {}
        for tick in unique:
            by_day.setdefault(tick.event_at.date(), []).append(tick)
        for batch_day, day_ticks in sorted(by_day.items()):
            self._persist_day(tuple(day_ticks), batch_day=batch_day)

    def _persist_day(
        self,
        ticks: tuple[ReferenceTick, ...],
        *,
        batch_day: date,
    ) -> None:
        rows = "\n".join(
            json.dumps(_json_row(tick), ensure_ascii=True, separators=(",", ":"))
            for tick in ticks
        )
        batch_start = min(tick.event_at for tick in ticks).isoformat()
        batch_end = max(tick.event_at for tick in ticks).isoformat()
        query = {
            "database": self._database,
            "date_time_input_format": "best_effort",
            "param_batch_start": batch_start,
            "param_batch_end": batch_end,
            "param_batch_day": batch_day.isoformat(),
            "param_batch_instruments": _clickhouse_string_array(
                tuple(dict.fromkeys(tick.instrument_id for tick in ticks))
            ),
        }
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=(BATCH_INSERT_REFERENCE_TICKS_SQL + "\n" + rows + "\n").encode(
                "utf-8"
            ),
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
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as error:
            transient = transient_clickhouse_error(
                error,
                operation="reference_tick_batch_insert",
            )
            if transient is not None:
                raise transient from error
            raise RuntimeError(
                "ClickHouse reference tick batch insert failed "
                f"with status {error.code}"
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


def _unique_ticks(
    ticks: tuple[ReferenceTick, ...],
) -> tuple[ReferenceTick, ...]:
    selected: dict[UUID, ReferenceTick] = {}
    for tick in ticks:
        current = selected.get(tick.event_id)
        if current is not None and current != tick:
            raise ValueError("conflicting reference ticks share event_id")
        selected[tick.event_id] = tick
    return tuple(selected.values())


def _clickhouse_string_array(values: tuple[str, ...]) -> str:
    escaped = tuple(
        "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        for value in values
    )
    return "[" + ",".join(escaped) + "]"


def _json_row(tick: ReferenceTick) -> Mapping[str, object]:
    parameters = _parameters(tick)
    return {
        "instrument_id": parameters["instrument_id"],
        "event_at": _clickhouse_datetime(tick.event_at),
        "received_at": _clickhouse_datetime(tick.received_at),
        "event_id": parameters["event_id"],
        "source_kind": parameters["source_kind"],
        "bid_price": parameters["bid_price"],
        "ask_price": parameters["ask_price"],
        "last_price": parameters["last_price"],
        "trade_price": parameters["trade_price"],
        "bid_quantity": int(parameters["bid_quantity"]),
        "ask_quantity": int(parameters["ask_quantity"]),
        "has_valid_book": int(parameters["has_valid_book"]),
        "has_last_price": int(parameters["has_last_price"]),
        "has_trade": int(parameters["has_trade"]),
    }


def _clickhouse_datetime(value: datetime) -> str:
    """Serialize an aware instant in ClickHouse's unambiguous UTC text form."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


class ClickHouseReferenceTickReader:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 15.0,
        limit: int = 20_000,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if limit <= 0:
            raise ValueError("reference tick read limit must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds
        self._limit = limit

    def load(
        self,
        *,
        instrument_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[ReferenceTick, ...]:
        if not instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if start_at.tzinfo is None or start_at.utcoffset() is None:
            raise ValueError("start_at must be timezone-aware")
        if end_at.tzinfo is None or end_at.utcoffset() is None:
            raise ValueError("end_at must be timezone-aware")
        if end_at < start_at:
            raise ValueError("end_at must not be before start_at")
        query = {
            "database": self._database,
            "param_instrument_id": instrument_id,
            "param_start_at": start_at.isoformat(),
            "param_end_at": end_at.isoformat(),
            "param_limit": str(self._limit),
        }
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=SELECT_REFERENCE_TICKS_SQL.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as error:
            transient = transient_clickhouse_error(
                error,
                operation="reference_tick_select",
            )
            if transient is not None:
                raise transient from error
            raise RuntimeError(
                f"ClickHouse reference tick select failed with status {error.code}"
            ) from error
        return tuple(
            _tick_from_row(json.loads(line))
            for line in payload.splitlines()
            if line
        )


def _tick_from_row(row: Mapping[str, object]) -> ReferenceTick:
    return ReferenceTick(
        instrument_id=str(row["instrument_id"]),
        event_at=_parse_clickhouse_time(str(row["event_at"])),
        received_at=_parse_clickhouse_time(str(row["received_at"])),
        event_id=UUID(str(row["event_id"])),
        source_kind=str(row["source_kind"]),
        bid_price=Decimal(str(row["bid_price"])),
        ask_price=Decimal(str(row["ask_price"])),
        last_price=Decimal(str(row["last_price"])),
        trade_price=Decimal(str(row["trade_price"])),
        bid_quantity=int(row["bid_quantity"]),
        ask_quantity=int(row["ask_quantity"]),
        has_valid_book=bool(int(row["has_valid_book"])),
        has_last_price=bool(int(row["has_last_price"])),
        has_trade=bool(int(row["has_trade"])),
    )


def _parse_clickhouse_time(value: str) -> datetime:
    text = value.rstrip("Z")
    if "." in text:
        prefix, fraction = text.split(".", 1)
        text = f"{prefix}.{fraction[:6]}"
    return datetime.fromisoformat(f"{text}+00:00")
