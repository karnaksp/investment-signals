"""Point-in-time candle cache assembled from local history and ClickHouse."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.application.historical_hypothesis_replay import (
    HistoricalCandleCachePort,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    HistoricalCandle,
)


SELECT_SQL = """
SELECT
    ticker,
    candle_at AS source_time,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    is_complete,
    source_at,
    received_at,
    record_version
FROM scientific_candles_1m
WHERE candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND source_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND received_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
FORMAT JSONEachRow
""".strip()


@dataclass(frozen=True, slots=True)
class VersionedHistoricalCandle:
    """Storage revision retained until the composite snapshot is sealed."""

    candle: HistoricalCandle
    record_version: int

    def __post_init__(self) -> None:
        if self.record_version < 0:
            raise ValueError("candle record_version must not be negative")


class VersionedScientificCandleSource(Protocol):
    def load_as_of(self, as_of: datetime) -> tuple[VersionedHistoricalCandle, ...]: ...


class ClickHouseScientificCandleSource:
    """Read the locally persisted candle journal through ClickHouse HTTP."""

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
        if not database.strip():
            raise ValueError("ClickHouse database must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def load_as_of(self, as_of: datetime) -> tuple[VersionedHistoricalCandle, ...]:
        cutoff = _aware_utc(as_of, "as_of")
        cutoff_text = _timestamp(cutoff)
        query = urlencode(
            {
                "database": self._database,
                "date_time_input_format": "best_effort",
                "param_as_of": cutoff_text,
            }
        )
        request = Request(
            f"{self._base_url}/?{query}",
            data=(SELECT_SQL + "\n").encode("utf-8"),
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
        except HTTPError as error:
            raise RuntimeError(
                f"ClickHouse scientific candle read failed with status {error.code}"
            ) from error
        except URLError as error:
            raise RuntimeError(
                "ClickHouse scientific candle read connection failed"
            ) from error

        rows = tuple(
            _versioned_candle(json.loads(line), cutoff=cutoff)
            for line in payload.splitlines()
            if line.strip()
        )
        return _deduplicate(rows)


class CompositeScientificCandleCache:
    """Seal a reusable point-in-time view across Parquet history and live rows."""

    def __init__(
        self,
        *,
        historical: HistoricalCandleCachePort,
        live: VersionedScientificCandleSource,
        as_of: datetime,
    ) -> None:
        self._historical = historical
        self._live = live
        self._as_of = _aware_utc(as_of, "as_of")
        self._snapshot: tuple[VersionedHistoricalCandle, ...] | None = None
        self._descriptor: CandleCacheDescriptor | None = None

    def describe(self) -> CandleCacheDescriptor:
        self._seal()
        if self._descriptor is None:  # pragma: no cover - guarded by _seal
            raise RuntimeError("composite candle cache descriptor was not sealed")
        return self._descriptor

    def load(self) -> tuple[HistoricalCandle, ...]:
        snapshot = self._seal()
        return tuple(item.candle for item in snapshot)

    def _seal(self) -> tuple[VersionedHistoricalCandle, ...]:
        if self._snapshot is not None:
            return self._snapshot
        historical = tuple(
            VersionedHistoricalCandle(candle=item, record_version=0)
            for item in self._historical.load()
            if item.at.astimezone(timezone.utc) <= self._as_of
        )
        snapshot = _deduplicate((*historical, *self._live.load_as_of(self._as_of)))
        if not snapshot:
            raise ValueError("composite candle cache contains no causal candles")

        candles = tuple(item.candle for item in snapshot)
        days = tuple(item.at.astimezone(timezone.utc).date() for item in candles)
        partitions = {
            (item.ticker, item.at.astimezone(timezone.utc).date()) for item in candles
        }
        descriptor = CandleCacheDescriptor(
            dataset_fingerprint=_snapshot_fingerprint(snapshot, self._as_of),
            partition_count=len(partitions),
            tickers=tuple(sorted({item.ticker for item in candles})),
            start_day=min(days),
            end_day=max(days),
        )
        self._snapshot = snapshot
        self._descriptor = descriptor
        return snapshot


def _versioned_candle(
    row: object,
    *,
    cutoff: datetime,
) -> VersionedHistoricalCandle:
    if not isinstance(row, dict):
        raise ValueError("ClickHouse candle row must be an object")
    source_time = _parse_timestamp(row.get("source_time"), "source_time")
    source_at = _parse_timestamp(row.get("source_at"), "source_at")
    received_at = _parse_timestamp(row.get("received_at"), "received_at")
    if max(source_time, source_at, received_at) > cutoff:
        raise ValueError("ClickHouse returned a candle beyond the causal cutoff")
    complete_value = row.get("is_complete", 0)
    complete = complete_value is True or str(complete_value).lower() in {
        "1",
        "true",
        "yes",
    }
    return VersionedHistoricalCandle(
        candle=HistoricalCandle(
            ticker=str(row.get("ticker", "")).strip().upper(),
            at=source_time,
            open=float(row["open_price"]),
            high=float(row["high_price"]),
            low=float(row["low_price"]),
            close=float(row["close_price"]),
            volume=float(row["volume"]),
            complete=complete,
        ),
        record_version=int(row["record_version"]),
    )


def _deduplicate(
    rows: tuple[VersionedHistoricalCandle, ...],
) -> tuple[VersionedHistoricalCandle, ...]:
    selected: dict[tuple[str, datetime], VersionedHistoricalCandle] = {}
    for row in rows:
        key = (row.candle.ticker, row.candle.at.astimezone(timezone.utc))
        current = selected.get(key)
        if current is None or row.record_version > current.record_version:
            selected[key] = row
            continue
        if (
            row.record_version == current.record_version
            and row.candle != current.candle
        ):
            raise ValueError(
                "conflicting candle payloads share ticker, source_time, and record_version"
            )
    return tuple(
        sorted(
            selected.values(),
            key=lambda item: (
                item.candle.ticker,
                item.candle.at.astimezone(timezone.utc),
            ),
        )
    )


def _snapshot_fingerprint(
    rows: tuple[VersionedHistoricalCandle, ...],
    as_of: datetime,
) -> str:
    payload = {
        "as_of": _timestamp(as_of),
        "candles": [
            {
                "ticker": item.candle.ticker,
                "source_time": _timestamp(item.candle.at),
                "open": _number(item.candle.open),
                "high": _number(item.candle.high),
                "low": _number(item.candle.low),
                "close": _number(item.candle.close),
                "volume": _number(item.candle.volume),
                "complete": item.candle.complete,
                "record_version": item.record_version,
            }
            for item in rows
        ],
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _parse_timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"ClickHouse {field} is not an ISO timestamp") from error
    return _aware_utc(parsed, field)


def _aware_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return (
        _aware_utc(value, "timestamp")
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _number(value: float) -> str:
    return format(value, ".17g")
