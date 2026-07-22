"""Point-in-time candle cache assembled from local history and ClickHouse."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
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
ORDER BY ticker, candle_at, record_version
SETTINGS
    max_execution_time = 300,
    max_threads = 1,
    max_block_size = 8192,
    output_format_parallel_formatting = 0,
    timeout_before_checking_execution_speed = 0,
    max_rows_to_read = 10000000,
    max_result_rows = 10000000,
    result_overflow_mode = 'throw',
    max_bytes_before_external_sort = 33554432
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

    def iter_as_of(
        self,
        as_of: datetime,
    ) -> Iterator[tuple[VersionedHistoricalCandle, ...]]: ...


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
        return tuple(
            row
            for partition in self.iter_as_of(as_of)
            for row in partition
        )

    def iter_as_of(
        self,
        as_of: datetime,
    ) -> Iterator[tuple[VersionedHistoricalCandle, ...]]:
        """Stream ordered, deduplicated ticker partitions from ClickHouse."""

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
                yield from _deduplicated_ticker_partitions(
                    (
                        _versioned_candle(json.loads(line), cutoff=cutoff)
                        for line in _response_lines(response)
                    )
                )
        except HTTPError as error:
            raise RuntimeError(
                f"ClickHouse scientific candle read failed with status {error.code}"
            ) from error
        except URLError as error:
            raise RuntimeError(
                "ClickHouse scientific candle read connection failed"
            ) from error


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
        self._fallback_live_snapshot: tuple[VersionedHistoricalCandle, ...] | None = None
        self._snapshot: tuple[HistoricalCandle, ...] | None = None
        self._descriptor: CandleCacheDescriptor | None = None

    def describe(self) -> CandleCacheDescriptor:
        if self._descriptor is None:
            historical = self._historical.describe()
            self._descriptor = _streaming_composite_descriptor(
                historical,
                self._iter_live_partitions(),
                as_of=self._as_of,
            )
        return self._descriptor

    def load(self) -> tuple[HistoricalCandle, ...]:
        return self._seal()

    def _seal(self) -> tuple[HistoricalCandle, ...]:
        if self._snapshot is not None:
            return self._snapshot
        snapshot = tuple(
            candle
            for partition in self.iter_ticker_partitions()
            for candle in partition
        )
        if not snapshot:
            raise ValueError("composite candle cache contains no causal candles")
        self._snapshot = snapshot
        return snapshot

    def iter_ticker_partitions(self) -> Iterator[tuple[HistoricalCandle, ...]]:
        """Merge local and live revisions while retaining one ticker in memory."""

        historical = _iter_historical_partitions(self._historical, self._as_of)
        live = self._iter_live_partitions()
        historical_partition = next(historical, None)
        live_partition = next(live, None)
        while historical_partition is not None or live_partition is not None:
            historical_ticker = (
                historical_partition[0].ticker
                if historical_partition is not None
                else None
            )
            live_ticker = (
                live_partition[0].candle.ticker
                if live_partition is not None
                else None
            )
            if live_ticker is None or (
                historical_ticker is not None and historical_ticker < live_ticker
            ):
                yield historical_partition  # type: ignore[misc]
                historical_partition = next(historical, None)
                continue
            if historical_ticker is None or live_ticker < historical_ticker:
                yield tuple(item.candle for item in live_partition)  # type: ignore[union-attr]
                live_partition = next(live, None)
                continue
            yield _merge_ticker_partition(
                historical_partition,  # type: ignore[arg-type]
                live_partition,  # type: ignore[arg-type]
            )
            historical_partition = next(historical, None)
            live_partition = next(live, None)

    def _iter_live_partitions(
        self,
    ) -> Iterator[tuple[VersionedHistoricalCandle, ...]]:
        iterator = getattr(self._live, "iter_as_of", None)
        if callable(iterator):
            yield from iterator(self._as_of)
            return
        if self._fallback_live_snapshot is None:
            self._fallback_live_snapshot = _deduplicate(
                self._live.load_as_of(self._as_of)
            )
        rows = self._fallback_live_snapshot
        yield from _partition_versioned_rows(rows)


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


def _streaming_composite_descriptor(
    historical: CandleCacheDescriptor,
    live_partitions: Iterator[tuple[VersionedHistoricalCandle, ...]],
    *,
    as_of: datetime,
) -> CandleCacheDescriptor:
    digest = sha256()
    digest.update(b"composite-scientific-candles-v2\0")
    digest.update(historical.dataset_fingerprint.encode("ascii"))
    digest.update(b"\0")
    digest.update(_timestamp(as_of).encode("ascii"))
    live_days: set[date] = set()
    live_partition_keys: set[tuple[str, date]] = set()
    live_tickers: set[str] = set()
    for partition in live_partitions:
        for item in partition:
            candle = item.candle
            candle_day = candle.at.astimezone(timezone.utc).date()
            live_days.add(candle_day)
            live_partition_keys.add((candle.ticker, candle_day))
            live_tickers.add(candle.ticker)
            digest.update(_versioned_fingerprint_row(item))
    known_historical_partitions = {
        partition
        for partition in live_partition_keys
        if partition[0] in historical.tickers
        and historical.start_day <= partition[1] <= historical.end_day
    }
    all_days = {historical.start_day, historical.end_day, *live_days}
    return CandleCacheDescriptor(
        dataset_fingerprint="sha256:" + digest.hexdigest(),
        partition_count=(
            historical.partition_count
            + len(live_partition_keys - known_historical_partitions)
        ),
        tickers=tuple(sorted({*historical.tickers, *live_tickers})),
        start_day=min(all_days),
        end_day=max(all_days),
    )


def _response_lines(response: object) -> Iterator[str]:
    """Decode one JSONEachRow record at a time for bounded client memory."""

    try:
        iterator = iter(response)  # type: ignore[arg-type]
    except TypeError:
        payload = response.read()  # type: ignore[attr-defined]
        iterator = iter(payload.splitlines())
    for raw_line in iterator:
        line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else str(raw_line)
        if line.strip():
            yield line


def _deduplicated_ticker_partitions(
    rows: Iterator[VersionedHistoricalCandle],
) -> Iterator[tuple[VersionedHistoricalCandle, ...]]:
    """Deduplicate the ordered response with memory bounded by one ticker."""

    ticker: str | None = None
    selected: list[VersionedHistoricalCandle] = []
    current: VersionedHistoricalCandle | None = None
    current_key: tuple[str, datetime] | None = None
    for row in rows:
        key = (row.candle.ticker, row.candle.at.astimezone(timezone.utc))
        if current_key is not None and key < current_key:
            raise ValueError("ClickHouse candle response is not deterministically ordered")
        if current_key == key:
            if row.record_version < current.record_version:  # type: ignore[union-attr]
                raise ValueError("ClickHouse candle revisions are not ordered")
            if (
                row.record_version == current.record_version  # type: ignore[union-attr]
                and row.candle != current.candle  # type: ignore[union-attr]
            ):
                raise ValueError(
                    "conflicting candle payloads share ticker, source_time, and record_version"
                )
            current = row
            continue
        if current is not None:
            selected.append(current)
        if ticker is not None and row.candle.ticker != ticker:
            yield tuple(selected)
            selected = []
        ticker = row.candle.ticker
        current = row
        current_key = key
    if current is not None:
        selected.append(current)
    if selected:
        yield tuple(selected)


def _partition_versioned_rows(
    rows: tuple[VersionedHistoricalCandle, ...],
) -> Iterator[tuple[VersionedHistoricalCandle, ...]]:
    current: list[VersionedHistoricalCandle] = []
    ticker: str | None = None
    for row in rows:
        if ticker is not None and row.candle.ticker != ticker:
            yield tuple(current)
            current = []
        ticker = row.candle.ticker
        current.append(row)
    if current:
        yield tuple(current)


def _iter_historical_partitions(
    historical: HistoricalCandleCachePort,
    as_of: datetime,
) -> Iterator[tuple[HistoricalCandle, ...]]:
    iterator = getattr(historical, "iter_ticker_partitions", None)
    source = iterator() if callable(iterator) else _partition_historical(historical.load())
    for partition in source:
        causal = tuple(
            item
            for item in partition
            if item.at.astimezone(timezone.utc) <= as_of
        )
        if causal:
            yield causal


def _partition_historical(
    rows: tuple[HistoricalCandle, ...],
) -> Iterator[tuple[HistoricalCandle, ...]]:
    current: list[HistoricalCandle] = []
    ticker: str | None = None
    for row in rows:
        if ticker is not None and row.ticker != ticker:
            yield tuple(current)
            current = []
        ticker = row.ticker
        current.append(row)
    if current:
        yield tuple(current)


def _merge_ticker_partition(
    historical: tuple[HistoricalCandle, ...],
    live: tuple[VersionedHistoricalCandle, ...],
) -> tuple[HistoricalCandle, ...]:
    selected = {item.at.astimezone(timezone.utc): item for item in historical}
    selected.update(
        {
            item.candle.at.astimezone(timezone.utc): item.candle
            for item in live
        }
    )
    return tuple(selected[key] for key in sorted(selected))


def _versioned_fingerprint_row(item: VersionedHistoricalCandle) -> bytes:
    candle = item.candle
    values = (
        candle.ticker,
        _timestamp(candle.at),
        _number(candle.open),
        _number(candle.high),
        _number(candle.low),
        _number(candle.close),
        _number(candle.volume),
        "1" if candle.complete else "0",
        str(item.record_version),
    )
    return ("\x1f".join(values) + "\n").encode("ascii")


def _parse_timestamp(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"ClickHouse {field} is not an ISO timestamp") from error
    # ClickHouse JSONEachRow renders DateTime64 columns without an explicit
    # suffix even when their schema timezone is UTC.  This adapter owns that
    # storage contract, so it restores UTC before handing the value to the
    # strict application/domain boundary.
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
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
