"""Point-in-time candle cache assembled from local history and ClickHouse."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from typing import Callable, Protocol, TypeVar
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


INSTRUMENT_RANGES_SQL = """
SELECT
    instrument_id,
    ticker,
    min(candle_at) AS first_candle_at
FROM scientific_candles_1m
WHERE candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND source_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND received_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
GROUP BY instrument_id, ticker
ORDER BY ticker, instrument_id
SETTINGS
    max_execution_time = 120,
    max_threads = 1,
    max_block_size = 8192,
    output_format_parallel_formatting = 0,
    timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()


SELECT_CHUNK_SQL = """
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
PREWHERE instrument_id = {instrument_id:String}
  AND trading_day >= toDate(parseDateTime64BestEffort({window_start:String}, 6, 'UTC'))
  AND trading_day <= toDate(parseDateTime64BestEffort({window_end:String}, 6, 'UTC'))
WHERE ticker = {ticker:String}
  AND candle_at >= parseDateTime64BestEffort({window_start:String}, 6, 'UTC')
  AND candle_at < parseDateTime64BestEffort({window_end:String}, 6, 'UTC')
  AND candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND source_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND received_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
ORDER BY candle_at, record_version
SETTINGS
    max_execution_time = 60,
    max_threads = 1,
    max_block_size = 8192,
    output_format_parallel_formatting = 0,
    timeout_before_checking_execution_speed = 0,
    max_result_rows = 1000000,
    result_overflow_mode = 'throw',
    max_bytes_before_external_sort = 16777216
FORMAT JSONEachRow
""".strip()

_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_ERROR_DETAIL_LIMIT = 320
_DEFAULT_CHUNK_DAYS = 31
_DEFAULT_MAX_ATTEMPTS = 4

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class VersionedHistoricalCandle:
    """Storage revision retained until the composite snapshot is sealed."""

    candle: HistoricalCandle
    record_version: int

    def __post_init__(self) -> None:
        if self.record_version < 0:
            raise ValueError("candle record_version must not be negative")


@dataclass(frozen=True, slots=True)
class _InstrumentRange:
    instrument_id: str
    ticker: str
    first_candle_at: datetime


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
        chunk_days: int = _DEFAULT_CHUNK_DAYS,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        retry_backoff_seconds: float = 0.25,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if not database.strip():
            raise ValueError("ClickHouse database must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        if chunk_days <= 0:
            raise ValueError("ClickHouse candle chunk_days must be positive")
        if max_attempts <= 0:
            raise ValueError("ClickHouse max_attempts must be positive")
        if retry_backoff_seconds < 0:
            raise ValueError("ClickHouse retry backoff must not be negative")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds
        self._chunk_days = chunk_days
        self._max_attempts = max_attempts
        self._retry_backoff_seconds = retry_backoff_seconds

    def load_as_of(self, as_of: datetime) -> tuple[VersionedHistoricalCandle, ...]:
        return tuple(row for partition in self.iter_as_of(as_of) for row in partition)

    def iter_as_of(
        self,
        as_of: datetime,
    ) -> Iterator[tuple[VersionedHistoricalCandle, ...]]:
        """Read retry-safe chunks while retaining at most one ticker in memory."""

        cutoff = _aware_utc(as_of, "as_of")
        ranges = self._request_rows(
            sql=INSTRUMENT_RANGES_SQL,
            parameters={"as_of": _timestamp(cutoff)},
            parser=lambda row: _instrument_range(row, cutoff=cutoff),
            context="instrument discovery",
        )
        previous_range: tuple[str, str] | None = None
        ticker_rows: list[VersionedHistoricalCandle] = []
        current_ticker: str | None = None
        for instrument_range in ranges:
            range_key = (instrument_range.ticker, instrument_range.instrument_id)
            if previous_range is not None and range_key <= previous_range:
                raise ValueError(
                    "ClickHouse scientific candle instruments are not ordered"
                )
            previous_range = range_key
            if current_ticker is not None and instrument_range.ticker != current_ticker:
                partition = _deduplicate(tuple(ticker_rows))
                if partition:
                    yield partition
                ticker_rows = []
            current_ticker = instrument_range.ticker
            for window_start, window_end in _time_windows(
                instrument_range.first_candle_at,
                cutoff,
                chunk_days=self._chunk_days,
            ):
                ticker_rows.extend(
                    self._request_rows(
                        sql=SELECT_CHUNK_SQL,
                        parameters={
                            "as_of": _timestamp(cutoff),
                            "instrument_id": instrument_range.instrument_id,
                            "ticker": instrument_range.ticker,
                            "window_start": _timestamp(window_start),
                            "window_end": _timestamp(window_end),
                        },
                        parser=lambda row: _versioned_candle(row, cutoff=cutoff),
                        context=(
                            f"instrument {instrument_range.ticker} "
                            f"[{_timestamp(window_start)}, {_timestamp(window_end)})"
                        ),
                    )
                )
        partition = _deduplicate(tuple(ticker_rows))
        if partition:
            yield partition

    def _request_rows(
        self,
        *,
        sql: str,
        parameters: dict[str, str],
        parser: Callable[[object], _T],
        context: str,
    ) -> tuple[_T, ...]:
        """Buffer one bounded request so a partial response can be retried safely."""

        query_parameters = {
            "database": self._database,
            "date_time_input_format": "best_effort",
            **{f"param_{key}": value for key, value in parameters.items()},
        }
        request = Request(
            f"{self._base_url}/?{urlencode(query_parameters)}",
            data=(sql + "\n").encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        for attempt in range(1, self._max_attempts + 1):
            try:
                with urlopen(request, timeout=self._timeout_seconds) as response:
                    return tuple(
                        parser(json.loads(line)) for line in _response_lines(response)
                    )
            except HTTPError as error:
                if (
                    error.code not in _RETRYABLE_HTTP_STATUSES
                    or attempt >= self._max_attempts
                ):
                    detail = _http_error_detail(error)
                    raise RuntimeError(
                        "ClickHouse scientific candle read failed with status "
                        f"{error.code} during {context}{detail}"
                    ) from error
            except (TimeoutError, URLError, OSError) as error:
                if attempt >= self._max_attempts:
                    raise RuntimeError(
                        "ClickHouse scientific candle read connection failed "
                        f"during {context}"
                    ) from error
            if self._retry_backoff_seconds:
                sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))
        raise AssertionError("unreachable ClickHouse retry state")


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
        self._fallback_live_snapshot: tuple[VersionedHistoricalCandle, ...] | None = (
            None
        )
        self._snapshot: tuple[HistoricalCandle, ...] | None = None
        self._descriptor: CandleCacheDescriptor | None = None
        self._partition_store: TemporaryDirectory[str] | None = None
        self._partition_paths: tuple[Path, ...] = ()
        self._partition_checksums: tuple[str, ...] = ()

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
        if self._partition_paths:
            return tuple(
                candle
                for partition in self.iter_ticker_partitions()
                for candle in partition
            )
        return self._seal()

    def materialize_ticker_partitions(self, working_root: str | Path) -> None:
        """Seal one immutable disk-backed view without retaining all candles.

        A portfolio evaluates several independently versioned hypothesis
        families against the same point-in-time dataset.  This method performs
        the expensive merge once, stores one file per ticker, and lets every
        family reread bounded partitions without another ClickHouse scan.
        """

        if self._partition_paths:
            return
        root = Path(working_root)
        root.mkdir(parents=True, exist_ok=True)
        store = TemporaryDirectory(prefix="candle-partitions-", dir=root)
        paths: list[Path] = []
        checksums: list[str] = []
        previous_ticker: str | None = None
        try:
            for index, partition in enumerate(self._iter_merged_partitions()):
                if not partition:
                    continue
                tickers = {item.ticker for item in partition}
                if len(tickers) != 1:
                    raise ValueError(
                        "materialized candle partitions require one ticker"
                    )
                ticker = next(iter(tickers))
                if previous_ticker is not None and ticker <= previous_ticker:
                    raise ValueError(
                        "materialized candle partitions must be ticker ordered"
                    )
                if any(
                    left.at >= right.at for left, right in zip(partition, partition[1:])
                ):
                    raise ValueError(
                        "materialized candle partitions must be time ordered"
                    )
                path = Path(store.name) / f"{index:04d}.jsonl"
                _write_materialized_partition(path, partition)
                paths.append(path)
                checksums.append(_file_sha256(path))
                previous_ticker = ticker
            if not paths:
                raise ValueError("composite candle cache contains no causal candles")
        except Exception:
            store.cleanup()
            raise
        self._partition_store = store
        self._partition_paths = tuple(paths)
        self._partition_checksums = tuple(checksums)
        # Fallback sources may have needed a full live tuple during sealing.
        # It is now represented by the private disk store and can be released.
        self._fallback_live_snapshot = None
        self._snapshot = None

    def close_materialized_partitions(self) -> None:
        """Release the private working set after one replay job."""

        self._partition_paths = ()
        self._partition_checksums = ()
        self._snapshot = None
        self._fallback_live_snapshot = None
        if self._partition_store is not None:
            self._partition_store.cleanup()
            self._partition_store = None

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

        if self._partition_paths:
            for path, expected_checksum in zip(
                self._partition_paths,
                self._partition_checksums,
                strict=True,
            ):
                yield _read_materialized_partition(path, expected_checksum)
            return
        # ``load`` seals the exact point-in-time composite used by the legacy,
        # R2, and next-candle engines.  Portfolio replay then asks for the same
        # ticker partitions once per prospective hypothesis.  Re-querying
        # ClickHouse here would both repeat the expensive ordered 2.5M-row
        # scan and needlessly rebuild identical domain objects.  Partition the
        # already sealed immutable tuple instead; no formula or cutoff changes.
        if self._snapshot is not None:
            yield from _partition_historical(self._snapshot)
            return

        yield from self._iter_merged_partitions()

    def _iter_merged_partitions(
        self,
    ) -> Iterator[tuple[HistoricalCandle, ...]]:
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
                live_partition[0].candle.ticker if live_partition is not None else None
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


def _instrument_range(
    row: object,
    *,
    cutoff: datetime,
) -> _InstrumentRange:
    if not isinstance(row, dict):
        raise ValueError("ClickHouse instrument range row must be an object")
    instrument_id = str(row.get("instrument_id", "")).strip()
    ticker = str(row.get("ticker", "")).strip().upper()
    first_candle_at = _parse_timestamp(
        row.get("first_candle_at"),
        "first_candle_at",
    )
    if not instrument_id:
        raise ValueError("ClickHouse instrument range has no instrument_id")
    if not ticker:
        raise ValueError("ClickHouse instrument range has no ticker")
    if first_candle_at > cutoff:
        raise ValueError("ClickHouse instrument range begins beyond the causal cutoff")
    return _InstrumentRange(
        instrument_id=instrument_id,
        ticker=ticker,
        first_candle_at=first_candle_at,
    )


def _time_windows(
    first_candle_at: datetime,
    cutoff: datetime,
    *,
    chunk_days: int,
) -> Iterator[tuple[datetime, datetime]]:
    """Build adjacent half-open windows, including a candle exactly at cutoff."""

    cursor = _aware_utc(first_candle_at, "first_candle_at")
    inclusive_cutoff = _aware_utc(cutoff, "cutoff") + timedelta(microseconds=1)
    step = timedelta(days=chunk_days)
    while cursor < inclusive_cutoff:
        window_end = min(cursor + step, inclusive_cutoff)
        yield cursor, window_end
        cursor = window_end


def _http_error_detail(error: HTTPError) -> str:
    """Return a bounded ClickHouse diagnostic without request headers or secrets."""

    try:
        payload = error.read(_ERROR_DETAIL_LIMIT + 1)
    except (OSError, ValueError):
        return ""
    if not payload:
        return ""
    text = payload.decode("utf-8", errors="replace")
    compact = " ".join(text.split())
    if len(compact) > _ERROR_DETAIL_LIMIT:
        compact = compact[:_ERROR_DETAIL_LIMIT] + "…"
    return f": {compact}" if compact else ""


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
        line = (
            raw_line.decode("utf-8") if isinstance(raw_line, bytes) else str(raw_line)
        )
        if line.strip():
            yield line


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
    source = (
        iterator() if callable(iterator) else _partition_historical(historical.load())
    )
    for partition in source:
        causal = tuple(
            item for item in partition if item.at.astimezone(timezone.utc) <= as_of
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
        {item.candle.at.astimezone(timezone.utc): item.candle for item in live}
    )
    return tuple(selected[key] for key in sorted(selected))


def _write_materialized_partition(
    path: Path,
    partition: tuple[HistoricalCandle, ...],
) -> None:
    """Write a private, safe and exactly round-trippable working partition."""

    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for candle in partition:
            handle.write(
                json.dumps(
                    (
                        candle.ticker,
                        candle.at.isoformat(timespec="microseconds"),
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                        candle.complete,
                    ),
                    ensure_ascii=True,
                    allow_nan=False,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")


def _read_materialized_partition(
    path: Path,
    expected_checksum: str,
) -> tuple[HistoricalCandle, ...]:
    rows: list[HistoricalCandle] = []
    raw_partition = path.read_bytes()
    if sha256(raw_partition).hexdigest() != expected_checksum:
        raise ValueError("materialized candle partition failed checksum validation")
    for raw_line in raw_partition.splitlines():
        if not raw_line.strip():
            continue
        payload = json.loads(raw_line)
        if not isinstance(payload, list) or len(payload) != 8:
            raise ValueError("materialized candle partition is invalid")
        if not isinstance(payload[7], bool):
            raise ValueError("materialized candle completeness flag is invalid")
        rows.append(
            HistoricalCandle(
                ticker=str(payload[0]),
                at=datetime.fromisoformat(str(payload[1])),
                open=float(payload[2]),
                high=float(payload[3]),
                low=float(payload[4]),
                close=float(payload[5]),
                volume=float(payload[6]),
                complete=payload[7],
            )
        )
    partition = tuple(rows)
    if not partition:
        raise ValueError("materialized candle partition is empty")
    return partition


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
