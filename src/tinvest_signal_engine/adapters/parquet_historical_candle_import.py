"""Read and independently verify an immutable local Parquet candle cache."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.historical_candle_import import (
    HistoricalCandleImportInventory,
    HistoricalCandlePartition,
    HistoricalCandlePartitionDescriptor,
    HistoricalCandlePartitionKey,
    inventory_fingerprint,
    partition_content_fingerprint,
)
from tinvest_signal_engine.domain.scientific_candles import (
    ScientificCandle,
    scientific_candle_fingerprint,
)


_MOSCOW = ZoneInfo("Europe/Moscow")
_PARTITION = re.compile(r"^date=(\d{4}-\d{2}-\d{2})\.parquet$")
_EXPECTED_FIELDS = {
    "ticker",
    "at",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "complete",
}


class ParquetHistoricalCandleImportSource:
    """No-network source: every imported byte must already exist in the cache."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        tickers: tuple[str, ...] = (),
        start_day: str | None = None,
        end_day: str | None = None,
        max_partitions: int | None = None,
        manifest_only: bool = False,
    ) -> None:
        self._cache_dir = Path(cache_dir).expanduser().resolve()
        self._tickers = frozenset(item.strip().upper() for item in tickers if item.strip())
        self._start_day = start_day
        self._end_day = end_day
        self._manifest_only = manifest_only
        if max_partitions is not None and max_partitions <= 0:
            raise ValueError("max_partitions must be positive")
        self._max_partitions = max_partitions
        self._inventory: HistoricalCandleImportInventory | None = None
        self._paths: dict[HistoricalCandlePartitionKey, Path] = {}
        self._manifest_rows: dict[str, int] = {}
        self._duckdb_connection: Any | None = None

    def close(self) -> None:
        if self._duckdb_connection is not None:
            self._duckdb_connection.close()
            self._duckdb_connection = None

    def inventory(self) -> HistoricalCandleImportInventory:
        if self._inventory is not None:
            return self._inventory
        manifest_path = self._cache_dir / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("historical cache manifest.json is missing")
        manifest_bytes = manifest_path.read_bytes()
        manifest = _manifest(manifest_bytes)
        self._manifest_rows = _manifest_rows(manifest)
        descriptors: list[HistoricalCandlePartitionDescriptor] = []
        for path in sorted(self._cache_dir.glob("ticker=*/date=*.parquet")):
            descriptor = self._descriptor(path)
            if descriptor is None:
                continue
            if (
                self._manifest_only
                and descriptor.key.manifest_key not in self._manifest_rows
            ):
                continue
            self._paths[descriptor.key] = path
            descriptors.append(descriptor)
        descriptors.sort(key=lambda item: item.key)
        if self._max_partitions is not None:
            descriptors = descriptors[: self._max_partitions]
            self._paths = {item.key: self._paths[item.key] for item in descriptors}
        selected_keys = {item.key.manifest_key for item in descriptors}
        covered = len(selected_keys.intersection(self._manifest_rows))
        if self._manifest_only:
            expected = sorted(
                key for key in self._manifest_rows if self._manifest_key_selected(key)
            )
            if self._max_partitions is not None:
                expected = expected[: self._max_partitions]
            if selected_keys != set(expected):
                raise ValueError(
                    "manifest-only historical inventory differs from sealed partitions"
                )
        if not descriptors:
            raise ValueError("historical candle cache contains no selected partitions")
        source_manifest_checksum = "sha256:" + sha256(manifest_bytes).hexdigest()
        frozen = tuple(descriptors)
        self._inventory = HistoricalCandleImportInventory(
            cache_kind=str(manifest["kind"]),
            source_manifest_checksum=source_manifest_checksum,
            inventory_fingerprint=inventory_fingerprint(frozen),
            partitions=frozen,
            manifest_covered_partitions=covered,
        )
        return self._inventory

    def load_partition(
        self,
        descriptor: HistoricalCandlePartitionDescriptor,
        *,
        instrument_id: str,
        received_at: datetime,
    ) -> HistoricalCandlePartition:
        self.inventory()
        path = self._paths.get(descriptor.key)
        if path is None:
            raise ValueError("partition is outside the sealed source inventory")
        if _file_checksum(path) != descriptor.file_checksum:
            raise ValueError("historical partition changed after inventory sealing")
        rows = self._read(path)
        expected_rows = self._manifest_rows.get(descriptor.key.manifest_key)
        if expected_rows is not None and expected_rows != len(rows):
            raise ValueError("historical partition row count differs from manifest")
        candles: list[ScientificCandle] = []
        previous: datetime | None = None
        seen: set[datetime] = set()
        for row in rows:
            at = _timestamp(row["at"])
            if at in seen:
                raise ValueError("historical partition contains duplicate timestamps")
            seen.add(at)
            if at.astimezone(_MOSCOW).date() != descriptor.key.trading_day:
                raise ValueError("historical timestamp is outside its Moscow trading day")
            ticker = str(row["ticker"]).strip().upper()
            if ticker != descriptor.key.ticker:
                raise ValueError("historical partition contains another ticker")
            complete = _boolean(row["complete"])
            if not complete:
                raise ValueError("historical partition contains incomplete candle")
            prices = tuple(
                _decimal(row[field]) for field in ("open", "high", "low", "close")
            )
            volume = _volume(row["volume"])
            source_at = at + timedelta(minutes=1)
            source_event_id = (
                f"backfill-v1:{instrument_id}:"
                f"{at.astimezone(timezone.utc).isoformat(timespec='microseconds')}"
            )
            has_gap = previous is not None and at - previous > timedelta(minutes=1)
            fingerprint = scientific_candle_fingerprint(
                instrument_id=instrument_id,
                ticker=ticker,
                exchange="TQBR",
                candle_at=at,
                open_price=prices[0],
                high_price=prices[1],
                low_price=prices[2],
                close_price=prices[3],
                volume=volume,
                complete=True,
                source_kind="backfill",
                source_at=source_at,
                source_event_id=source_event_id,
                has_gap=has_gap,
                schema_version="scientific-candle-v1",
            )
            candles.append(
                ScientificCandle(
                    instrument_id=instrument_id,
                    ticker=ticker,
                    exchange="TQBR",
                    trading_day=descriptor.key.trading_day,
                    candle_at=at,
                    open_price=prices[0],
                    high_price=prices[1],
                    low_price=prices[2],
                    close_price=prices[3],
                    volume=volume,
                    complete=True,
                    source_kind="backfill",
                    source_at=source_at,
                    received_at=_aware_utc(received_at),
                    source_event_id=source_event_id,
                    payload_fingerprint=fingerprint,
                    has_gap=has_gap,
                )
            )
            previous = at
        ordered = tuple(sorted(candles, key=lambda item: item.candle_at))
        return HistoricalCandlePartition(
            descriptor=descriptor,
            candles=ordered,
            content_fingerprint=partition_content_fingerprint(ordered),
        )

    def _descriptor(
        self,
        path: Path,
    ) -> HistoricalCandlePartitionDescriptor | None:
        ticker_dir = path.parent.name
        match = _PARTITION.fullmatch(path.name)
        if not ticker_dir.startswith("ticker=") or match is None:
            return None
        ticker = ticker_dir.removeprefix("ticker=").strip().upper()
        day = match.group(1)
        if self._tickers and ticker not in self._tickers:
            return None
        if self._start_day is not None and day < self._start_day:
            return None
        if self._end_day is not None and day > self._end_day:
            return None
        size = path.stat().st_size
        if size <= 0:
            raise ValueError(f"historical partition is empty on disk: {ticker}/{day}")
        return HistoricalCandlePartitionDescriptor(
            key=HistoricalCandlePartitionKey(ticker, datetime.fromisoformat(day).date()),
            file_checksum=_file_checksum(path),
            file_size=size,
        )

    def _manifest_key_selected(self, key: str) -> bool:
        try:
            ticker, day = key.split("/", maxsplit=1)
        except ValueError as error:
            raise ValueError("historical manifest partition key is invalid") from error
        ticker = ticker.strip().upper()
        if not ticker or not _PARTITION.fullmatch(f"date={day}.parquet"):
            raise ValueError("historical manifest partition key is invalid")
        if self._tickers and ticker not in self._tickers:
            return False
        if self._start_day is not None and day < self._start_day:
            return False
        if self._end_day is not None and day > self._end_day:
            return False
        return True

    def _read(self, path: Path) -> tuple[dict[str, object], ...]:
        if self._duckdb_connection is None:
            self._duckdb_connection = _duckdb().connect(database=":memory:")
        connection = self._duckdb_connection
        schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
        fields = {str(row[0]) for row in schema}
        if not _EXPECTED_FIELDS.issubset(fields):
            raise ValueError("historical partition schema is incomplete")
        # Casting the zoned timestamp to text avoids a runtime pytz dependency and
        # keeps the explicit offset available for strict validation below.
        rows = connection.execute(
            'SELECT ticker, CAST("at" AS VARCHAR) AS at, open, high, low, '
            "close, volume, complete FROM read_parquet(?) ORDER BY \"at\"",
            [str(path)],
        ).fetchall()
        columns = tuple(item[0] for item in connection.description)
        return tuple(dict(zip(columns, row)) for row in rows)


def _manifest(raw: bytes) -> Mapping[str, object]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("historical cache manifest is not valid UTF-8 JSON") from error
    if not isinstance(payload, Mapping):
        raise ValueError("historical cache manifest must be an object")
    if payload.get("kind") != "tinvest_research_candle_cache":
        raise ValueError("historical cache kind is unsupported")
    if payload.get("schema_version") != 1:
        raise ValueError("historical cache manifest schema is unsupported")
    scope = payload.get("scope")
    if not isinstance(scope, Mapping):
        raise ValueError("historical cache scope is missing")
    if scope.get("interval") != "1m":
        raise ValueError("historical cache is not one-minute data")
    if scope.get("source_type") != "CANDLE_SOURCE_EXCHANGE":
        raise ValueError("historical cache is not exchange candle data")
    privacy = payload.get("privacy")
    if isinstance(privacy, Mapping) and any(bool(value) for value in privacy.values()):
        raise ValueError("historical cache manifest reports persisted private data")
    quality = payload.get("quality")
    if not isinstance(quality, Mapping):
        raise ValueError("historical cache quality section is missing")
    failures = quality.get("failed_partitions", ())
    if failures:
        raise ValueError("historical cache manifest contains failed partitions")
    return payload


def _manifest_rows(manifest: Mapping[str, object]) -> dict[str, int]:
    quality = manifest["quality"]
    assert isinstance(quality, Mapping)
    raw = quality.get("rows_by_partition", {})
    if not isinstance(raw, Mapping):
        raise ValueError("historical manifest rows_by_partition must be an object")
    result: dict[str, int] = {}
    for key, value in raw.items():
        count = int(value)
        if count < 0:
            raise ValueError("historical manifest row count is negative")
        result[str(key)] = count
    declared = quality.get("partition_count")
    if declared is not None and int(declared) != len(result):
        raise ValueError("historical manifest partition count does not reconcile")
    return result


def _timestamp(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("historical candle timestamp is not ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("historical candle timestamp has no explicit timezone")
    return parsed.astimezone(timezone.utc)


def _decimal(value: object) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError("historical candle price is missing")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("historical candle price is invalid") from error
    if not result.is_finite() or result <= 0:
        raise ValueError("historical candle price must be finite and positive")
    return result


def _volume(value: object) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError("historical candle volume is invalid") from error
    if not math.isfinite(numeric) or numeric < 0 or not numeric.is_integer():
        raise ValueError("historical candle volume must be a non-negative integer")
    return int(numeric)


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError("historical candle completion flag is invalid")


def _file_checksum(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("historical import received_at must be timezone-aware")
    return value.astimezone(timezone.utc)


def _duckdb() -> Any:
    try:
        import duckdb  # type: ignore
    except ImportError as error:
        raise RuntimeError("DuckDB is required to import the Parquet cache") from error
    return duckdb
