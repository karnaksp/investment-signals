"""Domain values for importing an immutable historical candle cache."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
import json

from tinvest_signal_engine.domain.scientific_candles import ScientificCandle


class HistoricalImportState(StrEnum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True, order=True)
class HistoricalCandlePartitionKey:
    ticker: str
    trading_day: date

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if not ticker:
            raise ValueError("historical partition ticker is required")
        object.__setattr__(self, "ticker", ticker)

    @property
    def manifest_key(self) -> str:
        return f"{self.ticker}/{self.trading_day.isoformat()}"


@dataclass(frozen=True, slots=True)
class HistoricalCandlePartitionDescriptor:
    key: HistoricalCandlePartitionKey
    file_checksum: str
    file_size: int

    def __post_init__(self) -> None:
        _sha256(self.file_checksum, "file_checksum")
        if self.file_size <= 0:
            raise ValueError("historical partition file_size must be positive")


@dataclass(frozen=True, slots=True)
class HistoricalCandleImportInventory:
    cache_kind: str
    source_manifest_checksum: str
    inventory_fingerprint: str
    partitions: tuple[HistoricalCandlePartitionDescriptor, ...]
    manifest_covered_partitions: int

    def __post_init__(self) -> None:
        if not self.cache_kind.strip():
            raise ValueError("cache_kind is required")
        _sha256(self.source_manifest_checksum, "source_manifest_checksum")
        _sha256(self.inventory_fingerprint, "inventory_fingerprint")
        if not self.partitions:
            raise ValueError("historical import inventory must contain partitions")
        keys = tuple(item.key for item in self.partitions)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("historical import partitions must be unique and sorted")
        if not 0 <= self.manifest_covered_partitions <= len(self.partitions):
            raise ValueError("manifest coverage is outside the inventory")


@dataclass(frozen=True, slots=True)
class HistoricalCandlePartition:
    descriptor: HistoricalCandlePartitionDescriptor
    candles: tuple[ScientificCandle, ...]
    content_fingerprint: str

    def __post_init__(self) -> None:
        _sha256(self.content_fingerprint, "content_fingerprint")
        timestamps: set[datetime] = set()
        for candle in self.candles:
            if candle.source_kind != "backfill":
                raise ValueError("historical import accepts only backfill candles")
            if candle.ticker != self.descriptor.key.ticker:
                raise ValueError("historical partition contains another ticker")
            if candle.trading_day != self.descriptor.key.trading_day:
                raise ValueError("historical partition contains another trading day")
            timestamp = candle.candle_at.astimezone(timezone.utc)
            if timestamp in timestamps:
                raise ValueError("historical partition contains duplicate timestamps")
            timestamps.add(timestamp)
        ordered = tuple(
            sorted(self.candles, key=lambda item: item.candle_at.astimezone(timezone.utc))
        )
        if ordered != self.candles:
            raise ValueError("historical partition candles must be causally ordered")
        expected = partition_content_fingerprint(self.candles)
        if expected != self.content_fingerprint:
            raise ValueError("historical partition content fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class PersistedCandleSnapshot:
    instrument_id: str
    ticker: str
    trading_day: date
    candle_at: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    complete: bool
    source_kind: str
    payload_fingerprint: str

    def __post_init__(self) -> None:
        if not self.instrument_id.strip() or not self.ticker.strip():
            raise ValueError("persisted candle identity is required")
        if self.candle_at.tzinfo is None or self.candle_at.utcoffset() is None:
            raise ValueError("persisted candle timestamp must be timezone-aware")
        if self.source_kind not in {"backfill", "stream"}:
            raise ValueError("persisted candle source kind is invalid")
        _sha256(self.payload_fingerprint, "payload_fingerprint")

    @property
    def market_fingerprint(self) -> str:
        return market_candle_fingerprint(
            instrument_id=self.instrument_id,
            ticker=self.ticker,
            trading_day=self.trading_day,
            candle_at=self.candle_at,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            volume=self.volume,
            complete=self.complete,
        )


@dataclass(frozen=True, slots=True)
class HistoricalImportPartitionProgress:
    key: HistoricalCandlePartitionKey
    file_checksum: str
    content_fingerprint: str
    source_rows: int
    inserted_rows: int
    existing_rows: int
    gap_rows: int
    completed_at: datetime

    def __post_init__(self) -> None:
        _sha256(self.file_checksum, "file_checksum")
        _sha256(self.content_fingerprint, "content_fingerprint")
        if min(
            self.source_rows,
            self.inserted_rows,
            self.existing_rows,
            self.gap_rows,
        ) < 0:
            raise ValueError("historical progress counters must be non-negative")
        if self.inserted_rows + self.existing_rows != self.source_rows:
            raise ValueError("historical progress counters do not reconcile")
        if self.gap_rows > self.source_rows:
            raise ValueError("historical gap count exceeds source rows")
        _aware(self.completed_at, "completed_at")


@dataclass(frozen=True, slots=True)
class HistoricalCandleImportProgress:
    run_id: str
    state: HistoricalImportState
    inventory_fingerprint: str
    source_manifest_checksum: str
    started_at: datetime
    updated_at: datetime
    total_partitions: int
    manifest_covered_partitions: int
    partitions: tuple[HistoricalImportPartitionProgress, ...] = ()
    failure_reason_code: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("historical import run_id is required")
        _sha256(self.inventory_fingerprint, "inventory_fingerprint")
        _sha256(self.source_manifest_checksum, "source_manifest_checksum")
        _aware(self.started_at, "started_at")
        _aware(self.updated_at, "updated_at")
        if self.updated_at < self.started_at:
            raise ValueError("historical import update precedes its start")
        if self.total_partitions <= 0:
            raise ValueError("historical import total_partitions must be positive")
        if not 0 <= self.manifest_covered_partitions <= self.total_partitions:
            raise ValueError("historical import manifest coverage is invalid")
        keys = tuple(item.key for item in self.partitions)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("historical progress partitions must be unique and sorted")
        if len(keys) > self.total_partitions:
            raise ValueError("historical progress exceeds total partitions")
        if self.state is HistoricalImportState.FAILED and not self.failure_reason_code:
            raise ValueError("failed import must have a reason code")
        if self.state is not HistoricalImportState.FAILED and self.failure_reason_code:
            raise ValueError("only failed import may have a reason code")


@dataclass(frozen=True, slots=True)
class HistoricalCandleImportResult:
    run_id: str
    state: HistoricalImportState
    dry_run: bool
    inventory_fingerprint: str
    total_partitions: int
    completed_partitions: int
    source_rows: int
    inserted_rows: int
    existing_rows: int
    manifest_covered_partitions: int
    insert_batches: int = 0
    query_batches: int = 0
    gap_rows: int = 0

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("historical import result run_id is required")
        _sha256(self.inventory_fingerprint, "inventory_fingerprint")
        counters = (
            self.total_partitions,
            self.completed_partitions,
            self.source_rows,
            self.inserted_rows,
            self.existing_rows,
            self.manifest_covered_partitions,
            self.insert_batches,
            self.query_batches,
            self.gap_rows,
        )
        if min(counters) < 0 or self.completed_partitions > self.total_partitions:
            raise ValueError("historical import result counters are invalid")


def scientific_market_fingerprint(candle: ScientificCandle) -> str:
    return market_candle_fingerprint(
        instrument_id=candle.instrument_id,
        ticker=candle.ticker,
        trading_day=candle.trading_day,
        candle_at=candle.candle_at,
        open_price=candle.open_price,
        high_price=candle.high_price,
        low_price=candle.low_price,
        close_price=candle.close_price,
        volume=candle.volume,
        complete=candle.complete,
    )


def market_candle_fingerprint(
    *,
    instrument_id: str,
    ticker: str,
    trading_day: date,
    candle_at: datetime,
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    volume: int,
    complete: bool,
) -> str:
    payload = {
        "candle_at": _utc(candle_at),
        "close_price": _decimal(close_price),
        "complete": complete,
        "high_price": _decimal(high_price),
        "instrument_id": instrument_id,
        "low_price": _decimal(low_price),
        "open_price": _decimal(open_price),
        "ticker": ticker,
        "trading_day": trading_day.isoformat(),
        "volume": volume,
    }
    return _payload_fingerprint(payload)


def partition_content_fingerprint(candles: tuple[ScientificCandle, ...]) -> str:
    digest = sha256()
    for candle in candles:
        digest.update(scientific_market_fingerprint(candle).encode("ascii"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def inventory_fingerprint(
    descriptors: tuple[HistoricalCandlePartitionDescriptor, ...],
) -> str:
    digest = sha256()
    for item in descriptors:
        digest.update(item.key.manifest_key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(item.file_checksum.encode("ascii"))
        digest.update(b"\0")
        digest.update(str(item.file_size).encode("ascii"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def _payload_fingerprint(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")


def _utc(value: datetime) -> str:
    _aware(value, "timestamp")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _sha256(value: str, field: str) -> None:
    raw = value.removeprefix("sha256:")
    if len(raw) != 64 or any(char not in "0123456789abcdef" for char in raw):
        raise ValueError(f"{field} must be a sha256 fingerprint")


def _decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")
