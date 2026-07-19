"""Domain values for the reusable owner-local candle cache."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True, order=True)
class CandlePartitionKey:
    ticker: str
    trading_day: date

    def __post_init__(self) -> None:
        normalized = self.ticker.strip().upper()
        if not normalized:
            raise ValueError("partition ticker must not be empty")
        object.__setattr__(self, "ticker", normalized)

    @property
    def manifest_key(self) -> str:
        return f"{self.ticker}/{self.trading_day.isoformat()}"


@dataclass(frozen=True, slots=True)
class CachedCandle:
    ticker: str
    at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_buy: float = 0.0
    volume_sell: float = 0.0
    complete: bool = True

    def __post_init__(self) -> None:
        normalized = self.ticker.strip().upper()
        if not normalized:
            raise ValueError("candle ticker must not be empty")
        object.__setattr__(self, "ticker", normalized)
        if self.at.tzinfo is None or self.at.utcoffset() is None:
            raise ValueError("candle timestamp must be timezone-aware")
        if min(self.open, self.high, self.low, self.close) <= 0.0:
            raise ValueError("candle prices must be positive")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("candle OHLC bounds are inconsistent")
        if min(self.volume, self.volume_buy, self.volume_sell) < 0.0:
            raise ValueError("candle volumes must not be negative")


@dataclass(frozen=True, slots=True)
class CandleCacheScope:
    tickers: tuple[str, ...]
    start_day: date
    end_day: date

    def __post_init__(self) -> None:
        normalized = tuple(dict.fromkeys(item.strip().upper() for item in self.tickers))
        if not normalized or any(not item for item in normalized):
            raise ValueError("cache scope must contain tickers")
        if len(normalized) > 25:
            raise ValueError("cache scope supports at most 25 tickers")
        if self.end_day < self.start_day:
            raise ValueError("cache end_day must not precede start_day")
        object.__setattr__(self, "tickers", normalized)


@dataclass(frozen=True, slots=True)
class CandlePartitionState:
    key: CandlePartitionKey
    valid: bool
    row_count: int = 0

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("partition row_count must not be negative")
        if not self.valid and self.row_count:
            raise ValueError("invalid partition cannot declare rows")


@dataclass(frozen=True, slots=True)
class CandleCacheFailure:
    key: CandlePartitionKey
    reason_code: str

    def __post_init__(self) -> None:
        if not self.reason_code.strip():
            raise ValueError("cache failure reason_code must not be empty")


@dataclass(frozen=True, slots=True)
class CandleCacheInventory:
    dataset_fingerprint: str
    rows_by_partition: tuple[tuple[str, int], ...]
    morning_rows_by_partition: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if len(self.dataset_fingerprint) != 64:
            raise ValueError("dataset_fingerprint must be sha256 hex")
        if any(count < 0 for _, count in self.rows_by_partition):
            raise ValueError("partition row counts must not be negative")
        if any(count <= 0 for _, count in self.morning_rows_by_partition):
            raise ValueError("morning partition row counts must be positive")


@dataclass(frozen=True, slots=True)
class CandleCacheReceipt:
    scope: CandleCacheScope
    inventory: CandleCacheInventory
    skipped_partitions: int
    written_partitions: int
    failures: tuple[CandleCacheFailure, ...]

    def __post_init__(self) -> None:
        if min(self.skipped_partitions, self.written_partitions) < 0:
            raise ValueError("partition counters must not be negative")
