"""Build an idempotent candle cache through application-owned ports."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol

from tinvest_signal_engine.domain.candle_cache import (
    CachedCandle,
    CandleCacheFailure,
    CandleCacheInventory,
    CandleCacheReceipt,
    CandleCacheScope,
    CandlePartitionKey,
    CandlePartitionState,
)


class CandleHistorySourcePort(Protocol):
    def fetch(self, key: CandlePartitionKey) -> tuple[CachedCandle, ...]: ...


class CandlePartitionRepositoryPort(Protocol):
    def inspect(self, key: CandlePartitionKey) -> CandlePartitionState: ...

    def replace_atomically(
        self,
        key: CandlePartitionKey,
        candles: tuple[CachedCandle, ...],
    ) -> CandlePartitionState: ...

    def inventory(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> CandleCacheInventory: ...


class CandleCacheManifestPort(Protocol):
    def publish(self, receipt: CandleCacheReceipt) -> None: ...


class BuildReusableCandleCache:
    """Fetch only absent or invalid partitions and publish one safe manifest."""

    def __init__(
        self,
        *,
        source: CandleHistorySourcePort,
        repository: CandlePartitionRepositoryPort,
        manifest: CandleCacheManifestPort,
    ) -> None:
        self._source = source
        self._repository = repository
        self._manifest = manifest

    def execute(self, scope: CandleCacheScope) -> CandleCacheReceipt:
        keys = _partition_keys(scope)
        skipped = 0
        written = 0
        failures: list[CandleCacheFailure] = []
        for key in keys:
            current = self._repository.inspect(key)
            if current.valid:
                skipped += 1
                continue
            try:
                candles = self._source.fetch(key)
                stored = self._repository.replace_atomically(key, candles)
                if not stored.valid:
                    raise RuntimeError("partition verification failed after write")
                written += 1
            except Exception:
                failures.append(
                    CandleCacheFailure(
                        key=key,
                        reason_code="tinvest_candle_partition_failed",
                    )
                )
        receipt = CandleCacheReceipt(
            scope=scope,
            inventory=self._repository.inventory(keys),
            skipped_partitions=skipped,
            written_partitions=written,
            failures=tuple(failures),
        )
        self._manifest.publish(receipt)
        return receipt


def _partition_keys(scope: CandleCacheScope) -> tuple[CandlePartitionKey, ...]:
    keys: list[CandlePartitionKey] = []
    day = scope.start_day
    while day <= scope.end_day:
        keys.extend(CandlePartitionKey(ticker, day) for ticker in scope.tickers)
        day += timedelta(days=1)
    return tuple(keys)
