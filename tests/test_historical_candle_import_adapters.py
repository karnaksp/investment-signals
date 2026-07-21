from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

import tinvest_signal_engine.adapters.clickhouse_historical_candle_import as clickhouse_adapter
from tinvest_signal_engine.adapters.candle_cache import (
    JsonCandleCacheManifest,
    ParquetCandlePartitionRepository,
)
from tinvest_signal_engine.adapters.clickhouse_historical_candle_import import (
    ClickHouseHistoricalCandleImportDestination,
)
from tinvest_signal_engine.adapters.parquet_historical_candle_import import (
    ParquetHistoricalCandleImportSource,
)
from tinvest_signal_engine.application.historical_candle_import import (
    HistoricalDestinationPartition,
)
from tinvest_signal_engine.domain.candle_cache import (
    CachedCandle,
    CandleCacheReceipt,
    CandleCacheScope,
    CandlePartitionKey,
)


pytest.importorskip("duckdb")


class _Response:
    def __init__(self, payload: bytes = b"") -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._payload


def _cached(ticker: str, day: date) -> CachedCandle:
    return CachedCandle(
        ticker=ticker,
        at=datetime(day.year, day.month, day.day, 7, 0, tzinfo=UTC),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=100.0,
    )


def _cache_with_extra_partition(tmp_path: Path) -> Path:
    repository = ParquetCandlePartitionRepository(tmp_path)
    first = CandlePartitionKey("SBER", date(2026, 7, 1))
    second = CandlePartitionKey("SBER", date(2026, 7, 2))
    repository.replace_atomically(first, (_cached("SBER", first.trading_day),))
    inventory = repository.inventory((first,))
    JsonCandleCacheManifest(tmp_path).publish(
        CandleCacheReceipt(
            scope=CandleCacheScope(("SBER",), first.trading_day, first.trading_day),
            inventory=inventory,
            skipped_partitions=0,
            written_partitions=1,
            failures=(),
        )
    )
    repository.replace_atomically(second, (_cached("SBER", second.trading_day),))
    return tmp_path


def test_parquet_source_validates_manifest_and_independent_extra_partition(
    tmp_path: Path,
) -> None:
    source = ParquetHistoricalCandleImportSource(_cache_with_extra_partition(tmp_path))
    try:
        inventory = source.inventory()
        first = source.load_partition(
            inventory.partitions[0],
            instrument_id="SBER_TQBR",
            received_at=datetime(2026, 7, 22, tzinfo=UTC),
        )
        second = source.load_partition(
            inventory.partitions[1],
            instrument_id="SBER_TQBR",
            received_at=datetime(2026, 7, 22, tzinfo=UTC),
        )
    finally:
        source.close()

    assert len(inventory.partitions) == 2
    assert inventory.manifest_covered_partitions == 1
    assert len(first.candles) == len(second.candles) == 1
    assert first.candles[0].source_kind == "backfill"
    assert first.candles[0].source_at > first.candles[0].candle_at
    assert first.candles[0].source_event_id.startswith("backfill-v1:SBER_TQBR:")


def test_parquet_source_rejects_manifest_row_count_mismatch(tmp_path: Path) -> None:
    _cache_with_extra_partition(tmp_path)
    path = tmp_path / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["quality"]["rows_by_partition"]["SBER/2026-07-01"] = 2
    path.write_text(json.dumps(manifest), encoding="utf-8")
    source = ParquetHistoricalCandleImportSource(
        tmp_path,
        tickers=("SBER",),
        start_day="2026-07-01",
        end_day="2026-07-01",
    )
    try:
        inventory = source.inventory()
        with pytest.raises(ValueError, match="row count differs"):
            source.load_partition(
                inventory.partitions[0],
                instrument_id="SBER_TQBR",
                received_at=datetime(2026, 7, 22, tzinfo=UTC),
            )
    finally:
        source.close()


def test_clickhouse_inspection_batches_partition_predicates_and_maps_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    requests = []
    response_row = {
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "trading_day": "2026-07-01",
        "candle_at": "2026-07-01 07:00:00.000000",
        "open_price": "100.000000000",
        "high_price": "101.000000000",
        "low_price": "99.000000000",
        "close_price": "100.500000000",
        "volume": 100,
        "is_complete": 1,
        "source_kind": "stream",
        "payload_fingerprint": "a" * 64,
    }

    def urlopen(request, timeout):
        requests.append((request, timeout))
        return _Response((json.dumps(response_row) + "\n").encode())

    monkeypatch.setattr(clickhouse_adapter, "urlopen", urlopen)
    destination = ClickHouseHistoricalCandleImportDestination(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
    )
    cache = _cache_with_extra_partition(tmp_path)
    source = ParquetHistoricalCandleImportSource(cache)
    try:
        descriptors = source.inventory().partitions
    finally:
        source.close()
    rows = destination.inspect_partitions(
        tuple(HistoricalDestinationPartition(item, "SBER_TQBR") for item in descriptors)
    )

    assert len(requests) == 1
    request = requests[0][0]
    body = request.data.decode()
    assert body.count("instrument_id =") == 2
    query = parse_qs(urlparse(request.full_url).query)
    assert query["param_instrument_id_0"] == ["SBER_TQBR"]
    assert query["param_trading_day_1"] == ["2026-07-02"]
    assert len(rows) == 1
    assert rows[0].source_kind == "stream"
    assert rows[0].payload_fingerprint == "sha256:" + "a" * 64
