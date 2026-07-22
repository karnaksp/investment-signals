from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import ssl

import httpx
import pytest

import tinvest_signal_engine.adapters.candle_cache as candle_cache_adapter
from tinvest_signal_engine.adapters.candle_cache import (
    JsonCandleCacheManifest,
    ParquetCandlePartitionRepository,
    TInvestRestCandleHistorySource,
)
from tinvest_signal_engine.adapters.local_hypothesis_replay import LocalCandleCache
from tinvest_signal_engine.application.candle_cache import BuildReusableCandleCache
from tinvest_signal_engine.domain.candle_cache import (
    CachedCandle,
    CandleCacheScope,
    CandlePartitionKey,
    CandlePartitionState,
)


pytest.importorskip("duckdb")


class _Source:
    def __init__(self, rows: dict[str, tuple[CachedCandle, ...] | Exception]) -> None:
        self.rows = rows
        self.calls: list[str] = []

    def fetch(self, key: CandlePartitionKey) -> tuple[CachedCandle, ...]:
        self.calls.append(key.manifest_key)
        result = self.rows[key.manifest_key]
        if isinstance(result, Exception):
            raise result
        return result


def _candle(ticker: str, day: date, *, close: float = 100.0) -> CachedCandle:
    return CachedCandle(
        ticker=ticker,
        at=datetime(day.year, day.month, day.day, 4, 0, tzinfo=UTC),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=100.0,
        volume_buy=60.0,
        volume_sell=40.0,
    )


def _use_case(cache_dir: Path, source: _Source) -> BuildReusableCandleCache:
    return BuildReusableCandleCache(
        source=source,
        repository=ParquetCandlePartitionRepository(cache_dir),
        manifest=JsonCandleCacheManifest(cache_dir),
    )


def test_cache_resume_skips_valid_partition_and_keeps_fingerprint(
    tmp_path: Path,
) -> None:
    day = date(2026, 7, 15)
    key = CandlePartitionKey("SBER", day)
    source = _Source({key.manifest_key: (_candle("SBER", day),)})
    scope = CandleCacheScope(("SBER",), day, day)

    first = _use_case(tmp_path, source).execute(scope)
    first_bytes = (tmp_path / "ticker=SBER" / "date=2026-07-15.parquet").read_bytes()
    second = _use_case(tmp_path, source).execute(scope)

    assert source.calls == ["SBER/2026-07-15"]
    assert first.written_partitions == 1
    assert second.written_partitions == 0
    assert second.skipped_partitions == 1
    assert second.inventory.dataset_fingerprint == first.inventory.dataset_fingerprint
    assert (
        tmp_path / "ticker=SBER" / "date=2026-07-15.parquet"
    ).read_bytes() == first_bytes
    compatible_cache = LocalCandleCache(tmp_path)
    assert compatible_cache.describe().dataset_fingerprint == (
        f"sha256:{first.inventory.dataset_fingerprint}"
    )
    assert len(compatible_cache.load()) == 1


def test_corrupted_partition_is_detected_and_replaced(tmp_path: Path) -> None:
    day = date(2026, 7, 15)
    key = CandlePartitionKey("SBER", day)
    target = tmp_path / "ticker=SBER" / "date=2026-07-15.parquet"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"not parquet")
    source = _Source({key.manifest_key: (_candle("SBER", day),)})

    receipt = _use_case(tmp_path, source).execute(CandleCacheScope(("SBER",), day, day))

    assert receipt.written_partitions == 1
    assert not receipt.failures
    assert ParquetCandlePartitionRepository(tmp_path).inspect(key).valid is True


def test_partition_repository_reuses_one_database_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[object] = []

    class Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    class DuckDB:
        @staticmethod
        def connect(*, database: str) -> Connection:
            assert database == ":memory:"
            connection = Connection()
            connections.append(connection)
            return connection

    monkeypatch.setattr(candle_cache_adapter, "_duckdb", lambda: DuckDB())
    repository = ParquetCandlePartitionRepository(tmp_path)

    first = repository._database_connection()
    second = repository._database_connection()
    repository.close()

    assert first is second
    assert len(connections) == 1
    assert first.closed is True


def test_scope_inventory_uses_batched_footer_reads_instead_of_row_scan(
    tmp_path: Path,
) -> None:
    first_day = date(2026, 7, 15)
    keys = tuple(
        CandlePartitionKey("SBER", first_day + timedelta(days=offset))
        for offset in range(3)
    )
    writer = ParquetCandlePartitionRepository(tmp_path)
    try:
        writer.replace_atomically(keys[0], (_candle("SBER", keys[0].trading_day),))
        writer.replace_atomically(keys[1], ())
        writer.replace_atomically(keys[2], (_candle("SBER", keys[2].trading_day),))
    finally:
        writer.close()

    queries: list[str] = []
    connection = candle_cache_adapter._duckdb().connect(database=":memory:")

    class TrackingConnection:
        def execute(self, query: str, parameters: object = None) -> object:
            queries.append(query)
            if parameters is None:
                return connection.execute(query)
            return connection.execute(query, parameters)

        def close(self) -> None:
            connection.close()

    repository = ParquetCandlePartitionRepository(tmp_path)
    repository._database = TrackingConnection()
    try:
        states = repository.inspect_many(keys)
    finally:
        repository.close()

    assert [state.valid for state in states] == [True, True, True]
    assert [state.row_count for state in states] == [1, 0, 1]
    assert len(queries) == 3
    assert "parquet_schema" in queries[0]
    assert "parquet_file_metadata" in queries[1]
    assert "parquet_metadata" in queries[2]
    assert "read_parquet" not in queries[2]


def test_manifest_distinguishes_empty_day_and_records_actual_morning_rows(
    tmp_path: Path,
) -> None:
    first_day = date(2026, 7, 18)
    second_day = date(2026, 7, 19)
    source = _Source(
        {
            "SBER/2026-07-18": (_candle("SBER", first_day),),
            "SBER/2026-07-19": (),
        }
    )

    _use_case(tmp_path, source).execute(
        CandleCacheScope(("SBER",), first_day, second_day)
    )

    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["quality"]["empty_partitions"] == ["SBER/2026-07-19"]
    assert manifest["quality"]["failed_partitions"] == []
    assert manifest["quality"]["morning_session"] == {
        "partitions_with_rows": 1,
        "rows_by_partition": {"SBER/2026-07-18": 1},
        "rows_present": True,
        "window": "07:00-09:50 Europe/Moscow",
    }


def test_large_inventory_releases_each_partition_before_reading_next(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    partition_count = 4_500
    rows_per_partition = 10
    first_day = date(2025, 1, 1)
    keys = tuple(
        CandlePartitionKey("SBER", first_day + timedelta(days=offset))
        for offset in range(partition_count)
    )
    pending_keys = iter(keys)
    marker = tmp_path / "partition.parquet"
    marker.write_bytes(b"present")

    class TrackedRecord(dict[str, object]):
        active = 0
        peak = 0

        def __init__(self, values: dict[str, object]) -> None:
            super().__init__(values)
            type(self).active += 1
            type(self).peak = max(type(self).peak, type(self).active)

        def __del__(self) -> None:
            type(self).active -= 1

    def read_partition(_path: Path) -> tuple[dict[str, object], ...]:
        key = next(pending_keys)
        start = datetime.combine(key.trading_day, datetime.min.time(), tzinfo=UTC)
        return tuple(
            TrackedRecord(
                {
                    "ticker": key.ticker,
                    "at": start + timedelta(minutes=minute),
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 1.0,
                    "volume_buy": 1.0,
                    "volume_sell": 0.0,
                    "complete": True,
                }
            )
            for minute in range(rows_per_partition)
        )

    repository = ParquetCandlePartitionRepository(tmp_path)
    monkeypatch.setattr(repository, "_path", lambda _key: marker)
    monkeypatch.setattr(repository, "_read", read_partition)
    monkeypatch.setattr(
        repository,
        "inspect",
        lambda key: CandlePartitionState(key, True, rows_per_partition),
    )

    inventory = repository.inventory(keys)

    assert len(inventory.rows_by_partition) == partition_count
    assert TrackedRecord.peak <= rows_per_partition
    assert TrackedRecord.active == 0


def test_incomplete_candle_never_replaces_a_valid_historical_partition(
    tmp_path: Path,
) -> None:
    day = date(2026, 7, 15)
    repository = ParquetCandlePartitionRepository(tmp_path)
    key = CandlePartitionKey("SBER", day)
    repository.replace_atomically(key, (_candle("SBER", day),))
    target = tmp_path / "ticker=SBER" / "date=2026-07-15.parquet"
    before = target.read_bytes()
    incomplete = CachedCandle(
        ticker="SBER",
        at=datetime(2026, 7, 15, 8, 0, tzinfo=UTC),
        open=101.0,
        high=101.0,
        low=101.0,
        close=101.0,
        volume=10.0,
        complete=False,
    )

    with pytest.raises(ValueError, match="incomplete candle"):
        repository.replace_atomically(key, (incomplete,))

    assert target.read_bytes() == before


def test_failed_fetch_does_not_change_existing_valid_partition_or_leak_secrets(
    tmp_path: Path,
) -> None:
    day = date(2026, 7, 15)
    repository = ParquetCandlePartitionRepository(tmp_path)
    sber = CandlePartitionKey("SBER", day)
    repository.replace_atomically(sber, (_candle("SBER", day),))
    target = tmp_path / "ticker=SBER" / "date=2026-07-15.parquet"
    before = target.read_bytes()
    secret = "secret-token-value"
    account = "account-123"
    uid = "raw-instrument-uid"
    source = _Source(
        {
            "GAZP/2026-07-15": RuntimeError(f"{secret} {account} {uid}"),
        }
    )

    receipt = _use_case(tmp_path, source).execute(
        CandleCacheScope(("SBER", "GAZP"), day, day)
    )

    assert receipt.skipped_partitions == 1
    assert len(receipt.failures) == 1
    assert target.read_bytes() == before
    persisted = (tmp_path / "manifest.json").read_text(encoding="utf-8") + (
        tmp_path / "failure-summary.json"
    ).read_text(encoding="utf-8")
    assert secret not in persisted
    assert account not in persisted
    assert uid not in persisted
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["privacy"] == {
        "account_identifiers_persisted": False,
        "instrument_uids_persisted": False,
        "tokens_persisted": False,
    }
    assert manifest["quality"]["rows_by_partition"] == {"SBER/2026-07-15": 1}


def test_rest_source_keeps_uid_out_of_returned_candles_and_requests_full_session() -> (
    None
):
    uid = "raw-instrument-uid"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if "FindInstrument" in str(request.url):
            return httpx.Response(
                200,
                json={
                    "instruments": [
                        {
                            "ticker": "SBER",
                            "classCode": "TQBR",
                            "uid": uid,
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "candles": [
                    {
                        "time": "2026-07-15T04:00:00Z",
                        "open": {"units": "100", "nano": 0},
                        "high": {"units": "101", "nano": 0},
                        "low": {"units": "99", "nano": 0},
                        "close": {"units": "100", "nano": 500000000},
                        "volume": "10",
                        "volumeBuy": "6",
                        "volumeSell": "4",
                        "isComplete": True,
                    }
                ]
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    source = TInvestRestCandleHistorySource(
        token="token-value",
        client=client,
        request_interval_seconds=0,
    )
    try:
        candles = source.fetch(CandlePartitionKey("SBER", date(2026, 7, 15)))
    finally:
        client.close()

    assert len(candles) == 1
    assert candles[0].ticker == "SBER"
    assert uid not in repr(candles)
    candle_request = json.loads(requests[1].content)
    assert candle_request["instrumentId"] == uid
    assert candle_request["from"] == "2026-07-15T03:50:00Z"
    assert candle_request["to"] == "2026-07-15T21:00:00Z"


def test_rest_source_builds_ssl_context_from_trusted_ca_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = tmp_path / "trusted-ca.pem"
    bundle.write_text("test certificate bundle", encoding="utf-8")
    expected_context = ssl.create_default_context()
    created_for: list[str | None] = []
    client_options: list[dict[str, object]] = []

    def create_default_context(*, cafile: str | None = None) -> ssl.SSLContext:
        created_for.append(cafile)
        return expected_context

    class Client:
        def __init__(self, **options: object) -> None:
            client_options.append(options)

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        candle_cache_adapter.ssl,
        "create_default_context",
        create_default_context,
    )
    monkeypatch.setattr(candle_cache_adapter.httpx, "Client", Client)

    source = TInvestRestCandleHistorySource(
        token="token-value",
        ca_bundle_path=bundle,
    )
    source.close()

    assert created_for == [str(bundle)]
    assert client_options[0]["verify"] is expected_context


def test_rest_source_accepts_prebuilt_ssl_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_context = ssl.create_default_context()
    client_options: list[dict[str, object]] = []

    class Client:
        def __init__(self, **options: object) -> None:
            client_options.append(options)

        def close(self) -> None:
            return None

    monkeypatch.setattr(candle_cache_adapter.httpx, "Client", Client)

    source = TInvestRestCandleHistorySource(
        token="token-value",
        ssl_context=expected_context,
    )
    source.close()

    assert client_options[0]["verify"] is expected_context


def test_rest_source_rejects_missing_trusted_ca_bundle(tmp_path: Path) -> None:
    missing = tmp_path / "missing-ca.pem"

    with pytest.raises(FileNotFoundError, match="Trusted CA bundle does not exist"):
        TInvestRestCandleHistorySource(
            token="token-value",
            ca_bundle_path=missing,
        )
