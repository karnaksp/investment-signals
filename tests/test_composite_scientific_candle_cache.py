from __future__ import annotations

from datetime import date, datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

from tinvest_signal_engine.adapters.composite_scientific_candle_cache import (
    ClickHouseScientificCandleSource,
    CompositeScientificCandleCache,
    VersionedHistoricalCandle,
)
from tinvest_signal_engine.adapters.local_hypothesis_replay import LocalCandleCache
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    HistoricalCandle,
)


UTC = timezone.utc


class _Response:
    def __init__(self, payload: str) -> None:
        self._payload = payload.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


class _StreamingResponse(_Response):
    def __iter__(self):
        return iter(self._payload.splitlines(keepends=True))

    def read(self) -> bytes:
        raise AssertionError("streaming ClickHouse reads must not call read()")


class _InterruptedStreamingResponse(_StreamingResponse):
    def __iter__(self):
        lines = self._payload.splitlines(keepends=True)
        yield lines[0]
        raise TimeoutError("response stream interrupted")


class _LiveSource:
    def __init__(self, rows: tuple[VersionedHistoricalCandle, ...]) -> None:
        self.rows = rows
        self.calls: list[datetime] = []

    def load_as_of(self, as_of: datetime) -> tuple[VersionedHistoricalCandle, ...]:
        self.calls.append(as_of)
        return self.rows


class _StreamingLiveSource(_LiveSource):
    def iter_as_of(
        self,
        as_of: datetime,
    ):
        self.calls.append(as_of)
        yield self.rows


class _DescriptorOnlyHistory:
    def __init__(self) -> None:
        self.load_calls = 0

    def describe(self) -> CandleCacheDescriptor:
        return CandleCacheDescriptor(
            dataset_fingerprint="sha256:" + "a" * 64,
            partition_count=6_000,
            tickers=("SBER",),
            start_day=date(2025, 11, 20),
            end_day=date(2026, 7, 17),
        )

    def load(self) -> tuple[HistoricalCandle, ...]:
        self.load_calls += 1
        raise AssertionError("describe must not load the historical dataset")


def _candle(
    minute: int,
    *,
    close: float = 100.0,
    ticker: str = "SBER",
    complete: bool = True,
) -> HistoricalCandle:
    return HistoricalCandle(
        ticker=ticker,
        at=datetime(2026, 7, 17, 10, minute, tzinfo=UTC),
        open=100.0,
        high=max(101.0, close),
        low=min(99.0, close),
        close=close,
        volume=1_000.0,
        complete=complete,
    )


def _write_cache(cache_dir: Path, candles: tuple[HistoricalCandle, ...]) -> None:
    by_day: dict[tuple[str, str], list[HistoricalCandle]] = {}
    for candle in candles:
        key = (candle.ticker, candle.at.date().isoformat())
        by_day.setdefault(key, []).append(candle)
    for (ticker, day), rows in by_day.items():
        path = cache_dir / f"ticker={ticker}" / f"date={day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(
                json.dumps(
                    {
                        "ticker": item.ticker,
                        "at": item.at.isoformat(),
                        "open": item.open,
                        "high": item.high,
                        "low": item.low,
                        "close": item.close,
                        "volume": item.volume,
                        "complete": item.complete,
                    }
                )
                + "\n"
                for item in rows
            ),
            encoding="utf-8",
        )
    days = sorted(item.at.date().isoformat() for item in candles)
    (cache_dir / "manifest.json").write_text(
        json.dumps(
            {
                "kind": "tinvest_research_candle_cache",
                "scope": {
                    "tickers": sorted({item.ticker for item in candles}),
                    "from": days[0],
                    "to": days[-1],
                },
                "quality": {"partition_count": len(by_day)},
                "privacy": {
                    "tokens_persisted": False,
                    "account_identifiers_persisted": False,
                    "instrument_uids_persisted": False,
                },
                "content_fingerprint": "a" * 64,
            }
        ),
        encoding="utf-8",
    )


def _clickhouse_row(
    *,
    source_time: str = "2026-07-17T10:00:00Z",
    close: str = "102",
    version: int = 5,
    received_at: str = "2026-07-17T10:00:01Z",
) -> dict[str, object]:
    return {
        "ticker": "SBER",
        "source_time": source_time,
        "open_price": "100",
        "high_price": "103",
        "low_price": "99",
        "close_price": close,
        "volume": 1200,
        "is_complete": 1,
        "source_at": source_time,
        "received_at": received_at,
        "record_version": version,
    }


def _instrument_range_row(
    *,
    instrument_id: str = "uid-sber",
    ticker: str = "SBER",
    first_candle_at: str = "2026-07-17T10:00:00Z",
) -> dict[str, object]:
    return {
        "instrument_id": instrument_id,
        "ticker": ticker,
        "first_candle_at": first_candle_at,
    }


def test_clickhouse_source_queries_causal_snapshot_and_prefers_latest_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[object, float]] = []
    payload = "\n".join(
        json.dumps(row)
        for row in (
            _clickhouse_row(close="101", version=4),
            _clickhouse_row(close="102", version=5),
        )
    )

    def open_request(request, timeout: float):
        captured.append((request, timeout))
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(json.dumps(_instrument_range_row()))
        return _Response(payload)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret-token",
        timeout_seconds=7.0,
    )

    rows = source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))

    assert len(rows) == 1
    assert rows[0].record_version == 5
    assert rows[0].candle.close == 102.0
    assert len(captured) == 2
    request, timeout = captured[1]
    query = parse_qs(urlparse(request.full_url).query)
    assert query["database"] == ["signal_engine"]
    assert query["param_as_of"] == ["2026-07-17T11:00:00.000000Z"]
    assert query["param_instrument_id"] == ["uid-sber"]
    assert query["param_ticker"] == ["SBER"]
    sql = request.data.decode("utf-8")
    assert "candle_at <=" in sql
    assert "source_at <=" in sql
    assert "received_at <=" in sql
    assert "PREWHERE instrument_id =" in sql
    assert "ORDER BY candle_at, record_version" in sql
    assert "secret-token" not in request.full_url
    assert "secret-token" not in sql
    assert timeout == 7.0


def test_clickhouse_datetime64_without_suffix_restores_schema_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(
        _clickhouse_row(
            source_time="2026-07-17 10:00:00.000000",
            received_at="2026-07-17 10:00:01.000000",
        )
    )
    def open_request(request, timeout):
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(json.dumps(_instrument_range_row()))
        return _Response(payload)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
    )

    rows = source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))

    assert rows[0].candle.at == datetime(2026, 7, 17, 10, 0, tzinfo=UTC)


def test_clickhouse_source_rejects_future_row_even_if_backend_returns_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(_clickhouse_row(received_at="2026-07-17T12:00:00Z"))
    def open_request(request, timeout):
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(json.dumps(_instrument_range_row()))
        return _Response(payload)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
    )

    with pytest.raises(ValueError, match="causal cutoff"):
        source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))


def test_clickhouse_source_streams_and_bounds_deduplication_by_ticker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sber_rows = (
        _clickhouse_row(close="101", version=4),
        _clickhouse_row(close="102", version=5),
        {
            **_clickhouse_row(
                source_time="2026-07-17T10:01:00Z",
                received_at="2026-07-17T10:01:01Z",
            ),
            "ticker": "SBER",
        },
    )
    sberp_rows = (
        {
            **_clickhouse_row(
                source_time="2026-07-17T10:00:00Z",
                received_at="2026-07-17T10:00:01Z",
            ),
            "ticker": "SBERP",
        },
    )
    captured: dict[str, object] = {}

    def open_request(request, timeout: float):
        sql = request.data.decode("utf-8")
        if "min(candle_at)" in sql:
            return _StreamingResponse(
                "\n".join(
                    json.dumps(row)
                    for row in (
                        _instrument_range_row(),
                        _instrument_range_row(
                            instrument_id="uid-sberp",
                            ticker="SBERP",
                        ),
                    )
                )
            )
        captured["sql"] = sql
        query = parse_qs(urlparse(request.full_url).query)
        rows = sber_rows if query["param_ticker"] == ["SBER"] else sberp_rows
        return _StreamingResponse("\n".join(json.dumps(row) for row in rows))

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
    )

    partitions = tuple(
        source.iter_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))
    )

    assert tuple(partition[0].candle.ticker for partition in partitions) == (
        "SBER",
        "SBERP",
    )
    assert tuple(item.candle.close for item in partitions[0]) == (102.0, 102.0)
    assert "ORDER BY candle_at, record_version" in str(captured["sql"])
    assert "max_result_rows = 1000000" in str(captured["sql"])
    assert "ORDER BY ticker, candle_at" not in str(captured["sql"])
    assert str(captured["sql"]).index("SETTINGS") < str(captured["sql"]).index(
        "FORMAT JSONEachRow"
    )


def test_clickhouse_source_retries_only_failed_bounded_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery_calls = 0
    chunk_calls: dict[str, int] = {}

    def open_request(request, timeout):
        nonlocal discovery_calls
        sql = request.data.decode("utf-8")
        if "min(candle_at)" in sql:
            discovery_calls += 1
            return _Response(
                json.dumps(
                    _instrument_range_row(
                        first_candle_at="2026-06-01T10:00:00Z",
                    )
                )
            )
        query = parse_qs(urlparse(request.full_url).query)
        window_start = query["param_window_start"][0]
        chunk_calls[window_start] = chunk_calls.get(window_start, 0) + 1
        if window_start.startswith("2026-07-02") and chunk_calls[window_start] == 1:
            raise HTTPError(
                request.full_url,
                500,
                "Internal Server Error",
                {},
                BytesIO(b"Code: 241. Memory limit exceeded"),
            )
        source_time = (
            "2026-06-01T10:00:00Z"
            if window_start.startswith("2026-06-01")
            else "2026-07-02T10:00:00Z"
        )
        return _StreamingResponse(
            json.dumps(
                _clickhouse_row(
                    source_time=source_time,
                    received_at=source_time,
                )
            )
        )

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
        retry_backoff_seconds=0,
    )

    rows = source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))

    assert len(rows) == 2
    assert discovery_calls == 1
    assert chunk_calls == {
        "2026-06-01T10:00:00.000000Z": 1,
        "2026-07-02T10:00:00.000000Z": 2,
    }


def test_clickhouse_source_restarts_partial_chunk_without_duplicate_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk_calls = 0
    payload = "\n".join(
        json.dumps(row)
        for row in (
            _clickhouse_row(close="101", version=4),
            _clickhouse_row(close="102", version=5),
        )
    )

    def open_request(request, timeout):
        nonlocal chunk_calls
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(json.dumps(_instrument_range_row()))
        chunk_calls += 1
        if chunk_calls == 1:
            return _InterruptedStreamingResponse(payload)
        return _StreamingResponse(payload)

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
        retry_backoff_seconds=0,
    )

    rows = source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))

    assert len(rows) == 1
    assert rows[0].record_version == 5
    assert chunk_calls == 2


def test_clickhouse_source_reads_adjacent_time_chunks_without_overlap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk_queries: list[dict[str, list[str]]] = []

    def open_request(request, timeout):
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(
                json.dumps(
                    _instrument_range_row(
                        first_candle_at="2026-06-01T10:00:00Z",
                    )
                )
            )
        query = parse_qs(urlparse(request.full_url).query)
        chunk_queries.append(query)
        return _StreamingResponse("")

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
        chunk_days=31,
    )

    assert tuple(source.iter_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))) == ()

    assert len(chunk_queries) == 2
    assert (
        chunk_queries[0]["param_window_end"]
        == chunk_queries[1]["param_window_start"]
    )
    assert chunk_queries[-1]["param_window_end"] == [
        "2026-07-17T11:00:00.000001Z"
    ]


def test_clickhouse_source_reports_bounded_server_diagnostic_after_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def open_request(request, timeout):
        if "min(candle_at)" in request.data.decode("utf-8"):
            return _Response(json.dumps(_instrument_range_row()))
        raise HTTPError(
            request.full_url,
            500,
            "Internal Server Error",
            {},
            BytesIO(b"Code: 241. Memory limit exceeded"),
        )

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.composite_scientific_candle_cache.urlopen",
        open_request,
    )
    source = ClickHouseScientificCandleSource(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="reader",
        password="secret",
        max_attempts=2,
        retry_backoff_seconds=0,
    )

    with pytest.raises(
        RuntimeError,
        match=r"status 500.*Code: 241\. Memory limit exceeded",
    ):
        source.load_as_of(datetime(2026, 7, 17, 11, 0, tzinfo=UTC))


def test_composite_merges_local_cache_and_live_revisions_without_future_data(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "candles"
    old = _candle(0, close=100.0)
    future = HistoricalCandle(
        ticker="SBER",
        at=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=100.0,
    )
    _write_cache(cache_dir, (old, future))
    latest = VersionedHistoricalCandle(_candle(0, close=102.0), 7)
    next_minute = VersionedHistoricalCandle(_candle(1, close=101.0), 3)
    live = _LiveSource((next_minute, latest))
    cache = CompositeScientificCandleCache(
        historical=LocalCandleCache(cache_dir),
        live=live,
        as_of=datetime(2026, 7, 17, 11, 0, tzinfo=UTC),
    )

    descriptor = cache.describe()
    loaded = cache.load()

    assert [(item.at.minute, item.close) for item in loaded] == [
        (0, 102.0),
        (1, 101.0),
    ]
    assert descriptor.tickers == ("SBER",)
    assert descriptor.partition_count == 1
    assert descriptor.start_day == descriptor.end_day == old.at.date()
    assert descriptor.dataset_fingerprint.startswith("sha256:")
    assert live.calls == [datetime(2026, 7, 17, 11, 0, tzinfo=UTC)]


def test_sealed_composite_reuses_snapshot_for_every_partitioned_model(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "candles"
    _write_cache(cache_dir, (_candle(0), _candle(1)))
    live = _StreamingLiveSource(
        (
            VersionedHistoricalCandle(_candle(0, close=102.0), 7),
            VersionedHistoricalCandle(_candle(1, close=103.0), 8),
        )
    )
    cutoff = datetime(2026, 7, 17, 11, 0, tzinfo=UTC)
    cache = CompositeScientificCandleCache(
        historical=LocalCandleCache(cache_dir),
        live=live,
        as_of=cutoff,
    )

    cache.describe()
    sealed = cache.load()
    first_model = tuple(cache.iter_ticker_partitions())
    second_model = tuple(cache.iter_ticker_partitions())

    assert tuple(item for partition in first_model for item in partition) == sealed
    assert first_model == second_model
    # One descriptor scan and one sealing scan.  Any number of downstream
    # models reuse the sealed snapshot without another ClickHouse read.
    assert live.calls == [cutoff, cutoff]


def test_composite_fingerprint_is_stable_and_contains_no_storage_credentials(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "candles"
    _write_cache(cache_dir, (_candle(0),))
    rows = (
        VersionedHistoricalCandle(_candle(2), 2),
        VersionedHistoricalCandle(_candle(1), 1),
    )
    as_of = datetime(2026, 7, 17, 11, 0, tzinfo=UTC)
    first = CompositeScientificCandleCache(
        historical=LocalCandleCache(cache_dir), live=_LiveSource(rows), as_of=as_of
    ).describe()
    second = CompositeScientificCandleCache(
        historical=LocalCandleCache(cache_dir),
        live=_LiveSource(tuple(reversed(rows))),
        as_of=as_of,
    ).describe()

    assert first == second
    assert "secret" not in first.dataset_fingerprint
    assert "token" not in first.dataset_fingerprint


def test_composite_descriptor_does_not_scan_historical_candles() -> None:
    historical = _DescriptorOnlyHistory()
    live = _LiveSource(())
    cache = CompositeScientificCandleCache(
        historical=historical,
        live=live,
        as_of=datetime(2026, 7, 19, 12, 0, tzinfo=UTC),
    )

    descriptor = cache.describe()

    assert descriptor.partition_count == 6_000
    assert descriptor.tickers == ("SBER",)
    assert historical.load_calls == 0
    assert live.calls == [datetime(2026, 7, 19, 12, 0, tzinfo=UTC)]


def test_composite_rejects_same_version_with_different_payloads(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "candles"
    _write_cache(cache_dir, (_candle(1),))
    live = _LiveSource(
        (
            VersionedHistoricalCandle(_candle(0, close=101.0), 5),
            VersionedHistoricalCandle(_candle(0, close=102.0), 5),
        )
    )
    cache = CompositeScientificCandleCache(
        historical=LocalCandleCache(cache_dir),
        live=live,
        as_of=datetime(2026, 7, 17, 11, 0, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="conflicting candle payloads"):
        cache.load()
