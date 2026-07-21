from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.file_historical_candle_import import (
    AtomicFileHistoricalCandleImportProgress,
    HistoricalCandleImportAlreadyRunning,
)
from tinvest_signal_engine.services.historical_candle_import import main, parse_args
from tinvest_signal_engine.application.historical_candle_import import (
    HistoricalDestinationPartition,
    HistoricalImportConflict,
    ImportHistoricalScientificCandles,
)
from tinvest_signal_engine.domain.historical_candle_import import (
    HistoricalCandleImportInventory,
    HistoricalCandleImportProgress,
    HistoricalCandlePartition,
    HistoricalCandlePartitionDescriptor,
    HistoricalCandlePartitionKey,
    HistoricalImportState,
    PersistedCandleSnapshot,
    inventory_fingerprint,
    partition_content_fingerprint,
)
from tinvest_signal_engine.domain.scientific_candles import (
    ScientificCandle,
    scientific_candle_fingerprint,
)


NOW = datetime(2026, 7, 22, 7, 0, tzinfo=UTC)
MANIFEST_SHA = "sha256:" + "a" * 64


def _candle(
    ticker: str,
    day: date,
    minute: int,
    *,
    close: str = "100",
    received_at: datetime = NOW,
) -> ScientificCandle:
    at = datetime(day.year, day.month, day.day, 7, minute, tzinfo=UTC)
    instrument_id = f"{ticker}_TQBR"
    event_id = f"backfill-v1:{instrument_id}:{at.isoformat()}"
    price = Decimal(close)
    fingerprint = scientific_candle_fingerprint(
        instrument_id=instrument_id,
        ticker=ticker,
        exchange="TQBR",
        candle_at=at,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=100,
        complete=True,
        source_kind="backfill",
        source_at=at + timedelta(minutes=1),
        source_event_id=event_id,
        has_gap=False,
        schema_version="scientific-candle-v1",
    )
    return ScientificCandle(
        instrument_id=instrument_id,
        ticker=ticker,
        exchange="TQBR",
        trading_day=day,
        candle_at=at,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=100,
        complete=True,
        source_kind="backfill",
        source_at=at + timedelta(minutes=1),
        received_at=received_at,
        source_event_id=event_id,
        payload_fingerprint=fingerprint,
    )


def _descriptor(ticker: str, day: date, marker: str) -> HistoricalCandlePartitionDescriptor:
    return HistoricalCandlePartitionDescriptor(
        HistoricalCandlePartitionKey(ticker, day),
        "sha256:" + marker * 64,
        100,
    )


def _partition(
    descriptor: HistoricalCandlePartitionDescriptor,
    *,
    rows: int = 2,
) -> HistoricalCandlePartition:
    candles = tuple(
        _candle(descriptor.key.ticker, descriptor.key.trading_day, minute)
        for minute in range(rows)
    )
    return HistoricalCandlePartition(
        descriptor,
        candles,
        partition_content_fingerprint(candles),
    )


class _Source:
    def __init__(self, partitions: tuple[HistoricalCandlePartition, ...]) -> None:
        self.partitions = {item.descriptor.key: item for item in partitions}
        descriptors = tuple(sorted((item.descriptor for item in partitions), key=lambda x: x.key))
        self.inventory_value = HistoricalCandleImportInventory(
            "tinvest_research_candle_cache",
            MANIFEST_SHA,
            inventory_fingerprint(descriptors),
            descriptors,
            len(descriptors) - 1,
        )
        self.loads: list[HistoricalCandlePartitionKey] = []

    def inventory(self) -> HistoricalCandleImportInventory:
        return self.inventory_value

    def load_partition(self, descriptor, *, instrument_id, received_at):
        self.loads.append(descriptor.key)
        assert instrument_id == f"{descriptor.key.ticker}_TQBR"
        partition = self.partitions[descriptor.key]
        candles = tuple(replace(item, received_at=received_at) for item in partition.candles)
        return HistoricalCandlePartition(
            descriptor,
            candles,
            partition_content_fingerprint(candles),
        )


def _snapshot(candle: ScientificCandle, *, close: str | None = None, source="backfill"):
    price = Decimal(close) if close is not None else candle.close_price
    return PersistedCandleSnapshot(
        instrument_id=candle.instrument_id,
        ticker=candle.ticker,
        trading_day=candle.trading_day,
        candle_at=candle.candle_at,
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=candle.volume,
        complete=candle.complete,
        source_kind=source,
        payload_fingerprint="sha256:" + "b" * 64,
    )


class _Destination:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, date, datetime], PersistedCandleSnapshot] = {}
        self.inspect_calls = 0
        self.persist_calls: list[int] = []
        self.fail_once = False

    def inspect_partitions(self, requests: tuple[HistoricalDestinationPartition, ...]):
        self.inspect_calls += 1
        selected = {(item.instrument_id, item.descriptor.key.trading_day) for item in requests}
        return tuple(
            row
            for (instrument_id, day, _), row in self.rows.items()
            if (instrument_id, day) in selected
        )

    def persist_many(self, candles: tuple[ScientificCandle, ...]) -> None:
        self.persist_calls.append(len(candles))
        for candle in candles:
            key = (candle.instrument_id, candle.trading_day, candle.candle_at)
            self.rows[key] = _snapshot(candle)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("interrupted after atomic batch")


class _Progress:
    def __init__(self) -> None:
        self.value = None
        self.saved = []
        self.results = []

    def load(self):
        return self.value

    def save(self, progress):
        self.value = progress
        self.saved.append(progress)

    def publish_result(self, result):
        self.results.append(result)


def _runner(source, destination, progress, **kwargs):
    return ImportHistoricalScientificCandles(
        source=source,
        destination=destination,
        progress=progress,
        instrument_ids={ticker: f"{ticker}_TQBR" for ticker in {item.key.ticker for item in source.inventory_value.partitions}},
        now=lambda: NOW,
        **kwargs,
    )


def test_grouped_import_is_bounded_verified_and_idempotent() -> None:
    days = (date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3))
    partitions = tuple(
        _partition(_descriptor("SBER", day, str(index + 1)))
        for index, day in enumerate(days)
    )
    source = _Source(partitions)
    destination = _Destination()
    progress = _Progress()
    runner = _runner(
        source,
        destination,
        progress,
        batch_size=3,
        partition_group_size=2,
    )

    first = runner.execute()
    second = runner.execute()

    assert first.state is HistoricalImportState.COMPLETED
    assert first.source_rows == 6
    assert first.inserted_rows == 6
    assert first.insert_batches == 3
    assert first.query_batches == 4
    assert destination.persist_calls == [3, 1, 2]
    assert destination.inspect_calls == 4
    assert len(destination.rows) == 6
    assert len(source.loads) == 3
    assert second.state is HistoricalImportState.COMPLETED
    assert second.insert_batches == 0
    assert second.query_batches == 0
    assert len(destination.rows) == 6


def test_resume_after_interrupted_atomic_batch_does_not_duplicate() -> None:
    partition = _partition(_descriptor("SBER", date(2026, 7, 1), "1"))
    source = _Source((partition,))
    destination = _Destination()
    destination.fail_once = True
    progress = _Progress()
    runner = _runner(source, destination, progress)

    with pytest.raises(RuntimeError, match="interrupted"):
        runner.execute()

    assert progress.value.state is HistoricalImportState.FAILED
    assert len(destination.rows) == 2
    result = runner.execute()
    assert result.state is HistoricalImportState.COMPLETED
    assert result.inserted_rows == 0
    assert result.existing_rows == 2
    assert len(destination.rows) == 2


def test_existing_stream_row_is_reused_but_conflicting_market_data_is_rejected() -> None:
    partition = _partition(_descriptor("SBER", date(2026, 7, 1), "1"), rows=1)
    source = _Source((partition,))
    destination = _Destination()
    candle = partition.candles[0]
    key = (candle.instrument_id, candle.trading_day, candle.candle_at)
    destination.rows[key] = _snapshot(candle, source="stream")

    result = _runner(source, destination, _Progress()).execute()
    assert result.existing_rows == 1
    assert result.inserted_rows == 0

    destination.rows[key] = _snapshot(candle, close="101", source="stream")
    with pytest.raises(HistoricalImportConflict, match="conflicts"):
        _runner(source, destination, _Progress()).execute()


def test_dry_run_is_read_only_and_reports_would_insert() -> None:
    partition = _partition(_descriptor("SBER", date(2026, 7, 1), "1"))
    source = _Source((partition,))
    destination = _Destination()
    progress = _Progress()

    result = _runner(source, destination, progress).execute(dry_run=True)

    assert result.dry_run is True
    assert result.inserted_rows == 2
    assert result.query_batches == 1
    assert result.insert_batches == 0
    assert not destination.rows
    assert not progress.saved
    assert not progress.results


def test_source_inventory_change_is_fail_closed() -> None:
    partition = _partition(_descriptor("SBER", date(2026, 7, 1), "1"))
    source = _Source((partition,))
    progress = _Progress()
    progress.value = HistoricalCandleImportProgress(
        run_id="run-1",
        state=HistoricalImportState.RUNNING,
        inventory_fingerprint="sha256:" + "9" * 64,
        source_manifest_checksum=MANIFEST_SHA,
        started_at=NOW,
        updated_at=NOW,
        total_partitions=1,
        manifest_covered_partitions=0,
    )
    with pytest.raises(HistoricalImportConflict, match="inventory changed"):
        _runner(source, _Destination(), progress).execute()


def test_atomic_progress_round_trip_and_checksum_guard(tmp_path: Path) -> None:
    adapter = AtomicFileHistoricalCandleImportProgress(tmp_path)
    progress = HistoricalCandleImportProgress(
        run_id="run-1",
        state=HistoricalImportState.RUNNING,
        inventory_fingerprint="sha256:" + "1" * 64,
        source_manifest_checksum=MANIFEST_SHA,
        started_at=NOW,
        updated_at=NOW,
        total_partitions=1,
        manifest_covered_partitions=0,
    )
    adapter.save(progress)
    assert adapter.load() == progress
    assert adapter.status_payload()["partitions_completed"] == 0

    path = tmp_path / "progress.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["payload"]["run_id"] = "tampered"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        adapter.load()


def test_state_lock_rejects_concurrent_import(tmp_path: Path) -> None:
    first = AtomicFileHistoricalCandleImportProgress(tmp_path)
    second = AtomicFileHistoricalCandleImportProgress(tmp_path)
    with first.exclusive_run():
        with pytest.raises(HistoricalCandleImportAlreadyRunning):
            with second.exclusive_run():
                pass


def test_cli_defaults_to_large_clickhouse_batches_and_status_is_aggregate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = parse_args(["run"])
    assert args.batch_size == 50_000
    assert args.partition_group_size == 50
    assert main(["status", "--state-dir", str(tmp_path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "operation": "status",
        "schema_version": 1,
        "status": "not_started",
    }
