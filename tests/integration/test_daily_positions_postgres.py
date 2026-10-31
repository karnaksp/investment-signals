"""Daily reports are atomic/idempotent without contaminating detector state."""

from dataclasses import replace
from datetime import date, datetime, timezone
import os
from pathlib import Path
import uuid

import psycopg
import pytest

from tinvest_signal_engine.adapters.daily_positions_report import PostgresDailyPositionPublisher
from tinvest_signal_engine.adapters.asset_position_publisher import PostgresAssetPositionPublisher
from tinvest_signal_engine.adapters.postgres_reliability import PostgresReliableProcessingStore
from tinvest_signal_engine.application.reliable_processing import BrokerEvent, DetectionBatch
from tinvest_signal_engine.domain.daily_positions import (
    DailyPositionComparison, PositionSnapshot, PositionTotal,
)
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, EventReplayConflict
from tinvest_signal_engine.domain.commodity_positions import PositionContext


pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION", "").lower() not in {"1", "true", "yes"},
                    reason="Set RUN_INTEGRATION=1 and stack up")
@pytest.mark.parametrize("mode", ["legacy", "digest", "asset"])
def test_daily_report_replay_keeps_one_outbox_and_no_detector_checkpoint(mode):
    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("HOST_POSTGRES_PORT", "35432")),
        dbname=os.getenv("POSTGRES_DATABASE", "signal_engine"),
        user=os.getenv("POSTGRES_USERNAME", "signal_engine"),
        password=os.getenv("POSTGRES_PASSWORD", "signal_engine"), autocommit=True,
    )
    schema = "itest_daily_" + uuid.uuid4().hex
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'SET search_path TO "{schema}"')
            root = Path(__file__).resolve().parents[2]
            for migration in sorted((root / "sql/postgres/migrations").glob("*.up.sql")):
                cursor.execute(migration.read_text())
        store = PostgresReliableProcessingStore(connection, signal_table="market_signals")
        previous = PositionSnapshot(
            date(2026, 10, 29), datetime(2026, 10, 29, 8, tzinfo=timezone.utc),
            tuple(row for p in ("au", "ag") for row in (
                PositionTotal(p, "broker_clients", 100, 40, 0, 0),
                PositionTotal(p, "direct_members", 20, 80, 0, 0),
            )), "https://www.shfe.com.cn/previous", "a" * 64,
        )
        current = PositionSnapshot(
            date(2026, 10, 30), datetime(2026, 10, 30, 8, tzinfo=timezone.utc),
            tuple(row for p in ("au", "ag") for row in (
                PositionTotal(p, "broker_clients", 110, 40, 10, 0),
                PositionTotal(p, "direct_members", 20, 90, 0, 10),
            )), "https://www.shfe.com.cn/current", "b" * 64,
        )
        comparison = DailyPositionComparison(current, previous)
        publisher_type = PostgresAssetPositionPublisher if mode == "asset" else PostgresDailyPositionPublisher
        publisher = publisher_type(store=store, target=DeliveryTarget("telegram", "test"))
        context = None if mode == "legacy" else PositionContext()
        assert publisher.publish(comparison, generated_at=datetime.now(timezone.utc), context=context)
        assert not publisher.publish(comparison, generated_at=datetime.now(timezone.utc),
                                     context=PositionContext((), ("ICE",)) if mode != "legacy" else None)
        with pytest.raises(EventReplayConflict):
            publisher.publish(DailyPositionComparison(replace(current, source_sha256="c" * 64), previous),
                              generated_at=datetime.now(timezone.utc), context=context)
        # Tick processing must still require its state checkpoint and roll back
        # its inbox insert when no checkpoint is provided.
        tick = BrokerEvent("missing-checkpoint", "trade", "marketdata.raw", 0, 1, b"x" * 32, {})
        with pytest.raises(ValueError, match="state checkpoint"):
            store.persist_detection_once(tick, DetectionBatch())
        with connection.cursor() as cursor:
            reports = 2 if mode == "asset" else 1
            for table, count in (("processed_events", reports), ("market_signals", reports),
                                 ("delivery_outbox", reports), ("detector_state_snapshots", 0)):
                cursor.execute(f"SELECT count(*) FROM {table}")
                assert cursor.fetchone()[0] == count
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()
