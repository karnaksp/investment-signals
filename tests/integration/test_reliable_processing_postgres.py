"""PostgreSQL transaction/replay evidence for inbox, signal, and outbox."""

from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import pytest

from tinvest_signal_engine.adapters.postgres_reliability import (
    PostgresDeliveryQueue,
    PostgresObservationPublicationQueue,
    PostgresReliableProcessingStore,
)
from tinvest_signal_engine.application.reliable_processing import (
    BrokerEvent,
    DetectionBatch,
)
from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.domain.reliable_processing import (
    DeliveryTarget,
    EventReplayConflict,
    PreparedSignal,
    SignalRecord,
)


pytestmark = pytest.mark.integration


def _enabled() -> bool:
    return os.getenv("RUN_INTEGRATION", "").lower() in {"1", "true", "yes"}


@pytest.mark.skipif(not _enabled(), reason="Set RUN_INTEGRATION=1 and stack up")
def test_replay_keeps_one_inbox_signal_and_outbox_row() -> None:
    connection = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("HOST_POSTGRES_PORT", "35432")),
        dbname=os.getenv("POSTGRES_DATABASE", "signal_engine"),
        user=os.getenv("POSTGRES_USERNAME", "signal_engine"),
        password=os.getenv("POSTGRES_PASSWORD", "signal_engine"),
        autocommit=True,
    )
    schema = f"itest_{uuid.uuid4().hex}"
    root = Path(__file__).resolve().parents[2]
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'SET search_path TO "{schema}"')
            for path in sorted(
                (root / "sql" / "postgres" / "migrations").glob("*.up.sql")
            ):
                cursor.execute(path.read_text(encoding="utf-8"))

        store = PostgresReliableProcessingStore(
            connection,
            signal_table="market_signals",
        )
        event = _event("1", 1)
        prepared = (
            PreparedSignal(
                _signal(event),
                (DeliveryTarget("webhook", "https://local.invalid/hook"),),
            ),
        )

        batch = DetectionBatch(
            signals=prepared,
            observations=(_observation(event),),
        )
        first = store.persist_detection_once(event, batch)
        replay = store.persist_detection_once(event, batch)

        assert first.replayed is False
        assert replay.replayed is True
        assert len(replay.signals) == 1
        with pytest.raises(EventReplayConflict):
            store.find_processed(replace(event, payload_sha256=b"d" * 32))
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM processed_events")
            assert cursor.fetchone()[0] == 1
            cursor.execute("SELECT count(*) FROM market_signals")
            assert cursor.fetchone()[0] == 1
            cursor.execute("SELECT count(*) FROM delivery_outbox")
            assert cursor.fetchone()[0] == 1
            cursor.execute("SELECT count(*) FROM detector_observation_outbox")
            assert cursor.fetchone()[0] == 1

        queue = PostgresDeliveryQueue(connection)
        claim_at = datetime.now(tz=timezone.utc)
        task = queue.claim(
            available_at=claim_at,
            lease_until=claim_at + timedelta(seconds=60),
        )
        assert task is not None
        assert task.attempt_count == 1
        queue.mark_failed(
            task,
            reason_code="integration_retry",
            next_attempt_at=claim_at,
            dead_letter=False,
        )
        retry = queue.claim(
            available_at=claim_at,
            lease_until=claim_at + timedelta(seconds=60),
        )
        assert retry is not None
        assert retry.attempt_count == 2
        queue.mark_delivered(retry, delivered_at=claim_at)

        failed_event = _event("2", 2)
        failed_prepared = (
            PreparedSignal(
                _signal(failed_event),
                (DeliveryTarget("webhook", "https://local.invalid/hook"),),
            ),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE FUNCTION fail_outbox_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'simulated outbox crash';
                END
                $$
                """
            )
            cursor.execute(
                """
                CREATE TRIGGER fail_outbox_insert
                BEFORE INSERT ON delivery_outbox
                FOR EACH ROW EXECUTE FUNCTION fail_outbox_insert()
                """
            )
        with pytest.raises(psycopg.errors.RaiseException):
            store.persist_once(failed_event, failed_prepared)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM processed_events WHERE event_id = %s",
                (failed_event.event_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                "SELECT count(*) FROM market_signals WHERE source_event_id = %s",
                (failed_event.event_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute("DROP TRIGGER fail_outbox_insert ON delivery_outbox")
            cursor.execute("DROP FUNCTION fail_outbox_insert()")

        retried = store.persist_once(failed_event, failed_prepared)
        assert retried.replayed is False
        assert len(retried.signals) == 1

        observation_event = _event("3", 3)
        observation_batch = DetectionBatch(
            observations=(_observation(observation_event),)
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE FUNCTION fail_observation_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'simulated observation outbox crash';
                END
                $$
                """
            )
            cursor.execute(
                """
                CREATE TRIGGER fail_observation_insert
                BEFORE INSERT ON detector_observation_outbox
                FOR EACH ROW EXECUTE FUNCTION fail_observation_insert()
                """
            )
        with pytest.raises(psycopg.errors.RaiseException):
            store.persist_detection_once(observation_event, observation_batch)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM processed_events WHERE event_id = %s",
                (observation_event.event_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                SELECT count(*) FROM detector_observation_outbox
                WHERE source_event_id = %s
                """,
                (observation_event.event_id,),
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                """
                DROP TRIGGER fail_observation_insert
                ON detector_observation_outbox
                """
            )
            cursor.execute("DROP FUNCTION fail_observation_insert()")

        observation_retry = store.persist_detection_once(
            observation_event,
            observation_batch,
        )
        assert observation_retry.replayed is False

        collision_event = _event("4", 4)
        original = _observation(collision_event)
        collision_batch = DetectionBatch(
            observations=(original, replace(original, metric_value=999.0))
        )
        with pytest.raises(
            EventReplayConflict,
            match="observation id was reused",
        ):
            store.persist_detection_once(collision_event, collision_batch)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM processed_events WHERE event_id = %s",
                (collision_event.event_id,),
            )
            assert cursor.fetchone()[0] == 0

        observation_queue = PostgresObservationPublicationQueue(connection)
        publication_time = datetime.now(tz=timezone.utc)
        published_ids: set[str] = set()
        for _ in range(2):
            task = observation_queue.claim(
                available_at=publication_time,
                lease_until=publication_time + timedelta(seconds=30),
            )
            assert task is not None
            published_ids.add(task.observation.observation_id)
            observation_queue.mark_published(
                task,
                published_at=publication_time,
            )
        assert published_ids == {
            _observation(event).observation_id,
            _observation(observation_event).observation_id,
        }

        retry_event = _event("5", 5)
        store.persist_detection_once(
            retry_event,
            DetectionBatch(observations=(_observation(retry_event),)),
        )
        retry_time = datetime.now(tz=timezone.utc)
        retry_task = observation_queue.claim(
            available_at=retry_time,
            lease_until=retry_time + timedelta(seconds=30),
        )
        assert retry_task is not None
        observation_queue.mark_failed(
            retry_task,
            reason_code="clickhouse_unavailable",
            next_attempt_at=retry_time,
            dead_letter=False,
        )
        second_attempt = observation_queue.claim(
            available_at=retry_time,
            lease_until=retry_time + timedelta(seconds=30),
        )
        assert second_attempt is not None
        assert second_attempt.observation == retry_task.observation
        assert second_attempt.attempt_count == 2
        observation_queue.mark_published(
            second_attempt,
            published_at=retry_time,
        )

        reclaim_event = _event("6", 6)
        store.persist_detection_once(
            reclaim_event,
            DetectionBatch(observations=(_observation(reclaim_event),)),
        )
        reclaim_time = datetime.now(tz=timezone.utc)
        leased = observation_queue.claim(
            available_at=reclaim_time,
            lease_until=reclaim_time + timedelta(seconds=1),
        )
        assert leased is not None
        reclaimed = observation_queue.claim(
            available_at=reclaim_time + timedelta(seconds=2),
            lease_until=reclaim_time + timedelta(seconds=32),
        )
        assert reclaimed is not None
        assert reclaimed.observation == leased.observation
        assert reclaimed.attempt_count == 2
        observation_queue.mark_published(
            reclaimed,
            published_at=reclaim_time + timedelta(seconds=2),
        )
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()


def _event(suffix: str, offset: int) -> BrokerEvent:
    return BrokerEvent(
        event_id=f"event-integration-{suffix}",
        event_type="trade",
        topic="marketdata.raw",
        partition_id=0,
        offset_id=offset,
        payload_sha256=b"p" * 32,
        payload={},
    )


def _signal(event: BrokerEvent) -> SignalRecord:
    now = datetime.now(tz=timezone.utc)
    return SignalRecord(
        signal_id=str(uuid.uuid5(uuid.NAMESPACE_URL, event.event_id)),
        detected_at=now,
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="price_jump",
        severity=2,
        metric_value=1.0,
        baseline_value=0.5,
        z_score=3.0,
        window_seconds=60,
        summary="integration",
        payload={"delivery_status": "delivered"},
        source_event_id=event.event_id,
        source_event_at=now,
        signal_schema_version="1.0.0",
        expectation_catalog_version="1.0.0",
        detector_config_version="detector-1",
        delivery_config_version="delivery-1",
        cost_model_version="cost-1",
        provenance_status="complete",
    )


def _observation(event: BrokerEvent) -> DetectorObservation:
    now = datetime.now(tz=timezone.utc)
    return DetectorObservation(
        observation_id=str(uuid.uuid5(uuid.NAMESPACE_OID, event.event_id)),
        source_event_id=event.event_id,
        observed_at=now,
        instrument_id="SBER_TQBR",
        source_event_type="trade",
        signal_type="price_jump",
        metric_value=1.0,
        baseline_value=0.5,
        z_score=2.0,
        threshold_value=3.0,
        threshold_passed=False,
        detector_passed=False,
        signal_emitted=False,
        window_seconds=60,
        sampling_policy_version="history-v1",
        detector_config_version="detector-1",
        expectation_catalog_version="catalog-1",
        provenance_status="complete",
    )
