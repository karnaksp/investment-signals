"""PostgreSQL adapters for inbox, signal transaction, and delivery outbox."""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row

from tinvest_signal_engine.application.reliable_processing import (
    BrokerEvent,
    DetectionBatch,
    DetectorConfigAcknowledgement,
    DetectorStateCheckpoint,
    StoredEvent,
)
from tinvest_signal_engine.application.observation_publication import (
    ObservationPublicationTask,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.domain.detector_observations import DetectorObservation
from tinvest_signal_engine.domain.reliable_processing import (
    DeadLetterDelivery,
    DeliveryTask,
    EventReplayConflict,
    SignalRecord,
    deterministic_outbox_id,
)
from tinvest_signal_engine.serialization import json_dumps, parse_timestamp


def _safe_identifier(value: str) -> str:
    candidate = value.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", candidate):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return candidate


class PostgresReliableProcessingStore:
    def __init__(self, connection: Any, *, signal_table: str) -> None:
        self._connection = connection
        self._signal_table = _safe_identifier(signal_table)

    def find_processed(self, event: BrokerEvent) -> StoredEvent | None:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            row = self._find_inbox_row(cursor, event)
            if row is None:
                return None
            self._validate_inbox_row(row, event)
            signals = self._load_signals(cursor, event.event_id)
        return StoredEvent(signals=signals, replayed=True)

    def persist_detection_once(
        self,
        event: BrokerEvent,
        batch: DetectionBatch,
    ) -> StoredEvent:
        with self._connection.transaction():
            with self._connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    INSERT INTO processed_events (
                        event_id, topic, partition_id, offset_id, payload_sha256
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    RETURNING event_id
                    """,
                    (
                        event.event_id,
                        event.topic,
                        event.partition_id,
                        event.offset_id,
                        event.payload_sha256,
                    ),
                )
                inserted = cursor.fetchone() is not None
                if not inserted:
                    row = self._find_inbox_row(cursor, event)
                    if row is None:
                        raise EventReplayConflict(
                            "processed event conflict without matching inbox row"
                        )
                    self._validate_inbox_row(row, event)
                    return StoredEvent(
                        signals=self._load_signals(cursor, event.event_id),
                        replayed=True,
                    )

                for prepared in batch.signals:
                    self._validate_signal_association(event, prepared.signal)
                    self._insert_signal(cursor, prepared.signal)
                    for target in prepared.delivery_targets:
                        self._insert_outbox(cursor, prepared.signal, target)
                for observation in batch.observations:
                    self._insert_observation(cursor, event, observation)
                if batch.checkpoint is None:
                    raise ValueError("detection batch must include a state checkpoint")
                self._upsert_checkpoint(cursor, event, batch.checkpoint)

                stored = self._load_signals(cursor, event.event_id)
        return StoredEvent(signals=stored, replayed=False)

    def load_state_checkpoints(self) -> tuple[DetectorStateCheckpoint, ...]:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute("""
                SELECT instrument_id, state_schema_version,
                       detector_config_version, snapshot_payload,
                       snapshot_sha256
                FROM detector_state_snapshots
                ORDER BY instrument_id
                """)
            rows = cursor.fetchall()
        return tuple(
            DetectorStateCheckpoint(
                instrument_id=str(row["instrument_id"]),
                state_schema_version=str(row["state_schema_version"]),
                detector_config_version=str(row["detector_config_version"]),
                payload=bytes(row["snapshot_payload"]),
                payload_sha256=bytes(row["snapshot_sha256"]),
            )
            for row in rows
        )

    def persist_detector_config_ack(
        self,
        acknowledgement: DetectorConfigAcknowledgement,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO detector_config_acknowledgements (
                    detector_instance_id, detector_config_version, status,
                    failure_reason_code, configured_instruments_count, loaded_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    acknowledgement.detector_instance_id,
                    acknowledgement.detector_config_version,
                    acknowledgement.status,
                    acknowledgement.failure_reason_code,
                    acknowledgement.configured_instruments_count,
                    acknowledgement.loaded_at,
                ),
            )
            self._connection.commit()

    @staticmethod
    def _upsert_checkpoint(
        cursor: Any,
        event: BrokerEvent,
        checkpoint: DetectorStateCheckpoint,
    ) -> None:
        payload_instrument = event.payload.get("instrument_id")
        if payload_instrument != checkpoint.instrument_id:
            raise ValueError("checkpoint instrument must match broker event")
        cursor.execute(
            """
            INSERT INTO detector_state_snapshots (
                instrument_id, source_event_id, topic, partition_id, offset_id,
                state_schema_version, detector_config_version,
                snapshot_payload, snapshot_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id) DO UPDATE
            SET source_event_id = EXCLUDED.source_event_id,
                topic = EXCLUDED.topic,
                partition_id = EXCLUDED.partition_id,
                offset_id = EXCLUDED.offset_id,
                state_schema_version = EXCLUDED.state_schema_version,
                detector_config_version = EXCLUDED.detector_config_version,
                snapshot_payload = EXCLUDED.snapshot_payload,
                snapshot_sha256 = EXCLUDED.snapshot_sha256,
                updated_at = now()
            WHERE detector_state_snapshots.topic = EXCLUDED.topic
              AND detector_state_snapshots.partition_id = EXCLUDED.partition_id
              AND detector_state_snapshots.offset_id < EXCLUDED.offset_id
            RETURNING source_event_id
            """,
            (
                checkpoint.instrument_id,
                event.event_id,
                event.topic,
                event.partition_id,
                event.offset_id,
                checkpoint.state_schema_version,
                checkpoint.detector_config_version,
                checkpoint.payload,
                checkpoint.payload_sha256,
            ),
        )
        if cursor.fetchone() is None:
            raise EventReplayConflict(
                "detector state checkpoint did not advance its broker partition"
            )

    @staticmethod
    def _validate_signal_association(
        event: BrokerEvent,
        signal: SignalRecord,
    ) -> None:
        matches = (
            signal.source_event_id == event.event_id
            and signal.source_event_type == event.event_type
            and signal.instrument_id == event.payload.get("instrument_id")
        )
        source_time = event.payload.get("source_time")
        if source_time is not None:
            matches = (
                matches
                and isinstance(source_time, (str, datetime))
                and signal.source_event_at == parse_timestamp(source_time)
            )
        if not matches:
            raise ValueError("signal source association must match broker event")

    @staticmethod
    def _insert_observation(
        cursor: Any,
        event: BrokerEvent,
        observation: DetectorObservation,
    ) -> None:
        if (
            observation.source_event_id != event.event_id
            or observation.source_event_type != event.event_type
            or observation.instrument_id != event.payload.get("instrument_id")
        ):
            raise ValueError(
                "detector observation source association must match broker event"
            )
        payload = detector_observation_to_dict(observation)
        cursor.execute(
            """
            INSERT INTO detector_observation_outbox (
                observation_id, source_event_id, payload_json
            ) VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (observation_id) DO NOTHING
            RETURNING observation_id
            """,
            (
                observation.observation_id,
                observation.source_event_id,
                json_dumps(payload),
            ),
        )
        if cursor.fetchone() is not None:
            return
        cursor.execute(
            """
            SELECT source_event_id, payload_json
            FROM detector_observation_outbox
            WHERE observation_id = %s
            """,
            (observation.observation_id,),
        )
        row = cursor.fetchone()
        if row is None or (
            str(row["source_event_id"]) != observation.source_event_id
            or dict(row["payload_json"]) != payload
        ):
            raise EventReplayConflict(
                "observation id was reused with different content"
            )

    def count_delivered_since(
        self,
        *,
        since: datetime,
        instrument_id: str | None = None,
        signal_type: str | None = None,
    ) -> int:
        conditions = [
            "detected_at >= %s",
            "coalesce(payload_json->>'delivery_status', 'unknown') = 'delivered'",
        ]
        parameters: list[object] = [since]
        if instrument_id:
            conditions.append("instrument_id = %s")
            parameters.append(instrument_id)
        if signal_type:
            conditions.append("signal_type = %s")
            parameters.append(signal_type)
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM {self._signal_table} "
                f"WHERE {' AND '.join(conditions)}",
                tuple(parameters),
            )
            row = cursor.fetchone()
        return int(row[0] if row else 0)

    def close(self) -> None:
        self._connection.close()

    @staticmethod
    def _find_inbox_row(cursor: Any, event: BrokerEvent) -> dict[str, Any] | None:
        cursor.execute(
            """
            SELECT event_id, topic, partition_id, offset_id, payload_sha256
            FROM processed_events
            WHERE event_id = %s
               OR (topic = %s AND partition_id = %s AND offset_id = %s)
            ORDER BY CASE WHEN event_id = %s THEN 0 ELSE 1 END
            LIMIT 1
            """,
            (
                event.event_id,
                event.topic,
                event.partition_id,
                event.offset_id,
                event.event_id,
            ),
        )
        row = cursor.fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _validate_inbox_row(row: dict[str, Any], event: BrokerEvent) -> None:
        matches = (
            str(row["event_id"]) == event.event_id
            and str(row["topic"]) == event.topic
            and int(row["partition_id"]) == event.partition_id
            and int(row["offset_id"]) == event.offset_id
            and bytes(row["payload_sha256"]) == event.payload_sha256
        )
        if not matches:
            raise EventReplayConflict(
                "event id or broker position was reused with different content"
            )

    def _insert_signal(self, cursor: Any, signal: SignalRecord) -> None:
        if signal.source_event_id is None:
            raise ValueError("reliable signal requires source_event_id")
        cursor.execute(
            f"""
            INSERT INTO {self._signal_table} (
                signal_id, detected_at, instrument_id, ticker, class_code,
                alias, source_event_type, signal_type, severity, metric_value,
                baseline_value, z_score, window_seconds, summary, payload_json,
                source_event_id, source_event_at, signal_schema_version,
                expectation_catalog_version, detector_config_version,
                delivery_config_version, cost_model_version, provenance_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (signal_id) DO NOTHING
            RETURNING signal_id
            """,
            (
                signal.signal_id,
                signal.detected_at,
                signal.instrument_id,
                signal.ticker,
                signal.class_code,
                signal.alias,
                signal.source_event_type,
                signal.signal_type,
                signal.severity,
                signal.metric_value,
                signal.baseline_value,
                signal.z_score,
                signal.window_seconds,
                signal.summary,
                json_dumps(signal.payload),
                signal.source_event_id,
                signal.source_event_at,
                signal.signal_schema_version,
                signal.expectation_catalog_version,
                signal.detector_config_version,
                signal.delivery_config_version,
                signal.cost_model_version,
                signal.provenance_status,
            ),
        )
        if cursor.fetchone() is not None:
            return
        cursor.execute(
            f"""
            SELECT
                signal_id, detected_at, instrument_id, ticker, class_code,
                alias, source_event_type, signal_type, severity, metric_value,
                baseline_value, z_score, window_seconds, summary, payload_json,
                source_event_id, source_event_at, signal_schema_version,
                expectation_catalog_version, detector_config_version,
                delivery_config_version, cost_model_version, provenance_status
            FROM {self._signal_table}
            WHERE signal_id = %s
            """,
            (signal.signal_id,),
        )
        row = cursor.fetchone()
        if row is None or signal_record_from_row(dict(row)) != signal:
            raise EventReplayConflict("signal id was reused with different content")

    @staticmethod
    def _insert_outbox(cursor: Any, signal: SignalRecord, target: Any) -> None:
        outbox_id = deterministic_outbox_id(
            signal.signal_id,
            target.destination_type,
            target.key_hash,
        )
        cursor.execute(
            """
            INSERT INTO delivery_outbox (
                outbox_id, signal_id, destination_type,
                destination_key_hash, payload_json
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (signal_id, destination_type, destination_key_hash)
            DO NOTHING
            RETURNING outbox_id
            """,
            (
                outbox_id,
                signal.signal_id,
                target.destination_type,
                target.key_hash,
                json_dumps(signal_record_to_dict(signal)),
            ),
        )
        if cursor.fetchone() is not None:
            return
        cursor.execute(
            """
            SELECT outbox_id, payload_json
            FROM delivery_outbox
            WHERE signal_id = %s AND destination_type = %s
              AND destination_key_hash = %s
            """,
            (signal.signal_id, target.destination_type, target.key_hash),
        )
        row = cursor.fetchone()
        expected_payload = signal_record_to_dict(signal)
        if row is None or (
            str(row["outbox_id"]) != outbox_id
            or dict(row["payload_json"]) != expected_payload
        ):
            raise EventReplayConflict(
                "delivery target was reused with different content"
            )

    def _load_signals(self, cursor: Any, event_id: str) -> tuple[SignalRecord, ...]:
        cursor.execute(
            f"""
            SELECT
                signal_id, detected_at, instrument_id, ticker, class_code,
                alias, source_event_type, signal_type, severity, metric_value,
                baseline_value, z_score, window_seconds, summary, payload_json,
                source_event_id, source_event_at, signal_schema_version,
                expectation_catalog_version, detector_config_version,
                delivery_config_version, cost_model_version, provenance_status
            FROM {self._signal_table}
            WHERE source_event_id = %s
            ORDER BY signal_id
            """,
            (event_id,),
        )
        return tuple(signal_record_from_row(dict(row)) for row in cursor.fetchall())


class PostgresDeliveryQueue:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def claim(
        self,
        *,
        available_at: datetime,
        lease_until: datetime,
    ) -> DeliveryTask | None:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH candidate AS (
                    SELECT outbox_id
                    FROM delivery_outbox
                    WHERE next_attempt_at <= %s
                      AND status IN ('pending', 'failed', 'delivering')
                    ORDER BY next_attempt_at, outbox_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE delivery_outbox AS target
                SET status = 'delivering',
                    attempt_count = target.attempt_count + 1,
                    next_attempt_at = %s
                FROM candidate
                WHERE target.outbox_id = candidate.outbox_id
                RETURNING target.outbox_id, target.signal_id,
                          target.destination_type, target.payload_json,
                          target.attempt_count, target.last_error_code
                """,
                (available_at, lease_until),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        payload = row["payload_json"]
        return DeliveryTask(
            outbox_id=str(row["outbox_id"]),
            signal_id=str(row["signal_id"]),
            destination_type=str(row["destination_type"]),
            payload=dict(payload) if isinstance(payload, dict) else {},
            attempt_count=int(row["attempt_count"]),
            previous_error_code=(
                str(row["last_error_code"])
                if row["last_error_code"] is not None
                else None
            ),
        )

    def mark_delivered(
        self,
        task: DeliveryTask,
        *,
        delivered_at: datetime,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE delivery_outbox
                SET status = 'delivered', delivered_at = %s,
                    last_error_code = NULL
                WHERE outbox_id = %s AND status = 'delivering'
                  AND attempt_count = %s
                """,
                (delivered_at, task.outbox_id, task.attempt_count),
            )

    def mark_failed(
        self,
        task: DeliveryTask,
        *,
        reason_code: str,
        next_attempt_at: datetime,
        dead_letter: bool,
    ) -> None:
        status = "dead_letter" if dead_letter else "failed"
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE delivery_outbox
                SET status = %s, next_attempt_at = %s,
                    last_error_code = %s
                WHERE outbox_id = %s AND status = 'delivering'
                  AND attempt_count = %s
                """,
                (
                    status,
                    next_attempt_at,
                    reason_code[:200],
                    task.outbox_id,
                    task.attempt_count,
                ),
            )

    def get_for_manual_retry(
        self, *, outbox_id: str
    ) -> DeadLetterDelivery | None:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT outbox_id, destination_type, status,
                       attempt_count, last_error_code
                FROM delivery_outbox
                WHERE outbox_id = %s
                """,
                (outbox_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return DeadLetterDelivery(
            outbox_id=str(row["outbox_id"]),
            destination_type=str(row["destination_type"]),
            status=str(row["status"]),
            attempt_count=int(row["attempt_count"]),
            last_error_code=(
                str(row["last_error_code"])
                if row["last_error_code"] is not None
                else None
            ),
        )

    def requeue_dead_letter(
        self,
        delivery: DeadLetterDelivery,
        *,
        available_at: datetime,
    ) -> bool:
        """Atomically requeue the exact dead-letter state approved by the use case."""

        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE delivery_outbox
                SET status = 'pending',
                    attempt_count = 0,
                    next_attempt_at = %s,
                    delivered_at = NULL
                WHERE outbox_id = %s
                  AND status = 'dead_letter'
                  AND attempt_count = %s
                  AND last_error_code IS NOT DISTINCT FROM %s
                """,
                (
                    available_at,
                    delivery.outbox_id,
                    delivery.attempt_count,
                    delivery.last_error_code,
                ),
            )
            return cursor.rowcount == 1

    def close(self) -> None:
        self._connection.close()


class PostgresObservationPublicationQueue:
    """Lease-based queue over the immutable detector observation outbox."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def claim(
        self,
        *,
        available_at: datetime,
        lease_until: datetime,
    ) -> ObservationPublicationTask | None:
        tasks = self.claim_many(
            available_at=available_at,
            lease_until=lease_until,
            limit=1,
        )
        return tasks[0] if tasks else None

    def claim_many(
        self,
        *,
        available_at: datetime,
        lease_until: datetime,
        limit: int,
    ) -> tuple[ObservationPublicationTask, ...]:
        if limit < 1 or limit > 1000:
            raise ValueError("observation claim limit must be between 1 and 1000")
        with self._connection.transaction():
            with self._connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    UPDATE detector_observation_outbox
                    SET status = 'failed', claimed_at = NULL,
                        next_attempt_at = %s,
                        last_error_code = 'claim_lease_expired'
                    WHERE status = 'publishing'
                      AND next_attempt_at <= %s
                    """,
                    (available_at, available_at),
                )
                cursor.execute(
                    """
                    WITH candidate AS (
                        SELECT observation_id
                        FROM detector_observation_outbox
                        WHERE next_attempt_at <= %s
                          AND status IN ('pending', 'failed')
                        ORDER BY next_attempt_at, observation_id
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE detector_observation_outbox AS target
                    SET status = 'publishing',
                        attempt_count = target.attempt_count + 1,
                        claimed_at = %s,
                        next_attempt_at = %s,
                        last_error_code = NULL
                    FROM candidate
                    WHERE target.observation_id = candidate.observation_id
                    RETURNING target.payload_json, target.attempt_count
                    """,
                    (available_at, limit, available_at, lease_until),
                )
                rows = cursor.fetchall()
        return tuple(
            ObservationPublicationTask(
                observation=detector_observation_from_dict(dict(row["payload_json"])),
                attempt_count=int(row["attempt_count"]),
            )
            for row in rows
        )

    def purge_published(self, *, before: datetime, limit: int) -> int:
        if limit < 1 or limit > 10_000:
            raise ValueError("observation purge limit must be between 1 and 10000")
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                WITH purgeable AS (
                    SELECT observation_id
                    FROM detector_observation_outbox
                    WHERE status = 'published'
                      AND published_at < %s
                      AND published_at < now() - INTERVAL '7 days'
                    ORDER BY published_at, observation_id
                    LIMIT %s
                )
                DELETE FROM detector_observation_outbox AS target
                USING purgeable
                WHERE target.observation_id = purgeable.observation_id
                """,
                (before, limit),
            )
            return int(cursor.rowcount)

    def mark_published(
        self,
        task: ObservationPublicationTask,
        *,
        published_at: datetime,
    ) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE detector_observation_outbox
                SET status = 'published', claimed_at = NULL,
                    published_at = %s, last_error_code = NULL
                WHERE observation_id = %s AND status = 'publishing'
                  AND attempt_count = %s
                """,
                (
                    published_at,
                    task.observation.observation_id,
                    task.attempt_count,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("observation publication lease was lost")

    def mark_failed(
        self,
        task: ObservationPublicationTask,
        *,
        reason_code: str,
        next_attempt_at: datetime,
        dead_letter: bool,
    ) -> None:
        status = "dead_letter" if dead_letter else "failed"
        with self._connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE detector_observation_outbox
                SET status = %s, claimed_at = NULL,
                    next_attempt_at = %s, last_error_code = %s
                WHERE observation_id = %s AND status = 'publishing'
                  AND attempt_count = %s
                """,
                (
                    status,
                    next_attempt_at,
                    reason_code[:200],
                    task.observation.observation_id,
                    task.attempt_count,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("observation publication lease was lost")

    def close(self) -> None:
        self._connection.close()


def connect_reliable_processing_store(
    settings: RuntimeSettings,
) -> PostgresReliableProcessingStore:
    connection = _connect_with_retry(settings, service_name="detector")
    return PostgresReliableProcessingStore(
        connection,
        signal_table=settings.postgres_table,
    )


def connect_delivery_queue(settings: RuntimeSettings) -> PostgresDeliveryQueue:
    return PostgresDeliveryQueue(
        _connect_with_retry(settings, service_name="delivery-worker")
    )


def connect_observation_publication_queue(
    settings: RuntimeSettings,
) -> PostgresObservationPublicationQueue:
    return PostgresObservationPublicationQueue(
        _connect_with_retry(settings, service_name="observation-worker")
    )


def _connect_with_retry(settings: RuntimeSettings, *, service_name: str) -> Any:
    deadline = time.monotonic() + settings.postgres_startup_timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            connection = connect(
                host=settings.postgres_host,
                port=settings.postgres_port,
                dbname=settings.postgres_database,
                user=settings.postgres_username,
                password=settings.postgres_password,
                autocommit=True,
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return connection
        except Exception as error:
            last_error = error
            time.sleep(settings.postgres_startup_check_interval_seconds)
    raise RuntimeError(
        f"{service_name} could not connect to Postgres within "
        f"{settings.postgres_startup_timeout_seconds}s"
    ) from last_error


def detector_observation_to_dict(
    observation: DetectorObservation,
) -> dict[str, object]:
    """Map a domain observation to the immutable outbox transport record."""

    return {
        "observation_id": observation.observation_id,
        "source_event_id": observation.source_event_id,
        "observed_at": observation.observed_at.isoformat(),
        "instrument_id": observation.instrument_id,
        "source_event_type": observation.source_event_type,
        "signal_type": observation.signal_type,
        "metric_value": observation.metric_value,
        "baseline_value": observation.baseline_value,
        "z_score": observation.z_score,
        "threshold_value": observation.threshold_value,
        "threshold_passed": observation.threshold_passed,
        "detector_passed": observation.detector_passed,
        "signal_emitted": observation.signal_emitted,
        "window_seconds": observation.window_seconds,
        "sampling_policy_version": observation.sampling_policy_version,
        "detector_config_version": observation.detector_config_version,
        "expectation_catalog_version": observation.expectation_catalog_version,
        "provenance_status": observation.provenance_status,
    }


def detector_observation_from_dict(
    payload: dict[str, object],
) -> DetectorObservation:
    """Map an outbox record back to a validated domain observation."""

    observed_at = datetime.fromisoformat(str(payload["observed_at"]))
    catalog = payload.get("expectation_catalog_version")
    return DetectorObservation(
        observation_id=str(payload["observation_id"]),
        source_event_id=str(payload["source_event_id"]),
        observed_at=observed_at,
        instrument_id=str(payload["instrument_id"]),
        source_event_type=str(payload["source_event_type"]),
        signal_type=str(payload["signal_type"]),
        metric_value=float(payload["metric_value"]),
        baseline_value=float(payload["baseline_value"]),
        z_score=float(payload["z_score"]),
        threshold_value=float(payload["threshold_value"]),
        threshold_passed=_require_bool(payload, "threshold_passed"),
        detector_passed=_require_bool(payload, "detector_passed"),
        signal_emitted=_require_bool(payload, "signal_emitted"),
        window_seconds=int(payload["window_seconds"]),
        sampling_policy_version=str(payload["sampling_policy_version"]),
        detector_config_version=str(payload["detector_config_version"]),
        expectation_catalog_version=(str(catalog) if catalog is not None else None),
        provenance_status=str(payload["provenance_status"]),
    )


def _require_bool(payload: dict[str, object], key: str) -> bool:
    value = payload[key]
    if not isinstance(value, bool):
        raise ValueError(f"observation field {key} must be boolean")
    return value


def signal_record_to_dict(signal: SignalRecord) -> dict[str, object]:
    return {
        "signal_id": signal.signal_id,
        "detected_at": signal.detected_at.isoformat(),
        "instrument_id": signal.instrument_id,
        "ticker": signal.ticker,
        "class_code": signal.class_code,
        "alias": signal.alias,
        "source_event_type": signal.source_event_type,
        "signal_type": signal.signal_type,
        "severity": signal.severity,
        "metric_value": signal.metric_value,
        "baseline_value": signal.baseline_value,
        "z_score": signal.z_score,
        "window_seconds": signal.window_seconds,
        "summary": signal.summary,
        "payload": dict(signal.payload),
        "source_event_id": signal.source_event_id,
        "source_event_at": (
            signal.source_event_at.isoformat() if signal.source_event_at else None
        ),
        "signal_schema_version": signal.signal_schema_version,
        "expectation_catalog_version": signal.expectation_catalog_version,
        "detector_config_version": signal.detector_config_version,
        "delivery_config_version": signal.delivery_config_version,
        "cost_model_version": signal.cost_model_version,
        "provenance_status": signal.provenance_status,
    }


def signal_record_from_row(row: dict[str, Any]) -> SignalRecord:
    payload = row.get("payload_json")
    return SignalRecord(
        signal_id=str(row["signal_id"]),
        detected_at=row["detected_at"],
        instrument_id=str(row["instrument_id"]),
        ticker=str(row["ticker"]),
        class_code=str(row["class_code"]),
        alias=str(row["alias"]),
        source_event_type=str(row["source_event_type"]),
        signal_type=str(row["signal_type"]),
        severity=int(row["severity"]),
        metric_value=float(row["metric_value"]),
        baseline_value=float(row["baseline_value"]),
        z_score=float(row["z_score"]),
        window_seconds=int(row["window_seconds"]),
        summary=str(row["summary"]),
        payload=dict(payload) if isinstance(payload, dict) else {},
        source_event_id=(
            str(row["source_event_id"])
            if row.get("source_event_id") is not None
            else None
        ),
        source_event_at=row.get("source_event_at"),
        signal_schema_version=str(row["signal_schema_version"]),
        expectation_catalog_version=row.get("expectation_catalog_version"),
        detector_config_version=row.get("detector_config_version"),
        delivery_config_version=row.get("delivery_config_version"),
        cost_model_version=row.get("cost_model_version"),
        provenance_status=str(row["provenance_status"]),
    )
