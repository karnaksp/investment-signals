"""Reliable raw-event Kafka consumer for the reference-tick use case."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from random import random
from threading import Event
from typing import Any, Callable

from kafka import KafkaConsumer
from kafka.errors import CommitFailedError, KafkaTimeoutError
from kafka.structs import OffsetAndMetadata, TopicPartition

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.dependency_recovery import (
    DependencyRecoveryMetrics,
    NoopDependencyRecoveryMetrics,
    record_dependency_recovered,
    wait_for_dependency,
)
from tinvest_signal_engine.application.reference_ticks import (
    NormalizedMarketEvent,
    ReferenceTickProcessor,
)
from tinvest_signal_engine.application.worker_health import (
    NoopWorkerHealthReporter,
    WorkerHealthReporter,
)
from tinvest_signal_engine.data_quality import validate_normalized_event_dict
from tinvest_signal_engine.kafka_proto import build_raw_value_deserializer


logger = logging.getLogger(__name__)
_REFERENCE_TICK_MAX_POLL_RECORDS = 10_000
_REFERENCE_TICK_FETCH_MIN_BYTES = 512 * 1024
_REFERENCE_TICK_FETCH_MAX_WAIT_MS = 2_000
_MAX_POLL_INTERVAL_MS = 900_000


def build_reference_tick_consumer(
    *,
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    auto_offset_reset: str,
    value_format: str,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        group_id=group_id,
        fetch_min_bytes=_REFERENCE_TICK_FETCH_MIN_BYTES,
        fetch_max_wait_ms=_REFERENCE_TICK_FETCH_MAX_WAIT_MS,
        max_poll_interval_ms=_MAX_POLL_INTERVAL_MS,
        max_poll_records=_REFERENCE_TICK_MAX_POLL_RECORDS,
        value_deserializer=build_raw_value_deserializer(format_name=value_format),
    )


class ReferenceTickKafkaRuntime:
    def __init__(
        self,
        *,
        consumer: Any,
        processor: ReferenceTickProcessor,
        backoff: BoundedExponentialBackoff = BoundedExponentialBackoff(),
        metrics: DependencyRecoveryMetrics | None = None,
        health: WorkerHealthReporter | None = None,
        random_value: Callable[[], float] = random,
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._backoff = backoff
        self._metrics = metrics or NoopDependencyRecoveryMetrics()
        self._health = health or NoopWorkerHealthReporter()
        self._random_value = random_value

    def run(self, *, stop_event: Event | None = None) -> None:
        stop = stop_event or Event()
        consecutive_failures = 0
        try:
            while not stop.is_set():
                self._health.heartbeat()
                polled = self._consumer.poll(
                    timeout_ms=1_000,
                    max_records=_REFERENCE_TICK_MAX_POLL_RECORDS,
                )
                messages = tuple(
                    message
                    for partition_messages in polled.values()
                    for message in partition_messages
                )
                if not messages:
                    self._health.succeeded()
                    continue
                events: list[NormalizedMarketEvent] = []
                for message in messages:
                    raw = message.value
                    if not isinstance(raw, dict):
                        logger.warning("Skipping non-object reference event")
                        continue
                    issues = validate_normalized_event_dict(raw)
                    if issues:
                        logger.warning(
                            "Skipping invalid reference event event_id=%s issues=%s",
                            raw.get("event_id"),
                            ",".join(issues),
                        )
                        continue
                    try:
                        events.append(_normalized_market_event(raw))
                    except (TypeError, ValueError) as error:
                        logger.warning(
                            "Skipping unmappable reference event event_id=%s error=%s",
                            raw.get("event_id"),
                            error,
                        )
                while not stop.is_set():
                    try:
                        self._processor.process_many(tuple(events))
                    except TransientClickHouseError as error:
                        self._health.failed(error.reason_code)
                        consecutive_failures += 1
                        if wait_for_dependency(
                            worker="reference_tick_writer",
                            error=error,
                            consecutive_failures=consecutive_failures,
                            stop_event=stop,
                            backoff=self._backoff,
                            metrics=self._metrics,
                            logger=logger,
                            random_value=self._random_value,
                        ):
                            break
                        continue
                    record_dependency_recovered(
                        worker="reference_tick_writer",
                        operation="reference_tick_batch_insert",
                        consecutive_failures=consecutive_failures,
                        metrics=self._metrics,
                        logger=logger,
                    )
                    consecutive_failures = 0
                    break
                if stop.is_set():
                    break
                try:
                    self._commit_batch(messages)
                except (CommitFailedError, KafkaTimeoutError):
                    self._health.failed("kafka_commit_uncertain")
                    logger.warning(
                        "Reference tick Kafka commit became uncertain; "
                        "rejoining before the next batch"
                    )
                    continue
                self._health.succeeded(force=True)
        except KeyboardInterrupt:
            stop.set()
            logger.info("Reference tick writer stopped by user")
        except Exception:
            self._health.failed("worker_cycle_failed")
            raise
        finally:
            self._consumer.close()

    def _commit_batch(self, messages: tuple[Any, ...]) -> None:
        offsets: dict[TopicPartition, int] = {}
        for message in messages:
            partition = TopicPartition(message.topic, message.partition)
            offsets[partition] = max(offsets.get(partition, 0), message.offset + 1)
        position = {
            partition: OffsetAndMetadata(offset, "")
            for partition, offset in offsets.items()
        }
        self._consumer.commit(offsets=position)


def _normalized_market_event(raw: dict[str, object]) -> NormalizedMarketEvent:
    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    return NormalizedMarketEvent(
        event_id=str(raw["event_id"]),
        event_type=str(raw["event_type"]),
        instrument_id=str(raw["instrument_id"]),
        source_time=_timestamp(raw["source_time"]),
        received_at=_timestamp(raw["received_at"]),
        payload=payload,
    )


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("timestamp must be a string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)
