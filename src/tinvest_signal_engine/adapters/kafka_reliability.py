"""Kafka adapters and runtime loop for reliable detector processing."""

from __future__ import annotations

import logging
from hashlib import sha256
from time import monotonic
from typing import Any, Callable, Sequence

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition

from tinvest_signal_engine.application.observability import ReliabilityMetrics
from tinvest_signal_engine.application.reliable_processing import (
    BrokerEvent,
    ReliableEventProcessor,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.data_quality import (
    log_validation_failure,
    validate_normalized_event_dict,
)
from tinvest_signal_engine.domain.reliable_processing import (
    EventReplayConflict,
    SignalRecord,
)
from tinvest_signal_engine.kafka_proto import (
    build_raw_value_deserializer,
    build_signal_value_serializer,
)
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.schema_registry import (
    register_protobuf_schema,
    schema_subject_for_topic,
)
from tinvest_signal_engine.serialization import (
    json_dumps_bytes,
    kafka_json_serializer,
    to_plain_data,
)


logger = logging.getLogger(__name__)

_DETECTOR_MAX_POLL_RECORDS = 500
_DETECTOR_POLL_TIMEOUT_MS = 1_000


def build_raw_consumer(settings: RuntimeSettings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=False,
        group_id=settings.kafka_consumer_group,
        max_poll_records=_DETECTOR_MAX_POLL_RECORDS,
        value_deserializer=build_raw_value_deserializer(
            format_name=settings.kafka_raw_value_format,
        ),
    )


class KafkaSignalPublisher:
    def __init__(self, settings: RuntimeSettings) -> None:
        serializer = _signal_serializer(settings)
        producer_kwargs: dict[str, Any] = {
            "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
            "acks": "all",
            "retries": 10,
            "max_in_flight_requests_per_connection": 1,
            "linger_ms": settings.kafka_linger_ms,
            "batch_size": settings.kafka_batch_bytes,
            "key_serializer": lambda value: value.encode("utf-8"),
            "value_serializer": serializer,
        }
        compression = _compression_type(settings.kafka_compression_codec)
        if compression is not None:
            producer_kwargs["compression_type"] = compression
        self._producer = KafkaProducer(**producer_kwargs)
        self._topic = settings.kafka_signal_topic
        self._protobuf = settings.kafka_signal_value_format == "protobuf"

    def publish(self, signals: Sequence[SignalRecord]) -> None:
        futures = []
        for signal in signals:
            trigger = _trigger_signal(signal)
            value: Any = trigger if self._protobuf else trigger.to_dict()
            futures.append(
                self._producer.send(
                    self._topic,
                    key=signal.instrument_id,
                    value=value,
                )
            )
        for future in futures:
            future.get(timeout=30)

    def close(self) -> None:
        self._producer.flush(timeout=30)
        self._producer.close(timeout=30)


class KafkaDlqPublisher:
    def __init__(self, settings: RuntimeSettings) -> None:
        self._topic = settings.kafka_raw_dlq_topic
        self._producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            acks="all",
            retries=10,
            max_in_flight_requests_per_connection=1,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=kafka_json_serializer,
        )

    def publish(
        self,
        message: Any,
        *,
        reason_code: str,
        details: Sequence[str],
    ) -> None:
        raw = message.value
        event_id = str(raw.get("event_id") or "") if isinstance(raw, dict) else ""
        envelope = {
            "reason_code": reason_code,
            "details": list(details),
            "source": {
                "topic": str(message.topic),
                "partition": int(message.partition),
                "offset": int(message.offset),
            },
            "event": to_plain_data(raw),
        }
        self._producer.send(
            self._topic,
            key=event_id or f"{message.partition}:{message.offset}",
            value=envelope,
        ).get(timeout=30)

    def close(self) -> None:
        self._producer.flush(timeout=30)
        self._producer.close(timeout=30)


class ReliableDetectorRuntime:
    def __init__(
        self,
        *,
        consumer: Any,
        processor: ReliableEventProcessor,
        signal_publisher: KafkaSignalPublisher,
        dlq_publisher: KafkaDlqPublisher,
        metrics: ReliabilityMetrics,
        checkpoint: Callable[[], None],
    ) -> None:
        self._consumer = consumer
        self._processor = processor
        self._signal_publisher = signal_publisher
        self._dlq_publisher = dlq_publisher
        self._metrics = metrics
        self._checkpoint = checkpoint

    def run(self) -> None:
        clean_shutdown = False
        try:
            while True:
                self.run_once()
        except KeyboardInterrupt:
            clean_shutdown = True
            logger.info("Detector service stopped by user")
        finally:
            if clean_shutdown:
                self._checkpoint()
            self._signal_publisher.close()
            self._dlq_publisher.close()
            self._consumer.close()

    def run_once(self) -> int:
        """Poll and safely finish one bounded batch.

        Detection and the PostgreSQL inbox transaction deliberately remain
        event-scoped.  Only the expensive external checkpoint and Kafka commit
        are coalesced.  If a record fails, the successfully completed prefix is
        checkpointed and committed; the failed record and every record after it
        remain uncommitted for replay.
        """

        polled = self._consumer.poll(
            timeout_ms=_DETECTOR_POLL_TIMEOUT_MS,
            max_records=_DETECTOR_MAX_POLL_RECORDS,
        )
        messages = tuple(
            message
            for partition_messages in polled.values()
            for message in partition_messages
        )
        if not messages:
            return 0

        started = monotonic()
        completed: list[Any] = []
        try:
            for message in messages:
                self._process_message(message)
                completed.append(message)
        except Exception:
            self._finish_batch(
                completed,
                outcome="partial_failure",
                started=started,
            )
            raise

        self._finish_batch(completed, outcome="completed", started=started)
        return len(completed)

    def _process_message(self, message: Any) -> None:
        raw = message.value
        if not isinstance(raw, dict):
            self._dead_letter(
                message,
                reason_code="invalid_shape",
                details=(f"expected mapping, got {type(raw).__name__}",),
            )
            return
        issues = validate_normalized_event_dict(raw)
        if issues:
            log_validation_failure(errors=issues, sample=raw)
            self._dead_letter(
                message,
                reason_code="invalid_payload",
                details=tuple(issues),
            )
            return
        event = _broker_event(message, raw)
        try:
            self._processor.process(event)
        except EventReplayConflict as conflict:
            self._dead_letter(
                message,
                reason_code="replay_conflict",
                details=(str(conflict),),
            )

    def _finish_batch(
        self,
        messages: Sequence[Any],
        *,
        outcome: str,
        started: float,
    ) -> None:
        if not messages:
            self._metrics.detector_batch_completed(
                message_count=0,
                partition_count=0,
                outcome=outcome,
                duration_seconds=max(0.0, monotonic() - started),
            )
            return
        self._checkpoint()
        partition_count = self._commit_batch(messages)
        self._metrics.detector_batch_completed(
            message_count=len(messages),
            partition_count=partition_count,
            outcome=outcome,
            duration_seconds=max(0.0, monotonic() - started),
        )

    def _dead_letter(
        self,
        message: Any,
        *,
        reason_code: str,
        details: Sequence[str],
    ) -> None:
        self._dlq_publisher.publish(
            message,
            reason_code=reason_code,
            details=details,
        )
        self._metrics.dead_lettered(reason_code=reason_code)

    def _commit_batch(self, messages: Sequence[Any]) -> int:
        offsets_by_partition: dict[TopicPartition, list[int]] = {}
        for message in messages:
            partition = TopicPartition(message.topic, message.partition)
            offsets_by_partition.setdefault(partition, []).append(int(message.offset))
        position: dict[TopicPartition, OffsetAndMetadata] = {}
        for partition, offsets in offsets_by_partition.items():
            if any(
                current <= previous for previous, current in zip(offsets, offsets[1:])
            ):
                raise RuntimeError("refusing to commit non-increasing Kafka offsets")
            # Kafka offsets may legitimately contain gaps (for example after
            # compaction).  Committing one past the last *handled* record is the
            # broker-defined safe position; no unhandled record is skipped.
            position[partition] = OffsetAndMetadata(offsets[-1] + 1, "")
        self._consumer.commit(offsets=position)
        self._metrics.offset_committed()
        return len(position)


def _broker_event(message: Any, raw: dict[str, object]) -> BrokerEvent:
    canonical = json_dumps_bytes(raw)
    return BrokerEvent(
        event_id=str(raw["event_id"]),
        event_type=str(raw["event_type"]),
        topic=str(message.topic),
        partition_id=int(message.partition),
        offset_id=int(message.offset),
        payload_sha256=sha256(canonical).digest(),
        payload=raw,
    )


def _signal_serializer(settings: RuntimeSettings) -> Callable[[Any], bytes]:
    schema_id = settings.kafka_protobuf_schema_id_signal
    register = None
    if (
        settings.kafka_signal_value_format == "protobuf"
        and schema_id is None
        and settings.schema_registry_url
    ):
        proto_path = settings.proto_dir / "trigger_signal.proto"
        subject = schema_subject_for_topic(settings.kafka_signal_topic)

        def register() -> int:
            return register_protobuf_schema(
                settings.schema_registry_url or "",
                subject,
                proto_path,
            )

    return build_signal_value_serializer(
        format_name=settings.kafka_signal_value_format,
        schema_id=schema_id,
        register_schema=register,
    )


def _compression_type(value: str) -> str | None:
    normalized = (value or "").strip().lower()
    return None if normalized in {"", "none", "off", "plaintext"} else normalized


def _trigger_signal(signal: SignalRecord) -> TriggerSignal:
    return TriggerSignal(
        signal_id=signal.signal_id,
        detected_at=signal.detected_at,
        instrument_id=signal.instrument_id,
        ticker=signal.ticker,
        class_code=signal.class_code,
        alias=signal.alias,
        source_event_type=signal.source_event_type,
        signal_type=signal.signal_type,
        severity=signal.severity,
        metric_value=signal.metric_value,
        baseline_value=signal.baseline_value,
        z_score=signal.z_score,
        window_seconds=signal.window_seconds,
        summary=signal.summary,
        payload=dict(signal.payload),
        source_event_id=signal.source_event_id,
        source_event_at=signal.source_event_at,
        signal_schema_version=signal.signal_schema_version,
        expectation_catalog_version=signal.expectation_catalog_version,
        detector_config_version=signal.detector_config_version,
        delivery_config_version=signal.delivery_config_version,
        cost_model_version=signal.cost_model_version,
        provenance_status=signal.provenance_status,
    )
