"""Reliable raw-event Kafka consumer for the reference-tick use case."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from tinvest_signal_engine.application.reference_ticks import (
    NormalizedMarketEvent,
    ReferenceTickProcessor,
)
from tinvest_signal_engine.data_quality import validate_normalized_event_dict
from tinvest_signal_engine.kafka_proto import build_raw_value_deserializer


logger = logging.getLogger(__name__)


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
        value_deserializer=build_raw_value_deserializer(format_name=value_format),
    )


class ReferenceTickKafkaRuntime:
    def __init__(self, *, consumer: Any, processor: ReferenceTickProcessor) -> None:
        self._consumer = consumer
        self._processor = processor

    def run(self) -> None:
        try:
            while True:
                polled = self._consumer.poll(timeout_ms=1_000, max_records=500)
                messages = tuple(
                    message
                    for partition_messages in polled.values()
                    for message in partition_messages
                )
                if not messages:
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
                self._processor.process_many(tuple(events))
                self._commit_batch(messages)
        except KeyboardInterrupt:
            logger.info("Reference tick writer stopped by user")
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
