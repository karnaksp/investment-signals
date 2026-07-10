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
            for message in self._consumer:
                raw = message.value
                if not isinstance(raw, dict):
                    logger.warning("Skipping non-object reference event")
                    self._commit(message)
                    continue
                issues = validate_normalized_event_dict(raw)
                if issues:
                    logger.warning(
                        "Skipping invalid reference event event_id=%s issues=%s",
                        raw.get("event_id"),
                        ",".join(issues),
                    )
                    self._commit(message)
                    continue
                try:
                    event = _normalized_market_event(raw)
                    self._processor.process(event)
                except (TypeError, ValueError) as error:
                    logger.warning(
                        "Skipping unmappable reference event event_id=%s error=%s",
                        raw.get("event_id"),
                        error,
                    )
                self._commit(message)
        except KeyboardInterrupt:
            logger.info("Reference tick writer stopped by user")
        finally:
            self._consumer.close()

    def _commit(self, message: Any) -> None:
        position = {
            TopicPartition(message.topic, message.partition): OffsetAndMetadata(
                message.offset + 1,
                "",
            )
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
