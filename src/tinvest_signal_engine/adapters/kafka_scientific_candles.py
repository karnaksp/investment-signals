"""Manual-commit Kafka adapter for the scientific candle journal."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from tinvest_signal_engine.application.scientific_candles import (
    NormalizedCandleEvent,
    ScientificCandleJournalProcessor,
)
from tinvest_signal_engine.data_quality import validate_normalized_event_dict
from tinvest_signal_engine.kafka_proto import build_raw_value_deserializer


logger = logging.getLogger(__name__)


def build_scientific_candle_consumer(
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


class ScientificCandleKafkaRuntime:
    def __init__(
        self, *, consumer: Any, processor: ScientificCandleJournalProcessor
    ) -> None:
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
                events: list[NormalizedCandleEvent] = []
                for message in messages:
                    raw = message.value
                    if not isinstance(raw, dict):
                        logger.warning("Skipping non-object candle event")
                        continue
                    issues = validate_normalized_event_dict(raw)
                    if issues:
                        logger.warning(
                            "Skipping invalid candle event event_id=%s issues=%s",
                            raw.get("event_id"),
                            ",".join(issues),
                        )
                        continue
                    if raw.get("event_type") != "candle":
                        continue
                    try:
                        events.append(_event(raw))
                    except (TypeError, ValueError) as error:
                        logger.warning(
                            "Skipping unmappable candle event event_id=%s error=%s",
                            raw.get("event_id"),
                            error,
                        )
                self._processor.process_many(tuple(events))
                self._commit(messages)
        except KeyboardInterrupt:
            logger.info("Scientific candle writer stopped by user")
        finally:
            self._consumer.close()

    def _commit(self, messages: tuple[Any, ...]) -> None:
        offsets: dict[TopicPartition, int] = {}
        for message in messages:
            partition = TopicPartition(message.topic, message.partition)
            offsets[partition] = max(offsets.get(partition, 0), message.offset + 1)
        self._consumer.commit(
            offsets={
                partition: OffsetAndMetadata(offset, "")
                for partition, offset in offsets.items()
            }
        )


def _event(raw: dict[str, object]) -> NormalizedCandleEvent:
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("payload must be an object")
    return NormalizedCandleEvent(
        event_id=str(raw["event_id"]),
        event_type=str(raw["event_type"]),
        instrument_id=str(raw["instrument_id"]),
        ticker=str(raw.get("ticker") or raw["instrument_id"]),
        class_code=str(raw.get("class_code") or "TQBR"),
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
