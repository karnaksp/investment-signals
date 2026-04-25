from __future__ import annotations

import json
import logging

from kafka import KafkaConsumer, KafkaProducer

from ..config import RuntimeSettings, load_detector_settings
from ..detector_core import SignalDetector
from ..logging_utils import configure_logging
from ..models import NormalizedEvent
from ..sinks import WebhookAlertSink, create_clickhouse_signal_store_with_retry

logger = logging.getLogger(__name__)


def build_consumer(settings: RuntimeSettings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=True,
        group_id=settings.kafka_consumer_group,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def build_signal_producer(settings: RuntimeSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        acks="all",
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
    )


def main() -> None:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    detector = SignalDetector(load_detector_settings(settings.detector_path))
    consumer = build_consumer(settings)
    producer = build_signal_producer(settings)
    signal_store = create_clickhouse_signal_store_with_retry(
        settings,
        service_name="detector",
    )
    webhook_sink = WebhookAlertSink(settings.alert_webhook_url)

    logger.info("Starting detector service")

    try:
        for message in consumer:
            try:
                event = NormalizedEvent.from_dict(message.value)
                signals = detector.process(event)
                for signal in signals:
                    signal_store.insert_signal(signal)
                    producer.send(
                        settings.kafka_signal_topic,
                        key=signal.instrument_id,
                        value=signal.to_dict(),
                    )
                    logger.info("%s", signal.summary)
                    if webhook_sink.enabled:
                        try:
                            webhook_sink.send(signal)
                        except Exception:
                            logger.exception("Failed to deliver alert webhook")
            except Exception:
                logger.exception("Failed to process market event")
    except KeyboardInterrupt:
        logger.info("Detector service stopped by user")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        signal_store.close()
