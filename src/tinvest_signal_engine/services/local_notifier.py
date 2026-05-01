"""Потребитель топика сигналов на хосте; показ desktop-уведомлений (Windows)."""

from __future__ import annotations

import json
import logging

from kafka import KafkaConsumer

from ..config import RuntimeSettings
from ..desktop_notifications import build_desktop_notifier
from ..logging_utils import configure_logging
from ..models import TriggerSignal

logger = logging.getLogger(__name__)


def build_consumer(settings: RuntimeSettings) -> KafkaConsumer:
    return KafkaConsumer(
        settings.kafka_signal_topic,
        bootstrap_servers=settings.kafka_host_bootstrap_servers.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=settings.local_notifier_consumer_group,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def format_notification(signal: TriggerSignal) -> tuple[str, str]:
    title = f"{signal.ticker} | {signal.signal_type}"
    message = (
        f"{signal.summary}\n"
        f"Severity={signal.severity} | z={signal.z_score:.2f}"
    )
    return title[:64], message[:240]


def main() -> None:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    consumer = build_consumer(settings)
    notifier = build_desktop_notifier(
        duration_seconds=settings.local_notification_duration_seconds
    )

    logger.info(
        "Starting local desktop notifier on %s",
        settings.kafka_host_bootstrap_servers,
    )

    try:
        for message in consumer:
            try:
                signal = TriggerSignal.from_dict(message.value)
                title, text = format_notification(signal)
                notifier.notify(title, text)
                logger.info("Displayed desktop notification: %s", signal.summary)
            except Exception:
                logger.exception("Failed to display desktop notification")
    except KeyboardInterrupt:
        logger.info("Local desktop notifier stopped by user")
    finally:
        consumer.close()
