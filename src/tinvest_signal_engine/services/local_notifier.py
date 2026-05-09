"""Потребитель топика сигналов на хосте; показ desktop-уведомлений (Windows)."""

from __future__ import annotations

import logging

from kafka import KafkaConsumer

from ..config import RuntimeSettings
from ..desktop_notifications import build_desktop_notifier
from ..kafka_proto import build_signal_value_deserializer
from ..kafka_wire_config import validate_kafka_wire_settings
from ..logging_utils import configure_logging
from ..models import TriggerSignal
from ..signal_enrichment import enrich_signal_for_delivery
from ..signal_locale import signal_type_ru

logger = logging.getLogger(__name__)


def build_consumer(settings: RuntimeSettings) -> KafkaConsumer:
    deserializer = build_signal_value_deserializer(
        format_name=settings.kafka_signal_value_format,
    )
    return KafkaConsumer(
        settings.kafka_signal_topic,
        bootstrap_servers=settings.kafka_host_bootstrap_servers.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=settings.local_notifier_consumer_group,
        value_deserializer=deserializer,
    )


def format_notification(signal: TriggerSignal) -> tuple[str, str]:
    title = f"{signal.ticker} — {signal_type_ru(signal.signal_type)}"
    q = signal.payload.get("quality_score")
    qbit = f" Оценка {q}/100." if q is not None else ""
    message = (
        f"{signal.summary}{qbit}\n"
        f"Серьёзность {signal.severity} | |z|={abs(signal.z_score):.2f}"
    )
    return title[:64], message[:240]


def main() -> None:
    settings = RuntimeSettings.from_env()
    validate_kafka_wire_settings(settings, check_raw=False)
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
                raw = message.value
                if not isinstance(raw, dict):
                    logger.warning("Skipping non-dict signal payload: %r", type(raw))
                    continue
                signal = TriggerSignal.from_dict(raw)
                signal = enrich_signal_for_delivery(signal)
                title, text = format_notification(signal)
                notifier.notify(title, text)
                logger.info("Displayed desktop notification: %s", signal.summary)
            except Exception:
                logger.exception("Failed to display desktop notification")
    except KeyboardInterrupt:
        logger.info("Local desktop notifier stopped by user")
    finally:
        consumer.close()
