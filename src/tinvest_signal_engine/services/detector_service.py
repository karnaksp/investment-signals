from __future__ import annotations

import json
import logging
import time

from kafka import KafkaConsumer, KafkaProducer

from ..config import RuntimeSettings, load_detector_config
from ..detector_core import SignalDetector
from ..logging_utils import configure_logging
from ..models import NormalizedEvent
from ..sinks import (
    TelegramAlertSink,
    WebhookAlertSink,
    create_postgres_signal_store_with_retry,
)

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
    loaded = load_detector_config(
        settings.detector_path, settings.detector_overrides_path
    )
    detector = SignalDetector(loaded.default, loaded.per_instrument)
    detector_mtime = settings.detector_path.stat().st_mtime
    detector_overrides_mtime = (
        settings.detector_overrides_path.stat().st_mtime
        if settings.detector_overrides_path.exists()
        else None
    )
    reload_iv = settings.config_reload_interval_seconds
    last_config_poll = time.monotonic()
    consumer = build_consumer(settings)
    producer = build_signal_producer(settings)
    signal_store = create_postgres_signal_store_with_retry(
        settings,
        service_name="detector",
    )
    webhook_sink = WebhookAlertSink(settings.alert_webhook_url)
    telegram_sink = TelegramAlertSink(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        message_thread_id=settings.telegram_message_thread_id,
    )

    logger.info("Starting detector service")

    try:
        for message in consumer:
            if reload_iv > 0:
                now = time.monotonic()
                if now - last_config_poll >= reload_iv:
                    last_config_poll = now
                    try:
                        mtime = settings.detector_path.stat().st_mtime
                        overrides_mtime = (
                            settings.detector_overrides_path.stat().st_mtime
                            if settings.detector_overrides_path.exists()
                            else None
                        )
                        changed = (
                            mtime != detector_mtime
                            or overrides_mtime
                            != detector_overrides_mtime
                        )
                        if changed:
                            loaded = load_detector_config(
                                settings.detector_path,
                                settings.detector_overrides_path,
                            )
                            detector = SignalDetector(
                                loaded.default,
                                loaded.per_instrument,
                            )
                            detector_mtime = mtime
                            detector_overrides_mtime = overrides_mtime
                            logger.info(
                                "Reloaded detector config from %s (+ %s)",
                                settings.detector_path,
                                settings.detector_overrides_path,
                            )
                    except OSError:
                        logger.exception("Detector config not accessible")
                    except Exception:
                        logger.exception("Failed to reload detector config")
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
                    if telegram_sink.enabled:
                        try:
                            telegram_sink.send(signal)
                        except Exception:
                            logger.exception("Failed to send Telegram alert")
            except Exception:
                logger.exception("Failed to process market event")
    except KeyboardInterrupt:
        logger.info("Detector service stopped by user")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        webhook_sink.close()
        telegram_sink.close()
        signal_store.close()
