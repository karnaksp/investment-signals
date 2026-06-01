"""Сервис детектора: чтение raw-топика, запись сигналов в Postgres и Kafka."""

from __future__ import annotations

import logging
import time
from typing import Any

from kafka import KafkaConsumer, KafkaProducer

from ..config import RuntimeSettings, load_detector_config
from ..data_quality import log_validation_failure, validate_normalized_event_dict
from ..delivery_policy import DELIVERY_DELIVERED, DeliveryPolicy
from ..detector_core import SignalDetector
from ..kafka_proto import (
    build_raw_value_deserializer,
    build_signal_value_serializer,
)
from ..kafka_wire_config import validate_kafka_wire_settings
from ..logging_utils import configure_logging
from ..metrics import (
    observe_message,
    observe_signals,
    start_metrics_server,
    timed_process_block,
)
from ..models import NormalizedEvent, TriggerSignal
from ..redis_detector_state import flush_detector_to_redis, hydrate_detector_from_redis
from ..signal_enrichment import enrich_signal_for_delivery
from ..schema_registry import register_protobuf_schema, schema_subject_for_topic
from ..sinks import (
    TelegramAlertSink,
    WebhookAlertSink,
    create_postgres_signal_store_with_retry,
)

logger = logging.getLogger(__name__)


def _kafka_compression_type(settings: RuntimeSettings) -> str | None:
    codec = (settings.kafka_compression_codec or "").strip().lower()
    if codec in {"", "none", "off", "plaintext"}:
        return None
    return codec


def build_consumer(settings: RuntimeSettings) -> KafkaConsumer:
    deserializer = build_raw_value_deserializer(
        format_name=settings.kafka_raw_value_format,
    )
    return KafkaConsumer(
        settings.kafka_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        auto_offset_reset=settings.kafka_auto_offset_reset,
        enable_auto_commit=True,
        group_id=settings.kafka_consumer_group,
        value_deserializer=deserializer,
    )


def build_signal_producer(settings: RuntimeSettings) -> KafkaProducer:
    compression = _kafka_compression_type(settings)
    sid = settings.kafka_protobuf_schema_id_signal
    register_fn = None
    if (
        settings.kafka_signal_value_format == "protobuf"
        and sid is None
        and settings.schema_registry_url
    ):
        proto_path = settings.proto_dir / "trigger_signal.proto"
        subject = schema_subject_for_topic(settings.kafka_signal_topic)
        sr_url = settings.schema_registry_url

        def register_fn() -> int:
            return register_protobuf_schema(sr_url, subject, proto_path)

    value_serializer = build_signal_value_serializer(
        format_name=settings.kafka_signal_value_format,
        schema_id=sid,
        register_schema=register_fn,
    )
    producer_kwargs: dict[str, Any] = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        "acks": "all",
        "linger_ms": settings.kafka_linger_ms,
        "batch_size": settings.kafka_batch_bytes,
        "key_serializer": lambda value: value.encode("utf-8"),
        "value_serializer": value_serializer,
    }
    if compression is not None:
        producer_kwargs["compression_type"] = compression
    return KafkaProducer(**producer_kwargs)


def main() -> None:
    settings = RuntimeSettings.from_env()
    validate_kafka_wire_settings(settings)
    configure_logging(settings.log_level)
    if settings.metrics_listen_port:
        start_metrics_server(settings.metrics_listen_port)
    loaded = load_detector_config(
        settings.detector_path, settings.detector_overrides_path
    )
    detector = SignalDetector(
        loaded.default,
        loaded.per_instrument,
        lead_lag_pairs=loaded.lead_lag_pairs,
    )
    hydrate_detector_from_redis(detector, settings.redis_url)
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
    delivery_policy = DeliveryPolicy(
        settings,
        delivered_count_since=lambda since, instrument_id, signal_type: (
            signal_store.count_delivered_since(
                since=since,
                instrument_id=instrument_id,
                signal_type=signal_type,
            )
        ),
    )

    logger.info("Starting detector service")
    if settings.redis_url:
        try:
            import redis

            redis.Redis.from_url(settings.redis_url, decode_responses=True).ping()
            logger.info("Redis ping OK (%s)", settings.redis_url)
        except Exception:
            logger.exception("Redis unavailable at REDIS_URL (detector continues)")

    redis_flush_iv = max(0, settings.redis_alert_flush_interval_seconds)
    last_redis_flush = time.monotonic()

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
                            or overrides_mtime != detector_overrides_mtime
                        )
                        if changed:
                            flush_detector_to_redis(detector, settings.redis_url)
                            loaded = load_detector_config(
                                settings.detector_path,
                                settings.detector_overrides_path,
                            )
                            detector = SignalDetector(
                                loaded.default,
                                loaded.per_instrument,
                                lead_lag_pairs=loaded.lead_lag_pairs,
                            )
                            hydrate_detector_from_redis(detector, settings.redis_url)
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
            raw_value = message.value
            if not isinstance(raw_value, dict):
                observe_message(event_type="unknown", outcome="invalid_shape")
                logger.warning("Skipping non-dict Kafka payload: %r", type(raw_value))
                continue
            issues = validate_normalized_event_dict(raw_value)
            if issues:
                log_validation_failure(errors=issues, sample=raw_value)
                observe_message(
                    event_type=str(raw_value.get("event_type", "unknown")),
                    outcome="invalid_payload",
                )
                continue
            try:
                with timed_process_block():
                    event = NormalizedEvent.from_dict(raw_value)
                    signals = detector.process(event)
                    signals = detector.enrich_signals_with_unary(signals)
                    stored: list[TriggerSignal] = []
                    outbound: list[TriggerSignal] = []
                    for signal in signals:
                        signal = enrich_signal_for_delivery(signal)
                        signal = delivery_policy.apply(signal)
                        stored.append(signal)
                        if signal.payload.get("delivery_status") == DELIVERY_DELIVERED:
                            outbound.append(signal)
                    for signal in stored:
                        signal_store.insert_signal(signal)
                        out_val: Any = (
                            signal
                            if settings.kafka_signal_value_format == "protobuf"
                            else signal.to_dict()
                        )
                        producer.send(
                            settings.kafka_signal_topic,
                            key=signal.instrument_id,
                            value=out_val,
                        )
                        logger.info("%s", signal.summary)
                    for signal in outbound:
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
                    observe_message(event_type=event.event_type, outcome="ok")
                    if stored:
                        observe_signals(
                            [s.signal_type for s in stored]
                        )
                    if redis_flush_iv > 0 and settings.redis_url:
                        now_mono = time.monotonic()
                        if now_mono - last_redis_flush >= redis_flush_iv:
                            flush_detector_to_redis(detector, settings.redis_url)
                            last_redis_flush = now_mono
            except Exception:
                observe_message(
                    event_type=str(raw_value.get("event_type", "unknown")),
                    outcome="error",
                )
                logger.exception("Failed to process market event")
    except KeyboardInterrupt:
        logger.info("Detector service stopped by user")
    finally:
        flush_detector_to_redis(detector, settings.redis_url)
        producer.flush()
        producer.close()
        consumer.close()
        webhook_sink.close()
        telegram_sink.close()
        signal_store.close()
