"""Инжестор: подписка на MarketDataStream и публикация в топик сырых событий."""

from __future__ import annotations

import logging
import socket
import threading
import time
from typing import Any
from uuid import uuid4

from kafka import KafkaProducer
from tinkoff.invest import (
    CandleInstrument,
    Client,
    InfoInstrument,
    LastPriceInstrument,
    OrderBookInstrument,
    SubscriptionInterval,
    TradeInstrument,
)
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.exceptions import RequestError

from ..adapters.ingestor_health_file import AtomicJsonIngestorHealthStore
from ..application.ingestor_health import IngestorHealthTracker
from ..config import RuntimeSettings, load_instrument_configs
from ..domain.ingestor_health import (
    INGESTOR_CONFIGURATION_RELOAD,
    INGESTOR_CONNECTING,
    INGESTOR_DNS_RESOLUTION_FAILED,
    INGESTOR_MARKET_STREAM_FAILED,
    INGESTOR_PUBLISH_FAILED,
    INGESTOR_RECONNECTING,
    INGESTOR_TINVEST_REQUEST_FAILED,
)
from ..kafka_proto import build_raw_value_serializer
from ..kafka_wire_config import validate_kafka_wire_settings
from ..schema_registry import register_protobuf_schema, schema_subject_for_topic
from ..instruments import (
    InstrumentMetadata,
    build_instrument_registry,
    request_error_retry_delay_seconds,
)
from ..logging_utils import configure_logging
from ..models import NormalizedEvent
from ..serialization import parse_timestamp, to_plain_data, utc_now

logger = logging.getLogger(__name__)

CONTROL_FIELDS = (
    "subscribe_candles_response",
    "subscribe_order_book_response",
    "subscribe_trades_response",
    "subscribe_info_response",
    "subscribe_last_price_response",
    "ping",
)
PAYLOAD_FIELDS = (
    "trade",
    "last_price",
    "orderbook",
    "trading_status",
    "candle",
    "open_interest",
)
INTERVAL_MAP = {
    "1m": SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
    "5m": SubscriptionInterval.SUBSCRIPTION_INTERVAL_FIVE_MINUTES,
    "15m": getattr(
        SubscriptionInterval,
        "SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTE",
        SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
    ),
    "1h": getattr(
        SubscriptionInterval,
        "SUBSCRIPTION_INTERVAL_ONE_HOUR",
        SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
    ),
    "1d": getattr(
        SubscriptionInterval,
        "SUBSCRIPTION_INTERVAL_ONE_DAY",
        SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
    ),
}


class _KafkaPublishError(RuntimeError):
    pass


def _kafka_compression_type(settings: RuntimeSettings) -> str | None:
    codec = (settings.kafka_compression_codec or "").strip().lower()
    if codec in {"", "none", "off", "plaintext"}:
        return None
    return codec


def build_kafka_producer(settings: RuntimeSettings) -> KafkaProducer:
    compression = _kafka_compression_type(settings)
    sid = settings.kafka_protobuf_schema_id_raw
    register_fn = None
    if (
        settings.kafka_raw_value_format == "protobuf"
        and sid is None
        and settings.schema_registry_url
    ):
        proto_path = settings.proto_dir / "normalized_event.proto"
        subject = schema_subject_for_topic(settings.kafka_raw_topic)
        sr_url = settings.schema_registry_url

        def register_fn() -> int:
            return register_protobuf_schema(sr_url, subject, proto_path)

    value_serializer = build_raw_value_serializer(
        format_name=settings.kafka_raw_value_format,
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


def subscribe_to_stream(stream, instruments) -> None:
    trade_instruments = [
        TradeInstrument(instrument_id=item.instrument_id)
        for item in instruments
        if item.trades
    ]
    if trade_instruments:
        stream.trades.subscribe(trade_instruments)

    last_price_instruments = [
        LastPriceInstrument(instrument_id=item.instrument_id)
        for item in instruments
        if item.last_price
    ]
    if last_price_instruments:
        stream.last_price.subscribe(last_price_instruments)

    info_instruments = [
        InfoInstrument(instrument_id=item.instrument_id)
        for item in instruments
        if item.info
    ]
    if info_instruments:
        stream.info.subscribe(info_instruments)

    order_book_instruments = [
        OrderBookInstrument(
            instrument_id=item.instrument_id,
            depth=int(item.order_book_depth or 0),
        )
        for item in instruments
        if item.order_book_depth
    ]
    if order_book_instruments:
        stream.order_book.subscribe(order_book_instruments)

    candle_instruments = [
        CandleInstrument(
            instrument_id=item.instrument_id,
            interval=INTERVAL_MAP.get(
                item.candle_interval, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE
            ),
        )
        for item in instruments
        if item.candles
    ]
    if candle_instruments:
        stream.candles.waiting_close().subscribe(candle_instruments)


def normalize_stream_message(message, registry) -> NormalizedEvent | None:
    for field_name in CONTROL_FIELDS:
        plain_value = _extract_plain_field(message, field_name)
        if plain_value is not None:
            logger.debug("Skipping control message %s: %s", field_name, plain_value)
            return None

    for field_name in PAYLOAD_FIELDS:
        plain_value = _extract_plain_field(message, field_name)
        if plain_value is None:
            continue

        metadata = _resolve_metadata(registry, plain_value)
        source_time = _extract_source_time(plain_value)
        return NormalizedEvent(
            event_id=str(uuid4()),
            event_type=field_name,
            instrument_id=metadata.instrument_id,
            ticker=metadata.ticker,
            class_code=metadata.class_code,
            alias=metadata.alias,
            figi=metadata.figi,
            uid=metadata.uid,
            lot=int(metadata.lot or 0),
            source_time=source_time,
            received_at=utc_now(),
            payload=plain_value,
        )
    return None


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="ingestor")
    validate_kafka_wire_settings(settings, check_signal=False)
    configure_logging(settings.log_level)
    if not settings.tinvest_token:
        raise RuntimeError("TINVEST_TOKEN is required")

    health = IngestorHealthTracker(
        store=AtomicJsonIngestorHealthStore(
            settings.ingestor_health_snapshot_path
        ),
        clock=utc_now,
        stale_after_seconds=settings.ingestor_health_stale_after_seconds,
    )
    health_watchdog_stop = threading.Event()
    health_watchdog = threading.Thread(
        target=_watch_for_stale_stream,
        kwargs={
            "tracker": health,
            "stop": health_watchdog_stop,
            "interval_seconds": min(
                5.0,
                max(
                    1.0,
                    settings.ingestor_health_stale_after_seconds / 4.0,
                ),
            ),
        },
        name="ingestor-health-watchdog",
        daemon=True,
    )
    health_watchdog.start()
    target = INVEST_GRPC_API_SANDBOX if settings.tinvest_use_sandbox else None
    reload_iv = settings.config_reload_interval_seconds
    reconnecting = False
    kafka_producer: KafkaProducer | None = None

    try:
        kafka_producer = build_kafka_producer(settings)
        while True:
            try:
                instrument_configs = load_instrument_configs(
                    settings.instruments_path
                )
                health.connecting(
                    configured_instruments=len(instrument_configs),
                    reason_code=(
                        INGESTOR_RECONNECTING
                        if reconnecting
                        else INGESTOR_CONNECTING
                    ),
                )
                logger.info(
                    "Starting raw ingestor for %s instruments",
                    len(instrument_configs),
                )
                try:
                    instruments_mtime = settings.instruments_path.stat().st_mtime
                except OSError:
                    instruments_mtime = 0.0
                last_config_poll = time.monotonic()

                with Client(
                    settings.tinvest_token,
                    target=target,
                    app_name=settings.tinvest_app_name,
                ) as client:
                    registry = build_instrument_registry(client, instrument_configs)
                    logger.info(
                        "Resolved instruments: %s",
                        ", ".join(
                            f"{meta.ticker}:{meta.class_code}" for meta in registry
                        ),
                    )
                    market_data_stream = client.create_market_data_stream()
                    subscribe_to_stream(market_data_stream, instrument_configs)
                    last_published_at: dict[tuple[str, str], float] = {}

                    for message in market_data_stream:
                        if reload_iv > 0:
                            now = time.monotonic()
                            if now - last_config_poll >= reload_iv:
                                last_config_poll = now
                                try:
                                    new_mtime = (
                                        settings.instruments_path.stat().st_mtime
                                    )
                                    if new_mtime != instruments_mtime:
                                        logger.info(
                                            "instruments.yaml changed; "
                                            "reconnecting market data stream"
                                        )
                                        health.connecting(
                                            configured_instruments=len(
                                                instrument_configs
                                            ),
                                            reason_code=(
                                                INGESTOR_CONFIGURATION_RELOAD
                                            ),
                                        )
                                        reconnecting = True
                                        break
                                except OSError:
                                    logger.exception(
                                        "instruments config not accessible"
                                    )
                        normalized = normalize_stream_message(message, registry)
                        if normalized is None:
                            continue
                        if not _should_publish(normalized, settings, last_published_at):
                            continue
                        out_val: Any = (
                            normalized
                            if settings.kafka_raw_value_format == "protobuf"
                            else normalized.to_dict()
                        )
                        try:
                            kafka_producer.send(
                                settings.kafka_raw_topic,
                                key=normalized.instrument_id,
                                value=out_val,
                            )
                        except Exception as exc:
                            raise _KafkaPublishError from exc
                        health.publish_succeeded(
                            market_event_at=normalized.source_time,
                        )
                        reconnecting = False
                        logger.info(
                            "raw_kafka_send event_id=%s instrument_id=%s event_type=%s",
                            normalized.event_id,
                            normalized.instrument_id,
                            normalized.event_type,
                        )
                    else:
                        health.failed(
                            reason_code=INGESTOR_MARKET_STREAM_FAILED,
                        )
                        reconnecting = True
            except KeyboardInterrupt:
                raise
            except RequestError as exc:
                health.failed(
                    reason_code=_health_reason_code(exc),
                )
                reconnecting = True
                retry_delay = request_error_retry_delay_seconds(exc)
                logger.exception(
                    "T-Invest request failed; reconnecting in %ss",
                    retry_delay,
                )
                time.sleep(retry_delay)
            except Exception as exc:
                health.failed(reason_code=_health_reason_code(exc))
                reconnecting = True
                logger.exception("Market data stream crashed; reconnecting in 5s")
                time.sleep(5)
    finally:
        health_watchdog_stop.set()
        health_watchdog.join(timeout=10)
        if kafka_producer is not None:
            kafka_producer.flush()
            kafka_producer.close()


def _watch_for_stale_stream(
    *,
    tracker: IngestorHealthTracker,
    stop: threading.Event,
    interval_seconds: float,
) -> None:
    while not stop.wait(interval_seconds):
        try:
            tracker.evaluate_staleness()
        except Exception:
            logger.exception("Failed to persist ingestor health snapshot")


def _health_reason_code(exc: BaseException) -> str:
    if isinstance(exc, _KafkaPublishError):
        return INGESTOR_PUBLISH_FAILED
    if isinstance(exc, RequestError):
        if _exception_chain_contains(
            exc,
            socket.gaierror,
        ) or _request_error_indicates_dns(exc):
            return INGESTOR_DNS_RESOLUTION_FAILED
        return INGESTOR_TINVEST_REQUEST_FAILED
    if _exception_chain_contains(exc, socket.gaierror):
        return INGESTOR_DNS_RESOLUTION_FAILED
    return INGESTOR_MARKET_STREAM_FAILED


def _request_error_indicates_dns(exc: RequestError) -> bool:
    details = str(getattr(exc, "details", "")).casefold()
    return any(
        marker in details
        for marker in (
            "dns resolution failed",
            "name resolution failed",
            "temporary failure in name resolution",
            "nodename nor servname provided",
        )
    )


def _exception_chain_contains(
    exc: BaseException,
    expected_type: type[BaseException],
) -> bool:
    visited: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in visited:
        if isinstance(current, expected_type):
            return True
        visited.add(id(current))
        current = current.__cause__ or current.__context__
    return False


def _extract_plain_field(message, field_name: str) -> dict[str, Any] | None:
    value = getattr(message, field_name, None)
    if value is None:
        return None
    plain_value = to_plain_data(value)
    if not plain_value:
        return None
    if isinstance(plain_value, dict):
        return plain_value
    return {"value": plain_value}


def _should_publish(
    event: NormalizedEvent,
    settings: RuntimeSettings,
    last_published_at: dict[tuple[str, str], float],
) -> bool:
    if event.event_type != "orderbook":
        return True
    interval_ms = settings.ingestor_orderbook_min_interval_ms
    if interval_ms <= 0:
        return True
    key = (event.instrument_id, event.event_type)
    event_time = event.source_time.timestamp()
    previous = last_published_at.get(key)
    if previous is not None and (event_time - previous) * 1000.0 < interval_ms:
        return False
    last_published_at[key] = event_time
    return True


def _resolve_metadata(registry, payload: dict[str, Any]) -> InstrumentMetadata:
    figi = str(payload.get("figi", ""))
    uid = str(payload.get("instrument_uid", ""))
    instrument_id = str(payload.get("instrument_id", ""))
    resolved = registry.resolve(instrument_id=instrument_id, figi=figi, uid=uid)
    if resolved is not None:
        return resolved
    fallback_id = instrument_id or figi or uid or "unknown"
    return InstrumentMetadata(
        instrument_id=fallback_id,
        ticker=fallback_id,
        class_code="",
        alias=fallback_id.lower(),
        figi=figi,
        uid=uid,
        lot=0,
        currency="",
        name=fallback_id,
    )


def _extract_source_time(payload: dict[str, Any]):
    for key in ("time", "last_trade_ts"):
        value = payload.get(key)
        if not value:
            continue
        return parse_timestamp(value)
    return utc_now()
