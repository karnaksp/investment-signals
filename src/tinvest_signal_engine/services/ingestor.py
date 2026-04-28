from __future__ import annotations

import json
import logging
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

from ..config import RuntimeSettings, load_instrument_configs
from ..instruments import InstrumentMetadata, build_instrument_registry
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
PAYLOAD_FIELDS = ("trade", "last_price", "orderbook", "trading_status", "candle")
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


def build_kafka_producer(settings: RuntimeSettings) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        acks="all",
        linger_ms=50,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
    )


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
            source_time=source_time,
            received_at=utc_now(),
            payload=plain_value,
        )
    return None


def main() -> None:
    settings = RuntimeSettings.from_env()
    configure_logging(settings.log_level)
    if not settings.tinvest_token:
        raise RuntimeError("TINVEST_TOKEN is required")

    kafka_producer = build_kafka_producer(settings)
    target = INVEST_GRPC_API_SANDBOX if settings.tinvest_use_sandbox else None
    reload_iv = settings.config_reload_interval_seconds

    try:
        while True:
            try:
                instrument_configs = load_instrument_configs(
                    settings.instruments_path
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
                                        break
                                except OSError:
                                    logger.exception(
                                        "instruments config not accessible"
                                    )
                        normalized = normalize_stream_message(message, registry)
                        if normalized is None:
                            continue
                        kafka_producer.send(
                            settings.kafka_raw_topic,
                            key=normalized.instrument_id,
                            value=normalized.to_dict(),
                        )
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.exception("Market data stream crashed; reconnecting in 5s")
                time.sleep(5)
    finally:
        kafka_producer.flush()
        kafka_producer.close()


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
