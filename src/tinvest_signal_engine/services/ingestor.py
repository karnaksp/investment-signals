"""Инжестор: подписка на MarketDataStream и публикация в топик сырых событий."""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable
from uuid import NAMESPACE_URL, uuid4, uuid5

from kafka import KafkaProducer
from tinkoff.invest import (
    CandleInterval,
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
    INGESTOR_SCHEDULED_SLEEP,
    INGESTOR_STREAM_STALE,
    INGESTOR_TINVEST_REQUEST_FAILED,
)
from ..domain.market_schedule import MarketSchedule
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


class _StreamSessionGuard:
    """Stop a broker iterator that is silent or crossed the collection deadline."""

    def __init__(
        self,
        *,
        stream: Any,
        tracker: IngestorHealthTracker,
        schedule: MarketSchedule,
        stale_after_seconds: int,
        clock: Callable[[], Any] = utc_now,
    ) -> None:
        self._stream = stream
        self._tracker = tracker
        self._schedule = schedule
        self._stale_after_seconds = stale_after_seconds
        self._clock = clock
        self._last_event_at = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._watch,
            name="ingestor-stream-session-guard",
            daemon=True,
        )
        self.reason_code: str | None = None

    def start(self) -> None:
        self._thread.start()

    def observe_market_event(self) -> None:
        self._last_event_at = time.monotonic()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _watch(self) -> None:
        while not self._stop.wait(1.0):
            now = self._clock()
            if not self._schedule.is_collection_active(now):
                self.reason_code = INGESTOR_SCHEDULED_SLEEP
                logger.info("Collection window ended; stopping market stream")
                self._stream.stop()
                return
            if time.monotonic() - self._last_event_at <= self._stale_after_seconds:
                continue
            self.reason_code = INGESTOR_STREAM_STALE
            self._tracker.failed(reason_code=INGESTOR_STREAM_STALE)
            logger.error(
                "No market payload for %ss; forcing broker stream reconnect",
                self._stale_after_seconds,
            )
            self._stream.stop()
            return


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


def _recovery_trade_event(trade: Any, metadata: InstrumentMetadata) -> NormalizedEvent:
    plain = to_plain_data(trade)
    if not isinstance(plain, dict):
        raise ValueError("historical trade must map to an object")
    source_time = _extract_source_time(plain)
    identity = "|".join(
        (
            metadata.instrument_id,
            source_time.isoformat(),
            repr(plain.get("price")),
            str(plain.get("quantity")),
            str(plain.get("direction")),
        )
    )
    return NormalizedEvent(
        event_id=str(uuid5(NAMESPACE_URL, f"tinvest-recovery-trade:{identity}")),
        event_type="trade",
        instrument_id=metadata.instrument_id,
        ticker=metadata.ticker,
        class_code=metadata.class_code,
        alias=metadata.alias,
        figi=metadata.figi,
        uid=metadata.uid,
        lot=int(metadata.lot or 0),
        source_time=source_time,
        received_at=utc_now(),
        payload={**plain, "recovery_backfill": True},
    )


def _recovery_candle_event(
    candle: Any,
    metadata: InstrumentMetadata,
) -> NormalizedEvent:
    plain = to_plain_data(candle)
    if not isinstance(plain, dict):
        raise ValueError("historical candle must map to an object")
    source_time = _extract_source_time(plain)
    return NormalizedEvent(
        event_id=str(
            uuid5(
                NAMESPACE_URL,
                (
                    "tinvest-recovery-candle:"
                    f"{metadata.instrument_id}|{source_time.isoformat()}|1m"
                ),
            )
        ),
        event_type="candle",
        instrument_id=metadata.instrument_id,
        ticker=metadata.ticker,
        class_code=metadata.class_code,
        alias=metadata.alias,
        figi=metadata.figi,
        uid=metadata.uid,
        lot=int(metadata.lot or 0),
        source_time=source_time,
        received_at=utc_now(),
        payload={**plain, "recovery_backfill": True},
    )


def _recover_session_candles(
    *,
    client: Any,
    producer: KafkaProducer,
    settings: RuntimeSettings,
    schedule: MarketSchedule,
    registry: Any,
    instrument_configs: Any,
    lookback_minutes: int | None = None,
) -> int:
    effective_lookback = (
        settings.ingestor_recovery_lookback_minutes
        if lookback_minutes is None
        else max(0, lookback_minutes)
    )
    if effective_lookback <= 0:
        return 0
    now = utc_now()
    local_now = now.astimezone(schedule.timezone)
    collection_start = datetime.combine(
        local_now.date(),
        schedule.collection_start,
        tzinfo=schedule.timezone,
    )
    recovery_start = max(
        collection_start,
        local_now
        - timedelta(minutes=effective_lookback),
    )
    candle_enabled = {
        (item.ticker, item.class_code)
        for item in instrument_configs
        if item.candles
    }
    recovered = 0
    for metadata in registry:
        if (metadata.ticker, metadata.class_code) not in candle_enabled:
            continue
        try:
            response = client.market_data.get_candles(
                instrument_id=metadata.uid or metadata.figi,
                from_=recovery_start,
                to=local_now,
                interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
                limit=2500,
            )
        except RequestError:
            logger.exception(
                "Could not recover minute candles for %s",
                metadata.ticker,
            )
            continue
        for candle in response.candles:
            event = _recovery_candle_event(candle, metadata)
            value: Any = (
                event
                if settings.kafka_raw_value_format == "protobuf"
                else event.to_dict()
            )
            producer.send(
                settings.kafka_raw_topic,
                key=event.instrument_id,
                value=value,
            )
            recovered += 1
    if recovered:
        producer.flush()
        logger.info(
            "Recovered %s minute candles from %s to %s",
            recovered,
            recovery_start.isoformat(),
            local_now.isoformat(),
        )
    return recovered


def _candle_recovery_due(
    *,
    last_recovery_at: float | None,
    monotonic_now: float,
    interval_seconds: float,
) -> bool:
    return interval_seconds > 0 and (
        last_recovery_at is None
        or monotonic_now - last_recovery_at >= interval_seconds
    )


def _recover_missing_trades(
    *,
    client: Any,
    producer: KafkaProducer,
    tracker: IngestorHealthTracker,
    settings: RuntimeSettings,
    schedule: MarketSchedule,
    registry: Any,
    instrument_configs: Any,
    previous_market_event_at: datetime | None,
) -> int:
    if settings.ingestor_recovery_lookback_minutes <= 0:
        return 0
    now = utc_now()
    local_now = now.astimezone(schedule.timezone)
    collection_start = datetime.combine(
        local_now.date(),
        schedule.collection_start,
        tzinfo=schedule.timezone,
    )
    recovery_start = max(
        collection_start,
        now - timedelta(minutes=settings.ingestor_recovery_lookback_minutes),
    )
    if previous_market_event_at is not None:
        recovery_start = max(
            recovery_start,
            previous_market_event_at.astimezone(schedule.timezone)
            + timedelta(microseconds=1),
        )
    if recovery_start >= local_now - timedelta(seconds=2):
        return 0

    trade_enabled = {
        (item.ticker, item.class_code)
        for item in instrument_configs
        if item.trades
    }
    recovered = 0
    latest_recovered_at: datetime | None = None
    seen: set[str] = set()
    for metadata in registry:
        if (metadata.ticker, metadata.class_code) not in trade_enabled:
            continue
        chunk_start = recovery_start
        while chunk_start < local_now:
            chunk_end = min(chunk_start + timedelta(minutes=55), local_now)
            try:
                response = client.market_data.get_last_trades(
                    instrument_id=metadata.uid or metadata.figi,
                    from_=chunk_start,
                    to=chunk_end,
                )
            except RequestError:
                logger.exception(
                    "Could not recover trades for %s between %s and %s",
                    metadata.ticker,
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                )
                break
            for trade in response.trades:
                event = _recovery_trade_event(trade, metadata)
                if not recovery_start <= event.source_time.astimezone(
                    schedule.timezone
                ) <= local_now:
                    continue
                if event.event_id in seen:
                    continue
                seen.add(event.event_id)
                value: Any = (
                    event
                    if settings.kafka_raw_value_format == "protobuf"
                    else event.to_dict()
                )
                producer.send(
                    settings.kafka_raw_topic,
                    key=event.instrument_id,
                    value=value,
                )
                recovered += 1
                if (
                    latest_recovered_at is None
                    or event.source_time > latest_recovered_at
                ):
                    latest_recovered_at = event.source_time
            chunk_start = chunk_end + timedelta(microseconds=1)
    if recovered:
        producer.flush()
        if latest_recovered_at is not None:
            tracker.market_event_observed(
                market_event_at=latest_recovered_at,
            )
        logger.info(
            "Recovered %s missed trades from %s to %s",
            recovered,
            recovery_start.isoformat(),
            local_now.isoformat(),
        )
    return recovered


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="ingestor")
    validate_kafka_wire_settings(settings, check_signal=False)
    configure_logging(settings.log_level)
    if not settings.tinvest_token:
        raise RuntimeError("TINVEST_TOKEN is required")

    health_store = AtomicJsonIngestorHealthStore(
        settings.ingestor_health_snapshot_path
    )
    try:
        initial_health = health_store.load()
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        logger.exception("Ignoring invalid previous ingestor health snapshot")
        initial_health = None
    health = IngestorHealthTracker(
        store=health_store,
        clock=utc_now,
        stale_after_seconds=settings.ingestor_health_stale_after_seconds,
        initial_snapshot=initial_health,
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
    recovered_candle_day = None
    last_candle_recovery_at: float | None = None
    schedule = MarketSchedule.from_strings(
        timezone_name=settings.market_schedule_timezone,
        collection_start=settings.market_collection_start,
        collection_end=settings.market_collection_end,
        signal_start=settings.market_signal_start,
        signal_end=settings.market_signal_end,
    )

    try:
        kafka_producer = build_kafka_producer(settings)
        while True:
            try:
                instrument_configs = load_instrument_configs(
                    settings.instruments_path
                )
                previous_market_event_at = health.snapshot.last_market_event_at
                if not schedule.is_collection_active(utc_now()):
                    health.sleeping(
                        configured_instruments=len(instrument_configs),
                    )
                    wait_seconds = min(
                        60.0,
                        max(
                            1.0,
                            schedule.seconds_until_collection_start(utc_now()),
                        ),
                    )
                    logger.info(
                        "Market collection sleeps until %s; checking again in %.0fs",
                        schedule.next_collection_start(utc_now()).isoformat(),
                        wait_seconds,
                    )
                    reconnecting = False
                    time.sleep(wait_seconds)
                    continue
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
                    local_day = utc_now().astimezone(schedule.timezone).date()
                    if recovered_candle_day != local_day:
                        _recover_session_candles(
                            client=client,
                            producer=kafka_producer,
                            settings=settings,
                            schedule=schedule,
                            registry=registry,
                            instrument_configs=instrument_configs,
                        )
                        recovered_candle_day = local_day
                        last_candle_recovery_at = time.monotonic()
                    _recover_missing_trades(
                        client=client,
                        producer=kafka_producer,
                        tracker=health,
                        settings=settings,
                        schedule=schedule,
                        registry=registry,
                        instrument_configs=instrument_configs,
                        previous_market_event_at=previous_market_event_at,
                    )
                    market_data_stream = client.create_market_data_stream()
                    subscribe_to_stream(market_data_stream, instrument_configs)
                    last_published_at: dict[tuple[str, str], float] = {}
                    session_guard = _StreamSessionGuard(
                        stream=market_data_stream,
                        tracker=health,
                        schedule=schedule,
                        stale_after_seconds=(
                            settings.ingestor_health_stale_after_seconds
                        ),
                    )
                    session_guard.start()
                    try:
                        for message in market_data_stream:
                            monotonic_now = time.monotonic()
                            recovery_interval = (
                                settings.ingestor_candle_recovery_interval_seconds
                            )
                            if _candle_recovery_due(
                                last_recovery_at=last_candle_recovery_at,
                                monotonic_now=monotonic_now,
                                interval_seconds=recovery_interval,
                            ):
                                _recover_session_candles(
                                    client=client,
                                    producer=kafka_producer,
                                    settings=settings,
                                    schedule=schedule,
                                    registry=registry,
                                    instrument_configs=instrument_configs,
                                    lookback_minutes=(
                                        settings.ingestor_candle_recovery_overlap_minutes
                                    ),
                                )
                                last_candle_recovery_at = time.monotonic()
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
                            session_guard.observe_market_event()
                            if not _should_publish(
                                normalized,
                                settings,
                                last_published_at,
                            ):
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
                            if session_guard.reason_code == INGESTOR_SCHEDULED_SLEEP:
                                health.sleeping(
                                    configured_instruments=len(instrument_configs),
                                )
                                reconnecting = False
                            elif session_guard.reason_code == INGESTOR_STREAM_STALE:
                                reconnecting = True
                            else:
                                health.failed(
                                    reason_code=INGESTOR_MARKET_STREAM_FAILED,
                                )
                                reconnecting = True
                    finally:
                        session_guard.close()
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
