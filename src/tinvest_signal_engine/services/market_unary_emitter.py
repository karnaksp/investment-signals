"""Периодическая публика unary GetMarketValues / GetTechAnalysis в Kafka raw-топик."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.schemas import GetTechAnalysisRequest, MarketValueType

from ..config import RuntimeSettings
from ..instruments import InstrumentMetadata
from ..kafka_wire_config import validate_kafka_wire_settings
from ..logging_utils import configure_logging
from ..market_unary import (
    InstrumentRegistryCache,
    RequestError,
    fetch_market_values_batch,
    json_friendly,
    parse_indicator_interval,
    parse_indicator_type,
    parse_market_value_types_csv,
    parse_type_of_price,
    resolve_instrument_registry,
)
from ..metrics import (
    observe_unary_cycle_completed,
    observe_unary_error,
    observe_unary_publish,
    start_metrics_server,
    unary_cycle_timer,
)
from ..models import NormalizedEvent
from ..serialization import utc_now
from .ingestor import build_kafka_producer

logger = logging.getLogger(__name__)

_ALLOWED_MODES = frozenset({"market_values", "tech_analysis"})


def parse_market_unary_modes_csv(csv: str) -> frozenset[str]:
    out: set[str] = set()
    for part in (csv or "").split(","):
        p = part.strip().lower()
        if not p:
            continue
        if p in _ALLOWED_MODES:
            out.add(p)
        else:
            logger.warning("MARKET_UNARY_MODES: неизвестный режим %r (пропуск)", p)
    return frozenset(out)


def _tinvest_target(settings: RuntimeSettings) -> str | None:
    return INVEST_GRPC_API_SANDBOX if settings.tinvest_use_sandbox else None


def _kafka_raw_sink_topic(settings: RuntimeSettings) -> str:
    return settings.kafka_raw_unary_topic or settings.kafka_raw_topic


def _source_time_from_market_values(values: object) -> datetime:
    latest: datetime | None = None
    if not isinstance(values, list):
        return utc_now()
    for v in values:
        t = getattr(v, "time", None)
        if t is None:
            continue
        if latest is None or t > latest:
            latest = t
    if latest is None:
        return utc_now()
    if latest.tzinfo is None:
        return latest.replace(tzinfo=timezone.utc)
    return latest


def _source_time_from_tech_analysis(ta: object) -> datetime:
    now = utc_now()
    items = getattr(ta, "technical_indicators", None) or []
    latest: datetime | None = None
    for item in items:
        ts = getattr(item, "timestamp", None)
        if ts is None:
            continue
        if latest is None or ts > latest:
            latest = ts
    if latest is None:
        return now
    if latest.tzinfo is None:
        return latest.replace(tzinfo=timezone.utc)
    return latest


def _emit_market_values(
    settings,
    producer,
    metas: list[InstrumentMetadata],
    value_types: list[MarketValueType],
    batch_id: str,
    raw_topic: str,
) -> tuple[int, bool]:
    uids = [m.uid for m in metas if m.uid]
    if not uids:
        return 0, False
    try:
        mv = fetch_market_values_batch(
            settings, instrument_uids=uids, value_types=value_types
        )
    except RequestError:
        observe_unary_error(phase="get_market_values")
        logger.exception(
            "GetMarketValues batch failed poll_batch_id=%s", batch_id
        )
        return 0, True
    by_uid = {inst.instrument_uid: inst for inst in mv.instruments}
    sent = 0
    for meta in metas:
        if not meta.uid:
            continue
        inst = by_uid.get(meta.uid)
        if inst is None:
            logger.warning(
                "GetMarketValues: нет строки для uid=%s (%s) poll_batch_id=%s",
                meta.uid,
                meta.instrument_id,
                batch_id,
            )
            continue
        source_time = _source_time_from_market_values(inst.values)
        payload = {
            "poll_batch_id": batch_id,
            "source": "get_market_values",
            "values": json_friendly(inst.values),
        }
        ev = NormalizedEvent(
            event_id=str(uuid4()),
            event_type="market_values",
            instrument_id=meta.instrument_id,
            ticker=meta.ticker,
            class_code=meta.class_code,
            alias=meta.alias,
            figi=meta.figi,
            uid=meta.uid,
            lot=int(meta.lot or 0),
            source_time=source_time,
            received_at=utc_now(),
            payload=payload,
        )
        out_val = (
            ev
            if settings.kafka_raw_value_format == "protobuf"
            else ev.to_dict()
        )
        producer.send(raw_topic, key=meta.instrument_id, value=out_val)
        sent += 1
        logger.info(
            "unary_kafka_send poll_batch_id=%s instrument_id=%s event_type=market_values topic=%s",
            batch_id,
            meta.instrument_id,
            raw_topic,
        )
    if sent:
        observe_unary_publish(event_type="market_values", count=sent)
    return sent, False


def _emit_tech_analysis(
    settings,
    producer,
    metas: list[InstrumentMetadata],
    batch_id: str,
    raw_topic: str,
) -> tuple[int, bool]:
    now = datetime.now(timezone.utc)
    from_ts = now - timedelta(minutes=settings.market_unary_tech_window_minutes)
    ind = parse_indicator_type(settings.market_unary_tech_indicator)
    ival = parse_indicator_interval(settings.market_unary_tech_interval)
    top = parse_type_of_price(settings.market_unary_tech_type_of_price)
    length = settings.market_unary_tech_length
    sleep_s = max(0, settings.market_unary_tech_sleep_ms) / 1000.0
    target = _tinvest_target(settings)
    sent = 0
    any_api_error = False
    with Client(
        settings.tinvest_token,
        target=target,
        app_name=settings.tinvest_app_name,
    ) as client:
        for meta in metas:
            if not meta.uid:
                continue
            req = GetTechAnalysisRequest(
                indicator_type=ind,
                instrument_uid=meta.uid,
                from_=from_ts,
                to=now,
                interval=ival,
                type_of_price=top,
                length=length,
            )
            try:
                ta = client.market_data.get_tech_analysis(request=req)
            except RequestError:
                any_api_error = True
                observe_unary_error(phase="get_tech_analysis")
                logger.exception(
                    "GetTechAnalysis failed instrument_id=%s poll_batch_id=%s",
                    meta.instrument_id,
                    batch_id,
                )
                if sleep_s:
                    time.sleep(sleep_s)
                continue
            source_time = _source_time_from_tech_analysis(ta)
            payload = {
                "poll_batch_id": batch_id,
                "source": "get_tech_analysis",
                "indicator": ind.name,
                "interval": ival.name,
                "type_of_price": top.name,
                "length": length,
                "from": from_ts.isoformat(),
                "to": now.isoformat(),
                "response": json_friendly(ta),
            }
            ev = NormalizedEvent(
                event_id=str(uuid4()),
                event_type="tech_analysis",
                instrument_id=meta.instrument_id,
                ticker=meta.ticker,
                class_code=meta.class_code,
                alias=meta.alias,
                figi=meta.figi,
                uid=meta.uid,
                lot=int(meta.lot or 0),
                source_time=source_time,
                received_at=utc_now(),
                payload=payload,
            )
            out_val = (
                ev
                if settings.kafka_raw_value_format == "protobuf"
                else ev.to_dict()
            )
            producer.send(raw_topic, key=meta.instrument_id, value=out_val)
            sent += 1
            logger.info(
                "unary_kafka_send poll_batch_id=%s instrument_id=%s event_type=tech_analysis topic=%s",
                batch_id,
                meta.instrument_id,
                raw_topic,
            )
            if sleep_s:
                time.sleep(sleep_s)
    if sent:
        observe_unary_publish(event_type="tech_analysis", count=sent)
    return sent, any_api_error


def main() -> None:
    settings = RuntimeSettings.from_env(service_name="market_unary_emitter")
    configure_logging(settings.log_level)
    if settings.market_unary_poll_seconds <= 0:
        raise RuntimeError(
            "MARKET_UNARY_POLL_SECONDS должен быть > 0 "
            "(сервис только для периодического unary-эмита в Kafka)."
        )
    if not (settings.tinvest_token or "").strip():
        raise RuntimeError("TINVEST_TOKEN обязателен")
    modes = parse_market_unary_modes_csv(settings.market_unary_modes_csv)
    if not modes:
        raise RuntimeError(
            "После разбора MARKET_UNARY_MODES не осталось ни одного "
            f"из допустимых: {', '.join(sorted(_ALLOWED_MODES))}"
        )
    if "tech_analysis" in modes:
        parse_indicator_type(settings.market_unary_tech_indicator)
        parse_indicator_interval(settings.market_unary_tech_interval)
        parse_type_of_price(settings.market_unary_tech_type_of_price)
    try:
        value_types = parse_market_value_types_csv(
            settings.market_unary_market_value_types_csv
        )
    except ValueError as exc:
        raise RuntimeError(str(exc)) from exc

    validate_kafka_wire_settings(settings, check_signal=False)
    producer = build_kafka_producer(settings)
    registry_cache: InstrumentRegistryCache | None = None
    base_interval = max(5, settings.market_unary_poll_seconds)
    max_backoff = max(base_interval, settings.market_unary_max_backoff_seconds)
    next_sleep = base_interval
    raw_topic = _kafka_raw_sink_topic(settings)

    if settings.market_unary_metrics_listen_port:
        start_metrics_server(settings.market_unary_metrics_listen_port)

    logger.info(
        "Unary emitter: base_interval=%ss modes=%s kafka_topic=%s (raw unary override=%s)",
        base_interval,
        ",".join(sorted(modes)),
        raw_topic,
        settings.kafka_raw_unary_topic or "(same as raw)",
    )

    try:
        while True:
            had_error = False
            batch_id = ""
            try:
                with unary_cycle_timer():
                    registry, registry_cache = resolve_instrument_registry(
                        settings, registry_cache
                    )
                    metas = list(registry)
                    batch_id = str(uuid4())
                    if "market_values" in modes:
                        n_mv, err_mv = _emit_market_values(
                            settings,
                            producer,
                            metas,
                            value_types,
                            batch_id,
                            raw_topic,
                        )
                        had_error = had_error or err_mv
                        logger.info(
                            "Published %s market_values events poll_batch_id=%s err=%s",
                            n_mv,
                            batch_id,
                            err_mv,
                        )
                    if "tech_analysis" in modes:
                        n_ta, err_ta = _emit_tech_analysis(
                            settings,
                            producer,
                            metas,
                            batch_id,
                            raw_topic,
                        )
                        had_error = had_error or err_ta
                        logger.info(
                            "Published %s tech_analysis events poll_batch_id=%s err=%s",
                            n_ta,
                            batch_id,
                            err_ta,
                        )
            except Exception:
                had_error = True
                observe_unary_error(phase="cycle")
                logger.exception(
                    "Unary emit cycle failed poll_batch_id=%s", batch_id
                )
            observe_unary_cycle_completed()
            producer.flush()
            if had_error:
                next_sleep = min(
                    max_backoff,
                    max(base_interval, int(next_sleep * 1.5)),
                )
                logger.warning(
                    "Unary backoff: next_sleep_seconds=%s (base=%s max=%s)",
                    next_sleep,
                    base_interval,
                    max_backoff,
                )
            else:
                next_sleep = base_interval
            if settings.market_unary_single_shot:
                logger.info(
                    "MARKET_UNARY_SINGLE_SHOT=1: завершение после одного цикла"
                )
                break
            time.sleep(next_sleep)
    finally:
        producer.flush()
        producer.close()
