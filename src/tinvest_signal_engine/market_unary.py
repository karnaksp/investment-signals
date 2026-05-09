"""Унифицированные unary-вызовы T-Invest (GetMarketValues, GetTechAnalysis) для HTTP API."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.exceptions import RequestError
from tinkoff.invest.schemas import (
    GetMarketValuesRequest,
    GetMarketValuesResponse,
    GetTechAnalysisRequest,
    GetTechAnalysisResponse,
    IndicatorInterval,
    IndicatorType,
    MarketValueType,
    Quotation,
    TypeOfPrice,
)
from tinkoff.invest.utils import quotation_to_decimal

from .config import RuntimeSettings, load_instrument_configs
from .instruments import InstrumentRegistry, build_instrument_registry


@dataclass
class InstrumentRegistryCache:
    mtime: float
    registry: InstrumentRegistry


def _tinvest_target(settings: RuntimeSettings) -> str | None:
    return INVEST_GRPC_API_SANDBOX if settings.tinvest_use_sandbox else None


def resolve_instrument_registry(
    settings: RuntimeSettings,
    cache: InstrumentRegistryCache | None,
) -> tuple[InstrumentRegistry, InstrumentRegistryCache]:
    """Строит реестр инструментов при смене mtime конфига инструментов."""
    try:
        mtime = settings.instruments_path.stat().st_mtime
    except OSError:
        mtime = 0.0
    if cache is not None and cache.mtime == mtime:
        return cache.registry, cache
    configs = load_instrument_configs(settings.instruments_path)
    with Client(
        settings.tinvest_token,
        target=_tinvest_target(settings),
        app_name=settings.tinvest_app_name,
    ) as client:
        registry = build_instrument_registry(client, configs)
    return registry, InstrumentRegistryCache(mtime=mtime, registry=registry)


_MARKET_VALUE_ALIASES: dict[str, MarketValueType] = {
    "last_price": MarketValueType.INSTRUMENT_VALUE_LAST_PRICE,
    "last": MarketValueType.INSTRUMENT_VALUE_LAST_PRICE,
    "dealer": MarketValueType.INSTRUMENT_VALUE_LAST_PRICE_DEALER,
    "close_price": MarketValueType.INSTRUMENT_VALUE_CLOSE_PRICE,
    "close": MarketValueType.INSTRUMENT_VALUE_CLOSE_PRICE,
    "evening": MarketValueType.INSTRUMENT_VALUE_EVENING_SESSION_PRICE,
    "open_interest": MarketValueType.INSTRUMENT_VALUE_OPEN_INTEREST,
    "oi": MarketValueType.INSTRUMENT_VALUE_OPEN_INTEREST,
    "theor": MarketValueType.INSTRUMENT_VALUE_THEOR_PRICE,
    "theor_price": MarketValueType.INSTRUMENT_VALUE_THEOR_PRICE,
}


def parse_market_value_types_csv(csv: str) -> list[MarketValueType]:
    raw = (csv or "").strip().lower()
    if not raw:
        return [
            MarketValueType.INSTRUMENT_VALUE_LAST_PRICE,
            MarketValueType.INSTRUMENT_VALUE_OPEN_INTEREST,
            MarketValueType.INSTRUMENT_VALUE_CLOSE_PRICE,
        ]
    out: list[MarketValueType] = []
    for part in raw.split(","):
        key = part.strip()
        if not key:
            continue
        if key not in _MARKET_VALUE_ALIASES:
            allowed = ", ".join(sorted(_MARKET_VALUE_ALIASES))
            raise ValueError(f"Неизвестный тип значения: {key}. Допустимо: {allowed}")
        v = _MARKET_VALUE_ALIASES[key]
        if v not in out:
            out.append(v)
    if not out:
        raise ValueError("Список типов значений пуст после разбора")
    return out


_INDICATOR_ALIASES: dict[str, IndicatorType] = {
    "bb": IndicatorType.INDICATOR_TYPE_BB,
    "bollinger": IndicatorType.INDICATOR_TYPE_BB,
    "ema": IndicatorType.INDICATOR_TYPE_EMA,
    "rsi": IndicatorType.INDICATOR_TYPE_RSI,
    "macd": IndicatorType.INDICATOR_TYPE_MACD,
    "sma": IndicatorType.INDICATOR_TYPE_SMA,
}


def parse_indicator_type(name: str) -> IndicatorType:
    key = (name or "").strip().lower()
    if key not in _INDICATOR_ALIASES:
        allowed = ", ".join(sorted(_INDICATOR_ALIASES))
        raise ValueError(f"Неизвестный индикатор: {name}. Допустимо: {allowed}")
    return _INDICATOR_ALIASES[key]


_INTERVAL_ALIASES: dict[str, IndicatorInterval] = {
    "1m": IndicatorInterval.INDICATOR_INTERVAL_ONE_MINUTE,
    "5m": IndicatorInterval.INDICATOR_INTERVAL_FIVE_MINUTES,
    "15m": IndicatorInterval.INDICATOR_INTERVAL_FIFTEEN_MINUTES,
    "1h": IndicatorInterval.INDICATOR_INTERVAL_ONE_HOUR,
    "1d": IndicatorInterval.INDICATOR_INTERVAL_ONE_DAY,
    "2m": IndicatorInterval.INDICATOR_INTERVAL_2_MIN,
    "3m": IndicatorInterval.INDICATOR_INTERVAL_3_MIN,
    "10m": IndicatorInterval.INDICATOR_INTERVAL_10_MIN,
    "30m": IndicatorInterval.INDICATOR_INTERVAL_30_MIN,
    "2h": IndicatorInterval.INDICATOR_INTERVAL_2_HOUR,
    "4h": IndicatorInterval.INDICATOR_INTERVAL_4_HOUR,
    "week": IndicatorInterval.INDICATOR_INTERVAL_WEEK,
    "month": IndicatorInterval.INDICATOR_INTERVAL_MONTH,
}


def parse_indicator_interval(name: str) -> IndicatorInterval:
    key = (name or "").strip().lower()
    if key not in _INTERVAL_ALIASES:
        allowed = ", ".join(sorted(_INTERVAL_ALIASES))
        raise ValueError(f"Неизвестный интервал: {name}. Допустимо: {allowed}")
    return _INTERVAL_ALIASES[key]


_PRICE_ALIASES: dict[str, TypeOfPrice] = {
    "close": TypeOfPrice.TYPE_OF_PRICE_CLOSE,
    "open": TypeOfPrice.TYPE_OF_PRICE_OPEN,
    "high": TypeOfPrice.TYPE_OF_PRICE_HIGH,
    "low": TypeOfPrice.TYPE_OF_PRICE_LOW,
    "avg": TypeOfPrice.TYPE_OF_PRICE_AVG,
}


def parse_type_of_price(name: str) -> TypeOfPrice:
    key = (name or "").strip().lower()
    if key not in _PRICE_ALIASES:
        allowed = ", ".join(sorted(_PRICE_ALIASES))
        raise ValueError(f"Неизвестный тип цены: {name}. Допустимо: {allowed}")
    return _PRICE_ALIASES[key]


def json_friendly(obj: Any) -> Any:
    """Сериализация ответов SDK в JSON-совместимые структуры."""
    if obj is None:
        return None
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, Quotation):
        return str(quotation_to_decimal(obj))
    if isinstance(obj, list):
        return [json_friendly(x) for x in obj]
    if isinstance(obj, dict):
        return {k: json_friendly(v) for k, v in obj.items()}
    if hasattr(obj, "__dataclass_fields__"):
        return {
            k: json_friendly(getattr(obj, k))
            for k in obj.__dataclass_fields__  # type: ignore[attr-defined]
        }
    return obj


def fetch_market_values(
    settings: RuntimeSettings,
    *,
    instrument_uid: str,
    value_types: list[MarketValueType],
) -> GetMarketValuesResponse:
    return fetch_market_values_batch(
        settings,
        instrument_uids=[instrument_uid],
        value_types=value_types,
    )


def fetch_market_values_batch(
    settings: RuntimeSettings,
    *,
    instrument_uids: list[str],
    value_types: list[MarketValueType],
) -> GetMarketValuesResponse:
    """Один unary-запрос ``GetMarketValues`` по списку ``instrument_uid``."""
    if not instrument_uids:
        return GetMarketValuesResponse(instruments=[])
    req = GetMarketValuesRequest(
        instrument_id=list(instrument_uids),
        values=value_types,
    )
    with Client(
        settings.tinvest_token,
        target=_tinvest_target(settings),
        app_name=settings.tinvest_app_name,
    ) as client:
        return client.market_data.get_market_values(request=req)


def fetch_tech_analysis(
    settings: RuntimeSettings,
    *,
    instrument_uid: str,
    indicator_type: IndicatorType,
    interval: IndicatorInterval,
    type_of_price: TypeOfPrice,
    length: int,
    from_: datetime,
    to: datetime,
) -> GetTechAnalysisResponse:
    req = GetTechAnalysisRequest(
        indicator_type=indicator_type,
        instrument_uid=instrument_uid,
        from_=from_,
        to=to,
        interval=interval,
        type_of_price=type_of_price,
        length=length,
    )
    with Client(
        settings.tinvest_token,
        target=_tinvest_target(settings),
        app_name=settings.tinvest_app_name,
    ) as client:
        return client.market_data.get_tech_analysis(request=req)


__all__ = (
    "InstrumentRegistryCache",
    "RequestError",
    "fetch_market_values",
    "fetch_market_values_batch",
    "fetch_tech_analysis",
    "json_friendly",
    "parse_indicator_interval",
    "parse_indicator_type",
    "parse_market_value_types_csv",
    "parse_type_of_price",
    "resolve_instrument_registry",
)
