"""Разбор параметров unary market API (без вызова T-Invest)."""

from tinkoff.invest.schemas import (
    IndicatorInterval,
    IndicatorType,
    MarketValueType,
    TypeOfPrice,
)

from tinvest_signal_engine.market_unary import (
    parse_indicator_interval,
    parse_indicator_type,
    parse_market_value_types_csv,
    parse_type_of_price,
)


def test_parse_market_value_types_default() -> None:
    out = parse_market_value_types_csv("")
    assert MarketValueType.INSTRUMENT_VALUE_LAST_PRICE in out
    assert MarketValueType.INSTRUMENT_VALUE_OPEN_INTEREST in out


def test_parse_market_value_types_aliases() -> None:
    out = parse_market_value_types_csv("last, oi")
    assert out == [
        MarketValueType.INSTRUMENT_VALUE_LAST_PRICE,
        MarketValueType.INSTRUMENT_VALUE_OPEN_INTEREST,
    ]


def test_parse_indicator_and_interval() -> None:
    assert parse_indicator_type("RSI") == IndicatorType.INDICATOR_TYPE_RSI
    assert parse_indicator_interval("1h") == IndicatorInterval.INDICATOR_INTERVAL_ONE_HOUR


def test_parse_type_of_price() -> None:
    assert parse_type_of_price("CLOSE") == TypeOfPrice.TYPE_OF_PRICE_CLOSE
