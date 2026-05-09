"""Параметры сервиса unary → Kafka."""

from tinvest_signal_engine.data_quality import validate_normalized_event_dict
from tinvest_signal_engine.services.market_unary_emitter import parse_market_unary_modes_csv


def test_parse_market_unary_modes_csv() -> None:
    assert parse_market_unary_modes_csv("market_values") == frozenset({"market_values"})
    assert parse_market_unary_modes_csv(
        "market_values, tech_analysis, bogus"
    ) == frozenset({"market_values", "tech_analysis"})


def test_validate_market_values_poll_event() -> None:
    sample = {
        "event_id": "e1",
        "event_type": "market_values",
        "instrument_id": "X_TQBR",
        "ticker": "X",
        "class_code": "TQBR",
        "alias": "x",
        "lot": 1,
        "source_time": "2026-01-01T12:00:00+00:00",
        "received_at": "2026-01-01T12:00:01+00:00",
        "payload": {
            "poll_batch_id": "b",
            "source": "get_market_values",
            "values": [],
        },
    }
    assert validate_normalized_event_dict(sample) == []


def test_validate_tech_analysis_poll_event() -> None:
    sample = {
        "event_id": "e2",
        "event_type": "tech_analysis",
        "instrument_id": "X_TQBR",
        "ticker": "X",
        "class_code": "TQBR",
        "alias": "x",
        "lot": 1,
        "source_time": "2026-01-01T12:00:00+00:00",
        "received_at": "2026-01-01T12:00:01+00:00",
        "payload": {
            "poll_batch_id": "b",
            "source": "get_tech_analysis",
            "response": {"technical_indicators": []},
        },
    }
    assert validate_normalized_event_dict(sample) == []
