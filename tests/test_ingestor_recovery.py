from datetime import datetime, timezone

from tinvest_signal_engine.instruments import InstrumentMetadata
from tinvest_signal_engine.services.ingestor import _recovery_trade_event


def test_recovered_trade_is_deterministic_and_cannot_emit_realtime_signal() -> None:
    metadata = InstrumentMetadata(
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="BBG004730N88",
        uid="e6123145-9665-43e0-8413-cd61b8aa9b13",
        lot=10,
        currency="rub",
        name="Сбер Банк",
    )
    trade = {
        "time": datetime(2026, 7, 29, 5, 15, tzinfo=timezone.utc),
        "price": {"units": 276, "nano": 100_000_000},
        "quantity": 20,
        "direction": "TRADE_DIRECTION_BUY",
    }

    first = _recovery_trade_event(trade, metadata)
    second = _recovery_trade_event(trade, metadata)

    assert first.event_id == second.event_id
    assert first.event_type == "trade"
    assert first.source_time == trade["time"]
    assert first.payload["recovery_backfill"] is True
