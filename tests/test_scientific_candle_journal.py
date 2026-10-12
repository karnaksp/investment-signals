from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json

from tinvest_signal_engine.adapters.clickhouse_scientific_candles import _row
from tinvest_signal_engine.application.scientific_candles import (
    NormalizedCandleEvent,
    ScientificCandleJournalProcessor,
    scientific_candle_from_event,
)


NOW = datetime(2026, 7, 17, 7, 1, tzinfo=timezone.utc)


def _event(event_type: str = "candle") -> NormalizedCandleEvent:
    return NormalizedCandleEvent(
        event_id="candle-event-1",
        event_type=event_type,
        instrument_id="uid-sber",
        ticker="SBER",
        class_code="TQBR",
        source_time=NOW,
        received_at=NOW,
        payload={
            "open": {"units": 280, "nano": 100_000_000},
            "high": {"units": 281, "nano": 0},
            "low": {"units": 279, "nano": 900_000_000},
            "close": {"units": 280, "nano": 500_000_000},
            "volume": 42_000,
            "is_complete": True,
        },
    )


def test_maps_closed_stream_candle_with_stable_fingerprint() -> None:
    first = scientific_candle_from_event(_event())
    second = scientific_candle_from_event(_event())
    assert first == second
    assert first is not None
    assert first.open_price == Decimal("280.1")
    assert first.trading_day.isoformat() == "2026-07-17"
    assert first.payload_fingerprint.startswith("sha256:")


def test_non_candle_event_is_not_journaled() -> None:
    assert scientific_candle_from_event(_event("trade")) is None


def test_processor_persists_one_batch() -> None:
    class Store:
        rows = ()

        def persist_many(self, candles):
            self.rows = candles

    store = Store()
    count = ScientificCandleJournalProcessor(store).process_many(
        (_event(), _event("trade"))
    )
    assert count == 1
    assert len(store.rows) == 1


def test_clickhouse_row_contains_redacted_market_data_only() -> None:
    candle = scientific_candle_from_event(_event())
    assert candle is not None
    payload = _row(candle)
    encoded = json.dumps(payload)
    assert payload["source_event_id"] == "candle-event-1"
    assert payload["payload_fingerprint"] == candle.payload_fingerprint.removeprefix(
        "sha256:"
    )
    assert "token" not in encoded.lower()
    assert "account" not in encoded.lower()
