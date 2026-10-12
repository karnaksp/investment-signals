from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from tinvest_signal_engine.instruments import InstrumentMetadata
from tinvest_signal_engine.domain.market_schedule import MarketSchedule
from tinvest_signal_engine.services.ingestor import (
    _candle_recovery_due,
    _recover_session_candles,
    _recovery_candle_event,
)


def test_recovered_candle_is_deterministic_and_marked_as_backfill() -> None:
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
    candle = {
        "time": datetime(2026, 7, 29, 4, 15, tzinfo=timezone.utc),
        "open": {"units": 276, "nano": 0},
        "high": {"units": 277, "nano": 0},
        "low": {"units": 275, "nano": 0},
        "close": {"units": 276, "nano": 500_000_000},
        "volume": 10_000,
        "is_complete": True,
    }

    first = _recovery_candle_event(candle, metadata)
    second = _recovery_candle_event(candle, metadata)

    assert first.event_id == second.event_id
    assert first.event_type == "candle"
    assert first.payload["recovery_backfill"] is True


def test_periodic_candle_recovery_uses_small_overlap_window(monkeypatch) -> None:
    now = datetime(2026, 7, 30, 12, 34, tzinfo=timezone.utc)
    requested: list[tuple[datetime, datetime]] = []

    class MarketData:
        def get_candles(self, **kwargs):
            requested.append((kwargs["from_"], kwargs["to"]))
            return SimpleNamespace(candles=())

    class Producer:
        def flush(self):
            raise AssertionError("empty recovery must not flush")

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
    monkeypatch.setattr(
        "tinvest_signal_engine.services.ingestor.utc_now",
        lambda: now,
    )

    recovered = _recover_session_candles(
        client=SimpleNamespace(market_data=MarketData()),
        producer=Producer(),  # type: ignore[arg-type]
        settings=SimpleNamespace(
            ingestor_recovery_lookback_minutes=240,
            kafka_raw_value_format="json",
            kafka_raw_topic="raw.market",
        ),
        schedule=MarketSchedule.from_strings(
            timezone_name="Europe/Moscow",
            collection_start="07:00",
            collection_end="23:00",
            signal_start="07:15",
            signal_end="22:45",
        ),
        registry=(metadata,),
        instrument_configs=(
            SimpleNamespace(ticker="SBER", class_code="TQBR", candles=True),
        ),
        lookback_minutes=15,
    )

    assert recovered == 0
    assert requested == [
        (
            now.astimezone(MarketSchedule.from_strings(
                timezone_name="Europe/Moscow",
                collection_start="07:00",
                collection_end="23:00",
                signal_start="07:15",
                signal_end="22:45",
            ).timezone)
            - timedelta(minutes=15),
            now.astimezone(MarketSchedule.from_strings(
                timezone_name="Europe/Moscow",
                collection_start="07:00",
                collection_end="23:00",
                signal_start="07:15",
                signal_end="22:45",
            ).timezone),
        )
    ]


def test_periodic_candle_recovery_deadline_survives_stream_reconnect() -> None:
    last_recovery_at = 100.0

    assert not _candle_recovery_due(
        last_recovery_at=last_recovery_at,
        monotonic_now=399.0,
        interval_seconds=300.0,
    )
    assert _candle_recovery_due(
        last_recovery_at=last_recovery_at,
        monotonic_now=400.0,
        interval_seconds=300.0,
    )
