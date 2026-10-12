from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.models import NormalizedEvent
from tinvest_signal_engine.services.ingestor import _should_publish


def _settings(interval_ms: int) -> RuntimeSettings:
    return replace(
        RuntimeSettings.from_env(service_name="ingestor"),
        ingestor_orderbook_min_interval_ms=interval_ms,
    )


def _event(event_type: str, ts: datetime) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=f"{event_type}-{ts.timestamp()}",
        event_type=event_type,
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber_tqbr",
        figi="BBG004730N88",
        uid="uid-sber",
        lot=1,
        source_time=ts,
        received_at=ts,
        payload={},
    )


def test_orderbook_throttle_limits_per_instrument_publish_rate() -> None:
    settings = _settings(1000)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    last: dict[tuple[str, str], float] = {}

    assert _should_publish(_event("orderbook", start), settings, last) is True
    assert (
        _should_publish(
            _event("orderbook", start + timedelta(milliseconds=500)),
            settings,
            last,
        )
        is False
    )
    assert (
        _should_publish(
            _event("orderbook", start + timedelta(milliseconds=1000)),
            settings,
            last,
        )
        is True
    )


def test_orderbook_throttle_does_not_limit_trades() -> None:
    settings = _settings(1000)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    last: dict[tuple[str, str], float] = {}

    assert _should_publish(_event("trade", start), settings, last) is True
    assert (
        _should_publish(
            _event("trade", start + timedelta(milliseconds=1)),
            settings,
            last,
        )
        is True
    )
