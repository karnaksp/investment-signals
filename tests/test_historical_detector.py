"""Unit tests for historical anomaly bucket logic (no ClickHouse)."""

from __future__ import annotations

import datetime as dt

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.historical_baselines import incremental_utc_day_range


def test_historical_timeframe_csv_parsing() -> None:
    d = SignalDetector(DetectorSettings(), historical_store=None)
    cfg = DetectorSettings(historical_timeframes_csv="1m, 5m ")
    assert d._historical_timeframe_set(cfg) == {"1m", "5m"}


def test_floor_bucket_utc() -> None:
    d = SignalDetector(DetectorSettings(), historical_store=None)
    ts = dt.datetime(2026, 5, 14, 13, 37, 42, tzinfo=dt.timezone.utc)
    assert d._floor_bucket_utc(ts, 1) == dt.datetime(
        2026, 5, 14, 13, 37, 0, tzinfo=dt.timezone.utc
    )
    assert d._floor_bucket_utc(ts, 5) == dt.datetime(
        2026, 5, 14, 13, 35, 0, tzinfo=dt.timezone.utc
    )
    assert d._floor_bucket_utc(ts, 15) == dt.datetime(
        2026, 5, 14, 13, 30, 0, tzinfo=dt.timezone.utc
    )


def test_slot_minute_from_dt() -> None:
    d = SignalDetector(DetectorSettings(), historical_store=None)
    t = dt.datetime(2026, 5, 14, 10, 25, 0, tzinfo=dt.timezone.utc)
    assert d._slot_minute_from_dt(t) == 10 * 60 + 25


def test_micro_secondary_multiplier() -> None:
    d = SignalDetector(DetectorSettings(), historical_store=None)
    cfg = DetectorSettings(
        microstructure_secondary_mode=False,
        microstructure_secondary_threshold_multiplier=2.0,
    )
    assert d._micro_threshold_multiplier(cfg) == 1.0
    cfg2 = DetectorSettings(
        microstructure_secondary_mode=True,
        microstructure_secondary_threshold_multiplier=2.0,
    )
    assert d._micro_threshold_multiplier(cfg2) == 2.0


def test_incremental_utc_day_range() -> None:
    a, b = incremental_utc_day_range(n_calendar_days=2)
    assert a <= b
    assert (b - a).days == 1
