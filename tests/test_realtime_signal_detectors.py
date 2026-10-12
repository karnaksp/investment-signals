from collections import deque
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import (
    OrderBookDepthSnapshot,
    PricePoint,
    SignalDetector,
    SignedTradePoint,
)
from tinvest_signal_engine.models import NormalizedEvent


def _event(event_type: str, ts: datetime, payload: dict) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=f"{event_type}-{ts.timestamp()}",
        event_type=event_type,
        instrument_id="SiZ6_SPBFUT",
        ticker="SiZ6",
        class_code="SPBFUT",
        alias="usd-rub future",
        figi="figi",
        uid="uid",
        lot=1,
        source_time=ts,
        received_at=ts,
        payload=payload,
    )


def _q(value: int) -> dict[str, int]:
    return {"units": value, "nano": 0}


def test_detector_emits_confirmed_impulse() -> None:
    cfg = DetectorSettings(
        min_baseline_points=3,
        sample_every_seconds=1,
        alert_cooldown_seconds=0,
        price_return_zscore_threshold=2.0,
        confirmed_impulse_enabled=True,
        confirmed_impulse_min_move_bps=10.0,
        confirmed_impulse_min_abs_delta_qty=5.0,
    )
    detector = SignalDetector(cfg)
    now = datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)
    state = detector._states["SiZ6_SPBFUT"]
    state.price_points.append(PricePoint(now - timedelta(seconds=60), 100.0))
    state.signed_trade_points.append(SignedTradePoint(now, 20.0))
    state.return_history.extend((1.0, 1.5, 2.0))
    state.last_active_at["volume_spike"] = now - timedelta(seconds=5)

    signals = detector.process(_event("last_price", now, {"price": _q(101)}))

    assert {signal.signal_type for signal in signals} >= {
        "price_jump",
        "confirmed_impulse_long",
    }


def test_detector_emits_liquidity_vacuum() -> None:
    cfg = DetectorSettings(
        min_baseline_points=3,
        sample_every_seconds=1,
        alert_cooldown_seconds=0,
        spread_zscore_threshold=2.0,
        liquidity_vacuum_enabled=True,
        liquidity_vacuum_lookback_seconds=60,
        liquidity_vacuum_min_depth_drop_ratio=0.45,
        liquidity_vacuum_min_spread_zscore=2.0,
        liquidity_vacuum_min_reference_qty=10.0,
    )
    detector = SignalDetector(cfg)
    now = datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)
    state = detector._states["SiZ6_SPBFUT"]
    state.spread_history.extend((0.9, 1.0, 1.1))
    state.orderbook_depth_snapshots = deque(
        (
            OrderBookDepthSnapshot(
                now - timedelta(seconds=40), 100.0, 100.0, 100.0, 99.995, 100.005
            ),
            OrderBookDepthSnapshot(
                now - timedelta(seconds=20), 100.0, 100.0, 100.0, 99.995, 100.005
            ),
        ),
        maxlen=96,
    )

    signals = detector.process(
        _event(
            "orderbook",
            now,
            {
                "bids": [{"price": _q(99), "quantity": 20}],
                "asks": [{"price": _q(101), "quantity": 20}],
            },
        )
    )

    assert "liquidity_vacuum" in {signal.signal_type for signal in signals}


def test_detector_emits_futures_positioning_from_oi_delta() -> None:
    cfg = DetectorSettings(
        min_baseline_points=3,
        sample_every_seconds=1,
        alert_cooldown_seconds=0,
        open_interest_zscore_threshold=2.0,
        open_interest_min_price_move_bps=5.0,
    )
    detector = SignalDetector(cfg)
    now = datetime(2026, 10, 12, 9, 0, tzinfo=timezone.utc)
    state = detector._states["SiZ6_SPBFUT"]
    state.price_points.extend(
        (
            PricePoint(now - timedelta(seconds=60), 100.0),
            PricePoint(now, 101.0),
        )
    )
    state.last_open_interest = 1_000.0
    state.open_interest_history.extend((1.0, 2.0, 1.0))

    signals = detector.process(_event("open_interest", now, {"open_interest": 1_050}))

    signal = next(item for item in signals if item.signal_type == "futures_positioning")
    assert signal.payload["regime"] == "long_build"
