from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.models import NormalizedEvent


class SignalDetectorTest(unittest.TestCase):
    def test_volume_spike_signal_is_emitted(self) -> None:
        detector = SignalDetector(
            DetectorSettings(
                sample_every_seconds=5,
                min_baseline_points=5,
                baseline_points=20,
                trade_window_seconds=60,
                price_window_seconds=60,
                alert_cooldown_seconds=0,
            )
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        emitted = []
        for index in range(6):
            emitted.extend(
                detector.process(
                    _trade_event(
                        ts=start + timedelta(seconds=index * 5),
                        quantity=100,
                        price=100.0,
                    )
                )
            )

        emitted.extend(
            detector.process(
                _trade_event(
                    ts=start + timedelta(seconds=35),
                    quantity=3_000,
                    price=101.0,
                )
            )
        )

        signal_types = {signal.signal_type for signal in emitted}
        self.assertIn("volume_spike", signal_types)

    def test_trading_status_change_is_emitted(self) -> None:
        detector = SignalDetector(
            DetectorSettings(alert_cooldown_seconds=0)
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        first = detector.process(
            _status_event(start, "SECURITY_TRADING_STATUS_NORMAL_TRADING")
        )
        second = detector.process(
            _status_event(
                start + timedelta(seconds=5),
                "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING",
            )
        )

        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0].signal_type, "trading_status_changed")

    def test_per_instrument_volume_threshold_override(self) -> None:
        base = DetectorSettings(
            sample_every_seconds=5,
            min_baseline_points=5,
            baseline_points=20,
            trade_window_seconds=60,
            price_window_seconds=60,
            alert_cooldown_seconds=0,
            volume_zscore_threshold=3.0,
        )
        strict = DetectorSettings(
            sample_every_seconds=5,
            min_baseline_points=5,
            baseline_points=20,
            trade_window_seconds=60,
            price_window_seconds=60,
            alert_cooldown_seconds=0,
            volume_zscore_threshold=99.0,
        )
        detector = SignalDetector(base, {"ALT_TQBR": strict})
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        for index in range(6):
            detector.process(
                _trade_event(
                    ts=start + timedelta(seconds=index * 5),
                    quantity=100,
                    price=100.0,
                    instrument_id="ALT_TQBR",
                    ticker="ALT",
                )
            )

        emitted = detector.process(
            _trade_event(
                ts=start + timedelta(seconds=35),
                quantity=3_000,
                price=101.0,
                instrument_id="ALT_TQBR",
                ticker="ALT",
            )
        )
        types = {s.signal_type for s in emitted}
        self.assertNotIn(
            "volume_spike",
            types,
            "High per-instrument threshold should suppress volume_spike",
        )

    def test_combo_long_signal_is_emitted(self) -> None:
        detector = SignalDetector(
            DetectorSettings(
                sample_every_seconds=0,
                min_baseline_points=3,
                baseline_points=30,
                trade_window_seconds=2,
                price_window_seconds=60,
                orderbook_window_seconds=60,
                alert_cooldown_seconds=0,
                trade_count_zscore_threshold=1.0,
                spread_zscore_threshold=1.0,
                imbalance_zscore_threshold=1.0,
                imbalance_absolute_threshold=0.5,
                combo_enabled=True,
                combo_freshness_seconds=30,
                combo_min_score=6,
            )
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        for sec in (0, 3, 6, 9):
            detector.process(
                _trade_event(
                    ts=start + timedelta(seconds=sec),
                    quantity=10,
                    price=100.0,
                    direction="TRADE_DIRECTION_BUY",
                )
            )
        for sec in (1, 4, 7):
            detector.process(
                _orderbook_event(
                    ts=start + timedelta(seconds=sec),
                    best_bid=100.0,
                    best_ask=100.1,
                    bid_qty=100.0,
                    ask_qty=100.0,
                )
            )

        detector.process(
            _orderbook_event(
                ts=start + timedelta(seconds=12),
                best_bid=99.0,
                best_ask=101.5,
                bid_qty=900.0,
                ask_qty=100.0,
            )
        )

        emitted = []
        for ms in (100, 200, 300, 400):
            emitted.extend(
                detector.process(
                    _trade_event(
                        ts=start
                        + timedelta(seconds=12, milliseconds=ms),
                        quantity=50,
                        price=101.0,
                        direction="TRADE_DIRECTION_BUY",
                    )
                )
            )
        signal_types = {signal.signal_type for signal in emitted}
        self.assertIn("microstructure_combo_long", signal_types)


def _trade_event(
    *,
    ts: datetime,
    quantity: int,
    price: float,
    instrument_id: str = "SBER_TQBR",
    ticker: str = "SBER",
    class_code: str = "TQBR",
    direction: str | None = None,
) -> NormalizedEvent:
    units = int(price)
    nano = int((price - units) * 1_000_000_000)
    return NormalizedEvent(
        event_id=f"trade-{ts.timestamp()}-{quantity}",
        event_type="trade",
        instrument_id=instrument_id,
        ticker=ticker,
        class_code=class_code,
        alias="sber",
        figi="BBG004730N88",
        uid="uid-sber",
        source_time=ts,
        received_at=ts,
        payload={
            "quantity": quantity,
            "price": {"units": units, "nano": nano},
            **({"direction": direction} if direction else {}),
        },
    )


def _orderbook_event(
    *,
    ts: datetime,
    best_bid: float,
    best_ask: float,
    bid_qty: float,
    ask_qty: float,
) -> NormalizedEvent:
    bid_units = int(best_bid)
    bid_nano = int((best_bid - bid_units) * 1_000_000_000)
    ask_units = int(best_ask)
    ask_nano = int((best_ask - ask_units) * 1_000_000_000)
    return NormalizedEvent(
        event_id=f"orderbook-{ts.timestamp()}",
        event_type="orderbook",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="BBG004730N88",
        uid="uid-sber",
        source_time=ts,
        received_at=ts,
        payload={
            "bids": [
                {"price": {"units": bid_units, "nano": bid_nano}, "quantity": bid_qty}
            ],
            "asks": [
                {"price": {"units": ask_units, "nano": ask_nano}, "quantity": ask_qty}
            ],
        },
    )


def _status_event(ts: datetime, status: str) -> NormalizedEvent:
    return NormalizedEvent(
        event_id=f"status-{ts.timestamp()}",
        event_type="trading_status",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="BBG004730N88",
        uid="uid-sber",
        source_time=ts,
        received_at=ts,
        payload={"trading_status": status},
    )


if __name__ == "__main__":
    unittest.main()
