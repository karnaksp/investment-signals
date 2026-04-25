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


def _trade_event(
    *,
    ts: datetime,
    quantity: int,
    price: float,
) -> NormalizedEvent:
    units = int(price)
    nano = int((price - units) * 1_000_000_000)
    return NormalizedEvent(
        event_id=f"trade-{ts.timestamp()}-{quantity}",
        event_type="trade",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="BBG004730N88",
        uid="uid-sber",
        source_time=ts,
        received_at=ts,
        payload={
            "quantity": quantity,
            "price": {"units": units, "nano": nano},
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
