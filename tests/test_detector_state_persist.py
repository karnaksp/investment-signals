from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector, TradePoint
from tinvest_signal_engine.detector_state_persist import (
    export_window_state,
    hydrate_window_state,
)


class DetectorStatePersistTest(unittest.TestCase):
    def test_export_hydrate_preserves_trade_points(self) -> None:
        d1 = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        st = d1._states["SBER_TQBR"]
        st.trade_points.append(TradePoint(ts=ts, quantity=10.0, notional=1000.0))

        blob = export_window_state(d1)
        self.assertIn("SBER_TQBR", blob["instruments"])

        d2 = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        hydrate_window_state(d2, blob)
        restored = d2._states["SBER_TQBR"].trade_points[-1]
        self.assertEqual(restored.quantity, 10.0)
        self.assertEqual(restored.notional, 1000.0)

    def test_hydrate_empty_noop(self) -> None:
        d = SignalDetector(DetectorSettings())
        hydrate_window_state(d, {})
        self.assertEqual(len(d._states), 0)
