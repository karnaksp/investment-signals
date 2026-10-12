from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector, TradePoint
from tinvest_signal_engine.detector_state_persist import (
    export_instrument_state,
    export_window_state,
    hydrate_window_state,
    replace_instrument_states,
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

    def test_partition_checkpoint_replaces_state_and_cooldown(self) -> None:
        source = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        state = source._states["SBER_TQBR"]
        state.trade_points.append(TradePoint(ts=ts, quantity=10.0, notional=1000.0))
        state.last_alert_at["price_jump"] = ts
        source._mid_track["SBER_TQBR"].append((ts, 101.5))
        payload = export_instrument_state(source, "SBER_TQBR")

        target = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        target._states["STALE_TQBR"].trade_points.append(
            TradePoint(ts=ts, quantity=1.0, notional=1.0)
        )
        replace_instrument_states(target, [payload])

        self.assertNotIn("STALE_TQBR", target._states)
        restored = target._states["SBER_TQBR"]
        self.assertEqual(restored.trade_points[-1].quantity, 10.0)
        self.assertEqual(restored.last_alert_at["price_jump"], ts)
        self.assertEqual(target._mid_track["SBER_TQBR"][-1], (ts, 101.5))
