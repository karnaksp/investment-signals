from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import InstrumentState, SignalDetector
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal


class SignalDetectorTest(unittest.TestCase):
    def test_enrich_signals_with_unary_context(self) -> None:
        cfg = DetectorSettings(attach_unary_context_to_signals=True)
        detector = SignalDetector(cfg)
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        mv = NormalizedEvent(
            event_id="mv1",
            event_type="market_values",
            instrument_id="SBER_TQBR",
            ticker="SBER",
            class_code="TQBR",
            alias="sber",
            figi="f",
            uid="u",
            lot=1,
            source_time=ts,
            received_at=ts,
            payload={
                "poll_batch_id": "pb",
                "source": "get_market_values",
                "values": [],
            },
        )
        self.assertEqual(detector.process(mv), [])
        sig = TriggerSignal(
            signal_id="s1",
            detected_at=ts,
            instrument_id="SBER_TQBR",
            ticker="SBER",
            class_code="TQBR",
            alias="sber",
            source_event_type="trade",
            signal_type="volume_spike",
            severity=1,
            metric_value=1.0,
            baseline_value=1.0,
            z_score=1.0,
            window_seconds=60,
            summary="x",
            payload={},
        )
        out = detector.enrich_signals_with_unary([sig])
        self.assertIn("unary_context", out[0].payload)
        self.assertIn("market_values", out[0].payload["unary_context"])

    def test_enrich_skipped_when_attach_unary_disabled(self) -> None:
        cfg = DetectorSettings(attach_unary_context_to_signals=False)
        detector = SignalDetector(cfg)
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detector.process(
            NormalizedEvent(
                event_id="mv2",
                event_type="market_values",
                instrument_id="SBER_TQBR",
                ticker="SBER",
                class_code="TQBR",
                alias="sber",
                figi="f",
                uid="u",
                lot=1,
                source_time=ts,
                received_at=ts,
                payload={
                    "poll_batch_id": "pb",
                    "source": "get_market_values",
                    "values": [],
                },
            )
        )
        sig = TriggerSignal(
            signal_id="s2",
            detected_at=ts,
            instrument_id="SBER_TQBR",
            ticker="SBER",
            class_code="TQBR",
            alias="sber",
            source_event_type="trade",
            signal_type="volume_spike",
            severity=1,
            metric_value=1.0,
            baseline_value=1.0,
            z_score=1.0,
            window_seconds=60,
            summary="x",
            payload={},
        )
        out = detector.enrich_signals_with_unary([sig])
        self.assertNotIn("unary_context", out[0].payload)

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

    def test_price_jump_payload_includes_signed_move_context(self) -> None:
        detector = SignalDetector(
            DetectorSettings(
                sample_every_seconds=5,
                min_baseline_points=5,
                baseline_points=20,
                trade_window_seconds=60,
                price_window_seconds=60,
                alert_cooldown_seconds=0,
                price_return_zscore_threshold=1.0,
                price_move_absolute_threshold_bps=0.0,
            )
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)

        for index in range(6):
            detector.process(
                _trade_event(
                    ts=start + timedelta(seconds=index * 5),
                    quantity=10,
                    price=100.0,
                )
            )

        emitted = detector.process(
            _trade_event(
                ts=start + timedelta(seconds=35),
                quantity=10,
                price=102.0,
            )
        )

        price_jump = next(s for s in emitted if s.signal_type == "price_jump")
        self.assertEqual(price_jump.payload["price_direction"], "up")
        self.assertAlmostEqual(price_jump.payload["price_change_pct"], 2.0)
        self.assertEqual(price_jump.payload["start_price"], 100.0)
        self.assertEqual(price_jump.payload["current_price"], 102.0)

    def test_min_relative_metric_excursion_blocks_flat_baseline_spike(self) -> None:
        """Высокий z при нулевом std не должен проходить без заметного относит. отклонения."""
        cfg = DetectorSettings(
            min_baseline_points=3,
            baseline_points=10,
            alert_cooldown_seconds=0,
            volume_zscore_threshold=1.5,
            min_relative_metric_excursion=0.2,
        )
        detector = SignalDetector(cfg)
        state = InstrumentState()
        state.volume_history.extend([100.0, 100.0, 100.0])
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        event = _trade_event(ts=start, quantity=1, price=100.0)
        blocked = detector._maybe_emit_from_history(
            event=event,
            state=state,
            cfg=cfg,
            signal_type="volume_spike",
            source_event_type="trade",
            history=state.volume_history,
            threshold=cfg.volume_zscore_threshold,
            value=100.05,
            baseline_label="rolling volume",
            window_seconds=60,
            summary_template="{ticker}",
        )
        self.assertEqual(blocked, [])
        passed = detector._maybe_emit_from_history(
            event=event,
            state=state,
            cfg=cfg,
            signal_type="volume_spike",
            source_event_type="trade",
            history=state.volume_history,
            threshold=cfg.volume_zscore_threshold,
            value=125.0,
            baseline_label="rolling volume",
            window_seconds=60,
            summary_template="{ticker}",
        )
        self.assertEqual(len(passed), 1)
        self.assertEqual(passed[0].signal_type, "volume_spike")

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
        combo = next(s for s in emitted if s.signal_type == "microstructure_combo_long")
        detail = combo.payload.get("combo_detail")
        self.assertIsInstance(detail, dict)
        self.assertIn("points_awarded", detail)
        self.assertIn("flags", detail)
        self.assertEqual(detail.get("freshness_seconds"), 30)

    def test_orderbook_spoofing_bid_pull_is_emitted(self) -> None:
        detector = SignalDetector(
            DetectorSettings(
                spoofing_enabled=True,
                spoofing_min_wall_qty=100.0,
                spoofing_wall_ratio=2.0,
                spoofing_qty_drop_ratio=0.5,
                spoofing_max_mid_move_bps=10.0,
                spoofing_max_gap_seconds=1.0,
                spoofing_lookback_seconds=10.0,
                alert_cooldown_seconds=0,
            )
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detector.process(
            _orderbook_event(
                ts=start,
                best_bid=100.0,
                best_ask=100.1,
                bid_qty=100.0,
                ask_qty=100.0,
            )
        )
        detector.process(
            _orderbook_event(
                ts=start + timedelta(milliseconds=200),
                best_bid=100.0,
                best_ask=100.1,
                bid_qty=500.0,
                ask_qty=100.0,
            )
        )
        emitted = detector.process(
            _orderbook_event(
                ts=start + timedelta(milliseconds=400),
                best_bid=100.0,
                best_ask=100.1,
                bid_qty=50.0,
                ask_qty=100.0,
            )
        )
        types = {s.signal_type for s in emitted}
        self.assertIn("orderbook_spoofing_bid_pull", types)


class AlertStateExportHydrateTest(unittest.TestCase):
    def test_export_roundtrip_and_merge_policy(self) -> None:
        detector = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        start = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        detector._states["X_TQBR"].last_alert_at["volume_spike"] = start
        blob = detector.export_alert_state()
        self.assertEqual(blob["X_TQBR"]["volume_spike"], start.isoformat())

        fresh = SignalDetector(DetectorSettings(alert_cooldown_seconds=0))
        fresh.hydrate_alert_state(blob)
        self.assertEqual(
            fresh._states["X_TQBR"].last_alert_at["volume_spike"],
            start,
        )

        older = {
            "X_TQBR": {"volume_spike": (start - timedelta(hours=1)).isoformat()}
        }
        fresh.hydrate_alert_state(older)
        self.assertEqual(
            fresh._states["X_TQBR"].last_alert_at["volume_spike"],
            start,
        )

        newer = {
            "X_TQBR": {"volume_spike": (start + timedelta(hours=1)).isoformat()}
        }
        fresh.hydrate_alert_state(newer)
        self.assertEqual(
            fresh._states["X_TQBR"].last_alert_at["volume_spike"],
            start + timedelta(hours=1),
        )


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
        lot=1,
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
        lot=1,
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
        lot=1,
        source_time=ts,
        received_at=ts,
        payload={"trading_status": status},
    )


if __name__ == "__main__":
    unittest.main()
