"""Фильтры доставки и глобальный cooldown детектора."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import DetectorSettings, RuntimeSettings
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_delivery import (
    DeliveryRateLimiter,
    should_deliver_signal,
)
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


def _runtime(**kwargs: object) -> RuntimeSettings:
    base = RuntimeSettings.from_env()
    return replace(base, **kwargs)  # type: ignore[arg-type]


def _policy_settings() -> RuntimeSettings:
    return _runtime(
        signal_delivery_allowlist=frozenset({"large_trade_print"}),
        signal_min_quality_score=85,
        signal_delivery_min_abs_z=8.0,
        signal_delivery_min_whale_lots=800.0,
        signal_delivery_min_whale_z=12.0,
        signal_delivery_min_whale_baseline_ratio=8.0,
        signal_delivery_max_per_hour=6,
    )


def _signal(
    signal_type: str,
    *,
    metric: float = 1.0,
    baseline: float = 1.0,
    z: float = 0.0,
    qs: int = 50,
) -> TriggerSignal:
    ts = datetime(2026, 5, 21, tzinfo=timezone.utc)
    return TriggerSignal(
        signal_id="s",
        detected_at=ts,
        instrument_id="SRM6_SPBFUT",
        ticker="SRM6",
        class_code="SPBFUT",
        alias="srm6",
        source_event_type="trade",
        signal_type=signal_type,
        severity=2,
        metric_value=metric,
        baseline_value=baseline,
        z_score=z,
        window_seconds=0,
        summary="x",
        payload={"quality_score": qs, "trade_size": metric},
    )


class SignalDeliveryFilterTest(unittest.TestCase):
    def test_allowlist_blocks_volume_spike(self) -> None:
        settings = _policy_settings()
        whale = _signal("large_trade_print", metric=900, baseline=50, z=14, qs=98)
        vol = _signal("volume_spike", z=11.0, qs=97)
        self.assertTrue(should_deliver_signal(whale, settings))
        self.assertFalse(should_deliver_signal(vol, settings))

    def test_small_whale_from_user_example_blocked(self) -> None:
        settings = _policy_settings()
        small = _signal(
            "large_trade_print",
            metric=152.0,
            baseline=5.119,
            z=13.42,
            qs=98,
        )
        self.assertFalse(should_deliver_signal(small, settings))

    def test_large_whale_delivers(self) -> None:
        settings = _policy_settings()
        big = _signal(
            "large_trade_print",
            metric=1200.0,
            baseline=80.0,
            z=14.0,
            qs=98,
        )
        self.assertTrue(should_deliver_signal(big, settings))

    def test_whale_blocked_when_z_below_floor(self) -> None:
        settings = _policy_settings()
        sig = _signal(
            "large_trade_print",
            metric=900.0,
            baseline=100.0,
            z=11.0,
            qs=98,
        )
        self.assertFalse(should_deliver_signal(sig, settings))

    def test_hourly_rate_limit(self) -> None:
        limiter = DeliveryRateLimiter()
        for _ in range(3):
            limiter.record()
        self.assertFalse(limiter.allow(3))
        self.assertTrue(limiter.allow(4))


class GlobalCooldownTest(unittest.TestCase):
    def test_second_signal_type_blocked_within_global_window(self) -> None:
        cfg = DetectorSettings(
            alert_cooldown_seconds=0,
            alert_global_cooldown_seconds=1800,
            volume_zscore_threshold=1.0,
            trade_count_zscore_threshold=1.0,
            min_baseline_points=3,
            baseline_points=20,
            min_relative_metric_excursion=0.0,
        )
        detector = SignalDetector(cfg)
        ts = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)
        state = detector._states["TEST_SPBFUT"]  # noqa: SLF001

        detector._record_alert_sent(state, "volume_spike", ts)  # noqa: SLF001
        ready_vol = detector._is_alert_ready(  # noqa: SLF001
            state, "volume_spike", ts + timedelta(seconds=60), cfg
        )
        ready_trade = detector._is_alert_ready(  # noqa: SLF001
            state, "trade_count_spike", ts + timedelta(seconds=60), cfg
        )
        self.assertFalse(ready_vol)
        self.assertFalse(ready_trade)

        ready_later = detector._is_alert_ready(  # noqa: SLF001
            state,
            "trade_count_spike",
            ts + timedelta(seconds=1900),
            cfg,
        )
        self.assertTrue(ready_later)


class EnrichedSpamExamplesTest(unittest.TestCase):
    def test_user_examples_do_not_deliver_with_current_policy(self) -> None:
        settings = _policy_settings()
        ts = datetime(2026, 5, 21, 10, 51, tzinfo=timezone.utc)
        for signal_type, metric, baseline in (
            ("iceberg_refill_ask", 119.0, 56.5),
            ("trade_absorption_ask", 50.0, 2.0),
            ("spread_imbalance_regime_short", 0.1441, 0.2),
        ):
            raw = TriggerSignal(
                "x",
                ts,
                "EuM6_SPBFUT",
                "EuM6",
                "SPBFUT",
                "eum6",
                "trade",
                signal_type,
                2,
                metric,
                baseline,
                0.0,
                3,
                "t",
                {},
            )
            enriched = enrich_signal_for_delivery(raw)
            self.assertFalse(
                should_deliver_signal(enriched, settings),
                signal_type,
            )

        whale = enrich_signal_for_delivery(
            TriggerSignal(
                "x",
                ts,
                "SRM6_SPBFUT",
                "SRM6",
                "SPBFUT",
                "srm6",
                "trade",
                "large_trade_print",
                3,
                152.0,
                5.119,
                13.42,
                0,
                "t",
                {"trade_size": 152.0},
            )
        )
        self.assertFalse(should_deliver_signal(whale, settings))


if __name__ == "__main__":
    unittest.main()
