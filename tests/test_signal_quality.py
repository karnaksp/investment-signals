"""Проверка эвристики quality_score и порога доставки SIGNAL_MIN_QUALITY_SCORE."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery
from tinvest_signal_engine.signal_quality import compute_signal_quality

_DELIVERY_FLOOR = 70


def _signal(
    signal_type: str,
    *,
    metric: float,
    baseline: float,
    z_score: float = 0.0,
    severity: int = 2,
) -> TriggerSignal:
    ts = datetime(2026, 5, 21, 10, 51, tzinfo=timezone.utc)
    return TriggerSignal(
        signal_id="test",
        detected_at=ts,
        instrument_id="SVM6_SPBFUT",
        ticker="SVM6",
        class_code="SPBFUT",
        alias="svm6",
        source_event_type="trade",
        signal_type=signal_type,
        severity=severity,
        metric_value=metric,
        baseline_value=baseline,
        z_score=z_score,
        window_seconds=3,
        summary="test",
        payload={},
    )


class SignalQualitySpamFilterTest(unittest.TestCase):
    """Примеры из прод-уведомлений: слабые не проходят порог 55, whale — проходит."""

    def test_noisy_orderflow_below_delivery_floor(self) -> None:
        cases = (
            ("iceberg_refill_ask", 119.0, 56.5),
            ("trade_absorption_ask", 50.0, 2.0),
            ("iceberg_refill_ask", 44.0, 22.5),
            ("spread_imbalance_regime_short", 0.1441, 0.2),
        )
        for signal_type, metric, baseline in cases:
            sig = _signal(signal_type, metric=metric, baseline=baseline)
            q = compute_signal_quality(sig)["quality_score"]
            self.assertLess(
                q,
                _DELIVERY_FLOOR,
                f"{signal_type} quality_score={q} should be below {_DELIVERY_FLOOR}",
            )

    def test_whale_print_passes_delivery_floor(self) -> None:
        sig = _signal(
            "large_trade_print",
            metric=152.0,
            baseline=5.119,
            z_score=13.42,
            severity=3,
        )
        q = compute_signal_quality(sig)["quality_score"]
        self.assertGreaterEqual(q, _DELIVERY_FLOOR)

    def test_enriched_payload_carries_quality_for_detector_filter(self) -> None:
        weak = enrich_signal_for_delivery(
            _signal("iceberg_refill_ask", metric=119.0, baseline=56.5)
        )
        strong = enrich_signal_for_delivery(
            _signal(
                "large_trade_print",
                metric=152.0,
                baseline=5.119,
                z_score=13.42,
                severity=3,
            )
        )
        weak_q = weak.payload["quality_score"]
        strong_q = strong.payload["quality_score"]
        self.assertIsInstance(weak_q, int)
        self.assertIsInstance(strong_q, int)
        self.assertLess(weak_q, _DELIVERY_FLOOR)
        self.assertGreaterEqual(strong_q, _DELIVERY_FLOOR)

    def test_regime_disabled_in_default_detectors_yaml(self) -> None:
        from pathlib import Path

        from tinvest_signal_engine.config import load_detector_settings

        cfg = load_detector_settings(
            Path(__file__).resolve().parents[1] / "conf" / "detectors.yaml"
        )
        self.assertFalse(cfg.spread_imbalance_regime_enabled)
        self.assertFalse(cfg.combo_enabled)
        self.assertFalse(cfg.vpin_enabled)
        self.assertFalse(cfg.iceberg_enabled)
        self.assertFalse(cfg.absorption_enabled)
        self.assertGreaterEqual(cfg.alert_cooldown_seconds, 900)
        self.assertGreaterEqual(cfg.alert_global_cooldown_seconds, 3600)
        self.assertGreaterEqual(cfg.whale_min_absolute_qty, 600)


if __name__ == "__main__":
    unittest.main()
