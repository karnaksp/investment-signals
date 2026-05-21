"""
Эвристическая оценка «полезности» сигнала (0–100) для снижения шума в аналитике.

Не заменяет офлайн-разметку (hit/miss по forward VWAP), а ранжирует события
по силе отклонения, серьёзности и типу паттерна — в духе internal alert scoring
в торговых мониторингах (z-score / severity / magnitude vs baseline).
"""

from __future__ import annotations

from typing import Any

from .models import TriggerSignal

# Насколько «редкие» типы считаем более содержательными (0..1).
# Ключи совпадают с signal_type из detector_core; устаревшие имена оставлены как алиасы.
_SIGNAL_TYPE_WEIGHT: dict[str, float] = {
    "microstructure_combo_long": 1.0,
    "microstructure_combo_short": 1.0,
    "lead_lag_divergence": 0.95,
    "orderbook_spoofing_bid_pull": 0.9,
    "orderbook_spoofing_ask_pull": 0.9,
    "aggressive_trade_burst": 0.88,
    "obi_dynamics": 0.82,
    "obi_delta_spike": 0.82,
    "volume_spike": 0.75,
    "trade_rate_spike": 0.72,
    "trade_count_spike": 0.72,
    "price_jump": 0.78,
    "price_move_spike": 0.78,
    "spread_widening": 0.7,
    "spread_spike": 0.7,
    "orderbook_imbalance": 0.68,
    "imbalance_spike": 0.68,
    "trading_status_changed": 0.55,
    "market_access_changed": 0.62,
    "orderbook_snapshot_inconsistent": 0.7,
    "price_near_limit_band": 0.74,
    "open_interest_spike": 0.7,
    "candle_range_spike": 0.68,
    "vpin_spike": 0.85,
    "large_trade_print": 0.85,
    "trade_absorption_bid": 0.88,
    "trade_absorption_ask": 0.88,
    "iceberg_refill_bid": 0.88,
    "iceberg_refill_ask": 0.88,
    "spread_imbalance_regime_long": 0.75,
    "spread_imbalance_regime_short": 0.75,
}


def compute_signal_quality(signal: TriggerSignal) -> dict[str, Any]:
    z = abs(float(signal.z_score))
    # Нормализация |z|: типичный «сильный» хвост от 3 до ~8
    z_norm = max(0.0, min(1.0, (z - 1.5) / 5.5))

    base = abs(float(signal.baseline_value)) + 1e-12
    mag = abs(float(signal.metric_value)) / base
    mag_norm = max(0.0, min(1.0, (mag - 1.0) / 4.0))

    sev = int(signal.severity)
    sev_norm = max(1, min(3, sev)) / 3.0

    st = signal.signal_type
    if st.startswith("historical_"):
        w = 0.92
    else:
        w = _SIGNAL_TYPE_WEIGHT.get(st, 0.65)

    raw = (
        100.0
        * (
            0.45 * z_norm
            + 0.20 * mag_norm
            + 0.22 * sev_norm
            + 0.13 * w
        )
    )
    score = int(max(0, min(100, round(raw))))

    if score >= 72:
        tier = "high"
        tier_ru = "высокая"
        hint = "Сильное отклонение и/или редкий паттерн — приоритет просмотра."
    elif score >= 48:
        tier = "medium"
        tier_ru = "средняя"
        hint = "Умеренная аномалия; сверить с контекстом стакана/ленты."
    else:
        tier = "low"
        tier_ru = "низкая"
        hint = "Слабый сигнал: возможен рыночный шум, не действовать только по нему."

    return {
        "quality_score": score,
        "quality_tier": tier,
        "quality_tier_ru": tier_ru,
        "quality_hint_ru": hint,
        "quality_factors": {
            "z_norm": round(z_norm, 4),
            "magnitude_norm": round(mag_norm, 4),
            "severity_norm": round(sev_norm, 4),
            "type_weight": w,
            "abs_z": round(z, 4),
            "magnitude_ratio": round(mag, 4),
        },
    }
