"""Фильтры публикации сигналов (Kafka / Telegram / webhook)."""

from __future__ import annotations

import time
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import RuntimeSettings
    from .models import TriggerSignal

# Типы, где z-score по определению может быть 0 (эвристики, статусы).
_SIGNAL_TYPES_Z_EXEMPT: frozenset[str] = frozenset(
    {
        "large_trade_print",
        "trade_absorption_bid",
        "trade_absorption_ask",
        "iceberg_refill_bid",
        "iceberg_refill_ask",
        "spread_imbalance_regime_long",
        "spread_imbalance_regime_short",
        "trading_status_changed",
        "market_access_changed",
        "orderbook_snapshot_inconsistent",
        "price_near_limit_band",
        "lead_lag_divergence",
        "aggressive_trade_burst",
    }
)

_WHALE_SIGNAL_TYPE = "large_trade_print"


class DeliveryRateLimiter:
    """Скользящий лимит доставок за час (все инструменты)."""

    def __init__(self) -> None:
        self._timestamps: deque[float] = deque()

    def allow(self, max_per_hour: int | None) -> bool:
        if max_per_hour is None or max_per_hour <= 0:
            return True
        now = time.monotonic()
        cutoff = now - 3600.0
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()
        return len(self._timestamps) < max_per_hour

    def record(self) -> None:
        self._timestamps.append(time.monotonic())


_delivery_rate_limiter = DeliveryRateLimiter()


def delivery_rate_limiter() -> DeliveryRateLimiter:
    return _delivery_rate_limiter


def _whale_trade_lots(signal: TriggerSignal) -> float:
    payload = signal.payload or {}
    raw = payload.get("trade_size", signal.metric_value)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(signal.metric_value)


def _passes_whale_delivery_gates(
    signal: TriggerSignal, settings: RuntimeSettings
) -> bool:
    lots = _whale_trade_lots(signal)
    min_lots = settings.signal_delivery_min_whale_lots
    if min_lots is not None and min_lots > 0 and lots < min_lots:
        return False

    min_z = settings.signal_delivery_min_whale_z
    if min_z is not None and min_z > 0:
        if abs(float(signal.z_score)) < min_z:
            return False

    min_ratio = settings.signal_delivery_min_whale_baseline_ratio
    if min_ratio is not None and min_ratio > 0:
        base = abs(float(signal.baseline_value))
        if base >= 1e-9 and lots / base < min_ratio:
            return False

    return True


def should_deliver_signal(signal: TriggerSignal, settings: RuntimeSettings) -> bool:
    """True — сигнал можно писать в хранилище и слать в мессенджеры."""
    if not delivery_rate_limiter().allow(settings.signal_delivery_max_per_hour):
        return False

    allow = settings.signal_delivery_allowlist
    if allow is not None and signal.signal_type not in allow:
        return False

    min_q = settings.signal_min_quality_score
    if min_q is not None:
        qs = signal.payload.get("quality_score")
        if isinstance(qs, int) and qs < min_q:
            return False

    if signal.signal_type == _WHALE_SIGNAL_TYPE:
        return _passes_whale_delivery_gates(signal, settings)

    min_z = settings.signal_delivery_min_abs_z
    if min_z is not None and min_z > 0:
        if signal.signal_type not in _SIGNAL_TYPES_Z_EXEMPT:
            if abs(float(signal.z_score)) < min_z:
                return False

    return True


def record_delivered_signal() -> None:
    """Вызывать после успешной постановки сигнала в очередь доставки."""
    delivery_rate_limiter().record()
