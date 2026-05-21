"""Микроструктурные сигналы order flow: VPIN, whale print, absorption, iceberg, regime."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import fmean
from typing import Iterable

from .config import DetectorSettings


@dataclass
class IcebergWatch:
    touch_price: float
    qty_before_hits: float
    accumulated_hit_qty: float
    first_hit_ts: datetime
    last_hit_ts: datetime


@dataclass
class TouchSnapshot:
    ts: datetime
    best_bid: float
    best_ask: float
    best_bid_qty: float
    best_ask_qty: float
    mid: float


@dataclass
class OrderflowSignalCandidate:
    """Результат оценки одного сигнала (детектор строит TriggerSignal)."""

    signal_type: str
    metric_value: float
    baseline_value: float
    z_score: float
    window_seconds: int
    summary: str
    payload: dict
    severity: int | None = None


def z_score_from_history(history: Iterable[float], value: float) -> tuple[float, float]:
    samples = list(history)
    if not samples:
        return 0.0, 0.0
    baseline = fmean(samples)
    variance = fmean((sample - baseline) ** 2 for sample in samples)
    std = math.sqrt(variance)
    if std <= 1e-12:
        return baseline, 999.0 if value > baseline else 0.0
    return baseline, (value - baseline) / std


def severity_from_z_score(z_score: float) -> int:
    z = abs(float(z_score))
    if z >= 6:
        return 3
    if z >= 4:
        return 2
    return 1


def mid_range_bps(
    ring: deque[tuple[datetime, float]],
    now: datetime,
    window: timedelta,
) -> float | None:
    if not ring:
        return None
    start = now - window
    prices = [px for ts, px in ring if start <= ts <= now]
    if len(prices) < 2:
        return 0.0 if prices else None
    lo, hi = min(prices), max(prices)
    mid = (lo + hi) / 2.0
    if mid <= 0:
        return None
    return (hi - lo) / mid * 10_000.0


def feed_vpin_trade(
    *,
    buy_qty: float,
    sell_qty: float,
    bucket_buy: float,
    bucket_sell: float,
    bucket_target: float,
    bucket_imbalances: deque[float],
    lookback_buckets: int,
) -> tuple[float, float, list[float], float | None]:
    """
    Добавляет сделку в VPIN-корзину.

    Returns:
        (new_bucket_buy, new_bucket_sell, newly_closed_imbalance_ratios, current_vpin)
    """
    bucket_buy = float(bucket_buy) + max(0.0, buy_qty)
    bucket_sell = float(bucket_sell) + max(0.0, sell_qty)
    target = max(1e-9, float(bucket_target))
    closed: list[float] = []
    current_vpin: float | None = None

    while True:
        total = bucket_buy + bucket_sell
        if total < target:
            break
        # Закрываем корзину пропорционально (упрощение: один imbalance на полную корзину).
        imbalance_ratio = abs(bucket_buy - bucket_sell) / total
        closed.append(imbalance_ratio)
        bucket_imbalances.append(imbalance_ratio)
        overflow_buy = max(0.0, bucket_buy - target * (bucket_buy / total))
        overflow_sell = max(0.0, bucket_sell - target * (bucket_sell / total))
        bucket_buy = overflow_buy
        bucket_sell = overflow_sell
        if bucket_buy + bucket_sell < 1e-12:
            bucket_buy = 0.0
            bucket_sell = 0.0
            break

    if bucket_imbalances:
        tail = list(bucket_imbalances)[-max(1, lookback_buckets) :]
        current_vpin = fmean(tail)

    return bucket_buy, bucket_sell, closed, current_vpin


def evaluate_vpin_spike(
    *,
    vpin_history: deque[float],
    current_vpin: float,
    cfg: DetectorSettings,
    min_buckets: int,
) -> OrderflowSignalCandidate | None:
    if not cfg.vpin_enabled or current_vpin is None:
        return None
    if len(vpin_history) < max(cfg.min_baseline_points, min_buckets):
        return None
    baseline, z = z_score_from_history(vpin_history, current_vpin)
    eff = float(cfg.vpin_zscore_threshold)
    if cfg.microstructure_secondary_mode:
        eff *= cfg.microstructure_secondary_threshold_multiplier
    if z < eff:
        return None
    return OrderflowSignalCandidate(
        signal_type="vpin_spike",
        metric_value=current_vpin,
        baseline_value=baseline,
        z_score=z,
        window_seconds=cfg.trade_window_seconds,
        summary="VPIN elevated vs baseline (z-score spike).",
        payload={
            "vpin": current_vpin,
            "buckets_in_window": len(vpin_history),
            "lookback_buckets": cfg.vpin_lookback_buckets,
        },
    )


def evaluate_whale_print(
    *,
    trade_size: float,
    trade_size_history: deque[float],
    cfg: DetectorSettings,
) -> OrderflowSignalCandidate | None:
    if not cfg.whale_print_enabled or trade_size <= 0:
        return None
    if trade_size < cfg.whale_min_absolute_qty:
        return None
    if len(trade_size_history) < cfg.min_baseline_points:
        return None
    baseline, z = z_score_from_history(trade_size_history, trade_size)
    eff = float(cfg.whale_print_zscore_threshold)
    if cfg.microstructure_secondary_mode:
        eff *= cfg.microstructure_secondary_threshold_multiplier
    if z < eff:
        return None
    return OrderflowSignalCandidate(
        signal_type="large_trade_print",
        metric_value=trade_size,
        baseline_value=baseline,
        z_score=z,
        window_seconds=0,
        summary=(
            "Large trade print {metric:.2f} lots "
            "vs baseline {baseline:.2f} (z={z_score:.2f})."
        ),
        payload={
            "trade_size": trade_size,
            "whale_min_absolute_qty": cfg.whale_min_absolute_qty,
        },
    )


def evaluate_absorption(
    *,
    signed_points: Iterable[tuple[datetime, float]],
    mid_ring: deque[tuple[datetime, float]],
    now: datetime,
    cfg: DetectorSettings,
) -> list[OrderflowSignalCandidate]:
    if not cfg.absorption_enabled:
        return []
    window = timedelta(milliseconds=max(10, cfg.absorption_window_ms))
    start = now - window
    aggressive_sell = 0.0
    aggressive_buy = 0.0
    for ts, sq in signed_points:
        if ts < start:
            continue
        if sq < 0:
            aggressive_sell += -sq
        elif sq > 0:
            aggressive_buy += sq

    mid_move = mid_range_bps(mid_ring, now, window)
    if mid_move is None:
        return []

    out: list[OrderflowSignalCandidate] = []
    ratio_min = max(1.0, float(cfg.absorption_aggression_ratio))

    if (
        aggressive_sell >= cfg.absorption_min_aggressive_qty
        and mid_move <= cfg.absorption_max_mid_move_bps
        and aggressive_sell >= ratio_min * max(1e-9, aggressive_buy)
    ):
        out.append(
            OrderflowSignalCandidate(
                signal_type="trade_absorption_bid",
                metric_value=aggressive_sell,
                baseline_value=float(cfg.absorption_max_mid_move_bps),
                z_score=0.0,
                window_seconds=max(1, cfg.absorption_window_ms // 1000),
                summary=(
                    f"Bid absorption: sell aggression {aggressive_sell:.2f} lots "
                    f"with mid range {mid_move:.2f} bps (≤{cfg.absorption_max_mid_move_bps:.2f})."
                ),
                payload={
                    "aggressive_sell_qty": aggressive_sell,
                    "aggressive_buy_qty": aggressive_buy,
                    "mid_move_bps": mid_move,
                    "window_ms": cfg.absorption_window_ms,
                },
            )
        )

    if (
        aggressive_buy >= cfg.absorption_min_aggressive_qty
        and mid_move <= cfg.absorption_max_mid_move_bps
        and aggressive_buy >= ratio_min * max(1e-9, aggressive_sell)
    ):
        out.append(
            OrderflowSignalCandidate(
                signal_type="trade_absorption_ask",
                metric_value=aggressive_buy,
                baseline_value=float(cfg.absorption_max_mid_move_bps),
                z_score=0.0,
                window_seconds=max(1, cfg.absorption_window_ms // 1000),
                summary=(
                    f"Ask absorption: buy aggression {aggressive_buy:.2f} lots "
                    f"with mid range {mid_move:.2f} bps (≤{cfg.absorption_max_mid_move_bps:.2f})."
                ),
                payload={
                    "aggressive_sell_qty": aggressive_sell,
                    "aggressive_buy_qty": aggressive_buy,
                    "mid_move_bps": mid_move,
                    "window_ms": cfg.absorption_window_ms,
                },
            )
        )
    return out


def _price_near_touch(
    trade_price: float,
    touch_price: float,
    mid: float,
    tolerance_bps: float,
) -> bool:
    if touch_price <= 0 or trade_price <= 0:
        return False
    tol = max(1e-9, tolerance_bps / 10_000.0 * max(mid, touch_price))
    return abs(trade_price - touch_price) <= tol


def update_iceberg_on_trade(
    *,
    trade_price: float,
    signed_qty: float,
    trade_ts: datetime,
    touch: TouchSnapshot | None,
    watch_bid: IcebergWatch | None,
    watch_ask: IcebergWatch | None,
    cfg: DetectorSettings,
) -> tuple[IcebergWatch | None, IcebergWatch | None]:
    if not cfg.iceberg_enabled or touch is None or signed_qty == 0:
        return watch_bid, watch_ask
    hit_window = timedelta(milliseconds=max(10, cfg.iceberg_hit_window_ms))
    mid = touch.mid

    if signed_qty < 0 and _price_near_touch(
        trade_price, touch.best_bid, mid, cfg.iceberg_price_tolerance_bps
    ):
        hit_qty = -signed_qty
        if watch_bid is None:
            watch_bid = IcebergWatch(
                touch_price=touch.best_bid,
                qty_before_hits=touch.best_bid_qty,
                accumulated_hit_qty=hit_qty,
                first_hit_ts=trade_ts,
                last_hit_ts=trade_ts,
            )
        else:
            if trade_ts - watch_bid.first_hit_ts > hit_window:
                watch_bid = IcebergWatch(
                    touch_price=touch.best_bid,
                    qty_before_hits=touch.best_bid_qty,
                    accumulated_hit_qty=hit_qty,
                    first_hit_ts=trade_ts,
                    last_hit_ts=trade_ts,
                )
            else:
                watch_bid.accumulated_hit_qty += hit_qty
                watch_bid.last_hit_ts = trade_ts
        return watch_bid, watch_ask

    if signed_qty > 0 and _price_near_touch(
        trade_price, touch.best_ask, mid, cfg.iceberg_price_tolerance_bps
    ):
        hit_qty = signed_qty
        if watch_ask is None:
            watch_ask = IcebergWatch(
                touch_price=touch.best_ask,
                qty_before_hits=touch.best_ask_qty,
                accumulated_hit_qty=hit_qty,
                first_hit_ts=trade_ts,
                last_hit_ts=trade_ts,
            )
        else:
            if trade_ts - watch_ask.first_hit_ts > hit_window:
                watch_ask = IcebergWatch(
                    touch_price=touch.best_ask,
                    qty_before_hits=touch.best_ask_qty,
                    accumulated_hit_qty=hit_qty,
                    first_hit_ts=trade_ts,
                    last_hit_ts=trade_ts,
                )
            else:
                watch_ask.accumulated_hit_qty += hit_qty
                watch_ask.last_hit_ts = trade_ts
        return watch_bid, watch_ask

    return watch_bid, watch_ask


def evaluate_iceberg_refill(
    *,
    watch: IcebergWatch | None,
    cur_touch_qty: float,
    cur_ts: datetime,
    side: str,
    cfg: DetectorSettings,
) -> tuple[OrderflowSignalCandidate | None, IcebergWatch | None]:
    if watch is None or not cfg.iceberg_enabled:
        return None, watch
    gap = (cur_ts - watch.last_hit_ts).total_seconds()
    if gap <= 0 or gap > cfg.iceberg_max_gap_seconds:
        return None, None
    if watch.accumulated_hit_qty < cfg.iceberg_min_hit_qty:
        return None, watch

    expected_after_hits = watch.qty_before_hits - watch.accumulated_hit_qty
    refill = cur_touch_qty - expected_after_hits
    min_refill = max(
        cfg.iceberg_min_refill_qty,
        cfg.iceberg_min_refill_ratio * watch.accumulated_hit_qty,
    )
    if refill < min_refill:
        return None, watch

    signal_type = (
        "iceberg_refill_bid" if side == "bid" else "iceberg_refill_ask"
    )
    return (
        OrderflowSignalCandidate(
            signal_type=signal_type,
            metric_value=float(refill),
            baseline_value=float(min_refill),
            z_score=0.0,
            window_seconds=int(cfg.iceberg_max_gap_seconds),
            summary=(
                f"Iceberg refill heuristic ({side}): touch qty restored by "
                f"{refill:.2f} lots after {watch.accumulated_hit_qty:.2f} hits."
            ),
            payload={
                "side": side,
                "touch_price": watch.touch_price,
                "qty_before_hits": watch.qty_before_hits,
                "accumulated_hit_qty": watch.accumulated_hit_qty,
                "cur_touch_qty": cur_touch_qty,
                "refill_qty": refill,
                "gap_seconds": gap,
            },
        ),
        None,
    )


def evaluate_spread_imbalance_regime(
    *,
    spread_bps: float,
    imbalance_abs: float,
    imbalance_ratio: float,
    cfg: DetectorSettings,
) -> list[OrderflowSignalCandidate]:
    if not cfg.spread_imbalance_regime_enabled:
        return []
    if spread_bps > cfg.regime_max_spread_bps:
        return []
    if imbalance_abs < cfg.regime_min_imbalance_abs:
        return []

    out: list[OrderflowSignalCandidate] = []
    if imbalance_ratio >= cfg.regime_long_threshold:
        out.append(
            OrderflowSignalCandidate(
                signal_type="spread_imbalance_regime_long",
                metric_value=imbalance_ratio,
                baseline_value=cfg.regime_long_threshold,
                z_score=0.0,
                window_seconds=cfg.orderbook_window_seconds,
                summary=(
                    f"Tight spread ({spread_bps:.2f} bps) with bid-heavy book "
                    f"(imbalance ratio {imbalance_ratio:.2f})."
                ),
                payload={
                    "spread_bps": spread_bps,
                    "imbalance_abs": imbalance_abs,
                    "imbalance_ratio": imbalance_ratio,
                    "regime_max_spread_bps": cfg.regime_max_spread_bps,
                },
            )
        )
    if imbalance_ratio <= cfg.regime_short_threshold:
        out.append(
            OrderflowSignalCandidate(
                signal_type="spread_imbalance_regime_short",
                metric_value=imbalance_ratio,
                baseline_value=cfg.regime_short_threshold,
                z_score=0.0,
                window_seconds=cfg.orderbook_window_seconds,
                summary=(
                    f"Tight spread ({spread_bps:.2f} bps) with ask-heavy book "
                    f"(imbalance ratio {imbalance_ratio:.2f})."
                ),
                payload={
                    "spread_bps": spread_bps,
                    "imbalance_abs": imbalance_abs,
                    "imbalance_ratio": imbalance_ratio,
                    "regime_max_spread_bps": cfg.regime_max_spread_bps,
                },
            )
        )
    return out
