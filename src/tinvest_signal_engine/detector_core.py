from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from statistics import fmean
from typing import Iterable
from uuid import uuid4

from .config import DetectorSettings
from .models import NormalizedEvent, TriggerSignal
from .serialization import quotation_to_float, utc_now


@dataclass
class TradePoint:
    ts: datetime
    quantity: float
    notional: float


@dataclass
class SignedTradePoint:
    ts: datetime
    signed_quantity: float


@dataclass
class PricePoint:
    ts: datetime
    price: float


@dataclass
class InstrumentState:
    trade_points: deque[TradePoint] = field(default_factory=deque)
    signed_trade_points: deque[SignedTradePoint] = field(default_factory=deque)
    price_points: deque[PricePoint] = field(default_factory=deque)
    volume_history: deque[float] = field(default_factory=deque)
    trade_count_history: deque[float] = field(default_factory=deque)
    return_history: deque[float] = field(default_factory=deque)
    spread_history: deque[float] = field(default_factory=deque)
    imbalance_history: deque[float] = field(default_factory=deque)
    last_sample_at: dict[str, datetime] = field(default_factory=dict)
    last_alert_at: dict[str, datetime] = field(default_factory=dict)
    last_active_at: dict[str, datetime] = field(default_factory=dict)
    last_trading_status: str | None = None
    last_orderbook_imbalance_ratio: float | None = None


class SignalDetector:
    def __init__(
        self,
        settings: DetectorSettings,
        per_instrument: dict[str, DetectorSettings] | None = None,
    ):
        self._default_settings = settings
        self._per_instrument = per_instrument or {}
        self._states: dict[str, InstrumentState] = defaultdict(InstrumentState)

    def _settings_for(self, instrument_id: str) -> DetectorSettings:
        return self._per_instrument.get(instrument_id, self._default_settings)

    def process(self, event: NormalizedEvent) -> list[TriggerSignal]:
        cfg = self._settings_for(event.instrument_id)
        state = self._states[event.instrument_id]
        if event.event_type == "trade":
            return self._process_trade_event(event, state, cfg)
        if event.event_type == "last_price":
            return self._process_last_price_event(event, state, cfg)
        if event.event_type == "orderbook":
            return self._process_orderbook_event(event, state, cfg)
        if event.event_type == "trading_status":
            return self._process_trading_status_event(event, state, cfg)
        return []

    def _process_trade_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        quantity = float(event.payload.get("quantity", 0.0))
        price = quotation_to_float(event.payload.get("price"))
        signals: list[TriggerSignal] = []
        if price is None or quantity <= 0:
            return signals

        state.trade_points.append(
            TradePoint(
                ts=event.source_time,
                quantity=quantity,
                notional=price * quantity,
            )
        )
        signed_qty = _signed_quantity_from_trade_payload(
            event.payload,
            quantity=quantity,
        )
        if signed_qty != 0:
            state.signed_trade_points.append(
                SignedTradePoint(
                    ts=event.source_time,
                    signed_quantity=signed_qty,
                )
            )
        state.price_points.append(PricePoint(ts=event.source_time, price=price))
        self._prune_trade_points(state, event.source_time, cfg)
        self._prune_price_points(state, event.source_time, cfg)

        signals.extend(self._sample_trade_windows(event, state, cfg))
        signals.extend(
            self._sample_price_move(event, state, cfg, current_price=price)
        )
        return signals

    def _process_last_price_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        price = quotation_to_float(event.payload.get("price"))
        if price is None or price <= 0:
            return []
        state.price_points.append(PricePoint(ts=event.source_time, price=price))
        self._prune_price_points(state, event.source_time, cfg)
        return self._sample_price_move(event, state, cfg, current_price=price)

    def _process_orderbook_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "orderbook", event.source_time, cfg):
            return []

        bids = event.payload.get("bids") or []
        asks = event.payload.get("asks") or []
        if not bids or not asks:
            return []

        best_bid = quotation_to_float((bids[0] or {}).get("price"))
        best_ask = quotation_to_float((asks[0] or {}).get("price"))
        if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
            return []

        top_bids_qty = sum(float(level.get("quantity", 0.0)) for level in bids[:3])
        top_asks_qty = sum(float(level.get("quantity", 0.0)) for level in asks[:3])
        total_qty = top_bids_qty + top_asks_qty
        if total_qty <= 0:
            return []

        mid = (best_bid + best_ask) / 2.0
        spread_bps = ((best_ask - best_bid) / mid) * 10_000
        imbalance_abs = abs((top_bids_qty - top_asks_qty) / total_qty)
        imbalance_ratio = top_bids_qty / total_qty
        signals: list[TriggerSignal] = []

        signals.extend(
            self._maybe_emit_from_history(
                event=event,
                state=state,
                cfg=cfg,
                signal_type="spread_widening",
                source_event_type="orderbook",
                history=state.spread_history,
                threshold=cfg.spread_zscore_threshold,
                value=spread_bps,
                baseline_label="spread",
                window_seconds=cfg.orderbook_window_seconds,
                summary_template=(
                    "{ticker} spread widened to {metric:.2f} bps "
                    "vs baseline {baseline:.2f} (z={z_score:.2f})."
                ),
            )
        )

        if imbalance_abs >= cfg.imbalance_absolute_threshold:
            signals.extend(
                self._maybe_emit_from_history(
                    event=event,
                    state=state,
                    cfg=cfg,
                    signal_type="orderbook_imbalance",
                    source_event_type="orderbook",
                    history=state.imbalance_history,
                    threshold=cfg.imbalance_zscore_threshold,
                    value=imbalance_abs,
                    baseline_label="imbalance",
                    window_seconds=cfg.orderbook_window_seconds,
                    summary_template=(
                        "{ticker} order book imbalance reached {metric:.2f} "
                        "vs baseline {baseline:.2f} (z={z_score:.2f})."
                    ),
                )
            )

        state.spread_history.append(spread_bps)
        state.imbalance_history.append(imbalance_abs)
        state.last_orderbook_imbalance_ratio = imbalance_ratio
        state.last_sample_at["orderbook"] = event.source_time
        self._trim_histories(state, cfg)
        signals.extend(self._evaluate_combo(event, state, cfg))
        return signals

    def _process_trading_status_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        status = str(event.payload.get("trading_status", "")).strip()
        if not status:
            return []
        previous_status = state.last_trading_status
        state.last_trading_status = status
        if previous_status is None or previous_status == status:
            return []
        if not self._is_alert_ready(
            state, "trading_status_changed", event.source_time, cfg
        ):
            return []
        state.last_alert_at["trading_status_changed"] = event.source_time
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="trading_status",
                signal_type="trading_status_changed",
                severity=2,
                metric_value=1.0,
                baseline_value=0.0,
                z_score=0.0,
                window_seconds=0,
                summary=(
                    f"{event.ticker} trading status changed "
                    f"from {previous_status} to {status}."
                ),
                payload={
                    "previous_status": previous_status,
                    "current_status": status,
                    "event_payload": event.payload,
                },
            )
        ]

    def _sample_trade_windows(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "trade_window", event.source_time, cfg):
            return []
        total_qty = sum(point.quantity for point in state.trade_points)
        trade_count = float(len(state.trade_points))
        signals: list[TriggerSignal] = []

        signals.extend(
            self._maybe_emit_from_history(
                event=event,
                state=state,
                cfg=cfg,
                signal_type="volume_spike",
                source_event_type="trade",
                history=state.volume_history,
                threshold=cfg.volume_zscore_threshold,
                value=total_qty,
                baseline_label="rolling volume",
                window_seconds=cfg.trade_window_seconds,
                summary_template=(
                    "{ticker} rolling volume hit {metric:.2f} lots "
                    "vs baseline {baseline:.2f} (z={z_score:.2f})."
                ),
            )
        )

        signals.extend(
            self._maybe_emit_from_history(
                event=event,
                state=state,
                cfg=cfg,
                signal_type="trade_rate_spike",
                source_event_type="trade",
                history=state.trade_count_history,
                threshold=cfg.trade_count_zscore_threshold,
                value=trade_count,
                baseline_label="trade count",
                window_seconds=cfg.trade_window_seconds,
                summary_template=(
                    "{ticker} trade count reached {metric:.2f} "
                    "vs baseline {baseline:.2f} (z={z_score:.2f})."
                ),
            )
        )

        state.volume_history.append(total_qty)
        state.trade_count_history.append(trade_count)
        state.last_sample_at["trade_window"] = event.source_time
        self._trim_histories(state, cfg)
        signals.extend(self._evaluate_combo(event, state, cfg))
        return signals

    def _sample_price_move(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        current_price: float,
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "price_window", event.source_time, cfg):
            return []
        if len(state.price_points) < 2:
            return []

        oldest_price = state.price_points[0].price
        if oldest_price <= 0:
            return []
        move_bps = abs((current_price - oldest_price) / oldest_price) * 10_000
        if (
            cfg.price_move_absolute_threshold_bps > 0
            and move_bps < cfg.price_move_absolute_threshold_bps
        ):
            return []
        signals = self._maybe_emit_from_history(
            event=event,
            state=state,
            cfg=cfg,
            signal_type="price_jump",
            source_event_type=event.event_type,
            history=state.return_history,
            threshold=cfg.price_return_zscore_threshold,
            value=move_bps,
            baseline_label="price move",
            window_seconds=cfg.price_window_seconds,
            summary_template=(
                "{ticker} moved {metric:.2f} bps in {window}s "
                "vs baseline {baseline:.2f} (z={z_score:.2f})."
            ),
        )

        state.return_history.append(move_bps)
        state.last_sample_at["price_window"] = event.source_time
        self._trim_histories(state, cfg)
        return signals

    def _evaluate_combo(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if not cfg.combo_enabled:
            return []
        if not self._should_sample(state, "combo", event.source_time, cfg):
            return []
        state.last_sample_at["combo"] = event.source_time

        freshness_cutoff = event.source_time - timedelta(
            seconds=cfg.combo_freshness_seconds
        )

        def is_fresh(signal_type: str) -> bool:
            ts = state.last_active_at.get(signal_type)
            return ts is not None and ts >= freshness_cutoff

        spread_active = is_fresh("spread_widening")
        tick_rate_active = is_fresh("trade_rate_spike")
        imbalance_active = is_fresh("orderbook_imbalance")
        imbalance_ratio = state.last_orderbook_imbalance_ratio
        imbalance_display = (
            f"{imbalance_ratio:.2f}" if imbalance_ratio is not None else "n/a"
        )
        signed_delta_qty = sum(
            point.signed_quantity for point in state.signed_trade_points
        )

        long_score = 0
        short_score = 0
        if spread_active:
            long_score += cfg.combo_spread_points
            short_score += cfg.combo_spread_points
        if tick_rate_active:
            long_score += cfg.combo_tick_rate_points
            short_score += cfg.combo_tick_rate_points
        if (
            imbalance_active
            and imbalance_ratio is not None
            and imbalance_ratio >= cfg.combo_imbalance_long_threshold
        ):
            long_score += cfg.combo_imbalance_points
        if (
            imbalance_active
            and imbalance_ratio is not None
            and imbalance_ratio <= cfg.combo_imbalance_short_threshold
        ):
            short_score += cfg.combo_imbalance_points
        if signed_delta_qty >= cfg.combo_delta_min_abs_qty:
            long_score += cfg.combo_delta_points
        if signed_delta_qty <= -cfg.combo_delta_min_abs_qty:
            short_score += cfg.combo_delta_points

        signals: list[TriggerSignal] = []
        if long_score >= cfg.combo_min_score and self._is_alert_ready_for(
            state=state,
            signal_type="microstructure_combo_long",
            now=event.source_time,
            cooldown_seconds=cfg.combo_alert_cooldown_seconds,
        ):
            state.last_alert_at["microstructure_combo_long"] = event.source_time
            signals.append(
                TriggerSignal(
                    signal_id=str(uuid4()),
                    detected_at=utc_now(),
                    instrument_id=event.instrument_id,
                    ticker=event.ticker,
                    class_code=event.class_code,
                    alias=event.alias,
                    source_event_type=event.event_type,
                    signal_type="microstructure_combo_long",
                    severity=3 if long_score >= cfg.combo_min_score + 2 else 2,
                    metric_value=float(long_score),
                    baseline_value=float(cfg.combo_min_score),
                    z_score=0.0,
                    window_seconds=cfg.combo_freshness_seconds,
                    summary=(
                        f"{event.ticker} combo-long score={long_score} "
                        f"(spread={spread_active}, imbalance={imbalance_display} "
                        f"tick_rate={tick_rate_active}, delta={signed_delta_qty:.2f})."
                    ),
                    payload={
                        "score": long_score,
                        "min_score": cfg.combo_min_score,
                        "spread_active": spread_active,
                        "tick_rate_active": tick_rate_active,
                        "imbalance_ratio": imbalance_ratio,
                        "signed_delta_qty": signed_delta_qty,
                    },
                )
            )
        if short_score >= cfg.combo_min_score and self._is_alert_ready_for(
            state=state,
            signal_type="microstructure_combo_short",
            now=event.source_time,
            cooldown_seconds=cfg.combo_alert_cooldown_seconds,
        ):
            state.last_alert_at["microstructure_combo_short"] = event.source_time
            signals.append(
                TriggerSignal(
                    signal_id=str(uuid4()),
                    detected_at=utc_now(),
                    instrument_id=event.instrument_id,
                    ticker=event.ticker,
                    class_code=event.class_code,
                    alias=event.alias,
                    source_event_type=event.event_type,
                    signal_type="microstructure_combo_short",
                    severity=3 if short_score >= cfg.combo_min_score + 2 else 2,
                    metric_value=float(short_score),
                    baseline_value=float(cfg.combo_min_score),
                    z_score=0.0,
                    window_seconds=cfg.combo_freshness_seconds,
                    summary=(
                        f"{event.ticker} combo-short score={short_score} "
                        f"(spread={spread_active}, imbalance={imbalance_display} "
                        f"tick_rate={tick_rate_active}, delta={signed_delta_qty:.2f})."
                    ),
                    payload={
                        "score": short_score,
                        "min_score": cfg.combo_min_score,
                        "spread_active": spread_active,
                        "tick_rate_active": tick_rate_active,
                        "imbalance_ratio": imbalance_ratio,
                        "signed_delta_qty": signed_delta_qty,
                    },
                )
            )
        return signals

    def _maybe_emit_from_history(
        self,
        *,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        signal_type: str,
        source_event_type: str,
        history: deque[float],
        threshold: float,
        value: float,
        baseline_label: str,
        window_seconds: int,
        summary_template: str,
    ) -> list[TriggerSignal]:
        if len(history) < cfg.min_baseline_points:
            return []
        baseline, z_score = _z_score(history, value)
        if z_score < threshold:
            return []
        state.last_active_at[signal_type] = event.source_time
        if not self._is_alert_ready(state, signal_type, event.source_time, cfg):
            return []
        state.last_alert_at[signal_type] = event.source_time
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type=source_event_type,
                signal_type=signal_type,
                severity=_severity_from_z_score(z_score),
                metric_value=value,
                baseline_value=baseline,
                z_score=z_score,
                window_seconds=window_seconds,
                summary=summary_template.format(
                    ticker=event.ticker,
                    metric=value,
                    baseline=baseline,
                    z_score=z_score,
                    window=window_seconds,
                ),
                payload={
                    "baseline_label": baseline_label,
                    "event_payload": event.payload,
                },
            )
        ]

    def _prune_trade_points(
        self, state: InstrumentState, now: datetime, cfg: DetectorSettings
    ) -> None:
        cutoff = now - timedelta(seconds=cfg.trade_window_seconds)
        while state.trade_points and state.trade_points[0].ts < cutoff:
            state.trade_points.popleft()
        while state.signed_trade_points and state.signed_trade_points[0].ts < cutoff:
            state.signed_trade_points.popleft()

    def _prune_price_points(
        self, state: InstrumentState, now: datetime, cfg: DetectorSettings
    ) -> None:
        cutoff = now - timedelta(seconds=cfg.price_window_seconds)
        while state.price_points and state.price_points[0].ts < cutoff:
            state.price_points.popleft()

    def _should_sample(
        self,
        state: InstrumentState,
        sample_key: str,
        now: datetime,
        cfg: DetectorSettings,
    ) -> bool:
        last_sample_at = state.last_sample_at.get(sample_key)
        if last_sample_at is None:
            return True
        elapsed = (now - last_sample_at).total_seconds()
        return elapsed >= cfg.sample_every_seconds

    def _is_alert_ready(
        self,
        state: InstrumentState,
        signal_type: str,
        now: datetime,
        cfg: DetectorSettings,
    ) -> bool:
        last_alert_at = state.last_alert_at.get(signal_type)
        if last_alert_at is None:
            return True
        elapsed = (now - last_alert_at).total_seconds()
        return elapsed >= cfg.alert_cooldown_seconds

    def _is_alert_ready_for(
        self,
        *,
        state: InstrumentState,
        signal_type: str,
        now: datetime,
        cooldown_seconds: int,
    ) -> bool:
        last_alert_at = state.last_alert_at.get(signal_type)
        if last_alert_at is None:
            return True
        elapsed = (now - last_alert_at).total_seconds()
        return elapsed >= cooldown_seconds

    def _trim_histories(self, state: InstrumentState, cfg: DetectorSettings) -> None:
        maxlen = cfg.baseline_points
        for history in (
            state.volume_history,
            state.trade_count_history,
            state.return_history,
            state.spread_history,
            state.imbalance_history,
        ):
            while len(history) > maxlen:
                history.popleft()


def _severity_from_z_score(z_score: float) -> int:
    if z_score >= 6:
        return 3
    if z_score >= 4:
        return 2
    return 1


def _z_score(history: Iterable[float], value: float) -> tuple[float, float]:
    samples = list(history)
    baseline = fmean(samples)
    variance = fmean((sample - baseline) ** 2 for sample in samples)
    std = math.sqrt(variance)
    if std <= 1e-12:
        return baseline, 999.0 if value > baseline else 0.0
    z_score = (value - baseline) / std
    return baseline, z_score


def _signed_quantity_from_trade_payload(
    payload: dict,
    *,
    quantity: float,
) -> float:
    raw_direction = payload.get("direction")
    if isinstance(raw_direction, str):
        text = raw_direction.strip().upper()
        if text in {"TRADE_DIRECTION_BUY", "BUY"}:
            return quantity
        if text in {"TRADE_DIRECTION_SELL", "SELL"}:
            return -quantity
        return 0.0
    if isinstance(raw_direction, int):
        if raw_direction == 1:
            return quantity
        if raw_direction == 2:
            return -quantity
    return 0.0

