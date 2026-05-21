"""Скользящие окна, z-score и генерация ``TriggerSignal`` (см. ``models``)."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from statistics import fmean
from typing import Any, Iterable
from uuid import uuid4

from .config import DetectorSettings
from .historical_baselines import HistoricalBaselineStore, SlotBaseline
from .models import NormalizedEvent, TriggerSignal
from .orderflow_signals import (
    IcebergWatch,
    OrderflowSignalCandidate,
    TouchSnapshot,
    evaluate_absorption,
    evaluate_iceberg_refill,
    evaluate_spread_imbalance_regime,
    evaluate_vpin_spike,
    evaluate_whale_print,
    feed_vpin_trade,
    severity_from_z_score,
    update_iceberg_on_trade,
)
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
class OrderBookDepthSnapshot:
    ts: datetime
    bid_l3_qty: float
    ask_l3_qty: float
    mid: float
    best_bid: float
    best_ask: float
    best_bid_qty: float = 0.0
    best_ask_qty: float = 0.0


@dataclass
class HistBarAccumulator:
    """Closed-bar OHLC/VWAP parts for historical seasonal comparison (UTC buckets)."""

    bucket_start: datetime | None = None
    sum_qty: float = 0.0
    n_trades: int = 0
    sum_pv: float = 0.0
    open_px: float | None = None
    high_px: float | None = None
    low_px: float | None = None


def _default_hist_bars() -> dict[str, HistBarAccumulator]:
    return {k: HistBarAccumulator() for k in ("1m", "5m", "15m")}


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
    # Любой алерт по инструменту (для alert_global_cooldown_seconds).
    last_any_alert_at: datetime | None = None
    last_active_at: dict[str, datetime] = field(default_factory=dict)
    last_trading_status: str | None = None
    last_orderbook_imbalance_ratio: float | None = None
    orderbook_depth_snapshots: deque[OrderBookDepthSnapshot] = field(
        default_factory=lambda: deque(maxlen=96)
    )
    microburst_ticks: deque[tuple[datetime, float]] = field(
        default_factory=lambda: deque(maxlen=512)
    )
    obi_delta_history: deque[float] = field(default_factory=deque)
    last_sampled_obi: float | None = None
    open_interest_history: deque[float] = field(default_factory=deque)
    candle_range_history: deque[float] = field(default_factory=deque)
    last_limit_order_available: bool | None = None
    last_market_order_available: bool | None = None
    hist_bars: dict[str, HistBarAccumulator] = field(
        default_factory=_default_hist_bars
    )
    vpin_current_bucket_buy: float = 0.0
    vpin_current_bucket_sell: float = 0.0
    vpin_bucket_imbalances: deque[float] = field(default_factory=deque)
    vpin_history: deque[float] = field(default_factory=deque)
    trade_size_history: deque[float] = field(default_factory=deque)
    iceberg_watch_bid: IcebergWatch | None = None
    iceberg_watch_ask: IcebergWatch | None = None
    last_touch_snapshot: TouchSnapshot | None = None


_GLOBAL_ALERT_STATE_KEY = "__global__"


class SignalDetector:
    """Обновляет состояние по инструменту и возвращает список новых сигналов."""

    def __init__(
        self,
        settings: DetectorSettings,
        per_instrument: dict[str, DetectorSettings] | None = None,
        *,
        lead_lag_pairs: tuple[tuple[str, str], ...] = (),
        historical_store: HistoricalBaselineStore | None = None,
    ):
        self._default_settings = settings
        self._per_instrument = per_instrument or {}
        self._states: dict[str, InstrumentState] = defaultdict(InstrumentState)
        self._lead_lag_pairs = lead_lag_pairs
        self._historical_store = historical_store
        self._mid_track: dict[str, deque[tuple[datetime, float]]] = defaultdict(
            lambda: deque(maxlen=4000)
        )
        # Последние unary-снимки по instrument_id (не сериализуем в Redis).
        self._unary_context: dict[str, dict[str, Any]] = {}

    def _settings_for(self, instrument_id: str) -> DetectorSettings:
        return self._per_instrument.get(instrument_id, self._default_settings)

    def process(self, event: NormalizedEvent) -> list[TriggerSignal]:
        cfg = self._settings_for(event.instrument_id)
        state = self._states[event.instrument_id]
        if event.event_type == "trade":
            signals = self._process_trade_event(event, state, cfg)
        elif event.event_type == "last_price":
            signals = self._process_last_price_event(event, state, cfg)
        elif event.event_type == "orderbook":
            signals = self._process_orderbook_event(event, state, cfg)
        elif event.event_type == "trading_status":
            signals = self._process_trading_status_event(event, state, cfg)
        elif event.event_type == "open_interest":
            signals = self._process_open_interest_event(event, state, cfg)
        elif event.event_type == "candle":
            signals = self._process_candle_event(event, state, cfg)
        elif event.event_type in {"market_values", "tech_analysis"}:
            signals = self._process_unary_snapshot_event(event)
        else:
            signals = []
        signals = list(signals)
        signals.extend(self._maybe_lead_lag(event, cfg))
        return signals

    @staticmethod
    def _truncate_unary_payload(payload: dict[str, Any]) -> dict[str, Any]:
        """Ограничивает размер unary-payload для Postgres/Kafka."""
        p = deepcopy(payload)
        resp = p.get("response")
        if isinstance(resp, dict):
            ind = resp.get("technical_indicators")
            if isinstance(ind, list) and len(ind) > 80:
                resp = {**resp, "technical_indicators": ind[:80], "_truncated": True}
                p["response"] = resp
        vals = p.get("values")
        if isinstance(vals, list) and len(vals) > 40:
            p["values"] = vals[:40]
            p["_values_truncated"] = True
        return p

    def _process_unary_snapshot_event(
        self, event: NormalizedEvent
    ) -> list[TriggerSignal]:
        slot = self._unary_context.setdefault(event.instrument_id, {})
        slot[event.event_type] = {
            "event_id": event.event_id,
            "source_time": event.source_time.isoformat(),
            "received_at": event.received_at.isoformat(),
            "payload": self._truncate_unary_payload(dict(event.payload)),
        }
        return []

    def enrich_signals_with_unary(
        self, signals: list[TriggerSignal]
    ) -> list[TriggerSignal]:
        """Добавляет ``unary_context`` в payload сигналов (если включено в настройках)."""
        out: list[TriggerSignal] = []
        for s in signals:
            cfg = self._settings_for(s.instrument_id)
            if not cfg.attach_unary_context_to_signals:
                out.append(s)
                continue
            ctx = self._unary_context.get(s.instrument_id)
            if not ctx:
                out.append(s)
                continue
            out.append(
                replace(s, payload={**s.payload, "unary_context": dict(ctx)})
            )
        return out

    def export_alert_state(self) -> dict[str, dict[str, str]]:
        """Сериализует ``last_alert_at`` по инструментам (ISO 8601) для Redis/файла."""
        out: dict[str, dict[str, str]] = {}
        for iid, st in self._states.items():
            if st.last_alert_at or st.last_any_alert_at is not None:
                payload = {
                    k: v.isoformat() for k, v in st.last_alert_at.items()
                }
                if st.last_any_alert_at is not None:
                    payload[_GLOBAL_ALERT_STATE_KEY] = (
                        st.last_any_alert_at.isoformat()
                    )
                out[iid] = payload
        return out

    def hydrate_alert_state(self, data: dict[str, dict[str, str]]) -> None:
        """Восстанавливает cooldown из :meth:`export_alert_state`; берёт более позднее время."""
        for iid, type_map in data.items():
            state = self._states[iid]
            for sig_type, iso in type_map.items():
                try:
                    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue
                if sig_type == _GLOBAL_ALERT_STATE_KEY:
                    prev_any = state.last_any_alert_at
                    if prev_any is None or dt > prev_any:
                        state.last_any_alert_at = dt
                    continue
                prev = state.last_alert_at.get(sig_type)
                if prev is None or dt > prev:
                    state.last_alert_at[sig_type] = dt

    @staticmethod
    def _record_alert_sent(
        state: InstrumentState, signal_type: str, now: datetime
    ) -> None:
        state.last_alert_at[signal_type] = now
        prev_any = state.last_any_alert_at
        if prev_any is None or now > prev_any:
            state.last_any_alert_at = now

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
        self._push_mid(event.instrument_id, event.source_time, price, cfg)

        if signed_qty != 0:
            signals.extend(
                self._maybe_emit_trade_burst(
                    event, state, cfg, signed_qty=signed_qty
                )
            )

        signals.extend(self._process_orderflow_on_trade(event, state, cfg, price, quantity, signed_qty))
        signals.extend(self._sample_trade_windows(event, state, cfg))
        signals.extend(
            self._sample_price_move(event, state, cfg, current_price=price)
        )
        if (
            cfg.historical_baseline_enabled
            and self._historical_store is not None
            and self._historical_store.enabled
        ):
            signals.extend(
                self._process_historical_trade_buckets(event, state, cfg)
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
        self._push_mid(event.instrument_id, event.source_time, price, cfg)
        return self._sample_price_move(event, state, cfg, current_price=price)

    def _process_orderbook_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        bids = event.payload.get("bids") or []
        asks = event.payload.get("asks") or []
        if not bids or not asks:
            return []

        best_bid = quotation_to_float((bids[0] or {}).get("price"))
        best_ask = quotation_to_float((asks[0] or {}).get("price"))
        if best_bid is None or best_ask is None or best_bid <= 0 or best_ask <= 0:
            return []

        depth = max(1, min(20, cfg.order_book_depth_levels))
        top_bids_qty = _sum_orderbook_depth(bids, depth)
        top_asks_qty = _sum_orderbook_depth(asks, depth)
        best_bid_qty = float((bids[0] or {}).get("quantity", 0.0) or 0.0)
        best_ask_qty = float((asks[0] or {}).get("quantity", 0.0) or 0.0)
        total_qty = top_bids_qty + top_asks_qty
        if total_qty <= 0:
            return []

        mid = (best_bid + best_ask) / 2.0
        self._push_mid(event.instrument_id, event.source_time, mid, cfg)
        state.last_touch_snapshot = TouchSnapshot(
            ts=event.source_time,
            best_bid=best_bid,
            best_ask=best_ask,
            best_bid_qty=best_bid_qty,
            best_ask_qty=best_ask_qty,
            mid=mid,
        )
        self._record_orderbook_snapshot(
            state,
            OrderBookDepthSnapshot(
                ts=event.source_time,
                bid_l3_qty=top_bids_qty,
                ask_l3_qty=top_asks_qty,
                mid=mid,
                best_bid=best_bid,
                best_ask=best_ask,
                best_bid_qty=best_bid_qty,
                best_ask_qty=best_ask_qty,
            ),
            cfg,
        )
        signals: list[TriggerSignal] = list(
            self._maybe_emit_orderbook_spoofing(event, state, cfg)
        )

        if cfg.signal_orderbook_inconsistent and (
            event.payload.get("is_consistent") is False
        ):
            signals.extend(
                self._emit_orderbook_inconsistent(event, state, cfg, mid=mid)
            )

        if cfg.limit_band_warning_bps > 0:
            signals.extend(
                self._maybe_emit_price_near_limit_band(
                    event, state, cfg, mid=mid
                )
            )

        signals.extend(
            self._process_iceberg_on_orderbook(
                event,
                state,
                cfg,
                best_bid_qty=best_bid_qty,
                best_ask_qty=best_ask_qty,
            )
        )

        if not self._should_sample(state, "orderbook", event.source_time, cfg):
            return signals

        spread_bps = ((best_ask - best_bid) / mid) * 10_000
        imbalance_abs = abs((top_bids_qty - top_asks_qty) / total_qty)
        imbalance_ratio = top_bids_qty / total_qty

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

        if cfg.obi_dynamics_enabled:
            obi = (top_bids_qty - top_asks_qty) / total_qty
            if state.last_sampled_obi is not None:
                delta_obi = obi - state.last_sampled_obi
                if abs(delta_obi) >= cfg.obi_delta_absolute_threshold:
                    tmpl = (
                        "{ticker} L"
                        + str(depth)
                        + " OBI jump |Δ|={metric:.3f} "
                        "vs baseline {baseline:.3f} (z={z_score:.2f})."
                    )
                    signals.extend(
                        self._maybe_emit_from_history(
                            event=event,
                            state=state,
                            cfg=cfg,
                            signal_type="obi_dynamics",
                            source_event_type="orderbook",
                            history=state.obi_delta_history,
                            threshold=cfg.obi_delta_zscore_threshold,
                            value=abs(delta_obi),
                            baseline_label="obi delta",
                            window_seconds=cfg.orderbook_window_seconds,
                            summary_template=tmpl,
                        )
                    )
                state.obi_delta_history.append(abs(delta_obi))
            state.last_sampled_obi = obi

        state.last_sample_at["orderbook"] = event.source_time
        self._trim_histories(state, cfg)
        signals.extend(
            self._process_regime_on_orderbook(
                event,
                state,
                cfg,
                spread_bps=spread_bps,
                imbalance_abs=imbalance_abs,
                imbalance_ratio=imbalance_ratio,
            )
        )
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
        signals: list[TriggerSignal] = []
        if cfg.track_market_access_flags:
            signals.extend(
                self._maybe_emit_market_access_change(event, state, cfg)
            )

        previous_status = state.last_trading_status
        state.last_trading_status = status
        if previous_status is None or previous_status == status:
            return signals
        if not self._is_alert_ready(
            state, "trading_status_changed", event.source_time, cfg
        ):
            return signals
        self._record_alert_sent(
            state, "trading_status_changed", event.source_time
        )
        signals.append(
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
        )
        return signals

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

        imb_long_zone = (
            imbalance_active
            and imbalance_ratio is not None
            and imbalance_ratio >= cfg.combo_imbalance_long_threshold
        )
        imb_short_zone = (
            imbalance_active
            and imbalance_ratio is not None
            and imbalance_ratio <= cfg.combo_imbalance_short_threshold
        )
        delta_long_ok = signed_delta_qty >= cfg.combo_delta_min_abs_qty
        delta_short_ok = signed_delta_qty <= -cfg.combo_delta_min_abs_qty
        min_score_eff = self._effective_combo_min_score(cfg)
        combo_detail = {
            "freshness_seconds": cfg.combo_freshness_seconds,
            "flags": {
                "spread_active": spread_active,
                "tick_rate_active": tick_rate_active,
                "imbalance_active": imbalance_active,
                "imbalance_long_zone": imb_long_zone,
                "imbalance_short_zone": imb_short_zone,
                "delta_long": delta_long_ok,
                "delta_short": delta_short_ok,
            },
            "points_awarded": {
                "spread": cfg.combo_spread_points if spread_active else 0,
                "tick_rate": cfg.combo_tick_rate_points if tick_rate_active else 0,
                "imbalance_long": cfg.combo_imbalance_points if imb_long_zone else 0,
                "imbalance_short": cfg.combo_imbalance_points if imb_short_zone else 0,
                "delta_long": cfg.combo_delta_points if delta_long_ok else 0,
                "delta_short": cfg.combo_delta_points if delta_short_ok else 0,
            },
            "scores": {"long": long_score, "short": short_score},
            "thresholds": {
                "min_score": cfg.combo_min_score,
                "effective_min_score": min_score_eff,
                "microstructure_secondary_mode": cfg.microstructure_secondary_mode,
                "imbalance_long_ge": cfg.combo_imbalance_long_threshold,
                "imbalance_short_le": cfg.combo_imbalance_short_threshold,
                "delta_min_abs_qty": cfg.combo_delta_min_abs_qty,
            },
            "imbalance_ratio": imbalance_ratio,
            "signed_delta_qty": signed_delta_qty,
        }

        signals: list[TriggerSignal] = []
        if long_score >= min_score_eff and self._is_alert_ready_for(
            state=state,
            signal_type="microstructure_combo_long",
            now=event.source_time,
            cooldown_seconds=cfg.combo_alert_cooldown_seconds,
            cfg=cfg,
        ):
            self._record_alert_sent(
                state, "microstructure_combo_long", event.source_time
            )
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
                    severity=3 if long_score >= min_score_eff + 2 else 2,
                    metric_value=float(long_score),
                    baseline_value=float(min_score_eff),
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
                        "effective_min_score": min_score_eff,
                        "spread_active": spread_active,
                        "tick_rate_active": tick_rate_active,
                        "imbalance_ratio": imbalance_ratio,
                        "signed_delta_qty": signed_delta_qty,
                        "combo_detail": combo_detail,
                    },
                )
            )
        if short_score >= min_score_eff and self._is_alert_ready_for(
            state=state,
            signal_type="microstructure_combo_short",
            now=event.source_time,
            cooldown_seconds=cfg.combo_alert_cooldown_seconds,
            cfg=cfg,
        ):
            self._record_alert_sent(
                state, "microstructure_combo_short", event.source_time
            )
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
                    severity=3 if short_score >= min_score_eff + 2 else 2,
                    metric_value=float(short_score),
                    baseline_value=float(min_score_eff),
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
                        "effective_min_score": min_score_eff,
                        "spread_active": spread_active,
                        "tick_rate_active": tick_rate_active,
                        "imbalance_ratio": imbalance_ratio,
                        "signed_delta_qty": signed_delta_qty,
                        "combo_detail": combo_detail,
                    },
                )
            )
        return signals

    def _record_orderbook_snapshot(
        self,
        state: InstrumentState,
        snapshot: OrderBookDepthSnapshot,
        cfg: DetectorSettings,
    ) -> None:
        dq = state.orderbook_depth_snapshots
        cutoff = snapshot.ts - timedelta(seconds=cfg.spoofing_lookback_seconds)
        while dq and dq[0].ts < cutoff:
            dq.popleft()
        dq.append(snapshot)

    def _maybe_emit_orderbook_spoofing(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if not cfg.spoofing_enabled:
            return []
        dq = state.orderbook_depth_snapshots
        if len(dq) < 2:
            return []
        prev, cur = dq[-2], dq[-1]
        gap = (cur.ts - prev.ts).total_seconds()
        if gap <= 0 or gap > cfg.spoofing_max_gap_seconds:
            return []
        signals: list[TriggerSignal] = []
        signals.extend(
            self._spoofing_pull_signal(
                event=event,
                state=state,
                cfg=cfg,
                prev=prev,
                cur=cur,
                side="bid",
            )
        )
        signals.extend(
            self._spoofing_pull_signal(
                event=event,
                state=state,
                cfg=cfg,
                prev=prev,
                cur=cur,
                side="ask",
            )
        )
        return signals

    def _spoofing_pull_signal(
        self,
        *,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        prev: OrderBookDepthSnapshot,
        cur: OrderBookDepthSnapshot,
        side: str,
    ) -> list[TriggerSignal]:
        if side == "bid":
            prev_wall = prev.bid_l3_qty
            prev_other = prev.ask_l3_qty
            cur_wall = cur.bid_l3_qty
            signal_type = "orderbook_spoofing_bid_pull"
            wall_label = "bid"
            other_label = "ask"
        else:
            prev_wall = prev.ask_l3_qty
            prev_other = prev.bid_l3_qty
            cur_wall = cur.ask_l3_qty
            signal_type = "orderbook_spoofing_ask_pull"
            wall_label = "ask"
            other_label = "bid"

        if prev_wall < cfg.spoofing_min_wall_qty:
            return []
        if prev_wall < cfg.spoofing_wall_ratio * max(1e-12, prev_other):
            return []
        drop = (prev_wall - cur_wall) / prev_wall
        if prev.mid <= 0:
            return []
        mid_move_bps = abs(cur.mid - prev.mid) / prev.mid * 10_000
        if drop < cfg.spoofing_qty_drop_ratio:
            return []
        if mid_move_bps > cfg.spoofing_max_mid_move_bps:
            return []
        if not self._is_alert_ready(state, signal_type, event.source_time, cfg):
            return []
        self._record_alert_sent(state, signal_type, event.source_time)
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="orderbook",
                signal_type=signal_type,
                severity=2,
                metric_value=float(drop),
                baseline_value=float(cfg.spoofing_qty_drop_ratio),
                z_score=0.0,
                window_seconds=int(cfg.spoofing_max_gap_seconds),
                summary=(
                    f"{event.ticker} {wall_label} wall thinned by {drop * 100:.1f}% "
                    f"within {cfg.spoofing_max_gap_seconds:.1f}s while mid moved "
                    f"{mid_move_bps:.2f} bps (watch {other_label}-side liquidity)."
                ),
                payload={
                    "wall_side": wall_label,
                    "prev_wall_qty": prev_wall,
                    "cur_wall_qty": cur_wall,
                    "prev_other_qty": prev_other,
                    "cur_other_qty": (
                        cur.ask_l3_qty if side == "bid" else cur.bid_l3_qty
                    ),
                    "mid_move_bps": mid_move_bps,
                    "gap_seconds": (cur.ts - prev.ts).total_seconds(),
                    "event_payload": event.payload,
                },
            )
        ]

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
        eff_threshold = float(threshold) * self._micro_threshold_multiplier(cfg)
        if z_score < eff_threshold:
            return []
        if cfg.min_relative_metric_excursion > 0.0:
            b = float(baseline)
            if abs(b) >= 1e-9:
                rel = abs(float(value) - b) / max(abs(b), 1e-12)
                if rel < cfg.min_relative_metric_excursion:
                    return []
        state.last_active_at[signal_type] = event.source_time
        if not self._is_alert_ready(state, signal_type, event.source_time, cfg):
            return []
        self._record_alert_sent(state, signal_type, event.source_time)
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

    def _global_cooldown_blocks(
        self, state: InstrumentState, now: datetime, cfg: DetectorSettings
    ) -> bool:
        global_cd = int(cfg.alert_global_cooldown_seconds)
        if global_cd <= 0:
            return False
        last_any = state.last_any_alert_at
        if last_any is None:
            return False
        return (now - last_any).total_seconds() < global_cd

    def _is_alert_ready(
        self,
        state: InstrumentState,
        signal_type: str,
        now: datetime,
        cfg: DetectorSettings,
    ) -> bool:
        if self._global_cooldown_blocks(state, now, cfg):
            return False
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
        cfg: DetectorSettings,
    ) -> bool:
        if self._global_cooldown_blocks(state, now, cfg):
            return False
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
            state.obi_delta_history,
            state.open_interest_history,
            state.candle_range_history,
            state.vpin_history,
            state.trade_size_history,
            state.vpin_bucket_imbalances,
        ):
            while len(history) > maxlen:
                history.popleft()

    def _push_mid(
        self,
        instrument_id: str,
        ts: datetime,
        px: float,
        cfg: DetectorSettings,
    ) -> None:
        if px <= 0 or not math.isfinite(px):
            return
        dq = self._mid_track[instrument_id]
        dq.append((ts, float(px)))
        cutoff = ts - timedelta(seconds=max(5, cfg.lead_lag_window_seconds))
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _emit_orderflow_candidates(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        candidates: Iterable[OrderflowSignalCandidate],
        *,
        source_event_type: str,
        cooldown_seconds: int | None = None,
    ) -> list[TriggerSignal]:
        signals: list[TriggerSignal] = []
        for cand in candidates:
            cooldown = (
                cfg.alert_cooldown_seconds
                if cooldown_seconds is None
                else cooldown_seconds
            )
            if not self._is_alert_ready_for(
                state=state,
                signal_type=cand.signal_type,
                now=event.source_time,
                cooldown_seconds=cooldown,
                cfg=cfg,
            ):
                continue
            self._record_alert_sent(
                state, cand.signal_type, event.source_time
            )
            state.last_active_at[cand.signal_type] = event.source_time
            z = float(cand.z_score)
            severity = (
                cand.severity
                if cand.severity is not None
                else (severity_from_z_score(z) if z > 0 else 2)
            )
            summary = cand.summary.format(
                ticker=event.ticker,
                metric=cand.metric_value,
                baseline=cand.baseline_value,
                z_score=z,
                window=cand.window_seconds,
            )
            signals.append(
                TriggerSignal(
                    signal_id=str(uuid4()),
                    detected_at=utc_now(),
                    instrument_id=event.instrument_id,
                    ticker=event.ticker,
                    class_code=event.class_code,
                    alias=event.alias,
                    source_event_type=source_event_type,
                    signal_type=cand.signal_type,
                    severity=severity,
                    metric_value=cand.metric_value,
                    baseline_value=cand.baseline_value,
                    z_score=z,
                    window_seconds=cand.window_seconds,
                    summary=summary,
                    payload={**cand.payload, "event_payload": event.payload},
                )
            )
        return signals

    def _process_orderflow_on_trade(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        price: float,
        quantity: float,
        signed_qty: float,
    ) -> list[TriggerSignal]:
        signals: list[TriggerSignal] = []
        buy_qty = quantity if signed_qty > 0 else 0.0
        sell_qty = quantity if signed_qty < 0 else 0.0
        if signed_qty == 0 and quantity > 0:
            raw = event.payload.get("direction")
            if isinstance(raw, str) and raw.strip().upper() in {
                "TRADE_DIRECTION_BUY",
                "BUY",
            }:
                buy_qty = quantity
            elif isinstance(raw, str) and raw.strip().upper() in {
                "TRADE_DIRECTION_SELL",
                "SELL",
            }:
                sell_qty = quantity

        bucket_buy, bucket_sell, closed_buckets, current_vpin = feed_vpin_trade(
            buy_qty=buy_qty,
            sell_qty=sell_qty,
            bucket_buy=state.vpin_current_bucket_buy,
            bucket_sell=state.vpin_current_bucket_sell,
            bucket_target=cfg.vpin_bucket_volume_lots,
            bucket_imbalances=state.vpin_bucket_imbalances,
            lookback_buckets=cfg.vpin_lookback_buckets,
        )
        state.vpin_current_bucket_buy = bucket_buy
        state.vpin_current_bucket_sell = bucket_sell
        if closed_buckets and current_vpin is not None:
            vpin_cand = evaluate_vpin_spike(
                vpin_history=state.vpin_history,
                current_vpin=current_vpin,
                cfg=cfg,
                min_buckets=cfg.vpin_min_buckets_before_emit,
            )
            if vpin_cand is not None:
                vpin_cand.summary = (
                    f"{event.ticker} VPIN {current_vpin:.4f} vs baseline "
                    f"{{baseline:.4f}} (z={{z_score:.2f}})."
                )
                signals.extend(
                    self._emit_orderflow_candidates(
                        event, state, cfg, [vpin_cand], source_event_type="trade"
                    )
                )
            state.vpin_history.append(current_vpin)

        whale_cand = evaluate_whale_print(
            trade_size=quantity,
            trade_size_history=state.trade_size_history,
            cfg=cfg,
        )
        if whale_cand is not None:
            whale_cand.summary = (
                f"{event.ticker} large print {{metric:.2f}} lots "
                f"vs baseline {{baseline:.2f}} (z={{z_score:.2f}})."
            )
            signals.extend(
                self._emit_orderflow_candidates(
                    event, state, cfg, [whale_cand], source_event_type="trade"
                )
            )
        state.trade_size_history.append(quantity)

        mid_ring = self._mid_track[event.instrument_id]
        signed_iter = ((p.ts, p.signed_quantity) for p in state.signed_trade_points)
        absorption = evaluate_absorption(
            signed_points=signed_iter,
            mid_ring=mid_ring,
            now=event.source_time,
            cfg=cfg,
        )
        for cand in absorption:
            cand.summary = cand.summary.replace(
                "Bid absorption", f"{event.ticker} bid absorption"
            ).replace("Ask absorption", f"{event.ticker} ask absorption")
        signals.extend(
            self._emit_orderflow_candidates(
                event, state, cfg, absorption, source_event_type="trade"
            )
        )

        if signed_qty != 0:
            watch_bid, watch_ask = update_iceberg_on_trade(
                trade_price=price,
                signed_qty=signed_qty,
                trade_ts=event.source_time,
                touch=state.last_touch_snapshot,
                watch_bid=state.iceberg_watch_bid,
                watch_ask=state.iceberg_watch_ask,
                cfg=cfg,
            )
            state.iceberg_watch_bid = watch_bid
            state.iceberg_watch_ask = watch_ask

        self._trim_histories(state, cfg)
        return signals

    def _process_iceberg_on_orderbook(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        best_bid_qty: float,
        best_ask_qty: float,
    ) -> list[TriggerSignal]:
        bid_cand, state.iceberg_watch_bid = evaluate_iceberg_refill(
            watch=state.iceberg_watch_bid,
            cur_touch_qty=best_bid_qty,
            cur_ts=event.source_time,
            side="bid",
            cfg=cfg,
        )
        ask_cand, state.iceberg_watch_ask = evaluate_iceberg_refill(
            watch=state.iceberg_watch_ask,
            cur_touch_qty=best_ask_qty,
            cur_ts=event.source_time,
            side="ask",
            cfg=cfg,
        )
        iceberg: list[OrderflowSignalCandidate] = []
        if bid_cand is not None:
            bid_cand.summary = (
                f"{event.ticker} iceberg refill (bid): +{bid_cand.metric_value:.2f} lots."
            )
            iceberg.append(bid_cand)
        if ask_cand is not None:
            ask_cand.summary = (
                f"{event.ticker} iceberg refill (ask): +{ask_cand.metric_value:.2f} lots."
            )
            iceberg.append(ask_cand)
        return self._emit_orderflow_candidates(
            event, state, cfg, iceberg, source_event_type="orderbook"
        )

    def _process_regime_on_orderbook(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        spread_bps: float,
        imbalance_abs: float,
        imbalance_ratio: float,
    ) -> list[TriggerSignal]:
        regime = evaluate_spread_imbalance_regime(
            spread_bps=spread_bps,
            imbalance_abs=imbalance_abs,
            imbalance_ratio=imbalance_ratio,
            cfg=cfg,
        )
        for cand in regime:
            cand.summary = cand.summary.replace(
                "Tight spread", f"{event.ticker} tight spread"
            )
        return self._emit_orderflow_candidates(
            event,
            state,
            cfg,
            regime,
            source_event_type="orderbook",
            cooldown_seconds=cfg.regime_alert_cooldown_seconds,
        )

    def _maybe_emit_trade_burst(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        signed_qty: float,
    ) -> list[TriggerSignal]:
        if not cfg.trade_burst_enabled or signed_qty == 0.0:
            return []
        dq = state.microburst_ticks
        window = timedelta(milliseconds=max(10, cfg.trade_burst_window_ms))
        dq.append((event.source_time, signed_qty))
        while dq and event.source_time - dq[0][0] > window:
            dq.popleft()
        if len(dq) < cfg.trade_burst_min_trades:
            return []
        signs = [s for _, s in dq if s != 0.0]
        if not signs:
            return []
        same = all(s > 0 for s in signs) or all(s < 0 for s in signs)
        if not same:
            return []
        total_abs = sum(abs(s) for s in signs)
        if total_abs < cfg.trade_burst_min_abs_qty:
            return []
        if not self._is_alert_ready(
            state, "aggressive_trade_burst", event.source_time, cfg
        ):
            return []
        self._record_alert_sent(
            state, "aggressive_trade_burst", event.source_time
        )
        direction = "buy" if signs[0] > 0 else "sell"
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="trade",
                signal_type="aggressive_trade_burst",
                severity=2,
                metric_value=float(len(dq)),
                baseline_value=float(cfg.trade_burst_min_trades),
                z_score=0.0,
                window_seconds=cfg.trade_burst_window_ms // 1000 or 1,
                summary=(
                    f"{event.ticker} {direction} burst: {len(dq)} prints in "
                    f"{cfg.trade_burst_window_ms}ms, |Σqty|={total_abs:.2f}."
                ),
                payload={
                    "direction": direction,
                    "print_count": len(dq),
                    "window_ms": cfg.trade_burst_window_ms,
                    "abs_qty_sum": total_abs,
                    "lot": event.lot,
                },
            )
        ]

    def _emit_orderbook_inconsistent(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        mid: float,
    ) -> list[TriggerSignal]:
        if not self._is_alert_ready(
            state, "orderbook_snapshot_inconsistent", event.source_time, cfg
        ):
            return []
        self._record_alert_sent(
            state, "orderbook_snapshot_inconsistent", event.source_time
        )
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="orderbook",
                signal_type="orderbook_snapshot_inconsistent",
                severity=2,
                metric_value=float(mid),
                baseline_value=0.0,
                z_score=0.0,
                window_seconds=cfg.orderbook_window_seconds,
                summary=(
                    f"{event.ticker} order book snapshot marked inconsistent "
                    f"(mid={mid:.6g})."
                ),
                payload={"mid": mid, "event_payload": event.payload},
            )
        ]

    def _maybe_emit_price_near_limit_band(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        *,
        mid: float,
    ) -> list[TriggerSignal]:
        lim_up = quotation_to_float(event.payload.get("limit_up"))
        lim_dn = quotation_to_float(event.payload.get("limit_down"))
        if (
            lim_up is None
            or lim_dn is None
            or lim_up <= 0
            or lim_dn <= 0
            or mid <= 0
        ):
            return []
        dist_up = (lim_up - mid) / mid * 10_000.0
        dist_dn = (mid - lim_dn) / mid * 10_000.0
        nearest = min(dist_up, dist_dn)
        if nearest > cfg.limit_band_warning_bps:
            return []
        if not self._is_alert_ready(
            state, "price_near_limit_band", event.source_time, cfg
        ):
            return []
        self._record_alert_sent(
            state, "price_near_limit_band", event.source_time
        )
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="orderbook",
                signal_type="price_near_limit_band",
                severity=2 if nearest <= cfg.limit_band_warning_bps * 0.5 else 1,
                metric_value=float(nearest),
                baseline_value=float(cfg.limit_band_warning_bps),
                z_score=0.0,
                window_seconds=cfg.orderbook_window_seconds,
                summary=(
                    f"{event.ticker} mid within {nearest:.1f} bps of daily "
                    f"limit band (warn≤{cfg.limit_band_warning_bps:.0f} bps)."
                ),
                payload={
                    "nearest_limit_distance_bps": nearest,
                    "limit_up": lim_up,
                    "limit_down": lim_dn,
                    "mid": mid,
                    "event_payload": event.payload,
                },
            )
        ]

    def _maybe_emit_market_access_change(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        lo = _optional_bool(event.payload.get("limit_order_available_flag"))
        mo = _optional_bool(event.payload.get("market_order_available_flag"))
        if lo is None and mo is None:
            return []
        parts: list[str] = []
        if lo is not None:
            prev = state.last_limit_order_available
            if prev is not None and prev != lo:
                parts.append(f"limit_orders {'on' if lo else 'off'}")
            state.last_limit_order_available = lo
        if mo is not None:
            prev_m = state.last_market_order_available
            if prev_m is not None and prev_m != mo:
                parts.append(f"market_orders {'on' if mo else 'off'}")
            state.last_market_order_available = mo
        if not parts:
            return []
        if not self._is_alert_ready(
            state, "market_access_changed", event.source_time, cfg
        ):
            return []
        self._record_alert_sent(
            state, "market_access_changed", event.source_time
        )
        detail = "; ".join(parts)
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="trading_status",
                signal_type="market_access_changed",
                severity=2,
                metric_value=1.0,
                baseline_value=0.0,
                z_score=0.0,
                window_seconds=0,
                summary=f"{event.ticker} market access changed: {detail}.",
                payload={
                    "changes": parts,
                    "limit_order_available_flag": lo,
                    "market_order_available_flag": mo,
                    "event_payload": event.payload,
                },
            )
        ]

    def _process_open_interest_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if cfg.open_interest_zscore_threshold <= 0:
            return []
        try:
            oi = float(int(event.payload.get("open_interest", 0)))
        except (TypeError, ValueError):
            return []
        if not self._should_sample(
            state, "open_interest", event.source_time, cfg
        ):
            return []
        signals = self._maybe_emit_from_history(
            event=event,
            state=state,
            cfg=cfg,
            signal_type="open_interest_spike",
            source_event_type="open_interest",
            history=state.open_interest_history,
            threshold=cfg.open_interest_zscore_threshold,
            value=oi,
            baseline_label="open interest",
            window_seconds=cfg.trade_window_seconds,
            summary_template=(
                "{ticker} open interest {metric:.0f} vs baseline {baseline:.0f} "
                "(z={z_score:.2f})."
            ),
        )
        state.open_interest_history.append(oi)
        state.last_sample_at["open_interest"] = event.source_time
        self._trim_histories(state, cfg)
        return signals

    def _process_candle_event(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        if cfg.candle_range_zscore_threshold <= 0:
            return []
        o = quotation_to_float(event.payload.get("open"))
        h = quotation_to_float(event.payload.get("high"))
        low = quotation_to_float(event.payload.get("low"))
        if o is None or h is None or low is None or o <= 0:
            return []
        range_bps = (h - low) / o * 10_000.0
        signals = self._maybe_emit_from_history(
            event=event,
            state=state,
            cfg=cfg,
            signal_type="candle_range_spike",
            source_event_type="candle",
            history=state.candle_range_history,
            threshold=cfg.candle_range_zscore_threshold,
            value=range_bps,
            baseline_label="candle range bps",
            window_seconds=int(cfg.trade_window_seconds),
            summary_template=(
                "{ticker} candle range {metric:.1f} bps vs baseline {baseline:.1f} "
                "(z={z_score:.2f})."
            ),
        )
        state.candle_range_history.append(range_bps)
        self._trim_histories(state, cfg)
        return signals

    def _maybe_lead_lag(
        self, event: NormalizedEvent, cfg: DetectorSettings
    ) -> list[TriggerSignal]:
        if not cfg.lead_lag_enabled or not self._lead_lag_pairs:
            return []
        if event.event_type not in {"trade", "last_price", "orderbook"}:
            return []
        signals: list[TriggerSignal] = []
        window = timedelta(seconds=max(5, cfg.lead_lag_window_seconds))
        for leader_id, follower_id in self._lead_lag_pairs:
            if event.instrument_id != follower_id:
                continue
            leader_ring = self._mid_track.get(leader_id)
            follower_ring = self._mid_track.get(follower_id)
            if not leader_ring or not follower_ring:
                continue
            now = event.source_time
            leader_move = _range_bps_in_window(leader_ring, now, window)
            follower_move = _range_bps_in_window(follower_ring, now, window)
            if leader_move is None or follower_move is None:
                continue
            if leader_move < cfg.lead_lag_leader_move_bps:
                continue
            if follower_move > cfg.lead_lag_follower_max_bps:
                continue
            if not self._is_alert_ready(
                self._states[follower_id],
                "lead_lag_divergence",
                now,
                cfg,
            ):
                continue
            st = self._states[follower_id]
            self._record_alert_sent(st, "lead_lag_divergence", now)
            signals.append(
                TriggerSignal(
                    signal_id=str(uuid4()),
                    detected_at=utc_now(),
                    instrument_id=follower_id,
                    ticker=event.ticker,
                    class_code=event.class_code,
                    alias=event.alias,
                    source_event_type=event.event_type,
                    signal_type="lead_lag_divergence",
                    severity=2,
                    metric_value=float(leader_move),
                    baseline_value=float(cfg.lead_lag_follower_max_bps),
                    z_score=float(follower_move),
                    window_seconds=cfg.lead_lag_window_seconds,
                    summary=(
                        f"{event.ticker} vs leader {leader_id}: leader range "
                        f"{leader_move:.1f} bps in {cfg.lead_lag_window_seconds}s "
                        f"while this leg moved {follower_move:.1f} bps."
                    ),
                    payload={
                        "leader_instrument_id": leader_id,
                        "follower_instrument_id": follower_id,
                        "leader_range_bps": leader_move,
                        "follower_range_bps": follower_move,
                    },
                )
            )
        return signals

    def _ensure_utc_ts(self, ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def _floor_bucket_utc(self, ts: datetime, minutes: int) -> datetime:
        t = self._ensure_utc_ts(ts).replace(second=0, microsecond=0)
        if minutes <= 1:
            return t
        m = (t.minute // minutes) * minutes
        return t.replace(minute=m)

    def _slot_minute_from_dt(self, ts: datetime) -> int:
        t = self._ensure_utc_ts(ts)
        return int(t.hour * 60 + t.minute)

    def _micro_threshold_multiplier(self, cfg: DetectorSettings) -> float:
        if cfg.microstructure_secondary_mode:
            return max(1.0, float(cfg.microstructure_secondary_threshold_multiplier))
        return 1.0

    def _effective_combo_min_score(self, cfg: DetectorSettings) -> float:
        return float(cfg.combo_min_score) * self._micro_threshold_multiplier(cfg)

    def _historical_timeframe_set(self, cfg: DetectorSettings) -> set[str]:
        raw = (cfg.historical_timeframes_csv or "1m,5m,15m").lower()
        return {x.strip() for x in raw.split(",") if x.strip()}

    def _baseline_percentile_value(
        self, bl: SlotBaseline, cfg: DetectorSettings
    ) -> float:
        key = (cfg.historical_compare_percentile or "p95").strip().lower()
        if key in {"p99", "99"}:
            return float(bl.p99)
        if key in {"p90", "90"}:
            return float(bl.p90)
        if key in {"median", "p50", "50"}:
            return float(bl.median)
        return float(bl.p95)

    def _hist_touch_bar(self, acc: HistBarAccumulator, px: float, qty: float) -> None:
        acc.sum_qty += float(qty)
        acc.n_trades += 1
        acc.sum_pv += float(px) * float(qty)
        if acc.open_px is None:
            acc.open_px = float(px)
        if acc.high_px is None:
            acc.high_px = float(px)
        else:
            acc.high_px = max(float(acc.high_px), float(px))
        if acc.low_px is None:
            acc.low_px = float(px)
        else:
            acc.low_px = min(float(acc.low_px), float(px))

    def _hist_reset_bar(self, acc: HistBarAccumulator, px: float, qty: float) -> None:
        acc.sum_qty = float(qty)
        acc.n_trades = 1
        acc.sum_pv = float(px) * float(qty)
        acc.open_px = float(px)
        acc.high_px = float(px)
        acc.low_px = float(px)

    def _emit_closed_hist_bar(
        self,
        *,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        store: HistoricalBaselineStore,
        tf: str,
        window_seconds: int,
        acc: HistBarAccumulator,
    ) -> list[TriggerSignal]:
        if acc.bucket_start is None or acc.sum_qty <= 0:
            return []
        slot = self._slot_minute_from_dt(acc.bucket_start)
        vwap = acc.sum_pv / acc.sum_qty
        n = int(acc.n_trades)
        vol = float(acc.sum_qty)
        rate = float(n) / float(window_seconds)
        sigs: list[TriggerSignal] = []
        sigs.extend(
            self._try_emit_historical(
                event=event,
                state=state,
                cfg=cfg,
                store=store,
                signal_type=f"historical_volume_anomaly_{tf}",
                metric="volume_qty",
                timeframe=tf,
                slot_minute=slot,
                current_value=vol,
                window_seconds=window_seconds,
                summary_en=(
                    f"{event.ticker} {tf} volume {vol:.6g} vs seasonal slot "
                    f"UTC {slot // 60:02d}:{slot % 60:02d} (ClickHouse baseline)."
                ),
                extra={"vwap": float(vwap)},
            )
        )
        sigs.extend(
            self._try_emit_historical(
                event=event,
                state=state,
                cfg=cfg,
                store=store,
                signal_type=f"historical_trade_rate_anomaly_{tf}",
                metric="trade_rate",
                timeframe=tf,
                slot_minute=slot,
                current_value=rate,
                window_seconds=window_seconds,
                summary_en=(
                    f"{event.ticker} {tf} trade rate {rate:.4f} trades/s vs seasonal "
                    f"slot UTC {slot // 60:02d}:{slot % 60:02d}."
                ),
                extra={"trades_in_bucket": n},
            )
        )
        if tf in {"5m", "15m"} and acc.open_px is not None and float(acc.open_px) > 0:
            ret_bps = (
                (vwap - float(acc.open_px)) / float(acc.open_px)
            ) * 10_000.0
            sigs.extend(
                self._try_emit_historical(
                    event=event,
                    state=state,
                    cfg=cfg,
                    store=store,
                    signal_type=f"historical_return_anomaly_{tf}",
                    metric="return_bps_abs",
                    timeframe=tf,
                    slot_minute=slot,
                    current_value=float(abs(ret_bps)),
                    window_seconds=window_seconds,
                    summary_en=(
                        f"{event.ticker} {tf} |open→VWAP return|={abs(ret_bps):.2f} bps "
                        f"vs seasonal slot."
                    ),
                    extra={"signed_return_bps": float(ret_bps)},
                )
            )
        if (
            tf in {"5m", "15m"}
            and acc.high_px is not None
            and acc.low_px is not None
            and float(acc.high_px) > float(acc.low_px)
        ):
            hi = float(acc.high_px)
            lo = float(acc.low_px)
            mid = (hi + lo) / 2.0
            if mid > 0:
                rng = (hi - lo) / mid * 10_000.0
                sigs.extend(
                    self._try_emit_historical(
                        event=event,
                        state=state,
                        cfg=cfg,
                        store=store,
                        signal_type=f"historical_range_anomaly_{tf}",
                        metric="range_abs_bps",
                        timeframe=tf,
                        slot_minute=slot,
                        current_value=float(rng),
                        window_seconds=window_seconds,
                        summary_en=(
                            f"{event.ticker} {tf} range {rng:.2f} bps (H-L vs mid) "
                            f"vs seasonal slot."
                        ),
                        extra={"high_px": hi, "low_px": lo},
                    )
                )
        return sigs

    def _try_emit_historical(
        self,
        *,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
        store: HistoricalBaselineStore,
        signal_type: str,
        metric: str,
        timeframe: str,
        slot_minute: int,
        current_value: float,
        window_seconds: int,
        summary_en: str,
        extra: dict[str, Any],
    ) -> list[TriggerSignal]:
        if not math.isfinite(current_value) or current_value < 0:
            return []
        bl = store.lookup(event.instrument_id, timeframe, metric, slot_minute)
        if bl is None or int(bl.sample_days) < int(cfg.historical_min_sample_days):
            return []
        ref = self._baseline_percentile_value(bl, cfg)
        thr = float(ref) * max(1.0, float(cfg.historical_exceed_multiplier))
        if thr <= 0:
            return []
        if current_value < thr:
            return []
        if not self._is_alert_ready_for(
            state=state,
            signal_type=signal_type,
            now=event.source_time,
            cooldown_seconds=int(cfg.historical_alert_cooldown_seconds),
            cfg=cfg,
        ):
            return []
        self._record_alert_sent(state, signal_type, event.source_time)
        ratio = current_value / thr
        z_score = max(0.0, (ratio - 1.0) * 4.0)
        severity = _severity_from_z_score(z_score)
        pl: dict[str, Any] = {
            "historical": True,
            "timeframe": timeframe,
            "metric": metric,
            "slot_minute": int(slot_minute),
            "current_value": float(current_value),
            "expected_median": float(bl.median),
            "p90": float(bl.p90),
            "p95": float(bl.p95),
            "p99": float(bl.p99),
            "compare_percentile": cfg.historical_compare_percentile,
            "compare_threshold": float(thr),
            "lookback_days": int(cfg.historical_lookback_days),
            "sample_days": int(bl.sample_days),
            "event_payload": event.payload,
            **extra,
        }
        if cfg.historical_primary_signals_enabled:
            pl["signal_family"] = "historical_primary"
        return [
            TriggerSignal(
                signal_id=str(uuid4()),
                detected_at=utc_now(),
                instrument_id=event.instrument_id,
                ticker=event.ticker,
                class_code=event.class_code,
                alias=event.alias,
                source_event_type="trade",
                signal_type=signal_type,
                severity=severity,
                metric_value=float(current_value),
                baseline_value=float(ref),
                z_score=float(z_score),
                window_seconds=window_seconds,
                summary=summary_en,
                payload=pl,
            )
        ]

    def _process_historical_trade_buckets(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        cfg: DetectorSettings,
    ) -> list[TriggerSignal]:
        store = self._historical_store
        if store is None or not store.enabled:
            return []
        px = quotation_to_float(event.payload.get("price"))
        qty = float(event.payload.get("quantity", 0.0))
        if px is None or px <= 0 or not math.isfinite(qty) or qty <= 0:
            return []
        ts = event.source_time
        tfs = self._historical_timeframe_set(cfg)
        divs = {"1m": 1, "5m": 5, "15m": 15}
        wins = {"1m": 60, "5m": 300, "15m": 900}
        out: list[TriggerSignal] = []
        for tf in ("1m", "5m", "15m"):
            if tf not in tfs:
                continue
            div = divs[tf]
            m0 = self._floor_bucket_utc(ts, div)
            acc = state.hist_bars[tf]
            if acc.bucket_start is None:
                acc.bucket_start = m0
                self._hist_reset_bar(acc, float(px), float(qty))
            elif m0 > acc.bucket_start:
                out.extend(
                    self._emit_closed_hist_bar(
                        event=event,
                        state=state,
                        cfg=cfg,
                        store=store,
                        tf=tf,
                        window_seconds=wins[tf],
                        acc=acc,
                    )
                )
                acc.bucket_start = m0
                self._hist_reset_bar(acc, float(px), float(qty))
            else:
                self._hist_touch_bar(acc, float(px), float(qty))
        return out


def _sum_orderbook_depth(levels: list, n: int) -> float:
    total = 0.0
    for level in levels[: max(0, n)]:
        if isinstance(level, dict):
            total += float(level.get("quantity", 0.0))
    return total


def _range_bps_in_window(
    ring: deque[tuple[datetime, float]],
    now: datetime,
    window: timedelta,
) -> float | None:
    if not ring:
        return None
    start = now - window
    prices = [px for ts, px in ring if start <= ts <= now]
    if len(prices) < 2:
        return None
    lo, hi = min(prices), max(prices)
    mid = (lo + hi) / 2.0
    if mid <= 0:
        return None
    return (hi - lo) / mid * 10_000.0


def _severity_from_z_score(z_score: float) -> int:
    if z_score >= 6:
        return 3
    if z_score >= 4:
        return 2
    return 1


def _optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        t = value.strip().lower()
        if t in {"true", "1", "yes"}:
            return True
        if t in {"false", "0", "no"}:
            return False
    return None


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

