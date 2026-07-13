"""Скользящие окна, z-score и генерация ``TriggerSignal`` (см. ``models``)."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from statistics import fmean
from typing import Any, Iterable
from uuid import uuid4

from .config import DetectorSettings
from .domain.signal_identity import deterministic_signal_id
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
class OrderBookDepthSnapshot:
    ts: datetime
    bid_l3_qty: float
    ask_l3_qty: float
    mid: float
    best_bid: float
    best_ask: float


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


class SignalDetector:
    """Обновляет состояние по инструменту и возвращает список новых сигналов."""

    def __init__(
        self,
        settings: DetectorSettings,
        per_instrument: dict[str, DetectorSettings] | None = None,
        *,
        lead_lag_pairs: tuple[tuple[str, str], ...] = (),
        expectation_catalog_version: str | None = None,
        detector_config_version: str | None = None,
        delivery_config_version: str | None = None,
        cost_model_version: str | None = None,
    ):
        self._default_settings = settings
        self._per_instrument = per_instrument or {}
        self._states: dict[str, InstrumentState] = defaultdict(InstrumentState)
        self._lead_lag_pairs = lead_lag_pairs
        self._expectation_catalog_version = expectation_catalog_version
        self._detector_config_version = detector_config_version
        self._delivery_config_version = delivery_config_version
        self._cost_model_version = cost_model_version
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
        return [self._attach_provenance(signal, event) for signal in signals]

    def _attach_provenance(
        self,
        signal: TriggerSignal,
        event: NormalizedEvent,
    ) -> TriggerSignal:
        return replace(
            signal,
            signal_id=deterministic_signal_id(event.event_id, signal.signal_type),
            source_event_id=event.event_id,
            source_event_at=event.source_time,
            signal_schema_version="1.0.0",
            expectation_catalog_version=self._expectation_catalog_version,
            detector_config_version=self._detector_config_version,
            delivery_config_version=self._delivery_config_version,
            cost_model_version=self._cost_model_version,
            provenance_status="complete",
        )

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
            out.append(replace(s, payload={**s.payload, "unary_context": dict(ctx)}))
        return out

    def export_alert_state(self) -> dict[str, dict[str, str]]:
        """Сериализует ``last_alert_at`` по инструментам (ISO 8601) для Redis/файла."""
        out: dict[str, dict[str, str]] = {}
        for iid, st in self._states.items():
            if st.last_alert_at:
                out[iid] = {k: v.isoformat() for k, v in st.last_alert_at.items()}
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
                prev = state.last_alert_at.get(sig_type)
                if prev is None or dt > prev:
                    state.last_alert_at[sig_type] = dt

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
                notional=price * quantity * max(1, int(event.lot or 0)),
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
                self._maybe_emit_trade_burst(event, state, cfg, signed_qty=signed_qty)
            )

        signals.extend(self._sample_trade_windows(event, state, cfg))
        signals.extend(self._sample_price_move(event, state, cfg, current_price=price))
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
        total_qty = top_bids_qty + top_asks_qty
        if total_qty <= 0:
            return []

        mid = (best_bid + best_ask) / 2.0
        self._push_mid(event.instrument_id, event.source_time, mid, cfg)
        self._record_orderbook_snapshot(
            state,
            OrderBookDepthSnapshot(
                ts=event.source_time,
                bid_l3_qty=top_bids_qty,
                ask_l3_qty=top_asks_qty,
                mid=mid,
                best_bid=best_bid,
                best_ask=best_ask,
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
                self._maybe_emit_price_near_limit_band(event, state, cfg, mid=mid)
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
                payload_extra={
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "mid": mid,
                    "spread_price": best_ask - best_bid,
                    "spread_bps": spread_bps,
                    "depth_levels": depth,
                    "top_bid_qty": top_bids_qty,
                    "top_ask_qty": top_asks_qty,
                },
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
                    payload_extra={
                        "imbalance_abs": imbalance_abs,
                        "imbalance_ratio": imbalance_ratio,
                        "dominant_side": (
                            "bid" if top_bids_qty >= top_asks_qty else "ask"
                        ),
                        "depth_levels": depth,
                        "top_bid_qty": top_bids_qty,
                        "top_ask_qty": top_asks_qty,
                        "total_book_qty": total_qty,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "mid": mid,
                    },
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
                        "{ticker} L" + str(depth) + " OBI jump |Δ|={metric:.3f} "
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
                            payload_extra={
                                "obi": obi,
                                "previous_obi": state.last_sampled_obi,
                                "delta_obi": delta_obi,
                                "abs_delta_obi": abs(delta_obi),
                                "depth_levels": depth,
                                "top_bid_qty": top_bids_qty,
                                "top_ask_qty": top_asks_qty,
                            },
                        )
                    )
                state.obi_delta_history.append(abs(delta_obi))
            state.last_sampled_obi = obi

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
        signals: list[TriggerSignal] = []
        if cfg.track_market_access_flags:
            signals.extend(self._maybe_emit_market_access_change(event, state, cfg))

        previous_status = state.last_trading_status
        state.last_trading_status = status
        if previous_status is None or previous_status == status:
            return signals
        if not self._is_alert_ready(
            state, "trading_status_changed", event.source_time, cfg
        ):
            return signals
        state.last_alert_at["trading_status_changed"] = event.source_time
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
        total_notional = sum(point.notional for point in state.trade_points)
        trade_count = float(len(state.trade_points))
        lot = max(1, int(event.lot or 0))
        current_price = quotation_to_float(event.payload.get("price"))
        volatility_context = self._baseline_volatility_context(event, cfg)
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
                payload_extra={
                    "window_lots": total_qty,
                    "window_units": total_qty * lot,
                    "window_notional": total_notional,
                    "window_notional_currency": "price_units",
                    "last_price": current_price,
                    "lot": lot,
                    **volatility_context,
                },
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
                payload_extra={
                    "trade_count": trade_count,
                    "window_lots": total_qty,
                    "window_units": total_qty * lot,
                    "window_notional": total_notional,
                    "window_notional_currency": "price_units",
                    "avg_trade_notional": (
                        total_notional / trade_count if trade_count > 0 else None
                    ),
                    **volatility_context,
                    "last_price": current_price,
                    "lot": lot,
                },
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
        signed_move_bps = ((current_price - oldest_price) / oldest_price) * 10_000
        move_bps = abs(signed_move_bps)
        if (
            cfg.price_move_absolute_threshold_bps > 0
            and move_bps < cfg.price_move_absolute_threshold_bps
        ):
            return []
        volatility_context = self._baseline_volatility_context(event, cfg)
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
            payload_extra={
                "start_price": oldest_price,
                "current_price": current_price,
                "price_change": current_price - oldest_price,
                "price_change_bps": signed_move_bps,
                "price_change_pct": signed_move_bps / 100.0,
                "abs_price_change_bps": move_bps,
                "price_direction": "up" if signed_move_bps > 0 else "down",
                **volatility_context,
            },
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
                "imbalance_long_ge": cfg.combo_imbalance_long_threshold,
                "imbalance_short_le": cfg.combo_imbalance_short_threshold,
                "delta_min_abs_qty": cfg.combo_delta_min_abs_qty,
            },
            "imbalance_ratio": imbalance_ratio,
            "signed_delta_qty": signed_delta_qty,
        }

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
                        "combo_detail": combo_detail,
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
        state.last_alert_at[signal_type] = event.source_time
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
        payload_extra: dict[str, Any] | None = None,
    ) -> list[TriggerSignal]:
        if len(history) < cfg.min_baseline_points:
            return []
        baseline, z_score = _z_score(history, value)
        if z_score < threshold:
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
        state.last_alert_at[signal_type] = event.source_time
        payload = {
            "baseline_label": baseline_label,
            "event_payload": event.payload,
        }
        if payload_extra:
            payload.update(payload_extra)
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
                payload=payload,
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
            state.obi_delta_history,
            state.open_interest_history,
            state.candle_range_history,
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
        cutoff = ts - timedelta(
            seconds=max(
                5,
                cfg.lead_lag_window_seconds,
                cfg.baseline_volatility_window_seconds,
            )
        )
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _baseline_volatility_context(
        self,
        event: NormalizedEvent,
        cfg: DetectorSettings,
    ) -> dict[str, Any]:
        return {
            "baseline_volatility_bps": _realized_volatility_bps(
                self._mid_track[event.instrument_id]
            ),
            "baseline_volatility_horizon_seconds": (
                cfg.baseline_volatility_window_seconds
            ),
            "baseline_volatility_observed_until": event.source_time.isoformat(),
            "baseline_volatility_estimator_version": "tick-realized-rv-v1",
        }

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
        state.last_alert_at["aggressive_trade_burst"] = event.source_time
        direction = "buy" if signs[0] > 0 else "sell"
        price = quotation_to_float(event.payload.get("price"))
        lot = max(1, int(event.lot or 0))
        notional = price * total_abs * lot if price is not None else None
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
                    "abs_units_sum": total_abs * lot,
                    "last_price": price,
                    "estimated_notional": notional,
                    "estimated_notional_currency": "price_units",
                    "lot": lot,
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
        state.last_alert_at["orderbook_snapshot_inconsistent"] = event.source_time
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
        if lim_up is None or lim_dn is None or lim_up <= 0 or lim_dn <= 0 or mid <= 0:
            return []
        dist_up = (lim_up - mid) / mid * 10_000.0
        dist_dn = (mid - lim_dn) / mid * 10_000.0
        nearest = min(dist_up, dist_dn)
        nearest_side = "upper" if dist_up <= dist_dn else "lower"
        if nearest > cfg.limit_band_warning_bps:
            return []
        if not self._is_alert_ready(
            state, "price_near_limit_band", event.source_time, cfg
        ):
            return []
        state.last_alert_at["price_near_limit_band"] = event.source_time
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
                    "nearest_limit_side": nearest_side,
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
        state.last_alert_at["market_access_changed"] = event.source_time
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
        if not self._should_sample(state, "open_interest", event.source_time, cfg):
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
            payload_extra={
                "open_interest": oi,
                **self._baseline_volatility_context(event, cfg),
            },
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
        close = quotation_to_float(event.payload.get("close"))
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
            payload_extra={
                "open": o,
                "high": h,
                "low": low,
                "close": close,
                "range_bps": range_bps,
                "range_pct": range_bps / 100.0,
            },
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
            leader_signed_move = _signed_move_bps_in_window(leader_ring, now, window)
            if (
                leader_move is None
                or follower_move is None
                or leader_signed_move is None
                or abs(leader_signed_move) <= 1e-12
            ):
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
            st.last_alert_at["lead_lag_divergence"] = now
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
                        "leader_signed_move_bps": leader_signed_move,
                        "follower_range_bps": follower_move,
                        "follower_expected_direction": (
                            "long" if leader_signed_move > 0 else "short"
                        ),
                    },
                )
            )
        return signals


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


def _signed_move_bps_in_window(
    ring: deque[tuple[datetime, float]],
    now: datetime,
    window: timedelta,
) -> float | None:
    start = now - window
    prices = [px for ts, px in ring if start <= ts <= now]
    if len(prices) < 2 or prices[0] <= 0:
        return None
    return ((prices[-1] - prices[0]) / prices[0]) * 10_000.0


def _realized_volatility_bps(
    ring: deque[tuple[datetime, float]],
) -> float | None:
    """Realized path volatility as sqrt(sum of squared simple returns)."""

    prices = [price for _, price in ring if price > 0 and math.isfinite(price)]
    if len(prices) < 3:
        return None
    returns = [
        ((current / previous) - 1.0) * 10_000.0
        for previous, current in zip(prices, prices[1:])
    ]
    return math.sqrt(sum(value * value for value in returns))


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
