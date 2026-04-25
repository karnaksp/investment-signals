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
class PricePoint:
    ts: datetime
    price: float


@dataclass
class InstrumentState:
    trade_points: deque[TradePoint] = field(default_factory=deque)
    price_points: deque[PricePoint] = field(default_factory=deque)
    volume_history: deque[float] = field(default_factory=deque)
    trade_count_history: deque[float] = field(default_factory=deque)
    return_history: deque[float] = field(default_factory=deque)
    spread_history: deque[float] = field(default_factory=deque)
    imbalance_history: deque[float] = field(default_factory=deque)
    last_sample_at: dict[str, datetime] = field(default_factory=dict)
    last_alert_at: dict[str, datetime] = field(default_factory=dict)
    last_trading_status: str | None = None


class SignalDetector:
    def __init__(self, settings: DetectorSettings):
        self.settings = settings
        self._states: dict[str, InstrumentState] = defaultdict(InstrumentState)

    def process(self, event: NormalizedEvent) -> list[TriggerSignal]:
        state = self._states[event.instrument_id]
        if event.event_type == "trade":
            return self._process_trade_event(event, state)
        if event.event_type == "last_price":
            return self._process_last_price_event(event, state)
        if event.event_type == "orderbook":
            return self._process_orderbook_event(event, state)
        if event.event_type == "trading_status":
            return self._process_trading_status_event(event, state)
        return []

    def _process_trade_event(
        self, event: NormalizedEvent, state: InstrumentState
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
        state.price_points.append(PricePoint(ts=event.source_time, price=price))
        self._prune_trade_points(state, event.source_time)
        self._prune_price_points(state, event.source_time)

        signals.extend(self._sample_trade_windows(event, state))
        signals.extend(self._sample_price_move(event, state, current_price=price))
        return signals

    def _process_last_price_event(
        self, event: NormalizedEvent, state: InstrumentState
    ) -> list[TriggerSignal]:
        price = quotation_to_float(event.payload.get("price"))
        if price is None or price <= 0:
            return []
        state.price_points.append(PricePoint(ts=event.source_time, price=price))
        self._prune_price_points(state, event.source_time)
        return self._sample_price_move(event, state, current_price=price)

    def _process_orderbook_event(
        self, event: NormalizedEvent, state: InstrumentState
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "orderbook", event.source_time):
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
        signals: list[TriggerSignal] = []

        signals.extend(
            self._maybe_emit_from_history(
                event=event,
                state=state,
                signal_type="spread_widening",
                source_event_type="orderbook",
                history=state.spread_history,
                threshold=self.settings.spread_zscore_threshold,
                value=spread_bps,
                baseline_label="spread",
                window_seconds=self.settings.orderbook_window_seconds,
                summary_template=(
                    "{ticker} spread widened to {metric:.2f} bps "
                    "vs baseline {baseline:.2f} (z={z_score:.2f})."
                ),
            )
        )

        if imbalance_abs >= self.settings.imbalance_absolute_threshold:
            signals.extend(
                self._maybe_emit_from_history(
                    event=event,
                    state=state,
                    signal_type="orderbook_imbalance",
                    source_event_type="orderbook",
                    history=state.imbalance_history,
                    threshold=self.settings.imbalance_zscore_threshold,
                    value=imbalance_abs,
                    baseline_label="imbalance",
                    window_seconds=self.settings.orderbook_window_seconds,
                    summary_template=(
                        "{ticker} order book imbalance reached {metric:.2f} "
                        "vs baseline {baseline:.2f} (z={z_score:.2f})."
                    ),
                )
            )

        state.spread_history.append(spread_bps)
        state.imbalance_history.append(imbalance_abs)
        state.last_sample_at["orderbook"] = event.source_time
        self._trim_histories(state)
        return signals

    def _process_trading_status_event(
        self, event: NormalizedEvent, state: InstrumentState
    ) -> list[TriggerSignal]:
        status = str(event.payload.get("trading_status", "")).strip()
        if not status:
            return []
        previous_status = state.last_trading_status
        state.last_trading_status = status
        if previous_status is None or previous_status == status:
            return []
        if not self._is_alert_ready(
            state, "trading_status_changed", event.source_time
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
        self, event: NormalizedEvent, state: InstrumentState
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "trade_window", event.source_time):
            return []
        total_qty = sum(point.quantity for point in state.trade_points)
        trade_count = float(len(state.trade_points))
        signals: list[TriggerSignal] = []

        signals.extend(
            self._maybe_emit_from_history(
                event=event,
                state=state,
                signal_type="volume_spike",
                source_event_type="trade",
                history=state.volume_history,
                threshold=self.settings.volume_zscore_threshold,
                value=total_qty,
                baseline_label="rolling volume",
                window_seconds=self.settings.trade_window_seconds,
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
                signal_type="trade_rate_spike",
                source_event_type="trade",
                history=state.trade_count_history,
                threshold=self.settings.trade_count_zscore_threshold,
                value=trade_count,
                baseline_label="trade count",
                window_seconds=self.settings.trade_window_seconds,
                summary_template=(
                    "{ticker} trade count reached {metric:.2f} "
                    "vs baseline {baseline:.2f} (z={z_score:.2f})."
                ),
            )
        )

        state.volume_history.append(total_qty)
        state.trade_count_history.append(trade_count)
        state.last_sample_at["trade_window"] = event.source_time
        self._trim_histories(state)
        return signals

    def _sample_price_move(
        self,
        event: NormalizedEvent,
        state: InstrumentState,
        *,
        current_price: float,
    ) -> list[TriggerSignal]:
        if not self._should_sample(state, "price_window", event.source_time):
            return []
        if len(state.price_points) < 2:
            return []

        oldest_price = state.price_points[0].price
        if oldest_price <= 0:
            return []
        move_bps = abs((current_price - oldest_price) / oldest_price) * 10_000
        signals = self._maybe_emit_from_history(
            event=event,
            state=state,
            signal_type="price_jump",
            source_event_type=event.event_type,
            history=state.return_history,
            threshold=self.settings.price_return_zscore_threshold,
            value=move_bps,
            baseline_label="price move",
            window_seconds=self.settings.price_window_seconds,
            summary_template=(
                "{ticker} moved {metric:.2f} bps in {window}s "
                "vs baseline {baseline:.2f} (z={z_score:.2f})."
            ),
        )

        state.return_history.append(move_bps)
        state.last_sample_at["price_window"] = event.source_time
        self._trim_histories(state)
        return signals

    def _maybe_emit_from_history(
        self,
        *,
        event: NormalizedEvent,
        state: InstrumentState,
        signal_type: str,
        source_event_type: str,
        history: deque[float],
        threshold: float,
        value: float,
        baseline_label: str,
        window_seconds: int,
        summary_template: str,
    ) -> list[TriggerSignal]:
        if len(history) < self.settings.min_baseline_points:
            return []
        baseline, z_score = _z_score(history, value)
        if z_score < threshold:
            return []
        if not self._is_alert_ready(state, signal_type, event.source_time):
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

    def _prune_trade_points(self, state: InstrumentState, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.settings.trade_window_seconds)
        while state.trade_points and state.trade_points[0].ts < cutoff:
            state.trade_points.popleft()

    def _prune_price_points(self, state: InstrumentState, now: datetime) -> None:
        cutoff = now - timedelta(seconds=self.settings.price_window_seconds)
        while state.price_points and state.price_points[0].ts < cutoff:
            state.price_points.popleft()

    def _should_sample(
        self, state: InstrumentState, sample_key: str, now: datetime
    ) -> bool:
        last_sample_at = state.last_sample_at.get(sample_key)
        if last_sample_at is None:
            return True
        elapsed = (now - last_sample_at).total_seconds()
        return elapsed >= self.settings.sample_every_seconds

    def _is_alert_ready(
        self, state: InstrumentState, signal_type: str, now: datetime
    ) -> bool:
        last_alert_at = state.last_alert_at.get(signal_type)
        if last_alert_at is None:
            return True
        elapsed = (now - last_alert_at).total_seconds()
        return elapsed >= self.settings.alert_cooldown_seconds

    def _trim_histories(self, state: InstrumentState) -> None:
        maxlen = self.settings.baseline_points
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

