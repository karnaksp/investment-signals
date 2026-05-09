"""Сериализация скользящих окон детектора для Redis (рестарт / hot-reload)."""

from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING, Any

from .serialization import parse_timestamp

if TYPE_CHECKING:
    from .detector_core import SignalDetector

_MID_TRACK_MAXLEN = 4000
_ORDERBOOK_DEPTH_MAXLEN = 96
_MICROBURST_MAXLEN = 512


def export_window_state(detector: SignalDetector) -> dict[str, Any]:
    instruments: dict[str, Any] = {}
    for iid, st in detector._states.items():
        blob = _serialize_instrument_state(st)
        if blob is not None:
            instruments[iid] = blob
    mid_track: dict[str, list[list[Any]]] = {}
    for iid, ring in detector._mid_track.items():
        if ring:
            mid_track[iid] = [[ts.isoformat(), float(px)] for ts, px in ring]
    return {"instruments": instruments, "mid_track": mid_track}


def hydrate_window_state(detector: SignalDetector, data: dict[str, Any] | None) -> None:
    if not data:
        return
    for iid, blob in data.get("instruments", {}).items():
        if isinstance(blob, dict):
            detector._states[iid] = _deserialize_instrument_state(blob)
    mid = data.get("mid_track")
    if isinstance(mid, dict):
        detector._mid_track.clear()
        for iid, seq in mid.items():
            if not isinstance(seq, list):
                continue
            dq: deque[tuple[datetime, float]] = deque(maxlen=_MID_TRACK_MAXLEN)
            for item in seq:
                if (
                    isinstance(item, (list, tuple))
                    and len(item) == 2
                    and isinstance(item[0], str)
                ):
                    try:
                        dq.append((parse_timestamp(item[0]), float(item[1])))
                    except (TypeError, ValueError):
                        continue
            if dq:
                detector._mid_track[str(iid)] = dq


def _serialize_instrument_state(st: Any) -> dict[str, Any] | None:
    if not any(
        [
            st.trade_points,
            st.signed_trade_points,
            st.price_points,
            st.volume_history,
            st.trade_count_history,
            st.return_history,
            st.spread_history,
            st.imbalance_history,
            st.orderbook_depth_snapshots,
            st.microburst_ticks,
            st.obi_delta_history,
            st.open_interest_history,
            st.candle_range_history,
            bool(st.last_sample_at),
            bool(st.last_active_at),
            st.last_trading_status is not None,
            st.last_orderbook_imbalance_ratio is not None,
            st.last_limit_order_available is not None,
            st.last_market_order_available is not None,
        ]
    ):
        return None

    out: dict[str, Any] = {
        "trade_points": [
            {"ts": p.ts.isoformat(), "quantity": p.quantity, "notional": p.notional}
            for p in st.trade_points
        ],
        "signed_trade_points": [
            {"ts": p.ts.isoformat(), "signed_quantity": p.signed_quantity}
            for p in st.signed_trade_points
        ],
        "price_points": [
            {"ts": p.ts.isoformat(), "price": p.price} for p in st.price_points
        ],
        "volume_history": list(st.volume_history),
        "trade_count_history": list(st.trade_count_history),
        "return_history": list(st.return_history),
        "spread_history": list(st.spread_history),
        "imbalance_history": list(st.imbalance_history),
        "last_sample_at": {k: v.isoformat() for k, v in st.last_sample_at.items()},
        "last_active_at": {k: v.isoformat() for k, v in st.last_active_at.items()},
        "last_trading_status": st.last_trading_status,
        "last_orderbook_imbalance_ratio": st.last_orderbook_imbalance_ratio,
        "orderbook_depth_snapshots": [
            {
                "ts": snap.ts.isoformat(),
                "bid_l3_qty": snap.bid_l3_qty,
                "ask_l3_qty": snap.ask_l3_qty,
                "mid": snap.mid,
                "best_bid": snap.best_bid,
                "best_ask": snap.best_ask,
            }
            for snap in st.orderbook_depth_snapshots
        ],
        "microburst_ticks": [
            {"ts": t.isoformat(), "v": float(v)} for t, v in st.microburst_ticks
        ],
        "obi_delta_history": list(st.obi_delta_history),
        "last_sampled_obi": st.last_sampled_obi,
        "open_interest_history": list(st.open_interest_history),
        "candle_range_history": list(st.candle_range_history),
        "last_limit_order_available": st.last_limit_order_available,
        "last_market_order_available": st.last_market_order_available,
    }
    return out


def _deserialize_instrument_state(blob: dict[str, Any]) -> InstrumentState:
    from .detector_core import (
        InstrumentState,
        OrderBookDepthSnapshot,
        PricePoint,
        SignedTradePoint,
        TradePoint,
    )

    st = InstrumentState()
    for row in blob.get("trade_points") or []:
        if isinstance(row, dict) and "ts" in row:
            try:
                st.trade_points.append(
                    TradePoint(
                        ts=parse_timestamp(str(row["ts"])),
                        quantity=float(row.get("quantity", 0.0)),
                        notional=float(row.get("notional", 0.0)),
                    )
                )
            except (TypeError, ValueError):
                continue
    for row in blob.get("signed_trade_points") or []:
        if isinstance(row, dict) and "ts" in row:
            try:
                st.signed_trade_points.append(
                    SignedTradePoint(
                        ts=parse_timestamp(str(row["ts"])),
                        signed_quantity=float(row.get("signed_quantity", 0.0)),
                    )
                )
            except (TypeError, ValueError):
                continue
    for row in blob.get("price_points") or []:
        if isinstance(row, dict) and "ts" in row:
            try:
                st.price_points.append(
                    PricePoint(
                        ts=parse_timestamp(str(row["ts"])),
                        price=float(row.get("price", 0.0)),
                    )
                )
            except (TypeError, ValueError):
                continue
    for v in blob.get("volume_history") or []:
        try:
            st.volume_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for v in blob.get("trade_count_history") or []:
        try:
            st.trade_count_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for v in blob.get("return_history") or []:
        try:
            st.return_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for v in blob.get("spread_history") or []:
        try:
            st.spread_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for v in blob.get("imbalance_history") or []:
        try:
            st.imbalance_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for k, iso in (blob.get("last_sample_at") or {}).items():
        if isinstance(k, str) and isinstance(iso, str):
            try:
                st.last_sample_at[k] = parse_timestamp(iso)
            except (TypeError, ValueError):
                continue
    for k, iso in (blob.get("last_active_at") or {}).items():
        if isinstance(k, str) and isinstance(iso, str):
            try:
                st.last_active_at[k] = parse_timestamp(iso)
            except (TypeError, ValueError):
                continue
    if isinstance(blob.get("last_trading_status"), str):
        st.last_trading_status = blob["last_trading_status"]
    lr = blob.get("last_orderbook_imbalance_ratio")
    if lr is not None:
        try:
            st.last_orderbook_imbalance_ratio = float(lr)
        except (TypeError, ValueError):
            pass
    st.orderbook_depth_snapshots = deque(maxlen=_ORDERBOOK_DEPTH_MAXLEN)
    for row in blob.get("orderbook_depth_snapshots") or []:
        if not isinstance(row, dict):
            continue
        try:
            st.orderbook_depth_snapshots.append(
                OrderBookDepthSnapshot(
                    ts=parse_timestamp(str(row["ts"])),
                    bid_l3_qty=float(row.get("bid_l3_qty", 0.0)),
                    ask_l3_qty=float(row.get("ask_l3_qty", 0.0)),
                    mid=float(row.get("mid", 0.0)),
                    best_bid=float(row.get("best_bid", 0.0)),
                    best_ask=float(row.get("best_ask", 0.0)),
                )
            )
        except (TypeError, ValueError, KeyError):
            continue
    st.microburst_ticks = deque(maxlen=_MICROBURST_MAXLEN)
    for row in blob.get("microburst_ticks") or []:
        if isinstance(row, dict) and "ts" in row:
            try:
                st.microburst_ticks.append(
                    (parse_timestamp(str(row["ts"])), float(row.get("v", 0.0)))
                )
            except (TypeError, ValueError):
                continue
    for v in blob.get("obi_delta_history") or []:
        try:
            st.obi_delta_history.append(float(v))
        except (TypeError, ValueError):
            continue
    lso = blob.get("last_sampled_obi")
    if lso is not None:
        try:
            st.last_sampled_obi = float(lso)
        except (TypeError, ValueError):
            pass
    for v in blob.get("open_interest_history") or []:
        try:
            st.open_interest_history.append(float(v))
        except (TypeError, ValueError):
            continue
    for v in blob.get("candle_range_history") or []:
        try:
            st.candle_range_history.append(float(v))
        except (TypeError, ValueError):
            continue
    if isinstance(blob.get("last_limit_order_available"), bool):
        st.last_limit_order_available = blob["last_limit_order_available"]
    if isinstance(blob.get("last_market_order_available"), bool):
        st.last_market_order_available = blob["last_market_order_available"]
    return st
