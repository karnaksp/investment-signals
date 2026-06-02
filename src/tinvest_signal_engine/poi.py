"""Point-of-interest aggregation over stored detector signals."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from dateutil.parser import isoparse

POI_CONTRACT_VERSION = "poi_v1"
DEFAULT_CLUSTER_WINDOW_SECONDS = 300

_LONG_TYPES = {"microstructure_combo_long", "orderbook_spoofing_ask_pull"}
_SHORT_TYPES = {"microstructure_combo_short", "orderbook_spoofing_bid_pull"}
_ACTIVITY_TYPES = {"volume_spike", "trade_rate_spike"}
_PRICE_TYPES = {"price_jump", "candle_range_spike"}
_ORDERFLOW_TYPES = {
    "microstructure_combo_long",
    "microstructure_combo_short",
    "aggressive_trade_burst",
    "orderbook_imbalance",
    "obi_dynamics",
}


def build_pois_from_signal_rows(
    rows: list[dict[str, Any]],
    *,
    cluster_window_seconds: int = DEFAULT_CLUSTER_WINDOW_SECONDS,
    horizon: str = "15m",
) -> list[dict[str, Any]]:
    """Build read-time POI rows from admin signal rows."""
    signals = [_normalise_signal(row) for row in rows if row]
    signals = [row for row in signals if row.get("detected_at_dt") is not None]
    signals.sort(key=lambda row: (row["instrument_id"], row["detected_at_dt"]))

    clusters: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    window = timedelta(seconds=max(60, int(cluster_window_seconds)))
    for row in signals:
        if not current:
            current = [row]
            continue
        same_instrument = row["instrument_id"] == current[-1]["instrument_id"]
        close_enough = row["detected_at_dt"] - current[0]["detected_at_dt"] <= window
        if same_instrument and close_enough:
            current.append(row)
        else:
            clusters.append(current)
            current = [row]
    if current:
        clusters.append(current)

    pois = [_cluster_to_poi(cluster, horizon=horizon) for cluster in clusters]
    pois.sort(key=lambda row: (row["interest_score"], row["updated_at"]), reverse=True)
    return pois


def find_poi(pois: list[dict[str, Any]], poi_id: str) -> dict[str, Any] | None:
    for poi in pois:
        if poi.get("poi_id") == poi_id:
            return poi
    return None


def _cluster_to_poi(cluster: list[dict[str, Any]], *, horizon: str) -> dict[str, Any]:
    first = cluster[0]
    detected_at = min(row["detected_at_dt"] for row in cluster)
    updated_at = max(row["detected_at_dt"] for row in cluster)
    signal_types = {str(row.get("signal_type") or "") for row in cluster}
    bias = _bias(cluster)
    setup_type = _setup_type(signal_types, bias)
    score = _interest_score(cluster, setup_type=setup_type)
    price = _price(cluster)
    entry_zone, invalidation, target_1, target_2 = _levels(price, bias)
    confidence = "high" if score >= 82 else "medium" if score >= 62 else "low"
    poi_id = _poi_id(cluster, detected_at, updated_at)
    drivers = _drivers(cluster)
    summary = _summary(first, setup_type, bias, score, drivers)
    return {
        "poi_id": poi_id,
        "contract_version": POI_CONTRACT_VERSION,
        "instrument_id": first.get("instrument_id") or "",
        "ticker": first.get("ticker") or "",
        "class_code": first.get("class_code") or "",
        "detected_at": detected_at.isoformat(),
        "updated_at": updated_at.isoformat(),
        "setup_type": setup_type,
        "bias": bias,
        "horizon": horizon,
        "interest_score": score,
        "confidence": confidence,
        "price": price,
        "entry_zone": entry_zone,
        "invalidation_price": invalidation,
        "target_1": target_1,
        "target_2": target_2,
        "drivers": drivers,
        "nearby_signals": [_nearby_signal(row) for row in cluster],
        "human_summary_ru": summary,
    }


def _normalise_signal(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out["payload"] = dict(row.get("payload") or {})
    out["detected_at_dt"] = _parse_dt(row.get("detected_at"))
    return out


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        dt = isoparse(str(value))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _bias(cluster: list[dict[str, Any]]) -> str:
    long_score = 0
    short_score = 0
    for row in cluster:
        st = str(row.get("signal_type") or "")
        payload = row.get("payload") or {}
        if st in _LONG_TYPES:
            long_score += 3
        if st in _SHORT_TYPES:
            short_score += 3
        direction = str(payload.get("direction") or payload.get("price_direction") or "")
        if direction in {"buy", "up", "bid", "long"}:
            long_score += 1
        if direction in {"sell", "down", "ask", "short"}:
            short_score += 1
    if long_score > short_score:
        return "long"
    if short_score > long_score:
        return "short"
    if any(str(row.get("signal_type") or "").endswith("_changed") for row in cluster):
        return "status"
    return "watch"


def _setup_type(signal_types: set[str], bias: str) -> str:
    if "lead_lag_divergence" in signal_types:
        return "lead_lag"
    if "aggressive_trade_burst" in signal_types:
        return "aggressive_flow"
    if signal_types & {"orderbook_imbalance", "obi_dynamics"} and signal_types & _ACTIVITY_TYPES:
        return "liquidity_shift"
    if signal_types & _PRICE_TYPES and signal_types & (_ACTIVITY_TYPES | _ORDERFLOW_TYPES):
        return "momentum_breakout" if bias in {"long", "short"} else "news_like_activity"
    if signal_types & {"trading_status_changed", "market_access_changed"}:
        return "status_change"
    if len(signal_types) == 1:
        return "admin_observe"
    return "reversal_watch" if bias == "watch" else "momentum_breakout"


def _interest_score(cluster: list[dict[str, Any]], *, setup_type: str) -> int:
    max_quality = max((_quality(row) for row in cluster), default=0.0)
    max_severity = max((int(row.get("severity") or 1) for row in cluster), default=1)
    max_z = max((abs(float(row.get("z_score") or 0.0)) for row in cluster), default=0.0)
    confirmations = min(4, len({row.get("signal_type") for row in cluster}))
    score = max_quality * 0.65 + max_severity * 7 + min(max_z, 12) * 1.5 + confirmations * 5
    if setup_type not in {"admin_observe", "status_change"}:
        score += 6
    return max(1, min(100, int(round(score))))


def _quality(row: dict[str, Any]) -> float:
    value = (row.get("payload") or {}).get("quality_score")
    return float(value) if isinstance(value, (int, float)) else 0.0


def _price(cluster: list[dict[str, Any]]) -> float | None:
    keys = ("current_price", "last_price", "price", "mid", "start_price")
    for row in reversed(cluster):
        payload = row.get("payload") or {}
        for key in keys:
            value = payload.get(key)
            if isinstance(value, (int, float)) and value > 0:
                return float(value)
        metric = row.get("metric_value")
        if (
            row.get("signal_type") in {"price_jump", "price_near_limit_band"}
            and isinstance(metric, (int, float))
            and metric > 0
        ):
            return float(metric)
    return None


def _levels(price: float | None, bias: str) -> tuple[dict[str, float] | None, float | None, float | None, float | None]:
    if price is None or price <= 0 or bias not in {"long", "short"}:
        return None, None, None, None
    if bias == "long":
        return (
            {"low": round(price * 0.997, 6), "high": round(price * 1.003, 6)},
            round(price * 0.99, 6),
            round(price * 1.01, 6),
            round(price * 1.02, 6),
        )
    return (
        {"low": round(price * 0.997, 6), "high": round(price * 1.003, 6)},
        round(price * 1.01, 6),
        round(price * 0.99, 6),
        round(price * 0.98, 6),
    )


def _drivers(cluster: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in cluster:
        payload = row.get("payload") or {}
        out.append(
            {
                "signal_type": row.get("signal_type"),
                "quality_score": payload.get("quality_score"),
                "z_score": row.get("z_score"),
                "delivery_status": row.get("delivery_status") or payload.get("delivery_status") or "unknown",
                "delivery_reason": row.get("delivery_reason") or payload.get("delivery_reason") or "unknown",
                "headline": (payload.get("interpretation") or {}).get("headline_ru")
                or (payload.get("interpretation") or {}).get("headline")
                or row.get("summary"),
            }
        )
    return out


def _nearby_signal(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") or {}
    return {
        "signal_id": row.get("signal_id"),
        "detected_at": row["detected_at_dt"].isoformat() if row.get("detected_at_dt") else row.get("detected_at"),
        "signal_type": row.get("signal_type"),
        "severity": row.get("severity"),
        "quality_score": payload.get("quality_score"),
        "delivery_status": row.get("delivery_status") or payload.get("delivery_status") or "unknown",
        "summary": row.get("summary"),
    }


def _summary(
    first: dict[str, Any],
    setup_type: str,
    bias: str,
    score: int,
    drivers: list[dict[str, Any]],
) -> str:
    ticker = first.get("ticker") or first.get("instrument_id") or "instrument"
    driver_text = ", ".join(str(item.get("signal_type") or "") for item in drivers[:3])
    return f"{ticker}: точка интереса {setup_type}, bias={bias}, score={score}. Драйверы: {driver_text}."


def _poi_id(cluster: list[dict[str, Any]], detected_at: datetime, updated_at: datetime) -> str:
    signal_ids = ",".join(str(row.get("signal_id") or "") for row in cluster)
    base = f"{cluster[0].get('instrument_id')}|{detected_at.isoformat()}|{updated_at.isoformat()}|{signal_ids}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))
