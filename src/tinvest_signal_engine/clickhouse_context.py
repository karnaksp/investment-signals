"""Чтение сырых событий из ClickHouse по HTTP (JSONEachRow), без отдельного драйвера."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _escape_ch_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _ch_dt(value: datetime) -> str:
    normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return normalized.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _query_json_rows(
    base_url: str,
    query: str,
    *,
    username: str | None = None,
    password: str | None = None,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
    auth = None
    if username and password:
        auth = (username.strip(), password)
    with httpx.Client(timeout=timeout, auth=auth) as client:
        response = client.post(base_url.rstrip("/"), content=query.encode("utf-8"))
        response.raise_for_status()
    text = response.text.strip()
    if not text:
        return []
    rows: list[dict[str, Any]] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def fetch_raw_events_window(
    base_url: str,
    *,
    instrument_id: str,
    start: datetime,
    end: datetime,
    limit: int = 500,
    username: str | None = None,
    password: str | None = None,
) -> list[dict[str, Any]]:
    """События из ``signal_engine.market_raw_events`` в полуинтервале ``[start, end)``."""
    bu = base_url.rstrip("/")
    inst = _escape_ch_string(instrument_id.strip())
    if not inst:
        return []
    st = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    en = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    t0 = st.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t1 = en.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    lim = max(1, min(int(limit), 2_000))
    query = (
        "SELECT event_type, source_time, received_at, "
        "length(payload_json) AS payload_len "
        "FROM signal_engine.market_raw_events "
        f"WHERE instrument_id = '{inst}' "
        f"AND source_time >= toDateTime64('{t0}', 3, 'UTC') "
        f"AND source_time < toDateTime64('{t1}', 3, 'UTC') "
        "ORDER BY source_time ASC "
        f"LIMIT {lim} FORMAT JSONEachRow"
    )
    return _query_json_rows(bu, query, username=username, password=password)


def fetch_source_health(
    base_url: str,
    *,
    minutes: int = 1440,
    username: str | None = None,
    password: str | None = None,
) -> list[dict[str, Any]]:
    """Latest raw-event timestamp per instrument/source from ClickHouse."""
    bu = base_url.rstrip("/")
    m = max(1, min(int(minutes), 10_080))
    query = (
        "SELECT instrument_id, event_type, max(source_time) AS last_source_time, "
        "count() AS event_count "
        "FROM signal_engine.market_raw_events "
        f"WHERE source_time >= now() - INTERVAL {m} MINUTE "
        "GROUP BY instrument_id, event_type "
        "ORDER BY instrument_id ASC, event_type ASC "
        "FORMAT JSONEachRow"
    )
    return _query_json_rows(bu, query, username=username, password=password)


def fetch_instrument_insights(
    base_url: str,
    *,
    instrument_id: str,
    now: datetime | None = None,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """Current per-instrument market microstructure card from raw ClickHouse data.

    All values are derived from locally stored market-data events. Participant
    classes and real long/short positions are intentionally marked unavailable:
    they cannot be recovered from ordinary trade/order-book events.
    """

    bu = base_url.rstrip("/")
    inst = _escape_ch_string(instrument_id.strip())
    if not inst:
        return {"status": "missing_instrument", "instrument_id": instrument_id}

    local_now = (now or datetime.now(MOSCOW_TZ)).astimezone(MOSCOW_TZ)
    today_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = local_now - today_start
    previous_start = today_start - timedelta(days=1)
    previous_end = previous_start + elapsed
    today_start_utc = _ch_dt(today_start)
    now_utc = _ch_dt(local_now)
    previous_start_utc = _ch_dt(previous_start)
    previous_end_utc = _ch_dt(previous_end)
    recent_start_utc = _ch_dt(local_now - timedelta(minutes=60))

    trade_query = (
        "SELECT period, "
        "min(source_time) AS first_trade_at, "
        "max(source_time) AS last_trade_at, "
        "count() AS trade_count, "
        "sum(qty) AS quantity_lots, "
        "sum(qty * px) AS turnover_raw, "
        "sumIf(qty, positionCaseInsensitive(direction, 'BUY') > 0) AS aggressive_buy_lots, "
        "sumIf(qty, positionCaseInsensitive(direction, 'SELL') > 0) AS aggressive_sell_lots, "
        "sumIf(qty * px, positionCaseInsensitive(direction, 'BUY') > 0) AS aggressive_buy_turnover_raw, "
        "sumIf(qty * px, positionCaseInsensitive(direction, 'SELL') > 0) AS aggressive_sell_turnover_raw, "
        "avg(px) AS avg_trade_price "
        "FROM ("
        "SELECT if(source_time >= toDateTime64('{today_start}', 3, 'UTC'), 'today', 'previous_same_time') AS period, "
        "source_time, "
        "JSONExtractFloat(payload_json, 'quantity') AS qty, "
        "toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0)) "
        "+ toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0)) / 1000000000. AS px, "
        "JSONExtractString(payload_json, 'direction') AS direction "
        "FROM signal_engine.market_raw_events "
        "WHERE instrument_id = '{inst}' AND event_type = 'trade' "
        "AND ((source_time >= toDateTime64('{today_start}', 3, 'UTC') "
        "AND source_time < toDateTime64('{now}', 3, 'UTC')) "
        "OR (source_time >= toDateTime64('{previous_start}', 3, 'UTC') "
        "AND source_time < toDateTime64('{previous_end}', 3, 'UTC')))"
        ") "
        "WHERE qty > 0 AND px > 0 AND isFinite(qty) AND isFinite(px) "
        "GROUP BY period FORMAT JSONEachRow"
    ).format(
        inst=inst,
        today_start=today_start_utc,
        now=now_utc,
        previous_start=previous_start_utc,
        previous_end=previous_end_utc,
    )
    trade_rows = _query_json_rows(bu, trade_query, username=username, password=password)
    trades_by_period = {str(row.get("period")): row for row in trade_rows}

    orderbook_query = (
        "WITH "
        "JSONExtractArrayRaw(payload_json, 'bids') AS bids, "
        "JSONExtractArrayRaw(payload_json, 'asks') AS asks, "
        "arraySlice(bids, 1, 5) AS top_bids, "
        "arraySlice(asks, 1, 5) AS top_asks, "
        "arrayElement(bids, 1) AS best_bid, "
        "arrayElement(asks, 1) AS best_ask, "
        "toFloat64(coalesce(JSONExtractInt(best_bid, 'price', 'units'), 0)) "
        "+ toFloat64(coalesce(JSONExtractInt(best_bid, 'price', 'nano'), 0)) / 1000000000. AS bid_px, "
        "toFloat64(coalesce(JSONExtractInt(best_ask, 'price', 'units'), 0)) "
        "+ toFloat64(coalesce(JSONExtractInt(best_ask, 'price', 'nano'), 0)) / 1000000000. AS ask_px, "
        "arraySum(arrayMap(x -> JSONExtractFloat(x, 'quantity'), top_bids)) AS bid_qty_5, "
        "arraySum(arrayMap(x -> JSONExtractFloat(x, 'quantity'), top_asks)) AS ask_qty_5, "
        "greatest(bid_qty_5, ask_qty_5) / greatest(least(bid_qty_5, ask_qty_5), 1) AS wall_ratio "
        "SELECT source_time AS last_orderbook_at, "
        "bid_px AS best_bid, ask_px AS best_ask, "
        "if(bid_px > 0 AND ask_px > 0, (ask_px + bid_px) / 2, 0) AS mid_price, "
        "if(bid_px > 0 AND ask_px > 0, (ask_px - bid_px) / ((ask_px + bid_px) / 2) * 10000, 0) AS spread_bps, "
        "bid_qty_5, ask_qty_5, bid_qty_5 + ask_qty_5 AS visible_depth_5, "
        "if(bid_qty_5 + ask_qty_5 > 0, (bid_qty_5 - ask_qty_5) / (bid_qty_5 + ask_qty_5), 0) AS imbalance_ratio, "
        "wall_ratio "
        "FROM signal_engine.market_raw_events "
        "WHERE instrument_id = '{inst}' AND event_type = 'orderbook' "
        "AND source_time >= toDateTime64('{today_start}', 3, 'UTC') "
        "ORDER BY source_time DESC LIMIT 1 FORMAT JSONEachRow"
    ).format(inst=inst, today_start=today_start_utc)
    orderbook_rows = _query_json_rows(bu, orderbook_query, username=username, password=password)
    orderbook = orderbook_rows[0] if orderbook_rows else {}

    iceberg_query = (
        "SELECT "
        "count() AS price_levels, "
        "max(trade_count) AS max_same_price_prints, "
        "max(quantity_lots) AS max_same_price_lots, "
        "sum(if(trade_count >= 5, 1, 0)) AS repeated_price_levels "
        "FROM ("
        "SELECT round(px, 4) AS price_level, count() AS trade_count, sum(qty) AS quantity_lots "
        "FROM ("
        "SELECT JSONExtractFloat(payload_json, 'quantity') AS qty, "
        "toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0)) "
        "+ toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0)) / 1000000000. AS px "
        "FROM signal_engine.market_raw_events "
        "WHERE instrument_id = '{inst}' AND event_type = 'trade' "
        "AND source_time >= toDateTime64('{recent_start}', 3, 'UTC') "
        "AND source_time < toDateTime64('{now}', 3, 'UTC')"
        ") WHERE qty > 0 AND px > 0 "
        "GROUP BY price_level"
        ") FORMAT JSONEachRow"
    ).format(inst=inst, recent_start=recent_start_utc, now=now_utc)
    iceberg_rows = _query_json_rows(bu, iceberg_query, username=username, password=password)
    iceberg = iceberg_rows[0] if iceberg_rows else {}

    today = trades_by_period.get("today", {})
    previous = trades_by_period.get("previous_same_time", {})
    today_turnover = _float(today.get("turnover_raw"))
    previous_turnover = _float(previous.get("turnover_raw"))
    today_qty = _float(today.get("quantity_lots"))
    previous_qty = _float(previous.get("quantity_lots"))
    buy_qty = _float(today.get("aggressive_buy_lots"))
    sell_qty = _float(today.get("aggressive_sell_lots"))
    total_directional_qty = buy_qty + sell_qty
    imbalance_ratio = _float(orderbook.get("imbalance_ratio"))
    wall_ratio = _float(orderbook.get("wall_ratio"))
    repeated_price_levels = _int(iceberg.get("repeated_price_levels"))
    max_same_price_prints = _int(iceberg.get("max_same_price_prints"))

    possible_iceberg_score = 0
    possible_iceberg_reasons: list[str] = []
    if repeated_price_levels >= 2:
        possible_iceberg_score += 35
        possible_iceberg_reasons.append("несколько цен с повторяющимися принтами")
    if max_same_price_prints >= 10:
        possible_iceberg_score += 30
        possible_iceberg_reasons.append("много сделок на одной цене")
    if wall_ratio >= 3:
        possible_iceberg_score += 20
        possible_iceberg_reasons.append("заметная видимая стена в стакане")
    if abs(imbalance_ratio) >= 0.45:
        possible_iceberg_score += 15
        possible_iceberg_reasons.append("сильный дисбаланс верхних уровней")

    return {
        "status": "ok",
        "instrument_id": instrument_id,
        "as_of": local_now.isoformat(),
        "windows": {
            "today_start_moscow": today_start.isoformat(),
            "previous_start_moscow": previous_start.isoformat(),
            "previous_end_moscow": previous_end.isoformat(),
            "recent_microstructure_minutes": 60,
        },
        "volume": {
            "source": "observed_trades",
            "today": _trade_period_payload(today),
            "previous_same_time": _trade_period_payload(previous),
            "turnover_ratio_today_to_previous": _ratio(today_turnover, previous_turnover),
            "quantity_ratio_today_to_previous": _ratio(today_qty, previous_qty),
        },
        "aggressive_flow": {
            "source": "observed_trade_direction",
            "buy_lots": buy_qty,
            "sell_lots": sell_qty,
            "buy_share": buy_qty / total_directional_qty if total_directional_qty else None,
            "sell_share": sell_qty / total_directional_qty if total_directional_qty else None,
            "net_lots": buy_qty - sell_qty,
            "note": "Это агрессивные сделки из потока, а не реальные long/short позиции.",
        },
        "orderbook": {
            "source": "latest_observed_orderbook",
            "last_orderbook_at": orderbook.get("last_orderbook_at"),
            "best_bid": _nullable_float(orderbook.get("best_bid")),
            "best_ask": _nullable_float(orderbook.get("best_ask")),
            "mid_price": _nullable_float(orderbook.get("mid_price")),
            "spread_bps": _nullable_float(orderbook.get("spread_bps")),
            "bid_qty_top5": _nullable_float(orderbook.get("bid_qty_5")),
            "ask_qty_top5": _nullable_float(orderbook.get("ask_qty_5")),
            "visible_depth_top5": _nullable_float(orderbook.get("visible_depth_5")),
            "imbalance_ratio": _nullable_float(orderbook.get("imbalance_ratio")),
            "wall_ratio": _nullable_float(orderbook.get("wall_ratio")),
        },
        "hidden_liquidity": {
            "source": "heuristic_from_public_market_events",
            "status": "estimated",
            "possible_iceberg_score": min(100, possible_iceberg_score),
            "repeated_price_levels": repeated_price_levels,
            "max_same_price_prints": max_same_price_prints,
            "max_same_price_lots": _nullable_float(iceberg.get("max_same_price_lots")),
            "reasons": possible_iceberg_reasons,
            "caveat": "Это вероятностная оценка, не прямые данные о скрытых заявках.",
        },
        "unavailable": {
            "real_long_short_positions": {
                "status": "unavailable",
                "reason": "Нужны прямые данные биржи/брокера по позициям участников; из сделок и стакана это не восстанавливается.",
            },
            "legal_vs_individual_participants": {
                "status": "unavailable",
                "reason": "Класс участника не присутствует в обычном рыночном потоке.",
            },
            "official_moex_paid_statistics": {
                "status": "not_reconstructed",
                "reason": "Можно строить прокси по публичным событиям, но нельзя честно назвать их официальной платной статистикой MOEX.",
            },
        },
    }


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _int(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _nullable_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    parsed = _float(value)
    return parsed


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None


def _trade_period_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "first_trade_at": row.get("first_trade_at"),
        "last_trade_at": row.get("last_trade_at"),
        "trade_count": _int(row.get("trade_count")),
        "quantity_lots": _float(row.get("quantity_lots")),
        "turnover_raw": _float(row.get("turnover_raw")),
        "avg_trade_price": _nullable_float(row.get("avg_trade_price")),
    }
