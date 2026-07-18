"""Human-readable, structured interpretation for detector signals."""

from __future__ import annotations

import math
from typing import Any

from .models import TriggerSignal


def build_signal_interpretation(signal: TriggerSignal) -> dict[str, Any]:
    """Return a compact human-readable explanation and display facts.

    The detector emits numeric facts. This layer converts them into wording that
    is useful before opening a chart: direction, percent move, estimated money
    turnover, spread, book side, and similar context.
    """
    p = signal.payload or {}
    st = signal.signal_type

    if st == "price_jump":
        return _price_jump(signal, p)
    if st == "bond_maturity_convergence":
        return _bond_maturity_convergence(signal, p)
    if st == "volume_spike":
        return _volume_spike(signal, p)
    if st == "trade_rate_spike":
        return _trade_rate_spike(signal, p)
    if st == "spread_widening":
        return _spread_widening(signal, p)
    if st == "orderbook_imbalance":
        return _orderbook_imbalance(signal, p)
    if st == "obi_dynamics":
        return _obi_dynamics(signal, p)
    if st in {"microstructure_combo_long", "microstructure_combo_short"}:
        return _microstructure_combo(signal, p)
    if st in {"orderbook_spoofing_bid_pull", "orderbook_spoofing_ask_pull"}:
        return _spoofing_pull(signal, p)
    if st == "aggressive_trade_burst":
        return _aggressive_trade_burst(signal, p)
    if st == "lead_lag_divergence":
        return _lead_lag(signal, p)
    if st == "trading_status_changed":
        return _trading_status(signal, p)
    if st == "market_access_changed":
        return _market_access(signal, p)
    if st == "price_near_limit_band":
        return _price_near_limit(signal, p)
    if st == "open_interest_spike":
        return _open_interest(signal, p)
    if st == "candle_range_spike":
        return _candle_range(signal, p)
    if st == "orderbook_snapshot_inconsistent":
        return _orderbook_inconsistent(signal, p)
    return _generic(signal)


def _price_jump(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    pct = _num(p.get("price_change_pct"))
    change = _num(p.get("price_change"))
    start = _num(p.get("start_price"))
    current = _num(p.get("current_price"))
    signed_bps = _num(p.get("price_change_bps"))
    if pct is None and signed_bps is not None:
        pct = signed_bps / 100.0
    if pct is None and start is not None and current is not None and start > 0:
        pct = (current - start) / start * 100.0
    if change is None and start is not None and current is not None:
        change = current - start

    direction_raw = str(p.get("price_direction") or "").lower()
    if direction_raw in {"up", "down"}:
        direction = direction_raw
    elif change is not None:
        direction = "up" if change >= 0 else "down"
    elif signed_bps is not None:
        direction = "up" if signed_bps >= 0 else "down"
    elif pct is not None and p.get("price_change_pct") is not None:
        direction = "up" if pct >= 0 else "down"
    else:
        direction = "unknown"

    if pct is None:
        abs_bps = _num(p.get("abs_price_change_bps"), signal.metric_value)
        pct = abs_bps / 100.0 if abs_bps is not None else None

    if direction == "up":
        headline = f"Цена выросла на {_signed_pct(pct)} за {signal.window_seconds} с"
        direction_label = "вверх"
        pct_label = _signed_pct(pct)
    elif direction == "down":
        headline = f"Цена снизилась на {_signed_pct(pct)} за {signal.window_seconds} с"
        direction_label = "вниз"
        pct_label = _signed_pct(pct)
    else:
        headline = (
            f"Цена изменилась примерно на {_pct(pct)} за {signal.window_seconds} с"
        )
        direction_label = "неизвестно"
        pct_label = _pct(pct)
    if start is not None and current is not None:
        headline += f": {_price(start)} → {_price(current)}"
    if change is not None:
        headline += f" ({_signed_price(change)})"
    headline += "."
    facts = [
        _fact("Направление", direction_label, "direction"),
        _fact("Изменение", pct_label, "price_change_pct"),
        _fact("Старт", _price(start), "start_price"),
        _fact("Сейчас", _price(current), "current_price"),
        _fact("Δ цены", _signed_price(change), "price_change"),
        _fact("|move|", _bps(signal.metric_value), "abs_price_change_bps"),
        _fact("База", _bps(signal.baseline_value), "baseline_bps"),
    ]
    return _pack(signal, headline, facts, direction=direction)


def _bond_maturity_convergence(
    signal: TriggerSignal, p: dict[str, Any]
) -> dict[str, Any]:
    direction = str(p.get("price_direction") or "unknown")
    direction_ru = (
        "рост к номиналу"
        if direction == "up"
        else "снижение к номиналу"
        if direction == "down"
        else "направление не определено"
    )
    clean_price = _num(p.get("clean_price"))
    target = _num(p.get("target_clean_price"), 100.0)
    sessions = int(_num(p.get("sessions_to_maturity"), 0.0) or 0)
    success_rate = _num(p.get("historical_success_rate"))
    headline = (
        f"Ожидается {direction_ru}: чистая цена {_price(clean_price)} → "
        f"{_price(target)} примерно за {sessions} торговых сессий."
    )
    facts = [
        _fact("Направление", direction_ru, "direction"),
        _fact("Чистая цена", _price(clean_price), "clean_price"),
        _fact("Номинал", _price(target), "target_clean_price"),
        _fact("До погашения", f"{sessions} торговых сессий", "sessions"),
        _fact(
            "Подтверждено историей",
            _pct(success_rate * 100.0 if success_rate is not None else None),
            "historical_success_rate",
        ),
    ]
    return _pack(signal, headline, facts, direction=direction)


def _volume_spike(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    lots = _num(p.get("window_lots"), signal.metric_value)
    units = _num(p.get("window_units"))
    notional = _num(p.get("window_notional"))
    last_price = _num(p.get("last_price"))
    ratio = _ratio(signal.metric_value, signal.baseline_value)
    headline = (
        f"Объём за {signal.window_seconds} с: {_qty(lots)} лотов"
    )
    if units is not None:
        headline += f" / {_qty(units)} шт."
    if notional is not None:
        headline += f", оборот {_money(notional, signal)}"
    if ratio is not None:
        headline += f" ({ratio} к базе)"
    headline += "."
    facts = [
        _fact("Лоты", _qty(lots), "window_lots"),
        _fact("Штуки/контракты", _qty(units), "window_units"),
        _fact("Оборот", _money(notional, signal), "window_notional"),
        _fact("Последняя цена", _price(last_price), "last_price"),
        _fact("База объёма", _qty(signal.baseline_value), "baseline_lots"),
        _fact("К базе", ratio, "ratio_to_baseline"),
    ]
    return _pack(signal, headline, facts, direction="activity")


def _trade_rate_spike(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    count = _num(p.get("trade_count"), signal.metric_value)
    notional = _num(p.get("window_notional"))
    avg = _num(p.get("avg_trade_notional"))
    ratio = _ratio(signal.metric_value, signal.baseline_value)
    headline = f"Частота сделок: {_qty(count)} принтов за {signal.window_seconds} с"
    if notional is not None:
        headline += f", оборот {_money(notional, signal)}"
    if ratio is not None:
        headline += f" ({ratio} к базе)"
    headline += "."
    facts = [
        _fact("Сделки", _qty(count), "trade_count"),
        _fact("Оборот", _money(notional, signal), "window_notional"),
        _fact("Средний чек", _money(avg, signal), "avg_trade_notional"),
        _fact("Лоты", _qty(_num(p.get("window_lots"))), "window_lots"),
        _fact("К базе", ratio, "ratio_to_baseline"),
    ]
    return _pack(signal, headline, facts, direction="activity")


def _spread_widening(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    bid = _num(p.get("best_bid"))
    ask = _num(p.get("best_ask"))
    spread_price = _num(p.get("spread_price"))
    spread_bps = _num(p.get("spread_bps"), signal.metric_value)
    headline = f"Спред расширился до {_bps(spread_bps)}"
    if spread_price is not None:
        headline += f" ({_price(spread_price)})"
    if bid is not None and ask is not None:
        headline += f": bid {_price(bid)}, ask {_price(ask)}"
    headline += "."
    facts = [
        _fact("Спред", _bps(spread_bps), "spread_bps"),
        _fact("Спред в цене", _price(spread_price), "spread_price"),
        _fact("Bid", _price(bid), "best_bid"),
        _fact("Ask", _price(ask), "best_ask"),
        _fact("Bid объём", _qty(_num(p.get("top_bid_qty"))), "top_bid_qty"),
        _fact("Ask объём", _qty(_num(p.get("top_ask_qty"))), "top_ask_qty"),
    ]
    return _pack(signal, headline, facts, direction="liquidity")


def _orderbook_imbalance(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    ratio = _num(p.get("imbalance_ratio"))
    bid_qty = _num(p.get("top_bid_qty"))
    ask_qty = _num(p.get("top_ask_qty"))
    side = str(p.get("dominant_side") or ("bid" if (ratio or 0.5) >= 0.5 else "ask"))
    side_ru = "покупателей bid" if side == "bid" else "продавцов ask"
    share = ratio if side == "bid" else (1.0 - ratio if ratio is not None else None)
    headline = f"В стакане перекос в сторону {side_ru}"
    if share is not None:
        headline += f": {_pct(share * 100)} верхней глубины"
    if bid_qty is not None and ask_qty is not None:
        headline += f" (bid {_qty(bid_qty)} / ask {_qty(ask_qty)})"
    headline += "."
    facts = [
        _fact("Сторона", side, "dominant_side"),
        _fact("Bid share", _pct((ratio or 0) * 100) if ratio is not None else None, "imbalance_ratio"),
        _fact("Bid объём", _qty(bid_qty), "top_bid_qty"),
        _fact("Ask объём", _qty(ask_qty), "top_ask_qty"),
        _fact("Дисбаланс", _pct(_num(p.get("imbalance_abs"), signal.metric_value) * 100), "imbalance_abs"),
    ]
    return _pack(signal, headline, facts, direction=side)


def _obi_dynamics(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    delta = _num(p.get("delta_obi"))
    obi = _num(p.get("obi"))
    direction = "bid" if (delta or 0) > 0 else "ask"
    headline = (
        f"OBI быстро сместился в сторону {direction}: Δ={_signed_number(delta, 3)}"
    )
    if obi is not None:
        headline += f", текущий OBI={_signed_number(obi, 3)}"
    headline += "."
    facts = [
        _fact("OBI", _signed_number(obi, 3), "obi"),
        _fact("Δ OBI", _signed_number(delta, 3), "delta_obi"),
        _fact("Bid объём", _qty(_num(p.get("top_bid_qty"))), "top_bid_qty"),
        _fact("Ask объём", _qty(_num(p.get("top_ask_qty"))), "top_ask_qty"),
    ]
    return _pack(signal, headline, facts, direction=direction)


def _microstructure_combo(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    score = _num(p.get("score"), signal.metric_value)
    min_score = _num(p.get("min_score"), signal.baseline_value)
    detail = p.get("combo_detail") if isinstance(p.get("combo_detail"), dict) else {}
    points = detail.get("points_awarded") if isinstance(detail.get("points_awarded"), dict) else {}
    matched = []
    if points.get("spread"):
        matched.append("спред")
    if points.get("tick_rate"):
        matched.append("частота сделок")
    if points.get("imbalance_long") or points.get("imbalance_short"):
        matched.append("перекос стакана")
    if points.get("delta_long") or points.get("delta_short"):
        matched.append("агрессивная дельта")
    side = "long" if "long" in signal.signal_type else "short"
    headline = f"Комбо {side}: score {_fmt(score, 0)}/{_fmt(min_score, 0)}"
    if matched:
        headline += " за счёт: " + ", ".join(matched)
    headline += "."
    facts = [
        _fact("Score", f"{_fmt(score, 0)}/{_fmt(min_score, 0)}", "score"),
        _fact("Imbalance", _fmt(_num(p.get("imbalance_ratio")), 2), "imbalance_ratio"),
        _fact("Signed delta", _qty(_num(p.get("signed_delta_qty"))), "signed_delta_qty"),
        _fact("Matched", ", ".join(matched), "matched_components"),
    ]
    return _pack(signal, headline, facts, direction=side)


def _spoofing_pull(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    side = str(
        p.get("wall_side")
        or ("ask" if "ask_pull" in signal.signal_type else "bid")
    )
    side_ru = "аска" if side == "ask" else "бида" if side == "bid" else side
    prev = _num(p.get("prev_wall_qty"))
    cur = _num(p.get("cur_wall_qty"))
    drop = None
    if prev and prev > 0 and cur is not None:
        drop = (prev - cur) / prev * 100.0
    headline = f"Снятие крупной стены {side_ru}: объём упал на {_pct(drop)}"
    if prev is not None and cur is not None:
        headline += f" ({_qty(prev)} → {_qty(cur)} лотов)"
    headline += f", mid сдвинулся всего на {_bps(_num(p.get('mid_move_bps')))}."
    facts = [
        _fact("Сторона", side, "wall_side"),
        _fact("Drop", _pct(drop), "drop_pct"),
        _fact("Было", _qty(prev), "prev_wall_qty"),
        _fact("Стало", _qty(cur), "cur_wall_qty"),
        _fact("Mid move", _bps(_num(p.get("mid_move_bps"))), "mid_move_bps"),
    ]
    return _pack(signal, headline, facts, direction=side)


def _aggressive_trade_burst(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    direction = str(p.get("direction") or "")
    direction_ru = "покупок" if direction == "buy" else "продаж" if direction == "sell" else "сделок"
    prints = _num(p.get("print_count"), signal.metric_value)
    qty = _num(p.get("abs_qty_sum"))
    units = _num(p.get("abs_units_sum"))
    notional = _num(p.get("estimated_notional"))
    window_ms = _num(p.get("window_ms"))
    headline = f"Пачка агрессивных {direction_ru}: {_qty(prints)} принтов"
    if window_ms is not None:
        headline += f" за {_fmt(window_ms, 0)} мс"
    if notional is not None:
        headline += f", оценочный оборот {_money(notional, signal)}"
    headline += "."
    facts = [
        _fact("Направление", direction, "direction"),
        _fact("Принты", _qty(prints), "print_count"),
        _fact("Лоты", _qty(qty), "abs_qty_sum"),
        _fact("Штуки/контракты", _qty(units), "abs_units_sum"),
        _fact("Оборот", _money(notional, signal), "estimated_notional"),
    ]
    return _pack(signal, headline, facts, direction=direction)


def _lead_lag(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    leader = _num(p.get("leader_range_bps"), signal.metric_value)
    follower = _num(p.get("follower_range_bps"), signal.z_score)
    leader_id = str(p.get("leader_instrument_id") or "")
    headline = (
        f"Lead-lag: лидер прошёл {_bps(leader)}, а этот инструмент только {_bps(follower)} "
        f"за {signal.window_seconds} с."
    )
    facts = [
        _fact("Лидер", leader_id, "leader_instrument_id"),
        _fact("Leader range", _bps(leader), "leader_range_bps"),
        _fact("Follower range", _bps(follower), "follower_range_bps"),
    ]
    return _pack(signal, headline, facts, direction="lag")


def _trading_status(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    prev = str(p.get("previous_status", "—"))
    cur = str(p.get("current_status", "—"))
    return _pack(
        signal,
        f"Торговый статус изменился: {prev} → {cur}.",
        [_fact("Было", prev, "previous_status"), _fact("Стало", cur, "current_status")],
        direction="status",
    )


def _market_access(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    changes = p.get("changes") if isinstance(p.get("changes"), list) else []
    headline = "Изменился доступ к заявкам: " + (", ".join(map(str, changes)) or "см. payload") + "."
    facts = [
        _fact("Limit orders", str(p.get("limit_order_available_flag")), "limit_order_available_flag"),
        _fact("Market orders", str(p.get("market_order_available_flag")), "market_order_available_flag"),
    ]
    return _pack(signal, headline, facts, direction="status")


def _price_near_limit(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    side = str(p.get("nearest_limit_side") or "")
    side_ru = "верхнего" if side == "upper" else "нижнего"
    dist = _num(p.get("nearest_limit_distance_bps"), signal.metric_value)
    headline = f"Цена рядом с {side_ru} дневным лимитом: расстояние {_bps(dist)}."
    facts = [
        _fact("Сторона лимита", side, "nearest_limit_side"),
        _fact("Расстояние", _bps(dist), "nearest_limit_distance_bps"),
        _fact("Mid", _price(_num(p.get("mid"))), "mid"),
        _fact("Limit up", _price(_num(p.get("limit_up"))), "limit_up"),
        _fact("Limit down", _price(_num(p.get("limit_down"))), "limit_down"),
    ]
    return _pack(signal, headline, facts, direction=side)


def _open_interest(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    oi = _num(p.get("open_interest"), signal.metric_value)
    ratio = _ratio(signal.metric_value, signal.baseline_value)
    headline = f"Открытый интерес: {_qty(oi)}"
    if ratio is not None:
        headline += f" ({ratio} к базе)"
    headline += "."
    facts = [
        _fact("Open interest", _qty(oi), "open_interest"),
        _fact("База", _qty(signal.baseline_value), "baseline_open_interest"),
        _fact("К базе", ratio, "ratio_to_baseline"),
    ]
    return _pack(signal, headline, facts, direction="activity")


def _candle_range(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    range_pct = _num(p.get("range_pct"), signal.metric_value / 100.0)
    headline = f"Свеча с широким диапазоном: {_pct(range_pct)} за интервал."
    facts = [
        _fact("Range", _pct(range_pct), "range_pct"),
        _fact("Open", _price(_num(p.get("open"))), "open"),
        _fact("High", _price(_num(p.get("high"))), "high"),
        _fact("Low", _price(_num(p.get("low"))), "low"),
        _fact("Close", _price(_num(p.get("close"))), "close"),
    ]
    return _pack(signal, headline, facts, direction="volatility")


def _orderbook_inconsistent(signal: TriggerSignal, p: dict[str, Any]) -> dict[str, Any]:
    headline = f"Биржа пометила снимок стакана как несогласованный; mid={_price(_num(p.get('mid'), signal.metric_value))}."
    return _pack(signal, headline, [_fact("Mid", _price(_num(p.get("mid"), signal.metric_value)), "mid")], direction="data_quality")


def _generic(signal: TriggerSignal) -> dict[str, Any]:
    ratio = _ratio(signal.metric_value, signal.baseline_value)
    headline = (
        f"Метрика {signal.signal_type}: {_fmt(signal.metric_value, 4)} "
        f"против базы {_fmt(signal.baseline_value, 4)}"
    )
    if ratio is not None:
        headline += f" ({ratio})"
    headline += "."
    return _pack(
        signal,
        headline,
        [
            _fact("Metric", _fmt(signal.metric_value, 4), "metric_value"),
            _fact("Baseline", _fmt(signal.baseline_value, 4), "baseline_value"),
            _fact("z-score", _fmt(signal.z_score, 2), "z_score"),
        ],
        direction="unknown",
    )


def _pack(
    signal: TriggerSignal,
    headline: str,
    facts: list[dict[str, Any] | None],
    *,
    direction: str,
) -> dict[str, Any]:
    clean_facts = [fact for fact in facts if fact and fact.get("value")]
    return {
        "version": "signal_interpretation_v1",
        "headline_ru": headline,
        "direction": direction,
        "window_seconds": signal.window_seconds,
        "facts": clean_facts,
    }


def _fact(label: str, value: Any, key: str) -> dict[str, Any] | None:
    if value is None or value == "":
        return None
    return {"label": label, "value": str(value), "key": key}


def _num(*values: Any) -> float | None:
    for value in values:
        if value is None or value == "":
            continue
        try:
            out = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(out):
            return out
    return None


def _ratio(value: Any, baseline: Any) -> str | None:
    v = _num(value)
    b = _num(baseline)
    if v is None or b is None or abs(b) < 1e-12:
        return None
    return f"x{_fmt(v / b, 2)}"


def _fmt(value: Any, digits: int = 2) -> str:
    v = _num(value)
    if v is None:
        return "—"
    if abs(v) >= 1_000_000_000:
        return _decimal(v / 1_000_000_000, 2) + " млрд"
    if abs(v) >= 1_000_000:
        return _decimal(v / 1_000_000, 2) + " млн"
    if abs(v) >= 10_000:
        return _decimal(v / 1_000, 1) + " тыс"
    return _decimal(v, digits)


def _decimal(value: float, digits: int) -> str:
    if digits <= 0:
        text = f"{value:.0f}"
    else:
        text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    return text.replace(".", ",")


def _signed_number(value: Any, digits: int = 2) -> str:
    v = _num(value)
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return sign + _decimal(v, digits)


def _pct(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "—"
    return _decimal(v, 2) + "%"


def _signed_pct(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return sign + _decimal(v, 2) + "%"


def _bps(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "—"
    return _decimal(v, 1) + " б.п."


def _qty(value: Any) -> str:
    return _fmt(value, 0)


def _price(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "—"
    return _decimal(v, 4) + " ₽"


def _signed_price(value: Any) -> str:
    v = _num(value)
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return sign + _decimal(v, 4) + " ₽"


def _money(value: Any, signal: TriggerSignal) -> str:
    v = _num(value)
    if v is None:
        return "—"
    suffix = "₽" if signal.class_code.upper() else "в валюте цены"
    return "≈ " + _fmt(v, 2) + " " + suffix
