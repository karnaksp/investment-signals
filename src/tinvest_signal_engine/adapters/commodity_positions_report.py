"""A single bounded Telegram digest; exact quantities remain in its payload."""

from __future__ import annotations

from dataclasses import asdict
import html

from tinvest_signal_engine.domain.commodity_positions import PositionContext, TraderPositionChange


_MARKETS = {
    "gold": "Золото · COMEX", "silver": "Серебро · COMEX",
    "platinum": "Платина · NYMEX", "palladium": "Палладий · NYMEX",
    "copper": "Медь · COMEX", "wti": "Нефть WTI · NYMEX",
    "natural_gas": "Газ Henry Hub · NYMEX", "wheat_srw": "Пшеница SRW · CBOT",
    "brent": "Нефть Brent · ICE",
}
_PREFERENCES = {
    "unchanged": "без сдвига", "flip_long": "перевес в лонг", "flip_short": "перевес в шорт",
    "long_strengthens": "перевес лонгов вырос", "long_weakens": "перевес лонгов снизился",
    "short_strengthens": "перевес шортов вырос", "short_weakens": "перевес шортов снизился", "flat": "баланс",
}


def context_payload(context: PositionContext) -> dict:
    markets = []
    for market in context.markets:
        row = asdict(market)
        row["trading_day"] = market.trading_day.isoformat()
        row["previous_day"] = market.previous_day.isoformat()
        row["unit"] = "contracts"
        row["scope"] = "futures_only_all_maturities"
        markets.append(row)
    return {"weekly_markets": markets, "unavailable_sources": list(context.unavailable_sources)}


def _quantity(value: int, *, signed: bool = False) -> str:
    # Round only presentation, never the stored observation or comparison.
    if abs(value) >= 1_000_000:
        number = f"{value / 1_000_000:+.2f}" if signed else f"{value / 1_000_000:.2f}"
        suffix = "млн"
    elif abs(value) >= 1000:
        number = f"{value / 1000:+.1f}" if signed else f"{value / 1000:.1f}"
        suffix = " тыс."
    else:
        number = f"{value:+}" if signed else str(value)
        suffix = ""
    return number.replace(".", ",").replace("-", "−") + suffix


def _funds(market: dict) -> dict:
    return next(row for row in market["groups"] if row["category"] == "managed_money")


def _net(row: dict) -> str:
    value = row["longs"] - row["shorts"]
    change = value - (row["previous_longs"] - row["previous_shorts"])
    return f"{_quantity(value, signed=True)} (Δ{_quantity(change, signed=True)})"


def render_commodity_positions_html(payload: dict) -> str:
    markets = payload["weekly_markets"]
    lines = ["<b>Сырьё · позиции игроков</b>"]
    if markets:
        leaders = sorted(markets, key=lambda m: abs(
            _funds(m)["longs"] - _funds(m)["shorts"]
            - _funds(m)["previous_longs"] + _funds(m)["previous_shorts"]
        ) / m["open_interest"], reverse=True)[:3]
        lines.extend(("", "<b>Главные сдвиги фондов за неделю</b>"))
        for market in leaders:
            group = TraderPositionChange(**_funds(market))
            percent = f"{group.net_change / market['open_interest'] * 100:+.1f}".replace("-", "−").replace(".", ",")
            lines.append(f"• {_MARKETS[market['commodity']].split(' · ')[0]}: "
                         f"{_PREFERENCES[group.preference]}; Δнетто {percent}% ОИ")
    lines.extend(("", "<b>День · SHFE</b>",
                  f"{payload['trading_day']} к {payload['previous_day']}"))
    categories = {"broker_clients": "Через брокеров", "direct_members": "Напрямую на бирже"}
    for product in payload["products"]:
        lines.append("<b>" + {"au": "Золото", "ag": "Серебро"}[product["product"]] + "</b>")
        for row in product["groups"]:
            lines.append(f"{categories[row['category']]}: Л {_quantity(row['longs'])} "
                         f"(Δ{_quantity(row['long_change'], signed=True)}), "
                         f"Ш {_quantity(row['shorts'])} (Δ{_quantity(row['short_change'], signed=True)})")
            lines.append(f"Нетто {_net(row)}")
    if markets:
        lines.extend(("", "<b>Неделя · CFTC / ICE</b>"))
        pairs = {(m["trading_day"], m["previous_day"]) for m in markets}
        if len(pairs) == 1:
            current, previous = next(iter(pairs))
            lines.append(f"{current} к {previous} · недельные данные")
        for market in markets:
            title = _MARKETS[market["commodity"]]
            if len(pairs) > 1:
                title += f" · {market['trading_day']} к {market['previous_day']}"
            lines.append(f"<b>{title}</b>")
            groups = {r["category"]: r for r in market["groups"]}
            funds = groups["managed_money"]
            lines.append(f"Фонды: Л {_quantity(funds['longs'])} "
                         f"(Δ{_quantity(funds['longs'] - funds['previous_longs'], signed=True)}), "
                         f"Ш {_quantity(funds['shorts'])} "
                         f"(Δ{_quantity(funds['shorts'] - funds['previous_shorts'], signed=True)})")
            lines.append(f"Нетто {_net(funds)}")
            lines.append(f"Производители/торговцы: {_net(groups['producers_merchants'])}")
    if payload["unavailable_sources"]:
        unavailable = []
        for source in payload["unavailable_sources"]:
            name, _, commodity = source.partition(":")
            unavailable.append(name + (": " + _MARKETS[commodity].split(" · ")[0] if commodity in _MARKETS else ""))
        lines.extend(("", "Нет проверенных данных: " + html.escape(", ".join(unavailable))))
    lines.extend(("", "Л/Ш — лонги/шорты; нетто = Л − Ш; Δ — изменение к прошлой дате. "
                  "В контрактах; ОИ = открытый интерес. Рынки не суммируются.",
                  "Фонды — управляющие деньгами; производители/торговцы включают потребителей. "
                  "Их шорт может быть хеджем. Группы SHFE — способ доступа, не физики/юрики.",
                  "Недельный блок — контекст с задержкой, не сигнал в реальном времени."))
    links = [f'<a href="{html.escape(payload["source_url"], quote=True)}">SHFE</a>']
    for source in ("CFTC", "ICE"):
        match = next((m for m in markets if m["source"] == source), None)
        if match:
            links.append(f'<a href="{html.escape(match["source_url"], quote=True)}">{source}</a>')
    lines.append("Источники: " + " · ".join(links))
    message = "\n".join(lines)
    if len(message) > 4096:
        raise ValueError("commodity digest exceeds one Telegram message")
    return message
