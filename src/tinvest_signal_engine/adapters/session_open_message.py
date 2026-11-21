"""Compact Telegram notices for openings, closes and session breaks."""

from html import escape
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.session_open import SESSION_VERSION, SessionBoundary


ACTIONS = {
    "open": "Открытие торгов",
    "close": "Закрытие торгов",
    "break_start": "Перерыв",
    "break_end": "Возобновление торгов",
}

MARKET_DESCRIPTIONS = {
    "shfe_day": "Сырьевые фьючерсы",
    "shfe_night": "Драгметаллы",
    "shfe_night_base": "Промметаллы",
    "shfe_night_industrials": "Промтовары",
    "cme_resources": "Металлы и энергоносители",
    "dalian": "Промтовары и агросырьё",
    "zhengzhou": "Агросырьё и промтовары",
    "ine": "Сырьевые фьючерсы",
    "ine_night": "Нефть",
    "cme_grains": "Зерновые и масличные",
    "cme_fx": "Валютные фьючерсы",
    "eurex": "Фьючерсы на акции и ставки",
    "lme": "Промметаллы",
    "ice_brent": "Нефть Brent",
    "moex_morning": "Акции и фьючерсы",
}

MARKET_TITLES = {
    "shfe_night": "Шанхай · SHFE · ночь",
    "shfe_night_base": "Шанхай · SHFE · ночь",
    "shfe_night_industrials": "Шанхай · SHFE · ночь",
    "cme_grains": "Чикаго · CBOT",
    "cme_fx": "Чикаго · CME",
}

# Regional clusters for consolidating simultaneous notices into concise summaries
EUROPE_EQUITY_CITIES = {
    "london": "Лондон",
    "frankfurt": "Франкфурт (Xetra)",
    "paris": "Париж",
    "amsterdam": "Амстердам",
    "zurich": "Цюрих",
    "milan": "Милан",
    "madrid": "Мадрид",
    "stockholm": "Стокгольм",
    "helsinki": "Хельсинки",
    "brussels": "Брюссель",
    "lisbon": "Лиссабон",
    "oslo": "Осло",
}

NORTH_AMERICA_EQUITIES = {
    "us_stocks": "США (NYSE, Nasdaq)",
    "toronto": "Канада (TSX)",
    "mexico": "Мексика (BMV)",
    "sao_paulo": "Бразилия (B3)",
}

SHANGHAI_NIGHT_FUTURES = {
    "shfe_night": "драгметаллы",
    "shfe_night_base": "промметаллы",
    "shfe_night_industrials": "промтовары",
    "ine_night": "нефть",
}

UAE_EXCHANGES = {
    "abu_dhabi": "Абу-Даби · ADX",
    "dubai": "Дубай · DFM",
}

MOEX_MAIN = {
    "moex": "акции",
    "moex_futures": "фьючерсы",
}

CME_GLOBEX = {
    "cme_fx": "валюты",
    "cme_resources": "металлы и энергоносители",
}

ASIAN_FX = {
    "fx_hong_kong": "Гонконг",
    "fx_singapore": "Сингапур",
}


def boundary_payload(events: tuple[SessionBoundary, ...], *, display_timezone: str) -> dict:
    ZoneInfo(display_timezone)
    if not events or any((e.event_at, e.event_kind) != (events[0].event_at, events[0].event_kind) for e in events):
        raise ValueError("session notice must contain simultaneous events of the same kind")
    return {
        "session_version": SESSION_VERSION,
        "event_kind": events[0].event_kind,
        "event_at": events[0].event_at.isoformat(),
        "display_timezone": display_timezone,
        "events": [
            {
                "session_id": e.session_id,
                "title": e.title,
                "instruments": e.instruments,
                "timezone_name": e.timezone_name,
                "source_url": e.source_url,
                "kind": e.kind,
                "note": e.note,
            }
            for e in events
        ],
    }


def render_session_boundary_html(payload: dict) -> str:
    # Existing v1 outbox records use the same concise presentation.
    event_kind = payload.get("event_kind", "open")
    rows = payload.get("events") or [payload]
    action = ACTIONS.get(event_kind, "Открытие торгов")

    # Single-session notices retain historical exact line format
    if len(rows) == 1:
        row = rows[0]
        fx_only = row.get("kind") == "fx_center"
        if fx_only:
            action = "Начало валютной сессии" if event_kind == "open" else "Завершение валютной сессии"
        session_id = row.get("session_id", "")
        title = MARKET_TITLES.get(session_id, row.get("title", ""))
        description = (
            "Валюты"
            if fx_only
            else MARKET_DESCRIPTIONS.get(session_id, row.get("instruments", ""))
        )
        url = row.get("source_url", "")
        lines = [
            f"🔔 <b>{action}</b> в",
            f'<b><a href="{escape(url, quote=True)}">{escape(title)}</a></b>',
            escape(description),
        ]
        return "\n".join(lines)

    present_ids = {r.get("session_id") for r in rows}
    consumed_ids: set[str] = set()
    cards: list[dict] = []

    # 1. European Stock Exchanges Cluster
    eur_present = [sid for sid in EUROPE_EQUITY_CITIES if sid in present_ids]
    europe_is_cluster = len(eur_present) >= 3
    if europe_is_cluster:
        has_fx_london = "fx_london" in present_ids and event_kind == "open"
        consumed_ids.update(eur_present)
        if has_fx_london:
            consumed_ids.add("fx_london")
        cities = [
            "Лондон (LSE, FX)" if sid == "london" and has_fx_london else EUROPE_EQUITY_CITIES[sid]
            for sid in eur_present
        ]
        url = "https://www.euronext.com/en/trading/trading-hours-holidays"
        desc_inst = (
            "Акции, европейские фондовые индексы и валютная сессия"
            if has_fx_london
            else "Акции и европейские фондовые индексы (STOXX 600, DAX, FTSE, CAC)"
        )
        cards.append({
            "title": "Европейские биржи",
            "url": url,
            "descriptions": [" · ".join(cities), desc_inst],
        })

    # 2. North America Equities Cluster
    na_present = [sid for sid in ("us_stocks", "toronto", "mexico", "sao_paulo") if sid in present_ids]
    if len(na_present) >= 2 and "us_stocks" in na_present:
        consumed_ids.update(na_present)
        has_sa = "sao_paulo" in na_present
        title = "Биржи Америки" if has_sa else "Биржи Северной Америки"
        url = "https://www.nyse.com/markets/hours-calendars"
        cities = " · ".join(NORTH_AMERICA_EQUITIES[sid] for sid in na_present)
        desc = "Акции американских рынков" if has_sa else "Акции и фондовые индексы (S&P 500, Nasdaq, TSX)"
        cards.append({"title": title, "url": url, "descriptions": [cities, desc]})

    # 3. Shanghai Night Commodity Futures Cluster
    night_present = [sid for sid in SHANGHAI_NIGHT_FUTURES if sid in present_ids]
    if len(night_present) >= 2:
        consumed_ids.update(night_present)
        url = "https://www.shfe.cn/eng/reports/CalendarHolidays/TradingHours/"
        commodities = ", ".join(SHANGHAI_NIGHT_FUTURES[sid] for sid in night_present)
        cards.append({
            "title": "Шанхай · SHFE / INE (ночь)",
            "url": url,
            "descriptions": [f"Фьючерсы: {commodities}"],
        })

    # 4. China Daytime Commodities and Equities Cluster
    cday_present = [sid for sid in ("china_stocks", "shfe_day", "dalian", "zhengzhou", "ine") if sid in present_ids]
    if len(cday_present) >= 3:
        consumed_ids.update(cday_present)
        url = "https://www.shfe.cn/eng/reports/CalendarHolidays/TradingHours/"
        if "china_stocks" in cday_present:
            title = "Китай · Фондовые и товарные биржи"
            desc = "Акции (SSE, SZSE) и сырьевые фьючерсы (SHFE, DCE, CZCE, INE)"
        else:
            title = "Китай · Товарные биржи (SHFE, DCE, CZCE, INE)"
            desc = "Сырьевые фьючерсы: металлы, нефть, уголь, агросырьё"
        cards.append({"title": title, "url": url, "descriptions": [desc]})

    # 5. UAE Exchanges Cluster
    uae_present = [sid for sid in UAE_EXCHANGES if sid in present_ids]
    if len(uae_present) == 2:
        consumed_ids.update(uae_present)
        cards.append({
            "title": "ОАЭ · ADX / DFM",
            "url": "https://www.dfm.ae/investing/services/how-to-trade",
            "descriptions": ["Акции ОАЭ · Абу-Даби и Дубай"],
        })

    # 6. MOEX Main Session Cluster
    moex_present = [sid for sid in MOEX_MAIN if sid in present_ids]
    if len(moex_present) == 2:
        consumed_ids.update(moex_present)
        cards.append({
            "title": "Москва · MOEX",
            "url": "https://www.moex.com/n103379",
            "descriptions": ["Акции и срочный рынок (фьючерсы) · основная сессия"],
        })

    # 7. CME Globex Cluster
    cme_present = [sid for sid in CME_GLOBEX if sid in present_ids]
    if len(cme_present) == 2:
        consumed_ids.update(cme_present)
        cards.append({
            "title": "Чикаго · CME Globex",
            "url": "https://www.cmegroup.com/trading-hours.html",
            "descriptions": ["Товарные и валютные фьючерсы"],
        })

    # 8. Asian FX Hubs Cluster
    as_fx_present = [sid for sid in ASIAN_FX if sid in present_ids]
    if len(as_fx_present) == 2:
        consumed_ids.update(as_fx_present)
        cards.append({
            "title": "Азия · Валютные центры",
            "url": "https://www.oanda.com/us-en/skills-and-insights/education/trading-asset-classes/forex/how-to-choose-forex-pair/",
            "descriptions": ["Гонконг и Сингапур (валюты)"],
        })

    # Remaining non-consumed rows
    for row in rows:
        sid = row.get("session_id")
        if sid in consumed_ids:
            continue
        title = MARKET_TITLES.get(sid, row.get("title", ""))
        desc = (
            "Валюты"
            if row.get("kind") == "fx_center"
            else MARKET_DESCRIPTIONS.get(sid, row.get("instruments", ""))
        )
        url = row.get("source_url", "")
        existing = next((c for c in cards if c["title"] == title and c["url"] == url), None)
        if existing:
            if desc not in existing["descriptions"]:
                existing["descriptions"].append(desc)
        else:
            cards.append({
                "title": title,
                "url": url,
                "descriptions": [desc],
            })

    # Header determination
    if europe_is_cluster and (len(cards) == 1 or len(eur_present) >= len(rows) - 1):
        if event_kind == "open":
            header = "🔔 <b>Открытие торгов в Европе</b>"
        elif event_kind == "close":
            header = "🔔 <b>Закрытие торгов в Европе</b>"
        else:
            header = f"🔔 <b>{action} в Европе</b>"
    elif len(na_present) >= 2 and len(cards) == 1:
        if event_kind == "open":
            header = "🔔 <b>Открытие торгов в США и Северной Америке</b>"
        elif event_kind == "close":
            header = "🔔 <b>Закрытие торгов в Америке</b>"
        else:
            header = f"🔔 <b>{action}</b>"
    elif len(cday_present) >= 3 and len(cards) == 1:
        if event_kind == "close":
            header = "🔔 <b>Закрытие дневной сессии в Китае</b>"
        elif event_kind == "open":
            header = "🔔 <b>Открытие дневной сессии в Китае</b>"
        else:
            header = f"🔔 <b>{action} на биржах Китая</b>"
    elif len(night_present) >= 2 and len(cards) == 1:
        if event_kind == "open":
            header = "🔔 <b>Открытие ночной сессии в Китае (SHFE / INE)</b>"
        elif event_kind == "close":
            header = "🔔 <b>Закрытие ночной сессии в Китае (SHFE / INE)</b>"
        else:
            header = f"🔔 <b>{action}</b>"
    else:
        fx_only = all(row.get("kind") == "fx_center" for row in rows)
        if fx_only:
            action = "Начало валютной сессии" if event_kind == "open" else "Завершение валютной сессии"
        header = f"🔔 <b>{action}</b>"

    lines = [header]
    for index, c in enumerate(cards):
        if index or lines:
            lines.append("")
        escaped_url = escape(c["url"], quote=True)
        escaped_title = escape(c["title"])
        lines.append(f'<b><a href="{escaped_url}">{escaped_title}</a></b>')
        for d in c["descriptions"]:
            lines.append(escape(d))

    result = "\n".join(lines).strip()
    if len(result) > 4096:
        raise ValueError("session notice exceeds Telegram limit")
    return result
