"""Compact Telegram notices for openings, closes and session breaks."""

from html import escape
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.session_open import SESSION_VERSION, SessionBoundary


ACTIONS = {"open": "Открытие торгов", "close": "Закрытие торгов",
           "break_start": "Перерыв", "break_end": "Возобновление торгов"}

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


def boundary_payload(events: tuple[SessionBoundary, ...], *, display_timezone: str) -> dict:
    ZoneInfo(display_timezone)
    if not events or any((e.event_at, e.event_kind) != (events[0].event_at, events[0].event_kind) for e in events):
        raise ValueError("session notice must contain simultaneous events of the same kind")
    return {
        "session_version": SESSION_VERSION, "event_kind": events[0].event_kind,
        "event_at": events[0].event_at.isoformat(), "display_timezone": display_timezone,
        "events": [{"session_id": e.session_id, "title": e.title, "instruments": e.instruments,
                    "timezone_name": e.timezone_name, "source_url": e.source_url,
                    "kind": e.kind, "note": e.note} for e in events],
    }


def render_session_boundary_html(payload: dict) -> str:
    # Existing v1 outbox records use the same concise presentation.
    event_kind = payload.get("event_kind", "open")
    rows = payload.get("events") or [payload]
    fx_only = all(row["kind"] == "fx_center" for row in rows)
    action = ACTIONS[event_kind]
    if fx_only:
        action = "Начало валютной сессии" if event_kind == "open" else "Завершение валютной сессии"
    lines = [f"🔔 <b>{action}</b> в"]
    markets: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        session_id = row["session_id"]
        title = MARKET_TITLES.get(session_id, row["title"])
        description = ("Валюты" if row["kind"] == "fx_center" else
                       MARKET_DESCRIPTIONS.get(session_id, row["instruments"]))
        descriptions = markets.setdefault((title, row["source_url"]), [])
        if description not in descriptions:
            descriptions.append(description)
    for index, ((title, url), descriptions) in enumerate(markets.items()):
        if index:
            lines.append("")
        lines.append(f'<b><a href="{escape(url, quote=True)}">{escape(title)}</a></b>')
        lines.append(escape(" · ".join(descriptions)))
    result = "\n".join(lines)
    if len(result) > 4096:
        raise ValueError("session notice exceeds Telegram limit")
    return result
