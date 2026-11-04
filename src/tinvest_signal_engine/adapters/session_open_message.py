"""Short Telegram messages for scheduled openings, with no trading-score decoration."""

from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.session_open import SESSION_OPEN_VERSION, SessionOpening


def opening_payload(opening: SessionOpening, *, display_timezone: str) -> dict:
    ZoneInfo(display_timezone)
    return {
        "session_version": SESSION_OPEN_VERSION, "session_id": opening.session_id,
        "title": opening.title, "instruments": opening.instruments,
        "opens_at": opening.opens_at.isoformat(), "timezone_name": opening.timezone_name,
        "display_timezone": display_timezone, "source_url": opening.source_url,
        "kind": opening.kind, "note": opening.note,
    }


def render_session_open_html(payload: dict) -> str:
    at = datetime.fromisoformat(payload["opens_at"])
    local = at.astimezone(ZoneInfo(payload["timezone_name"]))
    display = at.astimezone(ZoneInfo(payload["display_timezone"]))
    action = "Начало валютной сессии" if payload["kind"] == "fx_center" else "Открытие торгов"
    display_name = {"Europe/Istanbul": "Стамбул", "Europe/Moscow": "МСК"}.get(
        payload["display_timezone"], payload["display_timezone"])
    lines = [f"🔔 <b>{action} · {escape(payload['title'])}</b>",
             escape(payload["instruments"]),
             f"🕒 {local:%H:%M} местное · {display:%H:%M} ({escape(display_name)})"]
    if payload.get("note"):
        lines.append(escape(payload["note"]))
    lines.append(f'<a href="{escape(payload["source_url"], quote=True)}">Расписание</a>')
    return "\n".join(lines)
