"""Compact Telegram notices for openings, closes and session breaks."""

from datetime import datetime
from html import escape
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.session_open import SESSION_VERSION, SessionBoundary


ACTIONS = {"open": "Открытие торгов", "close": "Закрытие торгов",
           "break_start": "Перерыв в торгах", "break_end": "Возобновление торгов"}


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
    # Existing v1 outbox records remain renderable during an upgrade.
    at = datetime.fromisoformat(payload.get("event_at") or payload["opens_at"])
    display = at.astimezone(ZoneInfo(payload["display_timezone"]))
    event_kind = payload.get("event_kind", "open")
    rows = payload.get("events") or [payload]
    fx_only = all(row["kind"] == "fx_center" for row in rows)
    action = ACTIONS[event_kind]
    if fx_only:
        action = "Начало валютной сессии" if event_kind == "open" else "Завершение валютной сессии"
    display_name = {"Europe/Istanbul": "Стамбул", "Europe/Moscow": "МСК"}.get(
        payload["display_timezone"], payload["display_timezone"])
    lines = [f"🔔 <b>{action}</b> · {display:%H:%M} ({escape(display_name)})"]
    for row in rows:
        local = at.astimezone(ZoneInfo(row["timezone_name"]))
        label = " · валюты" if row["kind"] == "fx_center" else ""
        lines.append(f'<b><a href="{escape(row["source_url"], quote=True)}">{escape(row["title"])}</a></b>'
                     f'{label} · {local:%H:%M} местное')
        lines.append(escape(row["instruments"]))
        if row.get("note"):
            lines.append(escape(row["note"]))
    if any(row["kind"] == "fx_center" for row in rows):
        lines.append("Валютные центры: условные часы активности, Forex продолжает торговаться между сессиями.")
    result = "\n".join(lines)
    if len(result) > 4096:
        raise ValueError("session notice exceeds Telegram limit")
    return result
