"""Обогащение сигнала перед записью в БД, Kafka и мессенджеры."""

from __future__ import annotations

from dataclasses import replace

from .models import TriggerSignal
from .signal_locale import build_summary_ru, build_telegram_html
from .signal_quality import compute_signal_quality
from .terminal_links import (
    t_invest_instrument_url,
    t_invest_terminal_open_chart_url,
)


def enrich_signal_for_delivery(signal: TriggerSignal) -> TriggerSignal:
    quality = compute_signal_quality(signal)
    summary_en = signal.payload.get("summary_en", signal.summary)
    instrument_url = t_invest_instrument_url(
        ticker=signal.ticker, class_code=signal.class_code
    )
    p = signal.payload or {}
    ep = p.get("event_payload") if isinstance(p.get("event_payload"), dict) else {}
    instrument_uid = (
        (p.get("instrument_uid") or p.get("uid") or ep.get("uid") or "")
        or None
    )
    if isinstance(instrument_uid, str):
        instrument_uid = instrument_uid.strip() or None
    terminal_search_url = t_invest_terminal_open_chart_url(
        ticker=signal.ticker,
        instrument_uid=instrument_uid,
        class_code=signal.class_code,
    )
    summary_ru = build_summary_ru(signal, quality)
    tg_html = build_telegram_html(
        signal,
        quality,
        ticker_terminal_url=terminal_search_url,
        instrument_page_url=instrument_url,
    )
    payload = {
        **signal.payload,
        "summary_en": summary_en,
        "summary_ru": summary_ru,
        "terminal_url": terminal_search_url,
        "instrument_page_url": instrument_url,
        "telegram_html": tg_html,
        **quality,
    }
    return replace(signal, summary=summary_ru, payload=payload)
