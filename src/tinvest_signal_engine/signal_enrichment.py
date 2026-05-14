"""Обогащение сигнала перед записью в БД, Kafka и мессенджеры."""

from __future__ import annotations

from dataclasses import replace
from functools import lru_cache
import os
from pathlib import Path

from .config import load_instrument_configs
from .models import TriggerSignal
from .signal_locale import build_summary_ru, build_telegram_html
from .signal_quality import compute_signal_quality
from .terminal_links import (
    t_invest_instrument_url,
    t_invest_terminal_open_chart_url,
)


def _instrument_config_path() -> Path:
    raw_path = (
        os.getenv("INSTRUMENTS_CONFIG") or "conf/instruments.yaml"
    ).strip()
    return Path(raw_path).expanduser()


@lru_cache(maxsize=8)
def _instrument_display_name_map(
    path_str: str, mtime_ns: int
) -> dict[str, str]:
    del mtime_ns  # only used to invalidate cache when YAML changes
    configs = load_instrument_configs(Path(path_str))
    return {
        item.instrument_id: item.display_name
        for item in configs
        if item.display_name
    }


def _resolve_instrument_display_name(signal: TriggerSignal) -> str | None:
    path = _instrument_config_path()
    try:
        stat = path.stat()
    except OSError:
        return None
    try:
        mapping = _instrument_display_name_map(
            str(path.resolve()), stat.st_mtime_ns
        )
    except (OSError, ValueError):
        return None
    return mapping.get(signal.instrument_id)


def enrich_signal_for_delivery(signal: TriggerSignal) -> TriggerSignal:
    quality = compute_signal_quality(signal)
    summary_en = signal.payload.get("summary_en", signal.summary)
    instrument_display_name = _resolve_instrument_display_name(signal)
    enriched_signal = replace(
        signal,
        payload={
            **signal.payload,
            "instrument_display_name": instrument_display_name,
        },
    )
    instrument_url = t_invest_instrument_url(
        ticker=signal.ticker, class_code=signal.class_code
    )
    p = signal.payload or {}
    ep = p.get("event_payload") if isinstance(p.get("event_payload"), dict) else {}
    instrument_uid = (
        (
            p.get("instrument_uid")
            or p.get("uid")
            or ep.get("uid")
            or ""
        )
        or None
    )
    if isinstance(instrument_uid, str):
        instrument_uid = instrument_uid.strip() or None
    terminal_search_url = t_invest_terminal_open_chart_url(
        ticker=signal.ticker,
        instrument_uid=instrument_uid,
        class_code=signal.class_code,
    )
    summary_ru = build_summary_ru(enriched_signal, quality)
    tg_html = build_telegram_html(
        enriched_signal,
        quality,
        ticker_terminal_url=terminal_search_url,
        instrument_page_url=instrument_url,
    )
    payload = {
        **signal.payload,
        "summary_en": summary_en,
        "summary_ru": summary_ru,
        "instrument_display_name": instrument_display_name,
        "terminal_url": terminal_search_url,
        "instrument_page_url": instrument_url,
        "telegram_html": tg_html,
        **quality,
    }
    return replace(signal, summary=summary_ru, payload=payload)
