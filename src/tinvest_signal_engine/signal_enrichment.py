"""Обогащение сигнала перед записью в БД, Kafka и мессенджеры."""

from __future__ import annotations

from dataclasses import replace

from .models import TriggerSignal
from .signal_interpretation import build_signal_interpretation
from .signal_locale import build_summary_ru, build_telegram_html
from .signal_quality import compute_signal_quality
from .terminal_links import (
    t_invest_instrument_url,
    t_invest_terminal_open_chart_url,
)


def enrich_signal_for_delivery(
    signal: TriggerSignal,
    *,
    llm_service: Any | None = None,
) -> TriggerSignal:
    if signal.signal_type in {"market_session_open", "market_session_close", "market_session_break_start", "market_session_break_end"}:
        from .adapters.session_open_message import render_session_boundary_html

        return replace(signal, payload={
            **signal.payload, "summary_ru": signal.summary,
            "telegram_html": render_session_boundary_html(signal.payload),
        })
    if signal.signal_type == "daily_futures_position_report":
        from .adapters.daily_positions_report import render_daily_positions_html

        # A dated bulletin is not a trading signal: no synthetic quality or
        # probability, and no terminal link to an invented tradable ticker.
        return replace(signal, payload={
            **signal.payload, "summary_ru": signal.summary,
            "telegram_html": render_daily_positions_html(signal.payload),
        })
    quality = compute_signal_quality(signal)
    summary_en = signal.payload.get("summary_en", signal.summary)
    instrument_url = t_invest_instrument_url(
        ticker=signal.ticker, class_code=signal.class_code
    )
    p = dict(signal.payload or {})
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
    interpretation = build_signal_interpretation(signal)

    # Optional LLM enrichment
    if llm_service is not False:
        effective_llm = llm_service
        if effective_llm is None:
            from .adapters.signal_llm_factory import get_global_llm_service

            effective_llm = get_global_llm_service()

        if effective_llm is not None and "llm_analysis" not in p:
            q_score = quality.get("quality_score")
            q_float = float(q_score) if q_score is not None else None
            p = effective_llm.enrich_payload(
                ticker=signal.ticker,
                signal_type=signal.signal_type,
                payload=p,
                quality_score=q_float,
                headline_ru=str(interpretation.get("headline_ru") or ""),
            )
            signal = replace(signal, payload=p)

    summary_ru = build_summary_ru(signal, quality)
    tg_html = build_telegram_html(
        signal,
        quality,
        ticker_terminal_url=terminal_search_url,
        instrument_page_url=instrument_url,
    )
    payload = {
        **p,
        "summary_en": summary_en,
        "summary_ru": summary_ru,
        "terminal_url": terminal_search_url,
        "instrument_page_url": instrument_url,
        "telegram_html": tg_html,
        "interpretation": interpretation,
        "interpretation_ru": interpretation.get("headline_ru"),
        **quality,
    }
    return replace(signal, summary=summary_ru, payload=payload)
