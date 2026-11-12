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

        if effective_llm is not None:
            # 2. Morning Retracement Guard
            if signal.signal_type == "morning_retracement_recommendation" and "retracement_guard" not in p:
                p = effective_llm.enrich_retracement_signal(p, ticker=signal.ticker)
                signal = replace(signal, payload=p)

            # 4. Derivatives & Spread Anomaly Interpretation
            elif signal.signal_type in {
                "calendar_spread_dislocation",
                "roll_migration_anomaly",
                "futures_curve_kink",
                "term_premium_shock",
                "futures_basis_momentum",
                "bond_par_convergence",
            } and "derivatives_interpretation" not in p:
                chain_id = str(p.get("chain_id") or p.get("series_id") or signal.ticker)
                p = effective_llm.enrich_derivatives_anomaly(
                    p,
                    chain_id=chain_id,
                    signal_type=signal.signal_type,
                    z_score=float(signal.z_score or 0.0),
                    metric_value=float(signal.metric_value or 0.0),
                    baseline_value=float(signal.baseline_value or 0.0),
                    ticker=signal.ticker,
                )
                signal = replace(signal, payload=p)

            # 6. Dividend Gap Guard (Check sharp downward jumps for ex-dividend dates)
            elif signal.signal_type in {"price_jump", "candle_range_spike", "confirmed_impulse_short"} and "dividend_gap" not in p:
                raw_ch = p.get("price_change_pct") or p.get("delta_pct") or ep.get("price_change_pct") or ep.get("delta_pct")
                try:
                    price_ch = float(raw_ch) if raw_ch is not None else 0.0
                except (ValueError, TypeError):
                    price_ch = 0.0
                if price_ch < -1.0:
                    div_dec = effective_llm.check_dividend_gap(
                        ticker=signal.ticker, price_change_pct=price_ch, payload=p
                    )
                    if div_dec and div_dec.is_dividend_gap:
                        from .domain.signal_llm import format_dividend_gap_html

                        p["dividend_gap"] = div_dec.to_dict()
                        p["dividend_gap_html"] = format_dividend_gap_html(div_dec)
                        signal = replace(signal, payload=p)

            # General signal LLM analysis
            elif "llm_analysis" not in p:
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

    # Append specialized LLM card representations to Telegram HTML if present
    if p.get("retracement_guard_html"):
        tg_html = f"{tg_html}\n\n{p['retracement_guard_html']}"
    if p.get("derivatives_interpretation_html"):
        tg_html = f"{tg_html}\n\n{p['derivatives_interpretation_html']}"
    if p.get("dividend_gap_html"):
        tg_html = f"{p['dividend_gap_html']}\n\n{tg_html}"
        if p.get("dividend_gap", {}).get("should_suppress_short"):
            summary_ru = f"ℹ️ [ДИВГЭП] {signal.ticker}: плановый экс-дивидендный гэп"
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
