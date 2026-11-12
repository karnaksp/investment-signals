"""Automated Loss Attribution service for post-mortem analysis of failed signals."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
from typing import Any

from tinvest_signal_engine.adapters.news_provider import RssNewsProvider
from tinvest_signal_engine.adapters.signal_llm_factory import get_global_llm_service
from tinvest_signal_engine.domain.signal_llm import (
    LossAttributionContext,
    LossAttributionResult,
    format_loss_attribution_html,
)

logger = logging.getLogger(__name__)


def analyze_losing_signal(
    *,
    signal_id: str,
    ticker: str,
    signal_type: str = "price_jump",
    direction: str = "LONG",
    entry_price: float = 300.0,
    exit_price: float = 295.5,
    horizon_seconds: int = 300,
    market_context: str = "Общерыночный откат индекса МосБиржи",
) -> LossAttributionResult | None:
    llm_service = get_global_llm_service()
    if not llm_service:
        logger.error("LLM service is not configured (check GEMINI_API_KEY)")
        return None

    news_provider = RssNewsProvider()
    news = news_provider.get_recent_news(ticker, max_age_hours=6, limit=4)

    loss_pct = abs((exit_price - entry_price) / entry_price * 100.0) if entry_price > 0 else 0.0

    context = LossAttributionContext(
        signal_id=signal_id,
        ticker=ticker,
        signal_type=signal_type,
        direction=direction,
        entry_price=entry_price,
        exit_price=exit_price,
        loss_pct=round(loss_pct, 2),
        horizon_seconds=horizon_seconds,
        subsequent_news=news,
        market_context=market_context,
    )

    return llm_service.analyze_loss_attribution(context)


def run_sample_loss_attribution(*, ticker: str = "SBER", preview: bool = True) -> str | None:
    result = analyze_losing_signal(
        signal_id="sample-loss-001",
        ticker=ticker,
        signal_type="price_jump",
        direction="LONG",
        entry_price=302.5,
        exit_price=297.8,
        horizon_seconds=300,
        market_context="Индекс МосБиржи снижался на 0.4% на фоне укрепления рубля",
    )
    if not result:
        print("Не удалось сформировать постмортем")
        return None

    html = format_loss_attribution_html(result, ticker=ticker)
    if preview:
        print("=== ПОСТМОРТЕМ СИГНАЛА ===")
        print(html)
        print("=========================")
    return html


def main() -> None:
    parser = argparse.ArgumentParser(description="Automated signal loss attribution")
    parser.add_argument("--ticker", default="SBER", help="Ticker for sample post-mortem")
    parser.add_argument("--preview", action="store_true", default=True, help="Print to stdout")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run_sample_loss_attribution(ticker=args.ticker, preview=args.preview)


if __name__ == "__main__":
    main()
