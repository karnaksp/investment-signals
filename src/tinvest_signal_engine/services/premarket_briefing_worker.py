"""Pre-Market Briefing service ('Утренний компас сессии')."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import sys

from tinvest_signal_engine.adapters.news_provider import RssNewsProvider
from tinvest_signal_engine.adapters.signal_llm_factory import get_global_llm_service
from tinvest_signal_engine.domain.signal_llm import (
    PreMarketBriefingContext,
    format_premarket_briefing_html,
)

logger = logging.getLogger(__name__)


def build_premarket_context(
    *,
    trading_date: str | None = None,
    brent_price: float = 73.5,
    brent_change_pct: float = -0.4,
    gold_price: float = 2680.0,
    gold_change_pct: float = +0.3,
    asia_sentiment: str = "Азия торгуется разнонаправленно (Nikkei +0.2%, Hang Seng -0.5%)",
    us_futures_sentiment: str = "Фьючерсы на S&P 500 около нуля (+0.1%)",
) -> PreMarketBriefingContext:
    now = datetime.now(tz=timezone.utc)
    date_str = trading_date or now.strftime("%Y-%m-%d")

    news_provider = RssNewsProvider()
    overnight_news = news_provider.get_recent_news("IMOEX", max_age_hours=16, limit=5)

    corporate_events = (
        "Сбербанк — публикация сокращенных финансовых результатов по РСБУ за прошлый месяц",
        "Т-Технологии (ТКС) — заседание совета директоров по дивидендной политике",
        "Роснефть — закрытие реестра под промежуточные дивиденды через 3 торговых дня",
    )
    macro_events = (
        "19:00 МСК — Оценка недельной инфляции (Росстат)",
        "16:30 МСК — Данные по первичным заявкам на пособия по безработице в США",
    )

    return PreMarketBriefingContext(
        trading_date=date_str,
        brent_price=brent_price,
        brent_change_pct=brent_change_pct,
        gold_price=gold_price,
        gold_change_pct=gold_change_pct,
        asia_sentiment=asia_sentiment,
        us_futures_sentiment=us_futures_sentiment,
        overnight_news=overnight_news,
        corporate_events=corporate_events,
        macro_events=macro_events,
    )


def run_premarket_briefing(*, preview: bool = False, send: bool = False) -> str | None:
    llm_service = get_global_llm_service()
    if not llm_service:
        logger.error("LLM service is not configured (check GEMINI_API_KEY)")
        return None

    context = build_premarket_context()
    result = llm_service.generate_premarket_briefing(context)
    if not result:
        logger.error("Failed to generate pre-market briefing from LLM")
        return None

    html_content = format_premarket_briefing_html(result)

    if preview or not send:
        print("=== ПРЕМАРКЕТ-БРИФИНГ ===")
        print(html_content)
        print("========================")

    if send:
        from tinvest_signal_engine.config import load_secret
        from tinvest_signal_engine.sinks import TelegramAlertSink
        import os

        token = os.getenv("TELEGRAM_BOT_TOKEN") or load_secret("TELEGRAM_BOT_TOKEN", service_name="premarket_briefing")
        chat_id = os.getenv("TELEGRAM_CHAT_ID") or load_secret("TELEGRAM_CHAT_ID", service_name="premarket_briefing")

        if token and chat_id:
            sink = TelegramAlertSink(bot_token=token, chat_id=chat_id)
            from tinvest_signal_engine.models import TriggerSignal
            dummy_signal = TriggerSignal(
                signal_id=f"briefing-{context.trading_date}",
                detected_at=datetime.now(tz=timezone.utc),
                ticker="MARKET",
                signal_type="premarket_briefing",
                summary=f"Утренний компас сессии: {result.overall_sentiment}",
                payload={"telegram_html": html_content},
            )
            sink.send(dummy_signal)
            logger.info("Pre-market briefing delivered to Telegram chat %s", chat_id)
        else:
            logger.warning("Telegram credentials not configured; briefing generated but not sent")

    return html_content


def main() -> None:
    parser = argparse.ArgumentParser(description="Pre-market morning briefing generator")
    parser.add_argument("--preview", action="store_true", help="Print briefing to stdout")
    parser.add_argument("--send", action="store_true", help="Send briefing to Telegram")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run_premarket_briefing(preview=args.preview or not args.send, send=args.send)


if __name__ == "__main__":
    main()
