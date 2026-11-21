"""Unit and integration tests for Trade Desk Copilot (Пункт 5)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence
import pytest

from tinvest_signal_engine.adapters.internet_news_search import GoogleNewsRssSearchAdapter
from tinvest_signal_engine.adapters.market_data_provider import CompositeMarketDataProvider
from tinvest_signal_engine.application.trader_copilot import (
    InternetNewsSearchPort,
    MarketDataProviderPort,
    TraderCopilotUseCase,
    TraderLlmPort,
    extract_ticker_from_text,
)
from tinvest_signal_engine.domain.trader_copilot import (
    InternetNewsItem,
    MarketContextSnapshot,
    TraderCopilotResponse,
    TraderDeskQuery,
    format_trader_response_html,
)


def test_extract_ticker_from_text():
    assert extract_ticker_from_text("Что думаешь по Сберу сегодня?") == "SBER"
    assert extract_ticker_from_text("Какой прогноз по акциям сбербанка?") == "SBER"
    assert extract_ticker_from_text("почему падает газпром?") == "GAZP"
    assert extract_ticker_from_text("что с лукойлом?") == "LKOH"
    assert extract_ticker_from_text("куда идет нефть brent?") == "BR"
    assert extract_ticker_from_text("какой курс доллара и сишки?") == "Si"
    assert extract_ticker_from_text("покупать ли золото?") == "GOLD"
    assert extract_ticker_from_text("что по акциям Т-Технологий?") == "T"
    assert extract_ticker_from_text("произвольный текст без тикера") is None


def test_domain_models_and_formatting():
    snapshot = MarketContextSnapshot(
        ticker="SBER",
        last_price=298.50,
        bid_price=298.40,
        ask_price=298.60,
        spread_bps=6.7,
        vwap=297.80,
        positions_ratio_summary="Физики 55% Long / Юрики 68% Short",
        recent_signals_summary=("14:10 · confirmed_impulse_long",),
    )

    response = TraderCopilotResponse(
        ticker="SBER",
        market_stance="BULLISH",
        thesis_title="Удержание выше VWAP и локальный импульс",
        microstructure_analysis="Лента плотная, айсберги покупателя на 298.00.",
        news_drivers_analysis="Позитивные ожидания отчета по РСБУ за октябрь.",
        key_levels=("Поддержка: 297.50", "Сопротивление: 302.00", "Стоп: 296.80"),
        actionable_setup="Лонг от 298.50 с целью 302.00 и стопом за 296.80.",
        risk_factors=("Укрепление рубля", "Общерыночный навес продавца"),
        news_catalysts=("Сбербанк отчитается о рекордной прибыли",),
        model_used="gemini-mock",
        tokens_used=240,
    )

    html_out = format_trader_response_html(response, market_context=snapshot)
    assert "Трейдинг-деск" not in html_out
    assert "Взгляд:" not in html_out
    assert "<b>SBER:</b>" in html_out
    assert "298.50 ₽" in html_out
    assert "Удержание выше VWAP" in html_out
    assert "Поддержка: 297.50" in html_out
    assert "Лонг от 298.50" in html_out
    assert "Сбербанк отчитается о рекордной прибыли" in html_out


class MockNewsSearch(InternetNewsSearchPort):
    def search_news(self, query: str, *, limit: int = 6) -> tuple[InternetNewsItem, ...]:
        return (
            InternetNewsItem(
                title=f"Тестовая новость для {query}",
                source="РБК",
                summary="Краткое содержание новости из сети",
            ),
        )


class MockMarketData(MarketDataProviderPort):
    def get_market_snapshot(self, ticker: str) -> MarketContextSnapshot | None:
        return MarketContextSnapshot(
            ticker=ticker,
            last_price=300.0,
            spread_bps=4.5,
            vwap=299.0,
            recent_signals_summary=(f"10:00 · price_jump on {ticker}",),
        )

    def get_recent_signals(self, ticker: str, *, limit: int = 5) -> tuple[str, ...]:
        return ("10:00 · price_jump",)


class MockTraderLlm(TraderLlmPort):
    def consult_trader(
        self,
        query: TraderDeskQuery,
        *,
        market_context: MarketContextSnapshot | None = None,
        news_items: Sequence[InternetNewsItem] = (),
    ) -> TraderCopilotResponse:
        assert query.ticker == "SBER"
        assert market_context is not None
        assert market_context.last_price == 300.0
        assert len(news_items) > 0
        return TraderCopilotResponse(
            ticker=query.ticker,
            market_stance="BULLISH",
            thesis_title="Лонговый приоритет по Сберу",
            microstructure_analysis="Цена выше VWAP.",
            news_drivers_analysis="Фон умеренно позитивный.",
            key_levels=("Поддержка: 298.00",),
            actionable_setup="Вход на тесте 299.50.",
            risk_factors=(),
            news_catalysts=tuple(n.title for n in news_items),
        )


def test_copilot_use_case_flow():
    use_case = TraderCopilotUseCase(
        llm=MockTraderLlm(),
        market_data=MockMarketData(),
        news_search=MockNewsSearch(),
    )

    query = TraderDeskQuery(query_text="Что думаешь по Сберу?")
    resp, mkt = use_case.answer_query(query)

    assert resp.ticker == "SBER"
    assert resp.market_stance == "BULLISH"
    assert "Лонговый приоритет" in resp.thesis_title
    assert mkt is not None
    assert mkt.last_price == 300.0


def test_google_news_rss_parser():
    xml_data = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>Google News</title>
        <item>
          <title><![CDATA[\xD0\x90\xD0\xBA\xD1\x86\xD0\xB8\xD0\xB8 \xD0\xA1\xD0\xB1\xD0\xB5\xD1\x80\xD0\xB0 \xD0\xB2\xD1\x8B\xD1\x80\xD0\xBE\xD1\x81\xD0\xBB\xD0\xB8 \xD0\xBD\xD0\xB0 2% - \xD0\xA0\xD0\x91\xD0\x9A]]></title>
          <link>https://example.com/sber</link>
          <pubDate>Thu, 12 Nov 2026 12:00:00 GMT</pubDate>
          <description><![CDATA[<p>\xD0\xA0\xD0\xBE\xD1\x81\xD1\x82 \xD0\xBF\xD1\x80\xD0\xBE\xD0\xB4\xD0\xBE\xD0\xBB\xD0\xB6\xD0\xB0\xD0\xB5\xD1\x82\xD1\x81\xD1\x8F</p>]]></description>
        </item>
      </channel>
    </rss>"""
    adapter = GoogleNewsRssSearchAdapter()
    items = adapter._parse_rss_items(xml_data, limit=5)
    assert len(items) == 1
    assert "Акции Сбера выросли на 2%" in items[0].title
    assert items[0].source == "РБК"
    assert "Рост продолжается" in items[0].summary


def test_futures_ticker_and_acronym_filtering():
    from tinvest_signal_engine.application.trader_copilot import (
        extract_ticker_from_text,
        resolve_base_ticker,
    )

    # 1. Futures codes
    t1 = extract_ticker_from_text("GZZ6 Тип: Всплеск объёма Терминал:...")
    assert t1 == "GZZ6"
    assert resolve_base_ticker(t1) == "GAZP"

    t2 = extract_ticker_from_text("Разбор по SiZ6 и курсу рубля")
    assert t2 == "SIZ6"
    assert resolve_base_ticker(t2) == "Si"

    # 2. Acronyms must NOT be matched as tickers
    assert extract_ticker_from_text("Трейдинг-деск · LLM Взгляд: [ШОРТ]") is None
    assert extract_ticker_from_text("Какие мошенники") is None
    assert extract_ticker_from_text("Проблема с API и HTML разметкой") is None


def test_direct_answer_formatting():
    response = TraderCopilotResponse(
        ticker="GZZ6",
        market_stance="NEUTRAL",
        thesis_title="Оценка подтвердилась, импульс затухает",
        microstructure_analysis="",
        news_drivers_analysis="",
        direct_answer="Оценка была верной. Всплеск объема на 13240 не получил продолжения.",
        actionable_setup="Вне позиции до выхода из диапазона 13200-13280.",
    )
    html_out = format_trader_response_html(response)
    assert "Трейдинг-деск" not in html_out
    assert "Взгляд:" not in html_out
    assert "Оценка была верной" in html_out
    assert "Вне позиции до выхода" in html_out
    # Must not contain boilerplate headers when fields are empty
    assert "Микроструктура и лента:" not in html_out
    assert "Новостной фон и драйверы:" not in html_out


def test_rss_spam_filtering():
    xml_data = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <item>
          <title><![CDATA[\xD0\x9C\xD0\xBE\xD1\x88\xD0\xB5\xD0\xBD\xD0\xBD\xD0\xB8\xD0\xBA\xD0\xB8 \xD0\xB8\xD1\x81\xD0\xBF\xD0\xBE\xD0\xBB\xD1\x8C\xD0\xB7\xD1\x83\xD1\x8E\xD1\x82 \xD0\xBD\xD0\xB5\xD0\xB9\xD1\x80\xD0\xBE\xD1\x81\xD0\xB5\xD1\x82\xD0\xB8]]></title>
          <link>https://example.com/scam</link>
          <description><![CDATA[\xD0\xA1\xD1\x85\xD0\xB5\xD0\xBC\xD0\xB0 \xD0\xBE\xD0\xB1\xD0\xBC\xD0\xB0\xD0\xBD\xD0\xB0]]></description>
        </item>
        <item>
          <title><![CDATA[\xD0\x93\xD0\xB0\xD0\xB7\xD0\xBF\xD1\x80\xD0\xBE\xD0\xBC \xD1\x83\xD0\xB2\xD0\xB5\xD0\xBB\xD0\xB8\xD1\x87\xD0\xB8\xD0\xBB \xD0\xBF\xD0\xBE\xD1\x81\xD1\x82\xD0\xB0\xD0\xB2\xD0\xBA\xD0\xB8 \xD0\xB3\xD0\xB0\xD0\xB7\xD0\xB0]]></title>
          <link>https://example.com/gazp</link>
          <description><![CDATA[\xD0\xAD\xD0\xBA\xD1\x81\xD0\xBF\xD0\xBE\xD1\x80\xD1\x82 \xD0\xB2\xD1\x8B\xD1\x80\xD0\xBE\xD1\x81 \xD0\xBD\xD0\xB0 5%]]></description>
        </item>
      </channel>
    </rss>"""
    adapter = GoogleNewsRssSearchAdapter()
    items = adapter._parse_rss_items(xml_data, limit=5)
    # The scam article must be filtered out, only Gazprom kept
    assert len(items) == 1
    assert "Газпром увеличил" in items[0].title


def test_dialogue_chain_tracking():
    from tinvest_signal_engine.services.trader_copilot_service import (
        _record_message,
        _build_dialogue_chain,
    )

    chat_id = 9999
    # 1. First signal
    _record_message(msg_id=1, chat_id=chat_id, text="GZZ6 Всплеск объема", user_name="Система", is_bot=True, reply_to_id=None)
    # 2. User replies to signal
    _record_message(msg_id=2, chat_id=chat_id, text="В итоге правильная оценка была?", user_name="D S", is_bot=False, reply_to_id=1)
    # 3. Bot responds to user
    _record_message(msg_id=3, chat_id=chat_id, text="Оценка подтвердилась...", user_name="Деск", is_bot=True, reply_to_id=2)
    # 4. User replies to bot response
    _record_message(msg_id=4, chat_id=chat_id, text="Какие мошенники", user_name="D S", is_bot=False, reply_to_id=3)

    chain = _build_dialogue_chain(chat_id=chat_id, current_msg_id=4, reply_to_dict=None)
    assert len(chain) == 3
    assert chain[0].text == "GZZ6 Всплеск объема"
    assert chain[1].text == "В итоге правильная оценка была?"
    assert chain[2].text == "Оценка подтвердилась..."
