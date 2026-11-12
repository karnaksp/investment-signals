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
    assert "<b>Трейдинг-деск</b> · SBER" in html_out
    assert "[ЛОНГ]" in html_out
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
