"""Application use case and ports for Professional Trade Desk Copilot."""

from __future__ import annotations

import re
from typing import Any, Protocol, Sequence

from tinvest_signal_engine.domain.trader_copilot import (
    InternetNewsItem,
    MarketContextSnapshot,
    TraderCopilotResponse,
    TraderDeskQuery,
)


class InternetNewsSearchPort(Protocol):
    """Port for searching real-time internet financial news."""

    def search_news(
        self, query: str, *, limit: int = 6
    ) -> tuple[InternetNewsItem, ...]: ...


class MarketDataProviderPort(Protocol):
    """Port for fetching live market prices, spreads, orderbook, and signals."""

    def get_market_snapshot(
        self, ticker: str
    ) -> MarketContextSnapshot | None: ...

    def get_recent_signals(
        self, ticker: str, *, limit: int = 5
    ) -> tuple[str, ...]: ...


class TraderLlmPort(Protocol):
    """Port for consulting the LLM as a senior prop-desk trader."""

    def consult_trader(
        self,
        query: TraderDeskQuery,
        *,
        market_context: MarketContextSnapshot | None = None,
        news_items: Sequence[InternetNewsItem] = (),
    ) -> TraderCopilotResponse: ...


# Common Russian colloquial words and stems to standard ticker mapping
TICKER_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"(сбер[а-яё]*|sber)", "SBER"),
    (r"(газпром[а-яё]*|gazp)", "GAZP"),
    (r"(лукойл[а-яё]*|lkoh)", "LKOH"),
    (r"(роснефт[а-яё]*|rosn)", "ROSN"),
    (r"(новатэк[а-яё]*|nvtk)", "NVTK"),
    (r"(втб[а-яё]*|vtbr)", "VTBR"),
    (r"(яндекс[а-яё]*|yndx)", "YNDX"),
    (r"(магнит[а-яё]*|mgnt)", "MGNT"),
    (r"(тиньк[а-яё]*|т-технолог[а-яё]*|\bткс\b|\btcs\b|\bт\b)", "T"),
    (r"(полюс[а-яё]*|plzl)", "PLZL"),
    (r"(алрос[а-яё]*|alrs)", "ALRS"),
    (r"(северстал[а-яё]*|chmf)", "CHMF"),
    (r"(нлмк[а-яё]*|nlmk)", "NLMK"),
    (r"(мосбирж[а-яё]*|moex)", "MOEX"),
    (r"(аэрофлот[а-яё]*|aflt)", "AFLT"),
    (r"(сургут[а-яё]*|sngs)", "SNGS"),
    (r"(\bмтс\b|mtss)", "MTSS"),
    (r"(нефт[а-яё]*|brent|брент|wti|\bbr\b)", "BR"),
    (r"(золот[а-яё]*|gold|xau)", "GOLD"),
    (r"(доллар[а-яё]*|бакс[а-яё]*|сишк[а-яё]*|\bsi\b)", "Si"),
    (r"(юан[а-яё]*|\bcny\b|\bcr\b)", "CR"),
    (r"(индекс[а-яё]*|мамб[а-яё]*|imoex|\bmx\b)", "IMOEX"),
)


def extract_ticker_from_text(text: str) -> str | None:
    """Extracts known instrument ticker from text or returns None."""
    if not text:
        return None
    lower = text.lower()
    for pattern, ticker in TICKER_PATTERNS:
        regex = rf"(?:^|[^\wа-яё]){pattern}(?:$|[^\wа-яё])"
        if re.search(regex, lower):
            return ticker

    # Also match uppercase tickers like SBER, GAZP, etc.
    match = re.search(r"\b([A-Z]{2,6})\b", text)
    if match:
        candidate = match.group(1)
        if candidate not in {"THE", "AND", "FOR", "BUY", "SELL", "LONG", "SHORT", "ASK"}:
            return candidate
    return None


class TraderCopilotUseCase:
    """Orchestrates market data fetching, live internet news search, and professional trader LLM consultation."""

    def __init__(
        self,
        *,
        llm: TraderLlmPort,
        market_data: MarketDataProviderPort,
        news_search: InternetNewsSearchPort,
    ) -> None:
        self._llm = llm
        self._market_data = market_data
        self._news_search = news_search

    def answer_query(
        self, query: TraderDeskQuery
    ) -> tuple[TraderCopilotResponse, MarketContextSnapshot | None]:
        # 1. Determine target ticker from query or replied message
        ticker = query.ticker
        if not ticker:
            ticker = extract_ticker_from_text(query.query_text)
        if not ticker and query.reply_to_text:
            ticker = extract_ticker_from_text(query.reply_to_text)

        effective_query = query if query.ticker == ticker else TraderDeskQuery(
            query_text=query.query_text,
            ticker=ticker,
            user_id=query.user_id,
            user_name=query.user_name,
            chat_id=query.chat_id,
            message_id=query.message_id,
            reply_to_text=query.reply_to_text,
            created_at=query.created_at,
        )

        # 2. Fetch live market context if ticker is known
        market_context: MarketContextSnapshot | None = None
        if ticker:
            market_context = self._market_data.get_market_snapshot(ticker)

        # 3. Search real-time internet news
        search_term = ticker or query.query_text[:50]
        if ticker in {"BR", "нефть"}:
            search_query = "нефть Brent котировки новости"
        elif ticker in {"GOLD", "золото"}:
            search_query = "золото цена динамика котировки"
        elif ticker in {"Si", "доллар"}:
            search_query = "курс доллара рубль новости ЦБ"
        elif ticker in {"CR", "юань"}:
            search_query = "курс юаня МосБиржа юань"
        elif ticker:
            search_query = f"{ticker} акции новости МосБиржа"
        else:
            search_query = f"{query.query_text} рынок новости"

        news_items = self._news_search.search_news(search_query, limit=5)

        # 4. Consult Trader LLM
        response = self._llm.consult_trader(
            effective_query,
            market_context=market_context,
            news_items=news_items,
        )

        return response, market_context
