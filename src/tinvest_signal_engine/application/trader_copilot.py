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


# Mapping from Moex futures prefix to underlying asset ticker
FUTURES_PREFIX_MAP: dict[str, str] = {
    "GZ": "GAZP",
    "SR": "SBER",
    "LK": "LKOH",
    "RN": "ROSN",
    "GM": "GMKN",
    "NV": "NVTK",
    "VB": "VTBR",
    "YD": "YNDX",
    "MG": "MGNT",
    "TT": "T",
    "PZ": "PLZL",
    "AL": "ALRS",
    "CH": "CHMF",
    "NL": "NLMK",
    "ME": "MOEX",
    "AF": "AFLT",
    "SN": "SNGS",
    "SP": "SNGSP",
    "MT": "MTSS",
    "BR": "BR",
    "NG": "NG",
    "GD": "GOLD",
    "GL": "GOLD",
    "SV": "SILV",
    "SI": "Si",
    "CR": "CR",
    "MX": "IMOEX",
    "RI": "RTS",
}

# Recognized equity tickers on MOEX
KNOWN_EQUITY_TICKERS: set[str] = {
    "SBER",
    "GAZP",
    "LKOH",
    "ROSN",
    "NVTK",
    "GMKN",
    "VTBR",
    "YNDX",
    "MGNT",
    "T",
    "PLZL",
    "ALRS",
    "CHMF",
    "NLMK",
    "MOEX",
    "AFLT",
    "SNGS",
    "SNGSP",
    "TATN",
    "TATNP",
    "MTSS",
    "IRAO",
    "OZON",
    "POSI",
    "ASTR",
    "SELG",
    "POLY",
    "PHOR",
    "RUAL",
    "MAGN",
    "TRNFP",
    "CBOM",
    "BSPB",
    "HYDR",
    "FEES",
    "PIKK",
    "LSRG",
    "SMLT",
    "AFKS",
    "SGZH",
    "VKCO",
    "HEAD",
    "LEAS",
    "WUSH",
    "SOFL",
    "FLOT",
    "FESH",
    "BR",
    "GOLD",
    "Si",
    "CR",
    "IMOEX",
    "RTS",
    "NG",
}

# Russian search queries for news
COMPANY_SEARCH_NAMES: dict[str, str] = {
    "GAZP": "Газпром",
    "SBER": "Сбербанк",
    "LKOH": "Лукойл",
    "ROSN": "Роснефть",
    "NVTK": "Новатэк",
    "GMKN": "Норникель",
    "VTBR": "ВТБ",
    "YNDX": "Яндекс",
    "MGNT": "Магнит",
    "T": "Т-Технологии",
    "PLZL": "Полюс",
    "ALRS": "Алроса",
    "CHMF": "Северсталь",
    "NLMK": "НЛМК",
    "MOEX": "Московская биржа",
    "AFLT": "Аэрофлот",
    "SNGS": "Сургутнефтегаз",
    "MTSS": "МТС",
    "BR": "Нефть Brent",
    "GOLD": "Золото котировки",
    "Si": "Курс доллара рубль",
    "CR": "Курс юаня",
    "IMOEX": "Индекс МосБиржи",
    "RTS": "Индекс РТС",
    "NG": "Природный газ",
}

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


def resolve_base_ticker(ticker: str) -> str:
    """Resolves futures contract code (e.g. GZZ6, SRZ6) to underlying asset (e.g. GAZP, SBER)."""
    fut_match = re.match(r"^([A-Za-z]{2})[FGHJKMNQUVXZ]\d{1,2}$", ticker)
    if fut_match:
        prefix = fut_match.group(1).upper()
        return FUTURES_PREFIX_MAP.get(prefix, ticker)
    return ticker


def extract_ticker_from_text(text: str) -> str | None:
    """Extracts known instrument ticker or futures code from text, ignoring random uppercase words."""
    if not text:
        return None

    # 1. Match futures contract codes: e.g. GZZ6, SRZ6, SiZ6, BRF7, CRH7
    fut_match = re.search(r"\b([A-Za-z]{2})[FGHJKMNQUVXZ](\d{1,2})\b", text)
    if fut_match:
        return fut_match.group(0).upper()

    # 2. Russian word stems (e.g. "сбер", "газпром", "нефть")
    lower = text.lower()
    for pattern, ticker in TICKER_PATTERNS:
        regex = rf"(?:^|[^\wа-яё]){pattern}(?:$|[^\wа-яё])"
        if re.search(regex, lower):
            return ticker

    # 3. Known uppercase tickers (strict whitelist, avoids acronyms like LLM, HTML, API)
    for word in re.findall(r"\b[A-Za-z]{1,6}\b", text):
        w_up = word.upper()
        if w_up in KNOWN_EQUITY_TICKERS:
            return w_up

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
        base_ticker = resolve_base_ticker(ticker) if ticker else ""
        if ticker:
            # Try specific ticker/contract first, then fallback to base underlying asset
            market_context = self._market_data.get_market_snapshot(ticker)
            if not market_context and base_ticker and base_ticker != ticker:
                market_context = self._market_data.get_market_snapshot(base_ticker)

        # 3. Search real-time internet news (only if ticker is resolved to avoid searching noise)
        news_items: tuple[InternetNewsItem, ...] = ()
        if ticker:
            search_name = COMPANY_SEARCH_NAMES.get(base_ticker or ticker, base_ticker or ticker)
            search_query = f"{search_name} акции котировки новости МосБиржа"
            news_items = self._news_search.search_news(search_query, limit=4)

        # 4. Consult Trader LLM
        response = self._llm.consult_trader(
            effective_query,
            market_context=market_context,
            news_items=news_items,
        )

        return response, market_context
