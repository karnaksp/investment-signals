"""News provider adapters implementing NewsProviderPort."""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Sequence

import httpx
from tinvest_signal_engine.adapters.llm_http import create_llm_http_client

from tinvest_signal_engine.application.signal_llm import NewsProviderPort
from tinvest_signal_engine.domain.signal_llm import NewsArticle

logger = logging.getLogger(__name__)

# Common ticker to company Russian alias keywords
TICKER_ALIASES: dict[str, tuple[str, ...]] = {
    "SBER": ("СБЕР", "СБЕРБАНК", "SBER"),
    "GAZP": ("ГАЗПРОМ", "GAZP"),
    "LKOH": ("ЛУКОЙЛ", "LKOH"),
    "GMKN": ("НОРНИКЕЛЬ", "НОРИЛЬСКИЙ НИКЕЛЬ", "GMKN"),
    "ROSN": ("РОСНЕФТЬ", "ROSN"),
    "NVTK": ("НОВАТЭК", "NVTK"),
    "YNDX": ("ЯНДЕКС", "YNDX"),
    "MGNT": ("МАГНИТ", "MGNT"),
    "VTBR": ("ВТБ", "VTBR"),
    "TATN": ("ТАТНЕФТЬ", "TATN"),
    "ALRS": ("АЛРОСА", "ALRS"),
    "CHMF": ("СЕВЕРСТАЛЬ", "CHMF"),
    "NLMK": ("НЛМК", "NLMK"),
    "PLZL": ("ПОЛЮС", "PLZL"),
    "MOEX": ("МОСБИРЖА", "МОСКОВСКАЯ БИРЖА", "MOEX"),
    "AFLT": ("АЭРОФЛОТ", "AFLT"),
    "SNGS": ("СУРГУТНЕФТЕГАЗ", "SNGS"),
    "MTSS": ("МТС", "MTSS"),
    # Commodities and Currencies for Position Reports
    "GOLD": ("ЗОЛОТО", "GOLD", "XAU"),
    "SILVER": ("СЕРЕБРО", "SILVER", "XAG"),
    "PLATINUM": ("ПЛАТИНА", "PLATINUM"),
    "PALLADIUM": ("ПАЛЛАДИЙ", "PALLADIUM"),
    "COPPER": ("МЕДЬ", "COPPER"),
    "WTI": ("WTI", "НЕФТЬ", "НЕФТИ"),
    "BRENT": ("BRENT", "НЕФТЬ", "НЕФТИ"),
    "NATURAL_GAS": ("ГАЗ", "ГАЗА", "HENRY HUB"),
    "WHEAT_SRW": ("ПШЕНИЦА", "ПШЕНИЦЫ", "ЗЕРНО"),
    "USD_RUB": ("ДОЛЛАР", "РУБЛЬ", "ВАЛЮТА", "ЦБ"),
    "CNY_RUB": ("ЮАНЬ", "РУБЛЬ", "ВАЛЮТА"),
    "EUR_RUB": ("ЕВРО", "РУБЛЬ", "ВАЛЮТА"),
}

FUTURES_TICKER_ALIASES: dict[str, tuple[str, ...]] = {
    "EU": ("ЕВРО", "EUR", "РУБЛЬ", "ВАЛЮТА", "ЦБ"),
    "SI": ("ДОЛЛАР", "USD", "РУБЛЬ", "ВАЛЮТА", "ЦБ"),
    "CR": ("ЮАНЬ", "CNY", "РУБЛЬ", "ВАЛЮТА", "ЦБ"),
    "ED": ("EUR/USD", "ЕВРО", "ДОЛЛАР"),
    "RI": ("РТС", "ИНДЕКС РТС", "RTS"),
    "MX": ("МОСБИРЖ", "ИНДЕКС МОСБИРЖИ", "IMOEX"),
    "MM": ("ИНДЕКС МОСБИРЖИ", "IMOEX"),
    "GD": ("ЗОЛОТО", "GOLD", "XAU"),
    "SV": ("СЕРЕБРО", "SILVER", "XAG"),
    "BR": ("НЕФТЬ", "BRENT", "WTI", "ОПЕК"),
    "NG": ("ГАЗ", "ГАЗА", "HENRY HUB"),
    "PD": ("ПАЛЛАДИЙ", "PALLADIUM"),
    "PT": ("ПЛАТИНА", "PLATINUM"),
    "SR": ("СБЕР", "СБЕРБАНК", "SBER"),
    "GZ": ("ГАЗПРОМ", "GAZP"),
    "LK": ("ЛУКОЙЛ", "LKOH"),
    "RN": ("РОСНЕФТЬ", "ROSN"),
    "VB": ("ВТБ", "VTBR"),
    "MN": ("МАГНИТ", "MGNT"),
    "AL": ("АЛРОСА", "ALRS"),
    "CH": ("СЕВЕРСТАЛЬ", "CHMF"),
    "NL": ("НЛМК", "NLMK"),
    "PZ": ("ПОЛЮС", "PLZL"),
    "AF": ("АЭРОФЛОТ", "AFLT"),
    "SN": ("СУРГУТНЕФТЕГАЗ", "SNGS"),
    "TT": ("Т-ТЕХНОЛОГИИ", "ТКС", "ТИНЬКОФФ"),
    "YD": ("ЯНДЕКС", "YNDX"),
}


def resolve_ticker_keywords(ticker: str) -> tuple[str, ...]:
    clean = (ticker or "").upper().strip()
    if not clean:
        return ()
    if clean in TICKER_ALIASES:
        return TICKER_ALIASES[clean]
    if len(clean) >= 4 and clean[:2] in FUTURES_TICKER_ALIASES:
        return FUTURES_TICKER_ALIASES[clean[:2]]
    if clean.endswith("P") and clean[:-1] in TICKER_ALIASES:
        return TICKER_ALIASES[clean[:-1]]
    return (clean,)


def get_search_query_for_ticker(ticker: str) -> str:
    clean = (ticker or "").upper().strip()
    if clean.startswith("EU"):
        return "курс евро рубль"
    if clean.startswith("SI"):
        return "курс доллар рубль"
    if clean.startswith("CR"):
        return "курс юань рубль"
    if clean.startswith("BR"):
        return "нефть Brent цена"
    if clean.startswith("GD"):
        return "цена золота котировки"
    if clean.startswith("SV"):
        return "цена серебра котировки"
    if clean.startswith("NG"):
        return "цена газа котировки"
    if clean.startswith("RI"):
        return "индекс РТС Мосбиржа"
    if clean.startswith("MX") or clean == "IMOEX":
        return "индекс Мосбиржи акции"
    if clean in ("SBER", "SBERP") or clean.startswith("SR"):
        return "Сбербанк акции"
    if clean == "GAZP" or clean.startswith("GZ"):
        return "Газпром акции"
    if clean == "LKOH" or clean.startswith("LK"):
        return "Лукойл акции"
    if clean == "ROSN" or clean.startswith("RN"):
        return "Роснефть акции"
    if clean == "NVTK":
        return "Новатэк акции"
    if clean == "VTBR" or clean.startswith("VB"):
        return "ВТБ банк акции"
    if clean in ("YNDX", "YDEX") or clean.startswith("YD"):
        return "Яндекс акции"
    if clean == "AFLT" or clean.startswith("AF"):
        return "Аэрофлот акции"
    if clean == "MGNT" or clean.startswith("MN"):
        return "Магнит акции"
    if clean == "PLZL" or clean.startswith("PZ"):
        return "Полюс золото акции"
    if clean == "ALRS" or clean.startswith("AL"):
        return "Алроса акции"
    if clean == "CHMF" or clean.startswith("CH"):
        return "Северсталь акции"
    if clean == "NLMK" or clean.startswith("NL"):
        return "НЛМК акции"
    if clean in ("T", "TCSG") or clean.startswith("TT"):
        return "Т-Технологии ТКС акции"
    if clean == "MOEX":
        return "Московская Биржа акции"
    return f"{clean} акции новости"


class PayloadNewsProvider(NewsProviderPort):
    """Extracts news items directly from signal payload if present."""

    def __init__(self, payload_key: str = "news") -> None:
        self._payload_key = payload_key

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        if payload is not None:
            return self.extract_from_payload(ticker, payload)[:limit]
        return ()

    def extract_from_payload(
        self, ticker: str, payload: dict[str, Any]
    ) -> tuple[NewsArticle, ...]:
        raw_list = payload.get(self._payload_key) or payload.get("news_items")
        if not isinstance(raw_list, list):
            return ()

        results: list[NewsArticle] = []
        for item in raw_list:
            if isinstance(item, str):
                results.append(NewsArticle(title=item, ticker=ticker))
            elif isinstance(item, dict):
                results.append(
                    NewsArticle(
                        title=str(item.get("title") or "").strip(),
                        summary=str(item.get("summary") or item.get("text") or "").strip(),
                        source=str(item.get("source") or "").strip(),
                        url=str(item.get("url") or "").strip(),
                        ticker=ticker,
                    )
                )
        return tuple(results)


class RssNewsProvider(NewsProviderPort):
    """Fetches financial RSS feeds with in-memory caching and ticker keyword matching."""

    def __init__(
        self,
        rss_urls: Sequence[str] | None = None,
        cache_ttl_seconds: float = 300.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._urls = tuple(
            rss_urls
            or (
                "https://rssexport.rbc.ru/rbcnews/news/30/full.rss",
            )
        )
        self._ttl = cache_ttl_seconds
        self._client = client or create_llm_http_client(timeout_seconds=4.0, follow_redirects=True)
        self._cached_articles: list[NewsArticle] = []
        self._last_fetch_ts: float = 0.0

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        self._ensure_cache()
        keywords = resolve_ticker_keywords(ticker)

        matched: list[NewsArticle] = []
        for article in self._cached_articles:
            text_to_search = f"{article.title} {article.summary}".upper()
            if any(re.search(rf"(?:\b|_){re.escape(kw)}", text_to_search) for kw in keywords):
                matched.append(article)
                if len(matched) >= limit:
                    break

        return tuple(matched)

    def _ensure_cache(self) -> None:
        now = time.time()
        if (now - self._last_fetch_ts) < self._ttl and self._cached_articles:
            return

        articles: list[NewsArticle] = []
        for url in self._urls:
            try:
                resp = self._client.get(url)
                if resp.status_code != 200:
                    continue
                root = ET.fromstring(resp.content)
                for item in root.findall(".//item"):
                    title = (item.findtext("title") or "").strip()
                    desc = (item.findtext("description") or "").strip()
                    link = (item.findtext("link") or "").strip()
                    pub_date = (item.findtext("pubDate") or "").strip()
                    if title:
                        articles.append(
                            NewsArticle(
                                title=title,
                                summary=desc[:300],
                                source="RSS",
                                url=link,
                                published_at=pub_date,
                            )
                        )
            except Exception as ex:
                logger.debug("Failed to fetch RSS from %s: %s", url, ex)

        if articles:
            self._cached_articles = articles
            self._last_fetch_ts = now


class GoogleNewsProvider(NewsProviderPort):
    """Fetches real-time financial news for a ticker via Google News RSS search."""

    def __init__(
        self,
        search_adapter: Any | None = None,
        cache_ttl_seconds: float = 120.0,
    ) -> None:
        if search_adapter is None:
            from tinvest_signal_engine.adapters.internet_news_search import (
                GoogleNewsRssSearchAdapter,
            )

            self._search_adapter = GoogleNewsRssSearchAdapter()
        else:
            self._search_adapter = search_adapter
        self._ttl = cache_ttl_seconds
        self._cache: dict[str, tuple[float, tuple[NewsArticle, ...]]] = {}

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        clean_t = (ticker or "").upper().strip()
        if not clean_t:
            return ()

        now = time.time()
        cached = self._cache.get(clean_t)
        if cached and (now - cached[0]) < self._ttl:
            return cached[1][:limit]

        query = get_search_query_for_ticker(clean_t)
        try:
            raw_items = self._search_adapter.search_news(query, limit=limit)
            articles: list[NewsArticle] = []
            for it in raw_items:
                summary_val = getattr(it, "summary", "") or getattr(it, "snippet", "")
                articles.append(
                    NewsArticle(
                        title=it.title,
                        summary=summary_val,
                        source=it.source or "Google News",
                        url=it.url,
                        published_at=it.published_at,
                        ticker=clean_t,
                    )
                )
            result = tuple(articles)
        except Exception as ex:
            logger.warning("GoogleNewsProvider failed for %s: %s", clean_t, ex)
            result = ()

        self._cache[clean_t] = (now, result)
        return result[:limit]


class CompositeNewsProvider(NewsProviderPort):
    """Queries multiple providers in priority order."""

    def __init__(self, providers: Sequence[NewsProviderPort]) -> None:
        self._providers = tuple(providers)

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        collected: list[NewsArticle] = []
        for p in self._providers:
            try:
                items = p.get_recent_news(
                    ticker,
                    max_age_hours=max_age_hours,
                    limit=limit - len(collected),
                    payload=payload,
                )
                collected.extend(items)
                if len(collected) >= limit:
                    break
            except Exception as ex:
                logger.warning("News provider %s failed: %s", type(p).__name__, ex)
        return tuple(collected)


class NullNewsProvider(NewsProviderPort):
    """No-op provider returning nothing."""

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        return ()
