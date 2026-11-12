"""News provider adapters implementing NewsProviderPort."""

from __future__ import annotations

import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Sequence

import httpx

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
}


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
        self._client = client or httpx.Client(timeout=4.0, follow_redirects=True)
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
        keywords = TICKER_ALIASES.get(ticker.upper(), (ticker.upper(),))

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
