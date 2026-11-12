"""Internet news search adapter implementing InternetNewsSearchPort."""

from __future__ import annotations

import logging
import re
from typing import Sequence
import urllib.parse
import xml.etree.ElementTree as ET

import httpx

from tinvest_signal_engine.application.trader_copilot import InternetNewsSearchPort
from tinvest_signal_engine.domain.trader_copilot import InternetNewsItem

logger = logging.getLogger(__name__)


class GoogleNewsRssSearchAdapter(InternetNewsSearchPort):
    """Searches real-time internet financial news using Google News RSS and fallback feeds."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 6.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._timeout = timeout_seconds
        self._client = client or httpx.Client(
            timeout=timeout_seconds, follow_redirects=True, trust_env=False
        )

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def search_news(
        self, query: str, *, limit: int = 6
    ) -> tuple[InternetNewsItem, ...]:
        clean_q = query.strip()
        if not clean_q:
            return ()

        encoded_q = urllib.parse.quote_plus(clean_q)
        url = f"https://news.google.com/rss/search?q={encoded_q}&hl=ru&gl=RU&ceid=RU:ru"

        try:
            resp = self._client.get(url)
            if resp.status_code == 200:
                items = self._parse_rss_items(resp.content, limit=limit)
                if items:
                    return items
        except Exception as ex:
            logger.warning("Google News RSS search failed for query '%s': %s", clean_q, ex)

        # Fallback to general RBC RSS if query search failed
        return self._fetch_rbc_fallback(clean_q, limit=limit)

    def _parse_rss_items(
        self, content: bytes, *, limit: int = 6
    ) -> tuple[InternetNewsItem, ...]:
        try:
            root = ET.fromstring(content)
        except Exception:
            return ()

        results: list[InternetNewsItem] = []
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            desc = (item.findtext("description") or "").strip()

            if not title:
                continue

            # Strip trailing source name from Google News title (e.g. "Заголовок - РБК")
            source = ""
            if " - " in title:
                parts = title.rsplit(" - ", 1)
                title = parts[0].strip()
                source = parts[1].strip()

            # Clean HTML tags from description if any
            clean_desc = re.sub(r"<[^>]+>", " ", desc).strip()
            clean_desc = " ".join(clean_desc.split())[:200]

            results.append(
                InternetNewsItem(
                    title=title,
                    source=source,
                    summary=clean_desc,
                    url=link,
                    published_at=pub_date,
                )
            )
            if len(results) >= limit:
                break

        return tuple(results)

    def _fetch_rbc_fallback(
        self, query: str, *, limit: int = 6
    ) -> tuple[InternetNewsItem, ...]:
        try:
            resp = self._client.get("https://rssexport.rbc.ru/rbcnews/news/30/full.rss")
            if resp.status_code != 200:
                return ()
            root = ET.fromstring(resp.content)
            results: list[InternetNewsItem] = []
            keywords = [w.lower() for w in query.split() if len(w) > 2]
            for item in root.findall(".//item"):
                title = (item.findtext("title") or "").strip()
                desc = (item.findtext("description") or "").strip()
                link = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                text = f"{title} {desc}".lower()
                if any(kw in text for kw in keywords) or not keywords:
                    results.append(
                        InternetNewsItem(
                            title=title,
                            source="РБК",
                            summary=desc[:200],
                            url=link,
                            published_at=pub_date,
                        )
                    )
                    if len(results) >= limit:
                        break
            return tuple(results)
        except Exception:
            return ()
