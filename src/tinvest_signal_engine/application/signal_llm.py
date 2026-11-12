"""Application use cases and ports for LLM-driven signal enrichment."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from tinvest_signal_engine.domain.signal_llm import (
    LlmAnalysisResult,
    LlmSignalContext,
    NewsArticle,
    format_llm_analysis_html,
    format_llm_analysis_plain,
)

logger = logging.getLogger(__name__)

DEFAULT_RELEVANT_SIGNAL_TYPES: frozenset[str] = frozenset(
    {
        "price_jump",
        "confirmed_impulse_long",
        "confirmed_impulse_short",
        "volume_spike",
        "trade_rate_spike",
        "open_interest_spike",
        "candle_range_spike",
        "morning_retracement_recommendation",
    }
)


class NewsProviderPort(Protocol):
    """Port for retrieving recent news articles for an instrument."""

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        ...


class LlmAnalyzerPort(Protocol):
    """Port for communicating with LLM models."""

    def is_news_relevant(
        self, ticker: str, signal_type: str, news_title: str
    ) -> bool:
        """Fast binary check (YES/NO) whether news is relevant to price move."""
        ...

    def analyze_signal(
        self, context: LlmSignalContext
    ) -> LlmAnalysisResult | None:
        """Deep structured analysis of signal and verified news."""
        ...


class SignalLlmEnrichmentUseCase:
    """Orchestrates smart gating, news retrieval, filtering, and LLM analysis."""

    def __init__(
        self,
        *,
        news_provider: NewsProviderPort,
        llm_analyzer: LlmAnalyzerPort,
        enabled: bool = True,
        min_quality_score: float = 0.65,
        cooldown_seconds: int = 1800,
        relevant_signal_types: frozenset[str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._news_provider = news_provider
        self._llm_analyzer = llm_analyzer
        self._enabled = enabled
        self._min_quality_score = min_quality_score
        self._cooldown_seconds = cooldown_seconds
        self._relevant_types = (
            relevant_signal_types
            if relevant_signal_types is not None
            else DEFAULT_RELEVANT_SIGNAL_TYPES
        )
        self._clock = clock or (lambda: datetime.now(tz=timezone.utc))
        self._last_analysis_at: dict[str, float] = {}

    @property
    def enabled(self) -> bool:
        return self._enabled

    def should_analyze(
        self,
        *,
        ticker: str,
        signal_type: str,
        quality_score: float | None = None,
    ) -> bool:
        """Determine whether the signal should trigger an LLM consultation."""
        if not self._enabled:
            return False

        if signal_type not in self._relevant_types:
            return False

        if quality_score is not None and quality_score < self._min_quality_score:
            return False

        now_ts = self._clock().timestamp()
        last_ts = self._last_analysis_at.get(ticker)
        if last_ts is not None and (now_ts - last_ts) < self._cooldown_seconds:
            logger.debug(
                "Skipping LLM analysis for %s due to cooldown (%.0fs left)",
                ticker,
                self._cooldown_seconds - (now_ts - last_ts),
            )
            return False

        return True

    def enrich_payload(
        self,
        *,
        ticker: str,
        signal_type: str,
        payload: dict[str, Any],
        quality_score: float | None = None,
        headline_ru: str = "",
    ) -> dict[str, Any]:
        """Perform gated LLM enrichment and return an updated payload dictionary."""
        if not self.should_analyze(
            ticker=ticker, signal_type=signal_type, quality_score=quality_score
        ):
            return payload

        try:
            # 1. Fetch raw candidate news articles
            raw_news = self._news_provider.get_recent_news(
                ticker, max_age_hours=4, limit=5, payload=payload
            )
            if not raw_news:
                # If there are no news items at all in the window, do not call deep LLM
                return payload

            # 2. Stage 1: Fast binary filter (Gatekeeper) per candidate news
            relevant_news: list[NewsArticle] = []
            for item in raw_news:
                try:
                    if self._llm_analyzer.is_news_relevant(
                        ticker, signal_type, item.title
                    ):
                        relevant_news.append(item)
                except Exception as ex:
                    logger.warning(
                        "Error checking news relevance for %s: %s", ticker, ex
                    )

            if not relevant_news:
                logger.debug(
                    "No relevant news passed binary gatekeeper for %s", ticker
                )
                return payload

            # 3. Stage 2: Deep structured analysis
            context = LlmSignalContext(
                ticker=ticker,
                signal_type=signal_type,
                price_change_pct=_to_float(payload.get("price_change_pct")),
                volume_ratio=_to_float(payload.get("volume_ratio")),
                orderbook_imbalance=_to_float(payload.get("obi") or payload.get("orderbook_imbalance")),
                headline_ru=headline_ru or str(payload.get("interpretation_ru") or ""),
                news_items=tuple(relevant_news[:3]),
                market_regime=str(payload.get("market_regime") or ""),
            )

            result = self._llm_analyzer.analyze_signal(context)
            if result is None:
                return payload

            # Update cooldown timestamp
            self._last_analysis_at[ticker] = self._clock().timestamp()

            html_block = format_llm_analysis_html(result)
            plain_block = format_llm_analysis_plain(result)

            return {
                **payload,
                "llm_analysis": result.to_dict(),
                "llm_analysis_html": html_block,
                "llm_analysis_plain": plain_block,
            }
        except Exception as ex:
            logger.warning("LLM enrichment failed for %s: %s", ticker, ex)
            return payload


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
