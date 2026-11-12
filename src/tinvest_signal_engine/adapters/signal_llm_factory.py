"""Factory for instantiating LLM signal enrichment service."""

from __future__ import annotations

import logging
import os
from typing import Any

from tinvest_signal_engine.adapters.gemini_analyzer import GeminiLlmAnalyzer
from tinvest_signal_engine.adapters.news_provider import (
    CompositeNewsProvider,
    PayloadNewsProvider,
    RssNewsProvider,
)
from tinvest_signal_engine.application.signal_llm import (
    SignalLlmEnrichmentUseCase,
)

logger = logging.getLogger(__name__)

_GLOBAL_LLM_SERVICE: SignalLlmEnrichmentUseCase | None = None


def create_llm_enrichment_service(
    *,
    api_key: str | None = None,
    enabled: bool | None = None,
    gatekeeper_model: str = "gemini-3.5-flash-lite",
    analyzer_model: str = "gemini-3.5-flash",
    timeout_seconds: float = 4.0,
    cooldown_seconds: int = 1800,
    min_quality_score: float = 0.65,
    rss_urls: list[str] | None = None,
) -> SignalLlmEnrichmentUseCase | None:
    """Create a configured SignalLlmEnrichmentUseCase, or None if disabled."""
    key = (api_key or os.getenv("GEMINI_API_KEY") or "").strip()
    is_enabled = (
        enabled
        if enabled is not None
        else (os.getenv("GEMINI_ENABLED", "true").lower() in {"1", "true", "yes"} and bool(key))
    )

    if not is_enabled or not key:
        logger.debug("LLM enrichment is disabled or API key is not configured")
        return None

    analyzer = GeminiLlmAnalyzer(
        api_key=key,
        gatekeeper_model=gatekeeper_model or os.getenv("GEMINI_GATEKEEPER_MODEL", "gemini-3.5-flash-lite"),
        analyzer_model=analyzer_model or os.getenv("GEMINI_ANALYZER_MODEL", "gemini-3.5-flash"),
        timeout_seconds=timeout_seconds,
    )

    rss_provider = RssNewsProvider(rss_urls=rss_urls)
    payload_provider = PayloadNewsProvider()
    composite_news = CompositeNewsProvider([payload_provider, rss_provider])

    use_case = SignalLlmEnrichmentUseCase(
        news_provider=composite_news,
        llm_analyzer=analyzer,
        enabled=True,
        min_quality_score=min_quality_score,
        cooldown_seconds=cooldown_seconds,
    )

    logger.info("Initialized Gemini LLM signal enrichment service (%s / %s)", gatekeeper_model, analyzer_model)
    return use_case


def get_global_llm_service() -> SignalLlmEnrichmentUseCase | None:
    """Return the globally configured LLM service, if any."""
    global _GLOBAL_LLM_SERVICE
    if _GLOBAL_LLM_SERVICE is None:
        _GLOBAL_LLM_SERVICE = create_llm_enrichment_service()
    return _GLOBAL_LLM_SERVICE


def set_global_llm_service(service: SignalLlmEnrichmentUseCase | None) -> None:
    """Explicitly set or reset the global LLM enrichment service."""
    global _GLOBAL_LLM_SERVICE
    _GLOBAL_LLM_SERVICE = service
