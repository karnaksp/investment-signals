"""Application use cases and ports for LLM-driven signal enrichment."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from tinvest_signal_engine.domain.signal_llm import (
    AssetPositionAnalysisResult,
    AssetPositionLlmContext,
    DerivativesAnomalyContext,
    DerivativesAnomalyResult,
    DividendGapContext,
    DividendGapDecision,
    LlmAnalysisResult,
    LlmSignalContext,
    LossAttributionContext,
    LossAttributionResult,
    NewsArticle,
    PreMarketBriefingContext,
    PreMarketBriefingResult,
    RetracementGuardContext,
    RetracementGuardDecision,
    format_derivatives_anomaly_html,
    format_dividend_gap_html,
    format_llm_analysis_html,
    format_llm_analysis_plain,
    format_loss_attribution_html,
    format_position_analysis_html,
    format_position_analysis_plain,
    format_premarket_briefing_html,
    format_retracement_guard_html,
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

    def analyze_position_report(
        self, context: AssetPositionLlmContext
    ) -> AssetPositionAnalysisResult | None:
        """Deep analysis of long/short positioning, macro context, and asset dynamics."""
        ...

    def generate_premarket_briefing(
        self, context: PreMarketBriefingContext
    ) -> PreMarketBriefingResult | None:
        """Synthesize morning pre-market briefing and trading compass."""
        ...

    def evaluate_retracement_guard(
        self, context: RetracementGuardContext
    ) -> RetracementGuardDecision | None:
        """Evaluate whether opening gap is fundamental shock vs technical."""
        ...

    def analyze_loss_attribution(
        self, context: LossAttributionContext
    ) -> LossAttributionResult | None:
        """Post-mortem analysis for losing/contradicted signals."""
        ...

    def interpret_derivatives_anomaly(
        self, context: DerivativesAnomalyContext
    ) -> DerivativesAnomalyResult | None:
        """Explain term structure, calendar spread, or basis dislocation and propose setup."""
        ...

    def check_dividend_gap(
        self, context: DividendGapContext
    ) -> DividendGapDecision | None:
        """Determine whether sudden price drop is an expected ex-dividend gap."""
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

        if quality_score is not None:
            norm_quality = quality_score / 100.0 if quality_score > 1.0 else quality_score
            if norm_quality < self._min_quality_score:
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

        norm_quality = (
            quality_score / 100.0 if (quality_score is not None and quality_score > 1.0)
            else (quality_score or 0.0)
        )

        try:
            # 1. Fetch raw candidate news articles
            raw_news = self._news_provider.get_recent_news(
                ticker, max_age_hours=4, limit=5, payload=payload
            )

            # 2. Stage 1: Fast binary filter (Gatekeeper) per candidate news
            relevant_news: list[NewsArticle] = []
            if raw_news:
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

            # If no confirmed news, but quality is high (>= 0.80), proceed with technical analysis
            if not relevant_news:
                if norm_quality < 0.80:
                    logger.debug(
                        "No relevant news passed binary gatekeeper for %s and quality %.2f < 0.80",
                        ticker,
                        norm_quality,
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

    def analyze_asset_positions(
        self, payload: dict[str, Any]
    ) -> AssetPositionAnalysisResult | None:
        """Analyze a daily long/short position card payload."""
        if not self._enabled:
            return None

        commodity = str(payload.get("commodity") or "").strip().lower()
        if not commodity:
            return None

        commodity_name = _COMMODITY_RU_NAMES.get(commodity, commodity.upper())
        trading_day = str(payload.get("trading_day") or "")
        previous_day = str(payload.get("previous_day") or "")
        source_exchange = str(payload.get("source_exchange") or "")

        sections = payload.get("sections") or []
        sections_summary_lines: list[str] = []
        for sec in sections:
            src = sec.get("source", "Exchange")
            period = "день" if sec.get("period") == "daily" else "неделя"
            sections_summary_lines.append(f"Площадка {src} ({period}):")
            for grp in sec.get("groups", []):
                cat = grp.get("category", "")
                cat_ru = _CATEGORY_RU_NAMES.get(cat, cat)
                longs = int(grp.get("longs") or 0)
                shorts = int(grp.get("shorts") or 0)
                prev_longs = int(grp.get("previous_longs") or 0)
                prev_shorts = int(grp.get("previous_shorts") or 0)
                d_long = longs - prev_longs
                d_short = shorts - prev_shorts
                total = longs + shorts
                long_pct = round(longs / total * 100) if total > 0 else 0
                sections_summary_lines.append(
                    f"  - {cat_ru}: Лонги {long_pct}% ({longs:,}, Δ={d_long:+d}), "
                    f"Шорты {100 - long_pct}% ({shorts:,}, Δ={d_short:+d})"
                )

        # Fetch recent news for this commodity
        raw_news = self._news_provider.get_recent_news(
            commodity.upper(), max_age_hours=24, limit=4, payload=payload
        )

        context = AssetPositionLlmContext(
            commodity=commodity,
            commodity_name=commodity_name,
            source_exchange=source_exchange,
            trading_day=trading_day,
            previous_day=previous_day,
            sections_summary="\n".join(sections_summary_lines),
            recent_news=raw_news,
        )

        try:
            return self._llm_analyzer.analyze_position_report(context)
        except Exception as ex:
            logger.warning("LLM position analysis failed for %s: %s", commodity, ex)
            return None

    def enrich_retracement_signal(
        self, payload: dict[str, Any], *, ticker: str
    ) -> dict[str, Any]:
        """Evaluate Morning Retracement Guard (filter out gaps caused by fundamental shocks)."""
        if not self._enabled:
            return payload

        gap_dir = payload.get("expected_direction") or "down"
        entry_p = _to_float(payload.get("entry_reference_price")) or 0.0
        target_p = _to_float(payload.get("target_price")) or 0.0
        prob = _to_float(payload.get("target_probability", payload.get("model_probability"))) or 0.5
        gap_pct = abs((target_p - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0

        # Retrieve recent news for ticker
        news = self._news_provider.get_recent_news(ticker, max_age_hours=12, limit=4, payload=payload)

        context = RetracementGuardContext(
            ticker=ticker,
            gap_direction=gap_dir,
            gap_pct=round(gap_pct, 2),
            model_probability=prob,
            entry_reference_price=entry_p,
            target_price=target_p,
            news_items=news,
        )

        try:
            decision = self._llm_analyzer.evaluate_retracement_guard(context)
            if decision:
                return {
                    **payload,
                    "retracement_guard": decision.to_dict(),
                    "retracement_guard_html": format_retracement_guard_html(decision),
                }
        except Exception as ex:
            logger.warning("Retracement guard evaluation failed for %s: %s", ticker, ex)

        return payload

    def enrich_derivatives_anomaly(
        self,
        payload: dict[str, Any],
        *,
        chain_id: str,
        signal_type: str,
        z_score: float,
        metric_value: float,
        baseline_value: float,
        ticker: str = "",
    ) -> dict[str, Any]:
        """Explain term structure, calendar spread or basis anomaly and suggest trade setup."""
        if not self._enabled:
            return payload

        front = payload.get("front_contract") or payload.get("leg1_ticker") or ""
        back = payload.get("back_contract") or payload.get("leg2_ticker") or ""
        context = DerivativesAnomalyContext(
            chain_id=chain_id,
            signal_type=signal_type,
            metric_value=metric_value,
            baseline_value=baseline_value,
            z_score=z_score,
            ticker=ticker,
            front_contract=front,
            back_contract=back,
        )

        try:
            result = self._llm_analyzer.interpret_derivatives_anomaly(context)
            if result:
                return {
                    **payload,
                    "derivatives_interpretation": result.to_dict(),
                    "derivatives_interpretation_html": format_derivatives_anomaly_html(result),
                }
        except Exception as ex:
            logger.warning("Derivatives anomaly interpretation failed for %s (%s): %s", chain_id, signal_type, ex)

        return payload

    def check_dividend_gap(
        self,
        *,
        ticker: str,
        price_change_pct: float,
        payload: dict[str, Any] | None = None,
    ) -> DividendGapDecision | None:
        """Check whether a sharp downward price move is a planned dividend cutoff."""
        if not self._enabled or price_change_pct >= -1.0:
            return None

        p = payload or {}
        news = self._news_provider.get_recent_news(ticker, max_age_hours=24, limit=3, payload=p)
        context = DividendGapContext(
            ticker=ticker,
            price_change_pct=price_change_pct,
            event_date=self._clock().strftime("%Y-%m-%d"),
            news_items=news,
        )

        try:
            return self._llm_analyzer.check_dividend_gap(context)
        except Exception as ex:
            logger.warning("Dividend gap check failed for %s: %s", ticker, ex)
            return None

    def generate_premarket_briefing(
        self, context: PreMarketBriefingContext
    ) -> PreMarketBriefingResult | None:
        """Synthesize daily pre-market briefing."""
        if not self._enabled:
            return None
        try:
            return self._llm_analyzer.generate_premarket_briefing(context)
        except Exception as ex:
            logger.warning("Pre-market briefing generation failed: %s", ex)
            return None

    def analyze_loss_attribution(
        self, context: LossAttributionContext
    ) -> LossAttributionResult | None:
        """Run post-mortem analysis for a failed signal."""
        if not self._enabled:
            return None
        try:
            return self._llm_analyzer.analyze_loss_attribution(context)
        except Exception as ex:
            logger.warning("Loss attribution analysis failed for %s: %s", context.signal_id, ex)
            return None



_COMMODITY_RU_NAMES: dict[str, str] = {
    "gold": "Золото",
    "silver": "Серебро",
    "platinum": "Платина",
    "palladium": "Палладий",
    "copper": "Медь",
    "wti": "Нефть WTI",
    "brent": "Нефть Brent",
    "natural_gas": "Газ Henry Hub",
    "wheat_srw": "Пшеница SRW",
    "usd_rub": "Доллар / рубль",
    "cny_rub": "Юань / рубль",
    "eur_rub": "Евро / рубль",
}

_CATEGORY_RU_NAMES: dict[str, str] = {
    "broker_clients": "Через брокеров (клиенты)",
    "direct_members": "Напрямую на бирже (проф. участники)",
    "managed_money": "Фонды (Managed Money / спекулянты)",
    "producers_merchants": "Производители и торговцы (хеджеры)",
    "swap_dealers": "Своп-дилеры",
    "other_reportables": "Другие крупные участники",
    "nonreportable": "Мелкие участники (розница)",
    "individuals": "Физические лица",
    "legal_entities": "Юридические лица",
}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
