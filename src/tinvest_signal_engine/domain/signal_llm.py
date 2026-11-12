"""Domain entities and value objects for LLM-driven signal analysis."""

from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class NewsArticle:
    """A news item related to an instrument or sector."""

    title: str
    summary: str = ""
    published_at: str = ""
    source: str = ""
    url: str = ""
    ticker: str = ""
    sector: str = ""


@dataclass(frozen=True)
class LlmSignalContext:
    """Normalized context supplied to the LLM for signal analysis."""

    ticker: str
    signal_type: str
    price_change_pct: float | None = None
    volume_ratio: float | None = None
    orderbook_imbalance: float | None = None
    headline_ru: str = ""
    news_items: tuple[NewsArticle, ...] = ()
    market_regime: str = ""


@dataclass(frozen=True)
class LlmAnalysisResult:
    """Structured output returned from the LLM signal analysis."""

    has_catalyst: bool
    catalyst_summary: str
    reaction_type: str
    trade_bias: str
    suggested_action: str
    risk_factor: str
    invalidation_level: str = ""
    confidence_score: float = 0.0
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "has_catalyst": self.has_catalyst,
            "catalyst_summary": self.catalyst_summary,
            "reaction_type": self.reaction_type,
            "trade_bias": self.trade_bias,
            "suggested_action": self.suggested_action,
            "risk_factor": self.risk_factor,
            "invalidation_level": self.invalidation_level,
            "confidence_score": self.confidence_score,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_llm_analysis_html(result: LlmAnalysisResult) -> str:
    """Format structured analysis into clean Telegram-compatible HTML without emojis or LLM tags."""
    bias_text = {
        "LONG": "<b>LONG</b>",
        "SHORT": "<b>SHORT</b>",
        "NEUTRAL": "<b>NEUTRAL</b>",
    }.get(result.trade_bias.upper(), f"<b>{html.escape(result.trade_bias)}</b>")

    lines = [
        f"<b>Анализ катализатора:</b> {bias_text} · <i>{html.escape(result.suggested_action)}</i>"
    ]

    if result.has_catalyst and result.catalyst_summary:
        lines.append(f"• <b>Катализатор:</b> {html.escape(result.catalyst_summary)}")
    elif not result.has_catalyst:
        lines.append("• <b>Катализатор:</b> Прямой инфоповод не подтвержден (возможно спекулятивный импульс)")

    if result.risk_factor:
        lines.append(f"• <b>Риск:</b> {html.escape(result.risk_factor)}")

    if result.invalidation_level:
        lines.append(f"• <b>Отмена:</b> {html.escape(result.invalidation_level)}")

    return "\n".join(lines)


def format_llm_analysis_plain(result: LlmAnalysisResult) -> str:
    """Format structured analysis into plain text for fallback."""
    lines = [f"[Анализ катализатора: {result.trade_bias} - {result.suggested_action}]"]
    if result.has_catalyst and result.catalyst_summary:
        lines.append(f"Катализатор: {result.catalyst_summary}")
    if result.risk_factor:
        lines.append(f"Риск: {result.risk_factor}")
    if result.invalidation_level:
        lines.append(f"Отмена: {result.invalidation_level}")
    return "\n".join(lines)


@dataclass(frozen=True)
class AssetPositionLlmContext:
    """Context extracted from daily long/short position reports for LLM interpretation."""

    commodity: str
    commodity_name: str
    source_exchange: str
    trading_day: str
    previous_day: str
    sections_summary: str
    recent_news: tuple[NewsArticle, ...] = ()


@dataclass(frozen=True)
class AssetPositionAnalysisResult:
    """LLM analysis and macro interpretation of long/short positioning."""

    commodity: str
    sentiment: str  # BULLISH, BEARISH, NEUTRAL, OVERHEATED_BULLISH, OVERHEATED_BEARISH, SQUEEZE_RISK
    bias_title: str
    smart_money_summary: str
    crowd_bias: str
    interpretation: str
    macro_drivers: str
    actionable_advice: str
    risk_factors: str = ""
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity": self.commodity,
            "sentiment": self.sentiment,
            "bias_title": self.bias_title,
            "smart_money_summary": self.smart_money_summary,
            "crowd_bias": self.crowd_bias,
            "interpretation": self.interpretation,
            "macro_drivers": self.macro_drivers,
            "actionable_advice": self.actionable_advice,
            "risk_factors": self.risk_factors,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_position_analysis_html(
    result: AssetPositionAnalysisResult, commodity_title: str = ""
) -> str:
    """Format position analysis into a clean Telegram HTML card without emojis or LLM tags."""
    sentiment_badges = {
        "BULLISH": "<b>BULLISH</b>",
        "BEARISH": "<b>BEARISH</b>",
        "NEUTRAL": "<b>NEUTRAL</b>",
        "OVERHEATED_BULLISH": "<b>ПЕРЕГРЕТЫЙ ЛОНГ</b>",
        "OVERHEATED_BEARISH": "<b>ПЕРЕГРЕТЫЙ ШОРТ</b>",
        "SQUEEZE_RISK": "<b>РИСК СКВИЗА</b>",
    }
    badge = sentiment_badges.get(
        result.sentiment.upper(), f"<b>{html.escape(result.sentiment)}</b>"
    )
    title = html.escape(commodity_title or result.commodity.upper())

    lines = [
        f"<b>Анализ позиционирования · {title}</b> ({badge})",
    ]
    if result.bias_title:
        lines.append(f"<b>{html.escape(result.bias_title)}</b>\n")

    if result.smart_money_summary:
        lines.append(f"• <b>Умный капитал:</b> {html.escape(result.smart_money_summary)}")

    if result.crowd_bias:
        lines.append(f"• <b>Толпа / розница:</b> {html.escape(result.crowd_bias)}")

    if result.interpretation:
        lines.append(f"• <b>Что значит перевес:</b> {html.escape(result.interpretation)}")

    if result.macro_drivers:
        lines.append(f"• <b>Макро / динамика:</b> {html.escape(result.macro_drivers)}")

    if result.actionable_advice:
        lines.append(f"• <b>Рекомендация:</b> {html.escape(result.actionable_advice)}")

    if result.risk_factors:
        lines.append(f"• <b>Главный риск:</b> {html.escape(result.risk_factors)}")

    return "\n".join(lines)


def format_position_analysis_plain(
    result: AssetPositionAnalysisResult, commodity_title: str = ""
) -> str:
    """Plain-text fallback for position report analysis."""
    title = commodity_title or result.commodity.upper()
    lines = [
        f"[Анализ позиций: {title} - {result.sentiment}]",
        result.bias_title,
        f"Умный капитал: {result.smart_money_summary}",
        f"Интерпретация: {result.interpretation}",
        f"Рекомендация: {result.actionable_advice}",
    ]
    return "\n".join(lines)


# ==============================================================================
# 1. Pre-Market Briefing Models (Премаркет-брифинг и утренний контекст сессии)
# ==============================================================================

@dataclass(frozen=True)
class PreMarketBriefingContext:
    """Market context collected prior to market opening."""

    trading_date: str
    brent_price: float | None = None
    brent_change_pct: float | None = None
    gold_price: float | None = None
    gold_change_pct: float | None = None
    asia_sentiment: str = ""
    us_futures_sentiment: str = ""
    overnight_news: tuple[NewsArticle, ...] = ()
    corporate_events: tuple[str, ...] = ()
    macro_events: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreMarketBriefingResult:
    """Synthesized pre-market intelligence."""

    trading_date: str
    overall_sentiment: str  # ПОЗИТИВНЫЙ, НЕГАТИВНЫЙ, НЕЙТРАЛЬНЫЙ, СМЕШАННЫЙ
    sentiment_headline: str
    key_drivers: tuple[str, ...]
    focus_sectors: tuple[str, ...]
    risk_factors: tuple[str, ...]
    actionable_summary: str
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "trading_date": self.trading_date,
            "overall_sentiment": self.overall_sentiment,
            "sentiment_headline": self.sentiment_headline,
            "key_drivers": list(self.key_drivers),
            "focus_sectors": list(self.focus_sectors),
            "risk_factors": list(self.risk_factors),
            "actionable_summary": self.actionable_summary,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_premarket_briefing_html(result: PreMarketBriefingResult) -> str:
    """Format morning pre-market briefing into clean Telegram HTML without emojis."""
    lines = [
        f"<b>Утренний обзор сессии · {html.escape(result.trading_date)}</b>",
        f"<b>Фон:</b> <b>{html.escape(result.overall_sentiment)}</b> — {html.escape(result.sentiment_headline)}\n",
    ]

    if result.key_drivers:
        lines.append("<b>Ключевые драйверы:</b>")
        for d in result.key_drivers:
            lines.append(f"• {html.escape(d)}")
        lines.append("")

    if result.focus_sectors:
        lines.append("<b>Сектора в фокусе:</b>")
        for s in result.focus_sectors:
            lines.append(f"• {html.escape(s)}")
        lines.append("")

    if result.risk_factors:
        lines.append("<b>Факторы риска:</b>")
        for r in result.risk_factors:
            lines.append(f"• {html.escape(r)}")
        lines.append("")

    if result.actionable_summary:
        lines.append(f"<b>Резюме для открытия:</b> <i>{html.escape(result.actionable_summary)}</i>")

    return "\n".join(lines).strip()


# ==============================================================================
# 2. Morning Retracement Guard Models (Фильтр утренних гэпов)
# ==============================================================================

@dataclass(frozen=True)
class RetracementGuardContext:
    """Context for checking whether a morning opening gap is fundamental shock vs technical."""

    ticker: str
    gap_direction: str  # up, down
    gap_pct: float
    model_probability: float
    entry_reference_price: float = 0.0
    target_price: float = 0.0
    news_items: tuple[NewsArticle, ...] = ()
    corporate_events: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetracementGuardDecision:
    """LLM decision whether to confirm, warn, or block an opening retracement trade."""

    ticker: str
    verdict: str  # CONFIRMED, WARNING, BLOCKED
    is_fundamental_shock: bool
    shock_reason: str
    recommendation: str
    confidence_score: float = 0.0
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "verdict": self.verdict,
            "is_fundamental_shock": self.is_fundamental_shock,
            "shock_reason": self.shock_reason,
            "recommendation": self.recommendation,
            "confidence_score": self.confidence_score,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_retracement_guard_html(decision: RetracementGuardDecision) -> str:
    """Format Retracement Guard advisory note for Telegram without emojis or LLM tags."""
    if decision.verdict == "BLOCKED" or decision.is_fundamental_shock:
        return (
            f"<b>Контроль утреннего отката: БЛОКИРОВАНО</b> — Гэп вызван фундаментальным шоком: "
            f"{html.escape(decision.shock_reason)}. "
            f"<i>{html.escape(decision.recommendation)}</i>"
        )
    if decision.verdict == "WARNING":
        return (
            f"<b>Контроль утреннего отката: ВНИМАНИЕ</b> — Гэп имеет фундаментальный фон ({html.escape(decision.shock_reason)}). "
            f"Повышенный риск безоткатного движения! <i>{html.escape(decision.recommendation)}</i>"
        )
    return (
        f"<b>Контроль утреннего отката: ПОДТВЕРЖДЕНО</b> — Фундаментальных шоков не обнаружено, гэп технический. "
        f"<i>{html.escape(decision.recommendation or 'Статистическая вероятность возврата валидна.')}</i>"
    )


# ==============================================================================
# 3. Automated Loss Attribution Models (Авто-постмортем убыточных сигналов)
# ==============================================================================

@dataclass(frozen=True)
class LossAttributionContext:
    """Context of a failed/contradicted signal for post-mortem analysis."""

    signal_id: str
    ticker: str
    signal_type: str
    direction: str  # LONG, SHORT, UP, DOWN
    entry_price: float
    exit_price: float
    loss_pct: float
    horizon_seconds: int
    subsequent_news: tuple[NewsArticle, ...] = ()
    market_context: str = ""


@dataclass(frozen=True)
class LossAttributionResult:
    """Root-cause analysis and calibration recommendation for a losing signal."""

    signal_id: str
    category: str  # NEWS_SHOCK, MARKET_PULLBACK, LOW_LIQUIDITY_SLIPPAGE, TREND_EXHAUSTION, FALSE_BREAKOUT, OTHER
    category_ru: str
    root_cause: str
    is_preventable: bool
    calibration_suggestion: str
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "category": self.category,
            "category_ru": self.category_ru,
            "root_cause": self.root_cause,
            "is_preventable": self.is_preventable,
            "calibration_suggestion": self.calibration_suggestion,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_loss_attribution_html(result: LossAttributionResult, ticker: str = "") -> str:
    """Format signal post-mortem into Telegram HTML without emojis."""
    preventable_str = "Да (фильтруемо новостным гейтом)" if result.is_preventable else "Нет (рыночный шум / стохастика)"
    lines = [
        f"<b>Постмортем сигнала · {html.escape(ticker or result.signal_id[:8])}</b>",
        f"• <b>Причина:</b> <b>{html.escape(result.category_ru)}</b> ({html.escape(result.category)})",
        f"• <b>Анализ:</b> {html.escape(result.root_cause)}",
        f"• <b>Предотвратимость:</b> {preventable_str}",
    ]
    if result.calibration_suggestion:
        lines.append(f"• <b>Корректировка:</b> <i>{html.escape(result.calibration_suggestion)}</i>")
    return "\n".join(lines)


# ==============================================================================
# 4. Derivatives & Spread Anomaly Interpretation Models
# ==============================================================================

@dataclass(frozen=True)
class DerivativesAnomalyContext:
    """Context for analyzing term structure, calendar spread, and basis dislocations."""

    chain_id: str
    signal_type: str
    metric_value: float
    baseline_value: float
    z_score: float
    ticker: str = ""
    front_contract: str = ""
    back_contract: str = ""
    market_context: str = ""


@dataclass(frozen=True)
class DerivativesAnomalyResult:
    """Economic explanation and trading setup for a futures/basis dislocation."""

    chain_id: str
    anomaly_title: str
    market_driver: str
    trade_setup: str
    risk_assessment: str
    confidence_score: float = 0.0
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain_id": self.chain_id,
            "anomaly_title": self.anomaly_title,
            "market_driver": self.market_driver,
            "trade_setup": self.trade_setup,
            "risk_assessment": self.risk_assessment,
            "confidence_score": self.confidence_score,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_derivatives_anomaly_html(result: DerivativesAnomalyResult) -> str:
    """Format derivatives anomaly interpretation for Telegram without emojis."""
    lines = [
        f"<b>Структура кривой ({html.escape(result.anomaly_title)})</b>",
        f"• <b>Причина дислокации:</b> {html.escape(result.market_driver)}",
        f"• <b>Торговый сетап:</b> <i>{html.escape(result.trade_setup)}</i>",
    ]
    if result.risk_assessment:
        lines.append(f"• <b>Риск:</b> {html.escape(result.risk_assessment)}")
    return "\n".join(lines)


# ==============================================================================
# 6. Dividend Gap Guard Models (Защита от ложных тревог при дивидендных гэпах)
# ==============================================================================

@dataclass(frozen=True)
class DividendGapContext:
    """Context for checking whether a sudden drop is an ex-dividend gap."""

    ticker: str
    price_change_pct: float
    event_date: str = ""
    ex_dividend_date: str | None = None
    expected_dividend_rub: float | None = None
    dividend_yield_pct: float | None = None
    news_items: tuple[NewsArticle, ...] = ()


@dataclass(frozen=True)
class DividendGapDecision:
    """Decision on whether a price drop is an expected ex-dividend gap."""

    ticker: str
    is_dividend_gap: bool
    dividend_rub: float | None = None
    yield_pct: float | None = None
    should_suppress_short: bool = False
    explanation: str = ""
    confidence_score: float = 0.0
    model_used: str = ""
    tokens_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "is_dividend_gap": self.is_dividend_gap,
            "dividend_rub": self.dividend_rub,
            "yield_pct": self.yield_pct,
            "should_suppress_short": self.should_suppress_short,
            "explanation": self.explanation,
            "confidence_score": self.confidence_score,
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
        }


def format_dividend_gap_html(decision: DividendGapDecision) -> str:
    """Format dividend gap explanation for Telegram without emojis."""
    lines = [
        f"<b>Дивидендная отсечка ({html.escape(decision.ticker)})</b>"
    ]
    details = []
    if decision.dividend_rub:
        details.append(f"дивиденд: {decision.dividend_rub:g} ₽")
    if decision.yield_pct:
        details.append(f"доходность: {decision.yield_pct:.1f}%")
    if details:
        lines.append(f"• <b>Параметры:</b> {', '.join(details)}")
    if decision.explanation:
        lines.append(f"• <b>Комментарий:</b> {html.escape(decision.explanation)}")
    if decision.should_suppress_short:
        lines.append("• <b>Статус:</b> <i>Плановый технический гэп. Алерты на шорт подавлены.</i>")
    return "\n".join(lines)

