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
    """Format structured LLM analysis into Telegram-compatible HTML."""
    bias_emoji = {
        "LONG": "🟢 <b>LONG</b>",
        "SHORT": "🔴 <b>SHORT</b>",
        "NEUTRAL": "⚪ <b>NEUTRAL</b>",
    }.get(result.trade_bias.upper(), f"<b>{html.escape(result.trade_bias)}</b>")

    lines = [
        f"🧠 <b>LLM-анализ:</b> {bias_emoji} · <i>{html.escape(result.suggested_action)}</i>"
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
    """Format structured LLM analysis into plain text for fallback."""
    lines = [f"[LLM-анализ: {result.trade_bias} - {result.suggested_action}]"]
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
    """Format position analysis into a clean Telegram HTML card."""
    sentiment_badges = {
        "BULLISH": "🟢 <b>BULLISH</b>",
        "BEARISH": "🔴 <b>BEARISH</b>",
        "NEUTRAL": "⚪ <b>NEUTRAL</b>",
        "OVERHEATED_BULLISH": "🟡 <b>ПЕРЕГРЕТЫЙ ЛОНГ</b>",
        "OVERHEATED_BEARISH": "🟡 <b>ПЕРЕГРЕТЫЙ ШОРТ</b>",
        "SQUEEZE_RISK": "⚠️ <b>РИСК СКВИЗА</b>",
    }
    badge = sentiment_badges.get(
        result.sentiment.upper(), f"<b>{html.escape(result.sentiment)}</b>"
    )
    title = html.escape(commodity_title or result.commodity.upper())

    lines = [
        f"🧠 <b>Анализ позиционирования · {title}</b> ({badge})",
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
