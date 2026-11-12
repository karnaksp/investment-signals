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
