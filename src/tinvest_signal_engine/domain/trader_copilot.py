"""Domain models and formatters for Professional Trade Desk Copilot."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import html
from typing import Any


@dataclass(frozen=True)
class TraderDeskQuery:
    """User query or reply to a signal directed at the trading desk copilot."""

    query_text: str
    ticker: str | None = None
    user_id: int | None = None
    user_name: str | None = None
    chat_id: int | str | None = None
    message_id: int | None = None
    reply_to_text: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_text": self.query_text,
            "ticker": self.ticker,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "chat_id": self.chat_id,
            "message_id": self.message_id,
            "reply_to_text": self.reply_to_text,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True)
class MarketContextSnapshot:
    """Live market microstructure data for the queried instrument."""

    ticker: str
    instrument_id: str | None = None
    last_price: float | None = None
    bid_price: float | None = None
    ask_price: float | None = None
    spread_bps: float | None = None
    vwap: float | None = None
    volume_24h: float | None = None
    positions_ratio_summary: str | None = None
    recent_signals_summary: tuple[str, ...] = ()
    captured_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "instrument_id": self.instrument_id,
            "last_price": self.last_price,
            "bid_price": self.bid_price,
            "ask_price": self.ask_price,
            "spread_bps": self.spread_bps,
            "vwap": self.vwap,
            "volume_24h": self.volume_24h,
            "positions_ratio_summary": self.positions_ratio_summary,
            "recent_signals_summary": list(self.recent_signals_summary),
            "captured_at": self.captured_at.isoformat(),
        }


@dataclass(frozen=True)
class InternetNewsItem:
    """Internet news item retrieved for real-time market background."""

    title: str
    source: str = ""
    summary: str = ""
    url: str = ""
    published_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "source": self.source,
            "summary": self.summary,
            "url": self.url,
            "published_at": self.published_at,
        }


@dataclass(frozen=True)
class TraderCopilotResponse:
    """Institutional professional trader's consultation and trade thesis."""

    ticker: str | None
    market_stance: str  # BULLISH, BEARISH, NEUTRAL, CAUTIOUS, ARBITRAGE
    thesis_title: str
    microstructure_analysis: str
    news_drivers_analysis: str
    key_levels: tuple[str, ...] = ()
    actionable_setup: str = ""
    risk_factors: tuple[str, ...] = ()
    news_catalysts: tuple[str, ...] = ()
    model_used: str = ""
    tokens_used: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "market_stance": self.market_stance,
            "thesis_title": self.thesis_title,
            "microstructure_analysis": self.microstructure_analysis,
            "news_drivers_analysis": self.news_drivers_analysis,
            "key_levels": list(self.key_levels),
            "actionable_setup": self.actionable_setup,
            "risk_factors": list(self.risk_factors),
            "news_catalysts": list(self.news_catalysts),
            "model_used": self.model_used,
            "tokens_used": self.tokens_used,
            "created_at": self.created_at.isoformat(),
        }


def format_trader_response_html(
    response: TraderCopilotResponse,
    *,
    market_context: MarketContextSnapshot | None = None,
    query_text: str | None = None,
) -> str:
    """Formats professional trader consultation into clean classical HTML without emojis or LLM tags."""
    stance_badges = {
        "BULLISH": "<b>[ЛОНГ]</b>",
        "BEARISH": "<b>[ШОРТ]</b>",
        "NEUTRAL": "<b>[ВНЕ РЫНКА]</b>",
        "CAUTIOUS": "<b>[ОСТОРОЖНО]</b>",
        "ARBITRAGE": "<b>[АРБИТРАЖ]</b>",
    }
    badge = stance_badges.get(response.market_stance.upper(), f"<b>[{response.market_stance}]</b>")
    ticker_hdr = f" · {response.ticker}" if response.ticker else ""

    lines = [
        f"<b>Трейдинг-деск</b>{ticker_hdr}",
        f"<b>Взгляд:</b> {badge} — {html.escape(response.thesis_title)}",
    ]

    # Add live price and stats header if available
    if market_context and market_context.last_price is not None and market_context.last_price > 0:
        price_str = f"<b>Текущая цена:</b> <code>{market_context.last_price:.2f} ₽</code>"
        if market_context.spread_bps is not None and market_context.spread_bps > 0:
            price_str += f" (спред: {market_context.spread_bps:.1f} б.п.)"
        lines.append(price_str)
        if market_context.positions_ratio_summary:
            lines.append(f"<b>Позиции:</b> {html.escape(market_context.positions_ratio_summary)}")

    lines.append("")

    if response.microstructure_analysis:
        lines.append("<b>Микроструктура и лента:</b>")
        lines.append(html.escape(response.microstructure_analysis))
        lines.append("")

    if response.news_drivers_analysis:
        lines.append("<b>Новостной фон и драйверы:</b>")
        lines.append(html.escape(response.news_drivers_analysis))
        lines.append("")

    if response.key_levels:
        lines.append("<b>Ключевые уровни:</b>")
        for lvl in response.key_levels:
            lines.append(f"• {html.escape(lvl)}")
        lines.append("")

    if response.actionable_setup:
        lines.append(f"<b>Торговый сетап:</b> <i>{html.escape(response.actionable_setup)}</i>")

    if response.risk_factors:
        lines.append("")
        lines.append("<b>Факторы риска:</b>")
        for rf in response.risk_factors:
            lines.append(f"• {html.escape(rf)}")

    if response.news_catalysts:
        lines.append("")
        lines.append("<b>Источники:</b>")
        for cat in response.news_catalysts[:3]:
            lines.append(f"• <i>{html.escape(cat)}</i>")

    return "\n".join(lines).strip()
