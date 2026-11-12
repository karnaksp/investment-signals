"""Gemini-powered Prop-Desk Trader LLM adapter implementing TraderLlmPort."""

from __future__ import annotations

import json
import logging
from typing import Any, Sequence

import httpx

from tinvest_signal_engine.application.trader_copilot import TraderLlmPort
from tinvest_signal_engine.domain.trader_copilot import (
    InternetNewsItem,
    MarketContextSnapshot,
    TraderCopilotResponse,
    TraderDeskQuery,
)

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"


def _extract_json(text: str) -> dict[str, Any]:
    clean = text.strip()
    if clean.startswith("```"):
        import re
        clean = re.sub(r"^```(?:json)?\n?", "", clean)
        clean = re.sub(r"\n?```$", "", clean)
    try:
        return json.loads(clean)
    except Exception:
        start = clean.find("{")
        end = clean.rfind("}")
        if start != -1 and end > start:
            return json.loads(clean[start : end + 1])
        raise


class GeminiTraderCopilotAdapter(TraderLlmPort):
    """Institutional Senior Trader / Desk Strategist using Google Gemini Flash models with quota fallback."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-3.5-flash-lite",
        fallback_models: Sequence[str] = (
            "gemini-flash-lite-latest",
            "gemini-3-flash-preview",
            "gemini-3.5-flash",
        ),
        timeout_seconds: float = 25.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._api_key = api_key.strip()
        self._models = (model, *fallback_models)
        self._timeout = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds, trust_env=False)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def consult_trader(
        self,
        query: TraderDeskQuery,
        *,
        market_context: MarketContextSnapshot | None = None,
        news_items: Sequence[InternetNewsItem] = (),
    ) -> TraderCopilotResponse:
        system_instruction = (
            "Ты — ведущий институциональный проп-трейдер и стратег деска Московской биржи (Head of Trading Desk).\n"
            "Твой стиль: профессиональный, четкий, аналитический, строго классический деловой язык, цифры, стакан, поток ордеров и факты.\n"
            "КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО использовать эмодзи или смайлики (никаких 🟢, 🔴, 📊, 💡, ⚠️, 🏛, 🎯 и т.д.)!\n"
            "КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО писать дисклеймеры вроде 'не является инвестиционной рекомендацией' "
            "и упоминать, что ты модель / LLM / нейросеть / ассистент!\n"
            "Форматируй текст строго стандартным текстом и маркированными списками.\n"
            "Дай жесткий практический разбор: где уровни, где сидит ликвидность, в чем реальная суть новостей из интернета, "
            "каков торговый сетап (Long / Short / Flat / Arbitrage), точка отмены сценария (Stop Loss) и цели (Take Profit)."
        )

        # Assemble market context section
        market_section = "Рыночные данные отсутствуют."
        if market_context:
            m_lines = [f"Тикер: {market_context.ticker}"]
            if market_context.last_price is not None:
                m_lines.append(f"Последняя цена сделки: {market_context.last_price:.2f} ₽")
            if market_context.bid_price and market_context.ask_price:
                m_lines.append(f"Лучший Bid: {market_context.bid_price:.2f} ₽, Ask: {market_context.ask_price:.2f} ₽")
            if market_context.spread_bps is not None:
                m_lines.append(f"Спред: {market_context.spread_bps:.1f} б.п.")
            if market_context.vwap is not None:
                m_lines.append(f"Дневной VWAP: {market_context.vwap:.2f} ₽")
            if market_context.positions_ratio_summary:
                m_lines.append(f"Позиции участников (физики vs юрики): {market_context.positions_ratio_summary}")
            if market_context.recent_signals_summary:
                m_lines.append("Последние алерты системы:")
                for s in market_context.recent_signals_summary:
                    m_lines.append(f"  • {s}")
            market_section = "\n".join(m_lines)

        # Assemble internet news section
        news_section = "Свежие новости из сети не найдены."
        if news_items:
            n_lines = []
            for item in news_items[:5]:
                src = f" ({item.source})" if item.source else ""
                n_lines.append(f"• {item.title}{src}")
                if item.summary:
                    n_lines.append(f"  Кратко: {item.summary[:150]}")
            news_section = "\n".join(n_lines)

        user_prompt = (
            f"ВОПРОС ТРЕЙДЕРА: \"{query.query_text}\"\n"
        )
        if query.reply_to_text:
            user_prompt += f"КОНТЕКСТ СИГНАЛА/СООБЩЕНИЯ, НА КОТОРЫЙ ОТВЕЧАЕТ ПОЛЬЗОВАТЕЛЬ:\n\"{query.reply_to_text}\"\n\n"

        user_prompt += (
            f"ТЕКУЩАЯ МИКРОСТРУКТУРА РЫНКА:\n{market_section}\n\n"
            f"АКТУАЛЬНЫЙ ИНТЕРНЕТ-НОВОСТНОЙ ФОН:\n{news_section}\n\n"
            "СФОРМИРУЙ ОТВЕТ СТРОГО В ВИДЕ JSON:\n"
            "{\n"
            '  "market_stance": "BULLISH" | "BEARISH" | "NEUTRAL" | "CAUTIOUS" | "ARBITRAGE",\n'
            '  "thesis_title": "Краткий заголовок торговой гипотезы (до 8 слов)",\n'
            '  "microstructure_analysis": "Анализ стакана, спреда, VWAP и баланса сил (2-4 предложения)",\n'
            '  "news_drivers_analysis": "Синтез реального новостного фона и драйверов (2-4 предложения)",\n'
            '  "key_levels": ["Поддержка: ...", "Сопротивление: ...", "Отмена сценария: ..."],\n'
            '  "actionable_setup": "Конкретный сетап с точкой входа, SL и TP (1-2 предложения)",\n'
            '  "risk_factors": ["Риск 1", "Риск 2"],\n'
            '  "news_catalysts": ["Новость 1", "Новость 2"]\n'
            "}"
        )

        data = {
            "systemInstruction": {"parts": [{"text": system_instruction}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "temperature": 0.2,
                "maxOutputTokens": 2048,
            },
        }

        tokens = 0
        last_model = self._models[0]
        for candidate_model in self._models:
            last_model = candidate_model
            url = f"{GEMINI_API_BASE}/models/{candidate_model}:generateContent?key={self._api_key}"
            try:
                resp = self._client.post(url, json=data)
                if resp.status_code == 200:
                    body = resp.json()
                    usage = body.get("usageMetadata") or {}
                    tokens = int(usage.get("totalTokenCount") or 0)
                    candidates = body.get("candidates") or []
                    if candidates:
                        parts = candidates[0].get("content", {}).get("parts", [])
                        if parts and "text" in parts[0]:
                            raw_json = parts[0]["text"]
                            parsed = _extract_json(raw_json)
                            return TraderCopilotResponse(
                                ticker=query.ticker or (market_context.ticker if market_context else None),
                                market_stance=str(parsed.get("market_stance") or "NEUTRAL").upper(),
                                thesis_title=str(parsed.get("thesis_title") or "Консультация торгового деска"),
                                microstructure_analysis=str(parsed.get("microstructure_analysis") or ""),
                                news_drivers_analysis=str(parsed.get("news_drivers_analysis") or ""),
                                key_levels=tuple(str(k) for k in parsed.get("key_levels", [])),
                                actionable_setup=str(parsed.get("actionable_setup") or ""),
                                risk_factors=tuple(str(r) for r in parsed.get("risk_factors", [])),
                                news_catalysts=tuple(str(c) for c in parsed.get("news_catalysts", [])),
                                model_used=candidate_model,
                                tokens_used=tokens,
                            )
                elif resp.status_code in {429, 503}:
                    logger.warning("Gemini model %s busy or quota exceeded (%s), trying fallback model...", candidate_model, resp.status_code)
                    continue
                else:
                    logger.warning("Gemini Trader request to %s failed: %s %s", candidate_model, resp.status_code, resp.text[:200])
            except Exception as ex:
                logger.error("Error communicating with Gemini Trader model %s: %s", candidate_model, ex)

        # Fallback response if all models failed
        return TraderCopilotResponse(
            ticker=query.ticker,
            market_stance="CAUTIOUS",
            thesis_title="Деск временно не сформировал ответ",
            microstructure_analysis=f"Данные по тикеру: последняя цена {market_context.last_price if market_context else 'н/д'}.",
            news_drivers_analysis="Не удалось получить консультацию LLM. Проверьте подключение к сервису.",
            key_levels=(),
            actionable_setup="Воздержаться от сделок до восстановления связи с аналитическим деском.",
            risk_factors=("Сбой внешнего аналитического сервиса",),
            news_catalysts=(),
            model_used=last_model,
            tokens_used=tokens,
        )
