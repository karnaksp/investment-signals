"""Gemini-powered Prop-Desk Trader LLM adapter implementing TraderLlmPort."""

from __future__ import annotations

import json
import logging
import re
import time
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
        clean = re.sub(r"^```(?:json)?\n?", "", clean)
        clean = re.sub(r"\n?```$", "", clean)
    try:
        return json.loads(clean)
    except Exception:
        start = clean.find("{")
        end = clean.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(clean[start : end + 1])
            except Exception:
                pass

    # Resilient regex parser if json.loads failed (e.g. unescaped quote in text)
    parsed: dict[str, Any] = {}
    for key in (
        "market_stance",
        "thesis_title",
        "microstructure_analysis",
        "news_drivers_analysis",
        "actionable_setup",
    ):
        m = re.search(rf'"{key}"\s*:\s*"(.*?)(?:"\s*,\s*"\w+"|\s*"\s*}})', clean, re.DOTALL)
        if m:
            parsed[key] = m.group(1).strip()

    for list_key in ("key_levels", "risk_factors", "news_catalysts"):
        m = re.search(rf'"{list_key}"\s*:\s*\[(.*?)\]', clean, re.DOTALL)
        if m:
            items = re.findall(r'"(.*?)"', m.group(1))
            parsed[list_key] = items

    if parsed.get("thesis_title") or parsed.get("microstructure_analysis") or parsed.get("market_stance"):
        return parsed

    raise ValueError(f"Could not parse valid JSON from model response: {text[:200]}")


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
                "maxOutputTokens": 4096,
                "responseSchema": {
                    "type": "OBJECT",
                    "properties": {
                        "market_stance": {
                            "type": "STRING",
                            "enum": ["BULLISH", "BEARISH", "NEUTRAL", "CAUTIOUS", "ARBITRAGE"],
                        },
                        "thesis_title": {"type": "STRING"},
                        "microstructure_analysis": {"type": "STRING"},
                        "news_drivers_analysis": {"type": "STRING"},
                        "key_levels": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "actionable_setup": {"type": "STRING"},
                        "risk_factors": {"type": "ARRAY", "items": {"type": "STRING"}},
                        "news_catalysts": {"type": "ARRAY", "items": {"type": "STRING"}},
                    },
                    "required": [
                        "market_stance",
                        "thesis_title",
                        "microstructure_analysis",
                        "news_drivers_analysis",
                        "actionable_setup",
                    ],
                },
            },
        }

        tokens = 0
        last_model = self._models[0]
        for candidate_model in self._models:
            last_model = candidate_model
            url = f"{GEMINI_API_BASE}/models/{candidate_model}:generateContent?key={self._api_key}"
            for attempt in range(2):
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
                    elif resp.status_code == 503:
                        logger.warning("Gemini model %s busy (503), attempt %d/2", candidate_model, attempt + 1)
                        if attempt == 0:
                            time.sleep(1.2)
                            continue
                    elif resp.status_code == 429:
                        logger.warning("Gemini model %s quota exceeded (429), trying fallback model...", candidate_model)
                        break
                    else:
                        logger.warning("Gemini Trader request to %s failed: %s %s", candidate_model, resp.status_code, resp.text[:200])
                        break
                except Exception as ex:
                    logger.error("Error communicating with Gemini Trader model %s: %s", candidate_model, ex)
                    break

        # Fallback response if all models failed
        return TraderCopilotResponse(
            ticker=query.ticker,
            market_stance="CAUTIOUS",
            thesis_title="Деск формирует оценку",
            microstructure_analysis=(
                f"Данные по инструменту: последняя цена {market_context.last_price:.2f} ₽."
                if (market_context and market_context.last_price is not None)
                else "Данные стакана и ленты временно недоступны."
            ),
            news_drivers_analysis="Высокая нагрузка на аналитический шлюз котировок. Повторите запрос через несколько секунд.",
            key_levels=(),
            actionable_setup="Воздержаться от сделок до обновления аналитической сводки.",
            risk_factors=("Временная задержка ответа аналитического шлюза",),
            news_catalysts=(),
            model_used=last_model,
            tokens_used=tokens,
        )
