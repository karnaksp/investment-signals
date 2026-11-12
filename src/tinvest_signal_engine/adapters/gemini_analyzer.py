"""Google Gemini adapter implementing LlmAnalyzerPort."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from tinvest_signal_engine.application.signal_llm import LlmAnalyzerPort
from tinvest_signal_engine.domain.signal_llm import (
    LlmAnalysisResult,
    LlmSignalContext,
)

logger = logging.getLogger(__name__)

GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"


class GeminiLlmAnalyzer(LlmAnalyzerPort):
    """Communicates with Google Gemini models using httpx without external SDKs."""

    def __init__(
        self,
        *,
        api_key: str,
        gatekeeper_model: str = "gemini-3.5-flash-lite",
        analyzer_model: str = "gemini-3.5-flash",
        timeout_seconds: float = 4.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._api_key = api_key.strip()
        self._gatekeeper_model = gatekeeper_model
        self._analyzer_model = analyzer_model
        self._timeout = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def is_news_relevant(
        self, ticker: str, signal_type: str, news_title: str
    ) -> bool:
        """Fast binary check using lightweight Flash-Lite model."""
        if not self.enabled or not news_title.strip():
            return False

        prompt = (
            f'Тикер: {ticker}. Движение: {signal_type}. Новость: "{news_title.strip()}".\n'
            "Является ли эта новость прямым фундаментальным или корпоративным катализатором "
            "движения котировок (отчетность, дивиденды, санкции, сделки M&A, аварии, регулирование)?\n"
            "Ответь строго одним словом: ДА или НЕТ."
        )

        url = f"{GEMINI_API_BASE}/models/{self._gatekeeper_model}:generateContent"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 10,
                "temperature": 0.0,
            },
        }

        try:
            resp = self._client.post(
                url,
                headers={"x-goog-api-key": self._api_key},
                json=payload,
                timeout=self._timeout,
            )
            if resp.status_code != 200:
                logger.warning(
                    "Gemini gatekeeper returned HTTP %s: %s",
                    resp.status_code,
                    resp.text[:150],
                )
                return False

            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return False

            text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip().upper()
            return text.startswith("ДА") or text.startswith("YES") or text == "ДА"
        except Exception as ex:
            logger.warning("Gemini gatekeeper error for %s: %s", ticker, ex)
            return False

    def analyze_signal(
        self, context: LlmSignalContext
    ) -> LlmAnalysisResult | None:
        """Structured analysis of signal and filtered news using Flash model."""
        if not self.enabled:
            return None

        news_text = "\n".join(
            f"- [{n.source or 'Новость'}] {n.title}" + (f": {n.summary}" if n.summary else "")
            for n in context.news_items
        )

        facts_lines = [
            f"Инструмент: {context.ticker}",
            f"Сигнал: {context.signal_type}",
        ]
        if context.headline_ru:
            facts_lines.append(f"Интерпретация движка: {context.headline_ru}")
        if context.price_change_pct is not None:
            facts_lines.append(f"Изменение цены: {context.price_change_pct:+.2f}%")
        if context.volume_ratio is not None:
            facts_lines.append(f"Кратность объема к норме: {context.volume_ratio:.1f}x")
        if context.orderbook_imbalance is not None:
            facts_lines.append(f"Дисбаланс стакана (OBI): {context.orderbook_imbalance:+.2f}")
        if context.market_regime:
            facts_lines.append(f"Общий рынок: {context.market_regime}")

        system_prompt = (
            "Ты — профессиональный финансовый аналитик Мосбиржи и микроструктуры рынка. "
            "Твоя задача — сопоставить техническую аномалию цены/объема с подтвержденным новостным фоном "
            "и сформировать точный, емкий торговый сценарий в формате строгого JSON без лишних вводных слов."
        )

        user_prompt = f"""РЫНОЧНЫЕ ФАКТЫ:
{chr(10).join(facts_lines)}

ПОДТВЕРЖДЕННЫЕ НОВОСТИ:
{news_text}

Сформируй JSON со следующими полями:
- "has_catalyst": boolean (есть ли прямая связь с новостью)
- "catalyst_summary": string (суть катализатора в 1 предложение на русском)
- "reaction_type": string (одно из: FUNDAMENTAL_PRICING, SPECULATIVE_PUMP, NOISE, LATE_OVERREACTION)
- "trade_bias": string ("LONG", "SHORT" или "NEUTRAL")
- "suggested_action": string (краткий план: BUY_MOMENTUM, BUY_PULLBACK, FADE, AVOID, HOLD)
- "risk_factor": string (главный риск для сценария в 1 предложение на русском)
- "invalidation_level": string (условие отмены сценария, например ценовой уровень)
- "confidence_score": float (от 0.0 до 1.0)
"""

        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 350,
                "temperature": 0.1,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                logger.warning(
                    "Gemini analyzer returned HTTP %s",
                    resp.status_code if resp else "None",
                )
                return None

            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None

            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)

            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            return LlmAnalysisResult(
                has_catalyst=bool(parsed.get("has_catalyst")),
                catalyst_summary=str(parsed.get("catalyst_summary") or "").strip(),
                reaction_type=str(parsed.get("reaction_type") or "UNKNOWN").strip(),
                trade_bias=str(parsed.get("trade_bias") or "NEUTRAL").strip(),
                suggested_action=str(parsed.get("suggested_action") or "AVOID").strip(),
                risk_factor=str(parsed.get("risk_factor") or "").strip(),
                invalidation_level=str(parsed.get("invalidation_level") or "").strip(),
                confidence_score=float(parsed.get("confidence_score") or 0.5),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini analyzer error for %s: %s", context.ticker, ex)
            return None

    def _post_with_fallback(
        self, url: str, payload: dict[str, Any]
    ) -> httpx.Response | None:
        """Post request with fallback in case thinkingConfig is not accepted."""
        headers = {"x-goog-api-key": self._api_key}
        try:
            resp = self._client.post(
                url, headers=headers, json=payload, timeout=self._timeout
            )
            if resp.status_code == 400 and "thinkingConfig" in payload.get("generationConfig", {}):
                # Try without thinkingConfig
                fallback_payload = dict(payload)
                gen_cfg = dict(fallback_payload.get("generationConfig", {}))
                gen_cfg.pop("thinkingConfig", None)
                fallback_payload["generationConfig"] = gen_cfg
                resp = self._client.post(
                    url,
                    headers=headers,
                    json=fallback_payload,
                    timeout=self._timeout,
                )
            return resp
        except Exception as ex:
            logger.warning("Gemini POST failed: %s", ex)
            return None
