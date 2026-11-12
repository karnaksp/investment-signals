"""Google Gemini adapter implementing LlmAnalyzerPort."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from tinvest_signal_engine.application.signal_llm import LlmAnalyzerPort
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
    PreMarketBriefingContext,
    PreMarketBriefingResult,
    RetracementGuardContext,
    RetracementGuardDecision,
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
        timeout_seconds: float = 25.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._api_key = api_key.strip()
        self._gatekeeper_model = gatekeeper_model
        self._analyzer_model = analyzer_model
        self._timeout = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds, trust_env=False)

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

        news_text = (
            "\n".join(
                f"- [{n.source or 'Новость'}] {n.title}" + (f": {n.summary}" if n.summary else "")
                for n in context.news_items
            )
            if context.news_items
            else "Специфичных корпоративных новостей в ленте за последние часы не зафиксировано (технический импульс / крупная заявка)."
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

    def analyze_position_report(
        self, context: AssetPositionLlmContext
    ) -> AssetPositionAnalysisResult | None:
        """Analyze long/short position breakdown, macro drivers and sentiment."""
        if not self.enabled:
            return None

        news_text = (
            "\n".join(
                f"- [{n.source or 'Новость'}] {n.title}" + (f": {n.summary}" if n.summary else "")
                for n in context.recent_news
            )
            if context.recent_news
            else "Специфичных срочных новостей за последние часы не зафиксировано."
        )

        system_prompt = (
            "Ты — старший макро-аналитик сырьевых и валютных рынков (эксперт по отчетам CoT / SHFE / Мосбиржи). "
            "Твоя задача — проанализировать отчет о позиционировании игроков (лонги/шорты фондов, производителей и розницы), "
            "сопоставить его с долгосрочной динамикой актива и новостным фоном и сформировать точный, профессиональный "
            "вывод в формате строгого JSON без лишних преамбул."
        )

        user_prompt = f"""АКТИВ: {context.commodity_name} (код: {context.commodity.upper()})
ДАТА ОТЧЕТА: {context.trading_day} (к {context.previous_day}), биржа: {context.source_exchange}

РАСКЛАД ПОЗИЦИЙ ПО ПЛОЩАДКАМ И ГРУППАМ:
{context.sections_summary}

НОВОСТНОЙ И МАКРО-ФОН:
{news_text}

Сформируй JSON со следующими полями:
- "sentiment": одно из: "BULLISH", "BEARISH", "NEUTRAL", "OVERHEATED_BULLISH", "OVERHEATED_BEARISH", "SQUEEZE_RISK"
- "bias_title": емкий аналитический заголовок (1 строка на русском)
- "smart_money_summary": действия крупных фондов/институционалов/производителей (1-2 емких предложения на русском)
- "crowd_bias": действия розницы/клиентов/физлиц (1 предложение на русском)
- "interpretation": структурный смысл текущего перевеса позиций и риска сквиза/разворота (2 предложения на русском)
- "macro_drivers": ключевой макроэкономический драйвер и долгосрочный контекст актива (1-2 предложения на русском)
- "actionable_advice": практическая рекомендация для инвестора/трейдера с риск-менеджментом (1-2 предложения на русском)
- "risk_factors": главный риск для описанного сценария (1 предложение на русском)
"""

        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 500,
                "temperature": 0.2,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                logger.warning(
                    "Gemini position analyzer returned HTTP %s",
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

            return AssetPositionAnalysisResult(
                commodity=context.commodity,
                sentiment=str(parsed.get("sentiment") or "NEUTRAL").strip(),
                bias_title=str(parsed.get("bias_title") or "").strip(),
                smart_money_summary=str(parsed.get("smart_money_summary") or "").strip(),
                crowd_bias=str(parsed.get("crowd_bias") or "").strip(),
                interpretation=str(parsed.get("interpretation") or "").strip(),
                macro_drivers=str(parsed.get("macro_drivers") or "").strip(),
                actionable_advice=str(parsed.get("actionable_advice") or "").strip(),
                risk_factors=str(parsed.get("risk_factors") or "").strip(),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini position analyzer error for %s: %s", context.commodity, ex)
            return None

    def generate_premarket_briefing(
        self, context: PreMarketBriefingContext
    ) -> PreMarketBriefingResult | None:
        """Synthesize daily pre-market briefing and morning trading compass."""
        if not self.enabled:
            return None

        news_text = "\n".join(
            f"- {a.title} ({a.source})" for a in context.overnight_news[:4]
        ) or "Существенных ночных корпоративных новостей нет."

        corp_text = "\n".join(f"- {e}" for e in context.corporate_events[:4]) or "Плановых отчетов на утро нет."
        macro_text = "\n".join(f"- {e}" for e in context.macro_events[:3]) or "Ключевых макро-релизов до полудня нет."

        system_prompt = (
            "Ты — главный стратег инвестиционной компании по российскому фондовому и срочному рынку (MOEX). "
            "Твоя задача — сформировать емкий, профессиональный 'Утренний компас сессии' перед открытием торгов."
        )

        brent_str = f"{context.brent_price:g}" if context.brent_price is not None else "Н/Д"
        brent_d = f"{context.brent_change_pct:+.1f}%" if context.brent_change_pct is not None else "+0.0%"
        gold_str = f"{context.gold_price:g}" if context.gold_price is not None else "Н/Д"
        gold_d = f"{context.gold_change_pct:+.1f}%" if context.gold_change_pct is not None else "+0.0%"

        user_prompt = f"""ДАТА: {context.trading_date}
ВНЕШНИЙ ФОН И СЫРЬЕ:
- Нефть Brent: {brent_str} (изм: {brent_d})
- Золото: {gold_str} (изм: {gold_d})
- Азиатские площадки: {context.asia_sentiment or 'смешанная динамика'}
- Фьючерсы США: {context.us_futures_sentiment or 'нейтрально'}

НОЧНЫЕ НОВОСТИ:
{news_text}

СОБЫТИЯ И КАЛЕНДАРЬ:
{corp_text}
{macro_text}

Сформируй JSON со следующими полями:
- "overall_sentiment": строго одно из: "ПОЗИТИВНЫЙ", "НЕГАТИВНЫЙ", "НЕЙТРАЛЬНЫЙ", "СМЕШАННЫЙ"
- "sentiment_headline": емкий заголовок баланса сил (1 строка на русском)
- "key_drivers": список из 2-3 ключевых рыночных драйверов (короткие фразы на русском)
- "focus_sectors": список из 2-3 секторов, где ожидается активность (на русском)
- "risk_factors": список из 1-2 главных рисков утренней сессии (на русском)
- "actionable_summary": практическое резюме для трейдера перед открытием (1-2 предложения на русском)
"""
        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 450,
                "temperature": 0.2,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)
            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            return PreMarketBriefingResult(
                trading_date=context.trading_date,
                overall_sentiment=str(parsed.get("overall_sentiment") or "НЕЙТРАЛЬНЫЙ").strip(),
                sentiment_headline=str(parsed.get("sentiment_headline") or "").strip(),
                key_drivers=tuple(str(x).strip() for x in (parsed.get("key_drivers") or [])),
                focus_sectors=tuple(str(x).strip() for x in (parsed.get("focus_sectors") or [])),
                risk_factors=tuple(str(x).strip() for x in (parsed.get("risk_factors") or [])),
                actionable_summary=str(parsed.get("actionable_summary") or "").strip(),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini premarket briefing error: %s", ex)
            return None

    def evaluate_retracement_guard(
        self, context: RetracementGuardContext
    ) -> RetracementGuardDecision | None:
        """Check if morning opening gap is caused by corporate shock vs technical."""
        if not self.enabled:
            return None

        news_text = "\n".join(
            f"- {a.title} ({a.source})" for a in context.news_items[:4]
        ) or "Свежих новостей по тикеру за последние 12 часов нет."

        system_prompt = (
            "Ты — риск-менеджер торговой системы утреннего возврата цены (Morning Retracement) на MOEX. "
            "Твоя задача — не допустить вход в сделку на закрытие гэпа, если гэп вызван корпоративным или санкционным шоком "
            "(отмена дивидендов, допэмиссия, санкции, авария, неожиданный убыток), так как при шоке цена продолжит безоткатное движение."
        )

        gap_side = "ВНИЗ" if context.gap_direction.lower() in ("up", "вверх") else "ВВЕРХ"

        user_prompt = f"""ТИКЕР: {context.ticker}
НАПРАВЛЕНИЕ ГЭПА: {gap_side}
РАЗМЕР ГЭПА: {context.gap_pct:.2f}%
ВЕРОЯТНОСТЬ ВОЗВРАТА ПО СТАТИСТИКЕ: {context.model_probability * 100:.0f}%

СВЕЖИЕ НОВОСТИ И СОБЫТИЯ ПО ТИКЕРУ:
{news_text}

Оцени, вызван ли гэп фундаментальным шоком:
Сформируй JSON со следующими полями:
- "is_fundamental_shock": true если гэп вызван реальным шоком/новостью, false если это технический рыночный гэп
- "verdict": строго одно из: "CONFIRMED" (гэп технический, возврат валиден), "WARNING" (есть новостной фон, повышенный риск), "BLOCKED" (сильный корпоративный шок, риск безоткатного тренда, сделка запрещена)
- "shock_reason": в 1 предложении поясни причину гэпа или укажи 'Фундаментальный шок отсутствует'
- "recommendation": краткая рекомендация трейдеру по стопу или размеру позиции (1 предложение)
- "confidence_score": число от 0.0 до 1.0
"""
        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 300,
                "temperature": 0.1,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)
            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            return RetracementGuardDecision(
                ticker=context.ticker,
                verdict=str(parsed.get("verdict") or "CONFIRMED").strip().upper(),
                is_fundamental_shock=bool(parsed.get("is_fundamental_shock")),
                shock_reason=str(parsed.get("shock_reason") or "").strip(),
                recommendation=str(parsed.get("recommendation") or "").strip(),
                confidence_score=float(parsed.get("confidence_score") or 0.8),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini retracement guard error for %s: %s", context.ticker, ex)
            return None

    def analyze_loss_attribution(
        self, context: LossAttributionContext
    ) -> LossAttributionResult | None:
        """Run post-mortem analysis for a losing signal."""
        if not self.enabled:
            return None

        news_text = "\n".join(
            f"- {a.title} ({a.source})" for a in context.subsequent_news[:3]
        ) or "В период удержания позиции явных новостей не зафиксировано."

        system_prompt = (
            "Ты — квант-аналитик по аудиту точности торговых сигналов. "
            "Твоя задача — провести объективный постмортем убыточного сигнала (Loss Attribution) для выявления причин стоп-лосса."
        )

        user_prompt = f"""СИГНАЛ: ID {context.signal_id} по {context.ticker}
ТИП СИГНАЛА: {context.signal_type}
НАПРАВЛЕНИЕ: {context.direction}
ВХОД: {context.entry_price}, ВЫХОД: {context.exit_price} (убыток: {context.loss_pct:.2f}%)
ГОРИЗОНТ: {context.horizon_seconds} сек
НОВОСТИ ПОСЛЕ СИГНАЛА:
{news_text}
КОНТЕКСТ РЫНКА: {context.market_context or 'Стандартная сессия'}

Сформируй JSON со следующими полями:
- "category": строго одно из: "NEWS_SHOCK", "MARKET_PULLBACK", "LOW_LIQUIDITY_SLIPPAGE", "TREND_EXHAUSTION", "FALSE_BREAKOUT", "OTHER"
- "category_ru": понятное название категории на русском (например 'Резкий новостной разворот', 'Общерыночная коррекция')
- "root_cause": точная причина срабатывания стопа (1-2 предложения на русском)
- "is_preventable": true если убыток можно было предотвратить предсигнальным фильтром, иначе false
- "calibration_suggestion": практический совет для калибровки детектора (1 предложение на русском)
"""
        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 300,
                "temperature": 0.2,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)
            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            return LossAttributionResult(
                signal_id=context.signal_id,
                category=str(parsed.get("category") or "OTHER").strip(),
                category_ru=str(parsed.get("category_ru") or "Рыночный шум").strip(),
                root_cause=str(parsed.get("root_cause") or "").strip(),
                is_preventable=bool(parsed.get("is_preventable")),
                calibration_suggestion=str(parsed.get("calibration_suggestion") or "").strip(),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini loss attribution error for %s: %s", context.signal_id, ex)
            return None

    def interpret_derivatives_anomaly(
        self, context: DerivativesAnomalyContext
    ) -> DerivativesAnomalyResult | None:
        """Interpret term structure, calendar spread or basis anomaly."""
        if not self.enabled:
            return None

        system_prompt = (
            "Ты — ведущий трейдер по срочному рынку (деривативы MOEX: фьючерсы на акции, валюты, сырье, индексы). "
            "Твоя задача — перевести математическую дислокацию кривой или спреда в прикладную рыночную логику и сетап."
        )

        user_prompt = f"""СЕРИЯ / ИНСТРУМЕНТ: {context.chain_id} (тикер: {context.ticker})
ТИП АНОМАЛИИ: {context.signal_type}
МЕТРИКА: значение {context.metric_value:g}, baseline {context.baseline_value:g}, z-score {context.z_score:+.2f}
КОНТРАКТЫ: ближний {context.front_contract or 'H1'}, дальний {context.back_contract or 'H2'}

Сформируй JSON со следующими полями:
- "anomaly_title": емкое русское название аномалии (например 'Расширение календарного спреда', 'Инверсия кривой / бэквордация')
- "market_driver": вероятная экономическая причина (ставка ЦБ, дефицит физического актива, дивиденды, налоги) (1-2 предложения на русском)
- "trade_setup": торговый сетап (календарный арбитраж, кэрри, покупка/продажа спреда) (1-2 предложения на русском)
- "risk_assessment": ключевой риск данной конструкции (1 предложение на русском)
- "confidence_score": число от 0.0 до 1.0
"""
        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 350,
                "temperature": 0.2,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)
            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            return DerivativesAnomalyResult(
                chain_id=context.chain_id,
                anomaly_title=str(parsed.get("anomaly_title") or context.signal_type).strip(),
                market_driver=str(parsed.get("market_driver") or "").strip(),
                trade_setup=str(parsed.get("trade_setup") or "").strip(),
                risk_assessment=str(parsed.get("risk_assessment") or "").strip(),
                confidence_score=float(parsed.get("confidence_score") or 0.8),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini derivatives interpretation error for %s: %s", context.chain_id, ex)
            return None

    def check_dividend_gap(
        self, context: DividendGapContext
    ) -> DividendGapDecision | None:
        """Check whether price drop is an expected ex-dividend gap."""
        if not self.enabled:
            return None

        news_text = "\n".join(
            f"- {a.title} ({a.source})" for a in context.news_items[:3]
        ) or "Новостей по тикеру не обнаружено."

        system_prompt = (
            "Ты — аналитик дивидендного календаря российского фондового рынка (MOEX). "
            "Твоя задача — определить, является ли зафиксированное падение котировок плановой дивидендной отсечкой (экс-дивидендный день)."
        )

        user_prompt = f"""ТИКЕР: {context.ticker}
ИЗМЕНЕНИЕ ЦЕНЫ: {context.price_change_pct:+.2f}%
ДАТА СОБЫТИЯ: {context.event_date}
НОВОСТИ ПО ЭМИТЕНТУ:
{news_text}

Сформируй JSON со следующими полями:
- "is_dividend_gap": true если падение совпадает с дивидендной отсечкой, иначе false
- "dividend_rub": ориентировочный размер дивиденда на акцию в рублях (число или null)
- "yield_pct": ориентировочная дивидендная доходность в % (число или null)
- "should_suppress_short": true если это дивидендный гэп и импульсные сигналы на шорт нужно подавить, иначе false
- "explanation": краткое пояснение ситуации (1-2 предложения на русском)
- "confidence_score": число от 0.0 до 1.0
"""
        url = f"{GEMINI_API_BASE}/models/{self._analyzer_model}:generateContent"
        payload: dict[str, Any] = {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "responseMimeType": "application/json",
                "maxOutputTokens": 250,
                "temperature": 0.1,
                "thinkingConfig": {"thinkingBudget": 0},
            },
        }

        try:
            resp = self._post_with_fallback(url, payload)
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            candidates = data.get("candidates") or []
            if not candidates:
                return None
            raw_text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "").strip()
            parsed = json.loads(raw_text)
            usage = data.get("usageMetadata", {})
            tokens_used = int(usage.get("totalTokenCount") or 0)

            is_div = bool(parsed.get("is_dividend_gap"))
            div_val = parsed.get("dividend_rub")
            yield_val = parsed.get("yield_pct")
            return DividendGapDecision(
                ticker=context.ticker,
                is_dividend_gap=is_div,
                dividend_rub=float(div_val) if div_val is not None else None,
                yield_pct=float(yield_val) if yield_val is not None else None,
                should_suppress_short=bool(parsed.get("should_suppress_short", is_div)),
                explanation=str(parsed.get("explanation") or "").strip(),
                confidence_score=float(parsed.get("confidence_score") or 0.8),
                model_used=self._analyzer_model,
                tokens_used=tokens_used,
            )
        except Exception as ex:
            logger.warning("Gemini dividend gap check error for %s: %s", context.ticker, ex)
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
