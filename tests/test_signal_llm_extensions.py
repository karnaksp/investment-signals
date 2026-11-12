"""Unit and integration tests for extended LLM capabilities:
1. Pre-Market Briefing
2. Morning Retracement Guard
3. Automated Loss Attribution
4. Derivatives & Spread Anomaly Interpretation
6. Dividend Gap Guard
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
import pytest

from tinvest_signal_engine.application.signal_llm import (
    LlmAnalyzerPort,
    NewsProviderPort,
    SignalLlmEnrichmentUseCase,
)
from tinvest_signal_engine.domain.signal_llm import (
    DerivativesAnomalyContext,
    DerivativesAnomalyResult,
    DividendGapContext,
    DividendGapDecision,
    LossAttributionContext,
    LossAttributionResult,
    NewsArticle,
    PreMarketBriefingContext,
    PreMarketBriefingResult,
    RetracementGuardContext,
    RetracementGuardDecision,
    format_derivatives_anomaly_html,
    format_dividend_gap_html,
    format_loss_attribution_html,
    format_premarket_briefing_html,
    format_retracement_guard_html,
)
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


class MockNewsProvider(NewsProviderPort):
    def get_recent_news(
        self, ticker: str, *, max_age_hours: int = 4, limit: int = 5, payload: dict[str, Any] | None = None
    ) -> tuple[NewsArticle, ...]:
        if ticker == "SHOCK":
            return (
                NewsArticle(title="Совет директоров рекомендовал не выплачивать дивиденды", source="Интерфакс", ticker=ticker),
            )
        if ticker == "DIV_TICKER":
            return (
                NewsArticle(title="Акции торгуются последний день с дивидендами", source="РБК", ticker=ticker),
            )
        return (
            NewsArticle(title="Плановые операции на открытии рынка", source="ТАСС", ticker=ticker),
        )


class MockLlmAnalyzer(LlmAnalyzerPort):
    def is_news_relevant(self, ticker: str, signal_type: str, news_title: str) -> bool:
        return "дивиденд" in news_title.lower() or "отчет" in news_title.lower()

    def analyze_signal(self, context: Any) -> Any:
        return None

    def analyze_position_report(self, context: Any) -> Any:
        return None

    def generate_premarket_briefing(self, context: PreMarketBriefingContext) -> PreMarketBriefingResult | None:
        return PreMarketBriefingResult(
            trading_date=context.trading_date,
            overall_sentiment="ПОЗИТИВНЫЙ",
            sentiment_headline="Нефть растет на фоне геополитики, фьючерсы в плюсе",
            key_drivers=("Brent выше $74", "Укрепление юаня"),
            focus_sectors=("Нефтегаз", "Финансы"),
            risk_factors=("Данные по инфляции вечером",),
            actionable_summary="Ожидается сильное открытие экспортеров, следить за пробоем 305 по SBER.",
            model_used="gemini-mock",
            tokens_used=150,
        )

    def evaluate_retracement_guard(self, context: RetracementGuardContext) -> RetracementGuardDecision | None:
        if context.ticker == "SHOCK":
            return RetracementGuardDecision(
                ticker=context.ticker,
                verdict="BLOCKED",
                is_fundamental_shock=True,
                shock_reason="Отмена дивидендов советом директоров",
                recommendation="Вход запрещен, риск безоткатного тренда",
                confidence_score=0.95,
                model_used="gemini-mock",
                tokens_used=120,
            )
        return RetracementGuardDecision(
            ticker=context.ticker,
            verdict="CONFIRMED",
            is_fundamental_shock=False,
            shock_reason="Фундаментальный шок отсутствует",
            recommendation="Гэп технический, статистика закрытия валидна",
            confidence_score=0.88,
            model_used="gemini-mock",
            tokens_used=95,
        )

    def analyze_loss_attribution(self, context: LossAttributionContext) -> LossAttributionResult | None:
        return LossAttributionResult(
            signal_id=context.signal_id,
            category="MARKET_PULLBACK",
            category_ru="Общерыночная коррекция",
            root_cause="Резкий разворот индекса IMOEX на фоне укрепления рубля выбил стоп-лосс.",
            is_preventable=False,
            calibration_suggestion="Увеличить порог z-score в условиях высокой корреляции с индексом.",
            model_used="gemini-mock",
            tokens_used=110,
        )

    def interpret_derivatives_anomaly(self, context: DerivativesAnomalyContext) -> DerivativesAnomalyResult | None:
        return DerivativesAnomalyResult(
            chain_id=context.chain_id,
            anomaly_title="Расширение календарного спреда",
            market_driver="Рынок закладывает повышение ключевой ставки ЦБ на ближайшем заседании.",
            trade_setup="Покупка ближнего фьючерса против продажи дальнего при спреде > 350 п.п.",
            risk_assessment="Риск неожиданного смягчения риторики регулятора.",
            confidence_score=0.85,
            model_used="gemini-mock",
            tokens_used=140,
        )

    def check_dividend_gap(self, context: DividendGapContext) -> DividendGapDecision | None:
        if context.ticker == "DIV_TICKER" or context.price_change_pct <= -5.0:
            return DividendGapDecision(
                ticker=context.ticker,
                is_dividend_gap=True,
                dividend_rub=33.3,
                yield_pct=6.8,
                should_suppress_short=True,
                explanation="Плановый экс-дивидендный гэп, реестр закрывается Т+1.",
                confidence_score=0.98,
                model_used="gemini-mock",
                tokens_used=80,
            )
        return DividendGapDecision(
            ticker=context.ticker,
            is_dividend_gap=False,
            should_suppress_short=False,
            explanation="Обычное рыночное падение котировок, отсечек не зафиксировано.",
            confidence_score=0.90,
            model_used="gemini-mock",
            tokens_used=75,
        )


@pytest.fixture
def llm_service():
    return SignalLlmEnrichmentUseCase(
        news_provider=MockNewsProvider(),
        llm_analyzer=MockLlmAnalyzer(),
        enabled=True,
    )


def test_premarket_briefing_formatting(llm_service):
    ctx = PreMarketBriefingContext(trading_date="2026-11-12")
    res = llm_service.generate_premarket_briefing(ctx)
    assert res is not None
    assert res.overall_sentiment == "ПОЗИТИВНЫЙ"
    html = format_premarket_briefing_html(res)
    assert "Утренний компас сессии" in html
    assert "ПОЗИТИВНЫЙ" in html
    assert "Brent выше $74" in html
    assert "Нефтегаз" in html


def test_retracement_guard_confirmed_vs_blocked(llm_service):
    # 1. Normal technical gap
    normal_payload = {
        "expected_direction": "up",
        "entry_reference_price": 280.0,
        "target_price": 284.0,
        "model_probability": 0.74,
    }
    enriched_normal = llm_service.enrich_retracement_signal(normal_payload, ticker="SBER")
    assert enriched_normal["retracement_guard"]["verdict"] == "CONFIRMED"
    assert "ПОДТВЕРЖДЕНО" in enriched_normal["retracement_guard_html"]

    # 2. Shock gap
    shock_payload = {
        "expected_direction": "up",
        "entry_reference_price": 150.0,
        "target_price": 155.0,
        "model_probability": 0.70,
    }
    enriched_shock = llm_service.enrich_retracement_signal(shock_payload, ticker="SHOCK")
    assert enriched_shock["retracement_guard"]["verdict"] == "BLOCKED"
    assert enriched_shock["retracement_guard"]["is_fundamental_shock"] is True
    assert "СИГНАЛ БЛОКИРОВАН" in enriched_shock["retracement_guard_html"]


def test_derivatives_anomaly_interpretation(llm_service):
    payload = {
        "chain_id": "SR",
        "leg1_ticker": "SRZ6",
        "leg2_ticker": "SRH7",
    }
    enriched = llm_service.enrich_derivatives_anomaly(
        payload,
        chain_id="SR",
        signal_type="calendar_spread_dislocation",
        z_score=3.2,
        metric_value=420.0,
        baseline_value=120.0,
        ticker="SR",
    )
    assert "derivatives_interpretation" in enriched
    assert enriched["derivatives_interpretation"]["anomaly_title"] == "Расширение календарного спреда"
    html = enriched["derivatives_interpretation_html"]
    assert "Анализ структуры кривой" in html
    assert "Покупка ближнего фьючерса" in html


def test_dividend_gap_guard(llm_service):
    # 1. Real dividend drop
    decision = llm_service.check_dividend_gap(ticker="DIV_TICKER", price_change_pct=-6.8)
    assert decision is not None
    assert decision.is_dividend_gap is True
    assert decision.should_suppress_short is True
    html = format_dividend_gap_html(decision)
    assert "Дивидендная отсечка" in html
    assert "33.3 ₽" in html

    # 2. Regular small drop
    decision_normal = llm_service.check_dividend_gap(ticker="GAZP", price_change_pct=-2.1)
    assert decision_normal is not None
    assert decision_normal.is_dividend_gap is False
    assert decision_normal.should_suppress_short is False


def test_loss_attribution_post_mortem(llm_service):
    ctx = LossAttributionContext(
        signal_id="sig-12345",
        ticker="LKOH",
        signal_type="price_jump",
        direction="LONG",
        entry_price=7100.0,
        exit_price=7010.0,
        loss_pct=1.27,
        horizon_seconds=300,
    )
    res = llm_service.analyze_loss_attribution(ctx)
    assert res is not None
    assert res.category == "MARKET_PULLBACK"
    assert "Общерыночная коррекция" in res.category_ru
    html = format_loss_attribution_html(res, ticker="LKOH")
    assert "Постмортем сигнала · LKOH" in html
    assert "Общерыночная коррекция" in html


def _sample_signal(**kwargs) -> TriggerSignal:
    defaults = dict(
        signal_id="00000000-0000-4000-8000-000000000001",
        detected_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        instrument_id="inst-1",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="confirmed_impulse_long",
        severity=2,
        metric_value=1200.0,
        baseline_value=400.0,
        z_score=4.2,
        window_seconds=60,
        summary="SBER test signal",
        payload={"price_change_pct": 2.1, "quality_score": 0.85},
    )
    defaults.update(kwargs)
    return TriggerSignal(**defaults)


def test_enrich_signal_for_delivery_integration(llm_service):
    # Test retracement recommendation integration
    ret_sig = _sample_signal(
        signal_id="ret-001",
        ticker="SBER",
        signal_type="morning_retracement_recommendation",
        summary="Утренний возврат SBER",
        payload={
            "expected_direction": "up",
            "entry_reference_price": 280.0,
            "target_price": 284.0,
            "model_probability": 0.74,
        },
    )
    enriched = enrich_signal_for_delivery(ret_sig, llm_service=llm_service)
    assert "retracement_guard" in enriched.payload
    assert "LLM Guard [ПОДТВЕРЖДЕНО]" in enriched.payload["telegram_html"]

    # Test derivatives anomaly integration
    deriv_sig = _sample_signal(
        signal_id="deriv-001",
        ticker="Si",
        signal_type="calendar_spread_dislocation",
        metric_value=320.0,
        baseline_value=110.0,
        z_score=2.8,
        summary="Дислокация спреда Si",
        payload={"chain_id": "Si", "leg1_ticker": "SiZ6", "leg2_ticker": "SiH7"},
    )
    enriched_deriv = enrich_signal_for_delivery(deriv_sig, llm_service=llm_service)
    assert "derivatives_interpretation" in enriched_deriv.payload
    assert "Анализ структуры кривой" in enriched_deriv.payload["telegram_html"]

    # Test dividend gap guard integration on sharp drop
    div_sig = _sample_signal(
        signal_id="div-001",
        ticker="DIV_TICKER",
        signal_type="price_jump",
        summary="Скачок цены вниз",
        payload={"price_change_pct": -6.8},
    )
    enriched_div = enrich_signal_for_delivery(div_sig, llm_service=llm_service)
    assert "dividend_gap" in enriched_div.payload
    assert enriched_div.payload["dividend_gap"]["is_dividend_gap"] is True
    assert "Дивидендная отсечка" in enriched_div.payload["telegram_html"]
    assert "[ДИВГЭП]" in enriched_div.summary

