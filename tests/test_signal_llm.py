"""Unit tests for LLM signal analysis, news filtering, and enrichment."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from tinvest_signal_engine.application.signal_llm import (
    LlmAnalyzerPort,
    NewsProviderPort,
    SignalLlmEnrichmentUseCase,
)
from tinvest_signal_engine.adapters.news_provider import (
    PayloadNewsProvider,
    RssNewsProvider,
)
from tinvest_signal_engine.domain.signal_llm import (
    LlmAnalysisResult,
    LlmSignalContext,
    NewsArticle,
    format_llm_analysis_html,
    format_llm_analysis_plain,
)
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


class MockNewsProvider(NewsProviderPort):
    def __init__(self, news: tuple[NewsArticle, ...]) -> None:
        self._news = news

    def get_recent_news(
        self,
        ticker: str,
        *,
        max_age_hours: int = 4,
        limit: int = 5,
        payload: dict[str, Any] | None = None,
    ) -> tuple[NewsArticle, ...]:
        return self._news[:limit]


class MockLlmAnalyzer(LlmAnalyzerPort):
    def __init__(
        self,
        relevant_titles: set[str] | None = None,
        analysis_to_return: LlmAnalysisResult | None = None,
    ) -> None:
        self.relevant_titles = relevant_titles or set()
        self.analysis_to_return = analysis_to_return
        self.is_news_relevant_calls: list[str] = []
        self.analyze_signal_calls: list[LlmSignalContext] = []

    def is_news_relevant(
        self, ticker: str, signal_type: str, news_title: str
    ) -> bool:
        self.is_news_relevant_calls.append(news_title)
        return news_title in self.relevant_titles

    def analyze_signal(
        self, context: LlmSignalContext
    ) -> LlmAnalysisResult | None:
        self.analyze_signal_calls.append(context)
        return self.analysis_to_return


def test_domain_formatting() -> None:
    result = LlmAnalysisResult(
        has_catalyst=True,
        catalyst_summary="Чистая прибыль выросла на 15%",
        reaction_type="FUNDAMENTAL_PRICING",
        trade_bias="LONG",
        suggested_action="BUY_PULLBACK",
        risk_factor="Слабость индекса IMOEX",
        invalidation_level="Ниже 270.0",
        confidence_score=0.85,
        model_used="gemini-3.5-flash",
        tokens_used=350,
    )

    html = format_llm_analysis_html(result)
    assert "LONG" in html
    assert "BUY_PULLBACK" in html
    assert "Чистая прибыль выросла на 15%" in html
    assert "Ниже 270.0" in html

    plain = format_llm_analysis_plain(result)
    assert "LONG - BUY_PULLBACK" in plain
    assert "Чистая прибыль выросла на 15%" in plain


def test_use_case_skips_when_disabled() -> None:
    news_provider = MockNewsProvider((NewsArticle(title="Отчетность Сбера"),))
    analyzer = MockLlmAnalyzer(
        relevant_titles={"Отчетность Сбера"},
        analysis_to_return=LlmAnalysisResult(
            has_catalyst=True,
            catalyst_summary="Прибыль",
            reaction_type="FUNDAMENTAL",
            trade_bias="LONG",
            suggested_action="BUY",
            risk_factor="Риск",
        ),
    )
    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
        enabled=False,
    )

    payload = {"test": 123}
    enriched = service.enrich_payload(
        ticker="SBER",
        signal_type="confirmed_impulse_long",
        payload=payload,
        quality_score=0.9,
    )
    assert enriched == payload
    assert len(analyzer.is_news_relevant_calls) == 0


def test_use_case_skips_low_quality() -> None:
    news_provider = MockNewsProvider((NewsArticle(title="Отчетность Сбера"),))
    analyzer = MockLlmAnalyzer()
    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
        min_quality_score=0.70,
    )

    payload = {"price_change_pct": 0.5}
    enriched = service.enrich_payload(
        ticker="SBER",
        signal_type="confirmed_impulse_long",
        payload=payload,
        quality_score=0.50,  # Below threshold
    )
    assert enriched == payload
    assert len(analyzer.is_news_relevant_calls) == 0


def test_use_case_skips_irrelevant_signal_type() -> None:
    news_provider = MockNewsProvider((NewsArticle(title="Отчетность"),))
    analyzer = MockLlmAnalyzer()
    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
    )

    payload = {"spoofing": True}
    enriched = service.enrich_payload(
        ticker="SBER",
        signal_type="orderbook_spoofing_bid_pull",  # Microstructure noise, not news-driven
        payload=payload,
        quality_score=0.95,
    )
    assert enriched == payload
    assert len(analyzer.is_news_relevant_calls) == 0


def test_use_case_cooldown_per_ticker() -> None:
    fixed_time = datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    current_time = fixed_time

    def clock() -> datetime:
        return current_time

    news_provider = MockNewsProvider((NewsArticle(title="Дивиденды"),))
    sample_analysis = LlmAnalysisResult(
        has_catalyst=True,
        catalyst_summary="Дивиденды 33 руб",
        reaction_type="FUNDAMENTAL",
        trade_bias="LONG",
        suggested_action="BUY",
        risk_factor="Нет",
    )
    analyzer = MockLlmAnalyzer(
        relevant_titles={"Дивиденды"},
        analysis_to_return=sample_analysis,
    )

    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
        cooldown_seconds=1800,  # 30 minutes
        clock=clock,
    )

    # First call: triggers analysis
    payload1 = service.enrich_payload(
        ticker="SBER",
        signal_type="confirmed_impulse_long",
        payload={"sample": 1},
        quality_score=0.85,
    )
    assert "llm_analysis" in payload1
    assert len(analyzer.analyze_signal_calls) == 1

    # Second call 5 minutes later: skipped due to cooldown!
    current_time = datetime(2026, 11, 12, 10, 5, tzinfo=timezone.utc)
    payload2 = service.enrich_payload(
        ticker="SBER",
        signal_type="confirmed_impulse_long",
        payload={"sample": 2},
        quality_score=0.85,
    )
    assert "llm_analysis" not in payload2
    assert len(analyzer.analyze_signal_calls) == 1

    # Different ticker at 10:05: NOT skipped!
    payload3 = service.enrich_payload(
        ticker="GAZP",
        signal_type="confirmed_impulse_long",
        payload={"sample": 3},
        quality_score=0.85,
    )
    assert "llm_analysis" in payload3
    assert len(analyzer.analyze_signal_calls) == 2


def test_use_case_fast_binary_filter() -> None:
    news_items = (
        NewsArticle(title="Чистая прибыль выросла"),
        NewsArticle(title="Выставка картин в офисе"),
    )
    news_provider = MockNewsProvider(news_items)
    analyzer = MockLlmAnalyzer(
        relevant_titles={"Чистая прибыль выросла"},
        analysis_to_return=LlmAnalysisResult(
            has_catalyst=True,
            catalyst_summary="Прибыль",
            reaction_type="FUNDAMENTAL",
            trade_bias="LONG",
            suggested_action="BUY",
            risk_factor="Риск",
        ),
    )
    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
    )

    enriched = service.enrich_payload(
        ticker="SBER",
        signal_type="confirmed_impulse_long",
        payload={"obi": 0.8},
        quality_score=0.8,
    )

    assert "llm_analysis" in enriched
    assert len(analyzer.is_news_relevant_calls) == 2
    # Verify deep analyzer was called with ONLY the relevant article
    assert len(analyzer.analyze_signal_calls) == 1
    passed_articles = analyzer.analyze_signal_calls[0].news_items
    assert len(passed_articles) == 1
    assert passed_articles[0].title == "Чистая прибыль выросла"


def test_payload_news_provider() -> None:
    provider = PayloadNewsProvider()
    payload = {
        "news": [
            "Совет директоров одобрил дивиденды",
            {"title": "Выручка +20%", "summary": "Отчет по МСФО"},
        ]
    }
    extracted = provider.extract_from_payload("LKOH", payload)
    assert len(extracted) == 2
    assert extracted[0].title == "Совет директоров одобрил дивиденды"
    assert extracted[1].title == "Выручка +20%"
    assert extracted[1].summary == "Отчет по МСФО"


def test_rss_news_provider_keyword_matching() -> None:
    provider = RssNewsProvider()
    provider._cached_articles = [
        NewsArticle(title="Сбербанк отчитался о рекордной прибыли", source="RBC"),
        NewsArticle(title="Новак заявил о балансе на рынке нефти", source="RBC"),
        NewsArticle(title="Акции Газпрома упали на фоне снижения экспорта", source="RBC"),
    ]
    provider._last_fetch_ts = 9999999999.0  # Prevent network fetch in test

    sber_news = provider.get_recent_news("SBER")
    assert len(sber_news) == 1
    assert "Сбербанк" in sber_news[0].title

    gazp_news = provider.get_recent_news("GAZP")
    assert len(gazp_news) == 1
    assert "Газпрома" in gazp_news[0].title


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
        summary="SBER rolling volume impulse.",
        payload={"price_change_pct": 2.1, "quality_score": 0.85},
    )
    defaults.update(kwargs)
    return TriggerSignal(**defaults)


def test_signal_enrichment_integration_with_llm() -> None:
    news_provider = MockNewsProvider((NewsArticle(title="Рекордные дивиденды"),))
    analyzer = MockLlmAnalyzer(
        relevant_titles={"Рекордные дивиденды"},
        analysis_to_return=LlmAnalysisResult(
            has_catalyst=True,
            catalyst_summary="Рекордные дивиденды 33 руб.",
            reaction_type="FUNDAMENTAL_PRICING",
            trade_bias="LONG",
            suggested_action="BUY_MOMENTUM",
            risk_factor="Перекупленность",
            invalidation_level="Ниже 275.0",
        ),
    )
    llm_service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
        min_quality_score=0.5,
    )

    signal = _sample_signal(
        signal_type="confirmed_impulse_long",
        ticker="SBER",
        payload={
            "price_change_pct": 2.1,
            "quality_score": 0.85,
        },
    )

    enriched = enrich_signal_for_delivery(signal, llm_service=llm_service)

    assert "llm_analysis" in enriched.payload
    tg_html = enriched.payload["telegram_html"]
    assert "Анализ катализатора:" in tg_html
    assert "LONG" in tg_html
    assert "BUY_MOMENTUM" in tg_html
    assert "Рекордные дивиденды 33 руб." in tg_html


def test_asset_position_analysis_formatting() -> None:
    from tinvest_signal_engine.domain.signal_llm import (
        AssetPositionAnalysisResult,
        format_position_analysis_html,
        format_position_analysis_plain,
    )

    pos_result = AssetPositionAnalysisResult(
        commodity="gold",
        sentiment="OVERHEATED_BULLISH",
        bias_title="Фонды наращивают ставку на рост золота",
        smart_money_summary="Фонды увеличили чистый лонг до 75%",
        crowd_bias="Розница в умеренном лонге",
        interpretation="Сильный тренд, но растет риск локального отката",
        macro_drivers="Закупки центробанков поддерживают восходящий тренд",
        actionable_advice="Искать покупки на коррекциях",
        risk_factors="Уход ниже $2650",
    )

    html = format_position_analysis_html(pos_result, commodity_title="Золото")
    assert "Анализ позиционирования · Золото" in html
    assert "ПЕРЕГРЕТЫЙ ЛОНГ" in html
    assert "Фонды увеличили чистый лонг до 75%" in html
    assert "Искать покупки на коррекциях" in html

    plain = format_position_analysis_plain(pos_result, commodity_title="Золото")
    assert "[Анализ позиций: Золото - OVERHEATED_BULLISH]" in plain
    assert "Фонды наращивают ставку на рост золота" in plain


def test_use_case_analyze_asset_positions() -> None:
    from tinvest_signal_engine.domain.signal_llm import AssetPositionAnalysisResult

    pos_sample = AssetPositionAnalysisResult(
        commodity="brent",
        sentiment="BEARISH",
        bias_title="Производители агрессивно хеджируют падение нефти",
        smart_money_summary="Коммерческие участники нарастили шорт",
        crowd_bias="Спекулянты сокращают лонги",
        interpretation="Слабый спрос давит на котировки",
        macro_drivers="Рост добычи вне ОПЕК+",
        actionable_advice="Продавать на отскоках",
        risk_factors="Геополитическое обострение на Ближнем Востоке",
    )

    class MockPositionLlmAnalyzer(MockLlmAnalyzer):
        def analyze_position_report(self, context):
            self.position_context = context
            return pos_sample

    analyzer = MockPositionLlmAnalyzer()
    service = SignalLlmEnrichmentUseCase(
        news_provider=MockNewsProvider(()),
        llm_analyzer=analyzer,
    )

    payload = {
        "commodity": "brent",
        "source_exchange": "ICE",
        "trading_day": "2026-11-10",
        "previous_day": "2026-11-03",
        "sections": [
            {
                "source": "ICE",
                "period": "weekly",
                "groups": [
                    {
                        "category": "managed_money",
                        "longs": 80000,
                        "shorts": 120000,
                        "previous_longs": 90000,
                        "previous_shorts": 110000,
                    }
                ],
            }
        ],
    }

    result = service.analyze_asset_positions(payload)
    assert result is not None
    assert result.sentiment == "BEARISH"
    assert result.commodity == "brent"
    assert "Производители агрессивно" in result.bias_title
    assert "Managed Money" in analyzer.position_context.sections_summary


def test_domain_formatting_with_market_context() -> None:
    result = LlmAnalysisResult(
        has_catalyst=False,
        catalyst_summary="Локальный спекулятивный импульс спроса в валютной секции",
        reaction_type="MOMENTUM_FLOW",
        trade_bias="LONG",
        suggested_action="BUY_MOMENTUM",
        risk_factor="Ложный пробой с резким возвратом к VWAP при затухании объема",
        invalidation_level="Пробой ниже 91.10",
        market_context_summary="Фьючерс EUR/RUB (EuZ6) зафиксировал оборот 97.34 млн ₽ при росте объема в 12.6 раз к норме",
        confidence_score=0.82,
    )
    html = format_llm_analysis_html(result)
    assert "<b>LONG</b>" in html
    assert "BUY_MOMENTUM" in html
    assert "• <b>Конъюнктура:</b>" in html
    assert "Фьючерс EUR/RUB" in html
    assert "• <b>Катализатор:</b> Локальный спекулятивный импульс" in html
    assert "• <b>Риск:</b> Ложный пробой" in html
    assert "• <b>Отмена:</b> Пробой ниже 91.10" in html


def test_resolve_instrument_info() -> None:
    from tinvest_signal_engine.domain.signal_llm import resolve_instrument_info

    name, asset_type = resolve_instrument_info("EuZ6", "SPBFUT")
    assert "EUR/RUB" in name
    assert "валютный фьючерс" in asset_type

    name_sber, asset_sber = resolve_instrument_info("SBER", "TQBR")
    assert "Сбербанк" in name_sber
    assert "акции" in asset_sber


def test_google_news_provider_and_keywords() -> None:
    from tinvest_signal_engine.adapters.news_provider import (
        GoogleNewsProvider,
        get_search_query_for_ticker,
        resolve_ticker_keywords,
    )
    from tinvest_signal_engine.domain.trader_copilot import InternetNewsItem

    assert "курс евро рубль" in get_search_query_for_ticker("EuZ6")
    assert "ЕВРО" in resolve_ticker_keywords("EuZ6")

    class DummySearchAdapter:
        def search_news(self, query: str, limit: int = 5):
            return (
                InternetNewsItem(
                    title="ЦБ установил курс евро",
                    summary="Евро вырос",
                    url="https://cbr.ru",
                ),
            )

    provider = GoogleNewsProvider(search_adapter=DummySearchAdapter())
    news = provider.get_recent_news("EuZ6")
    assert len(news) == 1
    assert "ЦБ установил курс евро" in news[0].title


def test_volume_spike_without_news_still_analyzed_with_market_context() -> None:
    # Simulates the user's EuZ6 case: volume spike, no news in feed, but rich microstructure
    analyzer = MockLlmAnalyzer(
        relevant_titles=set(),
        analysis_to_return=LlmAnalysisResult(
            has_catalyst=False,
            catalyst_summary="Спекулятивный приток в стакан EUR/RUB без внешнего инфоповода",
            reaction_type="MOMENTUM_FLOW",
            trade_bias="LONG",
            suggested_action="BUY_MOMENTUM",
            risk_factor="Быстрое затухание ликвидности",
            invalidation_level="Ниже 91.20",
            market_context_summary="Фьючерс EUR/RUB: резкий всплеск объема 1065 лотов (~97.34 млн ₽, x12.6 к базе)",
            confidence_score=0.85,
        ),
    )
    # News provider returns zero news
    news_provider = MockNewsProvider(())
    service = SignalLlmEnrichmentUseCase(
        news_provider=news_provider,
        llm_analyzer=analyzer,
        min_quality_score=0.65,
    )

    payload = {
        "class_code": "SPBFUT",
        "window_lots": 1065,
        "window_notional": 97340000.0,
        "ratio_to_baseline": 12.59,
        "last_price": 91.45,
        "spread_bps": 1.8,
        "obi": 0.45,
        "direction": "long",
        "quality_score": 0.70,
    }

    enriched = service.enrich_payload(
        ticker="EuZ6",
        signal_type="volume_spike",
        payload=payload,
        quality_score=0.70,
        headline_ru="Объём за 180 с: 1065 лотов, оборот ≈ 97,34 млн ₽ (x12,59 к базе).",
    )

    assert "llm_analysis" in enriched
    assert len(analyzer.analyze_signal_calls) == 1
    ctx = analyzer.analyze_signal_calls[0]
    assert ctx.ticker == "EuZ6"
    assert "EUR/RUB" in ctx.instrument_name
    assert ctx.lots == 1065
    assert ctx.notional_rub == 97340000.0
    assert ctx.volume_ratio == 12.59
    assert ctx.spread_bps == 1.8

    html = enriched["llm_analysis_html"]
    assert "Конъюнктура:" in html
    assert "97.34 млн ₽" in html
    assert "Спекулятивный приток" in html



@pytest.mark.parametrize("signal_type", [
    "obi_dynamics", "aggressive_trade_burst", "futures_positioning",
    "liquidity_vacuum", "microstructure_combo_long", "large_trade_print",
])
def test_realtime_signal_types_receive_analysis(signal_type):
    result = LlmAnalysisResult(
        False, "Активный поток", "MOMENTUM_FLOW", "LONG", "WAIT_PULLBACK", "Риск отката",
    )
    analyzer = MockLlmAnalyzer(analysis_to_return=result)
    service = SignalLlmEnrichmentUseCase(
        news_provider=MockNewsProvider(()), llm_analyzer=analyzer,
    )
    enriched = service.enrich_payload(
        ticker="SBER", signal_type=signal_type,
        payload={"window_notional": 45_000_000}, quality_score=0.95,
    )
    assert enriched["llm_analysis_status"] == "completed"
    assert len(analyzer.analyze_signal_calls) == 1
