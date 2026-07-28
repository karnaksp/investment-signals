"""Сигналы: русские подписи, ссылки терминала, обогащение."""

from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery
from tinvest_signal_engine.signal_interpretation import build_signal_interpretation
from tinvest_signal_engine.signal_locale import (
    build_plain_explanation_ru,
    build_telegram_html,
    signal_type_ru,
)
from tinvest_signal_engine.terminal_links import (
    t_invest_instrument_url,
    t_invest_terminal_open_chart_url,
    t_invest_terminal_search_url,
)


def _signal(**kwargs) -> TriggerSignal:
    defaults = dict(
        signal_id="00000000-0000-4000-8000-000000000001",
        detected_at=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        instrument_id="inst-1",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="volume_spike",
        severity=2,
        metric_value=1200.0,
        baseline_value=400.0,
        z_score=4.2,
        window_seconds=60,
        summary="SBER rolling volume hit 1200 vs baseline 400 (z=4.20).",
        payload={},
    )
    defaults.update(kwargs)
    return TriggerSignal(**defaults)


def test_signal_type_ru_known_detector_types() -> None:
    assert "Расширение" in signal_type_ru("spread_widening")
    assert "Дисбаланс" in signal_type_ru("orderbook_imbalance")
    assert "OBI" in signal_type_ru("obi_dynamics")
    assert "частоты" in signal_type_ru("trade_rate_spike").lower()


def test_terminal_search_url_deeplink_chart_widget() -> None:
    u = t_invest_terminal_search_url(ticker="GAZP", class_code="TQBR")
    assert "GAZP" in u.upper()
    assert "terminal" in u
    assert "widget_name=CHART_TV" in u
    assert "symbolId%2CGAZP" in u or "symbolId,GAZP" in u


def test_terminal_open_chart_spbfut_preserves_ticker_case() -> None:
    u = t_invest_terminal_open_chart_url(ticker="SiM6", class_code="SPBFUT")
    assert "symbolId%2CSiM6" in u or "symbolId,SiM6" in u


def test_terminal_open_chart_prefers_instrument_uid() -> None:
    u = t_invest_terminal_open_chart_url(
        ticker="SBER", instrument_uid="abc-uid-1", class_code="TQBR"
    )
    assert "instrumentUid" in u
    assert "abc-uid-1" in u
    assert "CHART_TV" in u


def test_enrich_preserves_summary_en_on_repeat() -> None:
    s0 = _signal()
    s1 = enrich_signal_for_delivery(s0)
    assert "Всплеск объёма" in s1.summary
    assert s1.payload.get("summary_en") == s0.summary
    assert "tbank.ru/terminal" in s1.payload.get("telegram_html", "")
    assert "<a href=" in s1.payload.get("telegram_html", "")
    s2 = enrich_signal_for_delivery(s1)
    assert s2.payload.get("summary_en") == s0.summary


def test_price_jump_interpretation_has_signed_percent() -> None:
    s = _signal(
        signal_type="price_jump",
        metric_value=125.0,
        baseline_value=20.0,
        payload={
            "start_price": 100.0,
            "current_price": 101.25,
            "price_change": 1.25,
            "price_change_pct": 1.25,
            "price_direction": "up",
        },
    )
    interp = build_signal_interpretation(s)
    assert interp["direction"] == "up"
    assert "+1,25%" in interp["headline_ru"]
    assert any(f["key"] == "price_change_pct" for f in interp["facts"])


def test_price_jump_without_signed_payload_does_not_guess_direction() -> None:
    s = _signal(
        signal_type="price_jump",
        metric_value=188.0,
        baseline_value=17.82,
        z_score=10.53,
        window_seconds=180,
        payload={},
    )
    interp = build_signal_interpretation(s)
    assert interp["direction"] == "unknown"
    assert "Цена изменилась примерно на 1,88%" in interp["headline_ru"]
    assert "выросла" not in interp["headline_ru"]


def test_telegram_price_jump_uses_interpretation_not_legacy_text() -> None:
    s = _signal(
        signal_type="price_jump",
        metric_value=188.0,
        baseline_value=17.82,
        z_score=10.53,
        window_seconds=180,
        payload={
            "start_price": 100.0,
            "current_price": 101.88,
            "price_change": 1.88,
            "price_change_pct": 1.88,
            "price_direction": "up",
        },
    )
    html = build_telegram_html(
        s,
        {"quality_score": 97, "quality_tier_ru": "высокая", "quality_hint_ru": "x"},
        ticker_terminal_url="https://example.test/chart",
        instrument_page_url="https://example.test/instrument",
    )
    assert "Цена выросла на +1,88%" in html
    assert "Диапазон движения цены" not in html


def test_volume_spike_interpretation_has_notional() -> None:
    s = _signal(
        signal_type="volume_spike",
        metric_value=1200.0,
        baseline_value=300.0,
        payload={
            "window_lots": 1200.0,
            "window_units": 12000.0,
            "window_notional": 3_420_000.0,
            "last_price": 285.0,
            "lot": 10,
        },
    )
    interp = build_signal_interpretation(s)
    assert "Оборот" in {f["label"] for f in interp["facts"]}
    assert "₽" in interp["headline_ru"]


def test_enrichment_adds_structured_interpretation() -> None:
    s = _signal(
        signal_type="price_jump",
        payload={
            "start_price": 100.0,
            "current_price": 98.0,
            "price_change": -2.0,
            "price_change_pct": -2.0,
            "price_direction": "down",
        },
    )
    out = enrich_signal_for_delivery(s)
    interp = out.payload.get("interpretation")
    assert isinstance(interp, dict)
    assert interp.get("direction") == "down"
    assert out.payload.get("interpretation_ru") == interp.get("headline_ru")


def test_spoofing_explanation_ask_not_bid() -> None:
    s = _signal(
        signal_type="orderbook_spoofing_ask_pull",
        summary="x",
        z_score=0.0,
    )
    text = build_plain_explanation_ru(s)
    assert "аска" in text
    assert "бида" not in text


def test_morning_retracement_telegram_shows_direction_probability_and_target() -> None:
    signal = _signal(
        signal_type="morning_retracement_recommendation",
        metric_value=0.73,
        baseline_value=0.65,
        payload={
            "expected_direction": "down",
            "entry_reference_price": 102.0,
            "target_price": 101.0,
            "initial_stop_price": 102.8,
            "break_even_trigger_price": 101.35,
            "expected_hit_at": "2026-07-28T08:20:00+03:00",
            "expected_hit_window_start": "2026-07-28T08:00:00+03:00",
            "expected_hit_window_end": "2026-07-28T09:10:00+03:00",
            "deadline_at": "2026-07-28T11:00:00+03:00",
            "model_probability": 0.73,
            "historical_target_probability": 0.72,
            "historical_target_probability_lower": 0.58,
            "evidence_sample_count": 72,
        },
    )

    html = build_telegram_html(
        signal,
        {
            "quality_score": 73,
            "quality_tier_ru": "исследовательская",
            "quality_hint_ru": "Вероятность рассчитана моделью события.",
        },
        ticker_terminal_url="https://example.test/chart",
        instrument_page_url="https://example.test/instrument",
    )

    assert "ВОЗВРАТ ВНИЗ" in html
    assert "73,0%" in html
    assert "R50" in html
    assert "101" in html
    assert "не гарантирует" in html


def test_instrument_url_tqbr() -> None:
    u = t_invest_instrument_url(ticker="SBER", class_code="TQBR")
    assert "SBER" in u
    assert "stocks" in u
