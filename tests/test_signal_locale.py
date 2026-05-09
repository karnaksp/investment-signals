"""Сигналы: русские подписи, ссылки терминала, обогащение."""

from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery
from tinvest_signal_engine.signal_locale import (
    build_plain_explanation_ru,
    signal_type_ru,
)
from tinvest_signal_engine.terminal_links import (
    t_invest_instrument_url,
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


def test_terminal_search_url_has_ticker() -> None:
    u = t_invest_terminal_search_url(ticker="GAZP")
    assert "GAZP" in u.upper()
    assert "terminal" in u


def test_enrich_preserves_summary_en_on_repeat() -> None:
    s0 = _signal()
    s1 = enrich_signal_for_delivery(s0)
    assert "Всплеск объёма" in s1.summary
    assert s1.payload.get("summary_en") == s0.summary
    assert "tbank.ru/terminal" in s1.payload.get("telegram_html", "")
    assert "<a href=" in s1.payload.get("telegram_html", "")
    s2 = enrich_signal_for_delivery(s1)
    assert s2.payload.get("summary_en") == s0.summary


def test_spoofing_explanation_ask_not_bid() -> None:
    s = _signal(
        signal_type="orderbook_spoofing_ask_pull",
        summary="x",
        z_score=0.0,
    )
    text = build_plain_explanation_ru(s)
    assert "аска" in text
    assert "бида" not in text


def test_instrument_url_tqbr() -> None:
    u = t_invest_instrument_url(ticker="SBER", class_code="TQBR")
    assert "SBER" in u
    assert "stocks" in u
