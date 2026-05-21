"""Сигналы: русские подписи, ссылки терминала, обогащение."""

from __future__ import annotations

from datetime import datetime, timezone
import textwrap

from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery
from tinvest_signal_engine.signal_locale import (
    build_plain_explanation_ru,
    build_delivery_details_ru,
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
    assert "Всплеск объёма —" not in s1.payload.get("telegram_html", "")
    assert "Суммарный объём сделок" not in s1.payload.get("telegram_html", "")
    assert "Серьёзность:" in s1.payload.get("telegram_html", "")
    s2 = enrich_signal_for_delivery(s1)
    assert s2.payload.get("summary_en") == s0.summary


def test_enrich_adds_display_name_from_instruments_yaml(
    tmp_path, monkeypatch
) -> None:
    instruments_yaml = tmp_path / "instruments.yaml"
    instruments_yaml.write_text(
        textwrap.dedent(
            """
            instruments:
              - ticker: SBER
                class_code: TQBR
                alias: sber
                display_name: Сбербанк
                subscriptions:
                  trades: true
                  last_price: true
                  info: false
            """
        ).strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("INSTRUMENTS_CONFIG", str(instruments_yaml))

    enriched = enrich_signal_for_delivery(_signal(instrument_id="SBER_TQBR"))

    assert enriched.payload.get("instrument_display_name") == "Сбербанк"
    assert "Сбербанк" in enriched.payload.get("telegram_html", "")
    assert "Сбербанк" in enriched.summary


def test_spoofing_explanation_ask_not_bid() -> None:
    s = _signal(
        signal_type="orderbook_spoofing_ask_pull",
        summary="x",
        z_score=0.0,
    )
    text = build_plain_explanation_ru(s)
    assert "аска" in text
    assert "бида" not in text


def test_delivery_details_keep_metrics_without_full_explanation() -> None:
    s = _signal()
    details = build_delivery_details_ru(
        s,
        {
            "quality_score": 64,
            "quality_tier_ru": "средняя",
            "quality_hint_ru": "Умеренная аномалия.",
        },
    )
    assert "Серьёзность:" in details
    assert "Оценка полезности:" in details
    assert "Суммарный объём сделок" not in details
    assert "Всплеск объёма —" not in details


def test_instrument_url_tqbr() -> None:
    u = t_invest_instrument_url(ticker="SBER", class_code="TQBR")
    assert "SBER" in u
    assert "stocks" in u
