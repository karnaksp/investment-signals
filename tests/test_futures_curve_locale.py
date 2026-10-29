from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_interpretation import build_signal_interpretation
from tinvest_signal_engine.signal_locale import build_plain_explanation_ru, build_telegram_html, signal_type_ru


TYPES = {
    "calendar_spread_dislocation": "Аномалия календарного спреда",
    "roll_migration_anomaly": "Необычный перенос позиций",
    "futures_curve_kink": "Излом фьючерсной кривой",
    "term_premium_shock": "Скачок премии за срок",
    "futures_basis_momentum": "Расхождение динамики сроков",
}


def _signal(signal_type: str, **payload: object) -> TriggerSignal:
    return TriggerSignal(
        signal_id="curve-signal",
        detected_at=datetime(2026, 10, 29, tzinfo=timezone.utc),
        instrument_id="SiZ6_SPBFUT",
        ticker="SiZ6",
        class_code="SPBFUT",
        alias="usd_rub",
        source_event_type="orderbook",
        signal_type=signal_type,
        severity=3,
        metric_value=35,
        baseline_value=10,
        z_score=5.2,
        window_seconds=300,
        summary="curve observation",
        payload={
            "curve_version": "futures_curve_v1",
            "chain_id": "usd_rub",
            "direction": "widening",
            "legs": [
                {"ticker": "SiZ6", "expiration_at": "2026-12-17T00:00:00Z", "mid": 81000, "notional_rub": 15_000_000},
                {"ticker": "SiH7", "expiration_at": "2027-03-18T00:00:00+00:00", "mid": 83000, "notional_rub": 20_000_000},
            ],
            "deviation_bps": 25,
            "window_notional": 35_000_000,
            "score": 5.2,
            "baseline_points": 120,
            "persistence_seconds": 30,
            "quote_age_seconds": 1.2,
            "quote_skew_seconds": 0.3,
            **payload,
        },
    )


@pytest.mark.parametrize("signal_type", TYPES)
def test_curve_interpretation_names_both_expiries_and_observed_strength(signal_type: str) -> None:
    signal = _signal(signal_type)
    interpretation = build_signal_interpretation(signal)
    headline = interpretation["headline_ru"]
    assert signal_type_ru(signal_type) == TYPES[signal_type]
    assert "SiZ6 (17.12.2026)" in headline
    assert "SiH7 (18.03.2027)" in headline
    assert "|z|=5,2" in headline
    assert "35 млн ₽" in headline
    assert "без прогноза направления цены" in headline
    assert "вероятность" not in headline.lower()
    assert build_plain_explanation_ru(signal) == headline


def test_curve_kink_names_three_maturities_and_middle_rich_direction() -> None:
    signal = _signal("futures_curve_kink", direction="middle_rich")
    signal.payload["legs"].append({"ticker": "SiM7", "expiration_at": "2027-06-17T00:00:00Z"})
    headline = build_signal_interpretation(signal)["headline_ru"]
    assert "SiM7 (17.06.2027)" in headline
    assert "средний срок дороже соседних" in headline
    assert "+25 б.п." in headline


def test_roll_share_changes_are_percentage_points_without_position_direction_claim() -> None:
    signal = _signal("roll_migration_anomaly", direction="migration_far", oi_share_change=0.075, volume_share_change=0.21)
    headline = build_signal_interpretation(signal)["headline_ru"]
    assert "в OI +7,5 п.п." in headline
    assert "в объёме +21 п.п." in headline
    assert "лонг" not in headline.lower()
    assert "шорт" not in headline.lower()


def test_carry_annual_premium_is_percentage_points_not_price_percent() -> None:
    headline = build_signal_interpretation(_signal("term_premium_shock", annual_premium_change=-0.035))["headline_ru"]
    assert "-3,5 п.п. годовых" in headline


def test_basis_momentum_shows_observation_horizon_and_experimental_adaptation() -> None:
    interpretation = build_signal_interpretation(_signal("futures_basis_momentum", horizon_seconds=3600, momentum_difference_bps=-32))
    assert "за 60 мин -32 б.п." in interpretation["headline_ru"]
    assert any("адаптация" in fact["value"] for fact in interpretation["facts"])


@pytest.mark.parametrize("signal_type", TYPES)
def test_curve_missing_and_nonfinite_metadata_is_rendered_without_crash(signal_type: str) -> None:
    signal = _signal(signal_type, legs=[None, {"ticker": "SiZ6", "expiration_at": "bad-date", "mid": float("nan")}], score=float("inf"), deviation_bps=float("nan"), window_notional=None, oi_share_change=float("nan"), annual_premium_change=float("inf"), horizon_seconds=None, momentum_difference_bps=float("nan"))
    interpretation = build_signal_interpretation(signal)
    assert "SiZ6 (исполнение —)" in interpretation["headline_ru"]
    rendered = json.dumps(interpretation, ensure_ascii=False).lower()
    assert "nan" not in rendered
    assert "infinity" not in rendered


def test_curve_telegram_escapes_leg_ticker_and_displays_signal_type() -> None:
    signal = _signal("calendar_spread_dislocation", legs=[{"ticker": "<SiZ6>", "expiration_at": "2026-12-17"}])
    rendered = build_telegram_html(signal, {}, ticker_terminal_url="https://example.com/terminal", instrument_page_url="https://example.com/instrument")
    assert "Тип: Аномалия календарного спреда" in rendered
    assert "&lt;SiZ6&gt; (17.12.2026)" in rendered
    assert "<SiZ6>" not in rendered
