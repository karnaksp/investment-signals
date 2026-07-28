from __future__ import annotations

from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.adapters.morning_retracement_runtime import (
    load_morning_retracement_policy,
)
from tinvest_signal_engine.application.morning_retracement_signals import (
    GenerateMorningRetracementRecommendations,
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    LinearProbabilityModel,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
)


MOSCOW = ZoneInfo("Europe/Moscow")
ROOT = Path(__file__).resolve().parents[1]


def _model(*, intercept: float = 10.0) -> LinearProbabilityModel:
    payload = {
        "schema": "linear-probability-model-v1",
        "link": "logit",
        "positive_class": 1,
        "feature_names": ["ticker=SBER"],
        "coefficients": [0.0],
        "intercept": intercept,
    }
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return LinearProbabilityModel(
        feature_names=("ticker=SBER",),
        coefficients=(0.0,),
        intercept=intercept,
        fingerprint="sha256:" + sha256(encoded).hexdigest(),
    )


def _policy() -> MorningRetracementRuntimePolicy:
    return MorningRetracementRuntimePolicy(
        policy_version="test-policy",
        hypothesis_id="h1-selective-morning-retracement",
        hypothesis_version="2.2.0",
        model=_model(),
        target_fraction=0.5,
        default_probability_threshold=0.5,
        stop_extension_fraction=0.4,
        break_even_trigger_fraction=0.33,
        deadline_local_minute=11 * 60,
        round_trip_cost_bps=10.0,
        require_volume_baseline=True,
        default_maximum_relative_volume=1.0,
        default_minimum_active_minute_ratio=0.0,
        historical_target_probability=0.72,
        historical_target_probability_lower=0.58,
        historical_non_loss_probability=0.72,
        historical_non_loss_probability_lower=0.60,
        historical_sample_count=72,
        historical_trading_days=22,
        expected_hit_minutes_p25=11,
        expected_hit_minutes_median=31,
        expected_hit_minutes_p75=76,
    )


def _settings(**overrides: object) -> MorningRetracementRuntimeSettings:
    values = {
        "enabled": True,
        "revision": 1,
        "probability_threshold": 0.5,
        "maximum_relative_volume": 1.0,
        "minimum_active_minute_ratio": 0.0,
        "minimum_excursion_bps": 40.0,
        "minimum_remaining_move_bps": 20.0,
        "first_decision_local_minute": 7 * 60 + 15,
        "last_decision_local_minute": 10 * 60,
        "maximum_signals_per_day": 10,
        "enabled_tickers": frozenset(),
        "telegram_enabled": False,
    }
    values.update(overrides)
    return MorningRetracementRuntimeSettings(**values)  # type: ignore[arg-type]


def _candle(at: datetime, price: float, volume: float = 100.0) -> HistoricalCandle:
    return HistoricalCandle(
        ticker="SBER",
        at=at,
        open=price,
        high=price + 0.05,
        low=price - 0.05,
        close=price,
        volume=volume,
        complete=True,
    )


def _series(*, cumulative_volume: float = 10_000.0) -> MorningRetracementMarketSeries:
    previous_start = datetime(2026, 7, 27, 9, 59, tzinfo=MOSCOW)
    previous = (
        _candle(previous_start, 99.8),
        _candle(previous_start + timedelta(hours=8), 100.0),
    )
    morning_start = datetime(2026, 7, 28, 7, 0, tzinfo=MOSCOW)
    morning = (
        _candle(morning_start, 100.4),
        _candle(morning_start + timedelta(minutes=5), 101.0),
        _candle(morning_start + timedelta(minutes=10), 102.0),
        _candle(morning_start + timedelta(minutes=15), 101.8),
        # A worker poll need not land on the exact five-minute boundary.
        _candle(morning_start + timedelta(minutes=16), 101.7),
    )
    return MorningRetracementMarketSeries(
        instrument_id="instrument-sber",
        ticker="SBER",
        class_code="TQBR",
        alias="SBER_TQBR",
        trading_day=date(2026, 7, 28),
        previous_session=previous,
        current_session=morning,
        historical_cumulative_volume=cumulative_volume,
    )


def test_runtime_artifact_is_valid_and_contains_the_sealed_model() -> None:
    policy = load_morning_retracement_policy(
        ROOT
        / "config"
        / "scientific_hypotheses"
        / "morning-retracement-runtime-v2.2.json"
    )

    assert policy.hypothesis_version == "2.2.0"
    assert policy.target_fraction == 0.5
    assert policy.model.feature_names
    assert 0.0 <= policy.model.probability({}) <= 1.0


def test_live_use_case_scores_latest_completed_five_minute_snapshot() -> None:
    result = GenerateMorningRetracementRecommendations(_policy()).execute(
        (_series(),),
        settings=_settings(),
    )

    assert len(result) == 1
    recommendation = result[0][1]
    assert recommendation.observed_at.astimezone(MOSCOW).strftime("%H:%M") == "07:15"
    assert recommendation.expected_direction == "down"
    assert recommendation.model_probability > 0.99
    assert recommendation.target_price == pytest.approx(101.025)
    assert recommendation.initial_stop_price == pytest.approx(102.87)


def test_owner_filters_disable_or_reject_overactive_events() -> None:
    use_case = GenerateMorningRetracementRecommendations(_policy())

    assert use_case.execute((_series(),), settings=_settings(enabled=False)) == ()
    assert (
        use_case.execute(
            (_series(cumulative_volume=100.0),),
            settings=_settings(maximum_relative_volume=0.5),
        )
        == ()
    )
    assert (
        use_case.execute(
            (_series(),),
            settings=_settings(enabled_tickers=frozenset({"GAZP"})),
        )
        == ()
    )


def test_stale_morning_snapshot_is_not_emitted_later_in_the_day() -> None:
    as_of = datetime(2026, 7, 28, 12, 0, tzinfo=MOSCOW)

    result = GenerateMorningRetracementRecommendations(_policy()).execute(
        (_series(),),
        settings=_settings(),
        as_of=as_of,
    )

    assert result == ()
