from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from typing import Mapping
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.adapters.morning_retracement_runtime import (
    ClickHouseMorningRetracementSource,
    _LATEST_SESSION_CANDLES_SQL,
    _VOLUME_HISTORY_SQL,
    load_morning_retracement_policy,
)
from tinvest_signal_engine.application.morning_retracement_signals import (
    GenerateMorningRetracementRecommendations,
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.config import InstrumentSubscriptionConfig
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
        "monitor_until_local_minute": 11 * 60,
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


def test_formal_signal_keeps_registered_five_minute_snapshot() -> None:
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


def test_live_assessment_scores_latest_completed_minute_snapshot() -> None:
    result = GenerateMorningRetracementRecommendations(_policy()).assess(
        (_series(),),
        settings=_settings(),
    )

    assert len(result) == 1
    assessment = result[0][1]
    assert (
        assessment.recommendation.observed_at.astimezone(MOSCOW).strftime("%H:%M")
        == "07:16"
    )
    assert assessment.eligible_for_signal is True


def test_live_monitoring_continues_after_signal_window_without_emitting() -> None:
    series = _series()
    observed_at = datetime(2026, 7, 28, 10, 30, tzinfo=MOSCOW)
    series = replace(
        series,
        current_session=series.current_session + (_candle(observed_at, 101.5),),
    )
    use_case = GenerateMorningRetracementRecommendations(_policy())

    assessments = use_case.assess(
        (series,),
        settings=_settings(),
        as_of=observed_at,
    )

    assert len(assessments) == 1
    assessment = assessments[0][1]
    assert assessment.training_window_ended is True
    assert assessment.eligible_for_signal is False
    assert "outside_signal_window" in assessment.reason_codes
    assert (
        use_case.execute(
            (series,),
            settings=_settings(),
            as_of=observed_at,
        )
        == ()
    )


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


def test_live_assessment_is_kept_when_notification_threshold_is_not_met() -> None:
    use_case = GenerateMorningRetracementRecommendations(_policy())

    assessments = use_case.assess(
        (_series(cumulative_volume=100.0),),
        settings=_settings(maximum_relative_volume=0.5),
    )

    assert len(assessments) == 1
    assessment = assessments[0][1]
    assert assessment.eligible_for_signal is False
    assert "relative_volume_above_maximum" in assessment.reason_codes
    assert assessment.recommendation.model_probability > 0.99


def test_stale_morning_snapshot_is_not_emitted_later_in_the_day() -> None:
    as_of = datetime(2026, 7, 28, 12, 0, tzinfo=MOSCOW)

    result = GenerateMorningRetracementRecommendations(_policy()).execute(
        (_series(),),
        settings=_settings(),
        as_of=as_of,
    )

    assert result == ()


class _RecordingMorningSource(ClickHouseMorningRetracementSource):
    def __init__(self) -> None:
        super().__init__(
            base_url="http://clickhouse.invalid",
            database="signal_engine",
            username="reader",
            password="secret",
        )
        self.calls: list[tuple[str, dict[str, str]]] = []

    def _rows(
        self,
        sql: str,
        parameters: Mapping[str, str],
    ) -> tuple[dict[str, object], ...]:
        self.calls.append((sql, dict(parameters)))
        return ()


class _PopulatedRecordingMorningSource(_RecordingMorningSource):
    def _rows(
        self,
        sql: str,
        parameters: Mapping[str, str],
    ) -> tuple[dict[str, object], ...]:
        self.calls.append((sql, dict(parameters)))
        if sql == _VOLUME_HISTORY_SQL:
            return (
                {
                    "ticker": "SBER",
                    "trading_day": "2026-07-27",
                    "cumulative_volume": 10_000,
                },
            )
        common = {
            "instrument_id": "SBER_TQBR",
            "ticker": "SBER",
            "open_price": 100,
            "high_price": 101,
            "low_price": 99,
            "close_price": 100,
            "volume": 100,
            "is_complete": 1,
        }
        return (
            {
                **common,
                "trading_day": "2026-07-27",
                "candle_at": "2026-07-27 08:00:00.000000",
            },
            {
                **common,
                "trading_day": "2026-07-28",
                "candle_at": "2026-07-28 08:00:00.000000",
            },
        )


def test_market_source_bounds_queries_and_reuses_five_minute_volume_window() -> None:
    source = _RecordingMorningSource()
    instrument = InstrumentSubscriptionConfig(
        ticker="SBER",
        class_code="TQBR",
        alias="sber_tqbr",
    )
    first = datetime(2026, 7, 28, 7, 16, tzinfo=MOSCOW)
    second = datetime(2026, 7, 28, 7, 19, tzinfo=MOSCOW)

    assert source.load(as_of=first, instruments=(instrument,)) == ()
    assert source.load(as_of=second, instruments=(instrument,)) == ()

    candle_calls = [call for call in source.calls if call[0] == _LATEST_SESSION_CANDLES_SQL]
    volume_calls = [call for call in source.calls if call[0] == _VOLUME_HISTORY_SQL]
    assert len(candle_calls) == 2
    assert len(volume_calls) == 1
    assert candle_calls[0][1]["instrument_ids"] == "['SBER_TQBR']"
    assert "max_execution_time = 10" in _LATEST_SESSION_CANDLES_SQL
    assert "LIMIT 2" in _LATEST_SESSION_CANDLES_SQL
    assert "instrument_id IN {instrument_ids:Array(String)}" in _VOLUME_HISTORY_SQL


def test_market_source_does_not_query_before_morning_session() -> None:
    source = _RecordingMorningSource()
    instrument = InstrumentSubscriptionConfig(
        ticker="SBER",
        class_code="TQBR",
        alias="sber_tqbr",
    )

    result = source.load(
        as_of=datetime(2026, 7, 28, 6, 59, tzinfo=MOSCOW),
        instruments=(instrument,),
    )

    assert result == ()
    assert source.calls == []


def test_market_source_reuses_frozen_snapshot_after_monitoring_window() -> None:
    source = _PopulatedRecordingMorningSource()
    instrument = InstrumentSubscriptionConfig(
        ticker="SBER",
        class_code="TQBR",
        alias="sber_tqbr",
    )
    first = datetime(2026, 7, 28, 12, 16, tzinfo=MOSCOW)
    second = datetime(2026, 7, 28, 18, 40, tzinfo=MOSCOW)

    first_result = source.load(as_of=first, instruments=(instrument,))
    second_result = source.load(as_of=second, instruments=(instrument,))

    assert len(first_result) == 1
    assert second_result == first_result
    assert len(source.calls) == 2
    assert source.calls[0][0] == _LATEST_SESSION_CANDLES_SQL
    assert source.calls[1][0] == _VOLUME_HISTORY_SQL
    assert source.calls[0][1]["local_minute"] == "660"
    assert source.calls[1][1]["local_minute"] == "660"
    assert source.calls[0][1]["as_of"].endswith("08:00:59.999999")
