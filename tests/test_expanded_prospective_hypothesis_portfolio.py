from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificRequest,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_scientific_models import (
    FrozenPairParameters,
    ProspectiveDecision,
    ProspectiveHypothesis,
    ProspectiveScientificPolicy,
    morning_regime_features,
    pair_residual_reversion_feature,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def _at(day: date, hour: int, minute: int) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=MOSCOW)


def _candle(
    ticker: str,
    at: datetime,
    open_price: float,
    close_price: float,
    *,
    volume: float = 100.0,
) -> HistoricalCandle:
    return HistoricalCandle(
        ticker=ticker,
        at=at,
        open=open_price,
        high=max(open_price, close_price),
        low=min(open_price, close_price),
        close=close_price,
        volume=volume,
    )


def test_portfolio_has_eleven_exactly_versioned_hypotheses() -> None:
    assert len(tuple(ProspectiveHypothesis)) == 11
    versions = {item.value: item.version for item in ProspectiveHypothesis}
    assert {
        key: versions[key] for key in ("H1", "H2", "H5", "H6", "H12")
    } == {
        "H1": "1.0.0",
        "H2": "1.0.0",
        "H5": "1.0.0",
        "H6": "1.0.0",
        "H12": "1.0.0",
    }


def test_morning_reversion_and_continuation_are_mutually_exclusive() -> None:
    policy = ProspectiveScientificPolicy(morning_history_days=2)
    observed_at = _at(date(2026, 7, 22), 10, 0)
    common = dict(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        feature_max_observed_at=observed_at - timedelta(minutes=10),
        horizon_seconds=1800,
        morning_deviation_bps=100.0,
        morning_deviation_z=2.5,
        market_return_bps=-1.0,
        market_coverage=1.0,
        history_count=2,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        valid_baseline=True,
        policy=policy,
    )

    reversion, continuation = morning_regime_features(
        cumulative_relative_volume=0.4,
        morning_range_percentile=0.5,
        **common,
    )
    assert reversion.decision is ProspectiveDecision.MATCHED
    assert reversion.expected_direction == -1
    assert continuation.decision is ProspectiveDecision.NOT_MATCHED

    reversion, continuation = morning_regime_features(
        cumulative_relative_volume=1.6,
        morning_range_percentile=0.95,
        **common,
    )
    assert reversion.decision is ProspectiveDecision.NOT_MATCHED
    assert continuation.decision is ProspectiveDecision.MATCHED
    assert continuation.expected_direction == 1


def test_morning_feature_rejects_future_input_boundary() -> None:
    observed_at = _at(date(2026, 7, 22), 10, 0)
    with pytest.raises(ValueError, match="future market data"):
        morning_regime_features(
            ticker="SBER",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            feature_max_observed_at=observed_at + timedelta(minutes=1),
            horizon_seconds=1800,
            morning_deviation_bps=100.0,
            morning_deviation_z=3.0,
            cumulative_relative_volume=0.4,
            morning_range_percentile=0.5,
            market_return_bps=0.0,
            market_coverage=1.0,
            history_count=2,
            history_observed_until=observed_at - timedelta(days=1),
            trading_gap=False,
            valid_baseline=True,
            policy=ProspectiveScientificPolicy(morning_history_days=2),
        )


def test_pair_model_must_be_frozen_strictly_before_observation() -> None:
    observed_at = _at(date(2026, 7, 22), 12, 0)
    parameters = FrozenPairParameters(
        left_ticker="SBER",
        right_ticker="SBERP",
        intercept=0.0,
        hedge_ratio=1.0,
        spread_mean=0.0,
        spread_std=0.01,
        correlation=0.9,
        training_points=500,
        trained_until=observed_at,
    )
    with pytest.raises(ValueError, match="completed earlier data"):
        pair_residual_reversion_feature(
            left_ticker="SBER",
            right_ticker="SBERP",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            left_price=100.0,
            right_price=50.0,
            parameters=parameters,
            corporate_action_suspected=False,
            liquid=True,
            policy=ProspectiveScientificPolicy(),
            horizon_seconds=1800,
        )


def test_same_phase_replay_is_deterministic_and_future_labels_do_not_change_features() -> None:
    days = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(10))
    candles: list[HistoricalCandle] = []
    for day_index, trading_day in enumerate(days):
        start = 100.0 + day_index
        for minute in range(30):
            candles.append(
                _candle(
                    "SBER",
                    _at(trading_day, 10, minute),
                    start,
                    start + minute * 0.01,
                )
            )
    policy = ProspectiveScientificPolicy(
        phase_recurrence_history_days=2,
        phase_recurrence_horizon_seconds=1800,
    )
    request = ProspectiveScientificRequest(
        selected_hypotheses=(ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE,),
        policy=policy,
    )
    first = build_prospective_scientific_research(
        candles, dataset_fingerprint="sha256:" + "a" * 64, request=request
    )
    repeated = build_prospective_scientific_research(
        reversed(candles), dataset_fingerprint="sha256:" + "a" * 64, request=request
    )
    assert first.report_fingerprint == repeated.report_fingerprint
    assert first.features == repeated.features

    changed = list(candles)
    last = changed[-1]
    changed[-1] = _candle(
        last.ticker,
        last.at,
        last.open,
        last.close + 5.0,
        volume=last.volume,
    )
    second = build_prospective_scientific_research(
        changed, dataset_fingerprint="sha256:" + "b" * 64, request=request
    )
    first_feature = next(item for item in first.features if item.trading_day == days[-1])
    second_feature = next(item for item in second.features if item.trading_day == days[-1])
    assert first_feature == second_feature
    first_outcome = next(
        item for item in first.outcomes if item.observation_id == first_feature.observation_id
    )
    second_outcome = next(
        item for item in second.outcomes if item.observation_id == second_feature.observation_id
    )
    assert first_outcome != second_outcome


def test_pair_replay_fits_only_training_days_and_is_deterministic() -> None:
    days = tuple(date(2026, 6, 1) + timedelta(days=index) for index in range(10))
    candles: list[HistoricalCandle] = []
    for day_index, trading_day in enumerate(days):
        for minute in range(120):
            at = _at(trading_day, 10 + minute // 60, minute % 60)
            common = 50.0 + day_index * 0.5 + minute * 0.01
            residual = 0.08 if (day_index + minute) % 2 else -0.08
            candles.append(_candle("SBERP", at, common, common, volume=100.0))
            candles.append(
                _candle(
                    "SBER",
                    at,
                    common * 2.0 + residual,
                    common * 2.0 + residual,
                    volume=100.0,
                )
            )
    policy = ProspectiveScientificPolicy(
        pair_min_training_points=5,
        pair_min_correlation=0.10,
        pair_horizons_seconds=(900,),
    )
    request = ProspectiveScientificRequest(
        selected_hypotheses=(ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,),
        policy=policy,
        pair_candidates=(("SBER", "SBERP"),),
    )
    first = build_prospective_scientific_research(
        candles, dataset_fingerprint="sha256:" + "c" * 64, request=request
    )
    second = build_prospective_scientific_research(
        reversed(candles), dataset_fingerprint="sha256:" + "c" * 64, request=request
    )
    assert first.report_fingerprint == second.report_fingerprint
    assert first.pair_parameters == second.pair_parameters
    assert len(first.pair_parameters) == 1
    assert first.features
    assert all(
        item.model_trained_until is not None
        and item.model_trained_until < item.observed_at
        and item.feature_max_observed_at <= item.observed_at
        for item in first.features
    )


def test_morning_replay_builds_past_only_features_and_separates_regimes() -> None:
    days = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(6))
    deviations = {
        "SBER": (0.0, 10.0, -10.0, 10.0, -10.0, 100.0),
        "GAZP": (0.0, -10.0, 10.0, -10.0, 10.0, -100.0),
    }
    candles: list[HistoricalCandle] = []
    for ticker, ticker_deviations in deviations.items():
        previous_close = 100.0
        for index, trading_day in enumerate(days):
            price = previous_close * (1.0 + ticker_deviations[index] / 10_000.0)
            volume = 10.0 if index == len(days) - 1 else 100.0
            for minute in range(7 * 60, 9 * 60 + 50):
                candles.append(
                    _candle(
                        ticker,
                        _at(trading_day, minute // 60, minute % 60),
                        price,
                        price,
                        volume=volume,
                    )
                )
            for minute in range(30):
                candles.append(
                    _candle(
                        ticker,
                        _at(trading_day, 10, minute),
                        price,
                        price,
                        volume=100.0,
                    )
                )
            previous_close = price
    policy = ProspectiveScientificPolicy(
        morning_history_days=2,
        morning_reversion_horizons_seconds=(1800,),
        morning_continuation_horizons_seconds=(1800,),
    )
    report = build_prospective_scientific_research(
        candles,
        dataset_fingerprint="sha256:" + "d" * 64,
        request=ProspectiveScientificRequest(
            selected_hypotheses=(
                ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
                ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
            ),
            policy=policy,
            market_universe=("SBER", "GAZP"),
        ),
    )
    latest = tuple(item for item in report.features if item.trading_day == days[-1])
    assert len(latest) == 4
    assert all(item.feature_max_observed_at < item.observed_at for item in latest)
    assert {
        item.hypothesis
        for item in latest
        if item.decision is ProspectiveDecision.MATCHED
    } == {ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION}


def test_open_close_basket_uses_opening_data_only_at_preclose_decision() -> None:
    days = tuple(date(2026, 7, 1) + timedelta(days=index) for index in range(6))
    candles: list[HistoricalCandle] = []
    for ticker in ("SBER", "GAZP"):
        for day_index, trading_day in enumerate(days):
            opening = 100.0 + day_index
            for minute in range(30):
                candles.append(
                    _candle(
                        ticker,
                        _at(trading_day, 10, minute),
                        opening,
                        opening + minute * 0.01,
                    )
                )
            for minute in range(30):
                candles.append(
                    _candle(
                        ticker,
                        _at(trading_day, 18, 10 + minute),
                        opening + 1.0,
                        opening + 1.0 + minute * 0.01,
                    )
                )
    report = build_prospective_scientific_research(
        candles,
        dataset_fingerprint="sha256:" + "e" * 64,
        request=ProspectiveScientificRequest(
            selected_hypotheses=(
                ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION,
            ),
            market_universe=("SBER", "GAZP"),
        ),
    )
    assert len(report.features) == len(days)
    assert all(
        item.feature_max_observed_at < item.observed_at
        and item.ticker == "MOEX_FIXED_BASKET"
        for item in report.features
    )
