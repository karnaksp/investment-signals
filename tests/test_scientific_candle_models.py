from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

import pytest

from tinvest_signal_engine.application.scientific_candle_models import (
    ScientificCandleResearchRequest,
    build_scientific_candle_model_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.scientific_candle_models import (
    AbstentionReason,
    CausalFeatureVector,
    FeatureDecision,
    HarTrainingPoint,
    ScientificCandleHypothesis,
    ScientificCandlePolicy,
    ScientificTarget,
    directional_outcome,
    fit_har_parameters,
    har_volatility_feature,
    opening_gap_feature,
    qlike_loss,
    relative_volume_activity_feature,
    residual_reversal_feature,
    variance_outcome,
)


UTC = timezone.utc
START_DAY = date(2026, 1, 5)
TICKERS = ("SBER", "GAZP", "LKOH", "ROSN", "NVTK", "MOEX")


def _policy() -> ScientificCandlePolicy:
    return ScientificCandlePolicy(
        opening_gap_min_bps=5.0,
        residual_move_min_bps=0.1,
        minimum_market_members=5,
        har_minimum_training_points=20,
        activity_history_days=5,
        activity_volume_percentile=0.80,
    )


def _at(day_offset: int, hour: int = 7, minute: int = 0) -> datetime:
    return datetime.combine(
        START_DAY + timedelta(days=day_offset),
        time(hour, minute),
        tzinfo=UTC,
    )


def _candles(days: int = 15) -> tuple[HistoricalCandle, ...]:
    rows: list[HistoricalCandle] = []
    previous = {ticker: 100.0 + index * 10.0 for index, ticker in enumerate(TICKERS)}
    for day_index in range(days):
        high_activity = day_index % 7 == 6
        for ticker_index, ticker in enumerate(TICKERS):
            opening = previous[ticker] * 1.002
            last = opening
            for minute_index in range(151):
                at = _at(day_index) + timedelta(minutes=minute_index)
                volatility = (0.015 + ticker_index * 0.002) * (
                    3.0 if high_activity else 1.0
                )
                oscillation = volatility if minute_index % 2 else -volatility
                drift = (ticker_index - 2.5) * 0.0008
                close = max(1.0, last + drift + oscillation)
                volume = (800.0 if high_activity else 100.0) * (
                    1.0 + ticker_index / 20.0
                )
                rows.append(
                    HistoricalCandle(
                        ticker=ticker,
                        at=at,
                        open=last,
                        high=max(last, close) + 0.01,
                        low=min(last, close) - 0.01,
                        close=close,
                        volume=volume,
                    )
                )
                last = close
            previous[ticker] = last
    return tuple(rows)


def test_h10_positive_gap_is_causal_and_uses_reversion_direction() -> None:
    observed_at = _at(1)
    feature = opening_gap_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        previous_close=100.0,
        opening_price=101.0,
        policy=_policy(),
    )

    assert feature.decision is FeatureDecision.MATCHED
    assert feature.expected_direction == -1
    assert feature.feature_max_observed_at == observed_at
    outcome = directional_outcome(
        feature,
        target_at=observed_at + timedelta(minutes=30),
        forward_return_bps=-30.0,
        policy=_policy(),
    )
    assert outcome.cost_adjusted_value == pytest.approx(20.0)
    assert outcome.supported is True


@pytest.mark.parametrize(
    ("opening_price", "decision", "reason"),
    [
        (99.0, FeatureDecision.ABSTAIN, AbstentionReason.NON_POSITIVE_OPENING_GAP),
        (100.01, FeatureDecision.NOT_MATCHED, AbstentionReason.CONDITIONS_NOT_MET),
    ],
)
def test_h10_refuses_non_positive_or_immaterial_gaps(
    opening_price: float,
    decision: FeatureDecision,
    reason: AbstentionReason,
) -> None:
    feature = opening_gap_feature(
        ticker="SBER",
        trading_day=START_DAY,
        observed_at=_at(0),
        previous_close=100.0,
        opening_price=opening_price,
        policy=_policy(),
    )
    assert feature.decision is decision
    assert feature.reason is reason


def test_h11_removes_market_move_before_choosing_reversal() -> None:
    feature = residual_reversal_feature(
        ticker="SBER",
        trading_day=START_DAY,
        observed_at=_at(0, 7, 5),
        instrument_return_bps=35.0,
        market_return_bps=10.0,
        market_members=6,
        policy=_policy(),
    )
    assert feature.value("market_residual_bps") == 25.0
    assert feature.expected_direction == -1
    assert feature.decision is FeatureDecision.MATCHED

    refused = residual_reversal_feature(
        ticker="SBER",
        trading_day=START_DAY,
        observed_at=_at(0, 7, 5),
        instrument_return_bps=35.0,
        market_return_bps=10.0,
        market_members=2,
        policy=_policy(),
    )
    assert refused.reason is AbstentionReason.INSUFFICIENT_MARKET_MEMBERS


def test_har_fit_is_deterministic_and_rejects_future_trained_parameters() -> None:
    points = tuple(
        HarTrainingPoint(
            feature_at=_at(index),
            target_at=_at(index) + timedelta(minutes=30),
            short_variance=float(index + 1),
            medium_variance=float(index + 2),
            long_variance=float(index + 3),
            target_variance=1.0
            + 0.5 * (index + 1)
            + 0.3 * (index + 2)
            + 0.2 * (index + 3),
        )
        for index in range(30)
    )
    first = fit_har_parameters(points, minimum_points=20, ridge_penalty=1e-6)
    second = fit_har_parameters(reversed(points), minimum_points=20, ridge_penalty=1e-6)
    assert first.predict(3.0, 4.0, 5.0) == pytest.approx(second.predict(3.0, 4.0, 5.0))
    assert first.predict(3.0, 4.0, 5.0) == pytest.approx(4.7, rel=1e-4)

    with pytest.raises(ValueError, match="future"):
        har_volatility_feature(
            ticker="SBER",
            trading_day=START_DAY,
            observed_at=first.trained_until - timedelta(seconds=1),
            short_variance=3.0,
            medium_variance=4.0,
            long_variance=5.0,
            parameters=first,
            policy=_policy(),
        )


def test_h15_uses_qlike_against_long_window_benchmark() -> None:
    points = tuple(
        HarTrainingPoint(
            feature_at=_at(index),
            target_at=_at(index) + timedelta(minutes=30),
            short_variance=float(index + 1),
            medium_variance=float(index + 2),
            long_variance=float(index + 3),
            target_variance=float(index + 2),
        )
        for index in range(30)
    )
    parameters = fit_har_parameters(points, minimum_points=20, ridge_penalty=1e-6)
    feature_at = parameters.trained_until + timedelta(days=1)
    feature = har_volatility_feature(
        ticker="SBER",
        trading_day=feature_at.date(),
        observed_at=feature_at,
        short_variance=10.0,
        medium_variance=12.0,
        long_variance=30.0,
        parameters=parameters,
        policy=_policy(),
    )
    outcome = variance_outcome(
        feature,
        target_at=feature_at + timedelta(minutes=30),
        actual_future_variance=12.0,
        policy=_policy(),
    )
    assert outcome.model_loss == pytest.approx(
        qlike_loss(12.0, feature.forecast_value or 0.0)
    )
    assert outcome.benchmark_loss == pytest.approx(qlike_loss(12.0, 30.0))


def test_h7_v2_requires_prior_phase_history_and_predicts_activity_only() -> None:
    policy = _policy()
    insufficient = relative_volume_activity_feature(
        ticker="SBER",
        trading_day=START_DAY,
        observed_at=_at(0, 7, 15),
        current_volume=500.0,
        historical_phase_volumes=(100.0,) * 4,
        baseline_future_variance=10.0,
        policy=policy,
    )
    assert insufficient.reason is AbstentionReason.INSUFFICIENT_HISTORY

    feature = relative_volume_activity_feature(
        ticker="SBER",
        trading_day=START_DAY,
        observed_at=_at(0, 7, 15),
        current_volume=500.0,
        historical_phase_volumes=(100.0, 110.0, 120.0, 130.0, 140.0),
        baseline_future_variance=10.0,
        policy=policy,
    )
    assert feature.hypothesis is ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2
    assert feature.expected_direction == 0
    assert feature.target is ScientificTarget.FUTURE_ACTIVITY_UPLIFT
    outcome = variance_outcome(
        feature,
        target_at=feature.observed_at + timedelta(minutes=30),
        actual_future_variance=15.0,
        policy=policy,
    )
    assert outcome.actual_value == pytest.approx(1.5)
    assert outcome.supported is True


def test_feature_contract_rejects_future_feature_timestamp() -> None:
    at = _at(0)
    with pytest.raises(ValueError, match="future"):
        CausalFeatureVector(
            observation_id="sha256:" + "a" * 64,
            hypothesis=ScientificCandleHypothesis.OPENING_GAP_REVERSION,
            hypothesis_version="1.0.0",
            ticker="SBER",
            trading_day=START_DAY,
            observed_at=at,
            feature_max_observed_at=at + timedelta(seconds=1),
            model_trained_until=None,
            horizon_seconds=300,
            target=ScientificTarget.DIRECTIONAL_RETURN_BPS,
            decision=FeatureDecision.MATCHED,
            reason=AbstentionReason.CONDITIONS_MATCHED,
            expected_direction=-1,
            forecast_value=-10.0,
            feature_values=(("gap", 10.0),),
        )


def test_full_package_is_deterministic_partitioned_and_causal() -> None:
    candles = _candles()
    request = ScientificCandleResearchRequest(policy=_policy())
    first = build_scientific_candle_model_research(
        candles,
        dataset_fingerprint="sha256:" + "1" * 64,
        request=request,
    )
    second = build_scientific_candle_model_research(
        tuple(reversed(candles)),
        dataset_fingerprint="sha256:" + "1" * 64,
        request=request,
    )

    assert first.report_fingerprint == second.report_fingerprint
    assert first.features == second.features
    assert {item.hypothesis for item in first.features} == set(
        ScientificCandleHypothesis
    )
    assert all(
        item.feature_max_observed_at <= item.observed_at for item in first.features
    )
    assert all(
        item.model_trained_until is None or item.model_trained_until <= item.observed_at
        for item in first.features
    )
    assert first.har_parameters is not None
    first_har = next(
        item
        for item in first.features
        if item.hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY
    )
    assert first.har_parameters.trained_until < first_har.observed_at


def test_future_label_change_does_not_change_h10_features() -> None:
    candles = _candles(days=8)
    request = ScientificCandleResearchRequest(
        selected_hypotheses=(ScientificCandleHypothesis.OPENING_GAP_REVERSION,),
        policy=_policy(),
    )
    baseline = build_scientific_candle_model_research(
        candles,
        dataset_fingerprint="sha256:" + "2" * 64,
        request=request,
    )
    target_feature = next(
        item for item in baseline.features if item.decision is FeatureDecision.MATCHED
    )
    target_outcome = baseline.outcomes[baseline.features.index(target_feature)]
    target_candle_at = target_outcome.target_at - timedelta(minutes=1)
    changed = tuple(
        replace(
            candle,
            high=candle.high + 1.0,
            close=candle.close + 1.0,
        )
        if candle.ticker == target_feature.ticker and candle.at == target_candle_at
        else candle
        for candle in candles
    )
    modified = build_scientific_candle_model_research(
        changed,
        dataset_fingerprint="sha256:" + "3" * 64,
        request=request,
    )

    assert baseline.features == modified.features
    assert baseline.outcomes != modified.outcomes
