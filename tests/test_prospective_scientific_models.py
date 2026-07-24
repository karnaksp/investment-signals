from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificRequest,
    ProspectiveScientificReport,
    build_prospective_scientific_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_scientific_models import (
    HarV2Parameters,
    JumpHistoryPoint,
    MetricUnit,
    ProspectiveDecision,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    TargetMetric,
    downside_semivariance_feature,
    har_v2_feature,
    har_v2_outcome,
    jump_regime_features,
    jump_regime_v3_features,
    relative_volume_volatility_feature,
    volatility_jump_feature,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def test_all_targets_and_feature_values_have_explicit_units() -> None:
    observed_at = datetime(2026, 7, 17, 11, 0, tzinfo=MOSCOW)
    policy = ProspectiveScientificPolicy(
        volume_history_days=2,
        semivariance_history_days=2,
        jump_variance_history_days=2,
    )

    volume = relative_volume_volatility_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        current_volume=300.0,
        historical_volumes=(100.0, 200.0),
        baseline_future_variance=12.0,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        policy=policy,
    )
    downside = downside_semivariance_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        downside_share=0.9,
        historical_downside_shares=(0.2, 0.4),
        baseline_future_variance=12.0,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        policy=policy,
    )
    jump = volatility_jump_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        jump_share=0.8,
        continuous_variance=9.0,
        historical_jump_shares=(0.1, 0.3),
        baseline_future_variance=12.0,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        policy=policy,
    )

    assert TargetMetric.FORWARD_RETURN.unit is MetricUnit.BASIS_POINTS
    assert TargetMetric.FUTURE_REALIZED_VARIANCE.unit is MetricUnit.BASIS_POINTS_SQUARED
    assert TargetMetric.FUTURE_VARIANCE_UPLIFT.unit is MetricUnit.RATIO
    assert volume.target_unit is MetricUnit.RATIO
    assert volume.value("current_window_volume") == 300.0
    assert (
        next(
            item.unit
            for item in volume.feature_values
            if item.name == "current_window_volume"
        )
        is MetricUnit.LOTS
    )
    assert all(feature.expected_direction == 0 for feature in (volume, downside, jump))


def test_jump_reversal_and_continuation_regimes_are_mutually_exclusive() -> None:
    policy = ProspectiveScientificPolicy(jump_history_days=4)
    observed_at = datetime(2026, 7, 17, 11, 0, tzinfo=MOSCOW)
    history = tuple(
        JumpHistoryPoint(
            absolute_return_bps=10.0 + index,
            volume=100.0 * (index + 1),
            range_bps=10.0 + index,
            illiquidity=10.0 + index,
        )
        for index in range(4)
    )

    low_activity = jump_regime_features(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        horizon_seconds=300,
        signed_return_bps=100.0,
        volume=50.0,
        range_bps=100.0,
        illiquidity=100.0,
        prior_history=history,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        policy=policy,
    )
    high_activity = jump_regime_features(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        horizon_seconds=300,
        signed_return_bps=100.0,
        volume=500.0,
        range_bps=100.0,
        illiquidity=1.0,
        prior_history=history,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=False,
        policy=policy,
    )

    assert low_activity[0].decision is ProspectiveDecision.MATCHED
    assert low_activity[0].expected_direction == -1
    assert low_activity[1].decision is ProspectiveDecision.NOT_MATCHED
    assert high_activity[0].decision is ProspectiveDecision.NOT_MATCHED
    assert high_activity[1].decision is ProspectiveDecision.MATCHED
    assert high_activity[1].expected_direction == 1
    for volume in range(25, 601, 25):
        h3, h4 = jump_regime_features(
            ticker="SBER",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            horizon_seconds=300,
            signed_return_bps=100.0,
            volume=float(volume),
            range_bps=100.0,
            illiquidity=100.0,
            prior_history=history,
            history_observed_until=observed_at - timedelta(days=1),
            trading_gap=False,
            policy=policy,
        )
        assert (
            sum(feature.decision is ProspectiveDecision.MATCHED for feature in (h3, h4))
            <= 1
        )


def test_jump_v3_selects_one_causal_regime_or_abstains_explicitly() -> None:
    policy = ProspectiveScientificPolicy(jump_history_days=4)
    observed_at = datetime(2026, 7, 24, 11, 0, tzinfo=MOSCOW)
    history_until = observed_at - timedelta(days=1)
    history = tuple(
        JumpHistoryPoint(
            absolute_return_bps=10.0 + index,
            volume=100.0 * (index + 1),
            range_bps=10.0 + index,
            illiquidity=10.0 + index,
        )
        for index in range(4)
    )
    common = dict(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        horizon_seconds=300,
        signed_return_bps=100.0,
        prior_history=history,
        history_observed_until=history_until,
        trading_gap=False,
        policy=policy,
    )

    reversal, continuation = jump_regime_v3_features(
        volume=50.0,
        range_bps=5.0,
        illiquidity=100.0,
        **common,
    )
    assert reversal.decision is ProspectiveDecision.MATCHED
    assert reversal.reason is ProspectiveReason.REVERSAL_REGIME_SELECTED
    assert reversal.expected_direction == -1
    assert continuation.decision is ProspectiveDecision.ABSTAIN
    assert continuation.reason is ProspectiveReason.REVERSAL_REGIME_SELECTED

    reversal, continuation = jump_regime_v3_features(
        volume=500.0,
        range_bps=100.0,
        illiquidity=1.0,
        **common,
    )
    assert reversal.decision is ProspectiveDecision.ABSTAIN
    assert reversal.reason is ProspectiveReason.CONTINUATION_REGIME_SELECTED
    assert continuation.decision is ProspectiveDecision.MATCHED
    assert continuation.reason is ProspectiveReason.CONTINUATION_REGIME_SELECTED
    assert continuation.expected_direction == 1

    ambiguous = jump_regime_v3_features(
        volume=250.0,
        range_bps=100.0,
        illiquidity=1.0,
        **common,
    )
    assert all(
        feature.decision is ProspectiveDecision.ABSTAIN
        and feature.reason is ProspectiveReason.ACTIVITY_REGIME_AMBIGUOUS
        for feature in ambiguous
    )
    below_threshold = jump_regime_v3_features(
        volume=50.0,
        range_bps=5.0,
        illiquidity=100.0,
        **(common | {"signed_return_bps": 5.0}),
    )
    assert all(
        feature.decision is ProspectiveDecision.NOT_MATCHED
        and feature.reason is ProspectiveReason.JUMP_THRESHOLD_NOT_MET
        for feature in below_threshold
    )
    assert all(
        feature.feature_max_observed_at == observed_at
        and feature.history_observed_until == history_until
        for feature in (*ambiguous, *below_threshold)
    )


def test_history_boundary_must_be_strictly_before_observation() -> None:
    observed_at = datetime(2026, 7, 17, 11, 0, tzinfo=MOSCOW)
    with pytest.raises(ValueError, match="must precede"):
        relative_volume_volatility_feature(
            ticker="SBER",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            current_volume=300.0,
            historical_volumes=(100.0, 200.0),
            baseline_future_variance=12.0,
            history_observed_until=observed_at,
            trading_gap=False,
            policy=ProspectiveScientificPolicy(volume_history_days=2),
        )
    with pytest.raises(ValueError, match="must precede"):
        har_v2_feature(
            ticker="SBER",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            short_variance=4.0,
            medium_variance=6.0,
            long_variance=8.0,
            parameters=HarV2Parameters(
                intercept=0.1,
                short_weight=0.4,
                medium_weight=0.3,
                long_weight=0.2,
                training_points=100,
                trained_until=observed_at,
            ),
            horizon_seconds=1800,
        )


def test_trading_gap_forces_abstention() -> None:
    observed_at = datetime(2026, 7, 17, 11, 0, tzinfo=MOSCOW)
    feature = relative_volume_volatility_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        current_volume=300.0,
        historical_volumes=(100.0, 200.0),
        baseline_future_variance=12.0,
        history_observed_until=observed_at - timedelta(days=1),
        trading_gap=True,
        policy=ProspectiveScientificPolicy(volume_history_days=2),
    )

    assert feature.decision is ProspectiveDecision.ABSTAIN
    assert feature.reason is ProspectiveReason.NON_CONTIGUOUS_WINDOW


def test_har_v2_reports_typed_loss_against_both_baselines() -> None:
    observed_at = datetime(2026, 7, 17, 11, 0, tzinfo=MOSCOW)
    feature = har_v2_feature(
        ticker="SBER",
        trading_day=observed_at.date(),
        observed_at=observed_at,
        short_variance=4.0,
        medium_variance=6.0,
        long_variance=8.0,
        parameters=HarV2Parameters(
            intercept=0.1,
            short_weight=0.4,
            medium_weight=0.3,
            long_weight=0.2,
            training_points=100,
            trained_until=observed_at - timedelta(days=1),
        ),
        horizon_seconds=1800,
    )
    outcome = har_v2_outcome(
        feature,
        target_at=observed_at + timedelta(minutes=30),
        actual_future_variance=9.0,
        ewma_baseline=7.0,
        phase_baseline=8.0,
    )

    assert outcome.available
    assert (
        outcome.metric("future_realized_variance").unit
        is MetricUnit.BASIS_POINTS_SQUARED
    )
    assert outcome.metric("har_qlike").unit is MetricUnit.DIMENSIONLESS_LOSS
    assert outcome.metric("ewma_qlike").unit is MetricUnit.DIMENSIONLESS_LOSS
    assert outcome.metric("phase_qlike").unit is MetricUnit.DIMENSIONLESS_LOSS


def test_h7_uses_exactly_40_prior_days_and_future_only_for_the_label() -> None:
    candles = _candles_for_days(41)
    request = ProspectiveScientificRequest(
        selected_hypotheses=(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,)
    )
    report = build_prospective_scientific_research(
        candles,
        dataset_fingerprint="sha256:source",
        request=request,
    )
    final_day = date(2026, 2, 10)
    current = _feature_at(report, final_day, time(10, 15))
    previous = _feature_at(report, final_day - timedelta(days=1), time(10, 15))

    assert current.value("prior_day_count") == 40.0
    assert previous.value("prior_day_count") == 39.0
    assert current.history_observed_until is not None
    assert current.history_observed_until < current.observed_at

    changed = list(candles)
    target_index = next(
        index
        for index, candle in enumerate(changed)
        if candle.at.date() == final_day and candle.at.time() == time(10, 30)
    )
    original = changed[target_index]
    changed[target_index] = replace(
        original,
        high=original.close * 1.03,
        low=original.open,
        close=original.close * 1.02,
    )
    changed_report = build_prospective_scientific_research(
        changed,
        dataset_fingerprint="sha256:changed-label",
        request=request,
    )
    changed_feature = _feature_at(changed_report, final_day, time(10, 15))
    original_outcome = _outcome_for(report, current.observation_id)
    changed_outcome = _outcome_for(changed_report, changed_feature.observation_id)

    assert changed_feature.observation_id == current.observation_id
    assert changed_feature.feature_values == current.feature_values
    assert changed_outcome.measurements != original_outcome.measurements


def test_missing_minute_is_not_bridged_by_a_feature_window() -> None:
    candles = list(_candles_for_days(5))
    final_day = date(2026, 1, 5)
    candles = [
        candle
        for candle in candles
        if not (candle.at.date() == final_day and candle.at.time() == time(10, 7))
    ]
    report = build_prospective_scientific_research(
        candles,
        dataset_fingerprint="sha256:gap",
        request=ProspectiveScientificRequest(
            selected_hypotheses=(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,)
        ),
    )

    assert not any(
        feature.trading_day == final_day
        and feature.observed_at.astimezone(MOSCOW).time() == time(10, 15)
        for feature in report.features
    )


def _candles_for_days(day_count: int) -> tuple[HistoricalCandle, ...]:
    first_day = date(2026, 1, 1)
    rows: list[HistoricalCandle] = []
    for day_offset in range(day_count):
        trading_day = first_day + timedelta(days=day_offset)
        current = datetime.combine(trading_day, time(10, 0), tzinfo=MOSCOW)
        price = 100.0 + day_offset * 0.01
        for minute in range(91):
            next_price = price * (1.0001 if minute % 2 == 0 else 0.99995)
            volume = 100.0
            if day_offset == day_count - 1 and minute < 15:
                volume = 300.0
            rows.append(
                HistoricalCandle(
                    ticker="SBER",
                    at=current + timedelta(minutes=minute),
                    open=price,
                    high=max(price, next_price),
                    low=min(price, next_price),
                    close=next_price,
                    volume=volume,
                )
            )
            price = next_price
    return tuple(rows)


def _feature_at(report: ProspectiveScientificReport, trading_day: date, clock: time):
    return next(
        feature
        for feature in report.features
        if feature.trading_day == trading_day
        and feature.observed_at.astimezone(MOSCOW).time() == clock
    )


def _outcome_for(
    report: ProspectiveScientificReport, observation_id: str
) -> ProspectiveOutcome:
    return next(
        outcome
        for outcome in report.outcomes
        if outcome.observation_id == observation_id
    )
