from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tinvest_signal_engine.adapters.research_hypothesis_features import (
    ResearchHypothesisFeatureAdapter,
)
from tinvest_signal_engine.application.hypothesis_observations import (
    EvaluateHypothesisObservation,
)
from tinvest_signal_engine.domain.hypothesis_formulas import (
    ExpectedEffect,
    FeatureName,
    HypothesisFeatureSet,
    HypothesisId,
    ObservationReason,
    ObservationVerdict,
    ObservedFeature,
    OutcomeAnchor,
    default_rule,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
    TradingPhase,
)


def _at_moscow(hour: int, minute: int) -> datetime:
    return datetime(2026, 7, 15, hour - 3, minute, tzinfo=timezone.utc)


def _features(at: datetime, **values: float) -> HypothesisFeatureSet:
    return HypothesisFeatureSet.from_iterable(
        ObservedFeature(
            name=FeatureName(name),
            value=value,
            observed_at=at,
            window_start=at - timedelta(minutes=20),
            window_end=at,
        )
        for name, value in values.items()
    )


@pytest.mark.parametrize(
    ("at", "expected"),
    [
        (_at_moscow(7, 0), TradingPhase.MORNING_LOW_LIQUIDITY),
        (_at_moscow(9, 49), TradingPhase.MORNING_LOW_LIQUIDITY),
        (_at_moscow(9, 50), TradingPhase.OPENING_TRANSITION),
        (_at_moscow(10, 0), TradingPhase.MAIN_OPENING),
        (_at_moscow(12, 0), TradingPhase.MAIN_CONTINUOUS),
        (_at_moscow(18, 10), TradingPhase.PRE_CLOSE),
        (_at_moscow(18, 40), TradingPhase.OUTSIDE_RESEARCH_SESSION),
    ],
)
def test_versioned_schedule_classifies_phase_boundaries(
    at: datetime,
    expected: TradingPhase,
) -> None:
    assert MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(at) is expected


def test_transition_is_not_signal_eligible() -> None:
    assert MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(_at_moscow(7, 0))
    assert not MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(_at_moscow(9, 55))
    assert MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(_at_moscow(10, 0))


def test_schedule_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(datetime(2026, 7, 15, 7, 0))


def test_research_adapter_maps_records_at_the_outer_boundary() -> None:
    result = ResearchHypothesisFeatureAdapter.from_records(({
        "name": "previous_close",
        "value": "100.5",
        "observed_at": "2026-07-15T04:00:00Z",
        "window_start": "2026-07-14T15:39:00Z",
        "window_end": "2026-07-14T15:39:00Z",
    },))

    feature = result.get(FeatureName.PREVIOUS_CLOSE)
    assert feature is not None
    assert feature.value == 100.5


def test_h1_matches_morning_reversal_and_is_deterministic() -> None:
    at = _at_moscow(8, 30)
    features = _features(
        at,
        previous_close=100.0,
        event_price=102.0,
        morning_deviation_z=2.5,
        cumulative_relative_volume=0.7,
    )
    evaluator = EvaluateHypothesisObservation()

    first = evaluator.execute(
        hypothesis_id=HypothesisId.H1,
        ticker="SBER",
        event_at=at,
        features=features,
    )
    second = evaluator.execute(
        hypothesis_id=HypothesisId.H1,
        ticker="SBER",
        event_at=at,
        features=features,
    )

    assert first == second
    assert first.verdict is ObservationVerdict.MATCHED
    assert first.expected_effect is ExpectedEffect.REVERSAL
    assert first.expected_direction == -1
    assert first.phase is TradingPhase.MORNING_LOW_LIQUIDITY
    assert first.outcome_anchor is OutcomeAnchor.MAIN_SESSION_OPEN
    assert first.horizons_seconds == (1800, 3600)


def test_any_feature_from_the_future_forces_abstention() -> None:
    at = _at_moscow(8, 30)
    valid = _features(
        at,
        previous_close=100.0,
        event_price=102.0,
        morning_deviation_z=2.5,
    )
    future = ObservedFeature(
        name=FeatureName.CUMULATIVE_RELATIVE_VOLUME,
        value=0.7,
        observed_at=at + timedelta(seconds=1),
        window_start=at - timedelta(minutes=10),
        window_end=at + timedelta(seconds=1),
    )

    result = EvaluateHypothesisObservation().execute(
        hypothesis_id=HypothesisId.H1,
        ticker="SBER",
        event_at=at,
        features=HypothesisFeatureSet(valid.values + (future,)),
    )

    assert result.verdict is ObservationVerdict.ABSTAIN
    assert result.reason is ObservationReason.FUTURE_FEATURE
    assert result.expected_direction == 0
    assert result.feature_cutoff_at == at


def test_missing_feature_and_trading_gap_are_explicit_abstentions() -> None:
    at = _at_moscow(11, 0)
    missing = EvaluateHypothesisObservation().execute(
        hypothesis_id=HypothesisId.H4,
        ticker="SBER",
        event_at=at,
        features=_features(at, five_minute_return_bps=20.0),
    )
    gap = EvaluateHypothesisObservation().execute(
        hypothesis_id=HypothesisId.H4,
        ticker="SBER",
        event_at=at,
        features=HypothesisFeatureSet(()),
        has_trading_gap=True,
    )

    assert (missing.verdict, missing.reason) == (
        ObservationVerdict.ABSTAIN,
        ObservationReason.MISSING_FEATURE,
    )
    assert (gap.verdict, gap.reason) == (
        ObservationVerdict.ABSTAIN,
        ObservationReason.TRADING_GAP,
    )


@pytest.mark.parametrize(
    ("hypothesis_id", "at", "values", "effect", "direction"),
    [
        (HypothesisId.H1, _at_moscow(8, 0), {
            "previous_close": 100, "event_price": 102, "morning_deviation_z": 2.1,
            "cumulative_relative_volume": 0.8,
        }, ExpectedEffect.REVERSAL, -1),
        (HypothesisId.H2, _at_moscow(8, 0), {
            "previous_close": 100, "event_price": 102, "morning_deviation_z": 2.1,
            "cumulative_relative_volume": 1.5, "range_percentile": 0.9,
        }, ExpectedEffect.CONTINUATION, 1),
        (HypothesisId.H3, _at_moscow(11, 0), {
            "five_minute_return_bps": 30, "five_minute_move_percentile": 0.99,
            "relative_volume_percentile": 0.49, "illiquidity_percentile": 0.75,
            "market_alignment": 0,
        }, ExpectedEffect.REVERSAL, -1),
        (HypothesisId.H4, _at_moscow(11, 0), {
            "five_minute_return_bps": -30, "five_minute_move_percentile": 0.99,
            "relative_volume_percentile": 0.9, "range_percentile": 0.9,
        }, ExpectedEffect.CONTINUATION, -1),
        (HypothesisId.H5, _at_moscow(11, 0), {
            "same_phase_mean_return_bps_20d": 3, "same_phase_history_days": 20,
        }, ExpectedEffect.PHASE_REPEAT, 1),
        (HypothesisId.H6, _at_moscow(11, 0), {
            "opening_basket_return_bps": -5,
        }, ExpectedEffect.MARKET_CONTINUATION, -1),
        (HypothesisId.H7, _at_moscow(11, 0), {
            "phase_volume_percentile": 0.9, "phase_history_days": 20,
        }, ExpectedEffect.ACTIVITY_UPLIFT, 0),
    ],
)
def test_all_h1_h7_rules_have_a_safe_executable_match(
    hypothesis_id: HypothesisId,
    at: datetime,
    values: dict[str, float],
    effect: ExpectedEffect,
    direction: int,
) -> None:
    result = EvaluateHypothesisObservation().execute(
        hypothesis_id=hypothesis_id,
        ticker="SBER",
        event_at=at,
        features=_features(at, **values),
    )

    assert result.verdict is ObservationVerdict.MATCHED
    assert result.reason is ObservationReason.CONDITIONS_MATCHED
    assert result.expected_effect is effect
    assert result.expected_direction == direction
    assert result.hypothesis_version == default_rule(hypothesis_id).version


def test_market_confirmed_move_makes_h3_abstain() -> None:
    at = _at_moscow(11, 0)
    result = EvaluateHypothesisObservation().execute(
        hypothesis_id=HypothesisId.H3,
        ticker="SBER",
        event_at=at,
        features=_features(
            at,
            five_minute_return_bps=30,
            five_minute_move_percentile=0.99,
            relative_volume_percentile=0.2,
            illiquidity_percentile=0.8,
            market_alignment=1,
        ),
    )

    assert result.verdict is ObservationVerdict.ABSTAIN
    assert result.reason is ObservationReason.MARKET_MOVE_CONFIRMS_EVENT
