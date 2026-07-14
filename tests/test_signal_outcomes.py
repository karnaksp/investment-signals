from __future__ import annotations

import pytest

from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    classify_directional_outcome,
)


def test_directional_outcome_subtracts_cost_from_expected_and_reverse() -> None:
    policy = DirectionalOutcomePolicy(
        minimum_move_bps=10.0,
        volatility_multiplier=0.0,
        round_trip_cost_bps=4.0,
    )

    insignificant = classify_directional_outcome(
        gross_expected_bps=13.999,
        baseline_sigma_bps=1.0,
        horizon_seconds=60,
        policy=policy,
    )
    confirmed = classify_directional_outcome(
        gross_expected_bps=14.0,
        baseline_sigma_bps=1.0,
        horizon_seconds=60,
        policy=policy,
    )
    contradicted = classify_directional_outcome(
        gross_expected_bps=-14.0,
        baseline_sigma_bps=1.0,
        horizon_seconds=60,
        policy=policy,
    )

    assert insignificant.verdict == "insignificant"
    assert insignificant.net_expected_bps == pytest.approx(9.999)
    assert insignificant.net_reverse_bps == pytest.approx(-17.999)
    assert insignificant.materiality_bps == 10.0
    assert confirmed.verdict == "confirmed"
    assert confirmed.net_expected_bps == 10.0
    assert confirmed.net_reverse_bps == -18.0
    assert contradicted.verdict == "contradicted"
    assert contradicted.net_expected_bps == -18.0
    assert contradicted.net_reverse_bps == 10.0


def test_directional_outcome_materiality_scales_from_baseline_window() -> None:
    policy = DirectionalOutcomePolicy(
        minimum_move_bps=1.0,
        volatility_multiplier=2.0,
        round_trip_cost_bps=0.0,
        baseline_volatility_window_seconds=60,
    )

    assessment = classify_directional_outcome(
        gross_expected_bps=20.0,
        baseline_sigma_bps=3.0,
        horizon_seconds=9 * 60,
        policy=policy,
    )

    assert assessment.verdict == "confirmed"
    assert assessment.materiality_bps == 18.0


def test_directional_outcome_rejects_invalid_policy_and_inputs() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        DirectionalOutcomePolicy(
            minimum_move_bps=-1.0,
            volatility_multiplier=0.0,
            round_trip_cost_bps=0.0,
        )
    policy = DirectionalOutcomePolicy(
        minimum_move_bps=1.0,
        volatility_multiplier=0.0,
        round_trip_cost_bps=0.0,
    )
    with pytest.raises(ValueError, match="baseline sigma"):
        classify_directional_outcome(
            gross_expected_bps=1.0,
            baseline_sigma_bps=-0.1,
            horizon_seconds=60,
            policy=policy,
        )
    with pytest.raises(ValueError, match="horizon"):
        classify_directional_outcome(
            gross_expected_bps=1.0,
            baseline_sigma_bps=0.1,
            horizon_seconds=0,
            policy=policy,
        )
