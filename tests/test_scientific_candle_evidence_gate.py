from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from tinvest_signal_engine.adapters.scientific_candle_replay import (
    ScientificCandleReplayArtifactAdapter,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.scientific_candle_models import (
    ScientificCandleResearchReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import ChronologicalSplit
from tinvest_signal_engine.domain.scientific_candle_models import (
    HarParameters,
    ScientificCandleHypothesis,
    ScientificCandlePolicy,
    directional_outcome,
    har_volatility_feature,
    opening_gap_feature,
    relative_volume_activity_feature,
    residual_reversal_feature,
    variance_outcome,
)


UTC = timezone.utc
START = date(2026, 1, 1)
TICKERS = ("SBER", "GAZP")


def test_full_gate_passes_activity_and_rejects_negative_directional_effects(
    tmp_path: Path,
) -> None:
    adapter = ScientificCandleReplayArtifactAdapter(
        tmp_path,
        evidence_policy=EvidenceGatePolicy(
            minimum_trading_days=7,
            minimum_eligible_events=14,
            controls_per_event=5,
            bootstrap_samples=300,
            bootstrap_seed=31,
            false_discovery_rate=0.05,
            required_positive_stability_blocks=4,
            maximum_instrument_share=0.50,
        ),
    )
    report = _portfolio_report()

    first = adapter.save(
        report,
        tuple(ScientificCandleHypothesis),
        cost_model_version="cost-v1",
    )
    second = adapter.save(
        report,
        tuple(reversed(tuple(ScientificCandleHypothesis))),
        cost_model_version="cost-v1",
    )

    assert first == second
    by_id = {str(row["hypothesis_id"]): row for row in first.evidence}
    assert by_id["H7V2"]["decision"] == "passed"
    assert by_id["H15"]["decision"] == "passed"
    assert by_id["H10"]["decision"] == "rejected"
    assert by_id["H11"]["decision"] == "rejected"
    for hypothesis_id in ("H7V2", "H15", "H10", "H11"):
        row = by_id[hypothesis_id]
        assert row["sample_count"] == 14
        assert row["matched_controls"] == 70
        assert row["controls_per_event"] == 5
        assert row["trading_days"] == 7
        assert row["total_blocks"] == 5
        assert row["maximum_ticker_share"] == 0.5
        assert row["adjusted_p_value"] is not None
        assert row["horizons"][0]["evidence_scope"] == "independent_gate"
    assert by_id["H7V2"]["matched_control_lift_ci95_lower"] > 0.0
    assert by_id["H15"]["matched_control_lift_ci95_lower"] > 0.0
    assert by_id["H10"]["matched_control_lift_ci95_upper"] < 0.0
    assert by_id["H11"]["matched_control_lift_ci95_upper"] < 0.0
    assert by_id["H7V2"]["stable_blocks"] == 5
    assert by_id["H15"]["stable_blocks"] == 5


def _portfolio_report() -> ScientificCandleResearchReport:
    train_days = tuple(START + timedelta(days=index) for index in range(5))
    validation_days = tuple(START + timedelta(days=index) for index in range(5, 15))
    holdout_days = tuple(START + timedelta(days=index) for index in range(15, 57))
    split = ChronologicalSplit(
        train_days=train_days,
        validation_days=validation_days,
        holdout_days=holdout_days,
    )
    policy = ScientificCandlePolicy(
        opening_gap_min_bps=5.0,
        residual_move_min_bps=5.0,
        activity_history_days=20,
        round_trip_cost_bps=10.0,
    )
    parameters = HarParameters(
        intercept=0.0,
        short_weight=1.0,
        medium_weight=0.0,
        long_weight=0.0,
        training_points=100,
        trained_until=_at(train_days[-1], hour=6),
    )
    pairs = []

    # H15's 90th-percentile event threshold is sealed on validation only.
    for index, trading_day in enumerate(validation_days, start=1):
        for ticker in TICKERS:
            observed_at = _at(trading_day)
            feature = har_volatility_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                short_variance=float(index),
                medium_variance=1.0,
                long_variance=1.0,
                parameters=parameters,
                policy=policy,
            )
            pairs.append(
                (
                    feature,
                    variance_outcome(
                        feature,
                        target_at=observed_at + timedelta(minutes=30),
                        actual_future_variance=float(index),
                        policy=policy,
                    ),
                )
            )

    for day_index, trading_day in enumerate(holdout_days):
        event = day_index < 7
        for ticker in TICKERS:
            observed_at = _at(trading_day)
            opening = opening_gap_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                previous_close=100.0,
                opening_price=101.0 if event else 100.01,
                policy=policy,
            )
            residual = residual_reversal_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                instrument_return_bps=50.0 if event else 1.0,
                market_return_bps=0.0,
                market_members=5,
                policy=policy,
            )
            har = har_volatility_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                short_variance=10.0 if event else 1.0,
                medium_variance=1.0,
                long_variance=1.0,
                parameters=parameters,
                policy=policy,
            )
            activity = relative_volume_activity_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                current_volume=1_000.0 if event else 1.0,
                historical_phase_volumes=(
                    tuple(float(value) for value in range(1, 21))
                    if event
                    else (100.0,) * 20
                ),
                baseline_future_variance=1.0,
                policy=policy,
            )
            target_at = observed_at + timedelta(minutes=30)
            pairs.extend(
                (
                    (
                        opening,
                        directional_outcome(
                            opening,
                            target_at=target_at,
                            forward_return_bps=30.0 if event else 0.0,
                            policy=policy,
                        ),
                    ),
                    (
                        residual,
                        directional_outcome(
                            residual,
                            target_at=target_at,
                            forward_return_bps=30.0 if event else 0.0,
                            policy=policy,
                        ),
                    ),
                    (
                        har,
                        variance_outcome(
                            har,
                            target_at=target_at,
                            actual_future_variance=10.0 if event else 1.0,
                            policy=policy,
                        ),
                    ),
                    (
                        activity,
                        variance_outcome(
                            activity,
                            target_at=target_at,
                            actual_future_variance=2.0 if event else 1.0,
                            policy=policy,
                        ),
                    ),
                )
            )

    pairs.sort(
        key=lambda pair: (
            pair[0].observed_at,
            pair[0].ticker,
            pair[0].hypothesis.value,
        )
    )
    return ScientificCandleResearchReport(
        dataset_fingerprint="sha256:" + "d" * 64,
        report_fingerprint="sha256:" + "e" * 64,
        split=split,
        policy=policy,
        selected_hypotheses=tuple(ScientificCandleHypothesis),
        har_parameters=parameters,
        features=tuple(pair[0] for pair in pairs),
        outcomes=tuple(pair[1] for pair in pairs),
    )


def _at(trading_day: date, *, hour: int = 7) -> datetime:
    return datetime.combine(trading_day, time(hour=hour), tzinfo=UTC)
