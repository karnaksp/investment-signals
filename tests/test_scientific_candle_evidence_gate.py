from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path

from tinvest_signal_engine.adapters.scientific_candle_replay import (
    INTERMEDIATE_SCIENTIFIC_CANDLE_EVIDENCE_POLICY,
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
    har_volatility_feature,
    relative_volume_activity_feature,
    variance_outcome,
)


UTC = timezone.utc
START = date(2026, 1, 1)
TICKERS = ("SBER", "GAZP")


def test_intermediate_policy_matches_preregistered_product_gate() -> None:
    policy = INTERMEDIATE_SCIENTIFIC_CANDLE_EVIDENCE_POLICY

    assert policy.minimum_trading_days == 20
    assert policy.minimum_eligible_events == 200
    assert policy.controls_per_event == 5
    assert policy.minimum_common_support_coverage == 0.10
    assert policy.required_positive_stability_blocks == 3
    assert policy.maximum_instrument_share == 0.40


def test_real_shaped_h15_and_h7_use_causal_common_support(
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
        (
            ScientificCandleHypothesis.HAR_VOLATILITY,
            ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
        ),
        cost_model_version="cost-v1",
    )
    second = adapter.save(
        report,
        (
            ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
            ScientificCandleHypothesis.HAR_VOLATILITY,
        ),
        cost_model_version="cost-v1",
    )

    assert first == second
    by_id = {str(row["hypothesis_id"]): row for row in first.evidence}
    assert by_id["H7V2"]["decision"] == "passed"
    assert by_id["H15"]["decision"] == "passed"
    for hypothesis_id in ("H7V2", "H15"):
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
    assert by_id["H7V2"]["stable_blocks"] == 5
    assert by_id["H15"]["stable_blocks"] == 5
    coverage = json.loads(
        (Path(first.artifact_uri) / "manifest.json").read_text(encoding="utf-8")
    )["evidence_coverage"]
    for hypothesis_id in ("H7V2", "H15"):
        assert coverage[hypothesis_id]["triggered_events"] == 14
        assert coverage[hypothesis_id]["eligible_common_support_events"] == 14
        assert coverage[hypothesis_id]["unmatched_events"] == 0
        assert coverage[hypothesis_id]["selection_policy"] == (
            "pre_outcome_deterministic_common_support_v1"
        )


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
            forecast = float(index + (100 if ticker == "GAZP" else 0))
            feature = har_volatility_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                short_variance=forecast,
                medium_variance=1.0,
                long_variance=forecast * 10.0,
                parameters=parameters,
                policy=policy,
            )
            pairs.append(
                (
                    feature,
                    variance_outcome(
                        feature,
                        target_at=observed_at + timedelta(minutes=30),
                        actual_future_variance=forecast,
                        policy=policy,
                    ),
                )
            )

    for day_index, trading_day in enumerate(holdout_days):
        event = day_index < 7
        for ticker in TICKERS:
            observed_at = _at(trading_day)
            normal_forecast = 101.0 if ticker == "GAZP" else 1.0
            event_forecast = 110.0 if ticker == "GAZP" else 10.0
            forecast = event_forecast if event else normal_forecast
            har = har_volatility_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                short_variance=forecast,
                medium_variance=1.0,
                # This HAR input helps define treatment.  A regression in the
                # old matcher put event and control observations in disjoint
                # volatility buckets by matching on it.
                long_variance=forecast * 10.0 if event else forecast,
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
                        har,
                        variance_outcome(
                            har,
                            target_at=target_at,
                            actual_future_variance=forecast,
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
        selected_hypotheses=(
            ScientificCandleHypothesis.HAR_VOLATILITY,
            ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
        ),
        har_parameters=parameters,
        features=tuple(pair[0] for pair in pairs),
        outcomes=tuple(pair[1] for pair in pairs),
    )


def _at(trading_day: date, *, hour: int = 7) -> datetime:
    return datetime.combine(trading_day, time(hour=hour), tzinfo=UTC)
