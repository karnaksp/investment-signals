from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.scientific_candle_replay import (
    ScientificCandleReplayArtifactAdapter,
)
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
from tinvest_signal_engine.services.hypothesis_replay_api import (
    ReplayEvidenceResponse,
)


UTC = timezone.utc
HOLDOUT_DAY = date(2026, 1, 9)


def _report() -> ScientificCandleResearchReport:
    policy = ScientificCandlePolicy(
        opening_gap_min_bps=5.0,
        activity_history_days=20,
        round_trip_cost_bps=10.0,
    )
    observed_at = datetime(2026, 1, 9, 7, 1, tzinfo=UTC)
    parameters = HarParameters(
        intercept=2.0,
        short_weight=0.0,
        medium_weight=0.0,
        long_weight=0.0,
        training_points=30,
        trained_until=datetime(2026, 1, 7, 15, 0, tzinfo=UTC),
    )
    h10 = opening_gap_feature(
        ticker="SBER",
        trading_day=HOLDOUT_DAY,
        observed_at=observed_at,
        previous_close=100.0,
        opening_price=101.0,
        policy=policy,
    )
    h11 = residual_reversal_feature(
        ticker="GAZP",
        trading_day=HOLDOUT_DAY,
        observed_at=observed_at,
        instrument_return_bps=50.0,
        market_return_bps=0.0,
        market_beta=1.0,
        beta_observed_until=datetime(2026, 1, 8, 15, 0, tzinfo=UTC),
        beta_history_days=20,
        basket_coverage=1.0,
        absolute_residual_history=tuple(float(item) for item in range(1, 21)),
        absolute_market_return_history=tuple(float(item) for item in range(1, 21)),
        policy=policy,
    )
    h15 = har_volatility_feature(
        ticker="LKOH",
        trading_day=HOLDOUT_DAY,
        observed_at=observed_at,
        short_variance=2.0,
        medium_variance=4.0,
        long_variance=10.0,
        parameters=parameters,
        policy=policy,
    )
    h7v2 = relative_volume_activity_feature(
        ticker="MOEX",
        trading_day=HOLDOUT_DAY,
        observed_at=observed_at,
        current_volume=100.0,
        historical_phase_volumes=tuple(float(item) for item in range(1, 21)),
        baseline_future_variance=2.0,
        policy=policy,
    )
    features = (h10, h11, h15, h7v2)
    outcomes = (
        directional_outcome(
            h10,
            target_at=datetime(2026, 1, 9, 7, 31, tzinfo=UTC),
            forward_return_bps=-30.0,
            policy=policy,
        ),
        directional_outcome(
            h11,
            target_at=datetime(2026, 1, 9, 7, 16, tzinfo=UTC),
            forward_return_bps=-30.0,
            policy=policy,
        ),
        variance_outcome(
            h15,
            target_at=datetime(2026, 1, 9, 7, 31, tzinfo=UTC),
            actual_future_variance=2.0,
            policy=policy,
        ),
        variance_outcome(
            h7v2,
            target_at=datetime(2026, 1, 9, 7, 31, tzinfo=UTC),
            actual_future_variance=4.0,
            policy=policy,
        ),
    )
    return ScientificCandleResearchReport(
        dataset_fingerprint="sha256:" + "b" * 64,
        report_fingerprint="sha256:" + "a" * 64,
        split=ChronologicalSplit(
            train_days=(date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)),
            validation_days=(date(2026, 1, 8),),
            holdout_days=(HOLDOUT_DAY,),
        ),
        policy=policy,
        selected_hypotheses=tuple(ScientificCandleHypothesis),
        har_parameters=parameters,
        features=features,
        outcomes=outcomes,
    )


def test_adapter_writes_deterministic_holdout_evidence(tmp_path: Path) -> None:
    adapter = ScientificCandleReplayArtifactAdapter(tmp_path)
    report = _report()

    first = adapter.save(
        report,
        tuple(reversed(tuple(ScientificCandleHypothesis))),
        cost_model_version="cost-v1",
    )
    second = adapter.save(
        report,
        tuple(ScientificCandleHypothesis),
        cost_model_version="cost-v1",
    )
    other_cost_version = adapter.save(
        report,
        tuple(ScientificCandleHypothesis),
        cost_model_version="cost-v2",
    )

    assert first == second
    assert other_cost_version.artifact_uri != first.artifact_uri
    assert first.artifact_fingerprint.startswith("sha256:")
    assert first.artifact_fingerprint != report.report_fingerprint
    assert [item["hypothesis_id"] for item in first.evidence] == [
        "H10",
        "H11",
        "H15",
        "H7V2",
    ]
    assert all(item["decision"] == "inconclusive" for item in first.evidence)
    assert all(item["independent_validation"] is True for item in first.evidence)
    assert all(item["sample_count"] == 1 for item in first.evidence)
    assert all(
        item["artifact_fingerprint"] == first.artifact_fingerprint
        for item in first.evidence
    )
    assert all(
        item["evidence_scope"] == "descriptive_only"
        for row in first.evidence
        for item in row["horizons"]
    )
    by_id = {item["hypothesis_id"]: item for item in first.evidence}
    assert by_id["H10"]["primary_metric_value"] == pytest.approx(20.0)
    assert by_id["H11"]["primary_metric_value"] == pytest.approx(20.0)
    assert by_id["H15"]["primary_metric_value"] > 0.0
    assert by_id["H7V2"]["primary_metric_value"] == pytest.approx(2.0)
    assert all(
        ReplayEvidenceResponse.model_validate(row).hypothesis_id == row["hypothesis_id"]
        for row in first.evidence
    )
    stored = json.loads((Path(first.artifact_uri) / "evidence.json").read_text())
    assert stored == json.loads(json.dumps(first.evidence))


def test_adapter_never_promotes_descriptive_rows_to_passed(tmp_path: Path) -> None:
    artifact = ScientificCandleReplayArtifactAdapter(tmp_path).save(
        _report(),
        (ScientificCandleHypothesis.OPENING_GAP_REVERSION,),
        cost_model_version="cost-v1",
    )

    row = artifact.evidence[0]
    assert row["decision"] == "inconclusive"
    assert row["matched_controls"] == 0
    assert row["matched_control_lift_ci95_lower"] is None
    assert row["adjusted_p_value"] is None
