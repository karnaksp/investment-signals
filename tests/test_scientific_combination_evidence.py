from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from tinvest_signal_engine.adapters.file_scientific_combination_evidence import (
    FileScientificCombinationEvidenceArtifacts,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.application.scientific_combination_evidence import (
    CombinationStatisticalState,
    EvaluateScientificCombinationPortfolio,
    EvaluateScientificCombinationPortfolioRequest,
    StoreScientificCombinationPortfolio,
)
from tinvest_signal_engine.domain.hypothesis_evidence import ChronologicalSplit
from tinvest_signal_engine.domain.prospective_scientific_models import (
    MetricUnit,
    MetricValue,
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    TargetMetric,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    CombinationReason,
    ScientificCombinationId,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def test_c1_batch_compares_with_registered_standalone_basis_and_passes() -> None:
    report = _c1_report(event_forward_bps=30.0)

    portfolio = _evaluator().execute(_request(report))

    result = _result(portfolio, ScientificCombinationId.C1, 300)
    assert result.statistical_state is CombinationStatisticalState.PASSED
    assert result.comparison_hypotheses == (
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
    )
    assert result.coverage.eligible_events == 10
    assert result.coverage.matched_events == 10
    assert result.coverage.selective_coverage == 10 / 60
    assert len(result.control_matches) == 10
    assert all(len(item.standalone_observation_ids) == 5 for item in result.control_matches)
    assert all(item.standalone_mean_net_bps == 5.0 for item in result.control_matches)
    assert all(item.incremental_lift_bps == 25.0 for item in result.control_matches)


def test_default_batch_retains_every_registered_combination_and_horizon() -> None:
    report = _c1_report(event_forward_bps=30.0)

    portfolio = _evaluator().execute(
        EvaluateScientificCombinationPortfolioRequest(
            report=report,
            cost_model_version="cost-v1",
        )
    )

    assert tuple(
        (item.combination_id.value, item.horizon_seconds)
        for item in portfolio.results
    ) == (
        ("C1", 300),
        ("C1", 900),
        ("C2", 300),
        ("C2", 900),
        ("C3", 1800),
        ("C4", 900),
        ("C4", 1800),
        ("C4", 3600),
    )
    assert portfolio.results[0].statistical_state is CombinationStatisticalState.PASSED
    assert all(
        item.statistical_state is CombinationStatisticalState.BLOCKED_DATA
        for item in portfolio.results[1:]
    )


def test_small_selective_coverage_is_blocked_instead_of_claimed_as_quality() -> None:
    report = _c1_report(event_forward_bps=30.0, event_count_per_ticker=1)

    result = _result(
        _evaluator().execute(_request(report)),
        ScientificCombinationId.C1,
        300,
    )

    assert result.statistical_state is CombinationStatisticalState.BLOCKED_DATA
    assert "minimum_eligible_events_not_met" in result.evidence.reason_codes
    assert "minimum_coverage_not_met" in result.evidence.reason_codes
    assert result.evidence.mean_lift_bps is None


def test_future_context_is_never_used_and_forces_explicit_abstention() -> None:
    report = _c1_report(event_forward_bps=30.0, context_shift=timedelta(minutes=1))

    portfolio = _evaluator().execute(_request(report))

    c1 = tuple(
        item
        for item in portfolio.observations
        if item.combination_id is ScientificCombinationId.C1
        and item.horizon_seconds == 300
    )
    assert c1
    assert all(item.decision is ProspectiveDecision.ABSTAIN for item in c1)
    assert all(item.reason is CombinationReason.FUTURE_COMPONENT for item in c1)
    assert all(item.max_used_observed_at is None for item in c1)
    assert _result(
        portfolio, ScientificCombinationId.C1, 300
    ).statistical_state is CombinationStatisticalState.BLOCKED_DATA


def test_batch_and_statistics_are_deterministic_under_source_ordering() -> None:
    report = _c1_report(event_forward_bps=30.0)
    reversed_pairs = tuple(
        reversed(tuple(zip(report.features, report.outcomes, strict=True)))
    )
    reordered = replace(
        report,
        features=tuple(item[0] for item in reversed_pairs),
        outcomes=tuple(item[1] for item in reversed_pairs),
    )

    first = _evaluator().execute(_request(report))
    second = _evaluator().execute(_request(reordered))

    assert second == first
    assert second.portfolio_fingerprint == first.portfolio_fingerprint


def test_negative_result_is_rejected_and_persisted_as_immutable_artifact(
    tmp_path: Path,
) -> None:
    portfolio = _evaluator().execute(
        _request(_c1_report(event_forward_bps=-20.0))
    )
    result = _result(portfolio, ScientificCombinationId.C1, 300)

    assert result.statistical_state is CombinationStatisticalState.REJECTED
    assert result.evidence.mean_lift_bps == -25.0

    store = StoreScientificCombinationPortfolio(
        FileScientificCombinationEvidenceArtifacts(tmp_path)
    )
    first = store.execute(portfolio)
    repeated = store.execute(portfolio)

    assert repeated == first
    payload = json.loads(
        (Path(first.artifact_uri) / "results.json").read_text(encoding="utf-8")
    )
    c1_300 = next(
        item
        for item in payload
        if item["combination_id"] == "C1" and item["horizon_seconds"] == 300
    )
    assert c1_300["statistical_state"] == "rejected"
    assert c1_300["evidence"]["mean_lift_bps"] == -25.0


def _evaluator() -> EvaluateScientificCombinationPortfolio:
    return EvaluateScientificCombinationPortfolio(
        EvidenceGatePolicy(
            minimum_trading_days=5,
            minimum_eligible_events=10,
            controls_per_event=5,
            bootstrap_samples=100,
            bootstrap_seed=23,
            false_discovery_rate=0.05,
            required_positive_stability_blocks=4,
            maximum_instrument_share=0.75,
            minimum_coverage=0.10,
        )
    )


def _request(
    report: ProspectiveScientificReport,
) -> EvaluateScientificCombinationPortfolioRequest:
    return EvaluateScientificCombinationPortfolioRequest(
        report=report,
        cost_model_version="cost-v1",
        combination_ids=(ScientificCombinationId.C1,),
    )


def _result(portfolio, combination_id, horizon_seconds):
    return next(
        item
        for item in portfolio.results
        if item.combination_id is combination_id
        and item.horizon_seconds == horizon_seconds
    )


def _c1_report(
    *,
    event_forward_bps: float,
    event_count_per_ticker: int = 5,
    context_shift: timedelta = timedelta(0),
) -> ProspectiveScientificReport:
    days = _business_days(date(2025, 10, 1), 180)
    split = ChronologicalSplit(
        train_days=days[:120],
        validation_days=days[120:150],
        holdout_days=days[150:],
    )
    event_days = set(days[150 : 150 + event_count_per_ticker])
    features: list[ProspectiveFeature] = []
    outcomes: list[ProspectiveOutcome] = []
    for ticker in ("SBER", "GAZP"):
        for trading_day in days:
            observed_at = datetime.combine(trading_day, time(11, 0), MOSCOW)
            event = trading_day in event_days
            h4 = _feature(
                ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
                ticker=ticker,
                observed_at=observed_at,
                horizon_seconds=300,
                decision=ProspectiveDecision.MATCHED,
                direction=1,
            )
            h7 = _feature(
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                ticker=ticker,
                observed_at=observed_at + context_shift,
                horizon_seconds=1800,
                decision=(
                    ProspectiveDecision.MATCHED
                    if event
                    else ProspectiveDecision.NOT_MATCHED
                ),
                direction=0,
            )
            h17 = _feature(
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
                ticker=ticker,
                observed_at=observed_at + context_shift,
                horizon_seconds=1800,
                decision=(
                    ProspectiveDecision.MATCHED
                    if event
                    else ProspectiveDecision.NOT_MATCHED
                ),
                direction=0,
            )
            for feature in (h4, h7, h17):
                features.append(feature)
                if feature is h4:
                    forward = event_forward_bps if event else 5.0
                    measurements = (
                        MetricValue(
                            "forward_return",
                            MetricUnit.BASIS_POINTS,
                            forward,
                        ),
                    )
                else:
                    measurements = (
                        MetricValue(
                            "future_variance_uplift",
                            MetricUnit.RATIO,
                            1.0 if event else 0.0,
                        ),
                    )
                outcomes.append(
                    ProspectiveOutcome(
                        observation_id=feature.observation_id,
                        target_at=(
                            feature.observed_at
                            + timedelta(seconds=feature.horizon_seconds)
                        ),
                        available=True,
                        reason=feature.reason,
                        target=feature.target,
                        measurements=measurements,
                    )
                )
    ordered = tuple(
        sorted(
            zip(features, outcomes, strict=True),
            key=lambda item: (
                item[0].observed_at,
                item[0].ticker,
                item[0].hypothesis.value,
            ),
        )
    )
    return ProspectiveScientificReport(
        dataset_fingerprint="sha256:" + "1" * 64,
        report_fingerprint="sha256:" + "2" * 64,
        split=split,
        policy=ProspectiveScientificPolicy(round_trip_cost_bps=0.0),
        selected_hypotheses=(
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ),
        har_v2_parameters=None,
        features=tuple(item[0] for item in ordered),
        outcomes=tuple(item[1] for item in ordered),
    )


def _feature(
    hypothesis: ProspectiveHypothesis,
    *,
    ticker: str,
    observed_at: datetime,
    horizon_seconds: int,
    decision: ProspectiveDecision,
    direction: int,
) -> ProspectiveFeature:
    reason = (
        ProspectiveReason.CONDITIONS_MATCHED
        if decision is ProspectiveDecision.MATCHED
        else ProspectiveReason.CONDITIONS_NOT_MET
    )
    identity = "|".join(
        (
            hypothesis.value,
            ticker,
            observed_at.isoformat(),
            str(horizon_seconds),
            decision.value,
        )
    )
    target = (
        TargetMetric.FORWARD_RETURN
        if hypothesis
        is ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2
        else TargetMetric.FUTURE_VARIANCE_UPLIFT
    )
    values = (
        ()
        if target is TargetMetric.FORWARD_RETURN
        else (
            MetricValue(
                "baseline_future_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                1.0,
            ),
        )
    )
    return ProspectiveFeature(
        observation_id="sha256:" + sha256(identity.encode()).hexdigest(),
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=observed_at.date(),
        observed_at=observed_at,
        feature_max_observed_at=observed_at,
        history_observed_until=observed_at - timedelta(days=1),
        model_trained_until=None,
        horizon_seconds=horizon_seconds,
        target=target,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        forecast=None,
        feature_values=values,
    )


def _business_days(start: date, count: int) -> tuple[date, ...]:
    result: list[date] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)
