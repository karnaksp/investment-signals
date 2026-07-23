from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.adapters.prospective_scientific_replay import (
    ProspectiveScientificReplayArtifactAdapter,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.prospective_scientific_evidence import (
    BOUNDED_CONTROL_REUSE_HYPOTHESES,
    BOUNDED_CONTROL_SELECTION_POLICY_VERSION,
    AssessProspectiveScientificEvidence,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    EvidenceDecision,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    HarV2Parameters,
    JumpHistoryPoint,
    ProspectiveFeature,
    ProspectiveDecision,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveScientificPolicy,
    downside_semivariance_feature,
    har_v2_feature,
    har_v2_outcome,
    directional_outcome,
    jump_regime_features,
    relative_volume_volatility_feature,
    variance_uplift_outcome,
    volatility_jump_feature,
)


MOSCOW = ZoneInfo("Europe/Moscow")
TRAIN_DAY = date(2025, 12, 29)
VALIDATION_DAY = date(2025, 12, 30)


def test_gate_uses_only_holdout_and_five_pre_outcome_controls() -> None:
    report, contaminated_id = _variance_portfolio_report(include_nearby=True)
    gate = AssessProspectiveScientificEvidence(_fast_policy())

    assessment = gate.execute(
        report,
        (
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ),
        cost_model_version="cost-v1",
    )

    for hypothesis in (
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
    ):
        request = assessment.request_for(hypothesis)
        coverage = assessment.coverage_for(hypothesis)
        diagnostics = assessment.for_hypothesis(hypothesis).diagnostics_v2
        assert len(request.groups) == 8
        assert diagnostics is not None
        assert diagnostics.version == "evidence-diagnostics-v2"
        assert diagnostics.eligible_event_count == coverage.triggered_events
        assert (
            diagnostics.matched_event_count == coverage.eligible_common_support_events
        )
        assert diagnostics.match_coverage == pytest.approx(
            coverage.eligible_common_support_events / coverage.triggered_events
        )
        assert diagnostics.data_coverage == 1.0
        for group in request.groups:
            assert len(group.controls) == 5
            if (
                hypothesis is ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
                and group.event.trading_day == date(2026, 1, 5)
            ):
                assert contaminated_id not in {
                    control.point_id for control in group.controls
                }
            for point in (group.event, *group.controls):
                assert point.partition is DatasetPartition.HOLDOUT
                assert point.features_observed_at <= point.occurred_at
                assert point.matching_key == group.event.matching_key


def test_reports_can_be_prepared_one_at_a_time_before_one_global_fdr_pass() -> None:
    report, _ = _variance_portfolio_report(include_nearby=False)
    hypotheses = (
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
    )
    gate = AssessProspectiveScientificEvidence(_fast_policy())
    expected = gate.execute(report, hypotheses, cost_model_version="cost-v1")
    prepared = []
    for index, hypothesis in enumerate(hypotheses):
        selected = tuple(
            pair
            for pair in zip(report.features, report.outcomes, strict=True)
            if pair[0].hypothesis is hypothesis
        )
        one_hypothesis_report = replace(
            report,
            report_fingerprint=f"sha256:{index + 1:064x}",
            selected_hypotheses=(hypothesis,),
            features=tuple(pair[0] for pair in selected),
            outcomes=tuple(pair[1] for pair in selected),
        )
        prepared.append(gate.prepare(one_hypothesis_report, hypothesis, "cost-v1"))

    actual = gate.assess_prepared(prepared)

    assert actual == expected
    assert all(bundle.adjusted_q_value is not None for bundle in actual.bundles)


def test_assess_prepared_requires_a_non_empty_global_family() -> None:
    gate = AssessProspectiveScientificEvidence(_fast_policy())

    with pytest.raises(ValueError, match="at least one prepared hypothesis"):
        gate.assess_prepared(())


def test_fdr_is_shared_and_incomplete_controls_fail_closed() -> None:
    report, _ = _variance_portfolio_report(include_nearby=False)
    assessment = AssessProspectiveScientificEvidence(_fast_policy()).execute(
        report,
        (
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ),
        cost_model_version="cost-v1",
    )

    h7 = assessment.for_hypothesis(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3)
    h16 = assessment.for_hypothesis(ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK)
    h17 = assessment.for_hypothesis(ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE)
    assert h7.decision is EvidenceDecision.PASSED
    assert h16.decision is EvidenceDecision.PASSED
    assert h7.adjusted_q_value is not None
    assert h16.adjusted_q_value is not None
    assert h17.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert "matched_controls_incomplete" in h17.reason_codes
    assert (
        assessment.coverage_for(
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE
        ).unmatched_events
        == 1
    )


def test_selected_hypotheses_use_bounded_reuse_and_fail_closed_on_one_cluster(
    tmp_path: Path,
) -> None:
    expected = {
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
    }
    assert BOUNDED_CONTROL_REUSE_HYPOTHESES == expected
    report = _bounded_reuse_h7_report()
    gate_policy = EvidenceGatePolicy(
        minimum_trading_days=2,
        minimum_eligible_events=2,
        controls_per_event=5,
        bootstrap_samples=100,
        required_positive_stability_blocks=1,
        maximum_instrument_share=0.99,
        minimum_coverage=0.10,
    )
    assessment = AssessProspectiveScientificEvidence(gate_policy).execute(
        report,
        (ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,),
        cost_model_version="cost-v1",
    )
    request = assessment.request_for(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
    )
    coverage = assessment.coverage_for(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
    )
    bundle = assessment.for_hypothesis(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
    )

    assert len(request.groups) == 2
    assert all(len(set(group.controls)) == 5 for group in request.groups)
    assert coverage.control_selection_policy_version == (
        BOUNDED_CONTROL_SELECTION_POLICY_VERSION
    )
    assert coverage.maximum_control_reuse == 5
    assert coverage.distinct_controls == 5
    assert coverage.maximum_observed_control_reuse == 2
    assert coverage.mean_control_reuse == 2.0
    assert coverage.independent_control_clusters == 1
    assert coverage.minimum_independent_control_clusters == 2
    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert "minimum_independent_control_clusters_not_met" in bundle.reason_codes
    assert bundle.diagnostics_v2 is not None
    assert bundle.diagnostics_v2.primary_p_value == 0.5

    artifact = ProspectiveScientificReplayArtifactAdapter(
        tmp_path,
        evidence_policy=gate_policy,
    ).save(
        report,
        (ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,),
        cost_model_version="cost-v1",
    )
    matching = artifact.evidence[0]["control_matching"]
    assert matching == {
        "selection_policy_version": BOUNDED_CONTROL_SELECTION_POLICY_VERSION,
        "maximum_control_reuse": 5,
        "distinct_controls": 5,
        "maximum_observed_control_reuse": 2,
        "mean_control_reuse": 2.0,
        "independent_control_clusters": 1,
        "minimum_independent_control_clusters": 2,
        "inference_unit": "control_dependency_cluster",
    }
    manifest = json.loads(
        (Path(artifact.artifact_uri) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["bounded_control_reuse"]["selection_policy_version"] == (
        BOUNDED_CONTROL_SELECTION_POLICY_VERSION
    )
    assert manifest["evidence_coverage"]["H7V3"]["maximum_observed_control_reuse"] == 2


def test_h15_primary_effect_beats_best_and_therefore_mean_benchmark() -> None:
    report = _har_report()
    assessment = AssessProspectiveScientificEvidence(
        EvidenceGatePolicy(
            minimum_trading_days=1,
            minimum_eligible_events=1,
            controls_per_event=5,
            bootstrap_samples=100,
            required_positive_stability_blocks=1,
            maximum_instrument_share=0.99,
            minimum_coverage=0.10,
        )
    ).execute(
        report,
        (ProspectiveHypothesis.HAR_VOLATILITY_V2,),
        cost_model_version="cost-v1",
    )
    group = assessment.request_for(ProspectiveHypothesis.HAR_VOLATILITY_V2).groups[0]
    event_feature = next(
        feature
        for feature in report.features
        if feature.observation_id == group.event.point_id
    )
    event_outcome = next(
        outcome
        for outcome in report.outcomes
        if outcome.observation_id == event_feature.observation_id
    )
    har_loss = event_outcome.metric("har_qlike").value
    ewma_loss = event_outcome.metric("ewma_qlike").value
    phase_loss = event_outcome.metric("phase_qlike").value
    conservative = min(ewma_loss, phase_loss) - har_loss
    average = (ewma_loss + phase_loss) / 2.0 - har_loss

    assert group.event.net_effect_bps == pytest.approx(conservative * 10_000.0)
    assert conservative <= average


def test_h3_h4_effect_is_cost_adjusted_directional_basis_points() -> None:
    report = _jump_report()
    assessment = AssessProspectiveScientificEvidence(
        EvidenceGatePolicy(
            minimum_trading_days=1,
            minimum_eligible_events=1,
            controls_per_event=5,
            bootstrap_samples=100,
            required_positive_stability_blocks=1,
            maximum_instrument_share=0.99,
            minimum_coverage=0.10,
        )
    ).execute(
        report,
        (
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ),
        cost_model_version="cost-v1",
    )

    for hypothesis in (
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
    ):
        group = assessment.request_for(hypothesis).groups[0]
        assert group.event.net_effect_bps == 10.0
        assert all(control.net_effect_bps == -10.0 for control in group.controls)
        assert group.lift_bps == 20.0


def test_adapter_is_deterministic_typed_and_immutable(tmp_path: Path) -> None:
    report, _ = _variance_portfolio_report(include_nearby=False)
    adapter = ProspectiveScientificReplayArtifactAdapter(
        tmp_path,
        evidence_policy=_fast_policy(),
    )
    selected = (
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
    )

    first = adapter.save(report, selected, cost_model_version="cost-v1")
    second = adapter.save(
        report, tuple(reversed(selected)), cost_model_version="cost-v1"
    )

    assert first == second
    by_id = {str(row["hypothesis_id"]): row for row in first.evidence}
    assert by_id["H7V3"]["claim_family"] == "activity"
    assert by_id["H7V3"]["effect_unit"] == "variance_uplift_ratio_x_10000"
    assert by_id["H7V3"]["target_metric"] == "future_variance_uplift"
    assert by_id["H7V3"]["claim_scope"] == ("independent_holdout_matched_controls")
    assert by_id["H17"]["decision"] == "blocked_by_data"
    complete_diagnostics = by_id["H7V3"]["diagnostics_v2"]
    assert complete_diagnostics == {
        "version": "evidence-diagnostics-v2",
        "event_prevalence": 8 / 48,
        "eligible_event_count": 8,
        "matched_event_count": 8,
        "match_coverage": 1.0,
        "data_coverage": 1.0,
        "reasons_histogram": ({"reason_code": "event_condition_not_met", "count": 40},),
        "primary_effect_estimate": 10_000.0,
        "primary_effect_interval": {
            "lower": 10_000.0,
            "estimate": 10_000.0,
            "upper": 10_000.0,
            "confidence_level": 0.95,
        },
        "primary_p_value": 0.00390625,
        "descriptive_only": False,
    }
    blocked_diagnostics = by_id["H17"]["diagnostics_v2"]
    assert blocked_diagnostics["version"] == "evidence-diagnostics-v2"
    assert blocked_diagnostics["event_prevalence"] == 0.2
    assert blocked_diagnostics["eligible_event_count"] == 1
    assert blocked_diagnostics["matched_event_count"] == 0
    assert blocked_diagnostics["match_coverage"] == 0.0
    assert blocked_diagnostics["data_coverage"] == 1.0
    assert blocked_diagnostics["primary_effect_estimate"] is None
    assert blocked_diagnostics["primary_effect_interval"] is None
    assert blocked_diagnostics["primary_p_value"] is None
    assert blocked_diagnostics["descriptive_only"] is True
    assert tuple(
        item["reason_code"] for item in blocked_diagnostics["reasons_histogram"]
    ) == tuple(
        sorted(item["reason_code"] for item in blocked_diagnostics["reasons_histogram"])
    )
    evidence_path = Path(first.artifact_uri) / "evidence.json"
    expected_bytes = (
        json.dumps(
            first.evidence,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    assert evidence_path.read_bytes() == expected_bytes
    stored = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert stored == json.loads(json.dumps(first.evidence))
    manifest = json.loads(
        (Path(first.artifact_uri) / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["artifact_schema"] == "prospective-scientific-evidence-v1.1.0"

    evidence_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="immutable prospective replay artifact"):
        adapter.save(report, selected, cost_model_version="cost-v1")


def test_sequential_portfolio_matches_single_full_report_assessment(
    tmp_path: Path,
) -> None:
    report, _ = _variance_portfolio_report(include_nearby=False)
    selected = (
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
    )
    full = ProspectiveScientificReplayArtifactAdapter(
        tmp_path / "full",
        evidence_policy=_fast_policy(),
    ).save(report, selected, cost_model_version="cost-v1")

    def reports():
        for index, hypothesis in enumerate(selected):
            pairs = tuple(
                (feature, outcome)
                for feature, outcome in zip(
                    report.features, report.outcomes, strict=True
                )
                if feature.hypothesis is hypothesis
            )
            yield replace(
                report,
                report_fingerprint="sha256:" + f"{index + 1:x}" * 64,
                selected_hypotheses=(hypothesis,),
                features=tuple(item[0] for item in pairs),
                outcomes=tuple(item[1] for item in pairs),
            )

    sequential = ProspectiveScientificReplayArtifactAdapter(
        tmp_path / "sequential",
        evidence_policy=_fast_policy(),
    ).save_portfolio(reports(), selected, cost_model_version="cost-v1")

    excluded = {"artifact_fingerprint", "generated_at"}
    assert tuple(
        {key: value for key, value in row.items() if key not in excluded}
        for row in sequential.evidence
    ) == tuple(
        {key: value for key, value in row.items() if key not in excluded}
        for row in full.evidence
    )


def test_adapter_preserves_rejected_and_inconclusive_results(tmp_path: Path) -> None:
    report, _ = _variance_portfolio_report(include_nearby=False)
    h7_event_number = 0
    changed: list[ProspectiveOutcome] = []
    for feature, outcome in zip(report.features, report.outcomes, strict=True):
        if (
            feature.hypothesis is ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK
            and feature.decision is ProspectiveDecision.MATCHED
        ):
            changed.append(
                variance_uplift_outcome(
                    feature,
                    target_at=outcome.target_at,
                    actual_future_variance=0.5,
                )
            )
        elif (
            feature.hypothesis is ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
            and feature.decision is ProspectiveDecision.MATCHED
        ):
            h7_event_number += 1
            changed.append(
                variance_uplift_outcome(
                    feature,
                    target_at=outcome.target_at,
                    actual_future_variance=(2.0 if h7_event_number <= 4 else 0.5),
                )
            )
        else:
            changed.append(outcome)
    changed_report = replace(
        report,
        report_fingerprint="sha256:" + "f" * 64,
        outcomes=tuple(changed),
    )
    artifact = ProspectiveScientificReplayArtifactAdapter(
        tmp_path,
        evidence_policy=_fast_policy(),
    ).save(
        changed_report,
        (
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ),
        cost_model_version="cost-v1",
    )
    decisions = {
        str(row["hypothesis_id"]): row["decision"] for row in artifact.evidence
    }

    assert decisions["H16"] == "rejected"
    assert decisions["H7V3"] == "inconclusive"


def _fast_policy() -> EvidenceGatePolicy:
    return EvidenceGatePolicy(
        minimum_trading_days=8,
        minimum_eligible_events=8,
        controls_per_event=5,
        bootstrap_samples=300,
        bootstrap_seed=29,
        false_discovery_rate=0.05,
        required_positive_stability_blocks=4,
        maximum_instrument_share=0.75,
        minimum_coverage=0.10,
    )


def _variance_portfolio_report(
    *, include_nearby: bool
) -> tuple[ProspectiveScientificReport, str]:
    policy = ProspectiveScientificPolicy(
        volume_history_days=1,
        semivariance_history_days=1,
        jump_variance_history_days=1,
    )
    mondays = tuple(date(2026, 1, 5) + timedelta(weeks=index) for index in range(48))
    pairs: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    event_days = {
        "SBER": frozenset(mondays[:4]),
        "GAZP": frozenset(mondays[24:28]),
    }
    active_days = {
        "SBER": mondays[:24],
        "GAZP": mondays[24:],
    }
    for ticker in ("SBER", "GAZP"):
        for trading_day in active_days[ticker]:
            event = trading_day in event_days[ticker]
            observed_at = _at(trading_day)
            pairs.extend(
                _variance_pairs(
                    ticker,
                    trading_day,
                    observed_at,
                    event=event,
                    policy=policy,
                    hypotheses=(
                        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
                    ),
                )
            )

    h17_day = mondays[0]
    h17_event = _variance_pairs(
        "LKOH",
        h17_day,
        _at(h17_day),
        event=True,
        policy=policy,
        hypotheses=(ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,),
    )
    pairs.extend(h17_event)
    for trading_day in mondays[1:5]:
        pairs.extend(
            _variance_pairs(
                "LKOH",
                trading_day,
                _at(trading_day),
                event=False,
                policy=policy,
                hypotheses=(ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,),
            )
        )

    contaminated_id = ""
    if include_nearby:
        contaminated = _variance_pairs(
            "SBER",
            mondays[0],
            _at(mondays[0]) + timedelta(minutes=4),
            event=False,
            policy=policy,
            hypotheses=(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,),
        )[0]
        contaminated_id = contaminated[0].observation_id
        pairs.append(contaminated)

    pairs.sort(
        key=lambda pair: (
            pair[0].observed_at,
            pair[0].ticker,
            pair[0].hypothesis.value,
        )
    )
    return (
        ProspectiveScientificReport(
            dataset_fingerprint="sha256:" + "d" * 64,
            report_fingerprint="sha256:" + "e" * 64,
            split=ChronologicalSplit(
                train_days=(TRAIN_DAY,),
                validation_days=(VALIDATION_DAY,),
                holdout_days=mondays,
            ),
            policy=policy,
            selected_hypotheses=(
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
            ),
            har_v2_parameters=None,
            features=tuple(pair[0] for pair in pairs),
            outcomes=tuple(pair[1] for pair in pairs),
        ),
        contaminated_id,
    )


def _bounded_reuse_h7_report() -> ProspectiveScientificReport:
    policy = ProspectiveScientificPolicy(volume_history_days=1)
    mondays = tuple(date(2026, 1, 5) + timedelta(weeks=index) for index in range(7))
    pairs = [
        _variance_pairs(
            "SBER",
            trading_day,
            _at(trading_day),
            event=index < 2,
            policy=policy,
            hypotheses=(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,),
        )[0]
        for index, trading_day in enumerate(mondays)
    ]
    return ProspectiveScientificReport(
        dataset_fingerprint="sha256:" + "1" * 64,
        report_fingerprint="sha256:" + "2" * 64,
        split=ChronologicalSplit(
            train_days=(TRAIN_DAY,),
            validation_days=(VALIDATION_DAY,),
            holdout_days=mondays,
        ),
        policy=policy,
        selected_hypotheses=(ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,),
        har_v2_parameters=None,
        features=tuple(pair[0] for pair in pairs),
        outcomes=tuple(pair[1] for pair in pairs),
    )


def _variance_pairs(
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    *,
    event: bool,
    policy: ProspectiveScientificPolicy,
    hypotheses: tuple[ProspectiveHypothesis, ...],
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for hypothesis in hypotheses:
        if hypothesis is ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3:
            feature = relative_volume_volatility_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                current_volume=200.0 if event else 50.0,
                historical_volumes=(100.0,),
                baseline_future_variance=1.0,
                history_observed_until=observed_at - timedelta(days=1),
                trading_gap=False,
                policy=policy,
            )
        elif hypothesis is ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK:
            feature = downside_semivariance_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                downside_share=0.9 if event else 0.1,
                historical_downside_shares=(0.5,),
                baseline_future_variance=1.0,
                history_observed_until=observed_at - timedelta(days=1),
                trading_gap=False,
                policy=policy,
            )
        else:
            feature = volatility_jump_feature(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                jump_share=0.9 if event else 0.1,
                continuous_variance=1.0,
                historical_jump_shares=(0.5,),
                baseline_future_variance=1.0,
                history_observed_until=observed_at - timedelta(days=1),
                trading_gap=False,
                policy=policy,
            )
        outcome = variance_uplift_outcome(
            feature,
            target_at=observed_at + timedelta(minutes=30),
            actual_future_variance=2.0 if event else 1.0,
        )
        result.append((feature, outcome))
    return result


def _har_report() -> ProspectiveScientificReport:
    train_days = (date(2026, 1, 1),)
    validation_days = tuple(date(2026, 1, 2) + timedelta(days=i) for i in range(10))
    holdout_days = tuple(date(2026, 1, 12) + timedelta(weeks=i) for i in range(6))
    policy = ProspectiveScientificPolicy(har_minimum_training_points=1)
    parameters = HarV2Parameters(
        intercept=0.0,
        short_weight=1.0,
        medium_weight=0.0,
        long_weight=0.0,
        training_points=100,
        trained_until=_at(train_days[0]) - timedelta(hours=1),
    )
    pairs: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for index, trading_day in enumerate(validation_days, start=1):
        pairs.append(_har_pair(trading_day, float(index), parameters, policy))
    for index, trading_day in enumerate(holdout_days):
        pairs.append(
            _har_pair(
                trading_day,
                20.0 if index == 0 else 1.0,
                parameters,
                policy,
                event=index == 0,
            )
        )
    pairs.sort(key=lambda pair: pair[0].observed_at)
    return ProspectiveScientificReport(
        dataset_fingerprint="sha256:" + "a" * 64,
        report_fingerprint="sha256:" + "b" * 64,
        split=ChronologicalSplit(train_days, validation_days, holdout_days),
        policy=policy,
        selected_hypotheses=(ProspectiveHypothesis.HAR_VOLATILITY_V2,),
        har_v2_parameters=parameters,
        features=tuple(pair[0] for pair in pairs),
        outcomes=tuple(pair[1] for pair in pairs),
    )


def _har_pair(
    trading_day: date,
    forecast: float,
    parameters: HarV2Parameters,
    policy: ProspectiveScientificPolicy,
    *,
    event: bool = False,
) -> tuple[ProspectiveFeature, ProspectiveOutcome]:
    observed_at = _at(trading_day)
    feature = har_v2_feature(
        ticker="SBER",
        trading_day=trading_day,
        observed_at=observed_at,
        short_variance=forecast,
        medium_variance=1.0,
        long_variance=1.0,
        parameters=parameters,
        horizon_seconds=policy.har_horizon_seconds,
    )
    outcome = har_v2_outcome(
        feature,
        target_at=observed_at + timedelta(minutes=30),
        actual_future_variance=forecast,
        ewma_baseline=forecast / 2.0 if event else forecast,
        phase_baseline=forecast * 0.6 if event else forecast,
    )
    return feature, outcome


def _jump_report() -> ProspectiveScientificReport:
    days = tuple(date(2026, 1, 5) + timedelta(weeks=index) for index in range(6))
    policy = ProspectiveScientificPolicy(
        jump_history_days=1,
        jump_horizons_seconds=(300,),
        round_trip_cost_bps=10.0,
    )
    history = (
        JumpHistoryPoint(
            absolute_return_bps=10.0,
            volume=100.0,
            range_bps=10.0,
            illiquidity=10.0,
        ),
    )
    pairs: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for ticker, event_kind in (("SBER", "low"), ("GAZP", "high")):
        for index, trading_day in enumerate(days):
            observed_at = _at(trading_day)
            kind = event_kind if index == 0 else "neutral"
            volume = 50.0 if kind == "low" else 200.0 if kind == "high" else 100.0
            range_bps = 100.0 if kind in {"low", "high"} else 1.0
            illiquidity = 100.0 if kind == "low" else 1.0
            h3, h4 = jump_regime_features(
                ticker=ticker,
                trading_day=trading_day,
                observed_at=observed_at,
                horizon_seconds=300,
                signed_return_bps=100.0,
                volume=volume,
                range_bps=range_bps,
                illiquidity=illiquidity,
                prior_history=history,
                history_observed_until=observed_at - timedelta(days=1),
                trading_gap=False,
                policy=policy,
            )
            for feature in (h3, h4):
                expected_event = index == 0 and (
                    (ticker == "SBER" and feature.hypothesis.value == "H3V2")
                    or (ticker == "GAZP" and feature.hypothesis.value == "H4V2")
                )
                forward = (
                    -20.0
                    if expected_event and feature.expected_direction == -1
                    else 20.0
                    if expected_event
                    else 0.0
                )
                pairs.append(
                    (
                        feature,
                        directional_outcome(
                            feature,
                            target_at=observed_at + timedelta(minutes=5),
                            forward_return_bps=forward,
                            round_trip_cost_bps=policy.round_trip_cost_bps,
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
    return ProspectiveScientificReport(
        dataset_fingerprint="sha256:" + "7" * 64,
        report_fingerprint="sha256:" + "8" * 64,
        split=ChronologicalSplit(
            train_days=(TRAIN_DAY,),
            validation_days=(VALIDATION_DAY,),
            holdout_days=days,
        ),
        policy=policy,
        selected_hypotheses=(
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ),
        har_v2_parameters=None,
        features=tuple(pair[0] for pair in pairs),
        outcomes=tuple(pair[1] for pair in pairs),
    )


def _at(trading_day: date) -> datetime:
    return datetime.combine(trading_day, time(11, 0), tzinfo=MOSCOW)
