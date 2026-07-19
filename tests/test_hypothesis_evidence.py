from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from itertools import chain

import pytest

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildChronologicalSplit,
    BuildMatchedControls,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    DatasetPartition,
    EvidenceDecision,
    MatchedControlGroup,
    MatchedControlsResult,
    StudyPoint,
    benjamini_hochberg,
    day_block_bootstrap_interval,
    five_block_stability,
    one_sided_sign_test_p_value,
    wilson_interval,
)


UTC = timezone.utc
START = date(2026, 1, 5)


def _point(
    point_id: str,
    *,
    day_offset: int = 0,
    seconds: int = 0,
    instrument: str = "SBER",
    scenario: str | None = None,
    nearby: tuple[str, ...] = (),
    session: str = "main-open",
    volatility: str = "medium",
    liquidity: str = "high",
    partition: DatasetPartition = DatasetPartition.HOLDOUT,
    effect: float = 10.0,
    cost_version: str = "cost-v1",
    feature_offset_seconds: int = -1,
) -> StudyPoint:
    trading_day = START + timedelta(days=day_offset)
    occurred_at = datetime.combine(
        trading_day,
        datetime.min.time(),
        tzinfo=UTC,
    ) + timedelta(hours=10, seconds=seconds)
    return StudyPoint(
        point_id=point_id,
        scenario_id=scenario,
        instrument_id=instrument,
        occurred_at=occurred_at,
        trading_day=trading_day,
        session_bucket=session,
        volatility_bucket=volatility,
        liquidity_bucket=liquidity,
        features_observed_at=occurred_at + timedelta(seconds=feature_offset_seconds),
        partition=partition,
        net_effect_bps=effect,
        cost_model_version=cost_version,
        nearby_scenario_ids=nearby,
    )


def _group(
    index: int,
    *,
    effect: float,
    instrument: str,
    day_offset: int,
    cost_version: str = "cost-v1",
) -> MatchedControlGroup:
    event = _point(
        f"event-{index}",
        day_offset=day_offset,
        seconds=index,
        instrument=instrument,
        scenario="h1",
        effect=effect,
        cost_version=cost_version,
    )
    controls = tuple(
        _point(
            f"control-{index}-{control_index}",
            day_offset=day_offset,
            seconds=index + control_index + 1000,
            instrument=instrument,
            effect=0.0,
            cost_version=cost_version,
        )
        for control_index in range(5)
    )
    return MatchedControlGroup(event=event, controls=controls)


def _request(
    hypothesis_id: str,
    effects_by_day: list[float],
    *,
    events_per_day: int = 10,
    instruments: tuple[str, ...] = ("SBER", "GAZP", "LKOH"),
) -> EvidenceRequest:
    groups = tuple(
        _group(
            day_index * events_per_day + event_index,
            effect=effect,
            instrument=instruments[(day_index * events_per_day + event_index) % len(instruments)],
            day_offset=day_index,
        )
        for day_index, effect in enumerate(effects_by_day)
        for event_index in range(events_per_day)
    )
    return EvidenceRequest(
        hypothesis_id=hypothesis_id,
        hypothesis_version="1.0.0",
        dataset_fingerprint="sha256:dataset",
        groups=groups,
        expected_eligible_events=len(groups),
    )


def _fast_policy(**changes: object) -> EvidenceGatePolicy:
    return replace(
        EvidenceGatePolicy(),
        bootstrap_samples=250,
        **changes,
    )


def test_split_is_strictly_chronological_60_20_20_by_day() -> None:
    days = [START + timedelta(days=index) for index in range(30)]
    split = BuildChronologicalSplit().execute(tuple(reversed(days)) + (days[0],))

    assert split.train_days == tuple(days[:18])
    assert split.validation_days == tuple(days[18:24])
    assert split.holdout_days == tuple(days[24:])
    assert split.partition_for(days[17]) is DatasetPartition.TRAIN
    assert split.partition_for(days[18]) is DatasetPartition.VALIDATION
    assert split.partition_for(days[24]) is DatasetPartition.HOLDOUT
    assert not (set(split.train_days) & set(split.validation_days))
    assert not (set(split.validation_days) & set(split.holdout_days))


def test_features_after_event_are_rejected_as_leakage() -> None:
    with pytest.raises(ValueError, match="must not use information after"):
        _point("leaked", feature_offset_seconds=1)


def test_matched_controls_require_all_strata_and_never_reuse_controls() -> None:
    events = (
        _point("event-1", scenario="h1", seconds=1),
        _point("event-2", scenario="h1", seconds=2),
    )
    valid = tuple(_point(f"candidate-{index}", seconds=100 + index) for index in range(10))
    wrong_strata = (
        _point("wrong-session", session="closing"),
        _point("wrong-volatility", volatility="high"),
        _point("wrong-liquidity", liquidity="low"),
        _point("wrong-instrument", instrument="GAZP"),
        _point("wrong-partition", partition=DatasetPartition.VALIDATION),
    )

    result = BuildMatchedControls().execute(events, valid + wrong_strata)

    assert len(result.groups) == 2
    assert result.unmatched_event_ids == ()
    ids = [control.point_id for group in result.groups for control in group.controls]
    assert len(ids) == 10
    assert len(set(ids)) == 10
    assert set(ids) == {item.point_id for item in valid}


def test_controls_exclude_target_events_and_same_scenario_window() -> None:
    event = _point("event", scenario="h1", seconds=0)
    contaminated = (
        _point("near-same", scenario="h1", seconds=60),
        _point("marked-near", seconds=500, nearby=("h1",)),
    )
    clean = tuple(_point(f"clean-{index}", seconds=700 + index) for index in range(5))

    result = BuildMatchedControls().execute((event,), (event,) + contaminated + clean)

    assert tuple(item.point_id for item in result.groups[0].controls) == tuple(
        item.point_id for item in clean
    )


def test_incomplete_control_set_is_reported_and_never_partially_emitted() -> None:
    event = _point("event", scenario="h1")
    candidates = tuple(_point(f"control-{index}") for index in range(4))

    result = BuildMatchedControls().execute((event,), candidates)

    assert result.groups == ()
    assert result.unmatched_event_ids == ("event",)


def test_matching_is_independent_of_input_order() -> None:
    events = (_point("event-1", seconds=1), _point("event-2", seconds=2))
    candidates = tuple(_point(f"candidate-{index}", seconds=100 + index) for index in range(10))
    builder = BuildMatchedControls()

    first = builder.execute(events, candidates)
    second = builder.execute(tuple(reversed(events)), tuple(reversed(candidates)))

    assert first == second


def test_matched_group_rejects_partition_and_scenario_overlap() -> None:
    event = _point("event", scenario="h1")
    cross_partition = _point("cross", partition=DatasetPartition.VALIDATION)
    with pytest.raises(ValueError, match="matching strata"):
        MatchedControlGroup(event=event, controls=(cross_partition,))

    contaminated = _point("contaminated", nearby=("h1",))
    with pytest.raises(ValueError, match="scenario exclusion"):
        MatchedControlGroup(event=event, controls=(contaminated,))


def test_result_object_rejects_control_reuse_across_events() -> None:
    shared = tuple(_point(f"shared-{index}") for index in range(5))
    groups = (
        MatchedControlGroup(event=_point("event-1"), controls=shared),
        MatchedControlGroup(event=_point("event-2"), controls=shared),
    )
    with pytest.raises(ValueError, match="must not be reused"):
        MatchedControlsResult(groups=groups, unmatched_event_ids=(), controls_per_event=5)


def test_wilson_interval_matches_known_half_success_case() -> None:
    interval = wilson_interval(50, 100)

    assert interval.estimate == 0.5
    assert interval.lower == pytest.approx(0.4038, abs=0.0001)
    assert interval.upper == pytest.approx(0.5962, abs=0.0001)


def test_day_block_bootstrap_is_deterministic_and_keeps_point_estimate() -> None:
    values = {
        START: (1.0, 2.0),
        START + timedelta(days=1): (10.0,),
        START + timedelta(days=2): (-2.0, -1.0, 0.0),
    }

    first = day_block_bootstrap_interval(values, samples=500, seed=71)
    second = day_block_bootstrap_interval(values, samples=500, seed=71)

    assert first == second
    assert first.estimate == pytest.approx(sum(chain.from_iterable(values.values())) / 6)


def test_benjamini_hochberg_applies_monotone_fdr_adjustment() -> None:
    results = {
        item.test_id: item
        for item in benjamini_hochberg(
            {"h1": 0.001, "h2": 0.01, "h3": 0.04, "h4": 0.20}
        )
    }

    assert results["h1"].q_value == pytest.approx(0.004)
    assert results["h2"].q_value == pytest.approx(0.02)
    assert results["h3"].q_value == pytest.approx(0.0533333333)
    assert results["h4"].q_value == pytest.approx(0.20)
    assert results["h1"].significant is True
    assert results["h2"].significant is True
    assert results["h3"].significant is False


def test_exact_sign_test_is_one_sided_and_deterministic() -> None:
    assert one_sided_sign_test_p_value(10, 10) == pytest.approx(1 / 1024)
    assert one_sided_sign_test_p_value(5, 10) == pytest.approx(0.623046875)


def test_stability_requires_four_of_five_chronological_blocks() -> None:
    four_positive = {
        START + timedelta(days=index): ((1.0,) if index < 4 else (-1.0,))
        for index in range(5)
    }
    three_positive = dict(four_positive)
    three_positive[START + timedelta(days=3)] = (-1.0,)

    stable = five_block_stability(four_positive)
    unstable = five_block_stability(three_positive)

    assert stable.assessed is True
    assert stable.stable is True
    assert stable.positive_blocks == 4
    assert tuple(block.block_number for block in stable.blocks) == (1, 2, 3, 4, 5)
    assert unstable.stable is False
    assert unstable.positive_blocks == 3


def test_portfolio_passes_robust_cost_adjusted_effect() -> None:
    request = _request("h-positive", [10.0] * 30)

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.PASSED
    assert bundle.reason_codes == ()
    assert bundle.trading_days == 30
    assert bundle.eligible_events == 300
    assert bundle.matched_controls == 1500
    assert bundle.cost_model_version == "cost-v1"
    assert bundle.mean_lift_bps == pytest.approx(10.0)
    assert bundle.lift_interval is not None
    assert bundle.lift_interval.lower > 0.0
    assert bundle.adjusted_q_value is not None
    assert bundle.adjusted_q_value <= 0.05
    assert bundle.fdr_significant is True
    assert bundle.stability.stable is True
    assert bundle.maximum_instrument_share == pytest.approx(1 / 3)


def test_negative_result_is_persisted_as_rejected_evidence_bundle() -> None:
    request = _request("h-negative", [-10.0] * 30)

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.REJECTED
    assert bundle.reason_codes == ("effect_rejected_on_holdout",)
    assert bundle.mean_lift_bps == pytest.approx(-10.0)
    assert bundle.lift_interval is not None
    assert bundle.lift_interval.upper < 0.0
    assert bundle.evidence_id.startswith("sha256:")


def test_mixed_result_is_retained_as_inconclusive() -> None:
    request = _request("h-mixed", [2.0] * 15 + [-2.0] * 15)

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.INCONCLUSIVE
    assert "positive_lower_confidence_bound_not_met" in bundle.reason_codes
    assert "multiple_testing_gate_not_met" in bundle.reason_codes
    assert bundle.mean_lift_bps == pytest.approx(0.0)


def test_insufficient_data_is_retained_as_blocked_bundle() -> None:
    request = _request("h-small", [5.0] * 5, events_per_day=2)

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert {
        "minimum_eligible_events_not_met",
        "minimum_trading_days_not_met",
    } <= set(bundle.reason_codes)
    assert bundle.eligible_events == 10
    assert bundle.matched_events == 10
    assert bundle.raw_p_value is None


def test_low_coverage_blocks_an_otherwise_positive_result() -> None:
    request = replace(
        _request("h-low-coverage", [5.0] * 30),
        total_available_observations=3_001,
    )

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert bundle.reason_codes == ("minimum_coverage_not_met",)
    assert bundle.eligible_events == 300
    assert bundle.raw_p_value is None


def test_incomplete_controls_and_mixed_cost_versions_block_the_gate() -> None:
    request = _request("h-costs", [5.0] * 30)
    groups = list(request.groups)
    groups[-1] = _group(
        9999,
        effect=5.0,
        instrument="SBER",
        day_offset=29,
        cost_version="cost-v2",
    )
    blocked = replace(
        request,
        groups=tuple(groups),
        unmatched_event_ids=("missing-event",),
    )

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((blocked,))

    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert "matched_controls_incomplete" in bundle.reason_codes
    assert "versioned_cost_model_required" in bundle.reason_codes


def test_non_holdout_evidence_cannot_pass_independent_gate() -> None:
    request = _request("h-training-only", [5.0] * 30)
    training_groups = tuple(
        MatchedControlGroup(
            event=replace(group.event, partition=DatasetPartition.TRAIN),
            controls=tuple(
                replace(control, partition=DatasetPartition.TRAIN)
                for control in group.controls
            ),
        )
        for group in request.groups
    )

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute(
        (replace(request, groups=training_groups),)
    )

    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert "independent_holdout_required" in bundle.reason_codes


def test_single_ticker_concentration_blocks_otherwise_positive_evidence() -> None:
    request = _request("h-concentrated", [8.0] * 30, instruments=("SBER",))

    (bundle,) = AssessEvidencePortfolio(_fast_policy()).execute((request,))

    assert bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
    assert bundle.reason_codes == ("single_instrument_concentration_exceeded",)
    assert bundle.maximum_instrument_share == 1.0


def test_multiple_hypotheses_share_one_fdr_family_and_keep_all_results() -> None:
    positive = _request("h-positive", [9.0] * 30)
    mixed = _request("h-mixed", [1.0] * 15 + [-1.0] * 15)

    bundles = AssessEvidencePortfolio(_fast_policy()).execute((positive, mixed))
    by_id = {bundle.hypothesis_id: bundle for bundle in bundles}

    assert set(by_id) == {"h-positive", "h-mixed"}
    assert by_id["h-positive"].decision is EvidenceDecision.PASSED
    assert by_id["h-mixed"].decision is EvidenceDecision.INCONCLUSIVE
    assert by_id["h-positive"].adjusted_q_value is not None
    assert by_id["h-mixed"].adjusted_q_value is not None


def test_evidence_artifact_identity_is_reproducible() -> None:
    request = _request("h-reproducible", [7.0] * 30)
    assessor = AssessEvidencePortfolio(_fast_policy())

    first = assessor.execute((request,))[0]
    second = assessor.execute((request,))[0]

    assert first == second
    assert first.evidence_id == second.evidence_id
