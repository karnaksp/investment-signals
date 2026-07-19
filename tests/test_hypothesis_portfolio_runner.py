from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    EvidenceGateAssessment,
    EvidenceGateDecision,
    EvidenceGatePolicyReference,
    EvidenceGateTier,
    HypothesisPortfolioSnapshot,
    PortfolioHypothesisRegistration,
    PortfolioItemExecutionError,
    PortfolioItemState,
    PortfolioRunState,
    ReplayHypothesisCommand,
    RunHypothesisPortfolio,
    RunHypothesisPortfolioRequest,
)
from tinvest_signal_engine.domain.scientific_hypotheses import (
    EvidenceLevel,
    HypothesisLifecycle,
    HypothesisOrigin,
    PreregisteredTest,
    ReplicationEvidence,
    ReplicationResult,
    ScientificHypothesis,
)


NOW = datetime(2026, 7, 19, tzinfo=timezone.utc)
DATASET = "sha256:dataset"
COST_MODEL = "costs-v1"


class _MemoryStore:
    def __init__(self) -> None:
        self.snapshots: dict[str, HypothesisPortfolioSnapshot] = {}

    def load(self, run_id: str) -> HypothesisPortfolioSnapshot | None:
        return self.snapshots.get(run_id)

    def save(
        self,
        snapshot: HypothesisPortfolioSnapshot,
        *,
        expected_revision: int | None,
    ) -> None:
        current = self.snapshots.get(snapshot.run_id)
        if expected_revision is None:
            if current is not None or snapshot.revision != 1:
                raise RuntimeError("initial revision conflict")
        elif current is None or current.revision != expected_revision:
            raise RuntimeError("revision conflict")
        elif snapshot.revision != expected_revision + 1:
            raise RuntimeError("revision must increase by one")
        self.snapshots[snapshot.run_id] = snapshot


class _ProgressRecorder:
    def __init__(self) -> None:
        self.snapshots: list[HypothesisPortfolioSnapshot] = []

    def publish(self, snapshot: HypothesisPortfolioSnapshot) -> None:
        self.snapshots.append(snapshot)


class _ScriptedReplay:
    def __init__(
        self, outcomes: dict[str, list[ReplicationEvidence | Exception]]
    ) -> None:
        self.outcomes = outcomes
        self.calls: list[ReplayHypothesisCommand] = []

    def replay(self, command: ReplayHypothesisCommand) -> ReplicationEvidence:
        self.calls.append(command)
        outcome = self.outcomes[command.replay_key].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _GateRecorder:
    def __init__(
        self,
        *,
        intermediate: EvidenceGateDecision = EvidenceGateDecision.PASSED,
        strict: EvidenceGateDecision = EvidenceGateDecision.INCONCLUSIVE,
    ) -> None:
        self.decisions = {
            EvidenceGateTier.INTERMEDIATE: intermediate,
            EvidenceGateTier.STRICT: strict,
        }
        self.calls: list[tuple[str, EvidenceGateTier, str]] = []

    def assess(
        self,
        *,
        tier: EvidenceGateTier,
        registration: PortfolioHypothesisRegistration,
        evidence: ReplicationEvidence,
        policy: EvidenceGatePolicyReference,
    ) -> EvidenceGateAssessment:
        self.calls.append((registration.item_key, tier, evidence.evidence_id))
        return EvidenceGateAssessment(
            tier=tier,
            decision=self.decisions[tier],
            policy_fingerprint=policy.fingerprint,
            reason_codes=("future_holdout_required",)
            if self.decisions[tier] is EvidenceGateDecision.INCONCLUSIVE
            else (),
        )


def _hypothesis(number: int, *, sealed: bool = True) -> ScientificHypothesis:
    hypothesis_id = f"h{number}-test-hypothesis"
    preregistration = PreregisteredTest.with_thresholds(
        registration_id=f"prereg-h{number}-v1",
        hypothesis_id=hypothesis_id,
        hypothesis_version="1.0.0",
        sealed_at=NOW if sealed else None,
        expected_direction="continuation",
        feature_definitions=("feature observed no later than event",),
        thresholds={"z_min": 2.0},
        market_phase="main_session",
        horizon_seconds=(300,),
        success_criterion="positive holdout lower confidence bound",
        abstention_conditions=("trading_gap",),
        cost_model_version=COST_MODEL,
        data_split_policy="60/20/20 chronological trading days",
        multiple_testing_policy="family correction",
    )
    return ScientificHypothesis(
        hypothesis_id=hypothesis_id,
        version="1.0.0",
        title=f"Hypothesis {number}",
        origin=HypothesisOrigin.RESEARCH_EXTENSION,
        source_ids=("source-1",),
        testable_statement="A sealed causal statement",
        economic_mechanism="A market mechanism",
        market_phase="main_session",
        trigger_conditions=("z >= 2",),
        expected_direction="continuation",
        horizon_seconds=(300,),
        abstention_conditions=("trading_gap",),
        falsification_criterion="holdout lower bound is not positive",
        original_market_result="published effect",
        evidence_level=EvidenceLevel.RESEARCHING,
        lifecycle=HypothesisLifecycle.PRE_REGISTERED,
        scientific_claim=True,
        preregistration=preregistration,
    )


def _registration(number: int, *, sealed: bool = True):
    return PortfolioHypothesisRegistration(
        replay_key=f"H{number}",
        hypothesis=_hypothesis(number, sealed=sealed),
        family_id="jump-family",
        primary_metric="cost_adjusted_return_bps",
        primary_horizon_seconds=300,
        intermediate_gate=EvidenceGatePolicyReference(
            policy_id="historical-candidate",
            version="1.0.0",
            fingerprint="sha256:intermediate",
        ),
        strict_gate=EvidenceGatePolicyReference(
            policy_id="strict-product",
            version="1.0.0",
            fingerprint="sha256:strict",
        ),
    )


def _request(*registrations: PortfolioHypothesisRegistration):
    return RunHypothesisPortfolioRequest(
        dataset_fingerprint=DATASET,
        cost_model_version=COST_MODEL,
        replay_engine_version="replay-v1",
        hypotheses=tuple(registrations),
    )


def _evidence(number: int, *, dataset: str = DATASET) -> ReplicationEvidence:
    return ReplicationEvidence(
        evidence_id=f"evidence-h{number}",
        hypothesis_id=f"h{number}-test-hypothesis",
        hypothesis_version="1.0.0",
        market="MOEX",
        observed_at=NOW,
        result=ReplicationResult.INCONCLUSIVE,
        independent_validation=False,
        trading_days=20,
        eligible_events=200,
        cost_adjusted=True,
        matched_controls_applied=True,
        multiple_testing_applied=True,
        stability_checked=True,
        mean_net_bps=2.0,
        result_summary="historical candidate only",
        artifact_uri=f"local://evidence/h{number}",
        primary_metric="cost_adjusted_return_bps",
        controls_per_event=5,
        lift_ci_lower=0.1,
        lift_ci_upper=3.0,
        adjusted_p_value=0.04,
        stable_blocks=3,
        total_blocks=5,
        max_ticker_share=0.30,
        max_period_share=0.35,
        dataset_fingerprint=dataset,
        formula_fingerprint=f"sha256:formula-h{number}",
        cost_model_version=COST_MODEL,
        abstention_rate=0.70,
        success_rate=0.65,
        success_wilson_lower=0.60,
    )


def _runner(replay: _ScriptedReplay, gates: _GateRecorder):
    store = _MemoryStore()
    progress = _ProgressRecorder()
    return (
        RunHypothesisPortfolio(
            replay=replay,
            evidence_gates=gates,
            store=store,
            progress=progress,
        ),
        store,
        progress,
    )


def test_portfolio_runs_all_items_and_persists_common_progress() -> None:
    replay = _ScriptedReplay({"H3": [_evidence(3)], "H4": [_evidence(4)]})
    gates = _GateRecorder()
    runner, store, progress = _runner(replay, gates)

    execution = runner.execute(_request(_registration(4), _registration(3)))

    assert execution.resumed is False
    assert execution.executed_item_keys == (
        "h3-test-hypothesis@1.0.0",
        "h4-test-hypothesis@1.0.0",
    )
    assert execution.snapshot.state is PortfolioRunState.COMPLETED
    assert execution.snapshot.progress.completed == 2
    assert execution.snapshot.progress.fraction == 1.0
    assert all(
        item.strict_assessment is not None
        and item.strict_assessment.decision is EvidenceGateDecision.INCONCLUSIVE
        for item in execution.snapshot.items
    )
    assert len(gates.calls) == 4
    assert progress.snapshots[0].progress.pending == 2
    assert progress.snapshots[-1].state is PortfolioRunState.COMPLETED
    assert store.load(execution.snapshot.run_id) == execution.snapshot


def test_resume_retries_only_failed_item_and_keeps_partial_result() -> None:
    replay = _ScriptedReplay(
        {
            "H3": [_evidence(3)],
            "H4": [PortfolioItemExecutionError("temporary_replay_failure"), _evidence(4)],
        }
    )
    gates = _GateRecorder()
    runner, _, _ = _runner(replay, gates)
    request = _request(_registration(3), _registration(4))

    first = runner.execute(request)

    assert first.snapshot.state is PortfolioRunState.PARTIAL
    first_items = {item.replay_key: item for item in first.snapshot.items}
    assert first_items["H3"].state is PortfolioItemState.COMPLETED
    assert first_items["H3"].evidence == _evidence(3)
    assert first_items["H4"].state is PortfolioItemState.FAILED
    assert first_items["H4"].failure_code == "temporary_replay_failure"

    second = runner.execute(request)

    assert second.resumed is True
    assert second.executed_item_keys == ("h4-test-hypothesis@1.0.0",)
    assert second.snapshot.state is PortfolioRunState.COMPLETED
    second_items = {item.replay_key: item for item in second.snapshot.items}
    assert second_items["H3"].attempts == 1
    assert second_items["H4"].attempts == 2
    assert [call.replay_key for call in replay.calls] == ["H3", "H4", "H4"]


def test_completed_portfolio_is_idempotent() -> None:
    replay = _ScriptedReplay({"H3": [_evidence(3)]})
    gates = _GateRecorder()
    runner, _, _ = _runner(replay, gates)
    request = _request(_registration(3))
    first = runner.execute(request)

    second = runner.execute(request)

    assert second.resumed is True
    assert second.executed_item_keys == ()
    assert second.snapshot == first.snapshot
    assert len(replay.calls) == 1
    assert len(gates.calls) == 2


def test_portfolio_fingerprint_is_order_independent_and_policy_sensitive() -> None:
    h3 = _registration(3)
    h4 = _registration(4)
    first = _request(h3, h4)
    reordered = _request(h4, h3)

    assert first.input_fingerprint == reordered.input_fingerprint
    assert first.run_id == reordered.run_id

    changed_policy = replace(
        h3,
        intermediate_gate=replace(
            h3.intermediate_gate,
            fingerprint="sha256:changed-intermediate-policy",
        ),
    )
    changed = _request(changed_policy, h4)
    assert changed.input_fingerprint != first.input_fingerprint
    assert changed.run_id != first.run_id


def test_unsealed_hypothesis_cannot_enter_portfolio() -> None:
    with pytest.raises(ValueError, match="preregistered and sealed"):
        _registration(3, sealed=False)


def test_registration_rejects_parameters_changed_after_sealing() -> None:
    hypothesis = replace(_hypothesis(3), expected_direction="reversal")

    with pytest.raises(ValueError, match="parameters do not match"):
        replace(_registration(3), hypothesis=hypothesis)


def test_replay_contract_mismatch_is_a_retryable_partial_failure() -> None:
    replay = _ScriptedReplay({"H3": [_evidence(3, dataset="sha256:other")]})
    gates = _GateRecorder()
    runner, _, _ = _runner(replay, gates)

    result = runner.execute(_request(_registration(3)))

    assert result.snapshot.state is PortfolioRunState.PARTIAL
    assert result.snapshot.items[0].failure_code == (
        "replay_dataset_fingerprint_mismatch"
    )
    assert gates.calls == []


def test_strict_gate_cannot_pass_when_intermediate_gate_does_not() -> None:
    replay = _ScriptedReplay({"H3": [_evidence(3)]})
    gates = _GateRecorder(
        intermediate=EvidenceGateDecision.REJECTED,
        strict=EvidenceGateDecision.PASSED,
    )
    runner, _, _ = _runner(replay, gates)

    result = runner.execute(_request(_registration(3)))

    assert result.snapshot.state is PortfolioRunState.PARTIAL
    assert result.snapshot.items[0].failure_code == (
        "inconsistent_evidence_gate_decisions"
    )
