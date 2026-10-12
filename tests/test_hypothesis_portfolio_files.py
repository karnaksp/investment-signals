from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.hypothesis_portfolio_files import (
    ImmutableFileHypothesisPortfolioStore,
    PortfolioRevisionConflict,
    SafeFileHypothesisPortfolioProgress,
)
from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    EvidenceGateAssessment,
    EvidenceGateDecision,
    EvidenceGatePolicyReference,
    EvidenceGateTier,
    HypothesisPortfolioSnapshot,
    PortfolioHypothesisRegistration,
    PortfolioItemExecutionError,
    PortfolioItemResult,
    PortfolioItemState,
    PortfolioRunState,
    ReplayHypothesisCommand,
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
from tinvest_signal_engine.services.hypothesis_portfolio_runtime import (
    build_file_hypothesis_portfolio_runtime,
)


NOW = datetime(2026, 7, 19, tzinfo=timezone.utc)
RUN_ID = "hypothesis-portfolio-" + "a" * 64
DATASET = "sha256:dataset"
COST_MODEL = "costs-v1"


def _evidence(number: int, *, secret: bool = False) -> ReplicationEvidence:
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
        result_summary=("sensitive-result-summary" if secret else "candidate"),
        artifact_uri=("/secret/artifact/path" if secret else "local://artifact"),
        primary_metric="cost_adjusted_return_bps",
        controls_per_event=5,
        lift_ci_lower=0.1,
        lift_ci_upper=3.0,
        adjusted_p_value=0.04,
        stable_blocks=3,
        total_blocks=5,
        max_ticker_share=0.30,
        max_period_share=0.35,
        dataset_fingerprint=DATASET,
        formula_fingerprint=f"sha256:formula-h{number}",
        cost_model_version=COST_MODEL,
        abstention_rate=0.70,
        success_rate=0.65,
        success_wilson_lower=0.60,
    )


def _assessment(tier: EvidenceGateTier) -> EvidenceGateAssessment:
    return EvidenceGateAssessment(
        tier=tier,
        decision=(
            EvidenceGateDecision.PASSED
            if tier is EvidenceGateTier.INTERMEDIATE
            else EvidenceGateDecision.INCONCLUSIVE
        ),
        policy_fingerprint=f"sha256:{tier.value}",
        reason_codes=("future_holdout_required",)
        if tier is EvidenceGateTier.STRICT
        else (),
    )


def _snapshot(
    *,
    revision: int = 1,
    state: PortfolioItemState = PortfolioItemState.COMPLETED,
    secret: bool = False,
) -> HypothesisPortfolioSnapshot:
    item = PortfolioItemResult(
        item_key="h3-test-hypothesis@1.0.0",
        replay_key="H3",
        registration_fingerprint="sha256:registration",
        state=state,
        attempts=1 if state is not PortfolioItemState.PENDING else 0,
        evidence=_evidence(3, secret=secret)
        if state is PortfolioItemState.COMPLETED
        else None,
        intermediate_assessment=_assessment(EvidenceGateTier.INTERMEDIATE)
        if state is PortfolioItemState.COMPLETED
        else None,
        strict_assessment=_assessment(EvidenceGateTier.STRICT)
        if state is PortfolioItemState.COMPLETED
        else None,
        failure_code="temporary_failure"
        if state is PortfolioItemState.FAILED
        else None,
    )
    run_state = (
        PortfolioRunState.COMPLETED
        if state is PortfolioItemState.COMPLETED
        else PortfolioRunState.PARTIAL
        if state is PortfolioItemState.FAILED
        else PortfolioRunState.RUNNING
    )
    return HypothesisPortfolioSnapshot(
        run_id=RUN_ID,
        input_fingerprint="sha256:input",
        state=run_state,
        revision=revision,
        items=(item,),
    )


def test_store_round_trips_and_never_overwrites_revisions(tmp_path: Path) -> None:
    store = ImmutableFileHypothesisPortfolioStore(tmp_path / "state")
    first = _snapshot()

    store.save(first, expected_revision=None)
    store.save(first, expected_revision=None)
    first_path = next((tmp_path / "state").glob("*/revisions/*.json"))
    first_bytes = first_path.read_bytes()

    second = replace(first, revision=2)
    store.save(second, expected_revision=1)

    assert store.load(RUN_ID) == second
    revision_files = sorted((tmp_path / "state").glob("*/revisions/*.json"))
    assert len(revision_files) == 2
    assert revision_files[0].read_bytes() == first_bytes
    assert revision_files[0].stat().st_mode & 0o777 == 0o600


def test_store_rejects_stale_or_conflicting_revision(tmp_path: Path) -> None:
    store = ImmutableFileHypothesisPortfolioStore(tmp_path / "state")
    first = _snapshot(state=PortfolioItemState.PENDING)
    store.save(first, expected_revision=None)

    with pytest.raises(PortfolioRevisionConflict, match="revision conflict"):
        store.save(replace(first, revision=2), expected_revision=7)

    different_first = replace(
        first,
        items=(replace(first.items[0], replay_key="H4"),),
    )
    with pytest.raises(PortfolioRevisionConflict, match="different state"):
        store.save(different_first, expected_revision=None)


def test_progress_projection_is_redacted_and_immutable(tmp_path: Path) -> None:
    progress = SafeFileHypothesisPortfolioProgress(tmp_path / "progress")
    snapshot = _snapshot(secret=True)

    progress.publish(snapshot)
    progress.publish(snapshot)

    payload = progress.read_latest(RUN_ID)
    assert payload is not None
    encoded = json.dumps(payload)
    assert "sensitive-result-summary" not in encoded
    assert "/secret/artifact/path" not in encoded
    assert "artifact_uri" not in encoded
    assert "mean_net_bps" not in encoded
    assert payload["progress"]["completed"] == 1
    assert payload["items"][0]["strict_decision"] == "inconclusive"
    revision_files = sorted((tmp_path / "progress").glob("*/revisions/*.json"))
    assert len(revision_files) == 1
    latest = next((tmp_path / "progress").glob("*/latest.json"))
    assert latest.stat().st_mode & 0o777 == 0o600


class _ScriptedReplay:
    def __init__(
        self, outcomes: dict[str, list[ReplicationEvidence | Exception]]
    ) -> None:
        self.outcomes = outcomes
        self.calls: list[str] = []

    def replay(self, command: ReplayHypothesisCommand) -> ReplicationEvidence:
        self.calls.append(command.replay_key)
        outcome = self.outcomes[command.replay_key].pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _Gates:
    def assess(
        self,
        *,
        tier: EvidenceGateTier,
        registration: PortfolioHypothesisRegistration,
        evidence: ReplicationEvidence,
        policy: EvidenceGatePolicyReference,
    ) -> EvidenceGateAssessment:
        del registration, evidence
        return EvidenceGateAssessment(
            tier=tier,
            decision=(
                EvidenceGateDecision.PASSED
                if tier is EvidenceGateTier.INTERMEDIATE
                else EvidenceGateDecision.INCONCLUSIVE
            ),
            policy_fingerprint=policy.fingerprint,
        )


def _registration(number: int) -> PortfolioHypothesisRegistration:
    hypothesis_id = f"h{number}-test-hypothesis"
    preregistration = PreregisteredTest.with_thresholds(
        registration_id=f"prereg-h{number}",
        hypothesis_id=hypothesis_id,
        hypothesis_version="1.0.0",
        sealed_at=NOW,
        expected_direction="continuation",
        feature_definitions=("causal feature",),
        thresholds={"z_min": 2.0},
        market_phase="main_session",
        horizon_seconds=(300,),
        success_criterion="positive lower bound",
        abstention_conditions=("trading_gap",),
        cost_model_version=COST_MODEL,
        data_split_policy="chronological",
        multiple_testing_policy="family correction",
    )
    hypothesis = ScientificHypothesis(
        hypothesis_id=hypothesis_id,
        version="1.0.0",
        title=f"H{number}",
        origin=HypothesisOrigin.RESEARCH_EXTENSION,
        source_ids=("source",),
        testable_statement="statement",
        economic_mechanism="mechanism",
        market_phase="main_session",
        trigger_conditions=("z >= 2",),
        expected_direction="continuation",
        horizon_seconds=(300,),
        abstention_conditions=("trading_gap",),
        falsification_criterion="lower bound is not positive",
        original_market_result="published",
        evidence_level=EvidenceLevel.RESEARCHING,
        lifecycle=HypothesisLifecycle.PRE_REGISTERED,
        scientific_claim=True,
        preregistration=preregistration,
    )
    return PortfolioHypothesisRegistration(
        replay_key=f"H{number}",
        hypothesis=hypothesis,
        family_id="jump-family",
        primary_metric="cost_adjusted_return_bps",
        primary_horizon_seconds=300,
        intermediate_gate=EvidenceGatePolicyReference(
            "intermediate", "1.0.0", "sha256:intermediate"
        ),
        strict_gate=EvidenceGatePolicyReference(
            "strict", "1.0.0", "sha256:strict"
        ),
    )


def _request() -> RunHypothesisPortfolioRequest:
    return RunHypothesisPortfolioRequest(
        dataset_fingerprint=DATASET,
        cost_model_version=COST_MODEL,
        replay_engine_version="replay-v1",
        hypotheses=(_registration(3), _registration(4)),
    )


def test_new_process_resumes_and_retries_only_failed_hypothesis(
    tmp_path: Path,
) -> None:
    request = _request()
    first_replay = _ScriptedReplay(
        {
            "H3": [_evidence(3)],
            "H4": [PortfolioItemExecutionError("temporary_failure")],
        }
    )
    first_runtime = build_file_hypothesis_portfolio_runtime(
        state_dir=tmp_path,
        replay=first_replay,
        evidence_gates=_Gates(),
    )

    first = first_runtime.runner.execute(request)
    assert first.snapshot.state is PortfolioRunState.PARTIAL

    second_replay = _ScriptedReplay({"H4": [_evidence(4)]})
    second_runtime = build_file_hypothesis_portfolio_runtime(
        state_dir=tmp_path,
        replay=second_replay,
        evidence_gates=_Gates(),
    )
    second = second_runtime.runner.execute(request)

    assert second.resumed is True
    assert second.executed_item_keys == ("h4-test-hypothesis@1.0.0",)
    assert second.snapshot.state is PortfolioRunState.COMPLETED
    assert first_replay.calls == ["H3", "H4"]
    assert second_replay.calls == ["H4"]
    items = {item.replay_key: item for item in second.snapshot.items}
    assert items["H3"].attempts == 1
    assert items["H4"].attempts == 2


def test_composition_repairs_progress_saved_before_crash(tmp_path: Path) -> None:
    state_root = tmp_path / "hypothesis-portfolios"
    store = ImmutableFileHypothesisPortfolioStore(state_root / "state")
    store.save(_snapshot(), expected_revision=None)

    runtime = build_file_hypothesis_portfolio_runtime(
        state_dir=tmp_path,
        replay=_ScriptedReplay({}),
        evidence_gates=_Gates(),
    )

    assert runtime.repaired_progress_runs == 1
    progress = runtime.progress.read_latest(RUN_ID)
    assert progress is not None
    assert progress["revision"] == 1
