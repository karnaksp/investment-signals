from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

import pytest

from tinvest_signal_engine.adapters.scientific_hypothesis_registry import (
    ScientificRegistryFormatError,
    VersionedScientificRegistry,
)
from tinvest_signal_engine.application.scientific_hypotheses import (
    AdmissionFailure,
    AssessScientificHypothesisAdmission,
)
from tinvest_signal_engine.domain.scientific_hypotheses import (
    EvidenceLevel,
    HypothesisLifecycle,
    HypothesisOrigin,
    PreregisteredTest,
    ReplicationEvidence,
    ReplicationResult,
    ScientificHypothesis,
    ScientificSource,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "config" / "scientific_hypotheses" / "registry-v1.yaml"
CLI = ROOT / "scripts" / "validate_scientific_hypothesis_registry.py"
SEALED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _Sources:
    def __init__(self, *sources: ScientificSource) -> None:
        self._sources = {source.source_id: source for source in sources}

    def get_source(self, source_id: str) -> ScientificSource | None:
        return self._sources.get(source_id)


class _Catalog:
    def __init__(self, previous: ScientificHypothesis | None = None) -> None:
        self._previous = previous

    def latest_version(self, hypothesis_id: str) -> ScientificHypothesis | None:
        if self._previous and self._previous.hypothesis_id == hypothesis_id:
            return self._previous
        return None


def _source(*, primary: bool = True) -> ScientificSource:
    return ScientificSource(
        source_id="paper-1",
        title="Primary paper",
        authors=("Researcher",),
        publication_year=2020,
        identifier="doi:example",
        url="https://example.test/paper",
        primary_publication=primary,
        market="US",
        sample_period="2000-2020",
        sample_description="Common stocks",
        data_frequency="one minute",
        economic_mechanism="Temporary liquidity pressure",
        limitations=("Different market",),
        original_result="Reversal after pressure",
    )


def _preregistration(
    *,
    version: str = "1.0.0",
    threshold: str = "3.0",
    sealed_at: datetime | None = SEALED_AT,
) -> PreregisteredTest:
    return PreregisteredTest.with_thresholds(
        thresholds={"move_z": threshold},
        registration_id=f"pre-h1-{version}",
        hypothesis_id="h1",
        hypothesis_version=version,
        sealed_at=sealed_at,
        expected_direction="reverse",
        feature_definitions=("move_z uses only timestamps <= signal time",),
        market_phase="morning",
        horizon_seconds=(300,),
        success_criterion="mean net bps > 0 on holdout",
        abstention_conditions=("missing reference price",),
        cost_model_version="1.0.0",
        data_split_policy="60/20/20 chronological trading days",
        multiple_testing_policy="Benjamini-Hochberg q<=0.05",
    )


def _hypothesis(
    *,
    origin: HypothesisOrigin = HypothesisOrigin.RESEARCH_EXTENSION,
    version: str = "1.0.0",
    preregistration: PreregisteredTest | None = None,
    scientific_claim: bool = True,
) -> ScientificHypothesis:
    return ScientificHypothesis(
        hypothesis_id="h1",
        version=version,
        title="Morning reversal",
        origin=origin,
        source_ids=("paper-1",),
        testable_statement="An unconfirmed morning deviation reverses within five minutes.",
        economic_mechanism="Temporary liquidity pressure",
        market_phase="morning",
        trigger_conditions=("move_z >= 3",),
        expected_direction="reverse",
        horizon_seconds=(300,),
        abstention_conditions=("missing reference price",),
        falsification_criterion="Holdout net effect is non-positive.",
        original_market_result="Reversal was observed on the source market.",
        evidence_level=EvidenceLevel.VALIDATED,
        lifecycle=HypothesisLifecycle.EVALUATED,
        scientific_claim=scientific_claim,
        preregistration=preregistration or _preregistration(version=version),
    )


def _evidence(
    *,
    version: str = "1.0.0",
    result: ReplicationResult = ReplicationResult.CONFIRMED,
) -> ReplicationEvidence:
    return ReplicationEvidence(
        evidence_id=f"ev-h1-{version}",
        hypothesis_id="h1",
        hypothesis_version=version,
        market="MOEX",
        observed_at=SEALED_AT + timedelta(days=90),
        result=result,
        independent_validation=True,
        trading_days=30,
        eligible_events=300,
        cost_adjusted=True,
        matched_controls_applied=True,
        multiple_testing_applied=True,
        stability_checked=True,
        mean_net_bps=7.5,
        result_summary="Positive on independent validation days.",
        artifact_uri="var/research/runs/run-1/model-results.json",
        primary_metric="matched_control_lift_net_bps",
        controls_per_event=5,
        lift_ci_lower=1.1,
        lift_ci_upper=12.4,
        adjusted_p_value=0.02,
        stable_blocks=4,
        total_blocks=5,
        max_ticker_share=0.2,
        max_period_share=0.3,
        dataset_fingerprint="sha256:dataset",
        formula_fingerprint="sha256:formula",
        cost_model_version="1.0.0",
        abstention_rate=0.35,
        success_rate=0.62,
        success_wilson_lower=0.56,
    )


def _gate(
    previous: ScientificHypothesis | None = None,
) -> AssessScientificHypothesisAdmission:
    return AssessScientificHypothesisAdmission(
        sources=_Sources(_source()),
        applied_catalog=_Catalog(previous),
    )


def _codes(decision: object) -> set[AdmissionFailure]:
    return {issue.code for issue in decision.issues}  # type: ignore[attr-defined]


def test_research_extension_with_independent_moex_evidence_is_admitted() -> None:
    decision = _gate().execute(_hypothesis(), _evidence())

    assert decision.admitted is True
    assert decision.scientific_support_allowed is True
    assert decision.issues == ()


@pytest.mark.parametrize(
    "origin",
    [HypothesisOrigin.AUTHOR_PROPOSED, HypothesisOrigin.DATA_DISCOVERED],
)
def test_local_origin_can_be_validated_but_cannot_claim_scientific_support(
    origin: HypothesisOrigin,
) -> None:
    locally_validated = _hypothesis(
        origin=origin,
        scientific_claim=False,
    )
    decision = _gate().execute(locally_validated, _evidence())
    assert decision.admitted is True
    assert decision.scientific_support_allowed is False

    forbidden_claim = replace(locally_validated, scientific_claim=True)
    rejected = _gate().execute(forbidden_claim, _evidence())
    assert AdmissionFailure.SCIENTIFIC_CLAIM_NOT_ALLOWED in _codes(rejected)


def test_missing_scientific_contract_fields_block_applied_catalog() -> None:
    hypothesis = replace(
        _hypothesis(preregistration=_preregistration(sealed_at=None)),
        source_ids=(),
        testable_statement="",
        falsification_criterion="",
    )

    decision = _gate().execute(hypothesis, None)

    assert decision.admitted is False
    assert {
        AdmissionFailure.MISSING_SOURCE_IDS,
        AdmissionFailure.MISSING_TESTABLE_STATEMENT,
        AdmissionFailure.MISSING_FALSIFICATION_CRITERION,
        AdmissionFailure.PREREGISTRATION_NOT_SEALED,
        AdmissionFailure.INDEPENDENT_MOEX_VALIDATION_REQUIRED,
    } <= _codes(decision)


def test_moex_cost_adjusted_validation_is_mandatory() -> None:
    evidence = replace(
        _evidence(),
        market="NYSE",
        independent_validation=False,
        cost_adjusted=False,
        result=ReplicationResult.INCONCLUSIVE,
    )

    decision = _gate().execute(_hypothesis(), evidence)

    assert {
        AdmissionFailure.INDEPENDENT_MOEX_VALIDATION_REQUIRED,
        AdmissionFailure.REPLICATION_COSTS_REQUIRED,
        AdmissionFailure.REPLICATION_NOT_CONFIRMED,
    } <= _codes(decision)

def test_independent_sample_and_control_advantage_are_informational() -> None:
    evidence = replace(
        _evidence(),
        independent_validation=False,
        lift_ci_lower=None,
        lift_ci_upper=None,
    )

    decision = _gate().execute(_hypothesis(), evidence)

    assert decision.admitted is True
    assert AdmissionFailure.INDEPENDENT_MOEX_VALIDATION_REQUIRED not in _codes(decision)
    assert AdmissionFailure.REPLICATION_CONFIDENCE_INTERVAL_FAILED not in _codes(
        decision
    )


def test_quantitative_evidence_gate_rejects_small_or_fragile_effect() -> None:
    evidence = replace(
        _evidence(),
        trading_days=29,
        eligible_events=299,
        controls_per_event=4,
        lift_ci_lower=0.0,
        adjusted_p_value=0.051,
        stable_blocks=3,
        max_ticker_share=0.51,
    )

    decision = _gate().execute(_hypothesis(), evidence)

    assert {
        AdmissionFailure.REPLICATION_SAMPLE_TOO_SMALL,
        AdmissionFailure.REPLICATION_CONTROLS_INSUFFICIENT,
        AdmissionFailure.REPLICATION_ADJUSTED_SIGNIFICANCE_FAILED,
        AdmissionFailure.REPLICATION_BLOCK_STABILITY_FAILED,
        AdmissionFailure.REPLICATION_CONCENTRATION_FAILED,
    } <= _codes(decision)


def test_strict_90_label_requires_wilson_lower_bound_of_ninety_percent() -> None:
    hypothesis = replace(_hypothesis(), evidence_level=EvidenceLevel.STRICT_90)

    rejected = _gate().execute(hypothesis, _evidence())
    assert AdmissionFailure.STRICT_90_EVIDENCE_FAILED in _codes(rejected)

    accepted = _gate().execute(
        hypothesis,
        replace(_evidence(), success_rate=0.96, success_wilson_lower=0.91),
    )
    assert accepted.admitted is True


def test_validation_must_happen_after_preregistration_is_sealed() -> None:
    evidence = replace(_evidence(), observed_at=SEALED_AT)

    decision = _gate().execute(_hypothesis(), evidence)

    assert AdmissionFailure.VALIDATION_PRECEDES_PREREGISTRATION in _codes(decision)


def test_changed_sealed_parameters_require_a_new_version() -> None:
    previous = _hypothesis()
    changed_in_place = replace(
        previous,
        preregistration=_preregistration(threshold="4.0"),
    )

    rejected = _gate(previous).execute(changed_in_place, _evidence())
    assert AdmissionFailure.SEALED_PARAMETERS_CHANGED_IN_PLACE in _codes(rejected)

    new_version = replace(
        changed_in_place,
        version="1.1.0",
        preregistration=_preregistration(version="1.1.0", threshold="4.0"),
    )
    accepted = _gate(previous).execute(new_version, _evidence(version="1.1.0"))
    assert accepted.admitted is True


def test_top_level_analytical_change_is_also_versioned() -> None:
    previous = _hypothesis()
    changed = replace(previous, testable_statement="A tuned statement after holdout.")

    decision = _gate(previous).execute(changed, _evidence())

    assert AdmissionFailure.SEALED_PARAMETERS_CHANGED_IN_PLACE in _codes(decision)


def test_registry_loads_preregistered_portfolio_and_recorded_rejection() -> None:
    registry = VersionedScientificRegistry.from_file(REGISTRY)

    assert registry.schema_version == "1.2.0"
    assert len(registry.sources) == 16
    assert all(source.primary_publication for source in registry.sources)
    assert len(registry.hypotheses) == 31
    assert (
        sum(
            item.lifecycle is HypothesisLifecycle.PRE_REGISTERED
            for item in registry.hypotheses
        )
        == 25
    )
    assert (
        sum(
            item.lifecycle is HypothesisLifecycle.SHADOW for item in registry.hypotheses
        )
        == 2
    )
    assert all(
        item.preregistration and item.preregistration.sealed
        for item in registry.hypotheses
    )
    assert {
        item.preregistration.registration_id
        for item in registry.hypotheses
        if item.preregistration is not None
    } >= {
        "prereg-h1-v2",
        "prereg-h1-v2-1",
        "prereg-h1-v2-2",
        "prereg-h3-v2",
        "prereg-h4-v2",
        "prereg-h3-v3",
        "prereg-h4-v3",
        "prereg-h7-v3",
        "prereg-h11-v2",
        "prereg-h12-v2",
        "prereg-h15-v2",
        "prereg-h16-v1",
        "prereg-h16-v2",
        "prereg-h17-v1",
        "prereg-h17-v2",
        "prereg-h12-v1",
    }
    assert len(registry.replication_evidence) == 4
    rejected = registry.replication_evidence[0]
    assert rejected.hypothesis_version == "2.0.0"
    assert rejected.result is ReplicationResult.REJECTED
    assert rejected.mean_net_bps is not None and rejected.mean_net_bps < 0.0
    inconclusive = registry.replication_evidence[1]
    assert inconclusive.hypothesis_version == "2.1.0"
    assert inconclusive.result is ReplicationResult.INCONCLUSIVE
    assert inconclusive.independent_validation is False
    filtered = registry.replication_evidence[2]
    assert filtered.hypothesis_version == "2.2.0"
    assert filtered.success_rate == pytest.approx(0.7222222222222222)
    assert filtered.success_wilson_lower == pytest.approx(0.5774647887323944)
    competing = registry.replication_evidence[3]
    assert competing.hypothesis_version == "2.3.0"
    assert competing.success_rate == pytest.approx(0.96)
    assert competing.success_wilson_lower == pytest.approx(0.8653990931249298)
    assert registry.applied_catalog == ()
    h3v3 = registry.get_hypothesis(
        "h3-jump-low-activity-reversal",
        "3.0.0",
    )
    h4v3 = registry.get_hypothesis(
        "h4-jump-high-activity-continuation",
        "3.0.0",
    )
    assert h3v3 is not None and h4v3 is not None
    assert h3v3.source_ids
    assert h4v3.source_ids
    assert h3v3.falsification_criterion
    assert h4v3.falsification_criterion
    assert h3v3.preregistration is not None
    assert h4v3.preregistration is not None
    assert "activity_regime_ambiguous" in h3v3.abstention_conditions
    assert "activity_regime_ambiguous" in h4v3.abstention_conditions


def test_registry_rejects_duplicate_source_ids() -> None:
    registry = VersionedScientificRegistry.from_file(REGISTRY)
    source = registry.sources[0]
    minimal = {
        "schema_version": "1.0.0",
        "sources": [],
        "hypotheses": [],
        "replication_evidence": [],
        "applied_catalog": [],
    }
    source_mapping = {
        "source_id": source.source_id,
        "title": source.title,
        "authors": list(source.authors),
        "publication_year": source.publication_year,
        "identifier": source.identifier,
        "url": source.url,
        "primary_publication": source.primary_publication,
        "market": source.market,
        "sample_period": source.sample_period,
        "sample_description": source.sample_description,
        "data_frequency": source.data_frequency,
        "economic_mechanism": source.economic_mechanism,
        "limitations": list(source.limitations),
        "original_result": source.original_result,
    }
    minimal["sources"] = [source_mapping, source_mapping]

    with pytest.raises(ScientificRegistryFormatError, match="Duplicate source_id"):
        VersionedScientificRegistry.from_mapping(minimal)


def test_registry_reads_versioned_json(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.2.0",
                "sources": [],
                "hypotheses": [],
                "replication_evidence": [],
                "applied_catalog": [],
            }
        ),
        encoding="utf-8",
    )

    registry = VersionedScientificRegistry.from_file(path)

    assert registry.schema_version == "1.2.0"


def test_registry_cli_validates_checked_in_source_registry() -> None:
    result = subprocess.run(
        [sys.executable, str(CLI), "--registry", str(REGISTRY), "--json"],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["sources"] == 16
    assert payload["hypotheses"] == 31
    assert payload["applied"] == 0
    assert payload["decisions"] == []


def test_registry_cli_rejects_unresolved_applied_reference(tmp_path: Path) -> None:
    path = tmp_path / "invalid-applied.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "1.0.0",
                "sources": [],
                "hypotheses": [],
                "replication_evidence": [],
                "applied_catalog": [
                    {
                        "hypothesis_id": "missing",
                        "version": "1.0.0",
                        "evidence_id": "missing-evidence",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(CLI), "--registry", str(path)],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1
    assert "missing hypothesis missing@1.0.0" in result.stderr
