"""Scientific admission gate independent from storage and transport details."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from tinvest_signal_engine.domain.scientific_hypotheses import (
    ReplicationEvidence,
    ReplicationResult,
    ScientificHypothesis,
    ScientificSource,
    semantic_version_key,
)


class ScientificSourcePort(Protocol):
    def get_source(self, source_id: str) -> ScientificSource | None: ...


class AppliedHypothesisCatalogPort(Protocol):
    def latest_version(self, hypothesis_id: str) -> ScientificHypothesis | None: ...


class AdmissionFailure(str, Enum):
    INVALID_VERSION = "invalid_version"
    MISSING_SOURCE_IDS = "missing_source_ids"
    SOURCE_NOT_FOUND = "source_not_found"
    PRIMARY_SOURCE_REQUIRED = "primary_source_required"
    MISSING_TESTABLE_STATEMENT = "missing_testable_statement"
    MISSING_FALSIFICATION_CRITERION = "missing_falsification_criterion"
    MISSING_PREREGISTRATION = "missing_preregistration"
    PREREGISTRATION_NOT_SEALED = "preregistration_not_sealed"
    INCOMPLETE_PREREGISTRATION = "incomplete_preregistration"
    PREREGISTRATION_IDENTITY_MISMATCH = "preregistration_identity_mismatch"
    PREREGISTRATION_PARAMETERS_MISMATCH = "preregistration_parameters_mismatch"
    SCIENTIFIC_CLAIM_NOT_ALLOWED = "scientific_claim_not_allowed"
    VERSION_NOT_INCREMENTED = "version_not_incremented"
    SEALED_PARAMETERS_CHANGED_IN_PLACE = "sealed_parameters_changed_in_place"
    REPLICATION_IDENTITY_MISMATCH = "replication_identity_mismatch"
    INDEPENDENT_MOEX_VALIDATION_REQUIRED = "independent_moex_validation_required"
    REPLICATION_NOT_CONFIRMED = "replication_not_confirmed"
    REPLICATION_CONTROLS_REQUIRED = "replication_controls_required"
    REPLICATION_COSTS_REQUIRED = "replication_costs_required"
    REPLICATION_MULTIPLE_TESTING_REQUIRED = "replication_multiple_testing_required"
    REPLICATION_STABILITY_REQUIRED = "replication_stability_required"
    REPLICATION_ARTIFACT_REQUIRED = "replication_artifact_required"
    VALIDATION_PRECEDES_PREREGISTRATION = "validation_precedes_preregistration"


@dataclass(frozen=True)
class AdmissionIssue:
    code: AdmissionFailure
    detail: str


@dataclass(frozen=True)
class AdmissionDecision:
    hypothesis_id: str
    version: str
    admitted: bool
    scientific_support_allowed: bool
    issues: tuple[AdmissionIssue, ...]


class AssessScientificHypothesisAdmission:
    """Decide whether a hypothesis can enter the applied product catalog."""

    def __init__(
        self,
        *,
        sources: ScientificSourcePort,
        applied_catalog: AppliedHypothesisCatalogPort,
    ) -> None:
        self._sources = sources
        self._applied_catalog = applied_catalog

    def execute(
        self,
        hypothesis: ScientificHypothesis,
        evidence: ReplicationEvidence | None,
    ) -> AdmissionDecision:
        issues: list[AdmissionIssue] = []
        current_version = semantic_version_key(hypothesis.version)
        if current_version is None:
            self._add(issues, AdmissionFailure.INVALID_VERSION, hypothesis.version)

        if not hypothesis.source_ids:
            self._add(issues, AdmissionFailure.MISSING_SOURCE_IDS)
        resolved_sources = []
        for source_id in hypothesis.source_ids:
            source = self._sources.get_source(source_id)
            if source is None:
                self._add(issues, AdmissionFailure.SOURCE_NOT_FOUND, source_id)
            else:
                resolved_sources.append(source)
        if hypothesis.source_ids and resolved_sources and not any(
            source.primary_publication for source in resolved_sources
        ):
            self._add(issues, AdmissionFailure.PRIMARY_SOURCE_REQUIRED)

        if not hypothesis.testable_statement.strip():
            self._add(issues, AdmissionFailure.MISSING_TESTABLE_STATEMENT)
        if not hypothesis.falsification_criterion.strip():
            self._add(issues, AdmissionFailure.MISSING_FALSIFICATION_CRITERION)

        preregistration = hypothesis.preregistration
        if preregistration is None:
            self._add(issues, AdmissionFailure.MISSING_PREREGISTRATION)
        else:
            if not preregistration.sealed:
                self._add(issues, AdmissionFailure.PREREGISTRATION_NOT_SEALED)
            if not all(
                (
                    preregistration.registration_id.strip(),
                    preregistration.expected_direction.strip(),
                    preregistration.feature_definitions,
                    preregistration.thresholds,
                    preregistration.market_phase.strip(),
                    preregistration.horizon_seconds,
                    preregistration.success_criterion.strip(),
                    preregistration.abstention_conditions,
                    preregistration.cost_model_version.strip(),
                    preregistration.data_split_policy.strip(),
                    preregistration.multiple_testing_policy.strip(),
                )
            ):
                self._add(issues, AdmissionFailure.INCOMPLETE_PREREGISTRATION)
            if (
                preregistration.hypothesis_id != hypothesis.hypothesis_id
                or preregistration.hypothesis_version != hypothesis.version
            ):
                self._add(issues, AdmissionFailure.PREREGISTRATION_IDENTITY_MISMATCH)
            if (
                preregistration.expected_direction != hypothesis.expected_direction
                or preregistration.market_phase != hypothesis.market_phase
                or preregistration.horizon_seconds != hypothesis.horizon_seconds
                or preregistration.abstention_conditions
                != hypothesis.abstention_conditions
            ):
                self._add(issues, AdmissionFailure.PREREGISTRATION_PARAMETERS_MISMATCH)

        scientific_support_allowed = (
            hypothesis.origin.supports_scientific_claim
            and bool(hypothesis.source_ids)
            and len(resolved_sources) == len(hypothesis.source_ids)
            and any(source.primary_publication for source in resolved_sources)
        )
        if hypothesis.scientific_claim and not scientific_support_allowed:
            self._add(issues, AdmissionFailure.SCIENTIFIC_CLAIM_NOT_ALLOWED)

        previous = self._applied_catalog.latest_version(hypothesis.hypothesis_id)
        if previous is not None:
            previous_version = semantic_version_key(previous.version)
            if (
                current_version is not None
                and previous_version is not None
                and current_version < previous_version
            ):
                self._add(issues, AdmissionFailure.VERSION_NOT_INCREMENTED)
            if previous.preregistration is not None and preregistration is not None:
                changed = (
                    previous.sealed_parameters_fingerprint()
                    != hypothesis.sealed_parameters_fingerprint()
                )
                if changed and hypothesis.version == previous.version:
                    self._add(
                        issues,
                        AdmissionFailure.SEALED_PARAMETERS_CHANGED_IN_PLACE,
                    )
                elif changed and (
                    current_version is None
                    or previous_version is None
                    or current_version <= previous_version
                ):
                    self._add(issues, AdmissionFailure.VERSION_NOT_INCREMENTED)

        self._validate_replication(hypothesis, evidence, issues)
        return AdmissionDecision(
            hypothesis_id=hypothesis.hypothesis_id,
            version=hypothesis.version,
            admitted=not issues,
            scientific_support_allowed=scientific_support_allowed,
            issues=tuple(issues),
        )

    @staticmethod
    def _add(
        issues: list[AdmissionIssue],
        code: AdmissionFailure,
        detail: str = "",
    ) -> None:
        issues.append(AdmissionIssue(code=code, detail=detail))

    def _validate_replication(
        self,
        hypothesis: ScientificHypothesis,
        evidence: ReplicationEvidence | None,
        issues: list[AdmissionIssue],
    ) -> None:
        if evidence is None:
            self._add(issues, AdmissionFailure.INDEPENDENT_MOEX_VALIDATION_REQUIRED)
            return
        if (
            evidence.hypothesis_id != hypothesis.hypothesis_id
            or evidence.hypothesis_version != hypothesis.version
        ):
            self._add(issues, AdmissionFailure.REPLICATION_IDENTITY_MISMATCH)
        if evidence.market.upper() != "MOEX" or not evidence.independent_validation:
            self._add(issues, AdmissionFailure.INDEPENDENT_MOEX_VALIDATION_REQUIRED)
        if evidence.result is not ReplicationResult.CONFIRMED:
            self._add(issues, AdmissionFailure.REPLICATION_NOT_CONFIRMED)
        if not evidence.matched_controls_applied:
            self._add(issues, AdmissionFailure.REPLICATION_CONTROLS_REQUIRED)
        if not evidence.cost_adjusted:
            self._add(issues, AdmissionFailure.REPLICATION_COSTS_REQUIRED)
        if not evidence.multiple_testing_applied:
            self._add(issues, AdmissionFailure.REPLICATION_MULTIPLE_TESTING_REQUIRED)
        if not evidence.stability_checked:
            self._add(issues, AdmissionFailure.REPLICATION_STABILITY_REQUIRED)
        if not evidence.artifact_uri.strip():
            self._add(issues, AdmissionFailure.REPLICATION_ARTIFACT_REQUIRED)
        if (
            hypothesis.preregistration is not None
            and hypothesis.preregistration.sealed_at is not None
            and evidence.observed_at <= hypothesis.preregistration.sealed_at
        ):
            self._add(issues, AdmissionFailure.VALIDATION_PRECEDES_PREREGISTRATION)
