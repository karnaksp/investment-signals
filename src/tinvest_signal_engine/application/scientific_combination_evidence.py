"""Batch evidence use cases for the preregistered C1-C5 combinations.

The application layer consumes already sealed prospective features and their
outcomes.  It never reads candles, databases, or framework records.  A
combination is compared with matched observations of its registered standalone
basis; arbitrary feature mining is deliberately outside this use case.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from enum import Enum
from hashlib import sha256
import json
from typing import Iterable, Mapping, Protocol
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildMatchedControls,
    EvidenceDiagnosticsInput,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.application.scientific_hypothesis_combinations import (
    ComposeScientificCombinationBatch,
    ComposeScientificCombinationBatchRequest,
    ComposeScientificCombinationRequest,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    EvidenceBundle,
    EvidenceDecision,
    EvidenceReasonCount,
    StudyPoint,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveScientificPolicy,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    CombinationComponentRole,
    ScientificCombinationId,
    ScientificCombinationObservation,
    preregistered_combination_definition,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
)


COMBINATION_ABSTAIN_POLICY_VERSION = "scientific-combination-abstain-v1"
COMBINATION_EVIDENCE_VERSION = "scientific-combination-evidence-v1"


class CombinationStatisticalState(str, Enum):
    """Product-neutral result of the scientific gate."""

    PASSED = "passed"
    REJECTED = "rejected"
    UNCERTAIN = "uncertain"
    BLOCKED_DATA = "blocked-data"


@dataclass(frozen=True, slots=True)
class CombinationOutcomeRecord:
    observation_id: str
    combination_id: ScientificCombinationId
    horizon_seconds: int
    target_at: datetime
    available: bool
    reason_code: str
    source_observation_id: str | None
    forward_return_bps: float | None
    net_directional_return_bps: float | None

    def __post_init__(self) -> None:
        if not self.observation_id.startswith("sha256:"):
            raise ValueError("combination outcome identity is invalid")
        if self.horizon_seconds <= 0:
            raise ValueError("combination outcome horizon must be positive")
        if self.target_at.tzinfo is None or self.target_at.utcoffset() is None:
            raise ValueError("combination target_at must be timezone-aware")
        if not self.reason_code.strip():
            raise ValueError("combination outcome reason is required")
        if self.available != (self.forward_return_bps is not None):
            raise ValueError("available combination outcome needs a forward return")
        if self.net_directional_return_bps is not None and not self.available:
            raise ValueError("unavailable outcome cannot carry a net return")


@dataclass(frozen=True, slots=True)
class CombinationControlMatch:
    event_observation_id: str
    standalone_observation_ids: tuple[str, ...]
    event_net_bps: float
    standalone_mean_net_bps: float
    incremental_lift_bps: float

    def __post_init__(self) -> None:
        if not self.event_observation_id.startswith("sha256:"):
            raise ValueError("combination control event identity is invalid")
        if len(self.standalone_observation_ids) != len(
            set(self.standalone_observation_ids)
        ):
            raise ValueError("standalone controls must be unique")


@dataclass(frozen=True, slots=True)
class CombinationEvidenceCoverage:
    total_observations: int
    matched_observations: int
    not_matched_observations: int
    abstained_observations: int
    available_outcomes: int
    eligible_events: int
    matched_events: int
    standalone_candidates: int
    reasons_histogram: tuple[EvidenceReasonCount, ...]

    def __post_init__(self) -> None:
        counts = (
            self.total_observations,
            self.matched_observations,
            self.not_matched_observations,
            self.abstained_observations,
            self.available_outcomes,
            self.eligible_events,
            self.matched_events,
            self.standalone_candidates,
        )
        if any(value < 0 for value in counts):
            raise ValueError("combination coverage counts must be non-negative")
        if (
            self.matched_observations
            + self.not_matched_observations
            + self.abstained_observations
            != self.total_observations
        ):
            raise ValueError("every combination observation must be classified")
        if self.available_outcomes > self.total_observations:
            raise ValueError("available outcomes cannot exceed observations")
        if self.eligible_events != self.matched_observations:
            raise ValueError("every matched combination is an eligible event")
        if self.matched_events > self.eligible_events:
            raise ValueError("matched evidence events cannot exceed eligible events")

    @property
    def selective_coverage(self) -> float | None:
        if not self.available_outcomes:
            return None
        return self.eligible_events / self.available_outcomes


@dataclass(frozen=True, slots=True)
class CombinationEvidenceResult:
    combination_id: ScientificCombinationId
    combination_version: str
    horizon_seconds: int
    statistical_state: CombinationStatisticalState
    comparison_hypotheses: tuple[ProspectiveHypothesis, ...]
    abstain_policy_version: str
    coverage: CombinationEvidenceCoverage
    control_matches: tuple[CombinationControlMatch, ...]
    evidence: EvidenceBundle

    def __post_init__(self) -> None:
        definition = preregistered_combination_definition(self.combination_id)
        if self.combination_version != definition.version:
            raise ValueError("combination evidence version drifted")
        if self.horizon_seconds not in definition.horizons_seconds:
            raise ValueError("combination evidence horizon is not registered")
        if self.comparison_hypotheses != definition.comparison_hypothesis_ids:
            raise ValueError("combination must be compared with its sealed basis")
        if self.abstain_policy_version != COMBINATION_ABSTAIN_POLICY_VERSION:
            raise ValueError("unsupported combination abstain policy")
        if self.statistical_state is not _statistical_state(self.evidence.decision):
            raise ValueError("combination state must reflect the evidence gate")


@dataclass(frozen=True, slots=True)
class ScientificCombinationPortfolio:
    evidence_version: str
    dataset_fingerprint: str
    source_report_fingerprint: str
    portfolio_fingerprint: str
    cost_model_version: str
    observations: tuple[ScientificCombinationObservation, ...]
    outcomes: tuple[CombinationOutcomeRecord, ...]
    results: tuple[CombinationEvidenceResult, ...]

    def __post_init__(self) -> None:
        if self.evidence_version != COMBINATION_EVIDENCE_VERSION:
            raise ValueError("unsupported combination evidence version")
        fingerprints = (
            self.dataset_fingerprint,
            self.source_report_fingerprint,
            self.portfolio_fingerprint,
        )
        if any(not item.startswith("sha256:") for item in fingerprints):
            raise ValueError("combination portfolio fingerprints must use sha256")
        if not self.cost_model_version.strip():
            raise ValueError("combination portfolio requires a cost model")
        if len(self.observations) != len(self.outcomes):
            raise ValueError("combination observations and outcomes must align")
        if any(
            observation.observation_id != outcome.observation_id
            for observation, outcome in zip(
                self.observations, self.outcomes, strict=True
            )
        ):
            raise ValueError("combination outcomes must align with observations")


@dataclass(frozen=True, slots=True)
class EvaluateScientificCombinationPortfolioRequest:
    report: ProspectiveScientificReport
    cost_model_version: str
    combination_ids: tuple[ScientificCombinationId, ...] = tuple(
        ScientificCombinationId
    )
    market_context_scope: str = "MOEX"

    def __post_init__(self) -> None:
        if not self.cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        if not self.combination_ids:
            raise ValueError("at least one combination is required")
        if len(set(self.combination_ids)) != len(self.combination_ids):
            raise ValueError("combination ids must be unique")
        if not self.market_context_scope.strip():
            raise ValueError("market_context_scope must not be empty")


@dataclass(frozen=True, slots=True)
class ScientificCombinationArtifactReference:
    artifact_uri: str
    artifact_fingerprint: str

    def __post_init__(self) -> None:
        if not self.artifact_uri.strip():
            raise ValueError("artifact_uri must not be empty")
        if not self.artifact_fingerprint.startswith("sha256:"):
            raise ValueError("artifact fingerprint must use sha256")


class ScientificCombinationArtifactPort(Protocol):
    """Application-owned boundary for immutable research artifacts."""

    def save(
        self, portfolio: ScientificCombinationPortfolio
    ) -> ScientificCombinationArtifactReference: ...


@dataclass(frozen=True, slots=True)
class ProspectiveScientificPartition:
    """One bounded trading-day input from an outer persistence adapter."""

    trading_day: date
    features: tuple[ProspectiveFeature, ...]
    outcomes: tuple[ProspectiveOutcome, ...]

    def __post_init__(self) -> None:
        if len(self.features) != len(self.outcomes):
            raise ValueError("prospective partition features and outcomes must align")
        if any(item.trading_day != self.trading_day for item in self.features):
            raise ValueError("prospective partition must contain one trading day")
        if any(
            feature.observation_id != outcome.observation_id
            for feature, outcome in zip(self.features, self.outcomes, strict=True)
        ):
            raise ValueError("prospective partition outcomes must align with features")


@dataclass(frozen=True, slots=True)
class ProspectiveScientificPartitionSourceDescriptor:
    dataset_fingerprint: str
    source_report_fingerprint: str
    split: ChronologicalSplit
    policy: ProspectiveScientificPolicy
    selected_hypotheses: tuple[ProspectiveHypothesis, ...]

    def __post_init__(self) -> None:
        if not self.dataset_fingerprint.startswith("sha256:"):
            raise ValueError("partition source dataset fingerprint must use sha256")
        if not self.source_report_fingerprint.startswith("sha256:"):
            raise ValueError("partition source report fingerprint must use sha256")
        if not self.selected_hypotheses:
            raise ValueError("partition source requires hypotheses")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("partition source hypotheses must be unique")


class ProspectiveScientificPartitionSourcePort(Protocol):
    def describe(self) -> ProspectiveScientificPartitionSourceDescriptor: ...

    def iter_partitions(self) -> Iterable[ProspectiveScientificPartition]: ...


@dataclass(frozen=True, slots=True)
class ScientificCombinationPartitionArtifact:
    trading_day: date
    observations: tuple[ScientificCombinationObservation, ...]
    outcomes: tuple[CombinationOutcomeRecord, ...]

    def __post_init__(self) -> None:
        if len(self.observations) != len(self.outcomes):
            raise ValueError("combination partition rows must align")
        if any(item.trading_day != self.trading_day for item in self.observations):
            raise ValueError("combination artifact partition must contain one day")
        if any(
            observation.observation_id != outcome.observation_id
            for observation, outcome in zip(
                self.observations, self.outcomes, strict=True
            )
        ):
            raise ValueError("combination partition outcomes must align")


@dataclass(frozen=True, slots=True)
class ScientificCombinationStreamingCompletion:
    run_id: str
    artifact: ScientificCombinationArtifactReference
    partition_count: int
    observation_count: int
    result_count: int
    resumed: bool

    def __post_init__(self) -> None:
        if not self.run_id.startswith("sha256:"):
            raise ValueError("combination streaming run id must use sha256")
        if min(
            self.partition_count,
            self.observation_count,
            self.result_count,
        ) < 0:
            raise ValueError("combination streaming counts must be non-negative")


class ScientificCombinationStreamingArtifactPort(Protocol):
    def load_completed(
        self, run_id: str
    ) -> ScientificCombinationStreamingCompletion | None: ...

    def stage_partition(
        self,
        run_id: str,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
        partition: ScientificCombinationPartitionArtifact,
    ) -> None: ...

    def complete(
        self,
        run_id: str,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
        results: tuple[CombinationEvidenceResult, ...],
        *,
        cost_model_version: str,
        partition_count: int,
        observation_count: int,
    ) -> ScientificCombinationStreamingCompletion: ...


class StoreScientificCombinationPortfolio:
    def __init__(self, artifacts: ScientificCombinationArtifactPort) -> None:
        self._artifacts = artifacts

    def execute(
        self, portfolio: ScientificCombinationPortfolio
    ) -> ScientificCombinationArtifactReference:
        return self._artifacts.save(portfolio)


class EvaluateScientificCombinationPortfolio:
    """Compose C1-C5, compare them with their standalone basis, and gate them."""

    def __init__(self, policy: EvidenceGatePolicy = EvidenceGatePolicy()) -> None:
        if policy.controls_per_event != 5:
            raise ValueError("scientific combinations require exactly five controls")
        self._policy = policy
        self._composer = ComposeScientificCombinationBatch()
        self._controls = BuildMatchedControls(
            controls_per_event=5,
            scenario_exclusion_window=timedelta(minutes=5),
        )
        self._portfolio = AssessEvidencePortfolio(policy)

    def execute(
        self,
        request: EvaluateScientificCombinationPortfolioRequest,
    ) -> ScientificCombinationPortfolio:
        report = request.report
        source_outcomes = {
            outcome.observation_id: outcome for outcome in report.outcomes
        }
        observations = self._compose_observations(request)
        outcomes = tuple(
            self._outcome(
                observation,
                source_outcomes=source_outcomes,
                round_trip_cost_bps=report.policy.round_trip_cost_bps,
            )
            for observation in observations
        )
        outcome_by_id = {item.observation_id: item for item in outcomes}

        prepared: list[_PreparedCombinationEvidence] = []
        for combination_id in sorted(
            request.combination_ids, key=lambda item: item.value
        ):
            definition = preregistered_combination_definition(combination_id)
            for horizon in definition.horizons_seconds:
                prepared.append(
                    self._prepare_evidence(
                        report=report,
                        observations=tuple(
                            item
                            for item in observations
                            if item.combination_id is combination_id
                            and item.horizon_seconds == horizon
                        ),
                        outcomes=outcome_by_id,
                        combination_id=combination_id,
                        horizon_seconds=horizon,
                        cost_model_version=request.cost_model_version,
                    )
                )

        bundles = self._portfolio.execute(
            tuple(item.request for item in prepared)
        )
        results = tuple(
            CombinationEvidenceResult(
                combination_id=item.combination_id,
                combination_version=item.combination_id.version,
                horizon_seconds=item.horizon_seconds,
                statistical_state=_statistical_state(bundle.decision),
                comparison_hypotheses=(
                    preregistered_combination_definition(
                        item.combination_id
                    ).comparison_hypothesis_ids
                ),
                abstain_policy_version=COMBINATION_ABSTAIN_POLICY_VERSION,
                coverage=item.coverage,
                control_matches=_control_matches(item.request),
                evidence=bundle,
            )
            for item, bundle in zip(prepared, bundles, strict=True)
        )
        portfolio_fingerprint = _portfolio_fingerprint(
            report=report,
            cost_model_version=request.cost_model_version,
            observations=observations,
            outcomes=outcomes,
            results=results,
            policy=self._policy,
        )
        return ScientificCombinationPortfolio(
            evidence_version=COMBINATION_EVIDENCE_VERSION,
            dataset_fingerprint=report.dataset_fingerprint,
            source_report_fingerprint=report.report_fingerprint,
            portfolio_fingerprint=portfolio_fingerprint,
            cost_model_version=request.cost_model_version,
            observations=observations,
            outcomes=outcomes,
            results=results,
        )

    def _compose_observations(
        self,
        request: EvaluateScientificCombinationPortfolioRequest,
    ) -> tuple[ScientificCombinationObservation, ...]:
        report = request.report
        component_index: defaultdict[
            tuple[date, ProspectiveHypothesis, str, int],
            list[ProspectiveFeature],
        ] = defaultdict(list)
        for feature in report.features:
            component_index[
                (
                    feature.trading_day,
                    feature.hypothesis,
                    feature.ticker,
                    feature.horizon_seconds,
                )
            ].append(feature)

        composition_requests: list[ComposeScientificCombinationRequest] = []
        seen: set[tuple[object, ...]] = set()
        for combination_id in sorted(
            request.combination_ids, key=lambda item: item.value
        ):
            definition = preregistered_combination_definition(combination_id)
            anchors = _combination_anchors(
                combination_id,
                report.features,
                definition.horizons_seconds,
                definition.comparison_hypothesis_ids,
            )
            for anchor in anchors:
                key = (
                    combination_id,
                    anchor.ticker,
                    anchor.trading_day,
                    anchor.observed_at,
                    anchor.horizon_seconds,
                )
                if key in seen:
                    continue
                seen.add(key)
                uses_market_context = any(
                    item.role is CombinationComponentRole.MARKET_CONTEXT
                    for item in definition.requirements
                )
                market_context_scope = _combination_context_scope(
                    combination_id=combination_id,
                    primary_scope=anchor.ticker,
                    default_market_context_scope=request.market_context_scope,
                    uses_market_context=uses_market_context,
                )
                components: list[ProspectiveFeature] = []
                for requirement in definition.requirements:
                    scope = (
                        anchor.ticker
                        if requirement.role is CombinationComponentRole.PRIMARY
                        else market_context_scope
                    )
                    if scope is None:
                        continue
                    requirement_horizon = requirement.horizon_for(
                        anchor.horizon_seconds
                    )
                    for feature in component_index.get(
                        (
                            anchor.trading_day,
                            requirement.hypothesis,
                            scope,
                            requirement_horizon,
                        ),
                        (),
                    ):
                        components.append(feature)
                composition_requests.append(
                    ComposeScientificCombinationRequest(
                        combination_id=combination_id,
                        primary_scope=anchor.ticker,
                        market_context_scope=market_context_scope,
                        trading_day=anchor.trading_day,
                        observed_at=anchor.observed_at,
                        horizon_seconds=anchor.horizon_seconds,
                        components=tuple(components),
                    )
                )
        return self._composer.execute(
            ComposeScientificCombinationBatchRequest(tuple(composition_requests))
        )

    @staticmethod
    def _outcome(
        observation: ScientificCombinationObservation,
        *,
        source_outcomes: Mapping[str, ProspectiveOutcome],
        round_trip_cost_bps: float,
    ) -> CombinationOutcomeRecord:
        definition = preregistered_combination_definition(
            observation.combination_id
        )
        primary_components = tuple(
            item
            for item in observation.components
            if item.hypothesis in definition.comparison_hypothesis_ids
            and (
                observation.decision is not ProspectiveDecision.MATCHED
                or item.expected_direction == observation.expected_direction
            )
        )
        source = next(
            (
                item
                for item in primary_components
                if item.decision is ProspectiveDecision.MATCHED
            ),
            primary_components[0] if primary_components else None,
        )
        if source is None:
            return CombinationOutcomeRecord(
                observation_id=observation.observation_id,
                combination_id=observation.combination_id,
                horizon_seconds=observation.horizon_seconds,
                target_at=observation.target_at,
                available=False,
                reason_code="directional_basis_unavailable",
                source_observation_id=None,
                forward_return_bps=None,
                net_directional_return_bps=None,
            )
        source_outcome = source_outcomes.get(source.observation_id)
        if (
            source_outcome is None
            or not source_outcome.available
            or source_outcome.target_at != observation.target_at
        ):
            return CombinationOutcomeRecord(
                observation_id=observation.observation_id,
                combination_id=observation.combination_id,
                horizon_seconds=observation.horizon_seconds,
                target_at=observation.target_at,
                available=False,
                reason_code="directional_outcome_unavailable",
                source_observation_id=source.observation_id,
                forward_return_bps=None,
                net_directional_return_bps=None,
            )
        try:
            forward = source_outcome.metric("forward_return").value
        except KeyError:
            return CombinationOutcomeRecord(
                observation_id=observation.observation_id,
                combination_id=observation.combination_id,
                horizon_seconds=observation.horizon_seconds,
                target_at=observation.target_at,
                available=False,
                reason_code="forward_return_unavailable",
                source_observation_id=source.observation_id,
                forward_return_bps=None,
                net_directional_return_bps=None,
            )
        net = (
            observation.expected_direction * forward - round_trip_cost_bps
            if observation.decision is ProspectiveDecision.MATCHED
            else None
        )
        return CombinationOutcomeRecord(
            observation_id=observation.observation_id,
            combination_id=observation.combination_id,
            horizon_seconds=observation.horizon_seconds,
            target_at=observation.target_at,
            available=True,
            reason_code="available",
            source_observation_id=source.observation_id,
            forward_return_bps=forward,
            net_directional_return_bps=net,
        )

    def _prepare_evidence(
        self,
        *,
        report: ProspectiveScientificReport,
        observations: tuple[ScientificCombinationObservation, ...],
        outcomes: Mapping[str, CombinationOutcomeRecord],
        combination_id: ScientificCombinationId,
        horizon_seconds: int,
        cost_model_version: str,
    ) -> _PreparedCombinationEvidence:
        definition = preregistered_combination_definition(combination_id)
        holdout = tuple(
            item
            for item in observations
            if report.split.partition_for(item.trading_day) is DatasetPartition.HOLDOUT
        )
        decisions = Counter(item.decision for item in holdout)
        reasons = Counter(
            item.reason.value
            for item in holdout
            if item.decision is ProspectiveDecision.ABSTAIN
        )
        available = sum(outcomes[item.observation_id].available for item in holdout)
        matched_observations = tuple(
            item for item in holdout if item.decision is ProspectiveDecision.MATCHED
        )
        events: list[StudyPoint] = []
        unavailable_event_ids: list[str] = []
        source_features = {item.observation_id: item for item in report.features}
        for observation in matched_observations:
            outcome = outcomes[observation.observation_id]
            if outcome.net_directional_return_bps is None:
                unavailable_event_ids.append(observation.observation_id)
                reasons[outcome.reason_code] += 1
                continue
            source = source_features.get(outcome.source_observation_id or "")
            if source is None:
                unavailable_event_ids.append(observation.observation_id)
                reasons["directional_basis_unavailable"] += 1
                continue
            events.append(
                _combination_study_point(
                    observation,
                    source,
                    net_effect=outcome.net_directional_return_bps,
                    cost_model_version=cost_model_version,
                )
            )

        candidates = _standalone_candidates(
            report,
            hypotheses=definition.comparison_hypothesis_ids,
            horizon_seconds=horizon_seconds,
            cost_model_version=cost_model_version,
        )
        with_exclusions = tuple(
            replace(
                candidate,
                nearby_scenario_ids=tuple(
                    event.scenario_id
                    for event in events
                    if event.scenario_id is not None
                    and event.instrument_id == candidate.instrument_id
                    and abs(event.occurred_at - candidate.occurred_at)
                    <= timedelta(minutes=5)
                ),
            )
            for candidate in candidates
        )
        matched = self._controls.execute(events, with_exclusions)
        unmatched = tuple(unavailable_event_ids) + matched.unmatched_event_ids
        if matched.unmatched_event_ids:
            reasons["standalone_controls_unavailable"] += len(
                matched.unmatched_event_ids
            )
        coverage = CombinationEvidenceCoverage(
            total_observations=len(holdout),
            matched_observations=decisions[ProspectiveDecision.MATCHED],
            not_matched_observations=decisions[ProspectiveDecision.NOT_MATCHED],
            abstained_observations=decisions[ProspectiveDecision.ABSTAIN],
            available_outcomes=available,
            eligible_events=len(matched_observations),
            matched_events=len(matched.groups),
            standalone_candidates=len(with_exclusions),
            reasons_histogram=tuple(
                EvidenceReasonCount(reason_code=code, count=count)
                for code, count in sorted(reasons.items())
                if count > 0
            ),
        )
        request = EvidenceRequest(
            hypothesis_id=f"{combination_id.value}:{horizon_seconds}",
            hypothesis_version=combination_id.version,
            dataset_fingerprint=report.dataset_fingerprint,
            groups=matched.groups,
            expected_eligible_events=len(matched_observations),
            unmatched_event_ids=unmatched,
            # The selection rate is intentionally measured against every
            # composed holdout observation, including abstentions.  A tiny,
            # highly selective sample must not look like high quality.
            total_available_observations=len(holdout) or None,
            diagnostics_input=EvidenceDiagnosticsInput(
                total_observation_count=len(holdout),
                available_observation_count=available,
                eligible_event_count=len(events),
                reasons_histogram=coverage.reasons_histogram,
            ),
        )
        return _PreparedCombinationEvidence(
            combination_id=combination_id,
            horizon_seconds=horizon_seconds,
            request=request,
            coverage=coverage,
        )


@dataclass(slots=True)
class _StreamingEvidenceAccumulator:
    combination_id: ScientificCombinationId
    horizon_seconds: int
    total_observations: int = 0
    matched_observations: int = 0
    not_matched_observations: int = 0
    abstained_observations: int = 0
    available_outcomes: int = 0
    reasons: Counter[str] = field(default_factory=Counter)
    events: list[StudyPoint] = field(default_factory=list)
    unavailable_event_ids: list[str] = field(default_factory=list)
    candidates: list[StudyPoint] = field(default_factory=list)


class EvaluateScientificCombinationPartitions:
    """Bounded-memory C1-C5 evaluation over trading-day partitions.

    Only event and standalone-control points survive the current partition.
    Full feature graphs and composed observations are sealed by the artifact
    port before the next trading day is requested.
    """

    def __init__(
        self,
        *,
        artifacts: ScientificCombinationStreamingArtifactPort,
        policy: EvidenceGatePolicy = EvidenceGatePolicy(),
    ) -> None:
        if policy.controls_per_event != 5:
            raise ValueError("scientific combinations require exactly five controls")
        self._artifacts = artifacts
        self._policy = policy
        self._composer = ComposeScientificCombinationBatch()
        self._controls = BuildMatchedControls(
            controls_per_event=5,
            scenario_exclusion_window=timedelta(minutes=5),
        )
        self._portfolio = AssessEvidencePortfolio(policy)

    def execute(
        self,
        source: ProspectiveScientificPartitionSourcePort,
        *,
        cost_model_version: str,
        combination_ids: tuple[ScientificCombinationId, ...] = tuple(
            ScientificCombinationId
        ),
        market_context_scope: str = "MOEX",
    ) -> ScientificCombinationStreamingCompletion:
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        if not combination_ids or len(set(combination_ids)) != len(combination_ids):
            raise ValueError("combination ids must be non-empty and unique")
        if not market_context_scope.strip():
            raise ValueError("market_context_scope must not be empty")
        descriptor = source.describe()
        run_id = scientific_combination_streaming_run_id(
            descriptor,
            cost_model_version=cost_model_version,
            combination_ids=combination_ids,
            market_context_scope=market_context_scope,
            evidence_policy=self._policy,
        )
        completed = self._artifacts.load_completed(run_id)
        if completed is not None:
            return completed

        accumulators = {
            (combination_id, horizon): _StreamingEvidenceAccumulator(
                combination_id=combination_id,
                horizon_seconds=horizon,
            )
            for combination_id in sorted(combination_ids, key=lambda item: item.value)
            for horizon in preregistered_combination_definition(
                combination_id
            ).horizons_seconds
        }
        partition_count = 0
        observation_count = 0
        previous_day: date | None = None
        for partition in source.iter_partitions():
            if previous_day is not None and partition.trading_day <= previous_day:
                raise ValueError("prospective partitions must be strictly ordered")
            previous_day = partition.trading_day
            observations = _compose_feature_partition(
                self._composer,
                partition.features,
                combination_ids=combination_ids,
                market_context_scope=market_context_scope,
            )
            source_outcomes = {
                outcome.observation_id: outcome for outcome in partition.outcomes
            }
            outcomes = tuple(
                EvaluateScientificCombinationPortfolio._outcome(
                    observation,
                    source_outcomes=source_outcomes,
                    round_trip_cost_bps=descriptor.policy.round_trip_cost_bps,
                )
                for observation in observations
            )
            self._artifacts.stage_partition(
                run_id,
                descriptor,
                ScientificCombinationPartitionArtifact(
                    trading_day=partition.trading_day,
                    observations=observations,
                    outcomes=outcomes,
                ),
            )
            partition_count += 1
            observation_count += len(observations)
            if (
                descriptor.split.partition_for(partition.trading_day)
                is not DatasetPartition.HOLDOUT
            ):
                continue
            self._accumulate_holdout(
                accumulators,
                descriptor=descriptor,
                partition=partition,
                observations=observations,
                outcomes=outcomes,
                cost_model_version=cost_model_version,
            )

        prepared = tuple(
            self._prepare_streaming(accumulator, descriptor=descriptor)
            for accumulator in accumulators.values()
        )
        bundles = self._portfolio.execute(tuple(item.request for item in prepared))
        results = tuple(
            CombinationEvidenceResult(
                combination_id=item.combination_id,
                combination_version=item.combination_id.version,
                horizon_seconds=item.horizon_seconds,
                statistical_state=_statistical_state(bundle.decision),
                comparison_hypotheses=(
                    preregistered_combination_definition(
                        item.combination_id
                    ).comparison_hypothesis_ids
                ),
                abstain_policy_version=COMBINATION_ABSTAIN_POLICY_VERSION,
                coverage=item.coverage,
                control_matches=_control_matches(item.request),
                evidence=bundle,
            )
            for item, bundle in zip(prepared, bundles, strict=True)
        )
        return self._artifacts.complete(
            run_id,
            descriptor,
            results,
            cost_model_version=cost_model_version,
            partition_count=partition_count,
            observation_count=observation_count,
        )

    @staticmethod
    def _accumulate_holdout(
        accumulators: Mapping[
            tuple[ScientificCombinationId, int], _StreamingEvidenceAccumulator
        ],
        *,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
        partition: ProspectiveScientificPartition,
        observations: tuple[ScientificCombinationObservation, ...],
        outcomes: tuple[CombinationOutcomeRecord, ...],
        cost_model_version: str,
    ) -> None:
        outcome_by_id = {item.observation_id: item for item in outcomes}
        source_features = {item.observation_id: item for item in partition.features}
        for observation in observations:
            accumulator = accumulators[
                (observation.combination_id, observation.horizon_seconds)
            ]
            accumulator.total_observations += 1
            if observation.decision is ProspectiveDecision.MATCHED:
                accumulator.matched_observations += 1
            elif observation.decision is ProspectiveDecision.NOT_MATCHED:
                accumulator.not_matched_observations += 1
            else:
                accumulator.abstained_observations += 1
                accumulator.reasons[observation.reason.value] += 1
            outcome = outcome_by_id[observation.observation_id]
            if outcome.available:
                accumulator.available_outcomes += 1
            if observation.decision is not ProspectiveDecision.MATCHED:
                continue
            if outcome.net_directional_return_bps is None:
                accumulator.unavailable_event_ids.append(observation.observation_id)
                accumulator.reasons[outcome.reason_code] += 1
                continue
            source = source_features.get(outcome.source_observation_id or "")
            if source is None:
                accumulator.unavailable_event_ids.append(observation.observation_id)
                accumulator.reasons["directional_basis_unavailable"] += 1
                continue
            accumulator.events.append(
                _combination_study_point(
                    observation,
                    source,
                    net_effect=outcome.net_directional_return_bps,
                    cost_model_version=cost_model_version,
                )
            )

        for accumulator in accumulators.values():
            definition = preregistered_combination_definition(
                accumulator.combination_id
            )
            accumulator.candidates.extend(
                _standalone_candidates_from_partition(
                    partition,
                    hypotheses=definition.comparison_hypothesis_ids,
                    horizon_seconds=accumulator.horizon_seconds,
                    round_trip_cost_bps=descriptor.policy.round_trip_cost_bps,
                    cost_model_version=cost_model_version,
                )
            )

    def _prepare_streaming(
        self,
        accumulator: _StreamingEvidenceAccumulator,
        *,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
    ) -> _PreparedCombinationEvidence:
        with_exclusions = tuple(
            replace(
                candidate,
                nearby_scenario_ids=tuple(
                    event.scenario_id
                    for event in accumulator.events
                    if event.scenario_id is not None
                    and event.instrument_id == candidate.instrument_id
                    and abs(event.occurred_at - candidate.occurred_at)
                    <= timedelta(minutes=5)
                ),
            )
            for candidate in accumulator.candidates
        )
        matched = self._controls.execute(accumulator.events, with_exclusions)
        unmatched = (
            tuple(accumulator.unavailable_event_ids) + matched.unmatched_event_ids
        )
        if matched.unmatched_event_ids:
            accumulator.reasons["standalone_controls_unavailable"] += len(
                matched.unmatched_event_ids
            )
        coverage = CombinationEvidenceCoverage(
            total_observations=accumulator.total_observations,
            matched_observations=accumulator.matched_observations,
            not_matched_observations=accumulator.not_matched_observations,
            abstained_observations=accumulator.abstained_observations,
            available_outcomes=accumulator.available_outcomes,
            eligible_events=accumulator.matched_observations,
            matched_events=len(matched.groups),
            standalone_candidates=len(with_exclusions),
            reasons_histogram=tuple(
                EvidenceReasonCount(reason_code=code, count=count)
                for code, count in sorted(accumulator.reasons.items())
                if count > 0
            ),
        )
        request = EvidenceRequest(
            hypothesis_id=(
                f"{accumulator.combination_id.value}:"
                f"{accumulator.horizon_seconds}"
            ),
            hypothesis_version=accumulator.combination_id.version,
            dataset_fingerprint=descriptor.dataset_fingerprint,
            groups=matched.groups,
            expected_eligible_events=accumulator.matched_observations,
            unmatched_event_ids=unmatched,
            total_available_observations=accumulator.total_observations or None,
            diagnostics_input=EvidenceDiagnosticsInput(
                total_observation_count=accumulator.total_observations,
                available_observation_count=accumulator.available_outcomes,
                eligible_event_count=len(accumulator.events),
                reasons_histogram=coverage.reasons_histogram,
            ),
        )
        return _PreparedCombinationEvidence(
            combination_id=accumulator.combination_id,
            horizon_seconds=accumulator.horizon_seconds,
            request=request,
            coverage=coverage,
        )


def scientific_combination_streaming_run_id(
    descriptor: ProspectiveScientificPartitionSourceDescriptor,
    *,
    cost_model_version: str,
    combination_ids: tuple[ScientificCombinationId, ...],
    market_context_scope: str,
    evidence_policy: EvidenceGatePolicy,
) -> str:
    payload = {
        "version": COMBINATION_EVIDENCE_VERSION,
        "dataset_fingerprint": descriptor.dataset_fingerprint,
        "source_report_fingerprint": descriptor.source_report_fingerprint,
        "cost_model_version": cost_model_version,
        "combination_ids": [
            item.value for item in sorted(combination_ids, key=lambda item: item.value)
        ],
        "market_context_scope": market_context_scope,
        "abstain_policy": COMBINATION_ABSTAIN_POLICY_VERSION,
        "evidence_policy": {
            key: getattr(evidence_policy, key)
            for key in evidence_policy.__dataclass_fields__
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()


def _compose_feature_partition(
    composer: ComposeScientificCombinationBatch,
    features: tuple[ProspectiveFeature, ...],
    *,
    combination_ids: tuple[ScientificCombinationId, ...],
    market_context_scope: str,
) -> tuple[ScientificCombinationObservation, ...]:
    component_index: defaultdict[
        tuple[date, ProspectiveHypothesis, str, int],
        list[ProspectiveFeature],
    ] = defaultdict(list)
    for feature in features:
        component_index[
            (
                feature.trading_day,
                feature.hypothesis,
                feature.ticker,
                feature.horizon_seconds,
            )
        ].append(feature)
    requests: list[ComposeScientificCombinationRequest] = []
    seen: set[tuple[object, ...]] = set()
    for combination_id in sorted(combination_ids, key=lambda item: item.value):
        definition = preregistered_combination_definition(combination_id)
        anchors = _combination_anchors(
            combination_id,
            features,
            definition.horizons_seconds,
            definition.comparison_hypothesis_ids,
        )
        for anchor in anchors:
            key = (
                combination_id,
                anchor.ticker,
                anchor.trading_day,
                anchor.observed_at,
                anchor.horizon_seconds,
            )
            if key in seen:
                continue
            seen.add(key)
            uses_market_context = any(
                item.role is CombinationComponentRole.MARKET_CONTEXT
                for item in definition.requirements
            )
            context_scope = _combination_context_scope(
                combination_id=combination_id,
                primary_scope=anchor.ticker,
                default_market_context_scope=market_context_scope,
                uses_market_context=uses_market_context,
            )
            components: list[ProspectiveFeature] = []
            for requirement in definition.requirements:
                scope = (
                    anchor.ticker
                    if requirement.role is CombinationComponentRole.PRIMARY
                    else context_scope
                )
                if scope is None:
                    continue
                components.extend(
                    component_index.get(
                        (
                            anchor.trading_day,
                            requirement.hypothesis,
                            scope,
                            requirement.horizon_for(anchor.horizon_seconds),
                        ),
                        (),
                    )
                )
            requests.append(
                ComposeScientificCombinationRequest(
                    combination_id=combination_id,
                    primary_scope=anchor.ticker,
                    market_context_scope=context_scope,
                    trading_day=anchor.trading_day,
                    observed_at=anchor.observed_at,
                    horizon_seconds=anchor.horizon_seconds,
                    components=tuple(components),
                )
            )
    return composer.execute(ComposeScientificCombinationBatchRequest(tuple(requests)))


def _combination_anchors(
    combination_id: ScientificCombinationId,
    features: Iterable[ProspectiveFeature],
    horizons_seconds: tuple[int, ...],
    comparison_hypotheses: tuple[ProspectiveHypothesis, ...],
) -> tuple[ProspectiveFeature, ...]:
    """Return the causal primary clock for one registered composition.

    C5 is clocked by H12V2 because its primary scope is the sealed pair.  H11V2
    is resolved for the left member of that pair at exactly the same completed
    candle boundary.  It must never create an independent stock-timed C5 row.
    """

    anchors = (
        (ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,)
        if combination_id is ScientificCombinationId.C5
        else comparison_hypotheses
    )
    return tuple(
        feature
        for feature in features
        if feature.hypothesis in anchors
        and feature.horizon_seconds in horizons_seconds
    )


def _combination_context_scope(
    *,
    combination_id: ScientificCombinationId,
    primary_scope: str,
    default_market_context_scope: str,
    uses_market_context: bool,
) -> str | None:
    if not uses_market_context:
        return None
    if combination_id is ScientificCombinationId.C5:
        left, separator, right = primary_scope.partition("/")
        if not separator or not left.strip() or not right.strip():
            return None
        return left
    return default_market_context_scope


@dataclass(frozen=True, slots=True)
class _PreparedCombinationEvidence:
    combination_id: ScientificCombinationId
    horizon_seconds: int
    request: EvidenceRequest
    coverage: CombinationEvidenceCoverage


def _standalone_candidates(
    report: ProspectiveScientificReport,
    *,
    hypotheses: tuple[ProspectiveHypothesis, ...],
    horizon_seconds: int,
    cost_model_version: str,
) -> tuple[StudyPoint, ...]:
    candidates: list[StudyPoint] = []
    for feature, outcome in zip(report.features, report.outcomes, strict=True):
        if feature.hypothesis not in hypotheses:
            continue
        if feature.horizon_seconds != horizon_seconds:
            continue
        if report.split.partition_for(feature.trading_day) is not DatasetPartition.HOLDOUT:
            continue
        if feature.decision is not ProspectiveDecision.MATCHED or not outcome.available:
            continue
        try:
            forward = outcome.metric("forward_return").value
        except KeyError:
            continue
        candidates.append(
            _feature_study_point(
                feature,
                net_effect=(
                    feature.expected_direction * forward
                    - report.policy.round_trip_cost_bps
                ),
                cost_model_version=cost_model_version,
            )
        )
    return tuple(
        sorted(candidates, key=lambda item: (item.occurred_at, item.point_id))
    )


def _standalone_candidates_from_partition(
    partition: ProspectiveScientificPartition,
    *,
    hypotheses: tuple[ProspectiveHypothesis, ...],
    horizon_seconds: int,
    round_trip_cost_bps: float,
    cost_model_version: str,
) -> tuple[StudyPoint, ...]:
    candidates: list[StudyPoint] = []
    for feature, outcome in zip(
        partition.features, partition.outcomes, strict=True
    ):
        if feature.hypothesis not in hypotheses:
            continue
        if feature.horizon_seconds != horizon_seconds:
            continue
        if feature.decision is not ProspectiveDecision.MATCHED or not outcome.available:
            continue
        try:
            forward = outcome.metric("forward_return").value
        except KeyError:
            continue
        candidates.append(
            _feature_study_point(
                feature,
                net_effect=(
                    feature.expected_direction * forward - round_trip_cost_bps
                ),
                cost_model_version=cost_model_version,
            )
        )
    return tuple(
        sorted(candidates, key=lambda item: (item.occurred_at, item.point_id))
    )


def _combination_study_point(
    observation: ScientificCombinationObservation,
    source: ProspectiveFeature,
    *,
    net_effect: float,
    cost_model_version: str,
) -> StudyPoint:
    return StudyPoint(
        point_id=observation.observation_id,
        scenario_id=observation.observation_id,
        instrument_id=observation.primary_scope,
        occurred_at=observation.observed_at,
        trading_day=observation.trading_day,
        session_bucket=_session_bucket(observation.observed_at),
        volatility_bucket="registered_standalone_basis",
        liquidity_bucket=(
            f"direction_{observation.expected_direction:+d}_horizon_"
            f"{observation.horizon_seconds}"
        ),
        features_observed_at=(
            observation.max_used_observed_at or source.feature_max_observed_at
        ),
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net_effect,
        cost_model_version=cost_model_version,
    )


def _feature_study_point(
    feature: ProspectiveFeature,
    *,
    net_effect: float,
    cost_model_version: str,
) -> StudyPoint:
    return StudyPoint(
        point_id=feature.observation_id,
        scenario_id=feature.observation_id,
        instrument_id=feature.ticker,
        occurred_at=feature.observed_at,
        trading_day=feature.trading_day,
        session_bucket=_session_bucket(feature.observed_at),
        volatility_bucket="registered_standalone_basis",
        liquidity_bucket=(
            f"direction_{feature.expected_direction:+d}_horizon_"
            f"{feature.horizon_seconds}"
        ),
        features_observed_at=feature.feature_max_observed_at,
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net_effect,
        cost_model_version=cost_model_version,
    )


def _session_bucket(observed_at: datetime) -> str:
    local = observed_at.astimezone(
        ZoneInfo(MOEX_EQUITY_PHASE_SCHEDULE_V1.timezone_name)
    )
    phase = MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(observed_at).value
    return f"{phase}:{local.hour:02d}"


def _control_matches(request: EvidenceRequest) -> tuple[CombinationControlMatch, ...]:
    return tuple(
        CombinationControlMatch(
            event_observation_id=group.event.point_id,
            standalone_observation_ids=tuple(
                item.point_id for item in group.controls
            ),
            event_net_bps=group.event.net_effect_bps,
            standalone_mean_net_bps=group.control_mean_bps,
            incremental_lift_bps=group.lift_bps,
        )
        for group in request.groups
    )


def _statistical_state(decision: EvidenceDecision) -> CombinationStatisticalState:
    return {
        EvidenceDecision.PASSED: CombinationStatisticalState.PASSED,
        EvidenceDecision.REJECTED: CombinationStatisticalState.REJECTED,
        EvidenceDecision.INCONCLUSIVE: CombinationStatisticalState.UNCERTAIN,
        EvidenceDecision.BLOCKED_BY_DATA: CombinationStatisticalState.BLOCKED_DATA,
    }[decision]


def _portfolio_fingerprint(
    *,
    report: ProspectiveScientificReport,
    cost_model_version: str,
    observations: tuple[ScientificCombinationObservation, ...],
    outcomes: tuple[CombinationOutcomeRecord, ...],
    results: tuple[CombinationEvidenceResult, ...],
    policy: EvidenceGatePolicy,
) -> str:
    payload = {
        "version": COMBINATION_EVIDENCE_VERSION,
        "dataset_fingerprint": report.dataset_fingerprint,
        "report_fingerprint": report.report_fingerprint,
        "cost_model_version": cost_model_version,
        "abstain_policy_version": COMBINATION_ABSTAIN_POLICY_VERSION,
        "policy": {
            key: getattr(policy, key)
            for key in policy.__dataclass_fields__
        },
        "observations": [
            (item.observation_id, item.payload_fingerprint) for item in observations
        ],
        "outcomes": [
            (
                item.observation_id,
                item.available,
                item.reason_code,
                item.source_observation_id,
                item.forward_return_bps,
                item.net_directional_return_bps,
            )
            for item in outcomes
        ],
        "results": [
            (
                item.combination_id.value,
                item.horizon_seconds,
                item.statistical_state.value,
                item.evidence.evidence_id,
                item.evidence.reason_codes,
                item.evidence.mean_lift_bps,
            )
            for item in results
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()
