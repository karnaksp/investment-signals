"""Independent evidence gate for prospective scientific model versions.

The use case consumes already sealed causal features and outcomes.  Treatment
thresholds and matching strata are derived without holdout outcomes; only the
independent holdout contributes effect values to the evidence portfolio.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import timedelta
from enum import Enum
from typing import Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildMatchedControls,
    EvidenceDiagnosticsInput,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    INDEPENDENT_PARTITIONED_HYPOTHESES,
    ProspectiveScientificReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    EvidenceBundle,
    EvidenceReasonCount,
    StudyPoint,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveScientificPolicy,
    TargetMetric,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
)


class ProspectiveClaimFamily(str, Enum):
    DIRECTIONAL = "directional"
    ACTIVITY = "activity"


class ProspectiveEffectUnit(str, Enum):
    BASIS_POINTS = "basis_points"
    VARIANCE_UPLIFT_RATIO_X_10000 = "variance_uplift_ratio_x_10000"
    QLIKE_IMPROVEMENT_X_10000 = "qlike_improvement_x_10000"


@dataclass(frozen=True, slots=True)
class ProspectiveEvidenceDefinition:
    hypothesis: ProspectiveHypothesis
    catalog_hypothesis_id: str
    claim_family: ProspectiveClaimFamily
    effect_unit: ProspectiveEffectUnit
    claim_scope: str
    target_metric: TargetMetric
    expected_direction: str


PROSPECTIVE_EVIDENCE_DEFINITIONS = {
    ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
            "h1-morning-low-volume-reversion",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "reversion_to_previous_close",
        )
    ),
    ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
            "h2-morning-high-volume-continuation",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "continuation",
        )
    ),
    ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            "h3-jump-low-activity-reversal",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "reversal",
        )
    ),
    ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            "h4-jump-high-activity-continuation",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "continuation",
        )
    ),
    ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
            "h3-jump-low-activity-reversal",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "reversal",
        )
    ),
    ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
            "h4-jump-high-activity-continuation",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "continuation",
        )
    ),
    ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            "h7-relative-volume-future-activity",
            ProspectiveClaimFamily.ACTIVITY,
            ProspectiveEffectUnit.VARIANCE_UPLIFT_RATIO_X_10000,
            "independent_holdout_matched_controls",
            TargetMetric.FUTURE_VARIANCE_UPLIFT,
            "volatility_increase",
        )
    ),
    ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE,
            "h5-same-phase-return-recurrence",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls",
            TargetMetric.FORWARD_RETURN,
            "same_as_prior_phase_mean",
        )
    ),
    ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION,
            "h6-open-close-market-continuation",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls",
            TargetMetric.FORWARD_RETURN,
            "same_as_opening_basket",
        )
    ),
    ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
            "h12-pair-residual-reversion",
            ProspectiveClaimFamily.DIRECTIONAL,
            ProspectiveEffectUnit.BASIS_POINTS,
            "independent_holdout_matched_controls_multi_horizon",
            TargetMetric.FORWARD_RETURN,
            "pair_residual_reversion",
        )
    ),
    ProspectiveHypothesis.HAR_VOLATILITY_V2: ProspectiveEvidenceDefinition(
        ProspectiveHypothesis.HAR_VOLATILITY_V2,
        "h15-har-volatility-forecast-v2",
        ProspectiveClaimFamily.ACTIVITY,
        ProspectiveEffectUnit.QLIKE_IMPROVEMENT_X_10000,
        "independent_holdout_vs_best_and_mean_ewma_phase_qlike",
        TargetMetric.FUTURE_REALIZED_VARIANCE,
        "forecast_loss_reduction",
    ),
    ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
            "h16-negative-semivariance-future-risk",
            ProspectiveClaimFamily.ACTIVITY,
            ProspectiveEffectUnit.VARIANCE_UPLIFT_RATIO_X_10000,
            "independent_holdout_matched_controls",
            TargetMetric.FUTURE_VARIANCE_UPLIFT,
            "volatility_increase",
        )
    ),
    ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE: (
        ProspectiveEvidenceDefinition(
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
            "h17-volatility-jump-persistence",
            ProspectiveClaimFamily.ACTIVITY,
            ProspectiveEffectUnit.VARIANCE_UPLIFT_RATIO_X_10000,
            "independent_holdout_matched_controls",
            TargetMetric.FUTURE_VARIANCE_UPLIFT,
            "volatility_increase",
        )
    ),
}

BOUNDED_CONTROL_REUSE_HYPOTHESES = frozenset(
    {
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
    }
)
BOUNDED_CONTROL_REUSE_LIMIT = 5
BOUNDED_CONTROL_SELECTION_POLICY_VERSION = (
    "bounded-control-reuse-5-rarity-first-exact-strata-exclusion-5m-v2"
)


@dataclass(frozen=True, slots=True)
class ProspectiveEvidenceCoverage:
    hypothesis_id: str
    available_holdout_observations: int
    triggered_events: int
    eligible_common_support_events: int
    unmatched_events: int
    control_candidates: int
    matched_event_ids: tuple[str, ...]
    control_selection_policy_version: str
    maximum_control_reuse: int
    distinct_controls: int
    maximum_observed_control_reuse: int
    mean_control_reuse: float
    independent_control_clusters: int
    minimum_independent_control_clusters: int

    def __post_init__(self) -> None:
        if not self.hypothesis_id.strip():
            raise ValueError("hypothesis_id must not be empty")
        counts = (
            self.available_holdout_observations,
            self.triggered_events,
            self.eligible_common_support_events,
            self.unmatched_events,
            self.control_candidates,
        )
        if any(value < 0 for value in counts):
            raise ValueError("evidence coverage counts must be non-negative")
        if self.eligible_common_support_events + self.unmatched_events != (
            self.triggered_events
        ):
            raise ValueError("every triggered event must be matched or unmatched")
        if len(self.matched_event_ids) != len(set(self.matched_event_ids)):
            raise ValueError("matched event ids must be unique")
        if not self.control_selection_policy_version.strip():
            raise ValueError("control selection policy version must not be empty")
        if self.maximum_control_reuse <= 0:
            raise ValueError("maximum control reuse must be positive")
        if (
            self.distinct_controls < 0
            or self.maximum_observed_control_reuse < 0
            or self.mean_control_reuse < 0.0
            or self.independent_control_clusters < 0
        ):
            raise ValueError("control reuse statistics must not be negative")
        if self.maximum_observed_control_reuse > self.maximum_control_reuse:
            raise ValueError("observed control reuse exceeds configured maximum")
        if self.minimum_independent_control_clusters <= 0:
            raise ValueError("minimum independent control clusters must be positive")

    @property
    def common_support_rate(self) -> float | None:
        if not self.triggered_events:
            return None
        return self.eligible_common_support_events / self.triggered_events


@dataclass(frozen=True, slots=True)
class ProspectiveEvidenceAssessment:
    """All requested hypotheses assessed as one multiple-testing family."""

    bundles: tuple[EvidenceBundle, ...]
    coverage: tuple[ProspectiveEvidenceCoverage, ...]
    requests: tuple[EvidenceRequest, ...]

    def __post_init__(self) -> None:
        bundle_ids = tuple(item.hypothesis_id for item in self.bundles)
        coverage_ids = tuple(item.hypothesis_id for item in self.coverage)
        request_ids = tuple(item.hypothesis_id for item in self.requests)
        if bundle_ids != coverage_ids or bundle_ids != request_ids:
            raise ValueError("evidence bundles, coverage, and requests must align")
        if len(bundle_ids) != len(set(bundle_ids)):
            raise ValueError("evidence hypotheses must be unique")

    def for_hypothesis(self, hypothesis: ProspectiveHypothesis) -> EvidenceBundle:
        return next(
            item for item in self.bundles if item.hypothesis_id == hypothesis.value
        )

    def coverage_for(
        self, hypothesis: ProspectiveHypothesis
    ) -> ProspectiveEvidenceCoverage:
        return next(
            item for item in self.coverage if item.hypothesis_id == hypothesis.value
        )

    def request_for(self, hypothesis: ProspectiveHypothesis) -> EvidenceRequest:
        return next(
            item for item in self.requests if item.hypothesis_id == hypothesis.value
        )


@dataclass(frozen=True, slots=True)
class PreparedProspectiveEvidence:
    """Compact per-hypothesis input retained after its report is released."""

    request: EvidenceRequest
    coverage: ProspectiveEvidenceCoverage

    def __post_init__(self) -> None:
        if self.request.hypothesis_id != self.coverage.hypothesis_id:
            raise ValueError("prepared evidence request and coverage must align")


class AssessProspectiveScientificEvidence:
    """Build five causal controls per event and evaluate the sealed holdout."""

    def __init__(self, policy: EvidenceGatePolicy = EvidenceGatePolicy()) -> None:
        if policy.controls_per_event != 5:
            raise ValueError("prospective evidence requires exactly five controls")
        self.policy = policy
        self._controls = BuildMatchedControls(
            controls_per_event=5,
            scenario_exclusion_window=timedelta(minutes=5),
        )
        self._bounded_reuse_controls = BuildMatchedControls(
            controls_per_event=5,
            scenario_exclusion_window=timedelta(minutes=5),
            maximum_control_reuse=BOUNDED_CONTROL_REUSE_LIMIT,
            selection_policy_version=(BOUNDED_CONTROL_SELECTION_POLICY_VERSION),
        )
        self._portfolio = AssessEvidencePortfolio(policy)

    def execute(
        self,
        report: ProspectiveScientificReport,
        requested_hypotheses: Sequence[ProspectiveHypothesis],
        *,
        cost_model_version: str,
    ) -> ProspectiveEvidenceAssessment:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        if not selected:
            raise ValueError("at least one prospective hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        unavailable = set(selected) - set(report.selected_hypotheses)
        if unavailable:
            raise ValueError("requested hypothesis is absent from research report")

        prepared: list[PreparedProspectiveEvidence] = []
        for hypothesis in selected:
            request, coverage = self._request(
                report,
                hypothesis,
                cost_model_version=cost_model_version,
            )
            prepared.append(
                PreparedProspectiveEvidence(request=request, coverage=coverage)
            )
        return self.assess_prepared(prepared)

    def prepare(
        self,
        report: ProspectiveScientificReport,
        hypothesis: ProspectiveHypothesis,
        cost_model_version: str,
    ) -> PreparedProspectiveEvidence:
        """Prepare one hypothesis so the large source report can be released."""

        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        if hypothesis not in report.selected_hypotheses:
            raise ValueError("requested hypothesis is absent from research report")
        request, coverage = self._request(
            report,
            hypothesis,
            cost_model_version=cost_model_version,
        )
        return PreparedProspectiveEvidence(request=request, coverage=coverage)

    def prepare_rows(
        self,
        rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
        *,
        hypothesis: ProspectiveHypothesis,
        split: ChronologicalSplit,
        policy: ProspectiveScientificPolicy,
        dataset_fingerprint: str,
        cost_model_version: str,
    ) -> PreparedProspectiveEvidence:
        """Prepare dense independent models from an externally ordered stream."""

        if hypothesis not in {
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
        }:
            raise ValueError("streaming evidence is sealed for H3/H4 versions only")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        request, coverage = self._request_from_rows(
            rows,
            hypothesis,
            split=split,
            policy=policy,
            dataset_fingerprint=dataset_fingerprint,
            cost_model_version=cost_model_version,
            event_thresholds={},
            volatility_cutoffs={},
        )
        return PreparedProspectiveEvidence(request=request, coverage=coverage)

    def prepare_replayable_rows(
        self,
        rows: Callable[[], Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]]],
        *,
        hypothesis: ProspectiveHypothesis,
        split: ChronologicalSplit,
        policy: ProspectiveScientificPolicy,
        dataset_fingerprint: str,
        cost_model_version: str,
    ) -> PreparedProspectiveEvidence:
        """Prepare an instrument-local model from a replayable row stream.

        Some models need validation-only volatility strata before matching the
        holdout.  The supplied factory permits a bounded second pass over an
        external spool instead of retaining the full feature/outcome graph.
        """

        if hypothesis not in INDEPENDENT_PARTITIONED_HYPOTHESES:
            raise ValueError("streaming evidence requires an instrument-local model")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        volatility_cutoffs = _volatility_cutoffs_from_rows(
            hypothesis,
            rows,
            split=split,
        )
        request, coverage = self._request_from_rows(
            rows(),
            hypothesis,
            split=split,
            policy=policy,
            dataset_fingerprint=dataset_fingerprint,
            cost_model_version=cost_model_version,
            event_thresholds={},
            volatility_cutoffs=volatility_cutoffs,
        )
        return PreparedProspectiveEvidence(request=request, coverage=coverage)

    def assess_prepared(
        self,
        prepared: Sequence[PreparedProspectiveEvidence],
    ) -> ProspectiveEvidenceAssessment:
        """Apply one multiple-testing correction to all prepared hypotheses."""

        if not prepared:
            raise ValueError("at least one prepared hypothesis is required")
        ordered = tuple(sorted(prepared, key=lambda item: item.request.test_id))
        requests = tuple(item.request for item in ordered)
        return ProspectiveEvidenceAssessment(
            bundles=self._portfolio.execute(requests),
            coverage=tuple(item.coverage for item in ordered),
            requests=requests,
        )

    def _request(
        self,
        report: ProspectiveScientificReport,
        hypothesis: ProspectiveHypothesis,
        *,
        cost_model_version: str,
    ) -> tuple[EvidenceRequest, ProspectiveEvidenceCoverage]:
        validation = tuple(
            (feature, outcome)
            for feature, outcome in zip(
                report.features,
                report.outcomes,
                strict=True,
            )
            if feature.hypothesis is hypothesis
            if report.split.partition_for(feature.trading_day)
            is DatasetPartition.VALIDATION
        )
        event_thresholds = _event_thresholds(hypothesis, validation)
        volatility_cutoffs = _volatility_cutoffs(hypothesis, validation)
        return self._request_from_rows(
            zip(report.features, report.outcomes, strict=True),
            hypothesis,
            split=report.split,
            policy=report.policy,
            dataset_fingerprint=report.dataset_fingerprint,
            cost_model_version=cost_model_version,
            event_thresholds=event_thresholds,
            volatility_cutoffs=volatility_cutoffs,
        )

    def _request_from_rows(
        self,
        rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
        hypothesis: ProspectiveHypothesis,
        *,
        split: ChronologicalSplit,
        policy: ProspectiveScientificPolicy,
        dataset_fingerprint: str,
        cost_model_version: str,
        event_thresholds: Mapping[tuple[str, str, int], float],
        volatility_cutoffs: Mapping[tuple[str, str, int], tuple[float, float]],
    ) -> tuple[EvidenceRequest, ProspectiveEvidenceCoverage]:
        events: list[StudyPoint] = []
        candidates: list[StudyPoint] = []
        diagnostic_reasons: Counter[str] = Counter()
        holdout_count = 0
        available_holdout_count = 0
        for feature, outcome in rows:
            if (
                feature.hypothesis is not hypothesis
                or split.partition_for(feature.trading_day)
                is not DatasetPartition.HOLDOUT
            ):
                continue
            holdout_count += 1
            if not outcome.available:
                diagnostic_reasons[outcome.reason.value] += 1
                continue
            available_holdout_count += 1
            effect = _effect_value(
                feature,
                outcome,
                round_trip_cost_bps=policy.round_trip_cost_bps,
            )
            if effect is None:
                diagnostic_reasons[f"feature_{feature.reason.value}"] += 1
                continue
            treated = _is_event(feature, event_thresholds)
            if not treated:
                diagnostic_reasons["event_condition_not_met"] += 1
            point = _study_point(
                feature,
                net_effect=effect,
                cost_model_version=cost_model_version,
                volatility_cutoffs=volatility_cutoffs,
                treated=treated,
            )
            (events if treated else candidates).append(point)

        candidates_with_exclusions = _with_scenario_exclusions(candidates, events)
        controls = (
            self._bounded_reuse_controls
            if hypothesis in BOUNDED_CONTROL_REUSE_HYPOTHESES
            else self._controls
        )
        matched = controls.execute(events, candidates_with_exclusions)
        reuse = matched.reuse_statistics
        minimum_independent_clusters = self.policy.minimum_trading_days
        coverage = ProspectiveEvidenceCoverage(
            hypothesis_id=hypothesis.value,
            available_holdout_observations=available_holdout_count,
            triggered_events=len(events),
            eligible_common_support_events=len(matched.groups),
            unmatched_events=len(matched.unmatched_event_ids),
            control_candidates=len(candidates_with_exclusions),
            matched_event_ids=tuple(group.event.point_id for group in matched.groups),
            control_selection_policy_version=matched.selection_policy_version,
            maximum_control_reuse=matched.maximum_control_reuse,
            distinct_controls=reuse.distinct_controls,
            maximum_observed_control_reuse=reuse.maximum_reuse,
            mean_control_reuse=reuse.mean_reuse,
            independent_control_clusters=reuse.independent_clusters,
            minimum_independent_control_clusters=minimum_independent_clusters,
        )
        return (
            EvidenceRequest(
                hypothesis_id=hypothesis.value,
                hypothesis_version=hypothesis.version,
                dataset_fingerprint=dataset_fingerprint,
                groups=matched.groups,
                expected_eligible_events=len(events),
                unmatched_event_ids=matched.unmatched_event_ids,
                total_available_observations=available_holdout_count or None,
                diagnostics_input=EvidenceDiagnosticsInput(
                    total_observation_count=holdout_count,
                    available_observation_count=available_holdout_count,
                    eligible_event_count=len(events),
                    reasons_histogram=tuple(
                        EvidenceReasonCount(reason_code=reason, count=count)
                        for reason, count in sorted(diagnostic_reasons.items())
                    ),
                ),
                control_reuse_statistics=reuse,
                control_selection_policy_version=(matched.selection_policy_version),
                maximum_control_reuse=matched.maximum_control_reuse,
                minimum_independent_control_clusters=(minimum_independent_clusters),
            ),
            coverage,
        )


def _with_scenario_exclusions(
    candidates: Sequence[StudyPoint],
    events: Sequence[StudyPoint],
) -> tuple[StudyPoint, ...]:
    """Attach the same five-minute exclusions with an indexed time window."""

    by_instrument: defaultdict[str, list[StudyPoint]] = defaultdict(list)
    for event in events:
        if event.scenario_id is not None:
            by_instrument[event.instrument_id].append(event)
    indexed = {
        instrument: (
            tuple(item.occurred_at for item in ordered),
            tuple(item.scenario_id for item in ordered),
        )
        for instrument, values in by_instrument.items()
        for ordered in (
            tuple(sorted(values, key=lambda item: (item.occurred_at, item.point_id))),
        )
    }
    window = timedelta(minutes=5)
    enriched: list[StudyPoint] = []
    for candidate in candidates:
        times, scenario_ids = indexed.get(candidate.instrument_id, ((), ()))
        start = bisect_left(times, candidate.occurred_at - window)
        stop = bisect_right(times, candidate.occurred_at + window)
        enriched.append(
            replace(
                candidate,
                nearby_scenario_ids=tuple(
                    scenario_id
                    for scenario_id in scenario_ids[start:stop]
                    if scenario_id is not None
                ),
            )
        )
    return tuple(enriched)


def _event_thresholds(
    hypothesis: ProspectiveHypothesis,
    validation: Sequence[tuple[ProspectiveFeature, ProspectiveOutcome]],
) -> Mapping[tuple[str, str, int], float]:
    if hypothesis is not ProspectiveHypothesis.HAR_VOLATILITY_V2:
        return {}
    forecasts: defaultdict[tuple[str, str, int], list[float]] = defaultdict(list)
    for feature, _ in validation:
        if feature.forecast is not None:
            forecasts[_stratum_key(feature)].append(feature.forecast.value)
    return {key: _quantile(values, 0.90) for key, values in sorted(forecasts.items())}


def _is_event(
    feature: ProspectiveFeature,
    thresholds: Mapping[tuple[str, str, int], float],
) -> bool:
    if feature.decision is ProspectiveDecision.ABSTAIN:
        return False
    if feature.hypothesis is ProspectiveHypothesis.HAR_VOLATILITY_V2:
        threshold = thresholds.get(_stratum_key(feature))
        return (
            threshold is not None
            and feature.forecast is not None
            and feature.forecast.value >= threshold
        )
    return feature.decision is ProspectiveDecision.MATCHED


def _effect_value(
    feature: ProspectiveFeature,
    outcome: ProspectiveOutcome,
    *,
    round_trip_cost_bps: float,
) -> float | None:
    if feature.decision is ProspectiveDecision.ABSTAIN or not outcome.available:
        return None
    if feature.hypothesis in {
        ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
        ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
        ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE,
        ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
    }:
        forward = outcome.metric("forward_return").value
        return feature.expected_direction * forward - round_trip_cost_bps
    if feature.hypothesis is ProspectiveHypothesis.HAR_VOLATILITY_V2:
        model = outcome.metric("har_qlike").value
        best_benchmark = min(
            outcome.metric("ewma_qlike").value,
            outcome.metric("phase_qlike").value,
        )
        # Beating the lower-loss (best) benchmark is the conservative primary
        # gate.  It also implies beating the arithmetic mean of both
        # benchmarks, so both comparisons are covered without double-testing
        # correlated versions of the same claim.
        # StudyPoint is a legacy common statistical carrier named in bps.
        # The evidence definition preserves that this is a scaled,
        # dimensionless QLIKE improvement rather than a trading return.
        return (best_benchmark - model) * 10_000.0
    return outcome.metric("future_variance_uplift").value * 10_000.0


def _study_point(
    feature: ProspectiveFeature,
    *,
    net_effect: float,
    cost_model_version: str,
    volatility_cutoffs: Mapping[tuple[str, str, int], tuple[float, float]],
    treated: bool,
) -> StudyPoint:
    direction = (
        feature.expected_direction
        if feature.target is TargetMetric.FORWARD_RETURN
        else 0
    )
    return StudyPoint(
        point_id=feature.observation_id,
        scenario_id=feature.observation_id if treated else None,
        instrument_id=feature.ticker,
        occurred_at=feature.observed_at,
        trading_day=feature.trading_day,
        session_bucket=_session_bucket(feature),
        volatility_bucket=_bucket(
            _volatility_proxy(feature),
            volatility_cutoffs.get(_stratum_key(feature)),
        ),
        liquidity_bucket=(
            f"direction_{direction:+d}_horizon_{feature.horizon_seconds}"
        ),
        features_observed_at=feature.feature_max_observed_at,
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net_effect,
        cost_model_version=cost_model_version,
    )


def _volatility_cutoffs(
    hypothesis: ProspectiveHypothesis,
    validation: Sequence[tuple[ProspectiveFeature, ProspectiveOutcome]],
) -> Mapping[tuple[str, str, int], tuple[float, float]]:
    if hypothesis in {
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
        ProspectiveHypothesis.HAR_VOLATILITY_V2,
    }:
        return {}
    values: defaultdict[tuple[str, str, int], list[float]] = defaultdict(list)
    for feature, _ in validation:
        values[_stratum_key(feature)].append(_volatility_proxy(feature))
    return {
        key: (_quantile(group, 1.0 / 3.0), _quantile(group, 2.0 / 3.0))
        for key, group in sorted(values.items())
    }


def _volatility_cutoffs_from_rows(
    hypothesis: ProspectiveHypothesis,
    rows: Callable[[], Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]]],
    *,
    split: ChronologicalSplit,
) -> Mapping[tuple[str, str, int], tuple[float, float]]:
    """Derive the same validation strata without materialising report rows."""

    if hypothesis in {
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
        ProspectiveHypothesis.HAR_VOLATILITY_V2,
    }:
        return {}
    values: defaultdict[tuple[str, str, int], list[float]] = defaultdict(list)
    for feature, _ in rows():
        if (
            feature.hypothesis is hypothesis
            and split.partition_for(feature.trading_day) is DatasetPartition.VALIDATION
        ):
            values[_stratum_key(feature)].append(_volatility_proxy(feature))
    return {
        key: (_quantile(group, 1.0 / 3.0), _quantile(group, 2.0 / 3.0))
        for key, group in sorted(values.items())
    }


def _volatility_proxy(feature: ProspectiveFeature) -> float:
    if feature.hypothesis in {
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
    }:
        return feature.value("baseline_future_variance")
    return 0.0


def _stratum_key(feature: ProspectiveFeature) -> tuple[str, str, int]:
    return (feature.ticker, _session_bucket(feature), feature.horizon_seconds)


def _session_bucket(feature: ProspectiveFeature) -> str:
    local = feature.observed_at.astimezone(
        ZoneInfo(MOEX_EQUITY_PHASE_SCHEDULE_V1.timezone_name)
    )
    phase = MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(feature.observed_at).value
    return f"{phase}:{local.hour:02d}:{(local.minute // 15) * 15:02d}"


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires observations")
    ordered = tuple(sorted(values))
    return ordered[int((len(ordered) - 1) * probability)]


def _bucket(value: float, cutoffs: tuple[float, float] | None) -> str:
    if cutoffs is None:
        return "pre_holdout_distribution_unavailable"
    low, high = cutoffs
    if value <= low:
        return "low"
    if value <= high:
        return "medium"
    return "high"
