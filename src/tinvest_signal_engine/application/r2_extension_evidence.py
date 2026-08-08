"""Independent matched-control evidence for the causal H10/H11 R2 replay."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import timedelta

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildChronologicalSplit,
    BuildMatchedControls,
    EvidenceDiagnosticsInput,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    R2ExtensionReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    DatasetPartition,
    EvidenceBundle,
    EvidenceReasonCount,
    StudyPoint,
)
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2Feature,
    R2Outcome,
)

R2_EVIDENCE_POLICY = EvidenceGatePolicy(
    minimum_trading_days=20,
    minimum_eligible_events=200,
    controls_per_event=5,
    false_discovery_rate=0.05,
    required_positive_stability_blocks=3,
    maximum_instrument_share=0.40,
    minimum_common_support_coverage=0.10,
)


@dataclass(frozen=True, slots=True)
class R2EvidenceCoverage:
    hypothesis_id: str
    primary_horizon_seconds: int
    holdout_observations: int
    available_holdout_observations: int
    triggered_events: int
    matched_events: int
    unmatched_events: int
    control_candidates: int


@dataclass(frozen=True, slots=True)
class R2EvidenceAssessment:
    bundles: tuple[EvidenceBundle, ...]
    coverage: tuple[R2EvidenceCoverage, ...]

    def for_hypothesis(self, hypothesis: R2ExtensionHypothesis) -> EvidenceBundle:
        return next(
            item for item in self.bundles if item.hypothesis_id == hypothesis.value
        )

    def coverage_for(self, hypothesis: R2ExtensionHypothesis) -> R2EvidenceCoverage:
        return next(
            item for item in self.coverage if item.hypothesis_id == hypothesis.value
        )


class AssessR2ExtensionEvidence:
    """Assess the sealed primary horizon on a chronological 20% holdout."""

    def __init__(
        self,
        policy: EvidenceGatePolicy = R2_EVIDENCE_POLICY,
    ) -> None:
        self.policy = policy
        self._controls = BuildMatchedControls(
            controls_per_event=policy.controls_per_event,
            scenario_exclusion_window=timedelta(minutes=5),
        )
        self._portfolio = AssessEvidencePortfolio(policy)

    def execute(
        self,
        report: R2ExtensionReport,
        requested_hypotheses: Sequence[R2ExtensionHypothesis],
        *,
        cost_model_version: str,
    ) -> R2EvidenceAssessment:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        if not selected:
            raise ValueError("at least one R2 hypothesis is required")
        if set(selected) != {R2ExtensionHypothesis.OPENING_GAP_REVERSION}:
            raise ValueError("independent R2 evidence is currently implemented for H10")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        trading_days = tuple(sorted({item.trading_day for item in report.features}))
        split = BuildChronologicalSplit().execute(trading_days)
        prepared = tuple(
            self._request(
                report,
                hypothesis,
                split=split,
                cost_model_version=cost_model_version,
            )
            for hypothesis in selected
        )
        return R2EvidenceAssessment(
            bundles=self._portfolio.execute(tuple(item[0] for item in prepared)),
            coverage=tuple(item[1] for item in prepared),
        )

    def _request(
        self,
        report: R2ExtensionReport,
        hypothesis: R2ExtensionHypothesis,
        *,
        split,
        cost_model_version: str,
    ) -> tuple[EvidenceRequest, R2EvidenceCoverage]:
        primary_horizon = min(
            feature.horizon_seconds
            for feature in report.features
            if feature.hypothesis is hypothesis
        )
        pairs = tuple(
            (feature, outcome)
            for feature, outcome in zip(report.features, report.outcomes, strict=True)
            if feature.hypothesis is hypothesis
            and feature.horizon_seconds == primary_horizon
        )
        validation = tuple(
            pair
            for pair in pairs
            if split.partition_for(pair[0].trading_day) is DatasetPartition.VALIDATION
            and pair[1].available
        )
        holdout = tuple(
            pair
            for pair in pairs
            if split.partition_for(pair[0].trading_day) is DatasetPartition.HOLDOUT
        )
        cutoffs = _volatility_cutoffs(validation)
        round_trip_cost_bps = _round_trip_cost(pairs)
        events: list[StudyPoint] = []
        candidates: list[StudyPoint] = []
        reasons: Counter[str] = Counter()
        available = 0
        for feature, outcome in holdout:
            if not outcome.available or outcome.forward_return_bps is None:
                reasons[outcome.reason.value] += 1
                continue
            available += 1
            if feature.decision is R2Decision.ABSTAIN:
                reasons[feature.reason.value] += 1
                continue
            direction = _counterfactual_direction(feature)
            point = _study_point(
                feature,
                net_effect_bps=(
                    direction * outcome.forward_return_bps - round_trip_cost_bps
                ),
                direction=direction,
                cost_model_version=cost_model_version,
                cutoffs=cutoffs.get(feature.ticker),
                treated=feature.decision is R2Decision.MATCHED,
            )
            if feature.decision is R2Decision.MATCHED:
                events.append(point)
            else:
                candidates.append(point)
                reasons[feature.reason.value] += 1
        candidates_with_exclusions = tuple(
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
        matched = self._controls.execute(events, candidates_with_exclusions)
        reuse = matched.reuse_statistics
        coverage = R2EvidenceCoverage(
            hypothesis_id=hypothesis.value,
            primary_horizon_seconds=primary_horizon,
            holdout_observations=len(holdout),
            available_holdout_observations=available,
            triggered_events=len(events),
            matched_events=len(matched.groups),
            unmatched_events=len(matched.unmatched_event_ids),
            control_candidates=len(candidates_with_exclusions),
        )
        return (
            EvidenceRequest(
                hypothesis_id=hypothesis.value,
                hypothesis_version=hypothesis.version,
                dataset_fingerprint=report.dataset_fingerprint,
                groups=matched.groups,
                expected_eligible_events=len(events),
                unmatched_event_ids=matched.unmatched_event_ids,
                total_available_observations=available or None,
                diagnostics_input=EvidenceDiagnosticsInput(
                    total_observation_count=len(holdout),
                    available_observation_count=available,
                    eligible_event_count=len(events),
                    reasons_histogram=tuple(
                        EvidenceReasonCount(reason_code=reason, count=count)
                        for reason, count in sorted(reasons.items())
                        if count > 0
                    ),
                ),
                control_reuse_statistics=reuse,
                control_selection_policy_version=matched.selection_policy_version,
                maximum_control_reuse=matched.maximum_control_reuse,
                minimum_independent_control_clusters=1,
            ),
            coverage,
        )


def _counterfactual_direction(feature: R2Feature) -> int:
    if feature.hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION:
        return -1
    residual = feature.value("residual_return_5m_bps")
    return -1 if residual > 0.0 else 1


def _round_trip_cost(
    pairs: Sequence[tuple[R2Feature, R2Outcome]],
) -> float:
    for feature, outcome in pairs:
        if (
            feature.decision is R2Decision.MATCHED
            and outcome.forward_return_bps is not None
            and outcome.cost_adjusted_signed_return_bps is not None
        ):
            return (
                feature.expected_direction * outcome.forward_return_bps
                - outcome.cost_adjusted_signed_return_bps
            )
    return 10.0


def _volatility_cutoffs(
    pairs: Sequence[tuple[R2Feature, R2Outcome]],
) -> dict[str, tuple[float, float]]:
    values: defaultdict[str, list[float]] = defaultdict(list)
    for feature, _ in pairs:
        values[feature.ticker].append(_volatility_proxy(feature))
    return {
        ticker: (_quantile(rows, 1 / 3), _quantile(rows, 2 / 3))
        for ticker, rows in values.items()
        if rows
    }


def _study_point(
    feature: R2Feature,
    *,
    net_effect_bps: float,
    direction: int,
    cost_model_version: str,
    cutoffs: tuple[float, float] | None,
    treated: bool,
) -> StudyPoint:
    return StudyPoint(
        point_id=feature.observation_id,
        scenario_id=feature.observation_id if treated else None,
        instrument_id=feature.ticker,
        occurred_at=feature.available_at,
        trading_day=feature.trading_day,
        session_bucket=f"{feature.hypothesis.value}:{feature.horizon_seconds}",
        volatility_bucket=_bucket(_volatility_proxy(feature), cutoffs),
        liquidity_bucket=f"same_instrument_direction_{direction:+d}",
        features_observed_at=feature.available_at,
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net_effect_bps,
        cost_model_version=cost_model_version,
    )


def _volatility_proxy(feature: R2Feature) -> float:
    if feature.hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION:
        return abs(feature.value("market_gap_z"))
    return abs(feature.value("market_return_5m_bps"))


def _bucket(value: float, cutoffs: tuple[float, float] | None) -> str:
    if cutoffs is None:
        return "neutral"
    return "low" if value < cutoffs[0] else "medium" if value < cutoffs[1] else "high"


def _quantile(values: Sequence[float], probability: float) -> float:
    ordered = tuple(sorted(values))
    return ordered[int((len(ordered) - 1) * probability)]
