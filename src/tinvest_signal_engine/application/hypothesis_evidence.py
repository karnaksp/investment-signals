"""Use cases that build controls and assess scientific hypothesis evidence."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import date, timedelta
from hashlib import sha256
import json
from typing import Sequence

from tinvest_signal_engine.domain.hypothesis_evidence import (
    ConfidenceInterval,
    ChronologicalSplit,
    DatasetPartition,
    EvidenceBundle,
    EvidenceDecision,
    InstrumentConcentration,
    MatchedControlGroup,
    MatchedControlsResult,
    StabilityAssessment,
    StudyPoint,
    benjamini_hochberg,
    chronological_split_60_20_20,
    day_block_bootstrap_interval,
    five_block_stability,
    one_sided_sign_test_p_value,
    wilson_interval,
)


class BuildChronologicalSplit:
    def execute(self, trading_days: Sequence[date]) -> ChronologicalSplit:
        return chronological_split_60_20_20(trading_days)


class BuildMatchedControls:
    """Select deterministic, non-reused matched controls without leakage."""

    def __init__(
        self,
        *,
        controls_per_event: int = 5,
        scenario_exclusion_window: timedelta = timedelta(minutes=5),
    ) -> None:
        if controls_per_event <= 0:
            raise ValueError("controls_per_event must be positive")
        if scenario_exclusion_window < timedelta(0):
            raise ValueError("scenario_exclusion_window must not be negative")
        self._controls_per_event = controls_per_event
        self._scenario_exclusion_window = scenario_exclusion_window

    def execute(
        self,
        events: Sequence[StudyPoint],
        candidates: Sequence[StudyPoint],
    ) -> MatchedControlsResult:
        event_ids = {event.point_id for event in events}
        if len(event_ids) != len(events):
            raise ValueError("event point_ids must be unique")
        candidate_ids = {candidate.point_id for candidate in candidates}
        if len(candidate_ids) != len(candidates):
            raise ValueError("candidate point_ids must be unique")

        used_controls: set[str] = set()
        groups: list[MatchedControlGroup] = []
        unmatched: list[str] = []
        for event in sorted(events, key=lambda item: (item.occurred_at, item.point_id)):
            eligible = [
                candidate
                for candidate in candidates
                if candidate.point_id not in event_ids
                and candidate.point_id not in used_controls
                and candidate.matching_key == event.matching_key
                and candidate.cost_model_version == event.cost_model_version
                and not self._scenario_overlap(event, candidate)
            ]
            eligible.sort(
                key=lambda candidate: (
                    abs((candidate.occurred_at - event.occurred_at).total_seconds()),
                    _stable_tie_break(event.point_id, candidate.point_id),
                    candidate.point_id,
                )
            )
            selected = tuple(eligible[: self._controls_per_event])
            if len(selected) != self._controls_per_event:
                unmatched.append(event.point_id)
                continue
            used_controls.update(control.point_id for control in selected)
            groups.append(MatchedControlGroup(event=event, controls=selected))
        return MatchedControlsResult(
            groups=tuple(groups),
            unmatched_event_ids=tuple(unmatched),
            controls_per_event=self._controls_per_event,
        )

    def _scenario_overlap(self, event: StudyPoint, candidate: StudyPoint) -> bool:
        if event.scenario_id is None:
            return False
        if event.scenario_id in candidate.nearby_scenario_ids:
            return True
        return (
            candidate.scenario_id == event.scenario_id
            and abs(candidate.occurred_at - event.occurred_at)
            <= self._scenario_exclusion_window
        )


@dataclass(frozen=True)
class EvidenceRequest:
    hypothesis_id: str
    hypothesis_version: str
    dataset_fingerprint: str
    groups: tuple[MatchedControlGroup, ...]
    expected_eligible_events: int
    unmatched_event_ids: tuple[str, ...] = ()
    total_available_observations: int | None = None

    def __post_init__(self) -> None:
        if not self.hypothesis_id.strip() or not self.hypothesis_version.strip():
            raise ValueError("hypothesis identity must not be empty")
        if not self.dataset_fingerprint.strip():
            raise ValueError("dataset_fingerprint must not be empty")
        if self.expected_eligible_events < 0:
            raise ValueError("expected_eligible_events must not be negative")
        if (
            self.total_available_observations is not None
            and self.total_available_observations <= 0
        ):
            raise ValueError("total_available_observations must be positive")
        if (
            self.total_available_observations is not None
            and self.expected_eligible_events > self.total_available_observations
        ):
            raise ValueError("eligible events cannot exceed available observations")
        event_ids = tuple(group.event.point_id for group in self.groups)
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("matched event ids must be unique")
        if set(event_ids) & set(self.unmatched_event_ids):
            raise ValueError("an event cannot be both matched and unmatched")

    @property
    def test_id(self) -> str:
        return f"{self.hypothesis_id}@{self.hypothesis_version}"


@dataclass(frozen=True)
class EvidenceGatePolicy:
    minimum_trading_days: int = 30
    minimum_eligible_events: int = 300
    controls_per_event: int = 5
    bootstrap_samples: int = 2_000
    bootstrap_seed: int = 17
    false_discovery_rate: float = 0.05
    required_positive_stability_blocks: int = 4
    maximum_instrument_share: float = 0.50
    minimum_coverage: float = 0.10

    def __post_init__(self) -> None:
        if self.minimum_trading_days <= 0 or self.minimum_eligible_events <= 0:
            raise ValueError("minimum sample gates must be positive")
        if self.controls_per_event <= 0 or self.bootstrap_samples <= 0:
            raise ValueError("control and bootstrap counts must be positive")
        if not 0.0 < self.false_discovery_rate < 1.0:
            raise ValueError("false_discovery_rate must be between zero and one")
        if not 0.0 < self.maximum_instrument_share < 1.0:
            raise ValueError("maximum_instrument_share must be between zero and one")
        if not 0.0 < self.minimum_coverage <= 1.0:
            raise ValueError("minimum_coverage must be in (0, 1]")


class AssessEvidencePortfolio:
    """Assess a multiple-testing family and retain every result, including failures."""

    def __init__(self, policy: EvidenceGatePolicy = EvidenceGatePolicy()) -> None:
        self._policy = policy

    def execute(self, requests: Sequence[EvidenceRequest]) -> tuple[EvidenceBundle, ...]:
        if len({request.test_id for request in requests}) != len(requests):
            raise ValueError("hypothesis id/version pairs must be unique in a portfolio")
        preliminary = [self._calculate(request) for request in requests]
        p_values = {
            request.test_id: bundle.raw_p_value
            for request, bundle in zip(requests, preliminary, strict=True)
            if bundle.raw_p_value is not None
        }
        adjusted = {
            item.test_id: item
            for item in benjamini_hochberg(
                p_values,
                false_discovery_rate=self._policy.false_discovery_rate,
            )
        }
        completed: list[EvidenceBundle] = []
        for request, bundle in zip(requests, preliminary, strict=True):
            test = adjusted.get(request.test_id)
            if test is None:
                completed.append(bundle)
                continue
            with_fdr = replace(
                bundle,
                adjusted_q_value=test.q_value,
                fdr_significant=test.significant,
            )
            completed.append(self._decide(with_fdr))
        return tuple(completed)

    def _calculate(self, request: EvidenceRequest) -> EvidenceBundle:
        groups = request.groups
        trading_days = {group.event.trading_day for group in groups}
        cost_versions = {group.event.cost_model_version for group in groups}
        bad_control_count = any(
            len(group.controls) != self._policy.controls_per_event for group in groups
        )
        reasons: list[str] = []
        if request.expected_eligible_events < self._policy.minimum_eligible_events:
            reasons.append("minimum_eligible_events_not_met")
        if (
            request.total_available_observations is not None
            and request.expected_eligible_events / request.total_available_observations
            < self._policy.minimum_coverage
        ):
            reasons.append("minimum_coverage_not_met")
        if len(trading_days) < self._policy.minimum_trading_days:
            reasons.append("minimum_trading_days_not_met")
        if request.unmatched_event_ids or len(groups) != request.expected_eligible_events:
            reasons.append("matched_controls_incomplete")
        if bad_control_count:
            reasons.append("controls_per_event_not_met")
        if len(cost_versions) != 1 or not all(cost_versions):
            reasons.append("versioned_cost_model_required")
        if any(
            point.partition is not DatasetPartition.HOLDOUT
            for group in groups
            for point in (group.event, *group.controls)
        ):
            reasons.append("independent_holdout_required")

        empty_stability = StabilityAssessment(
            blocks=(),
            required_positive_blocks=self._policy.required_positive_stability_blocks,
            positive_blocks=0,
            assessed=False,
            stable=False,
        )
        concentrations = _instrument_concentrations(groups)
        maximum_share = max((item.share for item in concentrations), default=None)
        if maximum_share is not None and maximum_share > self._policy.maximum_instrument_share:
            reasons.append("single_instrument_concentration_exceeded")

        if reasons or not groups:
            return EvidenceBundle(
                evidence_id=_evidence_id(request),
                hypothesis_id=request.hypothesis_id,
                hypothesis_version=request.hypothesis_version,
                dataset_fingerprint=request.dataset_fingerprint,
                decision=EvidenceDecision.BLOCKED_BY_DATA,
                reason_codes=tuple(dict.fromkeys(reasons or ["no_matched_events"])),
                trading_days=len(trading_days),
                eligible_events=request.expected_eligible_events,
                matched_events=len(groups),
                matched_controls=sum(len(group.controls) for group in groups),
                cost_model_version=next(iter(cost_versions), None),
                event_mean_net_bps=None,
                control_mean_net_bps=None,
                mean_lift_bps=None,
                lift_interval=None,
                positive_rate_interval=None,
                raw_p_value=None,
                adjusted_q_value=None,
                fdr_significant=False,
                stability=empty_stability,
                instrument_concentration=concentrations,
                maximum_instrument_share=maximum_share,
            )

        lifts_by_day: dict[date, list[float]] = defaultdict(list)
        for group in groups:
            lifts_by_day[group.event.trading_day].append(group.lift_bps)
        lifts = [group.lift_bps for group in groups]
        positives = sum(value > 0.0 for value in lifts)
        seed = self._policy.bootstrap_seed + int(
            sha256(request.test_id.encode("utf-8")).hexdigest()[:8], 16
        )
        lift_interval = day_block_bootstrap_interval(
            lifts_by_day,
            samples=self._policy.bootstrap_samples,
            seed=seed,
        )
        stability = five_block_stability(
            lifts_by_day,
            required_positive_blocks=self._policy.required_positive_stability_blocks,
        )
        return EvidenceBundle(
            evidence_id=_evidence_id(request),
            hypothesis_id=request.hypothesis_id,
            hypothesis_version=request.hypothesis_version,
            dataset_fingerprint=request.dataset_fingerprint,
            decision=EvidenceDecision.INCONCLUSIVE,
            reason_codes=(),
            trading_days=len(trading_days),
            eligible_events=request.expected_eligible_events,
            matched_events=len(groups),
            matched_controls=sum(len(group.controls) for group in groups),
            cost_model_version=next(iter(cost_versions)),
            event_mean_net_bps=sum(group.event.net_effect_bps for group in groups)
            / len(groups),
            control_mean_net_bps=sum(group.control_mean_bps for group in groups)
            / len(groups),
            mean_lift_bps=sum(lifts) / len(lifts),
            lift_interval=lift_interval,
            positive_rate_interval=wilson_interval(positives, len(lifts)),
            raw_p_value=one_sided_sign_test_p_value(positives, len(lifts)),
            adjusted_q_value=None,
            fdr_significant=False,
            stability=stability,
            instrument_concentration=concentrations,
            maximum_instrument_share=maximum_share,
        )

    @staticmethod
    def _decide(bundle: EvidenceBundle) -> EvidenceBundle:
        interval = bundle.lift_interval
        if interval is None:
            return bundle
        if interval.upper <= 0.0 or (
            bundle.stability.assessed and bundle.stability.positive_blocks <= 1
        ):
            return replace(
                bundle,
                decision=EvidenceDecision.REJECTED,
                reason_codes=("effect_rejected_on_holdout",),
            )
        reasons: list[str] = []
        if interval.lower <= 0.0:
            reasons.append("positive_lower_confidence_bound_not_met")
        if not bundle.fdr_significant:
            reasons.append("multiple_testing_gate_not_met")
        if not bundle.stability.assessed or not bundle.stability.stable:
            reasons.append("five_block_stability_not_met")
        if reasons:
            return replace(
                bundle,
                decision=EvidenceDecision.INCONCLUSIVE,
                reason_codes=tuple(reasons),
            )
        return replace(
            bundle,
            decision=EvidenceDecision.PASSED,
            reason_codes=(),
        )


def _stable_tie_break(event_id: str, candidate_id: str) -> str:
    return sha256(f"{event_id}\0{candidate_id}".encode("utf-8")).hexdigest()


def _instrument_concentrations(
    groups: Sequence[MatchedControlGroup],
) -> tuple[InstrumentConcentration, ...]:
    counts = Counter(group.event.instrument_id for group in groups)
    total = sum(counts.values())
    if total == 0:
        return ()
    return tuple(
        InstrumentConcentration(
            instrument_id=instrument_id,
            event_count=count,
            share=count / total,
        )
        for instrument_id, count in sorted(counts.items())
    )


def _evidence_id(request: EvidenceRequest) -> str:
    payload = {
        "hypothesis_id": request.hypothesis_id,
        "hypothesis_version": request.hypothesis_version,
        "dataset_fingerprint": request.dataset_fingerprint,
        "event_ids": tuple(group.event.point_id for group in request.groups),
        "control_ids": tuple(
            tuple(control.point_id for control in group.controls)
            for group in request.groups
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{sha256(encoded).hexdigest()}"
