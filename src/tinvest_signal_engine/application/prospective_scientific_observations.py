"""Prospective scientific observation and mature-outcome use cases."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveObservationProvenance,
    ProspectiveOutcomeEvidence,
    ProspectiveScientificObservation,
    ProspectiveScientificOutcome,
    build_prospective_observation,
    deterministic_prospective_outcome_id,
    prospective_outcome_payload_fingerprint,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    CausalFeatureVector,
    ScientificCandlePolicy,
    ScientificTarget,
    directional_outcome,
    variance_outcome,
)


class ProspectiveScientificStore(Protocol):
    def persist_observation(
        self, observation: ProspectiveScientificObservation
    ) -> PersistenceDisposition: ...

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveScientificObservation, ...]: ...

    def persist_outcome(
        self, outcome: ProspectiveScientificOutcome
    ) -> PersistenceDisposition: ...


class ProspectiveOutcomeEvidenceSource(Protocol):
    def load(
        self, observation: ProspectiveScientificObservation
    ) -> ProspectiveOutcomeEvidence: ...


class RecordProspectiveScientificObservation:
    """Seal and persist every feature evaluation, not only triggered events."""

    def __init__(self, store: ProspectiveScientificStore) -> None:
        self._store = store

    def execute(
        self,
        *,
        instrument_id: str,
        feature: CausalFeatureVector,
        policy_version: str,
        formula_version: str,
        provenance: ProspectiveObservationProvenance,
        recorded_at: datetime,
    ) -> tuple[ProspectiveScientificObservation, PersistenceDisposition]:
        observation = build_prospective_observation(
            instrument_id=instrument_id,
            feature=feature,
            policy_version=policy_version,
            formula_version=formula_version,
            provenance=provenance,
            recorded_at=recorded_at,
        )
        return observation, self._store.persist_observation(observation)


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeBatchResult:
    scanned: int
    stored: int
    replayed: int
    pending: int
    unavailable: int
    outcome_ids: tuple[str, ...]


class ProcessMatureProspectiveScientificOutcomes:
    """Seal outcomes only after horizon plus a bounded data-arrival grace."""

    def __init__(
        self,
        *,
        store: ProspectiveScientificStore,
        evidence: ProspectiveOutcomeEvidenceSource,
        policy: ScientificCandlePolicy,
        outcome_policy_version: str,
        grace_seconds: int = 30,
    ) -> None:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if grace_seconds < 0:
            raise ValueError("grace_seconds must not be negative")
        self._store = store
        self._evidence = evidence
        self._policy = policy
        self._outcome_policy_version = outcome_policy_version
        self._grace_seconds = grace_seconds

    def run_once(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> ProspectiveOutcomeBatchResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if limit <= 0:
            raise ValueError("limit must be positive")
        candidates = self._store.pending_observations(
            outcome_policy_version=self._outcome_policy_version,
            limit=limit,
        )
        stored = replayed = pending = unavailable = 0
        outcome_ids: list[str] = []
        for observation in candidates:
            if not observation.is_mature(now, grace_seconds=self._grace_seconds):
                pending += 1
                continue
            evidence = self._evidence.load(observation)
            _validate_evidence(observation, evidence)
            result = _evaluate(
                observation=observation,
                evidence=evidence,
                policy=self._policy,
            )
            outcome = _seal_outcome(
                observation=observation,
                evidence=evidence,
                result=result,
                outcome_policy_version=self._outcome_policy_version,
                evaluated_at=now,
            )
            disposition = self._store.persist_outcome(outcome)
            if disposition is PersistenceDisposition.INSERTED:
                stored += 1
            else:
                replayed += 1
            if not outcome.result.available:
                unavailable += 1
            outcome_ids.append(outcome.outcome_id)
        return ProspectiveOutcomeBatchResult(
            scanned=len(candidates),
            stored=stored,
            replayed=replayed,
            pending=pending,
            unavailable=unavailable,
            outcome_ids=tuple(outcome_ids),
        )


def _validate_evidence(
    observation: ProspectiveScientificObservation,
    evidence: ProspectiveOutcomeEvidence,
) -> None:
    if evidence.observation_id != observation.observation_id:
        raise ValueError("outcome evidence belongs to a different observation")
    if evidence.target_at != observation.target_at:
        raise ValueError("outcome evidence target differs from observation maturity")
    if evidence.source_window_start < observation.feature.observed_at:
        raise ValueError("outcome evidence begins before the observation cutoff")


def _evaluate(
    *,
    observation: ProspectiveScientificObservation,
    evidence: ProspectiveOutcomeEvidence,
    policy: ScientificCandlePolicy,
):
    feature = observation.feature
    actual = evidence.actual_value if evidence.available else None
    if feature.target is ScientificTarget.DIRECTIONAL_RETURN_BPS:
        result = directional_outcome(
            feature,
            target_at=evidence.target_at,
            forward_return_bps=actual,
            policy=policy,
        )
    elif feature.target in {
        ScientificTarget.FUTURE_REALIZED_VARIANCE,
        ScientificTarget.FUTURE_ACTIVITY_UPLIFT,
    }:
        result = variance_outcome(
            feature,
            target_at=evidence.target_at,
            actual_future_variance=actual,
            policy=policy,
        )
    else:
        raise ValueError(f"unsupported scientific target: {feature.target.value}")
    # Existing formula DTOs use their local feature identity.  The prospective
    # envelope has a stronger identity that also seals instrument, policy and
    # formula versions, so the persisted outcome joins to that envelope.
    return replace(result, observation_id=observation.observation_id)


def _seal_outcome(
    *,
    observation: ProspectiveScientificObservation,
    evidence: ProspectiveOutcomeEvidence,
    result,
    outcome_policy_version: str,
    evaluated_at: datetime,
) -> ProspectiveScientificOutcome:
    outcome_id = deterministic_prospective_outcome_id(
        observation_id=observation.observation_id,
        outcome_policy_version=outcome_policy_version,
    )
    fingerprint = prospective_outcome_payload_fingerprint(
        observation_id=observation.observation_id,
        hypothesis=observation.feature.hypothesis,
        hypothesis_version=observation.feature.hypothesis_version,
        instrument_id=observation.instrument_id,
        trading_day=observation.feature.trading_day,
        target=observation.feature.target,
        result=result,
        outcome_policy_version=outcome_policy_version,
        source_event_ids=evidence.source_event_ids,
        source_window_start=evidence.source_window_start,
        source_window_end=evidence.source_window_end,
        source_max_observed_at=evidence.source_max_observed_at,
        input_fingerprint=evidence.input_fingerprint,
    )
    return ProspectiveScientificOutcome(
        outcome_id=outcome_id,
        observation_id=observation.observation_id,
        hypothesis=observation.feature.hypothesis,
        hypothesis_version=observation.feature.hypothesis_version,
        instrument_id=observation.instrument_id,
        trading_day=observation.feature.trading_day,
        target=observation.feature.target,
        target_at=evidence.target_at,
        result=result,
        outcome_policy_version=outcome_policy_version,
        source_event_ids=evidence.source_event_ids,
        source_window_start=evidence.source_window_start,
        source_window_end=evidence.source_window_end,
        source_max_observed_at=evidence.source_max_observed_at,
        input_fingerprint=evidence.input_fingerprint,
        evaluated_at=evaluated_at,
        payload_fingerprint=fingerprint,
    )
