"""In-memory adapter for prospective live-shadow ports and tests."""

from __future__ import annotations

from tinvest_signal_engine.domain.prospective_live_shadow import (
    ProspectiveLiveObservation,
    ProspectiveLiveOutcome,
    deterministic_live_outcome_id,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
)


class InMemoryProspectiveLiveShadowStore:
    """Idempotent reference adapter; durable storage requires a root-owned schema."""

    def __init__(self) -> None:
        self._observations: dict[str, ProspectiveLiveObservation] = {}
        self._outcomes: dict[str, ProspectiveLiveOutcome] = {}

    def existing_observation_ids(
        self, observation_ids: tuple[str, ...]
    ) -> frozenset[str]:
        return frozenset(
            observation_id
            for observation_id in observation_ids
            if observation_id in self._observations
        )

    def persist_observation(
        self, observation: ProspectiveLiveObservation
    ) -> PersistenceDisposition:
        existing = self._observations.get(observation.observation_id)
        if existing is not None:
            if existing.payload_fingerprint != observation.payload_fingerprint:
                raise ProspectiveEvidenceConflict(
                    "prospective live observation identity conflict"
                )
            return PersistenceDisposition.REPLAYED
        self._observations[observation.observation_id] = observation
        return PersistenceDisposition.INSERTED

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveLiveObservation, ...]:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        pending = (
            item
            for item in self._observations.values()
            if deterministic_live_outcome_id(
                observation_id=item.observation_id,
                outcome_policy_version=outcome_policy_version,
            )
            not in self._outcomes
        )
        return tuple(
            sorted(pending, key=lambda item: (item.target_at, item.observation_id))
        )[:limit]

    def persist_outcome(
        self, outcome: ProspectiveLiveOutcome
    ) -> PersistenceDisposition:
        existing = self._outcomes.get(outcome.outcome_id)
        if existing is not None:
            if existing.payload_fingerprint != outcome.payload_fingerprint:
                raise ProspectiveEvidenceConflict(
                    "prospective live outcome identity conflict"
                )
            return PersistenceDisposition.REPLAYED
        self._outcomes[outcome.outcome_id] = outcome
        return PersistenceDisposition.INSERTED

    def observations(self) -> tuple[ProspectiveLiveObservation, ...]:
        return tuple(
            sorted(
                self._observations.values(),
                key=lambda item: (
                    item.feature.observed_at,
                    item.feature.hypothesis.value,
                    item.feature.horizon_seconds,
                    item.observation_id,
                ),
            )
        )

    def outcomes(
        self, *, outcome_policy_version: str
    ) -> tuple[ProspectiveLiveOutcome, ...]:
        return tuple(
            sorted(
                (
                    item
                    for item in self._outcomes.values()
                    if item.outcome_policy_version == outcome_policy_version
                ),
                key=lambda item: (item.outcome.target_at, item.outcome_id),
            )
        )
