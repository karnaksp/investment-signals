"""Record daily H10 evaluations and mature outcomes in the shadow contour."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
)
from tinvest_signal_engine.domain.r2_live_shadow import R2LiveShadowInput


class R2LiveShadowSource(Protocol):
    def load(self, *, as_of: datetime) -> tuple[R2LiveShadowInput, ...]: ...


class R2LiveShadowStore(Protocol):
    def persist_observation(
        self,
        item: R2LiveShadowInput,
        *,
        recorded_at: datetime,
    ) -> PersistenceDisposition: ...

    def persist_outcome(
        self,
        item: R2LiveShadowInput,
        *,
        evaluated_at: datetime,
    ) -> PersistenceDisposition: ...


@dataclass(frozen=True, slots=True)
class R2LiveShadowPassResult:
    observations_stored: int
    observations_replayed: int
    outcomes_stored: int
    outcomes_replayed: int


class ProcessR2OpeningGapLiveShadow:
    def __init__(
        self,
        *,
        source: R2LiveShadowSource,
        store: R2LiveShadowStore,
    ) -> None:
        self._source = source
        self._store = store

    def run_once(self, *, now: datetime) -> R2LiveShadowPassResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("R2 live-shadow now must be timezone-aware")
        stored_observations = replayed_observations = 0
        stored_outcomes = replayed_outcomes = 0
        for item in self._source.load(as_of=now):
            disposition = self._store.persist_observation(item, recorded_at=now)
            if disposition is PersistenceDisposition.INSERTED:
                stored_observations += 1
            else:
                replayed_observations += 1
            if item.outcome.target_at > now:
                continue
            disposition = self._store.persist_outcome(item, evaluated_at=now)
            if disposition is PersistenceDisposition.INSERTED:
                stored_outcomes += 1
            else:
                replayed_outcomes += 1
        return R2LiveShadowPassResult(
            observations_stored=stored_observations,
            observations_replayed=replayed_observations,
            outcomes_stored=stored_outcomes,
            outcomes_replayed=replayed_outcomes,
        )
