"""Use cases for deterministic C1-C4 scientific observation composition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveFeature,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
    ScientificCombinationObservation,
    compose_preregistered_combination,
)


@dataclass(frozen=True, slots=True)
class ComposeScientificCombinationRequest:
    combination_id: ScientificCombinationId
    primary_scope: str
    trading_day: date
    observed_at: datetime
    horizon_seconds: int
    components: tuple[ProspectiveFeature, ...]
    market_context_scope: str | None = None

    def __post_init__(self) -> None:
        if not self.primary_scope.strip():
            raise ValueError("primary_scope must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")


class ComposeScientificCombination:
    """Map an already sealed base-observation snapshot to one C1-C4 decision."""

    def execute(
        self,
        request: ComposeScientificCombinationRequest,
    ) -> ScientificCombinationObservation:
        return compose_preregistered_combination(
            combination_id=request.combination_id,
            primary_scope=request.primary_scope,
            market_context_scope=request.market_context_scope,
            trading_day=request.trading_day,
            observed_at=request.observed_at,
            horizon_seconds=request.horizon_seconds,
            components=request.components,
        )


@dataclass(frozen=True, slots=True)
class ComposeScientificCombinationBatchRequest:
    requests: tuple[ComposeScientificCombinationRequest, ...]

    def __post_init__(self) -> None:
        keys = tuple(
            (
                request.combination_id.value,
                request.primary_scope,
                request.market_context_scope or "",
                request.trading_day.isoformat(),
                request.observed_at.isoformat(),
                request.horizon_seconds,
            )
            for request in self.requests
        )
        if len(keys) != len(set(keys)):
            raise ValueError("combination batch requests must be unique")


class ComposeScientificCombinationBatch:
    """Compose and canonically order an adapter-owned snapshot batch."""

    def __init__(self) -> None:
        self._single = ComposeScientificCombination()

    def execute(
        self,
        request: ComposeScientificCombinationBatchRequest,
    ) -> tuple[ScientificCombinationObservation, ...]:
        rows = tuple(self._single.execute(item) for item in request.requests)
        return tuple(
            sorted(
                rows,
                key=lambda item: (
                    item.observed_at,
                    item.primary_scope,
                    item.combination_id.value,
                    item.horizon_seconds,
                ),
            )
        )
