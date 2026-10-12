"""Application use cases for automatic signal outcome evaluation."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol, Sequence

from tinvest_signal_engine.domain.reference_ticks import ReferenceTick
from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    DirectionalSignalOutcome,
    evaluate_directional_outcome,
    reference_price,
)


@dataclass(frozen=True)
class DirectionalSignalOutcomeRequest:
    signal_id: str
    instrument_id: str
    signal_type: str
    source_event_at: datetime
    expected_direction: int
    realized_volatility_bps: Decimal
    policy: DirectionalOutcomePolicy


class DirectionalSignalOutcomeStore(Protocol):
    def persist(self, outcome: DirectionalSignalOutcome) -> str: ...


class DirectionalSignalOutcomeCandidateSource(Protocol):
    def due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> Sequence[DirectionalSignalOutcomeRequest]: ...


class ReferenceTickReader(Protocol):
    def load(
        self,
        *,
        instrument_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> Sequence[ReferenceTick]: ...


@dataclass(frozen=True)
class DirectionalSignalOutcomeProcessingResult:
    status: Literal["pending", "stored"]
    reason_code: str
    outcome: DirectionalSignalOutcome | None = None
    outcome_id: str | None = None


@dataclass(frozen=True)
class DirectionalSignalOutcomeBatchResult:
    scanned: int
    stored: int
    pending: int
    outcome_ids: tuple[str, ...]
    reason_counts: tuple[tuple[str, int], ...]


class DirectionalSignalOutcomeProcessor:
    def __init__(self, store: DirectionalSignalOutcomeStore) -> None:
        self._store = store

    def process(
        self,
        *,
        request: DirectionalSignalOutcomeRequest,
        ticks: Sequence[ReferenceTick],
        now: datetime,
    ) -> DirectionalSignalOutcomeProcessingResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        maturity_at = request.source_event_at + timedelta(
            seconds=(
                request.policy.horizon_seconds + request.policy.forward_grace_seconds
            )
        )
        if now < maturity_at:
            return DirectionalSignalOutcomeProcessingResult(
                status="pending",
                reason_code="outcome_horizon_not_mature",
            )
        outcome = evaluate_directional_signal_from_ticks(request=request, ticks=ticks)
        outcome_id = self._store.persist(outcome)
        return DirectionalSignalOutcomeProcessingResult(
            status="stored",
            reason_code=outcome.reason_code,
            outcome=outcome,
            outcome_id=outcome_id,
        )


class DirectionalSignalOutcomeBatchProcessor:
    """Process due directional signals through application-owned ports.

    Adapters decide how stored signal records become
    ``DirectionalSignalOutcomeRequest`` instances and how reference ticks are
    loaded.  This use case only coordinates maturity, bounded tick retrieval,
    deterministic evaluation, and durable outcome persistence.
    """

    def __init__(
        self,
        *,
        candidates: DirectionalSignalOutcomeCandidateSource,
        ticks: ReferenceTickReader,
        store: DirectionalSignalOutcomeStore,
    ) -> None:
        self._candidates = candidates
        self._ticks = ticks
        self._processor = DirectionalSignalOutcomeProcessor(store)

    def process_due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> DirectionalSignalOutcomeBatchResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if limit <= 0:
            raise ValueError("limit must be positive")

        requests = tuple(self._candidates.due(now=now, limit=limit))
        stored = 0
        pending = 0
        outcome_ids: list[str] = []
        reason_counts: Counter[str] = Counter()

        for request in requests:
            maturity_at = _maturity_at(request)
            if now < maturity_at:
                pending += 1
                reason_counts["outcome_horizon_not_mature"] += 1
                continue

            tick_window_start = request.source_event_at - timedelta(
                seconds=request.policy.anchor_max_age_seconds,
            )
            tick_window_end = maturity_at
            result = self._processor.process(
                request=request,
                ticks=self._ticks.load(
                    instrument_id=request.instrument_id,
                    start_at=tick_window_start,
                    end_at=tick_window_end,
                ),
                now=now,
            )
            reason_counts[result.reason_code] += 1
            if result.status == "stored":
                stored += 1
                if result.outcome_id is not None:
                    outcome_ids.append(result.outcome_id)
            else:
                pending += 1

        return DirectionalSignalOutcomeBatchResult(
            scanned=len(requests),
            stored=stored,
            pending=pending,
            outcome_ids=tuple(outcome_ids),
            reason_counts=tuple(sorted(reason_counts.items())),
        )


def evaluate_directional_signal_from_ticks(
    *,
    request: DirectionalSignalOutcomeRequest,
    ticks: Sequence[ReferenceTick],
) -> DirectionalSignalOutcome:
    """Evaluate one signal using only eligible reference ticks.

    The anchor candidate is the latest valid price at or before source time. The
    forward candidate is the earliest valid price at or after the horizon target
    and no later than the configured grace deadline. The function never selects a
    pre-source anchor or pre-target forward price and leaves stale/missing price
    handling to the domain evaluator.
    """

    source_event_at = request.source_event_at
    if source_event_at.tzinfo is None or source_event_at.utcoffset() is None:
        raise ValueError("source_event_at must be timezone-aware")
    target_at = source_event_at + timedelta(seconds=request.policy.horizon_seconds)
    forward_deadline = target_at + timedelta(
        seconds=request.policy.forward_grace_seconds
    )
    eligible = tuple(
        sorted(
            (
                tick
                for tick in ticks
                if tick.instrument_id == request.instrument_id
                and reference_price(tick) is not None
            ),
            key=lambda tick: tick.event_at,
        )
    )
    anchor = _latest_at_or_before(eligible, source_event_at)
    forward = _first_in_window(eligible, target_at, forward_deadline)
    return evaluate_directional_outcome(
        signal_id=request.signal_id,
        instrument_id=request.instrument_id,
        signal_type=request.signal_type,
        source_event_at=source_event_at,
        expected_direction=request.expected_direction,
        anchor_tick=anchor,
        forward_tick=forward,
        realized_volatility_bps=request.realized_volatility_bps,
        policy=request.policy,
    )


def _maturity_at(request: DirectionalSignalOutcomeRequest) -> datetime:
    return request.source_event_at + timedelta(
        seconds=request.policy.horizon_seconds + request.policy.forward_grace_seconds,
    )


def _latest_at_or_before(
    ticks: Sequence[ReferenceTick],
    at: datetime,
) -> ReferenceTick | None:
    selected: ReferenceTick | None = None
    for tick in ticks:
        if tick.event_at <= at:
            selected = tick
        else:
            break
    return selected


def _first_in_window(
    ticks: Sequence[ReferenceTick],
    start: datetime,
    end: datetime,
) -> ReferenceTick | None:
    for tick in ticks:
        if tick.event_at < start:
            continue
        if tick.event_at > end:
            return None
        return tick
    return None
