"""Persist live morning assessments and seal their after-the-fact results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.morning_retracement_signals import (
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.domain.morning_retracement import (
    TradeExitReason,
    TradePolicy,
    simulate_trade,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    MorningRetracementLiveAssessment,
    MorningRetracementRuntimePolicy,
    MorningRetracementTrackedOutcome,
)


MOSCOW = ZoneInfo("Europe/Moscow")
OUTCOME_POLICY_VERSION = "morning-retracement-entry-tracking-v1"


@dataclass(frozen=True, slots=True)
class StoredMorningRetracementAssessment:
    observation_id: str
    assessment: MorningRetracementLiveAssessment


class MorningRetracementTrackingStore(Protocol):
    def persist_assessment(
        self,
        assessment: MorningRetracementLiveAssessment,
        *,
        recorded_at: datetime,
    ) -> str: ...

    def pending_assessments(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[StoredMorningRetracementAssessment, ...]: ...

    def persist_outcome(
        self,
        outcome: MorningRetracementTrackedOutcome,
        *,
        assessment: MorningRetracementLiveAssessment,
    ) -> None: ...


class RecordMorningRetracementAssessments:
    def __init__(self, store: MorningRetracementTrackingStore) -> None:
        self._store = store

    def execute(
        self,
        assessments: Sequence[MorningRetracementLiveAssessment],
        *,
        recorded_at: datetime,
    ) -> tuple[str, ...]:
        return tuple(
            self._store.persist_assessment(item, recorded_at=recorded_at)
            for item in assessments
        )


@dataclass(frozen=True, slots=True)
class MorningRetracementOutcomeBatch:
    scanned: int
    stored: int
    pending: int
    unavailable: int


class ProcessMorningRetracementOutcomes:
    """Evaluate every stored decision minute against the path through 11:00."""

    def __init__(
        self,
        *,
        store: MorningRetracementTrackingStore,
        policy: MorningRetracementRuntimePolicy,
        grace_seconds: int = 90,
    ) -> None:
        if grace_seconds < 0:
            raise ValueError("outcome grace must not be negative")
        self._store = store
        self._policy = policy
        self._grace_seconds = grace_seconds

    def execute(
        self,
        *,
        now: datetime,
        market: Sequence[MorningRetracementMarketSeries],
        limit: int = 500,
    ) -> MorningRetracementOutcomeBatch:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("outcome evaluation time must be timezone-aware")
        if limit <= 0:
            raise ValueError("outcome batch limit must be positive")
        pending_rows = self._store.pending_assessments(
            outcome_policy_version=OUTCOME_POLICY_VERSION,
            limit=limit,
        )
        market_by_instrument = {item.instrument_id: item for item in market}
        stored = pending = unavailable = 0
        for row in pending_rows:
            assessment = row.assessment
            deadline = _deadline(assessment, self._policy.deadline_local_minute)
            if now < deadline + timedelta(seconds=self._grace_seconds):
                pending += 1
                continue
            series = market_by_instrument.get(assessment.instrument_id)
            if series is None:
                pending += 1
                continue
            future = tuple(
                candle
                for candle in series.current_session
                if assessment.observed_at < candle.at <= deadline
            )
            simulation = simulate_trade(
                assessment.recommendation.snapshot,
                future,
                TradePolicy(
                    target_fraction=self._policy.target_fraction,
                    stop_extension_fraction=self._policy.stop_extension_fraction,
                    break_even_trigger_fraction=self._policy.break_even_trigger_fraction,
                    deadline_local_minute=self._policy.deadline_local_minute,
                    round_trip_cost_bps=self._policy.round_trip_cost_bps,
                ),
            )
            available = simulation.exit_reason is not TradeExitReason.UNAVAILABLE
            outcome = MorningRetracementTrackedOutcome(
                observation_id=row.observation_id,
                instrument_id=assessment.instrument_id,
                ticker=assessment.ticker,
                trading_day=assessment.trading_day,
                target_hit=(simulation.target_hit if available else None),
                non_loss=(simulation.non_loss if available else None),
                exit_reason=simulation.exit_reason.value,
                entry_at=simulation.entry_at,
                exit_at=simulation.exit_at,
                entry_price=simulation.entry_price,
                exit_price=simulation.exit_price,
                net_result_bps=simulation.net_result_bps,
                minutes_to_exit=(
                    (simulation.exit_at - simulation.entry_at).total_seconds() / 60.0
                    if simulation.exit_at is not None
                    and simulation.entry_at is not None
                    else None
                ),
                evaluated_at=now,
                outcome_policy_version=OUTCOME_POLICY_VERSION,
            )
            self._store.persist_outcome(outcome, assessment=assessment)
            stored += 1
            unavailable += int(not available)
        return MorningRetracementOutcomeBatch(
            scanned=len(pending_rows),
            stored=stored,
            pending=pending,
            unavailable=unavailable,
        )


def _deadline(
    assessment: MorningRetracementLiveAssessment,
    local_minute: int,
) -> datetime:
    observed = assessment.observed_at.astimezone(MOSCOW)
    return observed.replace(
        hour=local_minute // 60,
        minute=local_minute % 60,
        second=0,
        microsecond=0,
    )
