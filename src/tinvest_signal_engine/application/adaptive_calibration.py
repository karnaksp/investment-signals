"""Application use case for lightweight post-session detector calibration."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Protocol

from tinvest_signal_engine.domain.adaptive_calibration import (
    CalibrationObservation,
    DailyCalibrationDecision,
    DailyCalibrationPolicy,
    DetectorThresholds,
    calibrate_daily_thresholds,
)


class CalibrationOutcomeSource(Protocol):
    def mature_price_jump_outcomes(
        self,
        *,
        since: datetime,
        until: datetime,
    ) -> tuple[CalibrationObservation, ...]: ...


class ActiveThresholdSource(Protocol):
    def active_price_jump_thresholds(self) -> DetectorThresholds: ...


class CalibrationDecisionSink(Protocol):
    def persist(
        self,
        decision: DailyCalibrationDecision,
        *,
        evaluated_at: datetime,
    ) -> None: ...


class RunDailyAdaptiveCalibration:
    def __init__(
        self,
        *,
        outcomes: CalibrationOutcomeSource,
        active_thresholds: ActiveThresholdSource,
        decisions: CalibrationDecisionSink,
        policy: DailyCalibrationPolicy,
        lookback_days: int,
    ) -> None:
        if lookback_days <= 0:
            raise ValueError("lookback_days must be positive")
        self._outcomes = outcomes
        self._active_thresholds = active_thresholds
        self._decisions = decisions
        self._policy = policy
        self._lookback_days = lookback_days

    def execute(self, *, now: datetime) -> DailyCalibrationDecision:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        observations = self._outcomes.mature_price_jump_outcomes(
            since=now - timedelta(days=self._lookback_days),
            until=now,
        )
        decision = calibrate_daily_thresholds(
            observations,
            self._active_thresholds.active_price_jump_thresholds(),
            self._policy,
        )
        self._decisions.persist(decision, evaluated_at=now)
        return decision
