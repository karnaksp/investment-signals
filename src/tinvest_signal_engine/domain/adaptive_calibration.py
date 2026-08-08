"""Pure rules for bounded daily detector calibration.

The calibrator deliberately works only with mature, already persisted outcomes.
It can make a detector more selective, but it cannot lower a threshold because
events rejected by the current detector do not have comparable outcome labels.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from itertools import product
from math import log, sqrt
from statistics import fmean


_DECIDED_VERDICTS = frozenset({"confirmed", "contradicted", "insignificant"})


@dataclass(frozen=True, slots=True)
class CalibrationObservation:
    trading_day: date
    instrument_id: str
    z_score: float
    absolute_move_bps: float
    verdict: str

    def __post_init__(self) -> None:
        if not self.instrument_id.strip():
            raise ValueError("instrument_id must not be empty")
        if self.z_score < 0 or self.absolute_move_bps < 0:
            raise ValueError("calibration features must be non-negative")
        if self.verdict not in _DECIDED_VERDICTS:
            raise ValueError("calibration requires a decided outcome")


@dataclass(frozen=True, slots=True)
class DetectorThresholds:
    price_return_zscore_threshold: float
    price_move_absolute_threshold_bps: float

    def __post_init__(self) -> None:
        if self.price_return_zscore_threshold <= 0:
            raise ValueError("z-score threshold must be positive")
        if self.price_move_absolute_threshold_bps < 0:
            raise ValueError("absolute threshold must be non-negative")


@dataclass(frozen=True, slots=True)
class DailyCalibrationPolicy:
    minimum_sessions: int = 10
    minimum_training_observations: int = 30
    minimum_validation_observations: int = 10
    validation_session_share: float = 0.25
    minimum_candidate_coverage: float = 0.50
    minimum_validation_improvement: float = 0.02
    confidence_alpha: float = 0.05
    maximum_daily_zscore_increase: float = 0.15
    maximum_daily_absolute_increase_bps: float = 5.0

    def __post_init__(self) -> None:
        if self.minimum_sessions < 2:
            raise ValueError("minimum_sessions must be at least two")
        if min(
            self.minimum_training_observations,
            self.minimum_validation_observations,
        ) <= 0:
            raise ValueError("minimum observation counts must be positive")
        for value in (
            self.validation_session_share,
            self.minimum_candidate_coverage,
            self.confidence_alpha,
        ):
            if not 0 < value < 1:
                raise ValueError("calibration fractions must be between zero and one")
        if self.minimum_validation_improvement < 0:
            raise ValueError("minimum improvement must be non-negative")
        if self.maximum_daily_zscore_increase <= 0:
            raise ValueError("maximum z-score increase must be positive")
        if self.maximum_daily_absolute_increase_bps <= 0:
            raise ValueError("maximum absolute increase must be positive")


@dataclass(frozen=True, slots=True)
class ThresholdEvaluation:
    observations: int
    coverage: float
    mean_utility: float
    conservative_utility: float


@dataclass(frozen=True, slots=True)
class DailyCalibrationDecision:
    status: str
    reason_code: str
    active: DetectorThresholds
    candidate: DetectorThresholds | None
    training: ThresholdEvaluation | None
    validation: ThresholdEvaluation | None
    baseline_validation: ThresholdEvaluation | None
    training_days: tuple[date, ...]
    validation_days: tuple[date, ...]
    version: str

    @property
    def should_apply(self) -> bool:
        return self.status == "accepted" and self.candidate is not None


def calibrate_daily_thresholds(
    observations: tuple[CalibrationObservation, ...],
    active: DetectorThresholds,
    policy: DailyCalibrationPolicy = DailyCalibrationPolicy(),
) -> DailyCalibrationDecision:
    """Select and validate a bounded threshold update chronologically."""

    ordered = tuple(
        sorted(
            observations,
            key=lambda item: (
                item.trading_day,
                item.instrument_id,
                item.z_score,
                item.absolute_move_bps,
                item.verdict,
            ),
        )
    )
    days = tuple(sorted({item.trading_day for item in ordered}))
    if len(days) < policy.minimum_sessions:
        return _no_change("insufficient_sessions", active, days)

    validation_day_count = max(2, round(len(days) * policy.validation_session_share))
    validation_days = days[-validation_day_count:]
    training_days = days[:-validation_day_count]
    training = tuple(item for item in ordered if item.trading_day in training_days)
    validation = tuple(item for item in ordered if item.trading_day in validation_days)
    if len(training) < policy.minimum_training_observations:
        return _no_change(
            "insufficient_training_observations", active, days, training_days, validation_days
        )

    baseline_training = _evaluate(training, active, len(training), policy)
    baseline_validation = _evaluate(validation, active, len(validation), policy)
    if baseline_validation.observations < policy.minimum_validation_observations:
        return _no_change(
            "insufficient_validation_observations",
            active,
            days,
            training_days,
            validation_days,
            baseline_validation=baseline_validation,
        )

    candidates = _candidate_thresholds(training, active, policy)
    ranked: list[tuple[float, float, DetectorThresholds, ThresholdEvaluation]] = []
    for candidate in candidates:
        evaluation = _evaluate(training, candidate, len(training), policy)
        if evaluation.observations < policy.minimum_training_observations:
            continue
        if evaluation.coverage < policy.minimum_candidate_coverage:
            continue
        ranked.append(
            (
                evaluation.conservative_utility,
                evaluation.mean_utility,
                candidate,
                evaluation,
            )
        )
    if not ranked:
        return _no_change(
            "no_candidate_with_sufficient_coverage",
            active,
            days,
            training_days,
            validation_days,
            baseline_validation=baseline_validation,
        )

    _, _, candidate, training_evaluation = max(
        ranked,
        key=lambda item: (
            item[0],
            item[1],
            -item[2].price_return_zscore_threshold,
            -item[2].price_move_absolute_threshold_bps,
        ),
    )
    if candidate == active or (
        training_evaluation.conservative_utility
        <= baseline_training.conservative_utility
    ):
        return _no_change(
            "active_thresholds_remain_best",
            active,
            days,
            training_days,
            validation_days,
            baseline_validation=baseline_validation,
        )

    candidate_validation = _evaluate(validation, candidate, len(validation), policy)
    if candidate_validation.observations < policy.minimum_validation_observations:
        return _no_change(
            "candidate_validation_sample_too_small",
            active,
            days,
            training_days,
            validation_days,
            baseline_validation=baseline_validation,
        )
    improvement = (
        candidate_validation.conservative_utility
        - baseline_validation.conservative_utility
    )
    if improvement < policy.minimum_validation_improvement:
        return _no_change(
            "candidate_did_not_improve_validation",
            active,
            days,
            training_days,
            validation_days,
            baseline_validation=baseline_validation,
        )

    return DailyCalibrationDecision(
        status="accepted",
        reason_code="chronological_validation_improved",
        active=active,
        candidate=candidate,
        training=training_evaluation,
        validation=candidate_validation,
        baseline_validation=baseline_validation,
        training_days=training_days,
        validation_days=validation_days,
        version=_version(active, candidate, days),
    )


def _candidate_thresholds(
    observations: tuple[CalibrationObservation, ...],
    active: DetectorThresholds,
    policy: DailyCalibrationPolicy,
) -> tuple[DetectorThresholds, ...]:
    z_cap = active.price_return_zscore_threshold * (
        1 + policy.maximum_daily_zscore_increase
    )
    absolute_cap = (
        active.price_move_absolute_threshold_bps
        + policy.maximum_daily_absolute_increase_bps
    )
    z_values = _grid(
        (item.z_score for item in observations),
        floor=active.price_return_zscore_threshold,
        cap=z_cap,
    )
    absolute_values = _grid(
        (item.absolute_move_bps for item in observations),
        floor=active.price_move_absolute_threshold_bps,
        cap=absolute_cap,
    )
    return tuple(
        sorted(
            {
                DetectorThresholds(round(z_score, 4), round(move_bps, 4))
                for z_score, move_bps in product(z_values, absolute_values)
            },
            key=lambda item: (
                item.price_return_zscore_threshold,
                item.price_move_absolute_threshold_bps,
            ),
        )
    )


def _grid(values, *, floor: float, cap: float) -> tuple[float, ...]:
    ordered = tuple(sorted(float(value) for value in values if float(value) >= floor))
    if not ordered:
        return (floor,)
    result = {floor}
    for quantile in (0.50, 0.60, 0.70, 0.80):
        index = min(len(ordered) - 1, round((len(ordered) - 1) * quantile))
        result.add(min(cap, max(floor, ordered[index])))
    return tuple(sorted(result))


def _evaluate(
    observations: tuple[CalibrationObservation, ...],
    thresholds: DetectorThresholds,
    total: int,
    policy: DailyCalibrationPolicy,
) -> ThresholdEvaluation:
    selected = tuple(
        item
        for item in observations
        if item.z_score >= thresholds.price_return_zscore_threshold
        and item.absolute_move_bps >= thresholds.price_move_absolute_threshold_bps
    )
    utilities = tuple(_utility(item.verdict) for item in selected)
    if not utilities:
        return ThresholdEvaluation(0, 0.0, 0.0, float("-inf"))
    mean = fmean(utilities)
    penalty = sqrt(log(1 / policy.confidence_alpha) / (2 * len(utilities)))
    return ThresholdEvaluation(
        observations=len(utilities),
        coverage=(len(utilities) / total if total else 0.0),
        mean_utility=mean,
        conservative_utility=mean - penalty,
    )


def _utility(verdict: str) -> float:
    if verdict == "confirmed":
        return 1.0
    if verdict == "contradicted":
        return -1.0
    return 0.0


def _no_change(
    reason: str,
    active: DetectorThresholds,
    days: tuple[date, ...],
    training_days: tuple[date, ...] = (),
    validation_days: tuple[date, ...] = (),
    *,
    baseline_validation: ThresholdEvaluation | None = None,
) -> DailyCalibrationDecision:
    return DailyCalibrationDecision(
        status="no_change",
        reason_code=reason,
        active=active,
        candidate=None,
        training=None,
        validation=None,
        baseline_validation=baseline_validation,
        training_days=training_days,
        validation_days=validation_days,
        version=_version(active, None, days),
    )


def _version(
    active: DetectorThresholds,
    candidate: DetectorThresholds | None,
    days: tuple[date, ...],
) -> str:
    material = "|".join(
        (
            "adaptive-calibration-v1",
            repr(active),
            repr(candidate),
            *(item.isoformat() for item in days),
        )
    )
    return f"adaptive-v1-{sha256(material.encode('utf-8')).hexdigest()[:16]}"
