"""Causal primitives for the next candle-based scientific hypotheses.

The module deliberately contains only deterministic calculations and immutable
records.  Candle collection, chronological splitting, and persistence belong
to application and adapter layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
from math import isfinite, log
from typing import Iterable


class ScientificCandleHypothesis(str, Enum):
    OPENING_GAP_REVERSION = "H10"
    MARKET_RESIDUAL_REVERSION = "H11"
    HAR_VOLATILITY = "H15"
    RELATIVE_VOLUME_ACTIVITY_V2 = "H7V2"


class ScientificTarget(str, Enum):
    DIRECTIONAL_RETURN_BPS = "directional_return_bps"
    FUTURE_REALIZED_VARIANCE = "future_realized_variance"
    FUTURE_ACTIVITY_UPLIFT = "future_activity_uplift"


class FeatureDecision(str, Enum):
    MATCHED = "matched"
    NOT_MATCHED = "not_matched"
    ABSTAIN = "abstain"


class AbstentionReason(str, Enum):
    CONDITIONS_MATCHED = "conditions_matched"
    CONDITIONS_NOT_MET = "conditions_not_met"
    INSUFFICIENT_HISTORY = "insufficient_history"
    INSUFFICIENT_MARKET_MEMBERS = "insufficient_market_members"
    MARKET_BETA_UNAVAILABLE = "market_beta_unavailable"
    BASKET_COVERAGE_BELOW_MINIMUM = "basket_coverage_below_minimum"
    CORPORATE_ACTION_SUSPECTED = "corporate_action_suspected"
    MARKET_WIDE_MOVE_SAME_DIRECTION = "market_wide_move_same_direction"
    NON_POSITIVE_OPENING_GAP = "non_positive_opening_gap"
    DIRECTION_UNAVAILABLE = "direction_unavailable"
    NON_CONTIGUOUS_WINDOW = "non_contiguous_window"
    MODEL_NOT_TRAINED = "model_not_trained"
    INVALID_BASELINE = "invalid_baseline"
    OUTCOME_UNAVAILABLE = "outcome_unavailable"


@dataclass(frozen=True, slots=True)
class ScientificCandlePolicy:
    version: str = "scientific-candle-models-v1.0.0"
    opening_gap_min_bps: float = 20.0
    opening_gap_horizon_seconds: int = 1800
    residual_window_minutes: int = 5
    residual_horizon_seconds: int = 900
    residual_beta_lookback_days: int = 20
    residual_basket_coverage_min: float = 0.80
    residual_percentile_min: float = 0.99
    har_windows_minutes: tuple[int, int, int] = (5, 30, 120)
    har_horizon_seconds: int = 1800
    har_minimum_training_points: int = 30
    har_ridge_penalty: float = 1e-8
    activity_window_minutes: int = 15
    activity_horizon_seconds: int = 1800
    activity_history_days: int = 20
    activity_volume_percentile: float = 0.90
    activity_minimum_uplift: float = 1.25
    round_trip_cost_bps: float = 10.0

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        positive_values = (
            self.opening_gap_min_bps,
            self.opening_gap_horizon_seconds,
            self.residual_window_minutes,
            self.residual_horizon_seconds,
            self.residual_beta_lookback_days,
            *self.har_windows_minutes,
            self.har_horizon_seconds,
            self.har_minimum_training_points,
            self.activity_window_minutes,
            self.activity_horizon_seconds,
            self.activity_history_days,
            self.activity_minimum_uplift,
        )
        if any(value <= 0 for value in positive_values):
            raise ValueError(
                "policy windows, horizons, thresholds, and minima must be positive"
            )
        if tuple(sorted(self.har_windows_minutes)) != self.har_windows_minutes:
            raise ValueError("HAR windows must be strictly ordered")
        if len(set(self.har_windows_minutes)) != 3:
            raise ValueError("HAR requires three unique windows")
        if not 0.0 < self.activity_volume_percentile < 1.0:
            raise ValueError("activity_volume_percentile must be between zero and one")
        if not 0.0 < self.residual_basket_coverage_min <= 1.0:
            raise ValueError("residual_basket_coverage_min must be in (0, 1]")
        if not 0.0 < self.residual_percentile_min < 1.0:
            raise ValueError("residual_percentile_min must be between zero and one")
        if self.har_ridge_penalty < 0.0 or self.round_trip_cost_bps < 0.0:
            raise ValueError("ridge penalty and costs must be non-negative")


@dataclass(frozen=True, slots=True)
class HarTrainingPoint:
    feature_at: datetime
    target_at: datetime
    short_variance: float
    medium_variance: float
    long_variance: float
    target_variance: float

    def __post_init__(self) -> None:
        _require_aware(self.feature_at, "feature_at")
        _require_aware(self.target_at, "target_at")
        if self.target_at <= self.feature_at:
            raise ValueError("HAR target must occur after its features")
        values = (
            self.short_variance,
            self.medium_variance,
            self.long_variance,
            self.target_variance,
        )
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("HAR variances must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class HarParameters:
    intercept: float
    short_weight: float
    medium_weight: float
    long_weight: float
    training_points: int
    trained_until: datetime

    def __post_init__(self) -> None:
        _require_aware(self.trained_until, "trained_until")
        values = (
            self.intercept,
            self.short_weight,
            self.medium_weight,
            self.long_weight,
        )
        if any(not isfinite(value) for value in values):
            raise ValueError("HAR parameters must be finite")
        if self.training_points <= 0:
            raise ValueError("HAR training_points must be positive")

    def predict(self, short: float, medium: float, long: float) -> float:
        values = (short, medium, long)
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("HAR input variances must be finite and non-negative")
        forecast = (
            self.intercept
            + self.short_weight * short
            + self.medium_weight * medium
            + self.long_weight * long
        )
        return max(forecast, 1e-12)


@dataclass(frozen=True, slots=True)
class CausalFeatureVector:
    observation_id: str
    hypothesis: ScientificCandleHypothesis
    hypothesis_version: str
    ticker: str
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    model_trained_until: datetime | None
    horizon_seconds: int
    target: ScientificTarget
    decision: FeatureDecision
    reason: AbstentionReason
    expected_direction: int
    forecast_value: float | None
    feature_values: tuple[tuple[str, float], ...]

    def __post_init__(self) -> None:
        if not self.observation_id.startswith("sha256:"):
            raise ValueError("observation_id must use sha256")
        if not self.hypothesis_version.strip() or not self.ticker.strip():
            raise ValueError("hypothesis version and ticker are required")
        _require_aware(self.observed_at, "observed_at")
        _require_aware(self.feature_max_observed_at, "feature_max_observed_at")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("feature vector uses future market data")
        if self.model_trained_until is not None:
            _require_aware(self.model_trained_until, "model_trained_until")
            if self.model_trained_until > self.observed_at:
                raise ValueError("model was trained with data from the future")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.expected_direction not in {-1, 0, 1}:
            raise ValueError("expected_direction must be -1, 0, or 1")
        names = tuple(name for name, _ in self.feature_values)
        if len(names) != len(set(names)):
            raise ValueError("feature names must be unique")
        if any(
            not name.strip() or not isfinite(value)
            for name, value in self.feature_values
        ):
            raise ValueError("feature values must be named and finite")
        if self.decision is FeatureDecision.MATCHED and self.forecast_value is None:
            raise ValueError("matched feature requires a forecast")
        if self.forecast_value is not None and not isfinite(self.forecast_value):
            raise ValueError("forecast_value must be finite")

    def value(self, name: str) -> float:
        try:
            return next(value for key, value in self.feature_values if key == name)
        except StopIteration as exc:
            raise KeyError(name) from exc


@dataclass(frozen=True, slots=True)
class ScientificModelOutcome:
    observation_id: str
    target_at: datetime
    available: bool
    reason: AbstentionReason
    actual_value: float | None
    cost_adjusted_value: float | None
    model_loss: float | None
    benchmark_loss: float | None
    supported: bool | None

    def __post_init__(self) -> None:
        _require_aware(self.target_at, "target_at")
        numeric = (
            self.actual_value,
            self.cost_adjusted_value,
            self.model_loss,
            self.benchmark_loss,
        )
        if any(value is not None and not isfinite(value) for value in numeric):
            raise ValueError("outcome values must be finite")
        if self.available and self.actual_value is None:
            raise ValueError("available outcome requires actual_value")
        if not self.available and any(value is not None for value in numeric):
            raise ValueError("unavailable outcome must not contain results")


def fit_har_parameters(
    points: Iterable[HarTrainingPoint],
    *,
    minimum_points: int,
    ridge_penalty: float,
) -> HarParameters:
    training = tuple(points)
    if len(training) < minimum_points:
        raise ValueError("insufficient HAR training points")
    if ridge_penalty < 0.0:
        raise ValueError("ridge_penalty must be non-negative")
    size = 4
    matrix = [[0.0] * size for _ in range(size)]
    target = [0.0] * size
    for point in training:
        row = (1.0, point.short_variance, point.medium_variance, point.long_variance)
        for left in range(size):
            target[left] += row[left] * point.target_variance
            for right in range(size):
                matrix[left][right] += row[left] * row[right]
    for index in range(1, size):
        matrix[index][index] += ridge_penalty
    intercept, short, medium, long = _solve(matrix, target)
    return HarParameters(
        intercept=intercept,
        short_weight=short,
        medium_weight=medium,
        long_weight=long,
        training_points=len(training),
        trained_until=max(point.target_at for point in training),
    )


def opening_gap_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    previous_close: float,
    opening_price: float,
    policy: ScientificCandlePolicy,
) -> CausalFeatureVector:
    if previous_close <= 0.0 or opening_price <= 0.0:
        raise ValueError("opening gap prices must be positive")
    gap_bps = (opening_price / previous_close - 1.0) * 10_000.0
    if gap_bps <= 0.0:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.NON_POSITIVE_OPENING_GAP,
        )
    elif gap_bps < policy.opening_gap_min_bps:
        decision, reason = (
            FeatureDecision.NOT_MATCHED,
            AbstentionReason.CONDITIONS_NOT_MET,
        )
    else:
        decision, reason = FeatureDecision.MATCHED, AbstentionReason.CONDITIONS_MATCHED
    return _feature(
        hypothesis=ScientificCandleHypothesis.OPENING_GAP_REVERSION,
        version="1.0.0",
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        horizon_seconds=policy.opening_gap_horizon_seconds,
        target=ScientificTarget.DIRECTIONAL_RETURN_BPS,
        decision=decision,
        reason=reason,
        expected_direction=-1 if decision is FeatureDecision.MATCHED else 0,
        forecast_value=-gap_bps if decision is FeatureDecision.MATCHED else None,
        feature_values=(
            ("previous_close", previous_close),
            ("opening_price", opening_price),
            ("opening_gap_bps", gap_bps),
        ),
    )


def residual_reversal_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    instrument_return_bps: float,
    market_return_bps: float,
    market_beta: float | None,
    beta_observed_until: datetime | None,
    beta_history_days: int,
    basket_coverage: float,
    absolute_residual_history: Iterable[float],
    absolute_market_return_history: Iterable[float],
    trading_gap: bool = False,
    corporate_action_suspected: bool = False,
    policy: ScientificCandlePolicy,
) -> CausalFeatureVector:
    residual_history = tuple(absolute_residual_history)
    market_history = tuple(absolute_market_return_history)
    if not 0.0 <= basket_coverage <= 1.0:
        raise ValueError("basket_coverage must be between zero and one")
    if any(value < 0.0 or not isfinite(value) for value in residual_history):
        raise ValueError("absolute residual history must be finite and non-negative")
    if any(value < 0.0 or not isfinite(value) for value in market_history):
        raise ValueError("absolute market history must be finite and non-negative")
    if market_beta is not None and not isfinite(market_beta):
        raise ValueError("market_beta must be finite")
    if beta_observed_until is not None:
        _require_aware(beta_observed_until, "beta_observed_until")
        if beta_observed_until >= observed_at:
            raise ValueError("market beta must use completed prior trading days only")

    resolved_beta = market_beta or 0.0
    residual_bps = instrument_return_bps - resolved_beta * market_return_bps
    residual_percentile = (
        sum(value <= abs(residual_bps) for value in residual_history)
        / len(residual_history)
        if residual_history
        else 0.0
    )
    market_percentile = (
        sum(value <= abs(market_return_bps) for value in market_history)
        / len(market_history)
        if market_history
        else 0.0
    )
    same_market_direction = (
        instrument_return_bps != 0.0
        and market_return_bps != 0.0
        and (instrument_return_bps > 0.0) == (market_return_bps > 0.0)
    )
    if trading_gap:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.NON_CONTIGUOUS_WINDOW,
        )
    elif basket_coverage < policy.residual_basket_coverage_min:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.BASKET_COVERAGE_BELOW_MINIMUM,
        )
    elif corporate_action_suspected:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.CORPORATE_ACTION_SUSPECTED,
        )
    elif (
        market_beta is None
        or beta_observed_until is None
        or beta_history_days < policy.residual_beta_lookback_days
        or not residual_history
        or not market_history
    ):
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.MARKET_BETA_UNAVAILABLE,
        )
    elif same_market_direction and market_percentile >= policy.residual_percentile_min:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.MARKET_WIDE_MOVE_SAME_DIRECTION,
        )
    elif residual_bps == 0.0:
        decision, reason = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.DIRECTION_UNAVAILABLE,
        )
    elif residual_percentile < policy.residual_percentile_min:
        decision, reason = (
            FeatureDecision.NOT_MATCHED,
            AbstentionReason.CONDITIONS_NOT_MET,
        )
    else:
        decision, reason = FeatureDecision.MATCHED, AbstentionReason.CONDITIONS_MATCHED
    direction = -1 if residual_bps > 0.0 else 1
    return _feature(
        hypothesis=ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION,
        version="1.0.0",
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        model_trained_until=beta_observed_until,
        horizon_seconds=policy.residual_horizon_seconds,
        target=ScientificTarget.DIRECTIONAL_RETURN_BPS,
        decision=decision,
        reason=reason,
        expected_direction=direction if decision is FeatureDecision.MATCHED else 0,
        forecast_value=-residual_bps if decision is FeatureDecision.MATCHED else None,
        feature_values=(
            ("instrument_return_bps", instrument_return_bps),
            ("market_return_bps", market_return_bps),
            ("market_beta", resolved_beta),
            ("market_residual_bps", residual_bps),
            ("market_beta_history_days", float(beta_history_days)),
            ("basket_coverage", basket_coverage),
            ("absolute_residual_percentile", residual_percentile),
            ("absolute_market_return_percentile", market_percentile),
        ),
    )


def har_volatility_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    short_variance: float,
    medium_variance: float,
    long_variance: float,
    parameters: HarParameters | None,
    policy: ScientificCandlePolicy,
) -> CausalFeatureVector:
    if parameters is None:
        decision, reason, forecast = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.MODEL_NOT_TRAINED,
            None,
        )
    elif parameters.trained_until > observed_at:
        raise ValueError("HAR parameters use future labels")
    else:
        decision, reason = FeatureDecision.MATCHED, AbstentionReason.CONDITIONS_MATCHED
        forecast = parameters.predict(short_variance, medium_variance, long_variance)
    return _feature(
        hypothesis=ScientificCandleHypothesis.HAR_VOLATILITY,
        version="1.0.0",
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        model_trained_until=parameters.trained_until if parameters else None,
        horizon_seconds=policy.har_horizon_seconds,
        target=ScientificTarget.FUTURE_REALIZED_VARIANCE,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast_value=forecast,
        feature_values=(
            ("short_realized_variance", short_variance),
            ("medium_realized_variance", medium_variance),
            ("long_realized_variance", long_variance),
        ),
    )


def relative_volume_activity_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    current_volume: float,
    historical_phase_volumes: Iterable[float],
    baseline_future_variance: float,
    policy: ScientificCandlePolicy,
) -> CausalFeatureVector:
    history = tuple(historical_phase_volumes)
    if current_volume < 0.0 or any(value < 0.0 for value in history):
        raise ValueError("volume values must be non-negative")
    if len(history) < policy.activity_history_days:
        decision, reason, percentile = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.INSUFFICIENT_HISTORY,
            0.0,
        )
    elif baseline_future_variance <= 0.0:
        decision, reason, percentile = (
            FeatureDecision.ABSTAIN,
            AbstentionReason.INVALID_BASELINE,
            0.0,
        )
    else:
        percentile = sum(value <= current_volume for value in history) / len(history)
        if percentile >= policy.activity_volume_percentile:
            decision, reason = (
                FeatureDecision.MATCHED,
                AbstentionReason.CONDITIONS_MATCHED,
            )
        else:
            decision, reason = (
                FeatureDecision.NOT_MATCHED,
                AbstentionReason.CONDITIONS_NOT_MET,
            )
    return _feature(
        hypothesis=ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
        version="2.0.0",
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        horizon_seconds=policy.activity_horizon_seconds,
        target=ScientificTarget.FUTURE_ACTIVITY_UPLIFT,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast_value=(
            baseline_future_variance * policy.activity_minimum_uplift
            if decision is FeatureDecision.MATCHED
            else None
        ),
        feature_values=(
            ("current_window_volume", current_volume),
            ("phase_volume_percentile", percentile),
            ("phase_history_days", float(len(history))),
            ("baseline_future_variance", baseline_future_variance),
        ),
    )


def directional_outcome(
    feature: CausalFeatureVector,
    *,
    target_at: datetime,
    forward_return_bps: float | None,
    policy: ScientificCandlePolicy,
) -> ScientificModelOutcome:
    if feature.target is not ScientificTarget.DIRECTIONAL_RETURN_BPS:
        raise ValueError("directional outcome requires directional feature")
    if feature.decision is not FeatureDecision.MATCHED or forward_return_bps is None:
        return _unavailable_outcome(feature, target_at)
    net = feature.expected_direction * forward_return_bps - policy.round_trip_cost_bps
    return ScientificModelOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=True,
        reason=AbstentionReason.CONDITIONS_MATCHED,
        actual_value=forward_return_bps,
        cost_adjusted_value=net,
        model_loss=None,
        benchmark_loss=None,
        supported=net > 0.0,
    )


def variance_outcome(
    feature: CausalFeatureVector,
    *,
    target_at: datetime,
    actual_future_variance: float | None,
    policy: ScientificCandlePolicy,
) -> ScientificModelOutcome:
    if feature.target not in {
        ScientificTarget.FUTURE_REALIZED_VARIANCE,
        ScientificTarget.FUTURE_ACTIVITY_UPLIFT,
    }:
        raise ValueError("variance outcome requires an activity or variance feature")
    if (
        feature.decision is not FeatureDecision.MATCHED
        or actual_future_variance is None
        or actual_future_variance < 0.0
    ):
        return _unavailable_outcome(feature, target_at)
    if feature.target is ScientificTarget.FUTURE_REALIZED_VARIANCE:
        forecast = feature.forecast_value
        if forecast is None:
            return _unavailable_outcome(feature, target_at)
        benchmark = max(feature.value("long_realized_variance"), 1e-12)
        actual = max(actual_future_variance, 1e-12)
        model_loss = qlike_loss(actual, forecast)
        benchmark_loss = qlike_loss(actual, benchmark)
        return ScientificModelOutcome(
            observation_id=feature.observation_id,
            target_at=target_at,
            available=True,
            reason=AbstentionReason.CONDITIONS_MATCHED,
            actual_value=actual_future_variance,
            cost_adjusted_value=None,
            model_loss=model_loss,
            benchmark_loss=benchmark_loss,
            supported=model_loss < benchmark_loss,
        )
    baseline = feature.value("baseline_future_variance")
    if baseline <= 0.0:
        return _unavailable_outcome(feature, target_at)
    uplift = actual_future_variance / baseline
    return ScientificModelOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=True,
        reason=AbstentionReason.CONDITIONS_MATCHED,
        actual_value=uplift,
        cost_adjusted_value=None,
        model_loss=None,
        benchmark_loss=None,
        supported=uplift >= policy.activity_minimum_uplift,
    )


def qlike_loss(actual_variance: float, forecast_variance: float) -> float:
    if actual_variance <= 0.0 or forecast_variance <= 0.0:
        raise ValueError("QLIKE requires positive variances")
    ratio = actual_variance / forecast_variance
    return ratio - log(ratio) - 1.0


def _feature(
    *,
    hypothesis: ScientificCandleHypothesis,
    version: str,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
    target: ScientificTarget,
    decision: FeatureDecision,
    reason: AbstentionReason,
    expected_direction: int,
    forecast_value: float | None,
    feature_values: tuple[tuple[str, float], ...],
    model_trained_until: datetime | None = None,
) -> CausalFeatureVector:
    identity = "|".join((hypothesis.value, version, ticker, observed_at.isoformat()))
    return CausalFeatureVector(
        observation_id="sha256:" + sha256(identity.encode()).hexdigest(),
        hypothesis=hypothesis,
        hypothesis_version=version,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        feature_max_observed_at=observed_at,
        model_trained_until=model_trained_until,
        horizon_seconds=horizon_seconds,
        target=target,
        decision=decision,
        reason=reason,
        expected_direction=expected_direction,
        forecast_value=forecast_value,
        feature_values=feature_values,
    )


def _unavailable_outcome(
    feature: CausalFeatureVector,
    target_at: datetime,
) -> ScientificModelOutcome:
    return ScientificModelOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=False,
        reason=AbstentionReason.OUTCOME_UNAVAILABLE,
        actual_value=None,
        cost_adjusted_value=None,
        model_loss=None,
        benchmark_loss=None,
        supported=None,
    )


def _solve(matrix: list[list[float]], target: list[float]) -> tuple[float, ...]:
    size = len(target)
    augmented = [row[:] + [target[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-18:
            raise ValueError("HAR training matrix is singular")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            multiplier = augmented[row][column]
            augmented[row] = [
                value - multiplier * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column])
            ]
    return tuple(augmented[index][-1] for index in range(size))


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
