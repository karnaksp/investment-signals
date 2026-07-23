"""Causal domain primitives for preregistered prospective candle models.

The versions in this module are intentionally separate from the first candle
research package.  A new scientific formula is a new immutable version; old
H7V2 and H15 artifacts therefore remain reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
from math import ceil, exp, isfinite, log
from typing import Iterable


class ProspectiveHypothesis(str, Enum):
    MORNING_LOW_VOLUME_REVERSION = "H1"
    MORNING_HIGH_VOLUME_CONTINUATION = "H2"
    JUMP_LOW_ACTIVITY_REVERSAL_V2 = "H3V2"
    JUMP_HIGH_ACTIVITY_CONTINUATION_V2 = "H4V2"
    SAME_PHASE_RETURN_RECURRENCE = "H5"
    OPEN_CLOSE_MARKET_CONTINUATION = "H6"
    RELATIVE_VOLUME_VOLATILITY_V3 = "H7V3"
    PAIR_RESIDUAL_REVERSION = "H12"
    HAR_VOLATILITY_V2 = "H15V2"
    DOWNSIDE_SEMIVARIANCE_RISK = "H16"
    VOLATILITY_JUMP_PERSISTENCE = "H17"

    @property
    def version(self) -> str:
        return {
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION: "1.0.0",
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION: "1.0.0",
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2: "2.0.0",
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2: "2.0.0",
            ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE: "1.0.0",
            ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION: "1.0.0",
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3: "3.0.0",
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION: "1.0.0",
            ProspectiveHypothesis.HAR_VOLATILITY_V2: "2.0.0",
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK: "1.0.0",
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE: "1.0.0",
        }[self]


class MetricUnit(str, Enum):
    BASIS_POINTS = "basis_points"
    BASIS_POINTS_SQUARED = "basis_points_squared"
    LOTS = "lots"
    COUNT = "count"
    RATIO = "ratio"
    DIMENSIONLESS_LOSS = "dimensionless_loss"


class TargetMetric(str, Enum):
    FORWARD_RETURN = "forward_return"
    FUTURE_REALIZED_VARIANCE = "future_realized_variance"
    FUTURE_VARIANCE_UPLIFT = "future_variance_uplift"

    @property
    def unit(self) -> MetricUnit:
        return {
            TargetMetric.FORWARD_RETURN: MetricUnit.BASIS_POINTS,
            TargetMetric.FUTURE_REALIZED_VARIANCE: (MetricUnit.BASIS_POINTS_SQUARED),
            TargetMetric.FUTURE_VARIANCE_UPLIFT: MetricUnit.RATIO,
        }[self]


class ProspectiveDecision(str, Enum):
    MATCHED = "matched"
    NOT_MATCHED = "not_matched"
    ABSTAIN = "abstain"


class ProspectiveReason(str, Enum):
    CONDITIONS_MATCHED = "conditions_matched"
    CONDITIONS_NOT_MET = "conditions_not_met"
    INSUFFICIENT_PRIOR_DAYS = "insufficient_prior_days"
    NON_CONTIGUOUS_WINDOW = "non_contiguous_window"
    INVALID_BASELINE = "invalid_baseline"
    DIRECTION_UNAVAILABLE = "direction_unavailable"
    MODEL_NOT_TRAINED = "model_not_trained"
    OUTCOME_UNAVAILABLE = "outcome_unavailable"
    MARKET_WIDE_MOVE_SAME_DIRECTION = "market_wide_move_same_direction"
    BASKET_COVERAGE_BELOW_MINIMUM = "basket_coverage_below_minimum"
    PAIR_MODEL_UNAVAILABLE = "pair_model_unavailable"
    PAIR_RELATIONSHIP_UNSTABLE = "pair_relationship_unstable"
    CORPORATE_ACTION_SUSPECTED = "corporate_action_suspected"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"


@dataclass(frozen=True, slots=True)
class ProspectiveScientificPolicy:
    """Sealed thresholds for the preregistered prospective model versions."""

    version: str = "prospective-scientific-models-v1.0.0"
    morning_history_days: int = 40
    morning_deviation_z_min: float = 2.0
    morning_low_relative_volume_max: float = 0.50
    morning_high_relative_volume_min: float = 1.50
    morning_range_percentile_min: float = 0.90
    morning_market_move_bps_min: float = 5.0
    morning_reversion_horizons_seconds: tuple[int, ...] = (1800, 3600)
    morning_continuation_horizons_seconds: tuple[int, ...] = (900, 1800, 3600)
    jump_window_minutes: int = 5
    jump_history_days: int = 40
    jump_percentile: float = 0.99
    jump_low_volume_percentile: float = 0.50
    jump_high_volume_percentile: float = 0.90
    jump_high_range_percentile: float = 0.90
    jump_high_illiquidity_percentile: float = 0.90
    jump_horizons_seconds: tuple[int, ...] = (300, 900)
    volume_window_minutes: int = 15
    volume_history_days: int = 40
    volume_percentile: float = 0.90
    volume_horizon_seconds: int = 1800
    phase_recurrence_history_days: int = 20
    phase_recurrence_horizon_seconds: int = 1800
    open_close_basket_return_bps_min: float = 5.0
    open_close_basket_coverage_min: float = 0.80
    open_close_horizon_seconds: int = 1800
    pair_entry_z: float = 2.0
    pair_min_correlation: float = 0.70
    pair_min_training_points: int = 500
    pair_horizons_seconds: tuple[int, ...] = (900, 1800, 3600)
    har_windows_minutes: tuple[int, int, int] = (5, 30, 120)
    har_horizon_seconds: int = 1800
    har_minimum_training_points: int = 1000
    har_ridge_penalty: float = 1e-8
    har_ewma_alpha: float = 0.10
    semivariance_window_minutes: int = 30
    semivariance_history_days: int = 40
    semivariance_percentile: float = 0.90
    semivariance_horizon_seconds: int = 1800
    jump_variance_window_minutes: int = 30
    jump_variance_history_days: int = 60
    jump_variance_percentile: float = 0.95
    jump_variance_horizon_seconds: int = 1800
    round_trip_cost_bps: float = 10.0

    def required_history_trading_days_by_hypothesis(
        self,
    ) -> tuple[tuple[ProspectiveHypothesis, int], ...]:
        """Return the sealed history budget behind every prospective formula.

        HAR samples every half-hour.  Eighteen usable anchors per complete
        trading day is a conservative floor for the main MOEX session after
        excluding anchors whose forward target crosses the session boundary.
        Pair residuals use minute observations; 360 usable minutes is the
        corresponding conservative floor.
        """

        har_days = ceil(self.har_minimum_training_points / 18)
        pair_days = ceil(self.pair_min_training_points / 360)
        requirements = {
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION: (
                self.morning_history_days
            ),
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION: (
                self.morning_history_days
            ),
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2: (
                self.jump_history_days
            ),
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2: (
                self.jump_history_days
            ),
            ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE: (
                self.phase_recurrence_history_days
            ),
            ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION: 1,
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3: (
                self.volume_history_days
            ),
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION: pair_days,
            ProspectiveHypothesis.HAR_VOLATILITY_V2: har_days,
            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK: (
                self.semivariance_history_days
            ),
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE: (
                self.jump_variance_history_days
            ),
        }
        return tuple(sorted(requirements.items(), key=lambda item: item[0].value))

    @property
    def required_history_trading_days(self) -> int:
        return max(
            days for _, days in self.required_history_trading_days_by_hypothesis()
        )

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        positive = (
            self.morning_history_days,
            *self.morning_reversion_horizons_seconds,
            *self.morning_continuation_horizons_seconds,
            self.jump_window_minutes,
            self.jump_history_days,
            *self.jump_horizons_seconds,
            self.volume_window_minutes,
            self.volume_history_days,
            self.volume_horizon_seconds,
            self.phase_recurrence_history_days,
            self.phase_recurrence_horizon_seconds,
            self.open_close_horizon_seconds,
            self.pair_min_training_points,
            *self.pair_horizons_seconds,
            *self.har_windows_minutes,
            self.har_horizon_seconds,
            self.har_minimum_training_points,
            self.semivariance_window_minutes,
            self.semivariance_history_days,
            self.semivariance_horizon_seconds,
            self.jump_variance_window_minutes,
            self.jump_variance_history_days,
            self.jump_variance_horizon_seconds,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("policy windows, histories, and horizons must be positive")
        probabilities = (
            self.morning_low_relative_volume_max,
            self.morning_range_percentile_min,
            self.open_close_basket_coverage_min,
            self.pair_min_correlation,
            self.jump_percentile,
            self.jump_low_volume_percentile,
            self.jump_high_volume_percentile,
            self.jump_high_range_percentile,
            self.jump_high_illiquidity_percentile,
            self.volume_percentile,
            self.semivariance_percentile,
            self.jump_variance_percentile,
            self.har_ewma_alpha,
        )
        if any(value <= 0.0 or value >= 1.0 for value in probabilities):
            raise ValueError("policy percentiles and EWMA alpha must be in (0, 1)")
        if self.jump_low_volume_percentile >= self.jump_high_volume_percentile:
            raise ValueError("jump activity regimes must not overlap")
        if self.morning_low_relative_volume_max >= self.morning_high_relative_volume_min:
            raise ValueError("morning activity regimes must not overlap")
        if self.morning_deviation_z_min <= 0.0 or self.morning_market_move_bps_min < 0.0:
            raise ValueError("morning thresholds must be non-negative")
        if self.open_close_basket_return_bps_min <= 0.0 or self.pair_entry_z <= 0.0:
            raise ValueError("basket and pair thresholds must be positive")
        if tuple(sorted(self.har_windows_minutes)) != self.har_windows_minutes:
            raise ValueError("HAR windows must be strictly ordered")
        if len(set(self.har_windows_minutes)) != 3:
            raise ValueError("HAR requires three unique windows")
        if self.har_ridge_penalty < 0.0 or self.round_trip_cost_bps < 0.0:
            raise ValueError("ridge penalty and trading costs must be non-negative")


@dataclass(frozen=True, slots=True)
class MetricValue:
    name: str
    unit: MetricUnit
    value: float

    def __post_init__(self) -> None:
        if not self.name.strip() or not isfinite(self.value):
            raise ValueError("metric values must be named and finite")


@dataclass(frozen=True, slots=True)
class ProspectiveFeature:
    observation_id: str
    hypothesis: ProspectiveHypothesis
    ticker: str
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    history_observed_until: datetime | None
    model_trained_until: datetime | None
    horizon_seconds: int
    target: TargetMetric
    decision: ProspectiveDecision
    reason: ProspectiveReason
    expected_direction: int
    forecast: MetricValue | None
    feature_values: tuple[MetricValue, ...]

    def __post_init__(self) -> None:
        if not self.observation_id.startswith("sha256:") or not self.ticker.strip():
            raise ValueError("feature identity is invalid")
        _require_aware(self.observed_at, "observed_at")
        _require_aware(self.feature_max_observed_at, "feature_max_observed_at")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("feature uses future market data")
        for name, boundary in (
            ("history_observed_until", self.history_observed_until),
            ("model_trained_until", self.model_trained_until),
        ):
            if boundary is None:
                continue
            _require_aware(boundary, name)
            if boundary >= self.observed_at:
                raise ValueError(f"{name} must precede the observation")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.expected_direction not in {-1, 0, 1}:
            raise ValueError("expected_direction must be -1, 0, or 1")
        directional = self.target is TargetMetric.FORWARD_RETURN
        if not directional and self.expected_direction != 0:
            raise ValueError("only directional-return features may carry direction")
        if (
            directional
            and self.decision is ProspectiveDecision.MATCHED
            and self.expected_direction == 0
        ):
            raise ValueError("a matched directional feature requires a direction")
        names = tuple(item.name for item in self.feature_values)
        if len(names) != len(set(names)):
            raise ValueError("feature metric names must be unique")
        if self.forecast is not None and self.forecast.unit is not self.target.unit:
            raise ValueError("forecast unit must match target unit")

    @property
    def hypothesis_version(self) -> str:
        return self.hypothesis.version

    @property
    def target_unit(self) -> MetricUnit:
        return self.target.unit

    def value(self, name: str) -> float:
        try:
            return next(item.value for item in self.feature_values if item.name == name)
        except StopIteration as exc:
            raise KeyError(name) from exc


@dataclass(frozen=True, slots=True)
class ProspectiveOutcome:
    observation_id: str
    target_at: datetime
    available: bool
    reason: ProspectiveReason
    target: TargetMetric
    measurements: tuple[MetricValue, ...]

    def __post_init__(self) -> None:
        _require_aware(self.target_at, "target_at")
        names = tuple(item.name for item in self.measurements)
        if len(names) != len(set(names)):
            raise ValueError("outcome metric names must be unique")
        if self.available and not self.measurements:
            raise ValueError("available outcome requires measurements")
        if not self.available and self.measurements:
            raise ValueError("unavailable outcome must not carry measurements")

    def metric(self, name: str) -> MetricValue:
        try:
            return next(item for item in self.measurements if item.name == name)
        except StopIteration as exc:
            raise KeyError(name) from exc


@dataclass(frozen=True, slots=True)
class JumpHistoryPoint:
    absolute_return_bps: float
    volume: float
    range_bps: float
    illiquidity: float

    def __post_init__(self) -> None:
        values = (
            self.absolute_return_bps,
            self.volume,
            self.range_bps,
            self.illiquidity,
        )
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("jump history values must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class HarV2TrainingPoint:
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
            raise ValueError("HAR target must follow its feature")
        values = (
            self.short_variance,
            self.medium_variance,
            self.long_variance,
            self.target_variance,
        )
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("HAR variances must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class HarV2Parameters:
    intercept: float
    short_weight: float
    medium_weight: float
    long_weight: float
    training_points: int
    trained_until: datetime

    def __post_init__(self) -> None:
        _require_aware(self.trained_until, "trained_until")
        coefficients = (
            self.intercept,
            self.short_weight,
            self.medium_weight,
            self.long_weight,
        )
        if any(not isfinite(value) for value in coefficients):
            raise ValueError("HAR coefficients must be finite")
        if self.training_points <= 0:
            raise ValueError("HAR training_points must be positive")

    def predict(self, short: float, medium: float, long: float) -> float:
        values = (short, medium, long)
        if any(not isfinite(value) or value < 0.0 for value in values):
            raise ValueError("HAR input variances must be finite and non-negative")
        log_forecast = (
            self.intercept
            + self.short_weight * log(1.0 + short)
            + self.medium_weight * log(1.0 + medium)
            + self.long_weight * log(1.0 + long)
        )
        return max(exp(log_forecast) - 1.0, 1e-12)


@dataclass(frozen=True, slots=True)
class FrozenPairParameters:
    """Pair relation estimated only on a sealed earlier training period."""

    left_ticker: str
    right_ticker: str
    intercept: float
    hedge_ratio: float
    spread_mean: float
    spread_std: float
    correlation: float
    training_points: int
    trained_until: datetime

    def __post_init__(self) -> None:
        if not self.left_ticker.strip() or not self.right_ticker.strip():
            raise ValueError("pair tickers are required")
        if self.left_ticker == self.right_ticker:
            raise ValueError("pair tickers must differ")
        _require_aware(self.trained_until, "trained_until")
        values = (
            self.intercept,
            self.hedge_ratio,
            self.spread_mean,
            self.spread_std,
            self.correlation,
        )
        if any(not isfinite(value) for value in values):
            raise ValueError("pair parameters must be finite")
        if self.spread_std <= 0.0 or self.training_points <= 0:
            raise ValueError("pair model requires positive dispersion and sample size")
        if not -1.0 <= self.correlation <= 1.0:
            raise ValueError("pair correlation must be in [-1, 1]")

    @property
    def pair_id(self) -> str:
        return f"{self.left_ticker}/{self.right_ticker}"

    def spread(self, left_price: float, right_price: float) -> float:
        if left_price <= 0.0 or right_price <= 0.0:
            raise ValueError("pair prices must be positive")
        return log(left_price) - self.intercept - self.hedge_ratio * log(right_price)


def fit_har_v2_parameters(
    points: Iterable[HarV2TrainingPoint],
    *,
    minimum_points: int,
    ridge_penalty: float,
) -> HarV2Parameters:
    training = tuple(sorted(points, key=lambda item: (item.feature_at, item.target_at)))
    if len(training) < minimum_points:
        raise ValueError("insufficient HAR V2 training points")
    if ridge_penalty < 0.0:
        raise ValueError("ridge_penalty must be non-negative")
    size = 4
    matrix = [[0.0] * size for _ in range(size)]
    target = [0.0] * size
    for point in training:
        row = (
            1.0,
            log(1.0 + point.short_variance),
            log(1.0 + point.medium_variance),
            log(1.0 + point.long_variance),
        )
        value = log(1.0 + point.target_variance)
        for left in range(size):
            target[left] += row[left] * value
            for right in range(size):
                matrix[left][right] += row[left] * row[right]
    for index in range(1, size):
        matrix[index][index] += ridge_penalty
    intercept, short, medium, long = _solve(matrix, target)
    return HarV2Parameters(
        intercept=intercept,
        short_weight=short,
        medium_weight=medium,
        long_weight=long,
        training_points=len(training),
        trained_until=max(point.target_at for point in training),
    )


def jump_regime_features(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
    signed_return_bps: float,
    volume: float,
    range_bps: float,
    illiquidity: float,
    prior_history: Iterable[JumpHistoryPoint],
    history_observed_until: datetime | None,
    trading_gap: bool,
    policy: ProspectiveScientificPolicy,
) -> tuple[ProspectiveFeature, ProspectiveFeature]:
    history = tuple(prior_history)
    values = (signed_return_bps, volume, range_bps, illiquidity)
    if (
        any(not isfinite(value) for value in values)
        or min(volume, range_bps, illiquidity) < 0.0
    ):
        raise ValueError("jump feature values are invalid")
    sufficient = len(history) == policy.jump_history_days
    absolute_percentile = _percentile(
        (item.absolute_return_bps for item in history), abs(signed_return_bps)
    )
    volume_percentile = _percentile((item.volume for item in history), volume)
    range_percentile = _percentile((item.range_bps for item in history), range_bps)
    illiquidity_percentile = _percentile(
        (item.illiquidity for item in history), illiquidity
    )
    jump = sufficient and absolute_percentile >= policy.jump_percentile
    low_activity = (
        jump
        and volume_percentile < policy.jump_low_volume_percentile
        and illiquidity_percentile >= policy.jump_high_illiquidity_percentile
    )
    high_activity = (
        jump
        and volume_percentile >= policy.jump_high_volume_percentile
        and range_percentile >= policy.jump_high_range_percentile
    )
    if low_activity and high_activity:
        raise AssertionError(
            "H3V2 and H4V2 activity regimes must be mutually exclusive"
        )
    direction = 1 if signed_return_bps > 0.0 else -1 if signed_return_bps < 0.0 else 0
    common = (
        MetricValue("signed_return", MetricUnit.BASIS_POINTS, signed_return_bps),
        MetricValue(
            "absolute_return_percentile", MetricUnit.RATIO, absolute_percentile
        ),
        MetricValue("volume_percentile", MetricUnit.RATIO, volume_percentile),
        MetricValue("range_percentile", MetricUnit.RATIO, range_percentile),
        MetricValue("illiquidity_percentile", MetricUnit.RATIO, illiquidity_percentile),
        MetricValue("prior_day_count", MetricUnit.COUNT, float(len(history))),
    )
    return (
        _classified_directional_feature(
            hypothesis=ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            matched=low_activity,
            direction=-direction,
            ticker=ticker,
            trading_day=trading_day,
            observed_at=observed_at,
            horizon_seconds=horizon_seconds,
            history_observed_until=history_observed_until,
            trading_gap=trading_gap,
            sufficient=sufficient,
            common=common,
        ),
        _classified_directional_feature(
            hypothesis=ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            matched=high_activity,
            direction=direction,
            ticker=ticker,
            trading_day=trading_day,
            observed_at=observed_at,
            horizon_seconds=horizon_seconds,
            history_observed_until=history_observed_until,
            trading_gap=trading_gap,
            sufficient=sufficient,
            common=common,
        ),
    )


def morning_regime_features(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    feature_max_observed_at: datetime,
    horizon_seconds: int,
    morning_deviation_bps: float,
    morning_deviation_z: float,
    cumulative_relative_volume: float,
    morning_range_percentile: float,
    market_return_bps: float,
    market_coverage: float,
    history_count: int,
    history_observed_until: datetime | None,
    trading_gap: bool,
    valid_baseline: bool,
    policy: ProspectiveScientificPolicy,
) -> tuple[ProspectiveFeature, ProspectiveFeature]:
    """Classify mutually exclusive morning reversion and continuation regimes."""

    numeric = (
        morning_deviation_bps,
        morning_deviation_z,
        cumulative_relative_volume,
        morning_range_percentile,
        market_return_bps,
        market_coverage,
    )
    if any(not isfinite(value) for value in numeric):
        raise ValueError("morning features must be finite")
    if cumulative_relative_volume < 0.0:
        raise ValueError("morning relative volume must be non-negative")
    if not 0.0 <= morning_range_percentile <= 1.0:
        raise ValueError("morning range percentile must be in [0, 1]")
    if not 0.0 <= market_coverage <= 1.0:
        raise ValueError("market coverage must be in [0, 1]")
    if feature_max_observed_at > observed_at:
        raise ValueError("morning feature uses future market data")
    direction = _direction(morning_deviation_bps)
    sufficient = history_count == policy.morning_history_days
    extreme = abs(morning_deviation_z) >= policy.morning_deviation_z_min
    market_same_direction = (
        direction != 0
        and _direction(market_return_bps) == direction
        and abs(market_return_bps) >= policy.morning_market_move_bps_min
    )
    reversion = (
        extreme
        and cumulative_relative_volume <= policy.morning_low_relative_volume_max
        and not market_same_direction
    )
    continuation = (
        extreme
        and cumulative_relative_volume >= policy.morning_high_relative_volume_min
        and morning_range_percentile >= policy.morning_range_percentile_min
    )
    if reversion and continuation:
        raise AssertionError("H1 and H2 morning regimes must be mutually exclusive")
    common = (
        MetricValue("morning_deviation_bps", MetricUnit.BASIS_POINTS, morning_deviation_bps),
        MetricValue("morning_deviation_z", MetricUnit.RATIO, morning_deviation_z),
        MetricValue("cumulative_relative_volume", MetricUnit.RATIO, cumulative_relative_volume),
        MetricValue("morning_range_percentile", MetricUnit.RATIO, morning_range_percentile),
        MetricValue("market_return_bps", MetricUnit.BASIS_POINTS, market_return_bps),
        MetricValue("market_coverage", MetricUnit.RATIO, market_coverage),
        MetricValue("prior_day_count", MetricUnit.COUNT, float(history_count)),
    )
    reason_override = (
        ProspectiveReason.MARKET_WIDE_MOVE_SAME_DIRECTION
        if market_same_direction
        else None
    )
    return (
        _classified_directional_feature(
            hypothesis=ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
            matched=reversion,
            direction=-direction,
            ticker=ticker,
            trading_day=trading_day,
            observed_at=observed_at,
            horizon_seconds=horizon_seconds,
            history_observed_until=history_observed_until,
            trading_gap=trading_gap,
            sufficient=sufficient,
            common=common,
            feature_max_observed_at=feature_max_observed_at,
            valid_baseline=valid_baseline and market_coverage >= policy.open_close_basket_coverage_min,
            reason_override=reason_override,
        ),
        _classified_directional_feature(
            hypothesis=ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
            matched=continuation,
            direction=direction,
            ticker=ticker,
            trading_day=trading_day,
            observed_at=observed_at,
            horizon_seconds=horizon_seconds,
            history_observed_until=history_observed_until,
            trading_gap=trading_gap,
            sufficient=sufficient,
            common=common,
            feature_max_observed_at=feature_max_observed_at,
            valid_baseline=valid_baseline,
        ),
    )


def phase_recurrence_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    historical_same_phase_returns_bps: Iterable[float],
    history_observed_until: datetime | None,
    trading_gap: bool,
    policy: ProspectiveScientificPolicy,
) -> ProspectiveFeature:
    history = tuple(historical_same_phase_returns_bps)
    if any(not isfinite(value) for value in history):
        raise ValueError("same-phase returns must be finite")
    mean_return = sum(history) / len(history) if history else 0.0
    direction = _direction(mean_return)
    sufficient = len(history) == policy.phase_recurrence_history_days
    return _classified_directional_feature(
        hypothesis=ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE,
        matched=sufficient and direction != 0,
        direction=direction,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        horizon_seconds=policy.phase_recurrence_horizon_seconds,
        history_observed_until=history_observed_until,
        trading_gap=trading_gap,
        sufficient=sufficient,
        common=(
            MetricValue("same_phase_mean_return_bps", MetricUnit.BASIS_POINTS, mean_return),
            MetricValue("prior_day_count", MetricUnit.COUNT, float(len(history))),
        ),
    )


def open_close_basket_feature(
    *,
    trading_day: date,
    observed_at: datetime,
    feature_max_observed_at: datetime,
    opening_basket_return_bps: float,
    basket_coverage: float,
    shortened_session: bool,
    policy: ProspectiveScientificPolicy,
) -> ProspectiveFeature:
    if not isfinite(opening_basket_return_bps):
        raise ValueError("opening basket return must be finite")
    if not 0.0 <= basket_coverage <= 1.0:
        raise ValueError("basket coverage must be in [0, 1]")
    direction = _direction(opening_basket_return_bps)
    if shortened_session:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.NON_CONTIGUOUS_WINDOW,
        )
    elif basket_coverage < policy.open_close_basket_coverage_min:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.BASKET_COVERAGE_BELOW_MINIMUM,
        )
    elif direction == 0:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.DIRECTION_UNAVAILABLE,
        )
    elif abs(opening_basket_return_bps) >= policy.open_close_basket_return_bps_min:
        decision, reason = (
            ProspectiveDecision.MATCHED,
            ProspectiveReason.CONDITIONS_MATCHED,
        )
    else:
        decision, reason = (
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveReason.CONDITIONS_NOT_MET,
        )
    return _feature(
        hypothesis=ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION,
        ticker="MOEX_FIXED_BASKET",
        trading_day=trading_day,
        observed_at=observed_at,
        feature_max_observed_at=feature_max_observed_at,
        horizon_seconds=policy.open_close_horizon_seconds,
        target=TargetMetric.FORWARD_RETURN,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        forecast=None,
        feature_values=(
            MetricValue("opening_basket_return_bps", MetricUnit.BASIS_POINTS, opening_basket_return_bps),
            MetricValue("basket_coverage", MetricUnit.RATIO, basket_coverage),
        ),
    )


def pair_residual_reversion_feature(
    *,
    left_ticker: str,
    right_ticker: str,
    trading_day: date,
    observed_at: datetime,
    left_price: float,
    right_price: float,
    parameters: FrozenPairParameters | None,
    corporate_action_suspected: bool,
    liquid: bool,
    policy: ProspectiveScientificPolicy,
    horizon_seconds: int,
) -> ProspectiveFeature:
    if not left_ticker.strip() or not right_ticker.strip() or left_ticker == right_ticker:
        raise ValueError("a pair requires two distinct tickers")
    pair_id = f"{left_ticker}/{right_ticker}"
    if parameters is None:
        residual = residual_z = correlation = 0.0
        decision, reason, direction = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.PAIR_MODEL_UNAVAILABLE,
            0,
        )
        trained_until = None
    else:
        if (
            parameters.left_ticker != left_ticker
            or parameters.right_ticker != right_ticker
        ):
            raise ValueError("pair parameters belong to a different pair")
        if parameters.trained_until >= observed_at:
            raise ValueError("pair parameters must use completed earlier data only")
        residual = parameters.spread(left_price, right_price) - parameters.spread_mean
        residual_z = residual / parameters.spread_std
        correlation = parameters.correlation
        direction = -_direction(residual)
        trained_until = parameters.trained_until
        if corporate_action_suspected:
            decision, reason = (
                ProspectiveDecision.ABSTAIN,
                ProspectiveReason.CORPORATE_ACTION_SUSPECTED,
            )
        elif not liquid:
            decision, reason = (
                ProspectiveDecision.ABSTAIN,
                ProspectiveReason.INSUFFICIENT_LIQUIDITY,
            )
        elif (
            parameters.training_points < policy.pair_min_training_points
            or correlation < policy.pair_min_correlation
        ):
            decision, reason = (
                ProspectiveDecision.ABSTAIN,
                ProspectiveReason.PAIR_RELATIONSHIP_UNSTABLE,
            )
        elif direction == 0:
            decision, reason = (
                ProspectiveDecision.ABSTAIN,
                ProspectiveReason.DIRECTION_UNAVAILABLE,
            )
        elif abs(residual_z) >= policy.pair_entry_z:
            decision, reason = (
                ProspectiveDecision.MATCHED,
                ProspectiveReason.CONDITIONS_MATCHED,
            )
        else:
            decision, reason = (
                ProspectiveDecision.NOT_MATCHED,
                ProspectiveReason.CONDITIONS_NOT_MET,
            )
    return _feature(
        hypothesis=ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
        ticker=pair_id,
        trading_day=trading_day,
        observed_at=observed_at,
        model_trained_until=trained_until,
        horizon_seconds=horizon_seconds,
        target=TargetMetric.FORWARD_RETURN,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        forecast=None,
        feature_values=(
            MetricValue("pair_residual_log", MetricUnit.RATIO, residual),
            MetricValue("pair_residual_z", MetricUnit.RATIO, residual_z),
            MetricValue("pair_correlation", MetricUnit.RATIO, correlation),
            MetricValue(
                "pair_training_points",
                MetricUnit.COUNT,
                float(parameters.training_points if parameters else 0),
            ),
        ),
    )


def relative_volume_volatility_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    current_volume: float,
    historical_volumes: Iterable[float],
    baseline_future_variance: float,
    history_observed_until: datetime | None,
    trading_gap: bool,
    policy: ProspectiveScientificPolicy,
) -> ProspectiveFeature:
    history = tuple(historical_volumes)
    if current_volume < 0.0 or any(value < 0.0 for value in history):
        raise ValueError("volume values must be non-negative")
    percentile = _percentile(history, current_volume)
    decision, reason = _non_directional_decision(
        history_count=len(history),
        required_history=policy.volume_history_days,
        matched=percentile >= policy.volume_percentile,
        baseline=baseline_future_variance,
        trading_gap=trading_gap,
    )
    forecast = (
        MetricValue("minimum_variance_uplift", MetricUnit.RATIO, 0.0)
        if decision is ProspectiveDecision.MATCHED
        else None
    )
    return _feature(
        hypothesis=ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        history_observed_until=history_observed_until,
        horizon_seconds=policy.volume_horizon_seconds,
        target=TargetMetric.FUTURE_VARIANCE_UPLIFT,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast=forecast,
        feature_values=(
            MetricValue("current_window_volume", MetricUnit.LOTS, current_volume),
            MetricValue("phase_volume_percentile", MetricUnit.RATIO, percentile),
            MetricValue("prior_day_count", MetricUnit.COUNT, float(len(history))),
            MetricValue(
                "baseline_future_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                max(0.0, baseline_future_variance),
            ),
        ),
    )


def har_v2_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    short_variance: float,
    medium_variance: float,
    long_variance: float,
    parameters: HarV2Parameters | None,
    horizon_seconds: int,
) -> ProspectiveFeature:
    if parameters is None:
        decision, reason, forecast = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.MODEL_NOT_TRAINED,
            None,
        )
    else:
        decision, reason = (
            ProspectiveDecision.MATCHED,
            ProspectiveReason.CONDITIONS_MATCHED,
        )
        forecast = MetricValue(
            "har_forecast_variance",
            MetricUnit.BASIS_POINTS_SQUARED,
            parameters.predict(short_variance, medium_variance, long_variance),
        )
    return _feature(
        hypothesis=ProspectiveHypothesis.HAR_VOLATILITY_V2,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        model_trained_until=parameters.trained_until if parameters else None,
        horizon_seconds=horizon_seconds,
        target=TargetMetric.FUTURE_REALIZED_VARIANCE,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast=forecast,
        feature_values=(
            MetricValue(
                "short_realized_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                short_variance,
            ),
            MetricValue(
                "medium_realized_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                medium_variance,
            ),
            MetricValue(
                "long_realized_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                long_variance,
            ),
        ),
    )


def downside_semivariance_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    downside_share: float,
    historical_downside_shares: Iterable[float],
    baseline_future_variance: float,
    history_observed_until: datetime | None,
    trading_gap: bool,
    policy: ProspectiveScientificPolicy,
) -> ProspectiveFeature:
    history = tuple(historical_downside_shares)
    if not 0.0 <= downside_share <= 1.0 or any(
        not 0.0 <= value <= 1.0 for value in history
    ):
        raise ValueError("downside shares must be in [0, 1]")
    percentile = _percentile(history, downside_share)
    decision, reason = _non_directional_decision(
        history_count=len(history),
        required_history=policy.semivariance_history_days,
        matched=percentile >= policy.semivariance_percentile,
        baseline=baseline_future_variance,
        trading_gap=trading_gap,
    )
    return _feature(
        hypothesis=ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        history_observed_until=history_observed_until,
        horizon_seconds=policy.semivariance_horizon_seconds,
        target=TargetMetric.FUTURE_VARIANCE_UPLIFT,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast=(
            MetricValue("minimum_variance_uplift", MetricUnit.RATIO, 0.0)
            if decision is ProspectiveDecision.MATCHED
            else None
        ),
        feature_values=(
            MetricValue(
                "downside_semivariance_share", MetricUnit.RATIO, downside_share
            ),
            MetricValue("downside_share_percentile", MetricUnit.RATIO, percentile),
            MetricValue("prior_day_count", MetricUnit.COUNT, float(len(history))),
            MetricValue(
                "baseline_future_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                max(0.0, baseline_future_variance),
            ),
        ),
    )


def volatility_jump_feature(
    *,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    jump_share: float,
    continuous_variance: float,
    historical_jump_shares: Iterable[float],
    baseline_future_variance: float,
    history_observed_until: datetime | None,
    trading_gap: bool,
    policy: ProspectiveScientificPolicy,
) -> ProspectiveFeature:
    history = tuple(historical_jump_shares)
    if not 0.0 <= jump_share <= 1.0 or any(
        not 0.0 <= value <= 1.0 for value in history
    ):
        raise ValueError("jump shares must be in [0, 1]")
    if continuous_variance < 0.0 or not isfinite(continuous_variance):
        raise ValueError("continuous_variance must be finite and non-negative")
    percentile = _percentile(history, jump_share)
    decision, reason = _non_directional_decision(
        history_count=len(history),
        required_history=policy.jump_variance_history_days,
        matched=percentile >= policy.jump_variance_percentile,
        baseline=baseline_future_variance,
        trading_gap=trading_gap,
    )
    return _feature(
        hypothesis=ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        history_observed_until=history_observed_until,
        horizon_seconds=policy.jump_variance_horizon_seconds,
        target=TargetMetric.FUTURE_VARIANCE_UPLIFT,
        decision=decision,
        reason=reason,
        expected_direction=0,
        forecast=(
            MetricValue("minimum_variance_uplift", MetricUnit.RATIO, 0.0)
            if decision is ProspectiveDecision.MATCHED
            else None
        ),
        feature_values=(
            MetricValue("jump_variance_share", MetricUnit.RATIO, jump_share),
            MetricValue("jump_share_percentile", MetricUnit.RATIO, percentile),
            MetricValue(
                "continuous_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                continuous_variance,
            ),
            MetricValue("prior_day_count", MetricUnit.COUNT, float(len(history))),
            MetricValue(
                "baseline_future_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                max(0.0, baseline_future_variance),
            ),
        ),
    )


def directional_outcome(
    feature: ProspectiveFeature,
    *,
    target_at: datetime,
    forward_return_bps: float | None,
    round_trip_cost_bps: float,
) -> ProspectiveOutcome:
    if feature.target is not TargetMetric.FORWARD_RETURN:
        raise ValueError("directional outcome requires a forward-return feature")
    if forward_return_bps is None or feature.decision is ProspectiveDecision.ABSTAIN:
        return _unavailable_outcome(feature, target_at)
    measurements = [
        MetricValue("forward_return", MetricUnit.BASIS_POINTS, forward_return_bps)
    ]
    if feature.decision is ProspectiveDecision.MATCHED:
        measurements.append(
            MetricValue(
                "cost_adjusted_directional_return",
                MetricUnit.BASIS_POINTS,
                feature.expected_direction * forward_return_bps - round_trip_cost_bps,
            )
        )
    return ProspectiveOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=True,
        reason=feature.reason,
        target=feature.target,
        measurements=tuple(measurements),
    )


def variance_uplift_outcome(
    feature: ProspectiveFeature,
    *,
    target_at: datetime,
    actual_future_variance: float | None,
) -> ProspectiveOutcome:
    if feature.target is not TargetMetric.FUTURE_VARIANCE_UPLIFT:
        raise ValueError("variance uplift outcome requires a ratio target")
    if (
        actual_future_variance is None
        or feature.decision is ProspectiveDecision.ABSTAIN
    ):
        return _unavailable_outcome(feature, target_at)
    baseline = feature.value("baseline_future_variance")
    if baseline <= 0.0:
        return _unavailable_outcome(feature, target_at)
    return ProspectiveOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=True,
        reason=feature.reason,
        target=feature.target,
        measurements=(
            MetricValue(
                "future_realized_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                actual_future_variance,
            ),
            MetricValue(
                "future_variance_uplift",
                MetricUnit.RATIO,
                actual_future_variance / baseline - 1.0,
            ),
        ),
    )


def har_v2_outcome(
    feature: ProspectiveFeature,
    *,
    target_at: datetime,
    actual_future_variance: float | None,
    ewma_baseline: float | None,
    phase_baseline: float | None,
) -> ProspectiveOutcome:
    if feature.target is not TargetMetric.FUTURE_REALIZED_VARIANCE:
        raise ValueError("HAR outcome requires a variance target")
    forecast = feature.forecast
    if (
        feature.decision is ProspectiveDecision.ABSTAIN
        or forecast is None
        or actual_future_variance is None
        or ewma_baseline is None
        or phase_baseline is None
        or ewma_baseline <= 0.0
        or phase_baseline <= 0.0
    ):
        return _unavailable_outcome(feature, target_at)
    actual = max(actual_future_variance, 1e-12)
    model = max(forecast.value, 1e-12)
    ewma = max(ewma_baseline, 1e-12)
    phase = max(phase_baseline, 1e-12)
    return ProspectiveOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=True,
        reason=feature.reason,
        target=feature.target,
        measurements=(
            MetricValue(
                "future_realized_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                actual_future_variance,
            ),
            MetricValue(
                "har_forecast_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                forecast.value,
            ),
            MetricValue(
                "ewma_forecast_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                ewma_baseline,
            ),
            MetricValue(
                "phase_forecast_variance",
                MetricUnit.BASIS_POINTS_SQUARED,
                phase_baseline,
            ),
            MetricValue(
                "har_qlike", MetricUnit.DIMENSIONLESS_LOSS, qlike(actual, model)
            ),
            MetricValue(
                "ewma_qlike", MetricUnit.DIMENSIONLESS_LOSS, qlike(actual, ewma)
            ),
            MetricValue(
                "phase_qlike", MetricUnit.DIMENSIONLESS_LOSS, qlike(actual, phase)
            ),
            MetricValue(
                "har_absolute_error",
                MetricUnit.BASIS_POINTS_SQUARED,
                abs(actual - model),
            ),
            MetricValue(
                "ewma_absolute_error",
                MetricUnit.BASIS_POINTS_SQUARED,
                abs(actual - ewma),
            ),
            MetricValue(
                "phase_absolute_error",
                MetricUnit.BASIS_POINTS_SQUARED,
                abs(actual - phase),
            ),
        ),
    )


def qlike(actual_variance: float, forecast_variance: float) -> float:
    if actual_variance <= 0.0 or forecast_variance <= 0.0:
        raise ValueError("QLIKE requires positive variances")
    ratio = actual_variance / forecast_variance
    return ratio - log(ratio) - 1.0


def _classified_directional_feature(
    *,
    hypothesis: ProspectiveHypothesis,
    matched: bool,
    direction: int,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
    history_observed_until: datetime | None,
    trading_gap: bool,
    sufficient: bool,
    common: tuple[MetricValue, ...],
    feature_max_observed_at: datetime | None = None,
    valid_baseline: bool = True,
    reason_override: ProspectiveReason | None = None,
) -> ProspectiveFeature:
    if trading_gap:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.NON_CONTIGUOUS_WINDOW,
        )
    elif not sufficient:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.INSUFFICIENT_PRIOR_DAYS,
        )
    elif not valid_baseline:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.INVALID_BASELINE,
        )
    elif direction == 0:
        decision, reason = (
            ProspectiveDecision.ABSTAIN,
            ProspectiveReason.DIRECTION_UNAVAILABLE,
        )
    elif reason_override is not None:
        decision, reason = ProspectiveDecision.ABSTAIN, reason_override
    elif matched:
        decision, reason = (
            ProspectiveDecision.MATCHED,
            ProspectiveReason.CONDITIONS_MATCHED,
        )
    else:
        decision, reason = (
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveReason.CONDITIONS_NOT_MET,
        )
    return _feature(
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        feature_max_observed_at=feature_max_observed_at,
        history_observed_until=history_observed_until,
        horizon_seconds=horizon_seconds,
        target=TargetMetric.FORWARD_RETURN,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        forecast=None,
        feature_values=common,
    )


def _non_directional_decision(
    *,
    history_count: int,
    required_history: int,
    matched: bool,
    baseline: float,
    trading_gap: bool,
) -> tuple[ProspectiveDecision, ProspectiveReason]:
    if trading_gap:
        return ProspectiveDecision.ABSTAIN, ProspectiveReason.NON_CONTIGUOUS_WINDOW
    if history_count != required_history:
        return ProspectiveDecision.ABSTAIN, ProspectiveReason.INSUFFICIENT_PRIOR_DAYS
    if baseline <= 0.0:
        return ProspectiveDecision.ABSTAIN, ProspectiveReason.INVALID_BASELINE
    if matched:
        return ProspectiveDecision.MATCHED, ProspectiveReason.CONDITIONS_MATCHED
    return ProspectiveDecision.NOT_MATCHED, ProspectiveReason.CONDITIONS_NOT_MET


def _feature(
    *,
    hypothesis: ProspectiveHypothesis,
    ticker: str,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
    target: TargetMetric,
    decision: ProspectiveDecision,
    reason: ProspectiveReason,
    expected_direction: int,
    forecast: MetricValue | None,
    feature_values: tuple[MetricValue, ...],
    feature_max_observed_at: datetime | None = None,
    history_observed_until: datetime | None = None,
    model_trained_until: datetime | None = None,
) -> ProspectiveFeature:
    identity = "|".join(
        (
            hypothesis.value,
            hypothesis.version,
            ticker,
            observed_at.isoformat(),
            str(horizon_seconds),
        )
    )
    return ProspectiveFeature(
        observation_id="sha256:" + sha256(identity.encode()).hexdigest(),
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=trading_day,
        observed_at=observed_at,
        feature_max_observed_at=feature_max_observed_at or observed_at,
        history_observed_until=history_observed_until,
        model_trained_until=model_trained_until,
        horizon_seconds=horizon_seconds,
        target=target,
        decision=decision,
        reason=reason,
        expected_direction=expected_direction,
        forecast=forecast,
        feature_values=feature_values,
    )


def _unavailable_outcome(
    feature: ProspectiveFeature,
    target_at: datetime,
) -> ProspectiveOutcome:
    return ProspectiveOutcome(
        observation_id=feature.observation_id,
        target_at=target_at,
        available=False,
        reason=ProspectiveReason.OUTCOME_UNAVAILABLE,
        target=feature.target,
        measurements=(),
    )


def _direction(value: float) -> int:
    if value > 0.0:
        return 1
    if value < 0.0:
        return -1
    return 0


def _percentile(history: Iterable[float], value: float) -> float:
    values = tuple(history)
    if not values:
        return 0.0
    if any(not isfinite(item) for item in (*values, value)):
        raise ValueError("percentile values must be finite")
    return sum(item <= value for item in values) / len(values)


def _solve(matrix: list[list[float]], target: list[float]) -> tuple[float, ...]:
    size = len(target)
    augmented = [row[:] + [target[index]] for index, row in enumerate(matrix)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) <= 1e-12:
            augmented[column][column] += 1e-8
            pivot = column
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [
                left - factor * right
                for left, right in zip(augmented[row], augmented[column])
            ]
    return tuple(augmented[index][-1] for index in range(size))


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
