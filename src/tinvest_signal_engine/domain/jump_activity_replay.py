"""Domain records for causal H3/H4 candle replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from math import isfinite


class JumpHypothesis(str, Enum):
    LOW_ACTIVITY_REVERSAL = "H3"
    HIGH_ACTIVITY_CONTINUATION = "H4"


@dataclass(frozen=True)
class CandleBar:
    """One complete one-minute candle as observed at the end of its minute."""

    ticker: str
    opened_at: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    complete: bool = True

    def __post_init__(self) -> None:
        if not self.ticker.strip():
            raise ValueError("ticker must not be empty")
        if self.opened_at.tzinfo is None or self.opened_at.utcoffset() is None:
            raise ValueError("opened_at must be timezone-aware")
        if not all(
            isfinite(value)
            for value in (
                self.open_price,
                self.high_price,
                self.low_price,
                self.close_price,
                self.volume,
            )
        ):
            raise ValueError("candle values must be finite")
        if min(self.open_price, self.high_price, self.low_price, self.close_price) <= 0:
            raise ValueError("candle prices must be positive")
        if self.high_price < max(self.open_price, self.low_price, self.close_price):
            raise ValueError("high_price must contain open, low, and close")
        if self.low_price > min(self.open_price, self.high_price, self.close_price):
            raise ValueError("low_price must contain open, high, and close")
        if self.volume < 0:
            raise ValueError("volume must not be negative")

    @property
    def observed_at(self) -> datetime:
        return self.opened_at + timedelta(minutes=1)


@dataclass(frozen=True)
class CostModel:
    version: str
    round_trip_bps: float

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("cost model version must not be empty")
        if not isfinite(self.round_trip_bps) or self.round_trip_bps < 0:
            raise ValueError("round_trip_bps must be finite and non-negative")


@dataclass(frozen=True)
class JumpReplayPolicy:
    version: str = "h3-h4-replay-v1.0.0"
    history_window_minutes: int = 5
    volatility_window_minutes: int = 30
    event_cooldown_minutes: int = 5
    minimum_training_observations: int = 100
    jump_quantile: float = 0.99
    low_volume_quantile: float = 0.50
    high_activity_quantile: float = 0.90
    high_illiquidity_quantile: float = 0.90
    horizons_seconds: tuple[int, ...] = (300, 900, 1800)
    cost_model: CostModel = CostModel(
        version="research-cost-v1.0.0",
        round_trip_bps=10.0,
    )

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        if (
            self.history_window_minutes <= 0
            or self.volatility_window_minutes <= 1
            or self.event_cooldown_minutes < 0
        ):
            raise ValueError("feature windows must be positive")
        if self.minimum_training_observations <= 0:
            raise ValueError("minimum_training_observations must be positive")
        quantiles = (
            self.jump_quantile,
            self.low_volume_quantile,
            self.high_activity_quantile,
            self.high_illiquidity_quantile,
        )
        if any(value <= 0.0 or value >= 1.0 for value in quantiles):
            raise ValueError("quantiles must be strictly between zero and one")
        if self.low_volume_quantile >= self.high_activity_quantile:
            raise ValueError("low and high activity regimes must not overlap")
        if not self.horizons_seconds or any(
            horizon <= 0 or horizon % 60 for horizon in self.horizons_seconds
        ):
            raise ValueError("horizons must be positive whole minutes")
        if len(set(self.horizons_seconds)) != len(self.horizons_seconds):
            raise ValueError("horizons must be unique")


@dataclass(frozen=True)
class RawJumpFeature:
    """Feature values calculated only from candles observed by ``observed_at``."""

    feature_id: str
    ticker: str
    observed_at: datetime
    trading_day: date
    session_bucket: str
    anchor_price: float
    five_minute_return_bps: float
    absolute_return_bps: float
    five_minute_volume: float
    five_minute_range_bps: float
    illiquidity_proxy: float
    prior_volatility_bps: float
    feature_max_observed_at: datetime

    def __post_init__(self) -> None:
        if not self.feature_id.strip() or not self.ticker.strip():
            raise ValueError("feature identity must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("feature uses future market data")
        if self.anchor_price <= 0 or self.five_minute_volume < 0:
            raise ValueError("feature prices and volumes are invalid")
        if not all(
            isfinite(value)
            for value in (
                self.five_minute_return_bps,
                self.absolute_return_bps,
                self.five_minute_range_bps,
                self.illiquidity_proxy,
                self.prior_volatility_bps,
            )
        ):
            raise ValueError("feature values must be finite")

    @property
    def direction(self) -> int:
        if self.five_minute_return_bps > 0:
            return 1
        if self.five_minute_return_bps < 0:
            return -1
        return 0


@dataclass(frozen=True)
class FeatureThresholds:
    ticker: str
    session_bucket: str
    training_observations: int
    jump_absolute_return_bps: float
    median_volume: float
    high_volume: float
    high_range_bps: float
    high_illiquidity: float
    volatility_low_bps: float
    volatility_high_bps: float


@dataclass(frozen=True)
class ClassifiedJumpFeature:
    raw: RawJumpFeature
    thresholds: FeatureThresholds
    volume_ratio: float
    volume_percentile: float
    range_percentile: float
    illiquidity_percentile: float
    volatility_bucket: str
    liquidity_bucket: str
    hypothesis: JumpHypothesis | None

    def __post_init__(self) -> None:
        if self.thresholds.ticker != self.raw.ticker:
            raise ValueError("threshold ticker does not match feature ticker")
        if self.thresholds.session_bucket != self.raw.session_bucket:
            raise ValueError("threshold session does not match feature session")
        if any(
            value < 0.0 or value > 1.0
            for value in (
                self.volume_percentile,
                self.range_percentile,
                self.illiquidity_percentile,
            )
        ):
            raise ValueError("feature percentiles must be between zero and one")


@dataclass(frozen=True)
class HorizonOutcome:
    horizon_seconds: int
    target_observed_at: datetime
    available: bool
    reason_code: str
    forward_return_bps: float | None
    expected_direction: int
    net_effect_bps: float | None
    cost_model_version: str

    def __post_init__(self) -> None:
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.expected_direction not in {-1, 1}:
            raise ValueError("expected_direction must be -1 or 1")
        if self.target_observed_at.tzinfo is None:
            raise ValueError("target_observed_at must be timezone-aware")
        if self.available:
            if self.forward_return_bps is None or self.net_effect_bps is None:
                raise ValueError("available outcome requires numeric results")
            if self.reason_code != "ready":
                raise ValueError("available outcome reason must be ready")
        elif self.forward_return_bps is not None or self.net_effect_bps is not None:
            raise ValueError("unavailable outcome must not carry numeric results")


@dataclass(frozen=True)
class JumpObservation:
    observation_id: str
    hypothesis: JumpHypothesis
    feature: ClassifiedJumpFeature
    outcomes: tuple[HorizonOutcome, ...]

    def __post_init__(self) -> None:
        if self.feature.hypothesis is not self.hypothesis:
            raise ValueError("observation hypothesis must match classified feature")
        if len({outcome.horizon_seconds for outcome in self.outcomes}) != len(
            self.outcomes
        ):
            raise ValueError("outcome horizons must be unique")
