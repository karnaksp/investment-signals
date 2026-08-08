"""Production policy and forecast values for selective morning retracement."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
from math import exp, isfinite
from typing import Mapping

from tinvest_signal_engine.domain.morning_retracement import (
    MorningSnapshot,
    RetracementDirection,
)


@dataclass(frozen=True, slots=True)
class LinearProbabilityModel:
    feature_names: tuple[str, ...]
    coefficients: tuple[float, ...]
    intercept: float
    fingerprint: str

    def __post_init__(self) -> None:
        if not self.feature_names or len(self.feature_names) != len(self.coefficients):
            raise ValueError("linear model vocabulary and coefficients must align")
        if len(set(self.feature_names)) != len(self.feature_names):
            raise ValueError("linear model feature names must be unique")
        if not all(isfinite(value) for value in (*self.coefficients, self.intercept)):
            raise ValueError("linear model values must be finite")
        payload = {
            "schema": "linear-probability-model-v1",
            "link": "logit",
            "positive_class": 1,
            "feature_names": list(self.feature_names),
            "coefficients": list(self.coefficients),
            "intercept": self.intercept,
        }
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        expected = "sha256:" + sha256(encoded).hexdigest()
        if self.fingerprint != expected:
            raise ValueError("linear model fingerprint does not match its content")

    def probability(self, features: Mapping[str, float | str]) -> float:
        score = self.intercept
        for name, coefficient in zip(self.feature_names, self.coefficients):
            if "=" in name:
                field, expected = name.split("=", 1)
                value = 1.0 if str(features.get(field, "")) == expected else 0.0
            else:
                raw = features.get(name, 0.0)
                value = float(raw) if not isinstance(raw, str) else 0.0
            score += coefficient * value
        if score >= 0.0:
            inverse = exp(-min(score, 709.0))
            return 1.0 / (1.0 + inverse)
        positive = exp(max(score, -709.0))
        return positive / (1.0 + positive)


@dataclass(frozen=True, slots=True)
class MorningRetracementRuntimePolicy:
    policy_version: str
    hypothesis_id: str
    hypothesis_version: str
    model: LinearProbabilityModel
    target_fraction: float
    default_probability_threshold: float
    stop_extension_fraction: float
    break_even_trigger_fraction: float
    deadline_local_minute: int
    round_trip_cost_bps: float
    require_volume_baseline: bool
    default_maximum_relative_volume: float
    default_minimum_active_minute_ratio: float
    historical_target_probability: float
    historical_target_probability_lower: float
    historical_non_loss_probability: float
    historical_non_loss_probability_lower: float
    historical_sample_count: int
    historical_trading_days: int
    expected_hit_minutes_p25: int
    expected_hit_minutes_median: int
    expected_hit_minutes_p75: int
    non_loss_model: LinearProbabilityModel | None = None
    default_non_loss_probability_threshold: float | None = None
    break_even_target_progress_fraction: float | None = None
    default_minimum_current_retracement_fraction: float = 0.0
    default_minimum_relative_volume: float = 0.0
    default_enabled_tickers: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.default_non_loss_probability_threshold is not None and not (
            0.5 <= self.default_non_loss_probability_threshold <= 0.99
        ):
            raise ValueError(
                "default non-loss probability threshold must be between 0.50 and 0.99"
            )
        if self.break_even_target_progress_fraction is not None and not (
            0.0 < self.break_even_target_progress_fraction < 1.0
        ):
            raise ValueError(
                "break-even target progress fraction must be in (0, 1)"
            )
        if not 0.0 <= self.default_minimum_current_retracement_fraction < 1.0:
            raise ValueError(
                "default minimum current retracement fraction must be in [0, 1)"
            )
        if not 0.0 <= self.default_minimum_relative_volume <= 10.0:
            raise ValueError("default minimum relative volume must be in [0, 10]")
        if self.default_minimum_relative_volume > self.default_maximum_relative_volume:
            raise ValueError(
                "default relative volume minimum must not exceed maximum"
            )
        if any(not item.strip() for item in self.default_enabled_tickers):
            raise ValueError("default enabled tickers must not contain blanks")

    @property
    def effective_non_loss_model(self) -> LinearProbabilityModel:
        return self.non_loss_model or self.model

    @property
    def effective_non_loss_probability_threshold(self) -> float:
        return (
            self.default_probability_threshold
            if self.default_non_loss_probability_threshold is None
            else self.default_non_loss_probability_threshold
        )


@dataclass(frozen=True, slots=True)
class MorningRetracementRuntimeSettings:
    enabled: bool
    revision: int
    probability_threshold: float
    maximum_relative_volume: float
    minimum_active_minute_ratio: float
    minimum_excursion_bps: float
    minimum_remaining_move_bps: float
    first_decision_local_minute: int
    last_decision_local_minute: int
    monitor_until_local_minute: int
    maximum_signals_per_day: int
    enabled_tickers: frozenset[str]
    telegram_enabled: bool
    non_loss_probability_threshold: float | None = None
    minimum_current_retracement_fraction: float = 0.0
    minimum_relative_volume: float = 0.0

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("runtime settings revision must be positive")
        if not 0.5 <= self.probability_threshold <= 0.99:
            raise ValueError("probability threshold must be between 0.50 and 0.99")
        if self.non_loss_probability_threshold is not None and not (
            0.5 <= self.non_loss_probability_threshold <= 0.99
        ):
            raise ValueError(
                "non-loss probability threshold must be between 0.50 and 0.99"
            )
        if not 0.0 <= self.minimum_current_retracement_fraction < 1.0:
            raise ValueError(
                "minimum current retracement fraction must be in [0, 1)"
            )
        if not 0.0 <= self.minimum_relative_volume <= self.maximum_relative_volume:
            raise ValueError("relative volume minimum must not exceed maximum")
        if not 0.1 <= self.maximum_relative_volume <= 10.0:
            raise ValueError("maximum relative volume must be between 0.1 and 10.0")
        if not 0.0 <= self.minimum_active_minute_ratio <= 1.0:
            raise ValueError("minimum active minute ratio must be in [0, 1]")
        if min(self.minimum_excursion_bps, self.minimum_remaining_move_bps) < 0.0:
            raise ValueError("movement filters must not be negative")
        if not (
            7 * 60
            <= self.first_decision_local_minute
            <= self.last_decision_local_minute
            <= self.monitor_until_local_minute
            <= 11 * 60
        ):
            raise ValueError(
                "decision and monitoring windows must stay inside 07:00-11:00 Moscow"
            )
        if not 1 <= self.maximum_signals_per_day <= 25:
            raise ValueError("maximum signals per day must be between 1 and 25")

    def effective_non_loss_probability_threshold(
        self,
        policy: MorningRetracementRuntimePolicy,
    ) -> float:
        return (
            policy.effective_non_loss_probability_threshold
            if self.non_loss_probability_threshold is None
            else self.non_loss_probability_threshold
        )


@dataclass(frozen=True, slots=True)
class MorningRetracementRecommendation:
    snapshot: MorningSnapshot
    model_probability: float
    target_price: float
    initial_stop_price: float
    break_even_trigger_price: float
    break_even_stop_price: float
    relative_volume: float
    active_minute_ratio: float
    observed_at: datetime
    non_loss_probability: float | None = None

    @property
    def effective_non_loss_probability(self) -> float:
        return (
            self.model_probability
            if self.non_loss_probability is None
            else self.non_loss_probability
        )

    @property
    def expected_direction(self) -> str:
        return (
            "up"
            if self.snapshot.direction is RetracementDirection.RETURN_UP
            else "down"
        )


@dataclass(frozen=True, slots=True)
class MorningRetracementLiveAssessment:
    """One causal minute-by-minute model assessment.

    Assessments are persisted even when an owner filter rejects a notification.
    This keeps the live screen useful and preserves the negative examples needed
    to learn which entry minutes actually work.
    """

    instrument_id: str
    ticker: str
    trading_day: str
    recommendation: MorningRetracementRecommendation
    eligible_for_signal: bool
    reason_codes: tuple[str, ...]
    settings_revision: int
    policy_version: str
    hypothesis_version: str
    model_fingerprint: str
    probability_threshold: float
    maximum_relative_volume: float
    minimum_excursion_bps: float
    minimum_remaining_move_bps: float
    remaining_move_bps: float
    deadline_local_minute: int
    expected_hit_minutes_p25: int
    expected_hit_minutes_median: int
    expected_hit_minutes_p75: int
    training_window_ended: bool
    non_loss_probability_threshold: float | None = None
    non_loss_model_fingerprint: str | None = None
    target_fraction: float = 0.50
    current_retracement_fraction: float = 0.0
    minimum_current_retracement_fraction: float = 0.0
    minimum_relative_volume: float = 0.0

    def __post_init__(self) -> None:
        if not self.instrument_id.strip() or not self.ticker.strip():
            raise ValueError("live assessment instrument identity is required")
        if not self.trading_day.strip():
            raise ValueError("live assessment trading day is required")
        if self.settings_revision < 1:
            raise ValueError("live assessment settings revision must be positive")
        if self.eligible_for_signal == bool(self.reason_codes):
            raise ValueError("eligible assessment must have no rejection reasons")
        if self.remaining_move_bps < 0.0:
            raise ValueError("remaining move must not be negative")
        if not 0.0 < self.target_fraction <= 1.0:
            raise ValueError("target fraction must be in (0, 1]")
        if self.current_retracement_fraction < 0.0:
            raise ValueError("current retracement fraction must not be negative")
        if not 0.0 <= self.minimum_current_retracement_fraction < 1.0:
            raise ValueError(
                "minimum current retracement fraction must be in [0, 1)"
            )
        if not (
            0
            <= self.expected_hit_minutes_p25
            <= self.expected_hit_minutes_median
            <= self.expected_hit_minutes_p75
        ):
            raise ValueError("expected hit minute quantiles must be ordered")

    @property
    def observed_at(self) -> datetime:
        return self.recommendation.observed_at

    @property
    def observation_key(self) -> str:
        return (
            f"{self.instrument_id}:{self.trading_day}:"
            f"{self.observed_at.isoformat()}:{self.policy_version}:"
            f"settings-{self.settings_revision}"
        )


@dataclass(frozen=True, slots=True)
class MorningRetracementTrackedOutcome:
    observation_id: str
    instrument_id: str
    ticker: str
    trading_day: str
    target_hit: bool | None
    non_loss: bool | None
    exit_reason: str
    entry_at: datetime | None
    exit_at: datetime | None
    entry_price: float | None
    exit_price: float | None
    net_result_bps: float | None
    minutes_to_exit: float | None
    evaluated_at: datetime
    outcome_policy_version: str

    def __post_init__(self) -> None:
        if not self.observation_id.strip() or not self.instrument_id.strip():
            raise ValueError("tracked outcome identity is required")
        if self.evaluated_at.tzinfo is None or self.evaluated_at.utcoffset() is None:
            raise ValueError("tracked outcome evaluation time must be timezone-aware")
        if self.minutes_to_exit is not None and self.minutes_to_exit < 0.0:
            raise ValueError("minutes_to_exit must not be negative")


def build_recommendation(
    *,
    snapshot: MorningSnapshot,
    probability: float,
    relative_volume: float,
    active_minute_ratio: float,
    policy: MorningRetracementRuntimePolicy,
    non_loss_probability: float | None = None,
) -> MorningRetracementRecommendation:
    target = snapshot.target_price(policy.target_fraction)
    initial_stop = snapshot.running_extreme - int(snapshot.direction) * (
        policy.stop_extension_fraction * snapshot.excursion_price
    )
    if policy.break_even_target_progress_fraction is None:
        break_even_trigger = snapshot.current_price + int(snapshot.direction) * (
            policy.break_even_trigger_fraction * snapshot.excursion_price
        )
    else:
        break_even_trigger = snapshot.current_price + (
            target - snapshot.current_price
        ) * policy.break_even_target_progress_fraction
    cost_price = (
        snapshot.current_price * policy.round_trip_cost_bps / 10_000.0
        + snapshot.tick_size
    )
    break_even_stop = snapshot.current_price + int(snapshot.direction) * cost_price
    return MorningRetracementRecommendation(
        snapshot=snapshot,
        model_probability=probability,
        target_price=target,
        initial_stop_price=initial_stop,
        break_even_trigger_price=break_even_trigger,
        break_even_stop_price=break_even_stop,
        relative_volume=relative_volume,
        active_minute_ratio=active_minute_ratio,
        observed_at=snapshot.observed_at,
        non_loss_probability=non_loss_probability,
    )
