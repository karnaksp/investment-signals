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
    retracement_price,
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
    maximum_signals_per_day: int
    enabled_tickers: frozenset[str]
    telegram_enabled: bool

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("runtime settings revision must be positive")
        if not 0.5 <= self.probability_threshold <= 0.99:
            raise ValueError("probability threshold must be between 0.50 and 0.99")
        if not 0.1 <= self.maximum_relative_volume <= 2.0:
            raise ValueError("maximum relative volume must be between 0.1 and 2.0")
        if not 0.0 <= self.minimum_active_minute_ratio <= 1.0:
            raise ValueError("minimum active minute ratio must be in [0, 1]")
        if min(self.minimum_excursion_bps, self.minimum_remaining_move_bps) < 0.0:
            raise ValueError("movement filters must not be negative")
        if not (
            7 * 60
            <= self.first_decision_local_minute
            <= self.last_decision_local_minute
            <= 10 * 60
        ):
            raise ValueError("decision window must stay inside 07:00-10:00 Moscow")
        if not 1 <= self.maximum_signals_per_day <= 25:
            raise ValueError("maximum signals per day must be between 1 and 25")


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

    @property
    def expected_direction(self) -> str:
        return (
            "up"
            if self.snapshot.direction is RetracementDirection.RETURN_UP
            else "down"
        )


def build_recommendation(
    *,
    snapshot: MorningSnapshot,
    probability: float,
    relative_volume: float,
    active_minute_ratio: float,
    policy: MorningRetracementRuntimePolicy,
) -> MorningRetracementRecommendation:
    target = snapshot.target_price(policy.target_fraction)
    initial_stop = snapshot.running_extreme - int(snapshot.direction) * (
        policy.stop_extension_fraction * snapshot.excursion_price
    )
    break_even_trigger = retracement_price(
        previous_close=snapshot.previous_close,
        running_extreme=snapshot.running_extreme,
        fraction=policy.break_even_trigger_fraction,
    )
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
    )
