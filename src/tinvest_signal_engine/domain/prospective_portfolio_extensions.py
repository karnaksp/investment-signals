"""Causal domain records for the explicit H10/H11 R2 portfolio extension."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import json
from math import isfinite


class R2ExtensionHypothesis(str, Enum):
    OPENING_GAP_REVERSION = "H10"
    MARKET_RESIDUAL_REVERSION = "H11"

    @property
    def version(self) -> str:
        return "1.0.0"


class R2Decision(str, Enum):
    MATCHED = "matched"
    NOT_MATCHED = "not_matched"
    ABSTAIN = "abstain"


class R2Reason(str, Enum):
    CONDITIONS_MATCHED = "conditions_matched"
    CONDITIONS_NOT_MET = "conditions_not_met"
    MISSING_PREVIOUS_CLOSE = "missing_previous_close"
    INSUFFICIENT_HISTORY = "insufficient_history"
    SHORTENED_SESSION = "shortened_session"
    EXCHANGE_SCHEDULE_UNKNOWN = "exchange_schedule_unknown"
    MARKET_WIDE_POSITIVE_GAP = "market_wide_positive_gap"
    MARKET_BETA_UNAVAILABLE = "market_beta_unavailable"
    BASKET_COVERAGE_BELOW_MINIMUM = "basket_coverage_below_minimum"
    TRADING_GAP = "trading_gap"
    CORPORATE_ACTION_SUSPECTED = "corporate_action_suspected"
    MARKET_WIDE_MOVE_SAME_DIRECTION = "market_wide_move_same_direction"
    DIRECTION_UNAVAILABLE = "direction_unavailable"
    OUTCOME_UNAVAILABLE = "outcome_unavailable"


@dataclass(frozen=True, slots=True)
class R2ExtensionPolicy:
    """Immutable implementation parameters derived from the sealed registry."""

    version: str = "r2-h10-h11-v1.0.0"
    cost_model_version: str = "1.0.0"
    opening_gap_history_days: int = 20
    opening_gap_z_min: float = 2.0
    market_gap_z_abstention_min: float = 2.0
    opening_gap_horizons_seconds: tuple[int, ...] = (1800, 3600)
    residual_window_minutes: int = 5
    residual_beta_lookback_days: int = 20
    residual_percentile_min: float = 0.99
    residual_basket_coverage_min: float = 0.80
    residual_horizons_seconds: tuple[int, ...] = (900, 1800)
    round_trip_cost_bps: float = 10.0

    def __post_init__(self) -> None:
        positive = (
            self.opening_gap_history_days,
            *self.opening_gap_horizons_seconds,
            self.residual_window_minutes,
            self.residual_beta_lookback_days,
            *self.residual_horizons_seconds,
        )
        if (
            not self.version.strip()
            or not self.cost_model_version.strip()
            or any(value <= 0 for value in positive)
        ):
            raise ValueError("R2 policy identity, windows, and horizons must be positive")
        if self.opening_gap_z_min <= 0 or self.market_gap_z_abstention_min <= 0:
            raise ValueError("R2 z-score thresholds must be positive")
        if not 0 < self.residual_percentile_min < 1:
            raise ValueError("R2 residual percentile must be in (0, 1)")
        if not 0 < self.residual_basket_coverage_min <= 1:
            raise ValueError("R2 basket coverage must be in (0, 1]")
        if self.round_trip_cost_bps < 0:
            raise ValueError("R2 costs must be non-negative")
        if tuple(sorted(set(self.opening_gap_horizons_seconds))) != self.opening_gap_horizons_seconds:
            raise ValueError("R2 opening-gap horizons must be sorted and unique")
        if tuple(sorted(set(self.residual_horizons_seconds))) != self.residual_horizons_seconds:
            raise ValueError("R2 residual horizons must be sorted and unique")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            {
                "cost_model_version": self.cost_model_version,
                "market_gap_z_abstention_min": self.market_gap_z_abstention_min,
                "opening_gap_history_days": self.opening_gap_history_days,
                "opening_gap_horizons_seconds": self.opening_gap_horizons_seconds,
                "opening_gap_z_min": self.opening_gap_z_min,
                "residual_basket_coverage_min": self.residual_basket_coverage_min,
                "residual_beta_lookback_days": self.residual_beta_lookback_days,
                "residual_horizons_seconds": self.residual_horizons_seconds,
                "residual_percentile_min": self.residual_percentile_min,
                "residual_window_minutes": self.residual_window_minutes,
                "round_trip_cost_bps": self.round_trip_cost_bps,
                "version": self.version,
            }
        )


@dataclass(frozen=True, slots=True)
class R2Metric:
    name: str
    value: float

    def __post_init__(self) -> None:
        if not self.name.strip() or not isfinite(self.value):
            raise ValueError("R2 metrics must be named and finite")


@dataclass(frozen=True, slots=True)
class R2Feature:
    observation_id: str
    hypothesis: R2ExtensionHypothesis
    ticker: str
    trading_day: date
    event_at: datetime
    available_at: datetime
    feature_source_available_at: datetime
    history_available_at: datetime | None
    model_available_at: datetime | None
    horizon_seconds: int
    decision: R2Decision
    reason: R2Reason
    expected_direction: int
    values: tuple[R2Metric, ...]

    def __post_init__(self) -> None:
        if not self.observation_id.startswith("sha256:") or not self.ticker.strip():
            raise ValueError("R2 feature identity is invalid")
        for name, value in (
            ("event_at", self.event_at),
            ("available_at", self.available_at),
            ("feature_source_available_at", self.feature_source_available_at),
        ):
            _require_aware(value, name)
        if self.available_at < self.event_at:
            raise ValueError("R2 feature cannot be available before its event")
        if self.feature_source_available_at > self.available_at:
            raise ValueError("R2 feature uses a source unavailable at decision time")
        for name, value in (
            ("history_available_at", self.history_available_at),
            ("model_available_at", self.model_available_at),
        ):
            if value is None:
                continue
            _require_aware(value, name)
            if value >= self.event_at:
                raise ValueError(f"{name} must precede the event")
        if self.horizon_seconds <= 0:
            raise ValueError("R2 horizon must be positive")
        if self.expected_direction not in {-1, 0, 1}:
            raise ValueError("R2 direction must be -1, 0, or 1")
        if self.decision is R2Decision.MATCHED and self.expected_direction == 0:
            raise ValueError("matched R2 feature requires a direction")
        if self.decision is not R2Decision.MATCHED and self.expected_direction != 0:
            raise ValueError("non-matched R2 feature must not claim direction")
        names = tuple(item.name for item in self.values)
        if len(names) != len(set(names)):
            raise ValueError("R2 feature metric names must be unique")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            {
                "available_at": self.available_at.isoformat(),
                "decision": self.decision.value,
                "event_at": self.event_at.isoformat(),
                "expected_direction": self.expected_direction,
                "feature_source_available_at": self.feature_source_available_at.isoformat(),
                "history_available_at": _iso(self.history_available_at),
                "horizon_seconds": self.horizon_seconds,
                "hypothesis": self.hypothesis.value,
                "hypothesis_version": self.hypothesis.version,
                "model_available_at": _iso(self.model_available_at),
                "observation_id": self.observation_id,
                "reason": self.reason.value,
                "ticker": self.ticker,
                "trading_day": self.trading_day.isoformat(),
                "values": tuple((item.name, item.value) for item in self.values),
            }
        )

    def value(self, name: str) -> float:
        try:
            return next(item.value for item in self.values if item.name == name)
        except StopIteration as exc:
            raise KeyError(name) from exc


@dataclass(frozen=True, slots=True)
class R2Outcome:
    observation_id: str
    target_at: datetime
    available_at: datetime
    available: bool
    reason: R2Reason
    forward_return_bps: float | None
    cost_adjusted_signed_return_bps: float | None

    def __post_init__(self) -> None:
        _require_aware(self.target_at, "target_at")
        _require_aware(self.available_at, "available_at")
        if self.available_at < self.target_at:
            raise ValueError("R2 outcome cannot be available before its target")
        values = (self.forward_return_bps, self.cost_adjusted_signed_return_bps)
        if any(value is not None and not isfinite(value) for value in values):
            raise ValueError("R2 outcome values must be finite")
        if self.available and self.forward_return_bps is None:
            raise ValueError("available R2 outcome requires a forward return")
        if not self.available and any(value is not None for value in values):
            raise ValueError("unavailable R2 outcome must not carry measurements")

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            {
                "available": self.available,
                "available_at": self.available_at.isoformat(),
                "cost_adjusted_signed_return_bps": self.cost_adjusted_signed_return_bps,
                "forward_return_bps": self.forward_return_bps,
                "observation_id": self.observation_id,
                "reason": self.reason.value,
                "target_at": self.target_at.isoformat(),
            }
        )


def feature_identity(
    *,
    hypothesis: R2ExtensionHypothesis,
    ticker: str,
    available_at: datetime,
    horizon_seconds: int,
    policy_fingerprint: str,
) -> str:
    return _fingerprint(
        {
            "available_at": available_at.isoformat(),
            "horizon_seconds": horizon_seconds,
            "hypothesis": hypothesis.value,
            "hypothesis_version": hypothesis.version,
            "policy_fingerprint": policy_fingerprint,
            "ticker": ticker,
        }
    )


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
