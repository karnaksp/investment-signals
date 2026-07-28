"""Build causal morning-retracement research episodes from candle records."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from math import exp, log1p, sqrt
from statistics import fmean, median
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (
    MorningSnapshot,
    RetracementDirection,
    RetracementObservation,
    build_snapshot,
    observe_retracements,
)


MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True, slots=True)
class PreviousSignalEvent:
    ticker: str
    event_at: datetime
    signal_type: str
    direction: int
    outcome_ready_at: datetime | None = None
    outcome_confirmed: bool | None = None

    def __post_init__(self) -> None:
        if not self.ticker.strip() or not self.signal_type.strip():
            raise ValueError("previous signal identity must not be empty")
        if self.event_at.tzinfo is None or self.event_at.utcoffset() is None:
            raise ValueError("previous signal event_at must be timezone-aware")
        if self.direction not in (-1, 0, 1):
            raise ValueError("previous signal direction must be -1, 0, or 1")
        if self.outcome_ready_at is not None and (
            self.outcome_ready_at.tzinfo is None
            or self.outcome_ready_at.utcoffset() is None
        ):
            raise ValueError("outcome_ready_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ResearchFeature:
    name: str
    value: float
    observed_at: datetime
    source: str

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("feature name must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("feature timestamp must be timezone-aware")
        if self.source not in {"morning", "previous_session"}:
            raise ValueError("feature source is invalid")


@dataclass(frozen=True, slots=True)
class MorningRetracementResearchPolicy:
    version: str = "morning-retracement-discovery-v1.3.0"
    morning_start_minute: int = 7 * 60
    first_snapshot_minute: int = 7 * 60 + 15
    final_snapshot_minute: int = 10 * 60
    snapshot_step_minutes: int = 5
    outcome_deadline_minute: int = 11 * 60
    analytical_floor_bps: float = 10.0
    round_trip_cost_bps: float = 10.0
    retracement_fractions: tuple[float, ...] = (0.25, 0.50, 0.75, 1.0)
    previous_signal_decay_hours: tuple[int, ...] = (2, 4, 8, 16)

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("policy version must not be empty")
        if not (
            0
            <= self.morning_start_minute
            < self.first_snapshot_minute
            <= self.final_snapshot_minute
            < self.outcome_deadline_minute
            < 24 * 60
        ):
            raise ValueError("morning research minute boundaries are invalid")
        if self.snapshot_step_minutes <= 0:
            raise ValueError("snapshot step must be positive")
        if min(self.analytical_floor_bps, self.round_trip_cost_bps) < 0.0:
            raise ValueError("research costs and floor must not be negative")

    @property
    def tradable_excursion_bps(self) -> float:
        return 4.0 * self.round_trip_cost_bps


@dataclass(frozen=True, slots=True)
class MorningRetracementExample:
    episode_id: str
    row_id: str
    trading_day: date
    snapshot: MorningSnapshot
    feature_cutoff_at: datetime
    features: tuple[ResearchFeature, ...]
    labels: tuple[RetracementObservation, ...]
    future_candles: tuple[HistoricalCandle, ...]
    maximum_retracement_fraction: float
    maximum_adverse_extension_fraction: float
    label_available: bool

    def __post_init__(self) -> None:
        if self.feature_cutoff_at != self.snapshot.observed_at:
            raise ValueError("feature cutoff must equal snapshot time")
        if any(item.observed_at > self.feature_cutoff_at for item in self.features):
            raise ValueError("morning example contains feature leakage")
        if self.label_available and not self.future_candles:
            raise ValueError("available label requires future candles")

    def label_for(self, fraction: float) -> RetracementObservation:
        try:
            return next(item for item in self.labels if item.fraction == fraction)
        except StopIteration as exc:
            raise KeyError(f"retracement label {fraction} is unavailable") from exc

    def feature_values(self, source: str | None = None) -> dict[str, float]:
        return {
            item.name: item.value
            for item in self.features
            if source is None or item.source == source
        }


class BuildMorningRetracementResearch:
    """Application use case; all external records are mapped before entry."""

    def __init__(
        self,
        policy: MorningRetracementResearchPolicy = MorningRetracementResearchPolicy(),
    ) -> None:
        self._policy = policy

    def execute(
        self,
        candles: Sequence[HistoricalCandle],
        *,
        previous_signals: Sequence[PreviousSignalEvent] = (),
    ) -> tuple[MorningRetracementExample, ...]:
        series = _group_candles(candles)
        signals = _group_signals(previous_signals)
        days_by_ticker: defaultdict[str, list[date]] = defaultdict(list)
        for ticker, trading_day in series:
            days_by_ticker[ticker].append(trading_day)

        result: list[MorningRetracementExample] = []
        for ticker, days in sorted(days_by_ticker.items()):
            ordered_days = sorted(set(days))
            cumulative_volume_history: defaultdict[int, list[float]] = defaultdict(list)
            for position, trading_day in enumerate(ordered_days):
                current_rows = series[(ticker, trading_day)]
                if position == 0:
                    _record_cumulative_volume_history(
                        cumulative_volume_history,
                        current_rows,
                        policy=self._policy,
                    )
                    continue
                previous_day = ordered_days[position - 1]
                previous_rows = series[(ticker, previous_day)]
                previous_close = previous_rows[-1].close
                tick_size = _estimate_tick_size(previous_rows + current_rows)
                previous_features = _previous_session_features(previous_rows)
                previous_features += _previous_signal_features(
                    signals.get((ticker, previous_day), ()),
                    feature_cutoff_at=current_rows[0].at,
                    decay_hours=self._policy.previous_signal_decay_hours,
                )
                morning = tuple(
                    row
                    for row in current_rows
                    if self._policy.morning_start_minute
                    <= _local_minute(row.at)
                    <= self._policy.outcome_deadline_minute
                )
                if not morning:
                    continue
                for minute in range(
                    self._policy.first_snapshot_minute,
                    self._policy.final_snapshot_minute + 1,
                    self._policy.snapshot_step_minutes,
                ):
                    row = _at_or_before(morning, minute)
                    if row is None:
                        continue
                    observed = tuple(item for item in morning if item.at <= row.at)
                    snapshot = build_snapshot(
                        ticker=ticker,
                        observed_at=row.at,
                        previous_close=previous_close,
                        observed_candles=observed,
                        analytical_floor_bps=self._policy.analytical_floor_bps,
                        tick_size=tick_size,
                    )
                    if snapshot is None:
                        continue
                    future = tuple(
                        item
                        for item in morning
                        if item.at > snapshot.observed_at
                        and _local_minute(item.at)
                        <= self._policy.outcome_deadline_minute
                    )
                    label_available = bool(
                        future
                        and _local_minute(future[-1].at)
                        >= self._policy.outcome_deadline_minute - 1
                    )
                    labels = observe_retracements(
                        snapshot,
                        future if label_available else (),
                        fractions=self._policy.retracement_fractions,
                    )
                    features = (
                        _morning_features(
                            snapshot,
                            observed,
                            historical_cumulative_volume=_historical_cumulative_volume(
                                cumulative_volume_history,
                                minute,
                            ),
                        )
                        + _current_signal_features(
                            signals.get((ticker, trading_day), ()),
                            feature_cutoff_at=snapshot.observed_at,
                            excursion_direction=-int(snapshot.direction),
                        )
                        + tuple(
                            ResearchFeature(
                                name=item.name,
                                value=item.value,
                                observed_at=item.observed_at,
                                source=item.source,
                            )
                            for item in previous_features
                        )
                        + _cross_session_features(snapshot, previous_rows)
                    )
                    maximum_retracement, maximum_adverse = _path_extremes(
                        snapshot, future
                    )
                    episode_id = f"{ticker}:{trading_day.isoformat()}"
                    row_id = sha256(
                        f"{episode_id}:{snapshot.observed_at.isoformat()}:{self._policy.version}".encode()
                    ).hexdigest()
                    result.append(
                        MorningRetracementExample(
                            episode_id=episode_id,
                            row_id=row_id,
                            trading_day=trading_day,
                            snapshot=snapshot,
                            feature_cutoff_at=snapshot.observed_at,
                            features=features,
                            labels=labels,
                            future_candles=future,
                            maximum_retracement_fraction=maximum_retracement,
                            maximum_adverse_extension_fraction=maximum_adverse,
                            label_available=label_available,
                        )
                    )
                _record_cumulative_volume_history(
                    cumulative_volume_history,
                    current_rows,
                    policy=self._policy,
                )
        return tuple(
            sorted(
                result,
                key=lambda item: (
                    item.trading_day,
                    item.snapshot.ticker,
                    item.snapshot.observed_at,
                ),
            )
        )


def _group_candles(
    candles: Iterable[HistoricalCandle],
) -> dict[tuple[str, date], tuple[HistoricalCandle, ...]]:
    grouped: defaultdict[tuple[str, date], list[HistoricalCandle]] = defaultdict(list)
    for row in candles:
        if row.complete:
            grouped[(row.ticker, row.at.astimezone(MOSCOW).date())].append(row)
    return {
        key: tuple(sorted(rows, key=lambda item: item.at))
        for key, rows in grouped.items()
    }


def _group_signals(
    signals: Iterable[PreviousSignalEvent],
) -> dict[tuple[str, date], tuple[PreviousSignalEvent, ...]]:
    grouped: defaultdict[tuple[str, date], list[PreviousSignalEvent]] = defaultdict(
        list
    )
    for event in signals:
        grouped[(event.ticker, event.event_at.astimezone(MOSCOW).date())].append(event)
    return {
        key: tuple(sorted(rows, key=lambda item: item.event_at))
        for key, rows in grouped.items()
    }


def _previous_session_features(
    rows: tuple[HistoricalCandle, ...],
) -> tuple[ResearchFeature, ...]:
    observed_at = rows[-1].at
    total_volume = sum(item.volume for item in rows)
    total_value = sum(item.close * item.volume for item in rows)
    vwap = total_value / total_volume if total_volume > 0.0 else rows[-1].close
    high = max(item.high for item in rows)
    low = min(item.low for item in rows)
    span = high - low
    returns = [
        (right.close / left.close - 1.0) * 10_000.0
        for left, right in zip(rows, rows[1:])
        if left.close > 0.0
    ]
    last_60 = rows[-60:]
    preceding_60 = rows[-120:-60]
    closing_volume = sum(item.volume for item in last_60)
    prior_volume = sum(item.volume for item in preceding_60)
    values = {
        "prior_return_day_bps": (rows[-1].close / rows[0].open - 1.0) * 10_000.0,
        "prior_return_15m_bps": _window_return(rows, 15),
        "prior_return_30m_bps": _window_return(rows, 30),
        "prior_return_60m_bps": _window_return(rows, 60),
        "prior_close_position": ((rows[-1].close - low) / span if span > 0.0 else 0.5),
        "prior_close_to_vwap_bps": (rows[-1].close / vwap - 1.0) * 10_000.0,
        "prior_range_bps": span / rows[0].open * 10_000.0,
        "prior_realized_volatility_bps": sqrt(sum(value * value for value in returns)),
        "prior_log_total_volume": log1p(total_volume),
        "prior_closing_volume_ratio": (
            closing_volume / prior_volume if prior_volume > 0.0 else 0.0
        ),
        "prior_close_near_high": float(
            span > 0.0 and (high - rows[-1].close) / span <= 0.10
        ),
        "prior_close_near_low": float(
            span > 0.0 and (rows[-1].close - low) / span <= 0.10
        ),
    }
    return tuple(
        ResearchFeature(name, value, observed_at, "previous_session")
        for name, value in sorted(values.items())
    )


def _previous_signal_features(
    events: tuple[PreviousSignalEvent, ...],
    *,
    feature_cutoff_at: datetime,
    decay_hours: tuple[int, ...],
) -> tuple[ResearchFeature, ...]:
    if not events:
        return ()
    mature_outcomes = tuple(
        item
        for item in events
        if item.outcome_ready_at is not None
        and item.outcome_ready_at <= feature_cutoff_at
    )
    values: dict[str, float] = {
        "prior_signal_count": float(len(events)),
        "prior_signal_direction_score": float(sum(item.direction for item in events)),
        "prior_mature_signal_count": float(len(mature_outcomes)),
        "prior_confirmed_signal_count": float(
            sum(item.outcome_confirmed is True for item in mature_outcomes)
        ),
    }
    for hours in decay_hours:
        values[f"prior_signal_decay_{hours}h"] = sum(
            exp(-(feature_cutoff_at - item.event_at).total_seconds() / (hours * 3600.0))
            for item in events
            if item.event_at < feature_cutoff_at
        )
    for signal_type in sorted({item.signal_type for item in events}):
        safe_name = "".join(
            character if character.isalnum() else "_"
            for character in signal_type.lower()
        )
        values[f"prior_signal_type_{safe_name}_count"] = float(
            sum(item.signal_type == signal_type for item in events)
        )
    observed_at = max(item.event_at for item in events)
    return tuple(
        ResearchFeature(name, value, observed_at, "previous_session")
        for name, value in sorted(values.items())
    )


def _current_signal_features(
    events: tuple[PreviousSignalEvent, ...],
    *,
    feature_cutoff_at: datetime,
    excursion_direction: int,
) -> tuple[ResearchFeature, ...]:
    eligible = tuple(item for item in events if item.event_at <= feature_cutoff_at)
    if not eligible:
        return ()
    values: dict[str, float] = {
        "morning_signal_count": float(len(eligible)),
        "morning_signal_direction_score": float(
            sum(item.direction for item in eligible)
        ),
        "morning_signal_excursion_alignment": float(
            sum(item.direction == excursion_direction for item in eligible)
        ),
        "morning_signal_reversal_alignment": float(
            sum(item.direction == -excursion_direction for item in eligible)
        ),
        "morning_signal_decay_15m": sum(
            exp(-(feature_cutoff_at - item.event_at).total_seconds() / (15.0 * 60.0))
            for item in eligible
        ),
        "morning_signal_decay_60m": sum(
            exp(-(feature_cutoff_at - item.event_at).total_seconds() / 3600.0)
            for item in eligible
        ),
    }
    for signal_type in sorted({item.signal_type for item in eligible}):
        safe_name = "".join(
            character if character.isalnum() else "_"
            for character in signal_type.lower()
        )
        values[f"morning_signal_type_{safe_name}_count"] = float(
            sum(item.signal_type == signal_type for item in eligible)
        )
    observed_at = max(item.event_at for item in eligible)
    return tuple(
        ResearchFeature(name, value, observed_at, "morning")
        for name, value in sorted(values.items())
    )


def _morning_features(
    snapshot: MorningSnapshot,
    rows: tuple[HistoricalCandle, ...],
    *,
    historical_cumulative_volume: float | None,
) -> tuple[ResearchFeature, ...]:
    direction = int(snapshot.direction)
    high = max(item.high for item in rows)
    low = min(item.low for item in rows)
    volume = sum(item.volume for item in rows)
    vwap = (
        sum(item.close * item.volume for item in rows) / volume
        if volume > 0.0
        else rows[-1].close
    )
    returns = [
        (right.close / left.close - 1.0) * 10_000.0
        for left, right in zip(rows, rows[1:])
        if left.close > 0.0
    ]
    favorable_progress = direction * (snapshot.current_price - snapshot.running_extreme)
    streak = _directional_streak(rows)
    elapsed_minutes = max(
        1,
        _local_minute(snapshot.observed_at) - 7 * 60 + 1,
    )
    active_minutes = len({_local_minute(item.at) for item in rows})
    baseline_available = (
        historical_cumulative_volume is not None and historical_cumulative_volume > 0.0
    )
    relative_volume = (
        volume / historical_cumulative_volume if baseline_available else 0.0
    )
    values = {
        "decision_local_minute": float(_local_minute(snapshot.observed_at)),
        "excursion_bps": snapshot.excursion_bps,
        "expected_direction": float(direction),
        "current_retracement_fraction": max(
            0.0, favorable_progress / snapshot.excursion_price
        ),
        "minutes_since_extreme": (
            snapshot.observed_at - snapshot.extreme_at
        ).total_seconds()
        / 60.0,
        "morning_return_5m_bps": _window_return(rows, 5),
        "morning_return_15m_bps": _window_return(rows, 15),
        "morning_acceleration_bps": (
            _window_return(rows, 5) - _window_return(rows, 15) / 3.0
        ),
        "morning_realized_volatility_bps": sqrt(
            sum(value * value for value in returns)
        ),
        "morning_range_bps": (high - low) / snapshot.previous_close * 10_000.0,
        "morning_close_to_vwap_bps": (snapshot.current_price / vwap - 1.0) * 10_000.0,
        "morning_log_cumulative_volume": log1p(volume),
        "morning_log_cumulative_turnover_proxy": log1p(
            sum(item.close * item.volume for item in rows)
        ),
        "morning_active_minute_ratio": min(
            1.0,
            active_minutes / elapsed_minutes,
        ),
        "morning_volume_per_active_minute": (
            volume / active_minutes if active_minutes else 0.0
        ),
        "morning_volume_baseline_available": float(baseline_available),
        "morning_log_historical_cumulative_volume": (
            log1p(historical_cumulative_volume) if baseline_available else 0.0
        ),
        "morning_relative_volume": min(relative_volume, 10.0),
        "morning_relative_volume_at_most_half": float(
            baseline_available and relative_volume <= 0.50
        ),
        "morning_directional_streak": float(streak),
        "tradable_excursion": float(snapshot.excursion_bps >= 40.0),
    }
    return tuple(
        ResearchFeature(name, value, snapshot.observed_at, "morning")
        for name, value in sorted(values.items())
    )


def _historical_cumulative_volume(
    history: defaultdict[int, list[float]],
    local_minute: int,
    *,
    maximum_sessions: int = 20,
) -> float | None:
    values = history.get(local_minute, ())
    if not values:
        return None
    return float(median(values[-maximum_sessions:]))


def _record_cumulative_volume_history(
    history: defaultdict[int, list[float]],
    rows: tuple[HistoricalCandle, ...],
    *,
    policy: MorningRetracementResearchPolicy,
) -> None:
    morning = tuple(
        item
        for item in rows
        if policy.morning_start_minute
        <= _local_minute(item.at)
        <= policy.final_snapshot_minute
    )
    if not morning:
        return
    for minute in range(
        policy.first_snapshot_minute,
        policy.final_snapshot_minute + 1,
        policy.snapshot_step_minutes,
    ):
        observed = tuple(item for item in morning if _local_minute(item.at) <= minute)
        if observed:
            history[minute].append(sum(item.volume for item in observed))


def _cross_session_features(
    snapshot: MorningSnapshot,
    previous_rows: tuple[HistoricalCandle, ...],
) -> tuple[ResearchFeature, ...]:
    prior_direction = 1 if previous_rows[-1].close >= previous_rows[0].open else -1
    excursion_direction = -int(snapshot.direction)
    return (
        ResearchFeature(
            "morning_continues_prior_direction",
            float(prior_direction == excursion_direction),
            snapshot.observed_at,
            "previous_session",
        ),
        ResearchFeature(
            "morning_opposes_prior_direction",
            float(prior_direction != excursion_direction),
            snapshot.observed_at,
            "previous_session",
        ),
    )


def _path_extremes(
    snapshot: MorningSnapshot,
    future: tuple[HistoricalCandle, ...],
) -> tuple[float, float]:
    if not future:
        return 0.0, 0.0
    distance = snapshot.excursion_price
    if snapshot.direction is RetracementDirection.RETURN_UP:
        maximum_retracement = (
            max(item.high for item in future) - snapshot.running_extreme
        ) / distance
        maximum_adverse = (
            snapshot.running_extreme - min(item.low for item in future)
        ) / distance
    else:
        maximum_retracement = (
            snapshot.running_extreme - min(item.low for item in future)
        ) / distance
        maximum_adverse = (
            max(item.high for item in future) - snapshot.running_extreme
        ) / distance
    return max(0.0, maximum_retracement), max(0.0, maximum_adverse)


def _at_or_before(
    rows: tuple[HistoricalCandle, ...], local_minute: int
) -> HistoricalCandle | None:
    eligible = tuple(item for item in rows if _local_minute(item.at) <= local_minute)
    return eligible[-1] if eligible else None


def _local_minute(at: datetime) -> int:
    local = at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute


def _window_return(rows: tuple[HistoricalCandle, ...], minutes: int) -> float:
    window = rows[-max(2, minutes) :]
    if len(window) < 2:
        return 0.0
    return (window[-1].close / window[0].open - 1.0) * 10_000.0


def _directional_streak(rows: tuple[HistoricalCandle, ...]) -> int:
    if not rows:
        return 0
    signs = [
        1 if item.close > item.open else -1 if item.close < item.open else 0
        for item in rows
    ]
    last = signs[-1]
    if last == 0:
        return 0
    count = 0
    for sign in reversed(signs):
        if sign != last:
            break
        count += 1
    return count * last


def _estimate_tick_size(rows: tuple[HistoricalCandle, ...]) -> float:
    prices = sorted(
        {
            round(value, 10)
            for row in rows
            for value in (row.open, row.high, row.low, row.close)
        }
    )
    differences = [
        right - left for left, right in zip(prices, prices[1:]) if right - left > 1e-9
    ]
    if differences:
        return min(differences)
    return max(rows[-1].close * 1e-6, 1e-6)
