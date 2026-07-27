"""Causal morning-retracement values and conservative trade simulation.

The module is deliberately independent of storage, data frames, model
libraries, and broker SDKs.  It receives already observed candles and returns
immutable domain values.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import isfinite
from typing import Iterable

from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)


class RetracementDirection(int, Enum):
    RETURN_DOWN = -1
    RETURN_UP = 1


class TradeExitReason(str, Enum):
    TARGET = "target"
    INITIAL_STOP = "initial_stop"
    BREAK_EVEN = "break_even"
    DEADLINE = "deadline"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class MorningSnapshot:
    ticker: str
    observed_at: datetime
    previous_close: float
    current_price: float
    running_extreme: float
    extreme_at: datetime
    direction: RetracementDirection
    excursion_bps: float
    tick_size: float

    def __post_init__(self) -> None:
        if not self.ticker.strip():
            raise ValueError("ticker must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if self.extreme_at > self.observed_at:
            raise ValueError("running extreme cannot be observed in the future")
        if min(
            self.previous_close,
            self.current_price,
            self.running_extreme,
            self.tick_size,
        ) <= 0.0:
            raise ValueError("prices and tick size must be positive")
        if self.excursion_bps <= 0.0 or not isfinite(self.excursion_bps):
            raise ValueError("excursion_bps must be finite and positive")

    @property
    def excursion_price(self) -> float:
        return abs(self.running_extreme - self.previous_close)

    def target_price(self, fraction: float) -> float:
        return retracement_price(
            previous_close=self.previous_close,
            running_extreme=self.running_extreme,
            fraction=fraction,
        )


@dataclass(frozen=True, slots=True)
class RetracementObservation:
    fraction: float
    target_price: float
    reached: bool
    first_reached_at: datetime | None
    minutes_to_target: float | None


@dataclass(frozen=True, slots=True)
class TradePolicy:
    target_fraction: float
    stop_extension_fraction: float
    break_even_trigger_fraction: float
    deadline_local_minute: int
    round_trip_cost_bps: float
    doubled_slippage_bps: float = 0.0

    def __post_init__(self) -> None:
        for name, value in (
            ("target_fraction", self.target_fraction),
            ("stop_extension_fraction", self.stop_extension_fraction),
            ("break_even_trigger_fraction", self.break_even_trigger_fraction),
        ):
            if not 0.0 < value <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if not 0 <= self.deadline_local_minute < 24 * 60:
            raise ValueError("deadline_local_minute is invalid")
        if min(self.round_trip_cost_bps, self.doubled_slippage_bps) < 0.0:
            raise ValueError("costs must not be negative")

    @property
    def effective_round_trip_cost_bps(self) -> float:
        return self.round_trip_cost_bps + self.doubled_slippage_bps

    @property
    def key(self) -> str:
        return (
            f"r{int(self.target_fraction * 100)}"
            f"-s{int(self.stop_extension_fraction * 100)}"
            f"-be{int(self.break_even_trigger_fraction * 100)}"
            f"-d{self.deadline_local_minute}"
            f"-c{self.effective_round_trip_cost_bps:g}"
        )


@dataclass(frozen=True, slots=True)
class TradeSimulation:
    policy_key: str
    entry_at: datetime | None
    entry_price: float | None
    target_price: float
    initial_stop_price: float
    break_even_trigger_price: float | None
    break_even_stop_price: float | None
    break_even_armed_at: datetime | None
    exit_at: datetime | None
    exit_price: float | None
    exit_reason: TradeExitReason
    gross_result_bps: float | None
    net_result_bps: float | None

    @property
    def target_hit(self) -> bool:
        return self.exit_reason is TradeExitReason.TARGET

    @property
    def non_loss(self) -> bool:
        return self.net_result_bps is not None and self.net_result_bps >= 0.0


def retracement_price(
    *,
    previous_close: float,
    running_extreme: float,
    fraction: float,
) -> float:
    if min(previous_close, running_extreme) <= 0.0:
        raise ValueError("prices must be positive")
    if not 0.0 < fraction <= 1.0:
        raise ValueError("retracement fraction must be in (0, 1]")
    return running_extreme + fraction * (previous_close - running_extreme)


def build_snapshot(
    *,
    ticker: str,
    observed_at: datetime,
    previous_close: float,
    observed_candles: Iterable[HistoricalCandle],
    analytical_floor_bps: float,
    tick_size: float,
) -> MorningSnapshot | None:
    rows = tuple(
        sorted(
            (row for row in observed_candles if row.at <= observed_at and row.complete),
            key=lambda row: row.at,
        )
    )
    if not rows:
        return None
    high_row = max(rows, key=lambda row: (row.high, -row.at.timestamp()))
    low_row = min(rows, key=lambda row: (row.low, row.at.timestamp()))
    high_distance = high_row.high - previous_close
    low_distance = previous_close - low_row.low
    if max(high_distance, low_distance) <= 0.0:
        return None
    if high_distance >= low_distance:
        extreme = high_row.high
        extreme_at = high_row.at
        direction = RetracementDirection.RETURN_DOWN
    else:
        extreme = low_row.low
        extreme_at = low_row.at
        direction = RetracementDirection.RETURN_UP
    excursion_bps = abs(extreme / previous_close - 1.0) * 10_000.0
    measurement_floor = max(
        analytical_floor_bps,
        2.0 * tick_size / previous_close * 10_000.0,
    )
    if excursion_bps < measurement_floor:
        return None
    return MorningSnapshot(
        ticker=ticker,
        observed_at=observed_at,
        previous_close=previous_close,
        current_price=rows[-1].close,
        running_extreme=extreme,
        extreme_at=extreme_at,
        direction=direction,
        excursion_bps=excursion_bps,
        tick_size=tick_size,
    )


def observe_retracements(
    snapshot: MorningSnapshot,
    future_candles: Iterable[HistoricalCandle],
    *,
    fractions: tuple[float, ...] = (0.25, 0.50, 0.75, 1.0),
) -> tuple[RetracementObservation, ...]:
    future = tuple(
        sorted(
            (
                row
                for row in future_candles
                if row.complete and row.at > snapshot.observed_at
            ),
            key=lambda row: row.at,
        )
    )
    result: list[RetracementObservation] = []
    for fraction in fractions:
        target = snapshot.target_price(fraction)
        first = next(
            (row for row in future if _target_touched(row, target, snapshot.direction)),
            None,
        )
        result.append(
            RetracementObservation(
                fraction=fraction,
                target_price=target,
                reached=first is not None,
                first_reached_at=(first.at if first else None),
                minutes_to_target=(
                    (first.at - snapshot.observed_at).total_seconds() / 60.0
                    if first
                    else None
                ),
            )
        )
    return tuple(result)


def simulate_trade(
    snapshot: MorningSnapshot,
    future_candles: Iterable[HistoricalCandle],
    policy: TradePolicy,
) -> TradeSimulation:
    future = tuple(
        sorted(
            (
                row
                for row in future_candles
                if row.complete and row.at > snapshot.observed_at
            ),
            key=lambda row: row.at,
        )
    )
    target = snapshot.target_price(policy.target_fraction)
    direction = int(snapshot.direction)
    stop = snapshot.running_extreme - (
        direction * snapshot.excursion_price * policy.stop_extension_fraction
    )
    if not future:
        return TradeSimulation(
            policy_key=policy.key,
            entry_at=None,
            entry_price=None,
            target_price=target,
            initial_stop_price=stop,
            break_even_trigger_price=None,
            break_even_stop_price=None,
            break_even_armed_at=None,
            exit_at=None,
            exit_price=None,
            exit_reason=TradeExitReason.UNAVAILABLE,
            gross_result_bps=None,
            net_result_bps=None,
        )

    entry_row = future[0]
    market_entry = entry_row.open
    if direction * (target - market_entry) <= 0.0:
        # A gap through the target means the modeled decision could not have
        # entered before the claimed profit was realized.
        return TradeSimulation(
            policy_key=policy.key,
            entry_at=None,
            entry_price=None,
            target_price=target,
            initial_stop_price=stop,
            break_even_trigger_price=None,
            break_even_stop_price=None,
            break_even_armed_at=None,
            exit_at=None,
            exit_price=None,
            exit_reason=TradeExitReason.UNAVAILABLE,
            gross_result_bps=None,
            net_result_bps=None,
        )
    half_cost = policy.effective_round_trip_cost_bps / 2.0
    modeled_entry = market_entry * (1.0 + direction * half_cost / 10_000.0)
    trigger = market_entry + (
        direction
        * snapshot.excursion_price
        * policy.break_even_trigger_fraction
    )
    break_even_stop = _break_even_market_price(
        modeled_entry=modeled_entry,
        direction=direction,
        exit_cost_bps=half_cost,
        tick_size=snapshot.tick_size,
    )
    armed_at: datetime | None = None

    for row in future:
        protective = break_even_stop if armed_at is not None else stop
        if _stop_touched(row, protective, snapshot.direction):
            reason = (
                TradeExitReason.BREAK_EVEN
                if armed_at is not None
                else TradeExitReason.INITIAL_STOP
            )
            return _completed_trade(
                snapshot=snapshot,
                policy=policy,
                entry_at=entry_row.at,
                market_entry=market_entry,
                modeled_entry=modeled_entry,
                target=target,
                stop=stop,
                trigger=trigger,
                break_even_stop=break_even_stop,
                armed_at=armed_at,
                exit_at=row.at,
                market_exit=_protective_execution_price(
                    row, protective, snapshot.direction
                ),
                reason=reason,
            )
        if _target_touched(row, target, snapshot.direction):
            return _completed_trade(
                snapshot=snapshot,
                policy=policy,
                entry_at=entry_row.at,
                market_entry=market_entry,
                modeled_entry=modeled_entry,
                target=target,
                stop=stop,
                trigger=trigger,
                break_even_stop=break_even_stop,
                armed_at=armed_at,
                exit_at=row.at,
                market_exit=target,
                reason=TradeExitReason.TARGET,
            )
        if armed_at is None and _target_touched(row, trigger, snapshot.direction):
            # Minute OHLC does not reveal whether trigger or a subsequent
            # reversal happened first.  Counting a same-candle return to the
            # break-even level is the conservative interpretation.
            if row.low <= break_even_stop <= row.high:
                return _completed_trade(
                    snapshot=snapshot,
                    policy=policy,
                    entry_at=entry_row.at,
                    market_entry=market_entry,
                    modeled_entry=modeled_entry,
                    target=target,
                    stop=stop,
                    trigger=trigger,
                    break_even_stop=break_even_stop,
                    armed_at=row.at,
                    exit_at=row.at,
                    market_exit=break_even_stop,
                    reason=TradeExitReason.BREAK_EVEN,
                )
            armed_at = row.at

    last = future[-1]
    return _completed_trade(
        snapshot=snapshot,
        policy=policy,
        entry_at=entry_row.at,
        market_entry=market_entry,
        modeled_entry=modeled_entry,
        target=target,
        stop=stop,
        trigger=trigger,
        break_even_stop=break_even_stop,
        armed_at=armed_at,
        exit_at=last.at,
        market_exit=last.close,
        reason=TradeExitReason.DEADLINE,
    )


def _completed_trade(
    *,
    snapshot: MorningSnapshot,
    policy: TradePolicy,
    entry_at: datetime,
    market_entry: float,
    modeled_entry: float,
    target: float,
    stop: float,
    trigger: float,
    break_even_stop: float,
    armed_at: datetime | None,
    exit_at: datetime,
    market_exit: float,
    reason: TradeExitReason,
) -> TradeSimulation:
    direction = int(snapshot.direction)
    half_cost = policy.effective_round_trip_cost_bps / 2.0
    modeled_exit = market_exit * (1.0 - direction * half_cost / 10_000.0)
    gross = direction * (market_exit / market_entry - 1.0) * 10_000.0
    net = direction * (modeled_exit / modeled_entry - 1.0) * 10_000.0
    return TradeSimulation(
        policy_key=policy.key,
        entry_at=entry_at,
        entry_price=modeled_entry,
        target_price=target,
        initial_stop_price=stop,
        break_even_trigger_price=trigger,
        break_even_stop_price=break_even_stop,
        break_even_armed_at=armed_at,
        exit_at=exit_at,
        exit_price=modeled_exit,
        exit_reason=reason,
        gross_result_bps=gross,
        net_result_bps=net,
    )


def _break_even_market_price(
    *,
    modeled_entry: float,
    direction: int,
    exit_cost_bps: float,
    tick_size: float,
) -> float:
    denominator = 1.0 - direction * exit_cost_bps / 10_000.0
    return modeled_entry / denominator + direction * tick_size


def _target_touched(
    candle: HistoricalCandle,
    price: float,
    direction: RetracementDirection,
) -> bool:
    if direction is RetracementDirection.RETURN_UP:
        return candle.high >= price
    return candle.low <= price


def _stop_touched(
    candle: HistoricalCandle,
    price: float,
    direction: RetracementDirection,
) -> bool:
    if direction is RetracementDirection.RETURN_UP:
        return candle.low <= price
    return candle.high >= price


def _protective_execution_price(
    candle: HistoricalCandle,
    stop_price: float,
    direction: RetracementDirection,
) -> float:
    if direction is RetracementDirection.RETURN_UP:
        return min(stop_price, candle.open)
    return max(stop_price, candle.open)
