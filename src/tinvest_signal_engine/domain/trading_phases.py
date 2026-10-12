"""Versioned Moscow Exchange phase schedule used by research hypotheses.

The schedule is deliberately a domain value rather than a broker/API concern.
It describes the intervals admitted by the current preregistered research
policy.  A schedule change therefore requires a new version and a new
replication run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo


class TradingPhase(str, Enum):
    MORNING_LOW_LIQUIDITY = "morning_low_liquidity"
    OPENING_TRANSITION = "opening_transition"
    MAIN_OPENING = "main_opening"
    MAIN_CONTINUOUS = "main_continuous"
    PRE_CLOSE = "pre_close"
    OUTSIDE_RESEARCH_SESSION = "outside_research_session"


@dataclass(frozen=True, slots=True)
class TradingPhaseWindow:
    phase: TradingPhase
    start_minute: int
    end_minute: int
    signal_eligible: bool

    def __post_init__(self) -> None:
        if not 0 <= self.start_minute <= self.end_minute < 24 * 60:
            raise ValueError("phase window minutes must be within one local day")

    def contains(self, local_minute: int) -> bool:
        return self.start_minute <= local_minute <= self.end_minute


@dataclass(frozen=True, slots=True)
class TradingPhaseSchedule:
    version: str
    timezone_name: str
    windows: tuple[TradingPhaseWindow, ...]

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("schedule version must not be empty")
        if not self.windows:
            raise ValueError("schedule must contain phase windows")
        ordered = sorted(self.windows, key=lambda item: item.start_minute)
        for previous, current in zip(ordered, ordered[1:]):
            if current.start_minute <= previous.end_minute:
                raise ValueError("phase windows must not overlap")

    def phase_at(self, at: datetime) -> TradingPhase:
        if at.tzinfo is None or at.utcoffset() is None:
            raise ValueError("phase timestamps must be timezone-aware")
        local = at.astimezone(ZoneInfo(self.timezone_name))
        local_minute = local.hour * 60 + local.minute
        for window in self.windows:
            if window.contains(local_minute):
                return window.phase
        return TradingPhase.OUTSIDE_RESEARCH_SESSION

    def is_signal_eligible(self, at: datetime) -> bool:
        phase = self.phase_at(at)
        return any(
            window.phase is phase and window.signal_eligible
            for window in self.windows
        )

    def research_bucket(self, at: datetime) -> int:
        """Return a stable coarse bucket without conflating morning and main."""

        phase = self.phase_at(at)
        if phase is TradingPhase.MORNING_LOW_LIQUIDITY:
            return 0
        if phase is TradingPhase.MAIN_OPENING:
            return 1
        if phase is TradingPhase.MAIN_CONTINUOUS:
            local = at.astimezone(ZoneInfo(self.timezone_name))
            minutes_after_opening = local.hour * 60 + local.minute - 10 * 60 - 30
            return 2 + max(0, min(2, minutes_after_opening // 150))
        if phase is TradingPhase.PRE_CLOSE:
            return 5
        raise ValueError(f"phase {phase.value} has no research bucket")


def _minute(hour: int, minute: int) -> int:
    return hour * 60 + minute


MOEX_EQUITY_PHASE_SCHEDULE_V1 = TradingPhaseSchedule(
    version="moex-equity-phases-v1.0.0",
    timezone_name="Europe/Moscow",
    windows=(
        TradingPhaseWindow(
            TradingPhase.MORNING_LOW_LIQUIDITY,
            _minute(7, 0),
            _minute(9, 49),
            True,
        ),
        TradingPhaseWindow(
            TradingPhase.OPENING_TRANSITION,
            _minute(9, 50),
            _minute(9, 59),
            False,
        ),
        TradingPhaseWindow(
            TradingPhase.MAIN_OPENING,
            _minute(10, 0),
            _minute(10, 29),
            True,
        ),
        TradingPhaseWindow(
            TradingPhase.MAIN_CONTINUOUS,
            _minute(10, 30),
            _minute(18, 9),
            True,
        ),
        TradingPhaseWindow(
            TradingPhase.PRE_CLOSE,
            _minute(18, 10),
            _minute(18, 39),
            True,
        ),
    ),
)
