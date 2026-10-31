"""Collect and compare actual adjacent available exchange trading sessions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.daily_positions import (
    DailyPositionComparison, PositionSnapshot,
)


class DailyPositionSource(Protocol):
    def load(self, trading_day: date) -> PositionSnapshot | None: ...


class DailyPositionPublisher(Protocol):
    def publish(self, comparison: DailyPositionComparison, *, generated_at: datetime) -> bool: ...


@dataclass(frozen=True)
class DailyPositionReceipt:
    status: str
    trading_day: date | None = None
    previous_day: date | None = None


class CollectDailyPositions:
    def __init__(self, *, source: DailyPositionSource, publisher: DailyPositionPublisher) -> None:
        self._source = source
        self._publisher = publisher

    def execute(self, *, as_of: datetime) -> DailyPositionReceipt:
        if as_of.utcoffset() is None:
            raise ValueError("collection timestamp must be aware")
        today = as_of.astimezone(ZoneInfo("Asia/Shanghai")).date()
        snapshots: list[PositionSnapshot] = []
        # A missing file can be a holiday or a report not yet published. Network
        # and schema failures propagate; they must not masquerade as holidays.
        for days_ago in range(15):
            candidate = today - timedelta(days=days_ago)
            if candidate.weekday() >= 5:
                continue
            snapshot = self._source.load(candidate)
            if snapshot is None:
                continue
            if snapshot.trading_day != candidate or snapshot.published_at > as_of:
                raise ValueError("source returned the wrong or a future report")
            snapshots.append(snapshot)
            if len(snapshots) == 2:
                break
        if len(snapshots) < 2:
            return DailyPositionReceipt("insufficient_reports")
        current, previous = snapshots
        if (today - current.trading_day).days > 7:
            return DailyPositionReceipt("stale_source", current.trading_day, previous.trading_day)
        comparison = DailyPositionComparison(current, previous)
        queued = self._publisher.publish(comparison, generated_at=as_of)
        return DailyPositionReceipt(
            "queued" if queued else "already_queued", current.trading_day, previous.trading_day,
        )
