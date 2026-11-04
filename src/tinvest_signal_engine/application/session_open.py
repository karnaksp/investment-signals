"""Announce current session boundaries, grouping simultaneous markets."""

from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.session_open import SessionBoundary, require_aware


class SessionCalendar(Protocol):
    def boundaries(self, *, now: datetime) -> tuple[SessionBoundary, ...]: ...


class SessionPublisher(Protocol):
    def publish(self, events: tuple[SessionBoundary, ...], *, generated_at: datetime) -> bool: ...


class AnnounceSessionBoundaries:
    def __init__(self, *, calendar: SessionCalendar, publisher: SessionPublisher) -> None:
        self._calendar, self._publisher = calendar, publisher

    def execute(self, *, now: datetime) -> int:
        require_aware(now)
        groups: dict[tuple[datetime, str], list[SessionBoundary]] = {}
        for event in self._calendar.boundaries(now=now):
            if event.is_due(now):
                groups.setdefault((event.event_at, event.event_kind), []).append(event)
        return sum(self._publisher.publish(tuple(sorted(events, key=lambda e: e.session_id)), generated_at=now)
                   for _, events in sorted(groups.items()))
