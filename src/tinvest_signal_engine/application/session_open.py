"""Emit only current openings through an idempotent publisher."""

from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.session_open import SessionOpening, require_aware


class SessionCalendar(Protocol):
    def openings(self, *, now: datetime) -> tuple[SessionOpening, ...]: ...


class SessionPublisher(Protocol):
    def publish(self, opening: SessionOpening, *, generated_at: datetime) -> bool: ...


class AnnounceSessionOpenings:
    def __init__(self, *, calendar: SessionCalendar, publisher: SessionPublisher) -> None:
        self._calendar, self._publisher = calendar, publisher

    def execute(self, *, now: datetime) -> int:
        require_aware(now)
        return sum(
            self._publisher.publish(opening, generated_at=now)
            for opening in self._calendar.openings(now=now) if opening.is_due(now)
        )
