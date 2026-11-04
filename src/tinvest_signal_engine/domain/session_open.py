"""Scheduled session boundaries, useful only during the event minute."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessDecision


# Retain support for notifications queued by the opening-only release.
SESSION_OPEN_SIGNAL_TYPE = "market_session_open"
SESSION_OPEN_VERSION = "market_sessions_v1"
SESSION_VERSION = "market_sessions_v2"
SESSION_EVENT_KINDS = frozenset({"open", "close", "break_start", "break_end"})
SESSION_SIGNAL_TYPES = frozenset("market_session_" + kind for kind in SESSION_EVENT_KINDS)


@dataclass(frozen=True, slots=True)
class SessionBoundary:
    session_id: str
    title: str
    instruments: str
    event_at: datetime
    timezone_name: str
    source_url: str
    kind: str = "exchange"
    note: str = ""
    event_kind: str = "open"

    def __post_init__(self) -> None:
        require_aware(self.event_at)
        ZoneInfo(self.timezone_name)
        if self.event_at.second or self.event_at.microsecond:
            raise ValueError("session boundary must have minute precision")
        if not self.session_id or not self.title or not self.instruments:
            raise ValueError("session identity and description are required")
        if self.kind not in {"exchange", "fx_center"} or self.event_kind not in SESSION_EVENT_KINDS:
            raise ValueError("unknown session kind")
        if not self.source_url.startswith("https://"):
            raise ValueError("session source must use HTTPS")

    def is_due(self, now: datetime) -> bool:
        require_aware(now)
        return self.event_at <= now < self.event_at + timedelta(minutes=1)


def session_boundary_freshness(*, event_at: datetime, evaluated_at: datetime) -> DeliveryFreshnessDecision:
    require_aware(event_at)
    require_aware(evaluated_at)
    age = (evaluated_at - event_at).total_seconds()
    allowed = 0 <= age < 60 and not (event_at.second or event_at.microsecond)
    return DeliveryFreshnessDecision(
        allowed, "session_boundary_fresh" if allowed else "session_boundary_outside_minute",
        age, 60, event_at.astimezone(timezone.utc).date().isoformat(),
        evaluated_at.astimezone(timezone.utc).date().isoformat(),
    )


def require_aware(moment: datetime) -> None:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("session time must be timezone-aware")
