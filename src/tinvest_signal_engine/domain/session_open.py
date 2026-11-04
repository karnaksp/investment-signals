"""Scheduled market openings; an alert is useful only during its opening minute."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessDecision


SESSION_OPEN_SIGNAL_TYPE = "market_session_open"
SESSION_OPEN_VERSION = "market_sessions_v1"


@dataclass(frozen=True, slots=True)
class SessionOpening:
    session_id: str
    title: str
    instruments: str
    opens_at: datetime
    timezone_name: str
    source_url: str
    kind: str = "exchange"
    note: str = ""

    def __post_init__(self) -> None:
        require_aware(self.opens_at)
        ZoneInfo(self.timezone_name)
        if self.opens_at.second or self.opens_at.microsecond:
            raise ValueError("opening must have minute precision")
        if not self.session_id or not self.title or not self.instruments:
            raise ValueError("session identity and description are required")
        if self.kind not in {"exchange", "fx_center"}:
            raise ValueError("unknown session kind")
        if not self.source_url.startswith("https://"):
            raise ValueError("session source must use HTTPS")

    def is_due(self, now: datetime) -> bool:
        require_aware(now)
        return self.opens_at <= now < self.opens_at + timedelta(minutes=1)


def session_open_freshness(*, opens_at: datetime, evaluated_at: datetime) -> DeliveryFreshnessDecision:
    require_aware(opens_at)
    require_aware(evaluated_at)
    age = (evaluated_at - opens_at).total_seconds()
    allowed = 0 <= age < 60 and not (opens_at.second or opens_at.microsecond)
    return DeliveryFreshnessDecision(
        allowed, "session_open_fresh" if allowed else "session_open_outside_minute",
        age, 60, opens_at.astimezone(timezone.utc).date().isoformat(),
        evaluated_at.astimezone(timezone.utc).date().isoformat(),
    )


def require_aware(moment: datetime) -> None:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("session time must be timezone-aware")
