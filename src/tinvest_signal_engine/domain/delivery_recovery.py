"""Domain policy that keeps catch-up processing out of realtime delivery."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


DELIVERY_EVENT_FRESH = "delivery_event_fresh"
DELIVERY_EVENT_AGE_EXCEEDED = "delivery_event_age_exceeded"
DELIVERY_EVENT_CROSSED_SESSION = "delivery_event_crossed_session"
DELIVERY_EVENT_TIME_UNAVAILABLE = "delivery_event_time_unavailable"
DELIVERY_EVENT_TIME_IN_FUTURE = "delivery_event_time_in_future"


@dataclass(frozen=True, slots=True)
class DeliveryFreshnessDecision:
    allow_external_delivery: bool
    reason_code: str
    event_age_seconds: float | None
    maximum_event_age_seconds: int
    source_session: str | None
    evaluated_session: str


@dataclass(frozen=True, slots=True)
class DeliveryFreshnessPolicy:
    """Decide whether an event is still timely enough for an external alert.

    Signals rejected here remain valid local observations. Only external
    realtime delivery is denied. A local market date is used as the session
    fence so a previous-session event can never become a new-session alert,
    even when clocks or configured age windows are unusually permissive.
    """

    maximum_event_age_seconds: int
    session_timezone: str = "Europe/Moscow"
    maximum_future_skew_seconds: int = 5

    def __post_init__(self) -> None:
        if self.maximum_event_age_seconds <= 0:
            raise ValueError("maximum_event_age_seconds must be positive")
        if self.maximum_future_skew_seconds < 0:
            raise ValueError("maximum_future_skew_seconds must be non-negative")
        ZoneInfo(self.session_timezone)

    def decide(
        self,
        *,
        source_event_at: datetime | None,
        evaluated_at: datetime,
    ) -> DeliveryFreshnessDecision:
        _require_aware(evaluated_at, field_name="evaluated_at")
        evaluated_session = self._session_key(evaluated_at)
        if source_event_at is None:
            return DeliveryFreshnessDecision(
                allow_external_delivery=False,
                reason_code=DELIVERY_EVENT_TIME_UNAVAILABLE,
                event_age_seconds=None,
                maximum_event_age_seconds=self.maximum_event_age_seconds,
                source_session=None,
                evaluated_session=evaluated_session,
            )
        _require_aware(source_event_at, field_name="source_event_at")
        source_session = self._session_key(source_event_at)
        age_seconds = (evaluated_at - source_event_at).total_seconds()
        if age_seconds < -self.maximum_future_skew_seconds:
            return DeliveryFreshnessDecision(
                allow_external_delivery=False,
                reason_code=DELIVERY_EVENT_TIME_IN_FUTURE,
                event_age_seconds=age_seconds,
                maximum_event_age_seconds=self.maximum_event_age_seconds,
                source_session=source_session,
                evaluated_session=evaluated_session,
            )
        if source_session != evaluated_session:
            return DeliveryFreshnessDecision(
                allow_external_delivery=False,
                reason_code=DELIVERY_EVENT_CROSSED_SESSION,
                event_age_seconds=max(0.0, age_seconds),
                maximum_event_age_seconds=self.maximum_event_age_seconds,
                source_session=source_session,
                evaluated_session=evaluated_session,
            )
        if age_seconds > self.maximum_event_age_seconds:
            return DeliveryFreshnessDecision(
                allow_external_delivery=False,
                reason_code=DELIVERY_EVENT_AGE_EXCEEDED,
                event_age_seconds=age_seconds,
                maximum_event_age_seconds=self.maximum_event_age_seconds,
                source_session=source_session,
                evaluated_session=evaluated_session,
            )
        return DeliveryFreshnessDecision(
            allow_external_delivery=True,
            reason_code=DELIVERY_EVENT_FRESH,
            event_age_seconds=max(0.0, age_seconds),
            maximum_event_age_seconds=self.maximum_event_age_seconds,
            source_session=source_session,
            evaluated_session=evaluated_session,
        )

    def _session_key(self, at: datetime) -> str:
        return at.astimezone(ZoneInfo(self.session_timezone)).date().isoformat()


def _require_aware(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
