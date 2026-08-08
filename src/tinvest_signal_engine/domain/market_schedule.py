"""Daily market collection and signal-emission windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


@dataclass(frozen=True, slots=True)
class MarketSchedule:
    """Define when market data is collected and when signals may be emitted."""

    timezone_name: str = "Europe/Moscow"
    collection_start: time = time(7, 0)
    collection_end: time = time(23, 0)
    signal_start: time = time(7, 15)
    signal_end: time = time(22, 45)

    def __post_init__(self) -> None:
        ZoneInfo(self.timezone_name)
        if not self.collection_start < self.collection_end:
            raise ValueError("collection window must be within one calendar day")
        if not (
            self.collection_start
            <= self.signal_start
            < self.signal_end
            <= self.collection_end
        ):
            raise ValueError("signal window must be inside collection window")

    @classmethod
    def from_strings(
        cls,
        *,
        timezone_name: str = "Europe/Moscow",
        collection_start: str = "07:00",
        collection_end: str = "23:00",
        signal_start: str = "07:15",
        signal_end: str = "22:45",
    ) -> "MarketSchedule":
        return cls(
            timezone_name=timezone_name,
            collection_start=_parse_time(collection_start),
            collection_end=_parse_time(collection_end),
            signal_start=_parse_time(signal_start),
            signal_end=_parse_time(signal_end),
        )

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    def is_collection_active(self, moment: datetime) -> bool:
        local = self._local(moment)
        return self.collection_start <= local.time() < self.collection_end

    def is_signal_emission_active(self, moment: datetime) -> bool:
        local = self._local(moment)
        return self.signal_start <= local.time() < self.signal_end

    def next_collection_start(self, moment: datetime) -> datetime:
        local = self._local(moment)
        candidate = datetime.combine(
            local.date(),
            self.collection_start,
            tzinfo=self.timezone,
        )
        if local >= candidate:
            candidate += timedelta(days=1)
        return candidate

    def current_collection_end(self, moment: datetime) -> datetime:
        local = self._local(moment)
        return datetime.combine(
            local.date(),
            self.collection_end,
            tzinfo=self.timezone,
        )

    def seconds_until_collection_start(self, moment: datetime) -> float:
        return max(
            0.0,
            (self.next_collection_start(moment) - self._local(moment)).total_seconds(),
        )

    def seconds_until_collection_end(self, moment: datetime) -> float:
        if not self.is_collection_active(moment):
            return 0.0
        return max(
            0.0,
            (self.current_collection_end(moment) - self._local(moment)).total_seconds(),
        )

    def _local(self, moment: datetime) -> datetime:
        if moment.tzinfo is None or moment.utcoffset() is None:
            raise ValueError("moment must be timezone-aware")
        return moment.astimezone(self.timezone)


def _parse_time(value: str) -> time:
    try:
        parsed = time.fromisoformat(value.strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid market schedule time: {value!r}") from exc
    if parsed.tzinfo is not None:
        raise ValueError("market schedule times must not include a timezone")
    if parsed.second or parsed.microsecond:
        raise ValueError("market schedule times must have minute precision")
    return parsed
