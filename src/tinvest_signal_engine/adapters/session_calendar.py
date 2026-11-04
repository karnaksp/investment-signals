"""Map exchange-calendar records to application DTOs. Never guess past coverage."""

from datetime import date, datetime, time, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

import pandas_market_calendars as calendars

from tinvest_signal_engine.domain.session_open import SessionOpening, require_aware


logger = logging.getLogger(__name__)


class ExchangeSessionCalendar:
    def __init__(self, policy: dict) -> None:
        if policy.get("version") != 1 or type(policy.get("enabled")) is not bool:
            raise ValueError("session policy requires version 1 and explicit enabled")
        ZoneInfo(policy["display_timezone"])
        self.enabled = policy["enabled"]
        self.display_timezone = policy["display_timezone"]
        self._specs = policy["sessions"]
        if len({spec["id"] for spec in self._specs}) != len(self._specs):
            raise ValueError("duplicate session id")
        self._moex = policy["moex_2026"]
        self._calendars = {
            spec["calendar"]: calendars.get_calendar(spec["calendar"])
            for spec in self._specs if spec["calendar"] != "MOEX"
        }
        self._calendars["FOREX"] = calendars.get_calendar("FOREX")
        self._cache: dict[tuple[str, date], tuple[SessionOpening, ...]] = {}
        self.unavailable: tuple[str, ...] = ()

    def openings(self, *, now: datetime) -> tuple[SessionOpening, ...]:
        require_aware(now)
        if not self.enabled:
            return ()
        # Keep a bounded cache, including known closed dates. No network or
        # calendar generation belongs in the time-critical delivery path.
        self._cache = {key: value for key, value in self._cache.items()
                       if abs((key[1] - now.date()).days) <= 7}
        result, unavailable = [], []
        for spec in self._specs:
            local_day = now.astimezone(ZoneInfo(spec["timezone"])).date()
            key = spec["id"], local_day
            try:
                if key not in self._cache:
                    self._cache[key] = self._day_openings(spec, local_day)
                result.extend(self._cache[key])
            except Exception as error:
                unavailable.append(spec["id"])
                logger.error("Session calendar unavailable: session=%s reason=%s", spec["id"], type(error).__name__)
        self.unavailable = tuple(unavailable)
        return tuple(sorted(result, key=lambda item: (item.opens_at, item.session_id)))

    def _day_openings(self, spec: dict, day: date) -> tuple[SessionOpening, ...]:
        if spec.get("valid_through") and day > date.fromisoformat(spec["valid_through"]):
            raise ValueError("exchange calendar needs annual renewal")
        mode = spec.get("mode", "calendar")
        zone = ZoneInfo(spec["timezone"])
        note = spec.get("note", "")
        if mode == "moex":
            if day.year != 2026:
                raise ValueError("MOEX calendar needs annual renewal")
            if day.isoformat() in self._moex["closed_dates"]:
                return ()
            special = day.weekday() >= 5 or day.isoformat() in self._moex["holiday_sessions"]
            if special and day.isoformat() in self._moex["weekend_closed_dates"]:
                return ()
            at = time(10) if day < date(2026, 9, 14) else time(9)
            if special:
                at = time(9, 50)
                note = "Дополнительная сессия выходного/праздничного дня; доступны допущенные акции и фьючерсы."
            else:
                note = "Основная сессия. Для части акций сначала проводится аукцион открытия."
            times = [datetime.combine(day, at, zone)]
        elif mode in {"fx_center", "china_night"}:
            calendar = self._calendars[spec["calendar"]]
            if calendar.valid_days(day, day).empty:
                return ()
            at = datetime.combine(day, time.fromisoformat(spec["local_open"]), zone)
            if mode == "china_night":
                next_day = day + timedelta(days=1)
                while next_day.weekday() >= 5:
                    next_day += timedelta(days=1)
                if calendar.valid_days(next_day, next_day).empty:
                    return ()  # No night trading immediately before a holiday.
            else:
                fx = self._calendars["FOREX"].schedule(day - timedelta(days=2), day + timedelta(days=2))
                candidates = [max(at, row.market_open.to_pydatetime())
                              for row in fx.itertuples()
                              if row.market_close.to_pydatetime() > at
                              and row.market_open.to_pydatetime() < at + timedelta(hours=3)]
                if not candidates:
                    return ()
                at = min(candidates)  # Sydney cannot open before the weekly FX open.
            times = [at]
        elif mode == "calendar":
            # Futures session labels refer to the NEXT trading day. Query ahead
            # and filter actual timestamps, preserving the Sunday evening open.
            frame = self._calendars[spec["calendar"]].schedule(day, day + timedelta(days=5))
            times = [row.market_open.to_pydatetime() for row in frame.itertuples()
                     if row.market_open.to_pydatetime().astimezone(zone).date() == day]
        else:
            raise ValueError("unknown session calendar mode")
        return tuple(SessionOpening(
            session_id=spec["id"], title=spec["title"], instruments=spec["instruments"],
            opens_at=at.astimezone(timezone.utc), timezone_name=spec["timezone"],
            source_url=spec["source_url"], kind="fx_center" if mode == "fx_center" else "exchange", note=note,
        ) for at in times)
