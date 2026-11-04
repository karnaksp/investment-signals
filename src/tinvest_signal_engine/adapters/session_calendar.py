"""Translate exchange schedules into openings, closes and lunch boundaries."""

from datetime import date, datetime, time, timedelta, timezone
import logging
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as calendars

from tinvest_signal_engine.domain.session_open import SessionBoundary, require_aware


logger = logging.getLogger(__name__)


class ExchangeSessionCalendar:
    def __init__(self, policy: dict) -> None:
        if policy.get("version") != 2 or type(policy.get("enabled")) is not bool:
            raise ValueError("session policy requires version 2 and explicit enabled")
        ZoneInfo(policy["display_timezone"])
        self.enabled, self.display_timezone = policy["enabled"], policy["display_timezone"]
        self._specs, self._moex = policy["sessions"], policy["moex_2026"]
        if len({s["id"] for s in self._specs}) != len(self._specs):
            raise ValueError("duplicate session id")
        self._calendars = {}
        for spec in self._specs:
            ZoneInfo(spec["timezone"])
            if spec.get("mode", "calendar") not in {"calendar", "fixed", "fx_center", "china_night", "moex", "ice_brent"}:
                raise ValueError("unknown session calendar mode")
            if spec["calendar"] != "CUSTOM":
                cal = calendars.get_calendar(spec["calendar"])
                # exchange_calendars wrappers retain obsolete KRX lunch hours.
                for key in list(cal.discontinued_market_times):
                    cal.remove_time(key)
                for key, value in spec.get("regular_times", {}).items():
                    if key in cal.regular_market_times:
                        cal.change_time(key, time.fromisoformat(value))
                    else:
                        cal.add_time(key, time.fromisoformat(value))
                self._calendars[spec["id"]] = cal
        self._forex = calendars.get_calendar("FOREX")
        self._cache: dict[tuple[str, date], tuple[SessionBoundary, ...]] = {}
        self._failed: set[tuple[str, date]] = set()
        self.unavailable: tuple[str, ...] = ()

    def boundaries(self, *, now: datetime) -> tuple[SessionBoundary, ...]:
        require_aware(now)
        if not self.enabled:
            return ()
        self._cache = {key: value for key, value in self._cache.items() if abs((key[1]-now.date()).days) <= 7}
        self._failed.intersection_update(self._cache)
        result, unavailable = [], []
        for spec in self._specs:
            day = now.astimezone(ZoneInfo(spec["timezone"])).date()
            key = spec["id"], day
            if key not in self._cache:
                try:
                    self._cache[key] = self._day_boundaries(spec, day)
                except Exception as error:
                    self._cache[key] = ()
                    self._failed.add(key)
                    logger.error("Session calendar unavailable: session=%s reason=%s", spec["id"], type(error).__name__)
            if key in self._failed:
                unavailable.append(spec["id"])
            result.extend(self._cache[key])
        self.unavailable = tuple(unavailable)
        # Dedupe equivalent boundaries when querying overlapping overnight windows.
        unique = {(e.session_id, e.event_kind, e.event_at): e for e in result}
        return tuple(sorted(unique.values(), key=lambda e: (e.event_at, e.event_kind, e.session_id)))

    def _valid_day(self, spec: dict, day: date) -> bool:
        if day.isoformat() in spec.get("date_overrides", {}):
            return True
        if day.isoformat() in spec.get("closed_dates", []):
            return False
        if spec["calendar"] == "CUSTOM":
            return day.weekday() in spec.get("weekdays", [0, 1, 2, 3, 4])
        return not self._calendars[spec["id"]].valid_days(day, day).empty

    def _day_boundaries(self, spec: dict, day: date) -> tuple[SessionBoundary, ...]:
        if ((spec.get("valid_through") and day > date.fromisoformat(spec["valid_through"]))
                or (spec.get("valid_from") and day < date.fromisoformat(spec["valid_from"]))):
            raise ValueError("exchange calendar is outside verified coverage")
        zone = ZoneInfo(spec["timezone"])
        mode = spec.get("mode", "calendar")
        result = []

        def emit(kind, at, note=""):
            if at.astimezone(zone).date() == day:
                result.append(SessionBoundary(spec["id"], spec["title"], spec["instruments"],
                    at.astimezone(timezone.utc), spec["timezone"], spec["source_url"],
                    "fx_center" if mode == "fx_center" else "exchange", note or spec.get("note", ""), kind))

        def window(start, end, note=""):
            if end <= start:
                raise ValueError("session close must follow its open")
            emit("open", start, note)
            emit("close", end, note)

        if mode == "calendar":
            # Include prior labels for overnight closes and future labels for
            # Sunday futures opens. Filter actual timestamps, never label dates.
            cal = self._calendars[spec["id"]]
            frame = cal.schedule(day-timedelta(days=7), day+timedelta(days=5))
            for label, row in frame.iterrows():
                if label.date().isoformat() in spec.get("closed_dates", []):
                    continue
                start, end = row.market_open.to_pydatetime(), row.market_close.to_pydatetime()
                for key, value in spec.get("weekday_times", {}).get(str(label.date().weekday()), {}).items():
                    row[key] = pd.Timestamp(datetime.combine(label.date(), time.fromisoformat(value), zone))
                override = spec.get("date_overrides", {}).get(label.date().isoformat())
                if override:
                    continue  # Explicit windows, including special Sunday opens, below.
                window(start, end)
                for a, b in (("break_start", "break_end"), ("pause_start", "pause_end")):
                    if a in frame and b in frame:
                        left, right = row[a], row[b]
                        if not pd.isna(left) and not pd.isna(right) and start < left < right < end:
                            emit("break_start", left.to_pydatetime())
                            emit("break_end", right.to_pydatetime())
            for raw_day, override in spec.get("date_overrides", {}).items():
                special = date.fromisoformat(raw_day)
                window(datetime.combine(special, time.fromisoformat(override["open"]), zone),
                       datetime.combine(special, time.fromisoformat(override["close"]), zone), override.get("note", ""))
        elif mode == "moex":
            if day.year != 2026:
                raise ValueError("MOEX calendar needs annual renewal")
            raw = day.isoformat()
            if raw in self._moex["closed_dates"]:
                return ()
            special = day.weekday() >= 5 or raw in self._moex["holiday_sessions"]
            if special and raw in self._moex["weekend_closed_dates"]:
                return ()
            part = spec.get("part", "main")
            if special and part != "main":
                return ()
            hours = {"morning": ("06:50", "09:00"), "main": ("09:00", "19:00"), "evening": ("19:00", "23:50")}
            a, b = hours[part]
            note = spec.get("note", "")
            if special:
                a = "09:50"
                if raw == "2026-11-04" and spec.get("segment") == "futures":
                    b = "23:50"
                note = "Дополнительная сессия выходного/праздничного дня."
            window(datetime.combine(day, time.fromisoformat(a), zone), datetime.combine(day, time.fromisoformat(b), zone), note)
        else:
            for start_day in (day-timedelta(days=1), day):
                if not self._valid_day(spec, start_day):
                    continue
                a, b = spec["local_open"], spec["local_close"]
                note = spec.get("note", "")
                override = spec.get("date_overrides", {}).get(start_day.isoformat())
                if override:
                    a, b, note = override["open"], override["close"], override.get("note", note)
                if mode == "china_night":
                    next_day = start_day+timedelta(days=1)
                    while next_day.weekday() >= 5:
                        next_day += timedelta(days=1)
                    if not self._valid_day(spec, next_day):
                        continue
                start = datetime.combine(start_day, time.fromisoformat(a), zone)
                end = datetime.combine(start_day, time.fromisoformat(b), zone)
                if mode == "ice_brent" and start_day.weekday() == 0:
                    start = datetime.combine(start_day-timedelta(days=1), time(23), zone)
                elif end <= start:
                    end += timedelta(days=1)
                if mode == "fx_center":
                    fx = self._forex.schedule(start_day-timedelta(days=2), start_day+timedelta(days=2))
                    overlaps = [(max(start, r.market_open.to_pydatetime()), min(end, r.market_close.to_pydatetime()))
                                for r in fx.itertuples() if r.market_close.to_pydatetime() > start and r.market_open.to_pydatetime() < end]
                    if not overlaps:
                        continue
                    start, end = min(a for a, _ in overlaps), max(b for _, b in overlaps)
                    if start >= end:
                        continue
                window(start, end, note)
            if mode == "ice_brent" and day.weekday() == 6:
                monday = day+timedelta(days=1)
                if self._valid_day(spec, monday):
                    emit("open", datetime.combine(day, time(23), zone))
        return tuple(result)
