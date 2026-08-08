from datetime import datetime
from zoneinfo import ZoneInfo

from tinvest_signal_engine.services.morning_retracement_signal_worker import (
    _morning_session_deadline_passed,
    _morning_session_started,
    _seconds_until_next_morning_session,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def test_worker_waits_before_morning_and_wakes_at_seven() -> None:
    now = datetime(2026, 7, 30, 6, 45, tzinfo=MOSCOW)

    assert _morning_session_started(now) is False
    assert _morning_session_deadline_passed(now) is False
    assert _seconds_until_next_morning_session(now) == 15 * 60


def test_worker_finishes_outcomes_after_deadline_then_waits_for_next_day() -> None:
    now = datetime(2026, 7, 30, 11, 1, tzinfo=MOSCOW)

    assert _morning_session_started(now) is True
    assert _morning_session_deadline_passed(now) is True
    assert _seconds_until_next_morning_session(now) == (19 * 60 + 59) * 60


def test_worker_skips_weekend_when_calculating_next_session() -> None:
    friday = datetime(2026, 7, 31, 23, 0, tzinfo=MOSCOW)

    assert _seconds_until_next_morning_session(friday) == 56 * 60 * 60
