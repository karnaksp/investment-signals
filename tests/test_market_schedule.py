from datetime import datetime, timezone

import pytest

from tinvest_signal_engine.domain.market_schedule import MarketSchedule


UTC = timezone.utc


def _utc(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 7, 29, hour, minute, tzinfo=UTC)


def test_collection_window_is_0700_to_2300_moscow() -> None:
    schedule = MarketSchedule()

    assert not schedule.is_collection_active(_utc(3, 59))
    assert schedule.is_collection_active(_utc(4, 0))
    assert schedule.is_collection_active(_utc(19, 59))
    assert not schedule.is_collection_active(_utc(20, 0))


def test_signal_window_excludes_open_and_close_buffers() -> None:
    schedule = MarketSchedule()

    assert not schedule.is_signal_emission_active(_utc(4, 14))
    assert schedule.is_signal_emission_active(_utc(4, 15))
    assert schedule.is_signal_emission_active(_utc(19, 44))
    assert not schedule.is_signal_emission_active(_utc(19, 45))


def test_next_start_rolls_to_next_moscow_day_after_close() -> None:
    schedule = MarketSchedule()

    next_start = schedule.next_collection_start(_utc(21, 0))

    assert next_start.isoformat() == "2026-07-30T07:00:00+03:00"
    assert schedule.seconds_until_collection_start(_utc(21, 0)) == 7 * 60 * 60


def test_invalid_signal_window_is_rejected() -> None:
    with pytest.raises(ValueError, match="inside collection"):
        MarketSchedule.from_strings(signal_start="06:59")
