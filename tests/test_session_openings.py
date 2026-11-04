"""Calendar regressions and strict realtime notification boundaries."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from tinvest_signal_engine.adapters.delivery_recovery import QueuedDeliveryRecoveryAdapter
from tinvest_signal_engine.adapters.postgres_reliability import signal_record_to_dict
from tinvest_signal_engine.adapters.session_calendar import ExchangeSessionCalendar
from tinvest_signal_engine.adapters.session_open_message import opening_payload, render_session_open_html
from tinvest_signal_engine.adapters.session_open_publisher import PostgresSessionOpenPublisher
from tinvest_signal_engine.application.delivery import DurableDeliveryWorker
from tinvest_signal_engine.application.session_open import AnnounceSessionOpenings
from tinvest_signal_engine.config import load_secret
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask
from tinvest_signal_engine.domain.session_open import SessionOpening, session_open_freshness
from tinvest_signal_engine.sinks import TelegramAlertSink


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]


def moment(value):
    return datetime.fromisoformat(value)


@pytest.fixture(scope="module")
def calendar():
    return ExchangeSessionCalendar(json.loads((ROOT / "conf/market-sessions.json").read_text()))


def find(calendar, session_id, day):
    return [item for item in calendar.openings(now=moment(day + "T12:00:00+00:00"))
            if item.session_id == session_id]


@pytest.mark.parametrize("day,expected", [
    ("2026-10-23", "2026-10-23T13:30:00+00:00"),
    ("2026-10-30", "2026-10-30T13:30:00+00:00"),
    ("2026-11-04", "2026-11-04T14:30:00+00:00"),
])
def test_us_cash_open_tracks_us_dst_independently_of_europe(calendar, day, expected):
    assert find(calendar, "us_stocks", day)[0].opens_at == moment(expected)


@pytest.mark.parametrize("day,hour", [("2026-10-23", 7), ("2026-10-30", 8)])
def test_london_and_xetra_shift_together(calendar, day, hour):
    for name in ("london", "frankfurt", "fx_london"):
        assert find(calendar, name, day)[0].opens_at.hour == hour


@pytest.mark.parametrize("session,day", [
    ("us_stocks", "2026-11-26"), ("tokyo", "2026-11-03"),
    ("china_stocks", "2026-10-01"), ("shfe_day", "2026-10-07"),
    ("shfe_night", "2026-09-30"), ("shfe_night", "2026-02-13"),
    ("london", "2026-12-25"), ("us_stocks", "2026-11-07"),
    ("cme_resources", "2026-11-06"), ("moex", "2026-11-28"),
    ("moex", "2026-12-31"),
])
def test_no_false_open_on_exchange_closure(calendar, session, day):
    assert not find(calendar, session, day)


def test_shfe_friday_evening_belongs_to_monday_without_sunday_alert(calendar):
    assert find(calendar, "shfe_night", "2026-11-06")[0].opens_at == moment("2026-11-06T13:00:00+00:00")
    assert not find(calendar, "shfe_night", "2026-11-08")
    assert find(calendar, "shfe_day", "2026-10-08")  # First day after Golden Week.
    assert find(calendar, "shfe_night", "2026-10-08")


def test_cme_sunday_open_has_actual_sunday_timestamp(calendar):
    assert find(calendar, "cme_resources", "2026-11-08")[0].opens_at == moment("2026-11-08T23:00:00+00:00")


def test_moex_new_hours_and_holiday_weekend_overrides(calendar):
    assert find(calendar, "moex", "2026-11-05")[0].opens_at == moment("2026-11-05T06:00:00+00:00")
    holiday = find(calendar, "moex", "2026-11-04")[0]
    assert holiday.opens_at == moment("2026-11-04T06:50:00+00:00")
    assert "Дополнительная" in holiday.note
    assert find(calendar, "moex", "2026-11-07")[0].opens_at.hour == 6
    assert find(calendar, "moex", "2026-11-07")[0].opens_at.minute == 50


def test_sydney_fx_monday_does_not_open_before_global_weekly_market(calendar):
    opening = next(item for item in calendar.openings(now=moment("2026-11-08T22:30:00+00:00"))
                   if item.session_id == "fx_sydney")  # Monday in Sydney.
    assert opening.opens_at == moment("2026-11-08T22:00:00+00:00")
    assert opening.kind == "fx_center"


def test_unknown_calendar_coverage_blocks_only_affected_sessions(calendar):
    openings = calendar.openings(now=moment("2027-01-04T12:00:00+00:00"))
    assert "us_stocks" in {item.session_id for item in openings}
    assert "moex" in calendar.unavailable
    assert "china_stocks" in calendar.unavailable
    assert not {"moex", "shfe_day", "shfe_night", "china_stocks"} & {item.session_id for item in openings}


@pytest.fixture
def opening(calendar):
    return find(calendar, "china_stocks", "2026-11-05")[0]


@pytest.mark.parametrize("seconds,due", [(-.001, False), (0, True), (59.999, True), (60, False), (3600, False)])
def test_opening_minute_is_half_open(opening, seconds, due):
    now = opening.opens_at + timedelta(seconds=seconds)
    assert opening.is_due(now) is due
    assert session_open_freshness(opens_at=opening.opens_at, evaluated_at=now).allow_external_delivery is due


def test_naive_time_and_non_minute_opening_are_rejected(opening):
    with pytest.raises(ValueError, match="timezone-aware"):
        opening.is_due(datetime(2026, 11, 5))
    with pytest.raises(ValueError, match="minute precision"):
        replace(opening, opens_at=opening.opens_at + timedelta(seconds=1))


def test_only_current_minute_gets_published(opening):
    sent = []
    calendar = SimpleNamespace(openings=lambda **_: (opening, replace(opening, opens_at=opening.opens_at-timedelta(hours=1))))
    publisher = SimpleNamespace(publish=lambda item, **_: sent.append(item) or True)
    assert AnnounceSessionOpenings(calendar=calendar, publisher=publisher).execute(now=opening.opens_at) == 1
    assert sent == [opening]


def prepared(opening):
    store = SimpleNamespace(persist_session_open_once=lambda event, alert: SimpleNamespace(replayed=False))
    saved = []
    def persist(event, alert):
        saved.append((event, alert))
        return SimpleNamespace(replayed=False)
    store.persist_session_open_once = persist
    publisher = PostgresSessionOpenPublisher(store=store, target=DeliveryTarget("telegram", "test"), display_timezone="Europe/Istanbul")
    publisher.publish(opening, generated_at=opening.opens_at)
    return saved[0]


def test_recovery_checks_source_time_and_version(opening):
    event, alert = prepared(opening)
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", signal_record_to_dict(alert.signal), 1)
    adapter = QueuedDeliveryRecoveryAdapter(None, clock=lambda: opening.opens_at)
    assert adapter.evaluate(task).allow_external_delivery
    payload = task.payload | {"source_event_at": (opening.opens_at-timedelta(minutes=1)).isoformat()}
    assert adapter.evaluate(replace(task, payload=payload)).reason_code == "session_open_invalid"
    assert not adapter.evaluate(replace(task, payload=task.payload | {"payload": {}})).allow_external_delivery


@pytest.mark.parametrize("invalid_time", [None, 1, {}, "2026-11-05T01:30:00", "invalid"])
def test_invalid_opening_timestamp_cannot_crash_delivery_worker(opening, invalid_time):
    _, alert = prepared(opening)
    payload = signal_record_to_dict(alert.signal)
    payload["source_event_at"] = invalid_time
    payload["payload"] = payload["payload"] | {"opens_at": invalid_time}
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", payload, 1)
    decision = QueuedDeliveryRecoveryAdapter(None, clock=lambda: opening.opens_at).evaluate(task)
    assert decision.reason_code == "session_open_invalid"


def test_transient_retry_expires_when_opening_minute_ends(opening):
    _, alert = prepared(opening)
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", signal_record_to_dict(alert.signal), 2,
                        previous_error_code="delivery_network_error")
    failed = []
    queue = SimpleNamespace(claim=lambda **_: task, mark_failed=lambda *_, **kwargs: failed.append(kwargs))
    sender = SimpleNamespace(send=lambda _: pytest.fail("expired opening must never be sent"))
    now = opening.opens_at + timedelta(minutes=1)
    worker = DurableDeliveryWorker(queue=queue, sender=sender,
        metrics=SimpleNamespace(delivery_attempted=lambda **_: None), clock=lambda: now,
        lease_seconds=30, maximum_attempts=3, retry_base_seconds=5, retry_maximum_seconds=60,
        recovery_guard=QueuedDeliveryRecoveryAdapter(None, clock=lambda: now))
    assert worker.run_once().outcome == "suppressed_stale"
    assert failed[0]["dead_letter"]
    assert failed[0]["reason_code"] == "session_open_outside_minute"


def test_real_telegram_sender_formats_short_calendar_alert_without_trading_scores(opening):
    _, alert = prepared(opening)
    from tinvest_signal_engine.models import TriggerSignal
    requests = []
    def handle(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        sink = TelegramAlertSink(bot_token="test", chat_id="test", client=client)
        sink.send(TriggerSignal.from_dict(signal_record_to_dict(alert.signal)))
    assert len(requests) == 1
    text = requests[0]["text"]
    assert "Китай" in text and "09:30" in text and "04:30" in text
    assert "Стамбул" in text
    assert all(word not in text for word in ("Качество", "Вероятность", "Терминал", "z-score"))


def test_message_escapes_market_names(opening):
    payload = opening_payload(replace(opening, title="<market>"), display_timezone="Europe/Istanbul")
    assert "&lt;market&gt;" in render_session_open_html(payload)


def test_session_worker_has_only_destination_and_database_secrets():
    env = {key: "value" for key in ("TINVEST_TOKEN", "TELEGRAM_BOT_TOKEN", "POSTGRES_PASSWORD", "TELEGRAM_CHAT_ID")}
    assert load_secret("POSTGRES_PASSWORD", service_name="session_open_worker", environ=env) == "value"
    assert load_secret("TELEGRAM_CHAT_ID", service_name="session_open_worker", environ=env) == "value"
    for key in ("TINVEST_TOKEN", "TELEGRAM_BOT_TOKEN"):
        assert load_secret(key, service_name="session_open_worker", environ=env) is None
