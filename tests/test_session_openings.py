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
from tinvest_signal_engine.adapters.session_open_message import boundary_payload, render_session_boundary_html
from tinvest_signal_engine.adapters.session_open_publisher import PostgresSessionBoundaryPublisher
from tinvest_signal_engine.application.delivery import DurableDeliveryWorker
from tinvest_signal_engine.application.session_open import AnnounceSessionBoundaries
from tinvest_signal_engine.config import load_secret
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask
from tinvest_signal_engine.domain.session_open import SessionBoundary, session_boundary_freshness
from tinvest_signal_engine.sinks import TelegramAlertSink


UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]


def moment(value):
    return datetime.fromisoformat(value)


@pytest.fixture(scope="module")
def calendar():
    return ExchangeSessionCalendar(json.loads((ROOT / "conf/market-sessions.json").read_text()))


def find(calendar, session_id, day, event_kind="open"):
    return [item for item in calendar.boundaries(now=moment(day + "T12:00:00+00:00"))
            if item.session_id == session_id and item.event_kind == event_kind]


@pytest.mark.parametrize("day,expected", [
    ("2026-10-23", "2026-10-23T13:30:00+00:00"),
    ("2026-10-30", "2026-10-30T13:30:00+00:00"),
    ("2026-11-04", "2026-11-04T14:30:00+00:00"),
])
def test_us_cash_open_tracks_us_dst_independently_of_europe(calendar, day, expected):
    assert find(calendar, "us_stocks", day)[0].event_at == moment(expected)


@pytest.mark.parametrize("day,hour", [("2026-10-23", 7), ("2026-10-30", 8)])
def test_london_and_xetra_shift_together(calendar, day, hour):
    for name in ("london", "frankfurt", "fx_london"):
        assert find(calendar, name, day)[0].event_at.hour == hour


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
    assert find(calendar, "shfe_night", "2026-11-06")[0].event_at == moment("2026-11-06T13:00:00+00:00")
    assert not find(calendar, "shfe_night", "2026-11-08")
    assert find(calendar, "shfe_day", "2026-10-08")  # First day after Golden Week.
    assert find(calendar, "shfe_night", "2026-10-08")


def test_cme_sunday_open_has_actual_sunday_timestamp(calendar):
    assert find(calendar, "cme_resources", "2026-11-08")[0].event_at == moment("2026-11-08T23:00:00+00:00")


def test_moex_new_hours_and_holiday_weekend_overrides(calendar):
    assert find(calendar, "moex", "2026-11-05")[0].event_at == moment("2026-11-05T06:00:00+00:00")
    holiday = find(calendar, "moex", "2026-11-04")[0]
    assert holiday.event_at == moment("2026-11-04T06:50:00+00:00")
    assert "Дополнительная" in holiday.note
    assert find(calendar, "moex", "2026-11-07")[0].event_at.hour == 6
    assert find(calendar, "moex", "2026-11-07")[0].event_at.minute == 50


def test_sydney_fx_monday_does_not_open_before_global_weekly_market(calendar):
    opening = next(item for item in calendar.boundaries(now=moment("2026-11-08T22:30:00+00:00"))
                   if item.session_id == "fx_sydney" and item.event_kind == "open")  # Monday in Sydney.
    assert opening.event_at == moment("2026-11-08T22:00:00+00:00")
    assert opening.kind == "fx_center"


def test_unknown_calendar_coverage_blocks_only_affected_sessions(calendar):
    openings = calendar.boundaries(now=moment("2027-01-04T12:00:00+00:00"))
    assert "us_stocks" in {item.session_id for item in openings}
    assert "moex" in calendar.unavailable
    assert "china_stocks" in calendar.unavailable
    assert not {"moex", "shfe_day", "shfe_night", "china_stocks"} & {item.session_id for item in openings}


@pytest.fixture
def opening(calendar):
    return find(calendar, "china_stocks", "2026-11-05")[0]


@pytest.mark.parametrize("seconds,due", [(-.001, False), (0, True), (59.999, True), (60, False), (3600, False)])
def test_opening_minute_is_half_open(opening, seconds, due):
    now = opening.event_at + timedelta(seconds=seconds)
    assert opening.is_due(now) is due
    assert session_boundary_freshness(event_at=opening.event_at, evaluated_at=now).allow_external_delivery is due


def test_naive_time_and_non_minute_opening_are_rejected(opening):
    with pytest.raises(ValueError, match="timezone-aware"):
        opening.is_due(datetime(2026, 11, 5))
    with pytest.raises(ValueError, match="minute precision"):
        replace(opening, event_at=opening.event_at + timedelta(seconds=1))


def test_only_current_minute_gets_published(opening):
    sent = []
    calendar = SimpleNamespace(boundaries=lambda **_: (opening, replace(opening, event_at=opening.event_at-timedelta(hours=1))))
    publisher = SimpleNamespace(publish=lambda item, **_: sent.append(item) or True)
    assert AnnounceSessionBoundaries(calendar=calendar, publisher=publisher).execute(now=opening.event_at) == 1
    assert sent == [(opening,)]


def prepared(opening):
    store = SimpleNamespace(persist_session_notification_once=lambda event, alert: SimpleNamespace(replayed=False))
    saved = []
    def persist(event, alert):
        saved.append((event, alert))
        return SimpleNamespace(replayed=False)
    store.persist_session_notification_once = persist
    publisher = PostgresSessionBoundaryPublisher(store=store, target=DeliveryTarget("telegram", "test"), display_timezone="Europe/Istanbul")
    publisher.publish((opening,), generated_at=opening.event_at)
    return saved[0]


def test_recovery_checks_source_time_and_version(opening):
    event, alert = prepared(opening)
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", signal_record_to_dict(alert.signal), 1)
    adapter = QueuedDeliveryRecoveryAdapter(None, clock=lambda: opening.event_at)
    assert adapter.evaluate(task).allow_external_delivery
    payload = task.payload | {"source_event_at": (opening.event_at-timedelta(minutes=1)).isoformat()}
    assert adapter.evaluate(replace(task, payload=payload)).reason_code == "session_boundary_invalid"
    assert not adapter.evaluate(replace(task, payload=task.payload | {"payload": {}})).allow_external_delivery


@pytest.mark.parametrize("invalid_time", [None, 1, {}, "2026-11-05T01:30:00", "invalid"])
def test_invalid_opening_timestamp_cannot_crash_delivery_worker(opening, invalid_time):
    _, alert = prepared(opening)
    payload = signal_record_to_dict(alert.signal)
    payload["source_event_at"] = invalid_time
    payload["payload"] = payload["payload"] | {"event_at": invalid_time}
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", payload, 1)
    decision = QueuedDeliveryRecoveryAdapter(None, clock=lambda: opening.event_at).evaluate(task)
    assert decision.reason_code == "session_boundary_invalid"


def test_transient_retry_expires_when_opening_minute_ends(opening):
    _, alert = prepared(opening)
    task = DeliveryTask("outbox", alert.signal.signal_id, "telegram", signal_record_to_dict(alert.signal), 2,
                        previous_error_code="delivery_network_error")
    failed = []
    queue = SimpleNamespace(claim=lambda **_: task, mark_failed=lambda *_, **kwargs: failed.append(kwargs))
    sender = SimpleNamespace(send=lambda _: pytest.fail("expired opening must never be sent"))
    now = opening.event_at + timedelta(minutes=1)
    worker = DurableDeliveryWorker(queue=queue, sender=sender,
        metrics=SimpleNamespace(delivery_attempted=lambda **_: None), clock=lambda: now,
        lease_seconds=30, maximum_attempts=3, retry_base_seconds=5, retry_maximum_seconds=60,
        recovery_guard=QueuedDeliveryRecoveryAdapter(None, clock=lambda: now))
    assert worker.run_once().outcome == "suppressed_stale"
    assert failed[0]["dead_letter"]
    assert failed[0]["reason_code"] == "session_boundary_outside_minute"


@pytest.mark.parametrize("session_id,event_kind,action,description", [
    ("oslo", "close", "Закрытие торгов", "Акции · основная сессия"),
    ("shfe_night_industrials", "close", "Закрытие торгов", "Промтовары"),
    ("shfe_night_base", "break_start", "Перерыв", "Промметаллы"),
    ("shfe_night", "open", "Открытие торгов", "Драгметаллы"),
    ("tokyo", "break_end", "Возобновление торгов", "Акции Японии"),
    ("fx_tokyo", "open", "Начало валютной сессии", "Валюты"),
    ("fx_tokyo", "close", "Завершение валютной сессии", "Валюты"),
])
def test_real_telegram_sender_formats_short_calendar_alert_without_trading_scores(
        calendar, session_id, event_kind, action, description):
    opening = replace(find(calendar, session_id, "2026-11-05")[0], event_kind=event_kind)
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
    lines = text.splitlines()
    assert len(lines) == 3
    assert lines[0] == f"🔔 <b>{action}</b> в"
    assert opening.source_url in lines[1]
    assert lines[2] == description
    assert all(word not in text for word in (
        "Качество", "Вероятность", "Терминал", "z-score", "местное", "Стамбул",
        "Сталь, резина", "Золото и серебро", "Условное", "Forex продолжает",
    ))


def test_message_escapes_market_names(opening):
    payload = boundary_payload((replace(opening, title="<market>"),), display_timezone="Europe/Istanbul")
    assert "&lt;market&gt;" in render_session_boundary_html(payload)


def test_session_worker_has_only_destination_and_database_secrets():
    env = {key: "value" for key in ("TINVEST_TOKEN", "TELEGRAM_BOT_TOKEN", "POSTGRES_PASSWORD", "TELEGRAM_CHAT_ID")}
    assert load_secret("POSTGRES_PASSWORD", service_name="session_open_worker", environ=env) == "value"
    assert load_secret("TELEGRAM_CHAT_ID", service_name="session_open_worker", environ=env) == "value"
    for key in ("TINVEST_TOKEN", "TELEGRAM_BOT_TOKEN"):
        assert load_secret(key, service_name="session_open_worker", environ=env) is None

@pytest.mark.parametrize('session', ['tokyo','london','frankfurt','paris','amsterdam','zurich','milan','madrid','stockholm','helsinki','singapore','seoul','taipei','mumbai','jakarta','bangkok','kuala_lumpur','toronto','sao_paulo','mexico','johannesburg','dubai','abu_dhabi','riyadh','istanbul','ice_brent','lme','cme_grains','cme_fx'])
def test_each_major_hub_has_both_open_and_close(calendar, session):
    rows = [e for e in calendar.boundaries(now=moment('2026-11-05T12:00:00+00:00')) if e.session_id==session]
    assert {'open','close'} <= {e.event_kind for e in rows}, session


def test_japan_close_and_lunch_boundaries(calendar):
    assert find(calendar,'tokyo','2026-11-05','close')[0].event_at == moment('2026-11-05T06:30:00+00:00')
    assert find(calendar,'tokyo','2026-11-05','break_start')[0].event_at == moment('2026-11-05T02:30:00+00:00')
    assert find(calendar,'tokyo','2026-11-05','break_end')[0].event_at == moment('2026-11-05T03:30:00+00:00')


def test_europe_closes_at_same_instant_and_groups_into_one_notice(calendar):
    now=moment('2026-11-05T16:30:00+00:00')
    groups=[]
    publisher=SimpleNamespace(publish=lambda events, **_: groups.append(events) or True)
    AnnounceSessionBoundaries(calendar=calendar,publisher=publisher).execute(now=now)
    closes=[g for g in groups if g[0].event_kind=='close']
    assert len(closes)==1
    assert {'london','frankfurt','paris','amsterdam','zurich','milan','madrid'} <= {e.session_id for e in closes[0]}
    html=render_session_boundary_html(boundary_payload(closes[0],display_timezone='Europe/Istanbul'))
    assert 'Закрытие торгов' in html and 'Париж' in html and 'Амстердам' in html
    assert '19:30' not in html and '17:30' not in html
    assert len(html)<4096


def test_early_us_close_and_sgx_half_day(calendar):
    assert find(calendar,'us_stocks','2026-11-27','close')[0].event_at == moment('2026-11-27T18:00:00+00:00')
    assert find(calendar,'singapore','2026-12-24','close')[0].event_at == moment('2026-12-24T04:00:00+00:00')
    assert not find(calendar,'singapore','2026-12-24','break_start')


def test_korea_obsolete_lunch_is_removed_and_singapore_lunch_is_present(calendar):
    assert not find(calendar,'seoul','2026-11-05','break_start')
    assert not find(calendar,'seoul','2026-11-05','break_end')
    assert find(calendar,'singapore','2026-11-05','break_start')[0].event_at.hour==4
    assert find(calendar,'singapore','2026-11-05','break_end')[0].event_at.hour==5


def test_friday_shfe_close_is_saturday_and_holiday_tail_is_absent(calendar):
    assert find(calendar,'shfe_night','2026-11-07','close')[0].event_at==moment('2026-11-06T18:30:00+00:00')
    assert not find(calendar,'shfe_night','2026-10-01','close')
    assert find(calendar,'shfe_night_base','2026-11-07','close')[0].event_at==moment('2026-11-06T17:00:00+00:00')


def test_moex_holiday_futures_close_differs_from_stocks(calendar):
    assert find(calendar,'moex','2026-11-04','close')[0].event_at==moment('2026-11-04T16:00:00+00:00')
    assert find(calendar,'moex_futures','2026-11-04','close')[0].event_at==moment('2026-11-04T20:50:00+00:00')
    assert not find(calendar,'moex_evening','2026-11-04')
    assert find(calendar,'moex_evening','2026-11-05','close')[0].event_at==moment('2026-11-05T20:50:00+00:00')


def test_brent_sunday_open_and_shortened_thanksgiving_close(calendar):
    assert find(calendar,'ice_brent','2026-11-08')[0].event_at==moment('2026-11-08T23:00:00+00:00')
    assert not find(calendar,'ice_brent','2026-11-09')  # Already opened Sunday, no fictitious Monday reopen.
    assert find(calendar,'ice_brent','2026-11-26','close')[0].event_at==moment('2026-11-26T18:30:00+00:00')
    assert find(calendar,'ice_brent','2026-12-24','close')[0].event_at==moment('2026-12-24T19:00:00+00:00')


def test_muhurat_special_sunday_and_brazil_new_november_close(calendar):
    special=find(calendar,'mumbai','2026-11-08')[0]
    assert special.event_at==moment('2026-11-08T08:50:00+00:00')
    assert 'Muhurat' in special.note
    assert not find(calendar,'mumbai','2026-11-10')
    assert find(calendar,'sao_paulo','2026-11-05','close')[0].event_at==moment('2026-11-05T21:00:00+00:00')


def test_v1_opening_payload_remains_renderable(opening):
    p={'session_version':'market_sessions_v1','opens_at':opening.event_at.isoformat(),
       'display_timezone':'Europe/Istanbul','session_id':opening.session_id,'title':opening.title,
       'timezone_name':opening.timezone_name,'instruments':opening.instruments,'kind':'exchange',
       'source_url':opening.source_url,'note':''}
    assert 'Открытие торгов' in render_session_boundary_html(p)
    task=DeliveryTask('x','s','telegram',{'signal_type':'market_session_open','payload':p,
                     'source_event_at':opening.event_at.isoformat()},1)
    assert QueuedDeliveryRecoveryAdapter(None,clock=lambda:opening.event_at).evaluate(task).allow_external_delivery


def test_closing_payload_cannot_masquerade_as_opening(opening):
    _,alert=prepared(replace(opening,event_kind='close'))
    p=signal_record_to_dict(alert.signal)
    p['signal_type']='market_session_open'
    task=DeliveryTask('x','s','telegram',p,1)
    assert QueuedDeliveryRecoveryAdapter(None,clock=lambda:opening.event_at).evaluate(task).reason_code=='session_boundary_invalid'


def test_all_daily_group_messages_fit_telegram(calendar):
    from collections import defaultdict
    groups=defaultdict(list)
    for e in calendar.boundaries(now=moment('2026-11-05T12:00:00+00:00')):
        groups[(e.event_at,e.event_kind)].append(e)
    assert len(groups)>20
    for events in groups.values():
        html=render_session_boundary_html(boundary_payload(tuple(events),display_timezone='Europe/Istanbul'))
        assert len(html)<=4096
        assert all(word not in html for word in ('местное','(Стамбул)','Сталь, резина','Технический перерыв'))


def test_europe_openings_group_into_single_european_notice(calendar):
    now = moment("2026-11-05T08:00:00+00:00")
    groups = []
    publisher = SimpleNamespace(publish=lambda events, **_: groups.append(events) or True)
    AnnounceSessionBoundaries(calendar=calendar, publisher=publisher).execute(now=now)
    opens = [g for g in groups if g[0].event_kind == "open"]
    assert len(opens) == 1
    assert {"london", "frankfurt", "paris", "amsterdam", "zurich", "fx_london"} <= {e.session_id for e in opens[0]}
    html = render_session_boundary_html(boundary_payload(opens[0], display_timezone="Europe/Moscow"))
    assert "Открытие торгов в Европе" in html
    assert "Европейские биржи" in html
    assert "Париж" in html and "Франкфурт" in html and "Амстердам" in html and "Лондон" in html
    assert len(html) < 600  # Compact notice, not 2000+ chars


def test_us_openings_group_into_consolidated_notice(calendar):
    now = moment("2026-11-05T14:30:00+00:00")
    groups = []
    publisher = SimpleNamespace(publish=lambda events, **_: groups.append(events) or True)
    AnnounceSessionBoundaries(calendar=calendar, publisher=publisher).execute(now=now)
    opens = [g for g in groups if g[0].event_kind == "open"]
    assert len(opens) == 1
    assert {"us_stocks", "toronto", "mexico"} <= {e.session_id for e in opens[0]}
    html = render_session_boundary_html(boundary_payload(opens[0], display_timezone="Europe/Moscow"))
    assert "Открытие торгов в США и Северной Америке" in html
    assert "Биржи Северной Америки" in html
    assert "NYSE" in html and "TSX" in html and "BMV" in html
    assert len(html) < 500

