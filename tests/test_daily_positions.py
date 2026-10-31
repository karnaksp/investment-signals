from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json

import httpx
import pytest

from tinvest_signal_engine.adapters.daily_positions_report import (
    PostgresDailyPositionPublisher, comparison_payload, render_daily_positions_html,
)
from tinvest_signal_engine.adapters.delivery_recovery import QueuedDeliveryRecoveryAdapter
from tinvest_signal_engine.adapters.shfe_positions import ShfePositionSource, parse_shfe_positions
from tinvest_signal_engine.application.daily_positions import CollectDailyPositions
from tinvest_signal_engine.application.delivery_recovery import DeliveryRecoveryGuard
from tinvest_signal_engine.application.reliable_processing import StoredEvent
from tinvest_signal_engine.config import load_secret
from tinvest_signal_engine.domain.daily_positions import (
    DailyPositionComparison, REPORT_SIGNAL_TYPE, daily_report_freshness,
)
from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessPolicy
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


NOW = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)
TODAY = date(2026, 10, 30)
PREVIOUS = date(2026, 10, 29)
BROKER = "期货公司会员/境外特殊经纪参与者"
DIRECT = "非期货公司会员/境外特殊非经纪参与者"


def _raw(day=TODAY):
    quantities = ([(110, 45, 10, 5), (25, 90, 5, 10),
                   (180, 90, -20, -10), (40, 130, -10, -20)] if day == TODAY
                  else [(100, 40, 0, 0), (20, 80, 0, 0), (200, 100, 0, 0), (50, 150, 0, 0)])
    rows = []
    for (product, category), (longs, shorts, dl, ds) in zip(
        (("au", BROKER), ("au", DIRECT), ("ag", BROKER), ("ag", DIRECT)), quantities,
    ):
        rows.append({"INSTRUMENTID": product + "all", "PARTICIPANTID2": category,
                     "PARTICIPANTID3": category, "CJ2": longs, "CJ3": shorts,
                     "CJ2_CHG": dl, "CJ3_CHG": ds})
    return {"o_code": "0000", "report_date": day.strftime("%Y%m%d"),
            "update_date": day.strftime("%Y%m%d") + " 15:30:00", "o_cursor": rows}


def _snapshot(day=TODAY):
    return parse_shfe_positions(json.dumps(_raw(day)).encode(), trading_day=day)


def _comparison():
    return DailyPositionComparison(_snapshot(), _snapshot(PREVIOUS))


def test_complete_totals_ignore_rank_membership_and_contract_ranking_sums():
    raw = _raw()
    raw["o_cursor"].append({"INSTRUMENTID": "au2612", "RANK": 1,
                            "CJ2": 999999999, "CJ3": 999999999})
    raw["o_cursor"].append({"INSTRUMENTID": "au2612", "RANK": 999,
                            "PARTICIPANTABBR2": "合计", "CJ2": 999999999})
    parsed = parse_shfe_positions(json.dumps(raw).encode(), trading_day=TODAY)
    assert len(parsed.totals) == 4
    assert parsed.totals[0].longs == 110
    assert parsed.published_at.isoformat() == "2026-10-30T15:30:00+08:00"
    payload = comparison_payload(DailyPositionComparison(parsed, _snapshot(PREVIOUS)))
    assert payload["products"][0]["groups"][0]["long_change"] == 10


@pytest.mark.parametrize("corruption", [
    "wrong_date", "bad_status", "missing_category", "duplicate", "unbalanced",
    "unknown_category", "mixed_category", "fractional", "negative", "boolean", "missing_metal",
])
def test_bad_exchange_data_cannot_become_a_report(corruption):
    raw = _raw()
    if corruption == "wrong_date": raw["report_date"] = "20261028"
    if corruption == "bad_status": raw["o_code"] = "1000"
    if corruption == "missing_category": raw["o_cursor"].pop()
    if corruption == "missing_metal": raw["o_cursor"] = raw["o_cursor"][:2]
    if corruption == "duplicate": raw["o_cursor"].append(deepcopy(raw["o_cursor"][0]))
    if corruption == "unbalanced": raw["o_cursor"][0]["CJ2"] += 1
    if corruption == "unknown_category": raw["o_cursor"][0]["PARTICIPANTID2"] = "new category"
    if corruption == "mixed_category": raw["o_cursor"][0]["PARTICIPANTID3"] = DIRECT
    if corruption == "fractional": raw["o_cursor"][0]["CJ2"] = 110.1
    if corruption == "negative": raw["o_cursor"][0]["CJ2"] = -1
    if corruption == "boolean": raw["o_cursor"][0]["CJ2"] = True
    with pytest.raises(ValueError):
        parse_shfe_positions(json.dumps(raw).encode(), trading_day=TODAY)


def test_delta_must_match_adjacent_report_and_order_is_strict():
    current = _snapshot()
    bad = replace(current, totals=(replace(current.totals[0], long_change=999), *current.totals[1:]))
    with pytest.raises(ValueError, match="daily changes"):
        DailyPositionComparison(bad, _snapshot(PREVIOUS))
    with pytest.raises(ValueError, match="earlier"):
        DailyPositionComparison(current, current)


class _Publisher:
    def __init__(self): self.reports = []
    def publish(self, comparison, *, generated_at):
        self.reports.append((comparison, generated_at))
        return True


def test_weekend_uses_last_two_available_sessions():
    class Source:
        def load(self, day):
            assert day.weekday() < 5
            return {TODAY: _snapshot(), PREVIOUS: _snapshot(PREVIOUS)}.get(day)
    publisher = _Publisher()
    receipt = CollectDailyPositions(source=Source(), publisher=publisher).execute(as_of=NOW)
    assert (receipt.status, receipt.trading_day, receipt.previous_day) == ("queued", TODAY, PREVIOUS)
    assert len(publisher.reports) == 1


def test_long_holiday_compares_actual_previous_session_not_previous_calendar_day():
    previous = replace(_snapshot(PREVIOUS), trading_day=date(2026, 9, 30),
                       published_at=datetime(2026, 9, 30, 8, tzinfo=timezone.utc))
    current = replace(_snapshot(), trading_day=date(2026, 10, 9),
                      published_at=datetime(2026, 10, 9, 8, tzinfo=timezone.utc))
    class Source:
        def load(self, day): return {current.trading_day: current, previous.trading_day: previous}.get(day)
    publisher = _Publisher()
    receipt = CollectDailyPositions(source=Source(), publisher=publisher).execute(
        as_of=datetime(2026, 10, 9, 12, tzinfo=timezone.utc),
    )
    assert receipt.previous_day == date(2026, 9, 30)


def test_source_failure_does_not_silently_skip_a_trading_day():
    class Source:
        def load(self, day): raise TimeoutError("source unavailable")
    publisher = _Publisher()
    with pytest.raises(TimeoutError):
        CollectDailyPositions(source=Source(), publisher=publisher).execute(as_of=NOW)
    assert publisher.reports == []


def test_official_http_reports_are_cached_only_after_validation(tmp_path):
    calls = []
    def respond(request):
        calls.append(request.url)
        return httpx.Response(200, json=_raw())
    source = ShfePositionSource(cache_dir=tmp_path, client=httpx.Client(transport=httpx.MockTransport(respond)))
    first = source.load(TODAY)
    assert source.load(TODAY) == first
    assert len(calls) == 1
    assert len(list(tmp_path.glob("*.json"))) == 1


@pytest.mark.parametrize("status,body,expected", [
    (404, b"missing", None), (403, b"denied", httpx.HTTPStatusError),
    (200, b"<html>access check</html>", ValueError),
])
def test_missing_file_is_distinct_from_access_or_schema_failure(tmp_path, status, body, expected):
    source = ShfePositionSource(cache_dir=tmp_path, client=httpx.Client(transport=httpx.MockTransport(
        lambda request: httpx.Response(status, content=body),
    )))
    if expected is None:
        assert source.load(TODAY) is None
    else:
        with pytest.raises(expected): source.load(TODAY)
    assert list(tmp_path.iterdir()) == []


def test_report_identity_is_stable_across_retries_on_different_days():
    class Store:
        def __init__(self): self.events = []; self.signals = []
        def persist_report_once(self, event, prepared):
            self.events.append(event); self.signals.append(prepared.signal)
            return StoredEvent((prepared.signal,), replayed=len(self.events) > 1)
    store = Store()
    publisher = PostgresDailyPositionPublisher(store=store, target=DeliveryTarget("telegram", "test"))
    assert publisher.publish(_comparison(), generated_at=NOW)
    assert not publisher.publish(_comparison(), generated_at=NOW + timedelta(days=1))
    assert store.events[0] == store.events[1]
    assert store.signals[0].source_event_at == _snapshot().published_at
    assert store.signals[0].signal_id == store.signals[1].signal_id


def test_daily_bulletin_has_dates_and_quantities_without_fake_quality_or_terminal_links():
    payload = comparison_payload(_comparison())
    signal = TriggerSignal("x", NOW, "SHFE_DAILY_METALS", "SHFE", "REPORT", "shfe", "daily_position_report",
                           REPORT_SIGNAL_TYPE, 1, 0, 0, 0, 86400, "Дневные позиции SHFE", payload)
    enriched = enrich_signal_for_delivery(signal)
    message = enriched.payload["telegram_html"]
    assert "2026-10-30 к 2026-10-29" in message
    assert "Золото" in message and "Серебро" in message
    assert "Чистая позиция: +65; изменение +5" in message
    assert "качест" not in message.lower() and "терминал" not in message.lower()
    assert "quality_score" not in enriched.payload
    assert len(message) < 4096


@pytest.mark.parametrize("age_seconds,allowed", [(21600, True), (21601, False), (-5, True), (-6, False)])
def test_daily_delivery_has_a_bounded_materialization_age(age_seconds, allowed):
    assert daily_report_freshness(trading_day=TODAY, previous_day=PREVIOUS,
                                  generated_at=NOW - timedelta(seconds=age_seconds),
                                  evaluated_at=NOW).allow_external_delivery is allowed


def test_daily_outbox_accepts_yesterdays_source_but_keeps_tick_freshness_unchanged():
    class Metrics:
        def stale_delivery_suppressed(self, **_kw): pass
    guard = DeliveryRecoveryGuard(policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120),
                                  metrics=Metrics(), clock=lambda: NOW)
    adapter = QueuedDeliveryRecoveryAdapter(guard, clock=lambda: NOW)
    payload = {**comparison_payload(_comparison()), "report_generated_at": NOW.isoformat()}
    task = DeliveryTask("out", "signal", "telegram", {
        "signal_type": REPORT_SIGNAL_TYPE, "source_event_at": _snapshot().published_at.isoformat(),
        "payload": payload,
    }, 1)
    assert adapter.evaluate(task).allow_external_delivery
    tick = replace(task, payload={**task.payload, "signal_type": "volume_spike"})
    assert not adapter.evaluate(tick).allow_external_delivery
    bad = replace(task, payload={**task.payload, "payload": {**payload, "daily_report_version": "unknown"}})
    assert not adapter.evaluate(bad).allow_external_delivery


@pytest.mark.parametrize("current,previous", [
    (date(2026, 10, 23), date(2026, 10, 22)), (date(2026, 11, 1), TODAY), (TODAY, TODAY),
])
def test_stale_future_or_reversed_report_dates_fail_closed(current, previous):
    assert not daily_report_freshness(trading_day=current, previous_day=previous,
                                       generated_at=NOW, evaluated_at=NOW).allow_external_delivery


def test_report_scheduler_can_identify_destination_without_bot_credentials():
    values = {"TELEGRAM_CHAT_ID": "destination", "TELEGRAM_MESSAGE_THREAD_ID": "7",
              "TELEGRAM_BOT_TOKEN": "worker-only-secret"}
    assert load_secret("TELEGRAM_CHAT_ID", service_name="dagster", environ=values) == "destination"
    assert load_secret("TELEGRAM_MESSAGE_THREAD_ID", service_name="dagster", environ=values) == "7"
    assert load_secret("TELEGRAM_BOT_TOKEN", service_name="dagster", environ=values) is None
