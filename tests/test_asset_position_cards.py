from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
import http.client

import httpx
from PIL import Image, ImageDraw
import pytest

from test_commodity_positions import _comparison, _context, NOW
from tinvest_signal_engine.adapters.asset_position_cards import (
    asset_report_payload, group_takeaway, render_asset_positions_html, render_asset_positions_png,
)
from tinvest_signal_engine.adapters.asset_position_publisher import PostgresAssetPositionPublisher
from tinvest_signal_engine.adapters.delivery_recovery import QueuedDeliveryRecoveryAdapter
from tinvest_signal_engine.adapters.telegram_http import RawTelegramHttpResponse, TelegramMultiAddressHttpClient
from tinvest_signal_engine.adapters.telegram_position_cards import send_position_card
from tinvest_signal_engine.application.asset_positions import build_asset_position_reports
from tinvest_signal_engine.application.delivery_recovery import DeliveryRecoveryGuard
from tinvest_signal_engine.application.reliable_processing import StoredEvent
from tinvest_signal_engine.domain.asset_positions import ASSET_REPORT_VERSION, asset_report_freshness
from tinvest_signal_engine.domain.commodity_positions import PositionContext
from tinvest_signal_engine.domain.daily_positions import DailyPositionComparison, REPORT_SIGNAL_TYPE
from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessPolicy
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask, EventReplayConflict
from test_telegram_delivery_resilience import FakeResolver


def _payload():
    return asset_report_payload(build_asset_position_reports(_comparison(), _context())[0])


def test_each_asset_has_only_its_sources_and_retains_all_five_weekly_groups():
    reports = build_asset_position_reports(_comparison(), _context())
    assert len(reports) == 9
    assert [s.source for s in reports[0].sections] == ["SHFE", "CFTC"]
    assert [s.source for s in reports[1].sections] == ["SHFE", "CFTC"]
    assert reports[-1].commodity == "brent" and reports[-1].sections[0].source == "ICE"
    assert len(reports[0].sections[1].groups) == 5
    assert reports[0].sections[1].groups[0].spreading == 30
    assert reports[0].sections[1].open_interest == 560
    for report in reports:
        caption = render_asset_positions_html(asset_report_payload(report))
        assert len(caption) <= 1024
        assert "нетто" not in caption and "Δ" not in caption and "качество" not in caption
        assert "с задержкой" in caption
    assert "Серебро" not in render_asset_positions_html(_payload())


def test_source_outage_is_attached_only_to_relevant_assets():
    reports = build_asset_position_reports(_comparison(), PositionContext((), ("ICE", "CFTC")))
    assert len(reports) == 2
    assert all(r.unavailable_sources == ("CFTC",) for r in reports)


@pytest.mark.parametrize("values,words", [
    ((90, 40, 100, 40), "Перевес лонгов сократился"),
    ((40, 90, 40, 100), "Перевес шортов сократился"),
    ((40, 110, 40, 100), "Перевес шортов вырос"),
    ((110, 100, 90, 100), "Сторона сменилась: теперь больше лонгов"),
    ((90, 100, 110, 100), "Сторона сменилась: теперь больше шортов"),
    ((100, 100, 110, 100), "Перевес исчез"),
    ((100, 40, 100, 40), "Перевес не изменился"),
])
def test_narrative_describes_net_preference_not_price_or_gross_trade_direction(values, words):
    row = dict(zip(("longs", "shorts", "previous_longs", "previous_shorts"), values))
    assert group_takeaway(row, "daily").startswith(words)


class _Store:
    def __init__(self): self.events = {}; self.signals = {}
    def persist_report_once(self, event, prepared):
        old = self.events.get(event.event_id)
        if old:
            if old.payload_sha256 != event.payload_sha256: raise EventReplayConflict("changed source")
            return StoredEvent((self.signals[event.event_id],), replayed=True)
        self.events[event.event_id] = event
        self.signals[event.event_id] = prepared.signal
        return StoredEvent((prepared.signal,), replayed=False)


def test_daily_updates_queue_two_metals_and_do_not_repeat_seven_unchanged_weekly_reports():
    store = _Store()
    publisher = PostgresAssetPositionPublisher(store=store, target=DeliveryTarget("telegram", "test"))
    comparison, context = _comparison(), _context()
    assert publisher.publish(comparison, generated_at=NOW, context=context)
    assert len(store.events) == 9
    assert not publisher.publish(comparison, generated_at=NOW + timedelta(hours=1), context=context)
    next_current = replace(comparison.current, trading_day=date(2026, 11, 2),
                           published_at=datetime(2026, 11, 2, 8, tzinfo=timezone.utc),
                           totals=tuple(replace(g, long_change=0, short_change=0) for g in comparison.current.totals))
    next_day = DailyPositionComparison(next_current, comparison.current)
    assert publisher.publish(next_day, generated_at=NOW + timedelta(days=2), context=context)
    assert len(store.events) == 11


def test_source_archive_changes_do_not_resend_unchanged_weekly_observations():
    store = _Store()
    publisher = PostgresAssetPositionPublisher(store=store, target=DeliveryTarget("telegram", "test"))
    publisher.publish(_comparison(), generated_at=NOW, context=_context())
    new_context = PositionContext(tuple(replace(m, source_sha256="b" * 64) for m in _context().markets))
    assert not publisher.publish(_comparison(), generated_at=NOW, context=new_context)
    assert len(store.events) == 9
    changed = replace(_comparison().current, source_sha256="c" * 64)
    with pytest.raises(EventReplayConflict):
        publisher.publish(DailyPositionComparison(changed, _comparison().previous), generated_at=NOW, context=new_context)
    market = new_context.markets[2]
    revised = replace(market, groups=(replace(market.groups[0], longs=market.groups[0].longs + 1),
                                     replace(market.groups[1], longs=market.groups[1].longs - 1), *market.groups[2:]))
    with pytest.raises(EventReplayConflict):
        publisher.publish(_comparison(), generated_at=NOW,
                          context=PositionContext((*new_context.markets[:2], revised, *new_context.markets[3:])))


def test_photo_is_a_real_png_and_all_labels_fit_the_image(monkeypatch):
    original = ImageDraw.ImageDraw.text
    bounds = []
    def checked(self, xy, text, *args, **kwargs):
        bbox = self.textbbox(xy, text, font=kwargs.get("font"), anchor=kwargs.get("anchor"))
        bounds.append((text, bbox))
        return original(self, xy, text, *args, **kwargs)
    monkeypatch.setattr(ImageDraw.ImageDraw, "text", checked)
    photo = render_asset_positions_png(_payload())
    with Image.open(BytesIO(photo)) as image:
        assert image.format == "PNG" and image.mode == "RGB"
        assert image.size == (1080, 1260)
        assert len(photo) < 1_000_000
        assert all(0 <= box[0] < box[2] <= image.width and 0 <= box[1] < box[3] <= image.height for _, box in bounds)
    assert any("Через брокеров" == text for text, _ in bounds)
    assert any("Напрямую на бирже" == text for text, _ in bounds)
    assert any("27.10.2026 к 20.10.2026" == text for text, _ in bounds)


def test_all_long_all_short_and_no_directional_positions_render_without_zero_division():
    payload = _payload()
    for longs, shorts in ((0, 0), (100, 0), (0, 100)):
        payload["sections"][0]["groups"][0].update(longs=longs, shorts=shorts)
        assert render_asset_positions_png(payload).startswith(b"\x89PNG\r\n\x1a\n")


def test_upload_has_photo_caption_destination_and_no_text_fallback_on_ambiguous_delivery():
    calls = []
    def respond(request):
        calls.append(request)
        assert request.url.path == "/botredacted/sendPhoto"
        body = request.read()
        assert b'name="photo"' in body and b"image/png" in body and b"\x89PNG" in body
        assert b'name="message_thread_id"' in body and b"7" in body
        assert "Золото".encode() in body
        return httpx.Response(200, json={"ok": True})
    client = httpx.Client(transport=httpx.MockTransport(respond))
    send_position_card(client=client, bot_token="redacted", chat_id="test", message_thread_id=7, payload=_payload())
    assert len(calls) == 1


def test_dns_failover_upload_preserves_multipart_bytes_and_uses_verified_hostname():
    calls = []
    class Requester:
        def post(self, **kwargs):
            calls.append(kwargs)
            assert kwargs["hostname"] == "api.telegram.org"
            assert kwargs["path"] == "/botredacted/sendPhoto"
            assert kwargs["content_type"].startswith("multipart/form-data; boundary=")
            assert b"\x89PNG" in kwargs["payload"]
            return RawTelegramHttpResponse(200, (), b'{"ok":true}')
    client = TelegramMultiAddressHttpClient(resolver=FakeResolver(), requester=Requester())
    send_position_card(client=client, bot_token="redacted", chat_id="test", message_thread_id=None, payload=_payload())
    assert len(calls) == 1
    class Interrupted:
        def post(self, **kwargs):
            raise http.client.RemoteDisconnected("ambiguous upload")
    interrupted = TelegramMultiAddressHttpClient(resolver=FakeResolver(), requester=Interrupted())
    with pytest.raises(httpx.ProtocolError):
        send_position_card(client=interrupted, bot_token="redacted", chat_id="test", message_thread_id=None, payload=_payload())


@pytest.mark.parametrize("period,days,allowed", [("daily", 7, True), ("daily", 8, False),
                                                ("weekly", 14, True), ("weekly", 15, False)])
def test_asset_delivery_source_age_depends_on_explicit_period(period, days, allowed):
    day = NOW.date() - timedelta(days=days)
    assert asset_report_freshness(trading_day=day, previous_day=day-timedelta(days=7), period=period,
                                  generated_at=NOW, evaluated_at=NOW).allow_external_delivery is allowed


def test_asset_outbox_guard_rejects_unknown_period_inconsistent_dates_and_missing_primary():
    class Metrics:
        def stale_delivery_suppressed(self, **_kw): pass
    adapter = QueuedDeliveryRecoveryAdapter(DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120), metrics=Metrics(), clock=lambda: NOW,
    ), clock=lambda: NOW)
    payload = {**_payload(), "report_generated_at": NOW.isoformat()}
    assert payload["daily_report_version"] == ASSET_REPORT_VERSION
    task = DeliveryTask("out", "signal", "telegram", {"signal_type": REPORT_SIGNAL_TYPE, "payload": payload}, 1)
    assert adapter.evaluate(task).allow_external_delivery
    for changes in ({"period": "weekly"}, {"source_exchange": "CFTC"}, {"sections": []},
                    {"trading_day": "2026-10-29"}, {"report_generated_at": (NOW-timedelta(hours=7)).isoformat()}):
        invalid = replace(task, payload={**task.payload, "payload": {**deepcopy(payload), **changes}})
        assert not adapter.evaluate(invalid).allow_external_delivery
