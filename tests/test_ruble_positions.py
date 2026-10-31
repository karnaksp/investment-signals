from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone
from io import BytesIO
import json

import httpx
from PIL import Image, ImageDraw
import pytest

from tinvest_signal_engine.adapters.asset_position_cards import asset_report_payload, render_asset_positions_html, render_asset_positions_png
from tinvest_signal_engine.adapters.asset_position_publisher import PostgresAssetPositionPublisher
from tinvest_signal_engine.adapters.delivery_recovery import QueuedDeliveryRecoveryAdapter
from tinvest_signal_engine.adapters.moex_ruble_positions import MoexRublePositionSource, PAIRS, parse_moex_position_page, select_front_contracts
from tinvest_signal_engine.application.delivery_recovery import DeliveryRecoveryGuard
from tinvest_signal_engine.application.ruble_positions import CollectRublePositions
from tinvest_signal_engine.domain.asset_positions import AssetPositionReport
from tinvest_signal_engine.domain.daily_positions import REPORT_SIGNAL_TYPE
from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessPolicy
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask, EventReplayConflict
from test_asset_position_cards import _Store


NOW = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)


def _data(security="SiZ6", asset="Si"):
    base = {"tradedate": "2026-10-30", "asset": asset, "type": "F"}
    return {"securityData": {"SECID": security, "ASSETCODE": asset},
            "openPositionsPeriod": {"tillDateIso": "2026-10-30"},
            "offDays": {"2026-10-29": {"is_traded": 1}, "2026-10-30": {"is_traded": 1}},
            "openPositions": {"isError": False, "data": [
                {**base, "title": "Кол-во контрактов, шт.", "long_fiz": 110, "short_fiz": 50,
                 "long_jur": 40, "short_jur": 100, "total": 300},
                {**base, "title": "Изменение к пред. торговому дню, шт.", "long_fiz": 10, "short_fiz": 10,
                 "long_jur": -5, "short_jur": -5, "total": 10},
                {**base, "title": "Изменение к пред. торговому дню, %", "long_fiz": 10, "short_fiz": 25,
                 "long_jur": -11.11, "short_jur": -4.76, "total": 3.45},
            ]}}


def _page(data):
    raw = {"props": {"pageProps": {"recursiveComponentContentProps": {"dynamic-component-key": {"initData": {"data": data}}}}}}
    return ('<html><script id="__NEXT_DATA__" type="application/json">'+json.dumps(raw)+'</script></html>').encode()


def _parse(data=None, **kw):
    return parse_moex_position_page(_page(data or _data()), security_id="SiZ6", asset="Si",
                                    source_url="https://www.moex.com/ru/derivatives/futures-on-currency/siz6", as_of=kw.get("as_of", NOW))


def _catalog():
    return json.dumps({"securities": {"columns": ["SECID", "ASSETCODE", "LASTTRADEDATE"], "data": [
        ["SiU6", "Si", "2026-09-17"], ["SiZ6", "Si", "2026-12-17"], ["SiH7", "Si", "2027-03-18"],
        ["CRZ6", "CNY", "2026-12-17"], ["EuZ6", "Eu", "2026-12-17"],
        ["USDMZ6", "USDM", "2026-12-17"],
    ]}}).encode()


def test_public_daily_changes_reconstruct_previous_positions_and_normalize_double_counted_total():
    section = _parse()
    assert section.previous_day == date(2026, 10, 29)
    assert section.groups[0].previous_longs == 100 and section.groups[0].previous_shorts == 40
    assert section.open_interest == 150 and section.reported_gross_open_positions == 300
    assert section.previous_open_interest == 145 and section.previous_reported_gross_open_positions == 290
    assert section.comparison_method == "exchange_reported_daily_change"


@pytest.mark.parametrize("corruption", ["security", "asset", "options", "mixed_date", "duplicate", "missing",
                                         "float", "boolean", "negative", "bad_delta", "unbalanced", "percent",
                                         "period", "calendar", "upstream_error", "invalid_row"])
def test_bad_moex_data_cannot_become_a_position_card(corruption):
    raw = _data()
    rows = raw["openPositions"]["data"]
    if corruption == "security": raw["securityData"]["SECID"] = "SiH7"
    if corruption == "asset": rows[0]["asset"] = "Eu"
    if corruption == "options": rows[0]["type"] = "O"
    if corruption == "mixed_date": rows[1]["tradedate"] = "2026-10-29"
    if corruption == "duplicate": rows.append(deepcopy(rows[0]))
    if corruption == "missing": rows.pop(1)
    if corruption == "float": rows[0]["long_fiz"] = 110.5
    if corruption == "boolean": rows[0]["long_fiz"] = True
    if corruption == "negative": rows[0]["long_fiz"] = -1
    if corruption == "bad_delta": rows[1]["long_fiz"] = 200
    if corruption == "unbalanced": rows[0]["total"] = 302
    if corruption == "percent": rows[2]["long_fiz"] = 0
    if corruption == "period": raw["openPositionsPeriod"]["tillDateIso"] = "2026-10-29"
    if corruption == "calendar": raw["offDays"].pop("2026-10-29")
    if corruption == "upstream_error": raw["openPositions"]["isError"] = True
    if corruption == "invalid_row": rows.append(None)
    with pytest.raises(ValueError): _parse(raw)


@pytest.mark.parametrize("as_of", [datetime(2026, 10, 30, 20, tzinfo=timezone.utc),
                                    datetime(2026, 10, 29, tzinfo=timezone.utc),
                                    datetime(2026, 11, 7, tzinfo=timezone.utc)])
def test_unfinished_future_and_stale_reports_are_rejected(as_of):
    with pytest.raises(ValueError, match="completed day"): _parse(as_of=as_of)


def test_previous_day_uses_exchange_calendar_across_holidays():
    raw = _data()
    raw["offDays"] = {"2026-10-27": {"is_traded": 1}, "2026-10-28": {"is_traded": 0},
                      "2026-10-29": {"is_traded": 0}, "2026-10-30": {"is_traded": 1}}
    assert _parse(raw).previous_day == date(2026, 10, 27)


def test_front_resolution_excludes_expired_and_mini_contracts_and_rolls_forward():
    fronts = select_front_contracts(_catalog(), today=NOW.date())
    assert fronts == {"Si": "SiZ6", "CNY": "CRZ6", "Eu": "EuZ6"}
    assert select_front_contracts(_catalog(), today=date(2026, 12, 18)) == {"Si": "SiH7"}


def test_one_currency_failure_does_not_fabricate_zero_or_block_the_other_pairs(tmp_path):
    def respond(request):
        if request.url.host == "iss.moex.com": return httpx.Response(200, content=_catalog())
        if request.url.path.endswith("EURRUBF"): return httpx.Response(503)
        for _, asset, prefix, perpetual, perpetual_asset in PAIRS:
            if request.url.path.endswith((prefix+"Z6").lower()):
                return httpx.Response(200, content=_page(_data(prefix+"Z6", asset)))
            if request.url.path.endswith(perpetual):
                return httpx.Response(200, content=_page(_data(perpetual, perpetual_asset)))
        raise AssertionError(request.url)
    source = MoexRublePositionSource(cache_dir=tmp_path, client=httpx.Client(transport=httpx.MockTransport(respond)))
    batch = source.load(as_of=NOW)
    assert [r.commodity for r in batch.reports] == ["usd_rub", "cny_rub"]
    assert batch.unavailable_pairs == ("eur_rub",)
    assert all(len(r.sections) == 2 for r in batch.reports)
    assert len(list(tmp_path.glob("*.html.gz"))) == 5
    store = _Store()
    collector = CollectRublePositions(source=source, publisher=PostgresAssetPositionPublisher(
        store=store, target=DeliveryTarget("telegram", "test"),
    ))
    assert collector.execute(as_of=NOW).status == "queued"
    assert collector.execute(as_of=NOW).status == "already_queued"
    assert len(store.events) == 2


def test_ruble_card_keeps_legal_status_currency_direction_and_independent_contract_types(monkeypatch):
    section = _parse()
    perpetual = replace(section, market_id="USDRUBTOM")
    payload = asset_report_payload(AssetPositionReport("usd_rub", (section, perpetual)))
    caption = render_asset_positions_html(payload)
    assert len(caption) <= 1024
    assert "валюта против рубля" in caption and "с экспирацией" in caption and "вечные" in caption
    assert "Физические лица" in caption and "Через брокеров" not in caption and "фонды" not in caption.lower()
    assert "без спредов" not in caption
    labels = []
    original = ImageDraw.ImageDraw.text
    def checked(self, xy, text, *args, **kw):
        box = self.textbbox(xy, text, font=kw.get("font"), anchor=kw.get("anchor"))
        assert 0 <= box[0] < box[2] <= self._image.width and 0 <= box[1] < box[3] <= self._image.height
        labels.append(text)
        return original(self, xy, text, *args, **kw)
    monkeypatch.setattr(ImageDraw.ImageDraw, "text", checked)
    photo = render_asset_positions_png(payload)
    assert Image.open(BytesIO(photo)).size == (1080, 1260)
    assert "Юридические лица" in labels and not any("без спредов" in text for text in labels)


def test_moex_webpage_noise_does_not_conflict_but_changed_positions_do():
    section = _parse()
    report = AssetPositionReport("usd_rub", (section,))
    store = _Store()
    publisher = PostgresAssetPositionPublisher(store=store, target=DeliveryTarget("telegram", "test"))
    assert publisher.publish_reports((report,), generated_at=NOW)
    assert not publisher.publish_reports((replace(report, sections=(replace(section, source_sha256="b"*64),)),), generated_at=NOW)
    changed = replace(section, groups=(replace(section.groups[0], longs=111), replace(section.groups[1], longs=39)))
    with pytest.raises(EventReplayConflict): publisher.publish_reports((replace(report, sections=(changed,)),), generated_at=NOW)


def test_moex_delivery_guard_accepts_only_daily_source_with_matching_primary():
    class Metrics:
        def stale_delivery_suppressed(self, **_kw): pass
    adapter = QueuedDeliveryRecoveryAdapter(DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120), metrics=Metrics(), clock=lambda: NOW,
    ), clock=lambda: NOW)
    payload = {**asset_report_payload(AssetPositionReport("usd_rub", (_parse(),))), "report_generated_at": NOW.isoformat()}
    task = DeliveryTask("out", "signal", "telegram", {"signal_type": REPORT_SIGNAL_TYPE, "payload": payload}, 1)
    assert adapter.evaluate(task).allow_external_delivery
    bad = replace(task, payload={**task.payload, "payload": {**payload, "period": "weekly"}})
    assert not adapter.evaluate(bad).allow_external_delivery


def test_optional_new_metadata_does_not_change_existing_commodity_event_bodies():
    from test_commodity_positions import _comparison, _context
    from tinvest_signal_engine.application.asset_positions import build_asset_position_reports
    payload = asset_report_payload(build_asset_position_reports(_comparison(), _context())[0])
    assert all("comparison_method" not in s and "reported_gross_open_positions" not in s for s in payload["sections"])
