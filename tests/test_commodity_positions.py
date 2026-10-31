from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import csv
from io import StringIO
import json

import httpx
import pytest

from tinvest_signal_engine.adapters.daily_positions_report import (
    PostgresDailyPositionPublisher, comparison_payload, render_daily_positions_html,
)
from tinvest_signal_engine.adapters.delivery_recovery import QueuedDeliveryRecoveryAdapter
from tinvest_signal_engine.adapters.weekly_positions import (
    CFTC_MARKETS, WeeklyPositionSource, parse_cftc_positions, parse_ice_positions,
)
from tinvest_signal_engine.application.daily_positions import CollectDailyPositions, PositionSourceUnavailable
from tinvest_signal_engine.application.delivery_recovery import DeliveryRecoveryGuard
from tinvest_signal_engine.application.reliable_processing import StoredEvent
from tinvest_signal_engine.domain.commodity_positions import DIGEST_VERSION, PositionContext, TraderPositionChange
from tinvest_signal_engine.domain.daily_positions import DailyPositionComparison, PositionSnapshot, PositionTotal, REPORT_SIGNAL_TYPE
from tinvest_signal_engine.domain.delivery_recovery import DeliveryFreshnessPolicy
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, DeliveryTask


NOW = datetime(2026, 10, 31, 12, tzinfo=timezone.utc)


def _cftc_row(code="088691", *, previous=False):
    return {
        "cftc_contract_market_code": code, "futonly_or_combined": "FutOnly",
        "report_date_as_yyyy_mm_dd": "2026-10-20T00:00:00.000" if previous else "2026-10-27T00:00:00.000",
        "open_interest_all": "560", "change_in_open_interest_all": "0",
        "prod_merc_positions_long": "110" if previous else "100",
        "prod_merc_positions_short": "190" if previous else "200",
        "change_in_prod_merc_long": "-10", "change_in_prod_merc_short": "10",
        "m_money_positions_long_all": "140" if previous else "150",
        "m_money_positions_short_all": "60" if previous else "50",
        "m_money_positions_spread": "30",
        "change_in_m_money_long_all": "10", "change_in_m_money_short_all": "-10",
        "swap_positions_long_all": "100", "swap__positions_short_all": "80",
        "swap__positions_spread_all": "20", "change_in_swap_long_all": "0", "change_in_swap_short_all": "0",
        "other_rept_positions_long": "80", "other_rept_positions_short": "90",
        "other_rept_positions_spread": "10", "change_in_other_rept_long": "0", "change_in_other_rept_short": "0",
        "nonrept_positions_long_all": "70", "nonrept_positions_short_all": "80",
        "change_in_nonrept_long_all": "0", "change_in_nonrept_short_all": "0",
    }


def _cftc_body():
    return json.dumps([_cftc_row(code, previous=old) for code in CFTC_MARKETS for old in (False, True)]).encode()


def _ice_body(dates=("10/27/2026", "10/20/2026")):
    # Actual ICE CSV column spellings differ from CFTC's JSON, and ICE's
    # change fields are empty. Changes must be computed from the two rows.
    fields = ["Market_and_Exchange_Names", "FutOnly_or_Combined", "As_of_Date_Form_MM/DD/YYYY",
              "Open_Interest_All", "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All",
              "Swap_Positions_Long_All", "Swap_Positions_Short_All", "Swap_Positions_Spread_All",
              "M_Money_Positions_Long_All", "M_Money_Positions_Short_All", "M_Money_Positions_Spread_All",
              "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All", "Other_Rept_Positions_Spread_All",
              "NonRept_Positions_Long_All", "NonRept_Positions_Short_All", "Change_in_M_Money_Long_All"]
    stream = StringIO()
    writer = csv.writer(stream)
    writer.writerow(fields)
    for i, day in enumerate(dates):
        writer.writerow(["ICE Brent Crude Futures - ICE Futures Europe", "FutOnly", day,
                         560, 100 if i == 0 else 110, 200 if i == 0 else 190, 100, 80, 20,
                         150 if i == 0 else 140, 50 if i == 0 else 60, 30, 80, 90, 10, 70, 80, ""])
    writer.writerow(["ICE Brent Crude Futures - ICE Futures Europe", "Combined", dates[0], *([999999] * 15)])
    return stream.getvalue().encode()


def _comparison():
    def snapshot(day, old):
        totals = tuple(row for product in ("au", "ag") for row in (
            PositionTotal(product, "broker_clients", 100 if old else 110, 40, 0 if old else 10, 0),
            PositionTotal(product, "direct_members", 20, 80 if old else 90, 0, 0 if old else 10),
        ))
        return PositionSnapshot(day, datetime(day.year, day.month, day.day, 8, tzinfo=timezone.utc),
                                totals, "https://www.shfe.com.cn/report", "a" * 64)
    return DailyPositionComparison(snapshot(date(2026, 10, 30), False), snapshot(date(2026, 10, 29), True))


def _context():
    return PositionContext(parse_cftc_positions(_cftc_body(), as_of=NOW).markets
                           + parse_ice_positions(_ice_body(), as_of=NOW).markets)


def test_both_official_formats_reconcile_spreading_and_ignore_combined_reports():
    context = _context()
    assert len(context.markets) == 9
    for market in context.markets:
        assert market.open_interest == 560
        assert market.groups[0].net == 100
        assert market.groups[0].net_change == 20
        assert market.groups[0].spreading == 30
        assert market.previous_day == date(2026, 10, 20)
    assert context.markets[-1].source == "ICE" and context.markets[-1].market_id == "B"


@pytest.mark.parametrize("corruption", ["missing", "float", "boolean", "negative", "unbalanced", "delta",
                                         "oi_delta", "duplicate", "stale", "future", "period", "combined"])
def test_invalid_weekly_market_is_omitted_and_named_without_fabricating_zero(corruption):
    rows = json.loads(_cftc_body())
    current = rows[0]
    if corruption == "missing": del current["m_money_positions_short_all"]
    if corruption == "float": current["m_money_positions_long_all"] = 150.1
    if corruption == "boolean": current["m_money_positions_long_all"] = True
    if corruption == "negative": current["m_money_positions_long_all"] = "-1"
    if corruption == "unbalanced": current["open_interest_all"] = "561"
    if corruption == "delta": current["change_in_m_money_long_all"] = "11"
    if corruption == "oi_delta": current["change_in_open_interest_all"] = "1"
    if corruption == "duplicate": rows.append(deepcopy(current))
    if corruption == "stale":
        current["report_date_as_yyyy_mm_dd"] = "2026-10-13"
        rows[1]["report_date_as_yyyy_mm_dd"] = "2026-10-06"
    if corruption == "future": current["report_date_as_yyyy_mm_dd"] = "2026-11-03"
    if corruption == "period": rows[1]["report_date_as_yyyy_mm_dd"] = "2026-10-01"
    if corruption == "combined": current["futonly_or_combined"] = "Combined"
    result = parse_cftc_positions(json.dumps(rows).encode(), as_of=NOW)
    assert len(result.markets) == 7
    assert result.unavailable_sources == ("CFTC:gold",)


@pytest.mark.parametrize("body", [b"{}", b"[]", b"[null]", b"<html>blocked</html>"])
def test_invalid_response_fails_closed(body):
    with pytest.raises(ValueError): parse_cftc_positions(body, as_of=NOW)


@pytest.mark.parametrize("longs,shorts,old_longs,old_shorts,preference", [
    (90, 40, 100, 40, "long_weakens"), (120, 40, 100, 40, "long_strengthens"),
    (40, 120, 40, 100, "short_strengthens"), (40, 80, 40, 100, "short_weakens"),
    (120, 100, 80, 100, "flip_long"), (80, 100, 120, 100, "flip_short"),
    (100, 100, 120, 100, "flat"), (100, 40, 100, 40, "unchanged"),
])
def test_less_long_is_not_described_as_net_short(longs, shorts, old_longs, old_shorts, preference):
    assert TraderPositionChange("managed_money", longs, shorts, old_longs, old_shorts).preference == preference


def test_context_outage_still_delivers_valid_daily_report_with_explicit_notice():
    comparison = _comparison()
    class Source:
        def load(self, day):
            return {comparison.current.trading_day: comparison.current,
                    comparison.previous.trading_day: comparison.previous}.get(day)
    class Offline:
        name = "ICE"
        def load(self, **_kw): raise PositionSourceUnavailable("unavailable")
    class Publisher:
        def publish(self, report, *, generated_at, context):
            assert report == comparison and generated_at == NOW
            assert context == PositionContext((), ("ICE",))
            message = render_daily_positions_html(comparison_payload(report, context))
            assert "Нет проверенных данных: ICE" in message
            return True
    assert CollectDailyPositions(source=Source(), publisher=Publisher(), context_sources=(Offline(),)).execute(as_of=NOW).status == "queued"


def test_nine_markets_fit_one_message_with_exact_dates_labels_and_directions():
    context = _context()
    message = render_daily_positions_html(comparison_payload(_comparison(), context))
    assert len(message) <= 4096
    assert "Через брокеров" in message and "Напрямую на бирже" in message
    assert "2026-10-30 к 2026-10-29" in message
    assert "2026-10-27 к 2026-10-20" in message
    assert "Фонды: Л 150 (Δ+10), Ш 50 (Δ−10)" in message
    assert "Нетто +100 (Δ+20)" in message
    assert "Палладий" in message and "Brent" in message and "Henry Hub" in message
    assert "не сигнал в реальном времени" in message
    assert "quality_score" not in message and "вероятность" not in message
    payload = comparison_payload(_comparison(), context)
    assert len(payload["weekly_markets"][0]["groups"]) == 5
    # Ordering is not a domain invariant; headings must still use managed money.
    reordered = replace(context.markets[0], groups=tuple(reversed(context.markets[0].groups)))
    render_daily_positions_html(comparison_payload(_comparison(), PositionContext((reordered,))))


def test_different_reporting_dates_are_never_hidden_under_one_weekly_date():
    context = _context()
    older = replace(context.markets[-1], trading_day=date(2026, 10, 20), previous_day=date(2026, 10, 13))
    message = render_daily_positions_html(comparison_payload(_comparison(), PositionContext((context.markets[0], older))))
    assert "Нефть Brent · ICE · 2026-10-20 к 2026-10-13" in message


def test_weekly_cache_is_validated_and_retry_uses_original_raw_bytes(tmp_path):
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, content=_cftc_body())
    client = httpx.Client(transport=httpx.MockTransport(respond))
    source = WeeklyPositionSource("CFTC", cache_dir=tmp_path, client=client)
    result = source.load(as_of=NOW)
    assert source.load(as_of=NOW) == result
    assert len(requests) == 1 and len(list(tmp_path.glob("*.raw"))) == 1
    assert requests[0].url.params["$limit"] == "96"


def test_broken_source_is_unavailable_and_never_cached(tmp_path):
    client = httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(200, content=b"invalid")))
    with pytest.raises(PositionSourceUnavailable): WeeklyPositionSource("ICE", cache_dir=tmp_path, client=client).load(as_of=NOW)
    assert list(tmp_path.iterdir()) == []


def test_ice_year_boundary_reads_previous_annual_file(tmp_path):
    january = datetime(2027, 1, 6, tzinfo=timezone.utc)
    current = _ice_body(("01/05/2027",)).decode().splitlines()[:2]
    prior = _ice_body(("12/29/2026", "12/22/2026"))
    calls = []
    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(200, content=("\n".join(current).encode() if "2027" in request.url.path else prior))
    source = WeeklyPositionSource("ICE", cache_dir=tmp_path, client=httpx.Client(transport=httpx.MockTransport(respond)))
    result = source.load(as_of=january)
    assert len(calls) == 2
    assert result.markets[0].trading_day == date(2027, 1, 5)
    assert result.markets[0].previous_day == date(2026, 12, 29)


def test_digest_identity_freezes_weekly_enrichment_but_keeps_primary_source_strict():
    class Store:
        def __init__(self): self.events = []
        def persist_report_once(self, event, prepared):
            self.events.append(event)
            return StoredEvent((prepared.signal,), replayed=len(self.events) > 1)
    store = Store()
    publisher = PostgresDailyPositionPublisher(store=store, target=DeliveryTarget("telegram", "test"))
    assert publisher.publish(_comparison(), generated_at=NOW, context=_context())
    assert not publisher.publish(_comparison(), generated_at=NOW + timedelta(days=1), context=PositionContext((), ("ICE",)))
    assert store.events[0] == store.events[1]
    publisher.publish(_comparison(), generated_at=NOW)
    assert store.events[2].event_id != store.events[0].event_id
    changed = replace(_comparison().current, source_sha256="b" * 64)
    publisher.publish(DailyPositionComparison(changed, _comparison().previous), generated_at=NOW, context=_context())
    assert store.events[3].payload_sha256 != store.events[0].payload_sha256


def test_delivery_guard_accepts_known_digest_only_and_preserves_generation_limit():
    class Metrics:
        def stale_delivery_suppressed(self, **_kw): pass
    adapter = QueuedDeliveryRecoveryAdapter(DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120), metrics=Metrics(), clock=lambda: NOW,
    ), clock=lambda: NOW)
    payload = {**comparison_payload(_comparison(), _context()), "report_generated_at": NOW.isoformat()}
    task = DeliveryTask("out", "signal", "telegram", {"signal_type": REPORT_SIGNAL_TYPE, "payload": payload}, 1)
    assert payload["daily_report_version"] == DIGEST_VERSION
    assert adapter.evaluate(task).allow_external_delivery
    for changes in ({"primary_source": "OTHER"}, {"source_exchange": "SHFE"},
                    {"daily_report_version": "unknown"}, {"report_generated_at": (NOW - timedelta(hours=7)).isoformat()}):
        invalid = replace(task, payload={**task.payload, "payload": {**payload, **changes}})
        assert not adapter.evaluate(invalid).allow_external_delivery
