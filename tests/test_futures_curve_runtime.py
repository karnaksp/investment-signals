from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
import yaml

from tinvest_signal_engine.adapters.futures_curve_runtime import (
    FuturesCurveRuntimeAdapter, load_curve_configuration,
)
from tinvest_signal_engine.adapters.legacy_detection import LegacyDetectionAdapter
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.delivery_policy import DeliveryPolicy
from tinvest_signal_engine.domain.futures_curve import FuturesCurveDetector
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal
from tinvest_signal_engine.signal_enrichment import enrich_signal_for_delivery


NOW = datetime(2026, 10, 29, 12, tzinfo=timezone.utc)
TYPES = ("calendar_spread_dislocation", "roll_migration_anomaly", "futures_curve_kink",
         "term_premium_shock", "futures_basis_momentum")


def _files(tmp_path, *, currency="rub", increment=1, amount=1):
    instruments = tmp_path / "instruments.yaml"
    policy = tmp_path / "curve.json"
    instruments.write_text(yaml.safe_dump({"instruments": [{
        "ticker": "SiZ6", "class_code": "SPBFUT", "alias": "usd_rub",
        "futures_contract": {
            "chain_id": "usd_rub", "basic_asset": "USD/RUB", "expiration_at": "2026-12-17T00:00:00Z",
            "basic_asset_size": 1000, "min_price_increment": increment,
            "min_price_increment_amount": amount, "settlement_currency": currency,
            "settlement_type": "physical",
        },
    }]}), encoding="utf-8")
    policy.write_text(json.dumps({"version": 1, "enabled": True, "settings": {}}))
    return instruments, policy


def test_curve_adapter_keeps_price_and_cash_units_separate(tmp_path):
    paths = _files(tmp_path, increment=0.5, amount=5)
    config = load_curve_configuration(*paths)
    assert len(config.contracts) == 1
    assert config.contracts[0].price_multiplier == 10
    assert config.contracts[0].currency == "RUB"
    assert config.contracts[0].chain_id == "usd_rub"


@pytest.mark.parametrize("currency,increment,amount", [
    ("usd", 1, 1), ("rub", 0, 1), ("rub", 1, 0), ("rub", 1, float("nan")),
])
def test_unknown_monetary_specs_cannot_produce_cash_activity(tmp_path, currency, increment, amount):
    assert load_curve_configuration(*_files(tmp_path, currency=currency, increment=increment, amount=amount)).contracts == ()


def _event(kind="orderbook", **payload):
    return NormalizedEvent(
        event_id="wire-event", event_type=kind, instrument_id="SiZ6_SPBFUT",
        ticker="SiZ6", class_code="SPBFUT", alias="usd_rub", figi="", uid="",
        lot=1, source_time=NOW, received_at=NOW, payload=payload,
    )


class _Engine:
    def __init__(self):
        self.events = []

    def process(self, event, *, emit_signals=True):
        self.events.append(event)
        return ()

    def export_state(self):
        return {"format_version": 1}

    def restore_state(self, _state):
        return False


def _adapter(tmp_path):
    paths = _files(tmp_path)
    config = load_curve_configuration(*paths)
    engine = _Engine()
    adapter = FuturesCurveRuntimeAdapter(
        factory=lambda: (engine, config), instruments_path=paths[0], policy_path=paths[1],
        state_path=tmp_path / "state.json",
        clock=lambda: NOW,
    )
    return adapter, engine


def test_wire_book_selects_best_prices_and_preserves_timestamps(tmp_path):
    adapter, engine = _adapter(tmp_path)
    adapter.process(_event(bids=[{"price": 100}, {"price": 101}], asks=[{"price": 103}, {"price": 102}]), emit_signals=True)
    mapped = engine.events[0]
    assert (mapped.bid, mapped.ask) == (101, 102)
    assert mapped.at == NOW and mapped.received_at == NOW


def test_inconsistent_or_empty_book_invalidates_previous_quote(tmp_path):
    adapter, engine = _adapter(tmp_path)
    adapter.process(_event(bids=[{"price": 100}], asks=[{"price": 101}], is_consistent=False), emit_signals=True)
    assert engine.events[0].bid is None and engine.events[0].ask is None


def test_recovery_backfill_cannot_seed_curve_or_send_notifications(tmp_path):
    adapter, engine = _adapter(tmp_path)
    assert adapter.process(_event("trade", quantity=1000, recovery_backfill=True), emit_signals=True) == ()
    assert engine.events == []


def _signal(kind=TYPES[0], **overrides):
    legs = [{"instrument_id": f"{ticker}_SPBFUT", "ticker": ticker,
             "notional_rub": 10_000_000, "trade_count": 30}
            for ticker in ("SiZ6", "SiH7", "SiM7")]
    payload = {
        "curve_strong": True, "curve_version": "futures_curve_v1", "evidence_status": "experimental",
        "baseline_points": 120, "persistence_seconds": 30, "quote_age_seconds": 1,
        "quote_skew_seconds": 1, "window_notional": 30_000_000, "legs": legs,
    }
    payload.update(overrides)
    return TriggerSignal(
        signal_id="00000000-0000-4000-8000-000000000001", detected_at=NOW,
        instrument_id="SiZ6_SPBFUT", ticker="SiZ6", class_code="SPBFUT", alias="usd_rub",
        source_event_type="orderbook", signal_type=kind, severity=3, metric_value=25,
        baseline_value=0, z_score=5, window_seconds=300, summary="curve", payload=payload,
    )


def _policy(monkeypatch, **extra):
    monkeypatch.setenv("SIGNAL_DELIVERY_NOTIONAL_POLICY_FILE", "conf/delivery-notional-policy.json")
    monkeypatch.setenv("SIGNAL_DELIVERY_TYPE_RULES_JSON", "{}")
    monkeypatch.setenv("SIGNAL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("SIGNAL_DELIVERY_MAX_PER_HOUR", "30")
    monkeypatch.setenv("SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS", "0")
    return DeliveryPolicy(RuntimeSettings.from_env(), **extra)


@pytest.mark.parametrize("kind", TYPES)
def test_only_strong_experimental_curve_observations_reach_realtime(monkeypatch, kind):
    out = _policy(monkeypatch).apply(_signal(kind))
    assert out.payload["delivery_status"] == "delivered"
    assert out.payload["delivery_reason"] == "futures_curve_strong_observation"


@pytest.mark.parametrize("field,value", [
    ("curve_strong", False), ("baseline_points", 119), ("persistence_seconds", 29),
    ("quote_age_seconds", 5.01), ("quote_skew_seconds", 2.01), ("quote_age_seconds", float("nan")),
    ("legs", []), ("evidence_status", "applied"),
])
def test_custom_always_cannot_bypass_curve_safety_gate(monkeypatch, field, value):
    policy = _policy(monkeypatch)
    policy._type_rules = {TYPES[0]: {"always": True}}
    out = policy.apply(_signal(**{field: value}))
    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "futures_curve_below_strong_gate"


def test_curve_group_has_separate_six_per_hour_limit(monkeypatch):
    policy = _policy(monkeypatch)
    for index in range(6):
        assert policy.apply(replace(_signal(), detected_at=NOW + timedelta(minutes=index))).payload["delivery_status"] == "delivered"
    assert policy.apply(replace(_signal(), detected_at=NOW + timedelta(minutes=6))).payload["delivery_reason"] == "futures_curve_hourly_limit"


def test_curve_hourly_limit_survives_restart_and_history_failure(monkeypatch):
    policy = _policy(monkeypatch, delivered_count_since=lambda *_args: 2)
    assert policy.apply(_signal()).payload["delivery_reason"] == "futures_curve_hourly_limit"
    def broken(*_args):
        raise RuntimeError("history offline")
    policy = _policy(monkeypatch, delivered_count_since=broken)
    assert policy.apply(_signal()).payload["delivery_status"] == "suppressed"


def test_real_curve_to_russian_realtime_pipeline_uses_strict_live_defaults(tmp_path, monkeypatch):
    instruments, policy_path = _files(tmp_path)
    raw = yaml.safe_load(instruments.read_text())
    second = json.loads(json.dumps(raw["instruments"][0]))
    second.update(ticker="SiH7", alias="usd_rub__SiH7")
    second["futures_contract"]["expiration_at"] = "2027-03-19T00:00:00Z"
    raw["instruments"].append(second)
    instruments.write_text(yaml.safe_dump(raw))
    policy_path.write_text(json.dumps({"version": 1, "enabled": True,
                                      "settings": {"enabled_types": ["term_premium_shock"]}}))
    def factory():
        config = load_curve_configuration(instruments, policy_path)
        return FuturesCurveDetector(config.contracts, config.settings), config
    adapter = FuturesCurveRuntimeAdapter(factory=factory, instruments_path=instruments,
                                        policy_path=policy_path, state_path=tmp_path / "state.json",
                                        clock=lambda: NOW)
    assert adapter.is_secondary("SiH7_SPBFUT")
    sequence = 0
    def feed(second, prices, *, trades=False):
        nonlocal sequence
        found = []
        for ticker, price in zip(("SiZ6", "SiH7"), prices):
            sequence += 1
            event = replace(_event(bids=[{"price": price - 0.01}], asks=[{"price": price + 0.01}]),
                            event_id=str(sequence), instrument_id=f"{ticker}_SPBFUT", ticker=ticker,
                            source_time=NOW + timedelta(seconds=second), received_at=NOW + timedelta(seconds=second))
            found.extend(adapter.process(event, emit_signals=True))
            if trades:
                for _ in range(20):
                    sequence += 1
                    adapter.process(replace(event, event_id=str(sequence), event_type="trade",
                                            payload={"price": price, "quantity": 10}), emit_signals=True)
        return found
    for second in range(0, 3630, 30):
        assert not feed(second, (80_000, 81_000), trades=True)
    signals = []
    for second in range(3630, 3662):
        signals.extend(feed(second, (80_000, 82_000)))
    assert len(signals) == 1
    assert signals[0].signal_type == "term_premium_shock"
    assert signals[0].payload["annual_premium_change"] > 0.03
    enriched = enrich_signal_for_delivery(signals[0])
    assert enriched.payload["quality_tier"] == "high"
    out = _policy(monkeypatch).apply(enriched)
    assert out.payload["delivery_status"] == "delivered"
    assert "SiZ6" in out.payload["telegram_html"] and "SiH7" in out.payload["telegram_html"]
    assert "годовых" in out.payload["telegram_html"]


def test_curve_process_does_not_write_uncommitted_state(tmp_path):
    adapter, _ = _adapter(tmp_path)
    adapter.process(_event(bids=[{"price": 100}], asks=[{"price": 101}]), emit_signals=True)
    assert not (tmp_path / "state.json").exists()
    adapter.checkpoint()
    assert (tmp_path / "state.json").exists()


def test_postgres_rollback_clears_uncommitted_curve_state(tmp_path):
    paths = _files(tmp_path)
    def factory():
        config = load_curve_configuration(*paths)
        return FuturesCurveDetector(config.contracts, config.settings), config
    curve = FuturesCurveRuntimeAdapter(factory=factory, instruments_path=paths[0],
                                      policy_path=paths[1], state_path=tmp_path / "state.json",
                                      clock=lambda: NOW)
    adapter = LegacyDetectionAdapter(RuntimeSettings.from_env(), delivered_count_since=lambda **_kw: 0,
                                     futures_curve=curve)
    event = _event(bids=[{"price": 100}], asks=[{"price": 101}])
    adapter.detect_batch(event.to_dict())
    assert curve._engine._legs[event.instrument_id].quote is not None
    # ReliableEventProcessor calls replace_state when persistence rolls back.
    adapter.replace_state(())
    assert curve._engine._legs[event.instrument_id].quote is None
    assert not curve._engine._cooldowns


def test_kafka_queue_delay_cannot_make_stale_quotes_look_fresh(tmp_path):
    paths = _files(tmp_path)
    config = load_curve_configuration(*paths)
    engine = FuturesCurveDetector(config.contracts, config.settings)
    processing_time = NOW + timedelta(seconds=6)
    adapter = FuturesCurveRuntimeAdapter(
        factory=lambda: (engine, config), instruments_path=paths[0],
        policy_path=paths[1], state_path=tmp_path / "state.json",
        clock=lambda: processing_time,
    )
    event = _event(bids=[{"price": 100}], asks=[{"price": 101}])
    adapter.process(event, emit_signals=True)
    assert engine._legs[event.instrument_id].quote is None

    fresh = replace(event, event_id="fresh", source_time=processing_time,
                    received_at=processing_time)
    adapter.process(fresh, emit_signals=True)
    assert engine._legs[event.instrument_id].quote is not None
