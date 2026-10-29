from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import math

import pytest

from tinvest_signal_engine.domain.futures_curve import (
    CurveContract,
    CurveMarketEvent,
    CurveSettings,
    FuturesCurveDetector,
)


NOW = datetime(2026, 10, 29, 10, tzinfo=timezone.utc)


def _contracts(days=(30, 90, 180)):
    return tuple(
        CurveContract(str(index), f"F{index}", "usd_rub", NOW + timedelta(days=day), 1000, specification_key="USD/RUB:1000")
        for index, day in enumerate(days)
    )


def _settings(signal_type="calendar_spread_dislocation", **kwargs):
    values = dict(
        enabled_types=(signal_type,), sample_interval_seconds=1,
        min_baseline_points=3, min_warmup_seconds=2,
        min_total_notional_rub=0, min_leg_notional_rub=0,
        min_leg_trades=0, persistence_seconds=1,
        min_deviation_bps=5, min_kink_deviation_bps=5,
        min_momentum_bps=5, dispersion_floor_bps=0.1,
        carry_dispersion_floor=0.0001, min_carry_change=0.001,
        momentum_horizon_seconds=3, roll_window_seconds=3,
        cooldown_seconds=30,
    )
    values.update(kwargs)
    return CurveSettings(**values)


class Feed:
    def __init__(self, detector):
        self.detector = detector
        self.sequence = 0

    def event(self, instrument, second, kind="orderbook", **kwargs):
        self.sequence += 1
        at = NOW + timedelta(seconds=second)
        return self.detector.process(CurveMarketEvent(str(self.sequence), str(instrument), at, kind, **kwargs))

    def quotes(self, second, prices=(100, 101, 102)):
        signals = []
        for index, price in enumerate(prices):
            signals.extend(self.event(index, second, bid=price - 0.0001, ask=price + 0.0001))
        return signals

    def sample(self, second, prices=(100, 101, 102), quantities=(1, 1, 1), oi=(10000, 10000, 10000)):
        signals = list(self.quotes(second, prices))
        for index in range(len(prices)):
            self.event(index, second, "trade", quantity=quantities[index])
            self.event(index, second, "open_interest", open_interest=oi[index])
        signals.extend(self.quotes(second, prices))
        return signals


def _warm(feed, count=8):
    for second in range(count):
        assert not feed.sample(second)


@pytest.mark.parametrize(
    ("signal_type", "prices"),
    [
        ("calendar_spread_dislocation", (100, 102, 102)),
        ("term_premium_shock", (100, 102, 102)),
        ("futures_curve_kink", (100, 103, 102)),
        ("futures_basis_momentum", (100, 102, 102)),
    ],
)
def test_strong_price_events_require_prior_baseline_and_persistence(signal_type, prices):
    detector = FuturesCurveDetector(_contracts(), _settings(signal_type))
    feed = Feed(detector)
    _warm(feed)
    assert not feed.sample(8, prices)
    signals = feed.sample(9, prices)
    assert len(signals) == 1
    signal = signals[0]
    assert signal.signal_type == signal_type
    assert abs(signal.z_score) >= detector.settings.z_threshold
    assert signal.payload["curve_strong"] is True
    assert signal.payload["evidence_status"] == "experimental"
    assert signal.payload["baseline_points"] >= 3
    assert signal.payload["persistence_seconds"] == 1
    assert all(leg["expiration_at"] for leg in signal.payload["legs"])
    assert not feed.sample(10, prices)


def test_roll_requires_oi_migration_volume_transfer_and_spread_support():
    detector = FuturesCurveDetector(_contracts(), _settings("roll_migration_anomaly"))
    feed = Feed(detector)
    _warm(feed)
    shock = dict(prices=(100, 102, 102), quantities=(1, 100, 1), oi=(7000, 13000, 10000))
    assert not feed.sample(8, **shock)
    signals = feed.sample(9, **shock)
    assert len(signals) == 1
    assert signals[0].signal_type == "roll_migration_anomaly"
    payload = signals[0].payload
    assert payload["near_oi_delta"] < 0 < payload["far_oi_delta"]
    assert payload["matched_contracts"] >= 500
    assert payload["volume_share_change"] >= 0.15
    assert payload["direction"] == "migration_far"


@pytest.mark.parametrize("bad", [None, 0, -1, float("nan"), float("inf")])
def test_invalid_quote_invalidates_previous_quote_and_pending_candidate(bad):
    detector = FuturesCurveDetector(_contracts()[:2], _settings())
    feed = Feed(detector)
    _warm(feed)
    feed.sample(8, prices=(100, 102, 102))
    assert not feed.event(0, 9, bid=bad, ask=100.001)
    assert detector._legs["0"].quote is None
    assert not feed.event(1, 9, bid=101.9999, ask=102.0001)
    assert not feed.quotes(10, (100, 102, 102))


def test_crossed_book_does_not_retain_last_good_quote():
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    feed.quotes(0)
    feed.event(0, 1, bid=101, ask=100)
    assert detector._legs["0"].quote is None


def test_stale_and_asynchronous_quotes_cannot_create_signal():
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    _warm(feed)
    assert not feed.event(1, 15, bid=102, ask=102.001)
    assert not feed.event(0, 16, bid=100, ask=100.001, received_at=NOW + timedelta(seconds=30))
    assert not detector._pending


def test_out_of_order_message_does_not_overwrite_new_quote():
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    feed.quotes(10)
    before = detector._legs["0"].quote
    assert not feed.event(0, 8, bid=110, ask=111)
    assert detector._legs["0"].quote == before


def test_duplicate_trade_is_not_double_counted():
    detector = FuturesCurveDetector(_contracts(), _settings())
    Feed(detector).quotes(0)
    trade = CurveMarketEvent("trade", "0", NOW, "trade", quantity=10)
    detector.process(trade)
    detector.process(trade)
    assert detector._activity(_contracts()[:2], NOW)[1][0] == 1


def test_low_activity_prevents_alert_despite_price_extreme():
    detector = FuturesCurveDetector(_contracts(), _settings(min_total_notional_rub=20_000_000, min_leg_notional_rub=5_000_000, min_leg_trades=20))
    feed = Feed(detector)
    _warm(feed)
    assert not feed.sample(8, prices=(100, 102, 102))
    assert not feed.sample(9, prices=(100, 102, 102))


def test_activity_gates_require_each_leg_not_only_large_total():
    detector = FuturesCurveDetector(_contracts(), _settings(min_total_notional_rub=20_000_000, min_leg_notional_rub=5_000_000, min_leg_trades=1))
    feed = Feed(detector)
    _warm(feed)
    shock = dict(prices=(100, 102, 102), quantities=(1, 10000, 1))
    assert not feed.sample(8, **shock)
    assert not feed.sample(9, **shock)


def test_strong_event_with_real_activity_gates_passes():
    detector = FuturesCurveDetector(_contracts()[:2], _settings(min_total_notional_rub=20_000_000, min_leg_notional_rub=5_000_000, min_leg_trades=20))
    feed = Feed(detector)
    _warm(feed)
    feed.quotes(8, (100, 102))
    for index in (0, 1):
        for _ in range(20):
            feed.event(index, 8, "trade", quantity=10)
    assert not feed.quotes(8, (100, 102))
    signals = feed.quotes(9, (100, 102))
    assert len(signals) == 1
    assert signals[0].payload["window_notional"] >= 20_000_000
    assert all(leg["trade_count"] >= 20 for leg in signals[0].payload["legs"])


def test_lone_growth_of_far_oi_is_not_mislabelled_as_roll():
    detector = FuturesCurveDetector(_contracts(), _settings("roll_migration_anomaly"))
    feed = Feed(detector)
    _warm(feed)
    shock = dict(prices=(100, 102, 102), quantities=(1, 100, 1), oi=(10000, 18000, 10000))
    assert not feed.sample(8, **shock)
    assert not feed.sample(9, **shock)


def test_feed_gap_restarts_reference_instead_of_alerting_on_old_session():
    detector = FuturesCurveDetector(_contracts(), _settings(max_sample_gap_seconds=10))
    feed = Feed(detector)
    _warm(feed)
    assert not feed.sample(30, (100, 102, 102))
    assert not feed.sample(31, (100, 102, 102))


def test_future_dated_event_does_not_poison_newer_valid_source_sequence():
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    feed.quotes(0)
    assert not feed.event(0, 100, bid=200, ask=201, received_at=NOW)
    feed.event(0, 1, bid=100, ask=100.001)
    assert detector._legs["0"].quote[0] == NOW + timedelta(seconds=1)


def test_disabled_emission_updates_reference_without_burning_cooldown():
    detector = FuturesCurveDetector(_contracts()[:2], _settings())
    for second in range(8):
        for index, price in enumerate((100, 101)):
            event = CurveMarketEvent(f"warm-{second}-{index}", str(index), NOW + timedelta(seconds=second), "orderbook", bid=price - 0.0001, ask=price + 0.0001)
            assert not detector.process(event, emit_signals=False)
    assert detector._history
    for second in (8, 9):
        for index, price in enumerate((100, 102)):
            event = CurveMarketEvent(f"quiet-{second}-{index}", str(index), NOW + timedelta(seconds=second), "orderbook", bid=price - 0.0001, ask=price + 0.0001)
            assert not detector.process(event, emit_signals=False)
    assert not detector._pending
    assert not detector._cooldowns
    feed = Feed(detector)
    assert not feed.quotes(10, (100, 102))
    assert len(feed.quotes(11, (100, 102))) == 1


@pytest.mark.parametrize("price", [math.nan, math.inf, 0, -1])
def test_invalid_trade_price_never_inflates_activity(price):
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    feed.quotes(0)
    feed.event(0, 0, "trade", quantity=10, trade_price=price)
    assert detector._activity(_contracts()[:2], NOW)[0] == [0, 0]


def test_trade_notional_numeric_overflow_is_rejected():
    detector = FuturesCurveDetector(_contracts(), _settings())
    Feed(detector).event(0, 0, "trade", quantity=1e300, trade_price=1e300)
    assert not detector._legs["0"].trades


def test_recent_volume_buckets_are_bounded_and_expire():
    detector = FuturesCurveDetector(_contracts(), _settings(activity_window_seconds=3))
    feed = Feed(detector)
    for second in range(15):
        feed.sample(second)
        for _ in range(30):
            feed.event(0, second, "trade", quantity=1)
    assert len(detector._legs["0"].trades) <= 3
    assert detector._activity(_contracts()[:2], NOW + timedelta(seconds=30))[0] == [0, 0]


def test_expired_contract_is_excluded():
    contracts = (replace(_contracts()[0], expiration_at=NOW), *_contracts()[1:])
    detector = FuturesCurveDetector(contracts, _settings())
    feed = Feed(detector)
    _warm(feed)
    assert not feed.sample(8, (150, 101, 102))
    assert not feed.sample(9, (150, 101, 102))


@pytest.mark.parametrize("replacement", [dict(currency="USD"), dict(price_multiplier=0), dict(price_multiplier=math.nan), dict(specification_key="different")])
def test_incompatible_contract_does_not_form_pair(replacement):
    first, second, *_ = _contracts()
    detector = FuturesCurveDetector((first, replace(second, **replacement)), _settings())
    assert not detector._groups


def test_equal_expirations_cannot_form_spread():
    first, second, *_ = _contracts()
    detector = FuturesCurveDetector((first, replace(second, expiration_at=first.expiration_at)), _settings())
    feed = Feed(detector)
    for second in range(10):
        assert not feed.quotes(second, (100, 101))


def test_kink_uses_expiration_weights_for_unequal_spacing():
    contracts = _contracts(days=(30, 90, 210))
    detector = FuturesCurveDetector(contracts, _settings("futures_curve_kink"))
    feed = Feed(detector)
    fair = (100, math.exp(math.log(100) * 2 / 3 + math.log(103) / 3), 103)
    for second in range(8):
        assert not feed.sample(second, fair)
    assert abs(detector._history["usd_rub|0,1,2|futures_curve_kink"][-1][1]) < 1e-8
    assert not feed.sample(8, (100, fair[1] * 1.01, 103))
    assert feed.sample(9, (100, fair[1] * 1.01, 103))


def test_momentum_never_uses_shorter_than_configured_horizon():
    detector = FuturesCurveDetector(_contracts(), _settings("futures_basis_momentum", momentum_horizon_seconds=100))
    feed = Feed(detector)
    _warm(feed)
    for second in range(8, 20):
        assert not feed.sample(second, (100, 102, 102))
    assert not any(key.endswith("futures_basis_momentum") for key in detector._history)


def test_correlated_price_types_share_cooldown():
    detector = FuturesCurveDetector(_contracts(), _settings(enabled_types=("calendar_spread_dislocation", "term_premium_shock", "futures_curve_kink", "futures_basis_momentum")))
    feed = Feed(detector)
    _warm(feed)
    signals = feed.sample(8, (100, 103, 102)) + feed.sample(9, (100, 103, 102))
    assert len(signals) == 1
    for second in range(10, 20):
        assert not feed.sample(second, (100, 103, 102))


def test_state_roundtrip_preserves_baseline_and_cooldown():
    contracts, settings = _contracts(), _settings()
    detector = FuturesCurveDetector(contracts, settings)
    feed = Feed(detector)
    _warm(feed)
    saved = json.loads(json.dumps(detector.export_state()))
    restored = FuturesCurveDetector(contracts, settings)
    assert restored.restore_state(saved)
    resumed = Feed(restored)
    resumed.sequence = feed.sequence
    assert not resumed.sample(8, (100, 102, 102))
    assert len(resumed.sample(9, (100, 102, 102))) == 1
    assert restored.restore_state(json.loads(json.dumps(restored.export_state())))
    assert not resumed.sample(10, (100, 102, 102))


def test_state_with_invalid_values_or_changed_spec_is_rejected():
    detector = FuturesCurveDetector(_contracts(), _settings())
    feed = Feed(detector)
    _warm(feed)
    saved = detector.export_state()
    changed = FuturesCurveDetector(_contracts(), _settings(min_deviation_bps=50))
    assert not changed.restore_state(saved)
    saved["legs"]["0"]["quote"][1] = math.nan
    assert not detector.restore_state(saved)
    assert not detector._history


@pytest.mark.parametrize("kwargs", [dict(sample_interval_seconds=0), dict(min_baseline_points=1), dict(z_threshold=math.nan), dict(dispersion_floor_bps=0), dict(enabled_types=("unknown",))])
def test_invalid_configuration_fails_explicitly(kwargs):
    with pytest.raises(ValueError):
        CurveSettings(**kwargs)
