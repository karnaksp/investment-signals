from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.delivery_policy import DeliveryPolicy
from tinvest_signal_engine.models import TriggerSignal


def _settings(monkeypatch, **env: str) -> RuntimeSettings:
    for key in (
        "SIGNAL_DELIVERY_ENABLED",
        "SIGNAL_DELIVERY_MIN_QUALITY",
        "SIGNAL_DELIVERY_MAX_PER_HOUR",
        "SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS",
        "SIGNAL_DELIVERY_TYPE_RULES_JSON",
        "SIGNAL_MIN_QUALITY_SCORE",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return RuntimeSettings.from_env()


def _signal(**kwargs) -> TriggerSignal:
    defaults = {
        "signal_id": "00000000-0000-4000-8000-000000000001",
        "detected_at": datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "source_event_type": "trade",
        "signal_type": "volume_spike",
        "severity": 2,
        "metric_value": 100.0,
        "baseline_value": 10.0,
        "z_score": 4.0,
        "window_seconds": 60,
        "summary": "x",
        "payload": {"quality_score": 70},
    }
    defaults.update(kwargs)
    return TriggerSignal(**defaults)


def test_combo_score_six_is_delivered(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))
    sig = _signal(
        signal_type="microstructure_combo_long",
        z_score=0.0,
        payload={"quality_score": 30, "score": 6},
    )

    out = policy.apply(sig)

    assert out.payload["delivery_status"] == "delivered"
    assert out.payload["delivery_reason"] == "combo_score_ge_6"
    assert out.payload["delivery_policy_version"] == "delivery_v3"
    assert out.payload["delivery_priority"] == "high"
    assert out.payload["delivery_channel"] == "realtime"
    assert out.payload["delivery_explanation_ru"]


def test_validated_bond_convergence_is_delivered(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))
    out = policy.apply(
        _signal(
            signal_type="bond_maturity_convergence",
            source_event_type="bond_convergence_observation",
            z_score=0.0,
            payload={
                "quality_score": 94,
                "historical_success_rate": 0.9365,
                "historical_wilson_lower_bound": 0.8923,
                "historical_eligible_observations": 189,
            },
        )
    )

    assert out.payload["delivery_status"] == "delivered"
    assert out.payload["delivery_reason"] == "validated_bond_convergence"


def test_low_quality_spread_is_suppressed_without_context(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))
    sig = _signal(
        signal_type="spread_widening",
        source_event_type="orderbook",
        z_score=3.8,
        payload={"quality_score": 50},
    )

    out = policy.apply(sig)

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "liquidity_without_context"
    assert out.payload["delivery_priority"] in {"low", "medium"}
    assert out.payload["delivery_channel"] == "admin_only"
    assert out.payload["delivery_explanation_ru"]


def test_liquidity_signal_delivered_near_activity_context(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch, SIGNAL_DELIVERY_MIN_QUALITY="90"))
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    policy.apply(
        _signal(
            detected_at=start,
            signal_type="volume_spike",
            payload={"quality_score": 40},
            z_score=3.0,
        )
    )
    out = policy.apply(
        _signal(
            detected_at=start + timedelta(minutes=1),
            signal_type="orderbook_imbalance",
            source_event_type="orderbook",
            payload={"quality_score": 65},
            z_score=3.5,
        )
    )

    assert out.payload["delivery_status"] == "delivered"
    assert out.payload["delivery_reason"] == "liquidity_near_activity"


def test_rate_limit_suppresses_excess_delivery(monkeypatch) -> None:
    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_MAX_PER_HOUR="1",
            SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS="0",
        )
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    first = policy.apply(
        _signal(detected_at=start, payload={"quality_score": 90}, z_score=6.5)
    )
    second = policy.apply(
        _signal(
            signal_id="00000000-0000-4000-8000-000000000002",
            detected_at=start + timedelta(minutes=10),
            payload={"quality_score": 90},
            z_score=6.5,
            instrument_id="GAZP_TQBR",
            ticker="GAZP",
            alias="gazp",
        )
    )

    assert first.payload["delivery_status"] == "delivered"
    assert second.payload["delivery_status"] == "suppressed"
    assert second.payload["delivery_reason"] == "rate_limit_per_hour"


def test_legacy_quality_floor_applies_only_to_delivery(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch, SIGNAL_MIN_QUALITY_SCORE="85"))
    out = policy.apply(_signal(payload={"quality_score": 70}))

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "momentum_below_quality_and_z"


def test_price_jump_requires_extreme_or_activity_context(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))

    plain = policy.apply(
        _signal(
            signal_type="price_jump",
            payload={"quality_score": 82},
            z_score=6.2,
        )
    )
    extreme = policy.apply(
        _signal(
            signal_id="00000000-0000-4000-8000-000000000003",
            instrument_id="GAZP_TQBR",
            ticker="GAZP",
            alias="gazp",
            signal_type="price_jump",
            payload={"quality_score": 95},
            z_score=8.2,
        )
    )

    assert plain.payload["delivery_status"] == "suppressed"
    assert plain.payload["delivery_reason"] == "price_without_confirmation"
    assert extreme.payload["delivery_status"] == "delivered"
    assert extreme.payload["delivery_reason"] == "price_extreme_quality_and_z"


def test_liquidity_high_quality_without_context_is_suppressed(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))

    out = policy.apply(
        _signal(
            signal_type="spread_widening",
            source_event_type="orderbook",
            payload={"quality_score": 90},
            z_score=20.0,
        )
    )

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "liquidity_without_context"


def test_instrument_cooldown_suppresses_repeated_delivery(monkeypatch) -> None:
    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_MAX_PER_HOUR="10",
            SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS="900",
        )
    )
    start = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    first = policy.apply(
        _signal(detected_at=start, payload={"quality_score": 90}, z_score=6.5)
    )
    second = policy.apply(
        _signal(
            signal_id="00000000-0000-4000-8000-000000000004",
            detected_at=start + timedelta(minutes=5),
            signal_type="trade_rate_spike",
            payload={"quality_score": 95},
            z_score=8.0,
        )
    )

    assert first.payload["delivery_status"] == "delivered"
    assert second.payload["delivery_status"] == "suppressed"
    assert second.payload["delivery_reason"] == "instrument_cooldown"


def test_persistent_rate_limit_survives_restart(monkeypatch) -> None:
    def count_since(
        since: datetime, instrument_id: str | None, signal_type: str | None
    ) -> int:
        assert instrument_id is None
        assert signal_type is None
        return 1

    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_MAX_PER_HOUR="1",
            SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS="0",
        ),
        delivered_count_since=count_since,
    )

    out = policy.apply(_signal(payload={"quality_score": 90}, z_score=6.5))

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "rate_limit_per_hour"


def test_experimental_signal_is_admin_only_by_default(monkeypatch) -> None:
    policy = DeliveryPolicy(_settings(monkeypatch))
    out = policy.apply(
        _signal(
            signal_type="candle_range_spike",
            source_event_type="candle",
            payload={"quality_score": 99},
            z_score=20.0,
        )
    )

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "experimental_admin_only"
    assert out.payload["delivery_rule"] == "controlled_rollout_admin_only"
    assert out.payload["delivery_channel"] == "admin_only"


def test_custom_rule_can_explicitly_promote_experimental_signal(monkeypatch) -> None:
    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_TYPE_RULES_JSON=json.dumps(
                {"candle_range_spike": {"always": True}}
            ),
        )
    )

    out = policy.apply(
        _signal(
            signal_type="candle_range_spike",
            source_event_type="candle",
            payload={"quality_score": 20},
            z_score=1.0,
        )
    )

    assert out.payload["delivery_status"] == "delivered"
    assert out.payload["delivery_reason"] == "type_rule_always"
    assert out.payload["delivery_channel"] == "realtime"


def test_custom_rule_can_hold_signal_as_digest_candidate(monkeypatch) -> None:
    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_TYPE_RULES_JSON=json.dumps(
                {"candle_range_spike": {"channel": "digest", "min_quality": 70}}
            ),
        )
    )

    out = policy.apply(
        _signal(
            signal_type="candle_range_spike",
            source_event_type="candle",
            payload={"quality_score": 90},
            z_score=3.0,
        )
    )

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "type_rule_digest"
    assert out.payload["delivery_channel"] == "digest"
    assert out.payload["delivery_priority"] == "medium"


def test_custom_rule_can_force_admin_only(monkeypatch) -> None:
    policy = DeliveryPolicy(
        _settings(
            monkeypatch,
            SIGNAL_DELIVERY_TYPE_RULES_JSON=json.dumps(
                {"volume_spike": {"admin_only": True}}
            ),
        )
    )

    out = policy.apply(_signal(payload={"quality_score": 95}, z_score=9.0))

    assert out.payload["delivery_status"] == "suppressed"
    assert out.payload["delivery_reason"] == "type_rule_admin_only"
    assert out.payload["delivery_channel"] == "admin_only"
