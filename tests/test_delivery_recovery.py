from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from tinvest_signal_engine.adapters.legacy_detection import LegacyDetectionAdapter
from tinvest_signal_engine.adapters.delivery_recovery import (
    QueuedDeliveryRecoveryAdapter,
)
from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.config import RuntimeSettings
from tinvest_signal_engine.delivery_policy import DeliveryPolicy
from tinvest_signal_engine.domain.delivery_recovery import (
    DELIVERY_EVENT_AGE_EXCEEDED,
    DELIVERY_EVENT_CROSSED_SESSION,
    DELIVERY_EVENT_FRESH,
    DELIVERY_EVENT_TIME_IN_FUTURE,
    DELIVERY_EVENT_TIME_UNAVAILABLE,
    DeliveryFreshnessPolicy,
)
from tinvest_signal_engine.models import TriggerSignal
from tinvest_signal_engine.domain.reliable_processing import DeliveryTask


@dataclass
class _Metrics:
    suppressions: list[tuple[str, str]] = field(default_factory=list)

    def stale_delivery_suppressed(
        self,
        *,
        reason_code: str,
        signal_type: str,
    ) -> None:
        self.suppressions.append((reason_code, signal_type))


def _signal(*, source_event_at: datetime) -> TriggerSignal:
    return TriggerSignal(
        signal_id="00000000-0000-4000-8000-000000000001",
        detected_at=source_event_at,
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="volume_spike",
        severity=2,
        metric_value=100.0,
        baseline_value=10.0,
        z_score=6.5,
        window_seconds=60,
        summary="volume",
        payload={"quality_score": 95},
        source_event_id="event-1",
        source_event_at=source_event_at,
    )


def _adapter(
    *,
    evaluated_at: datetime,
    maximum_age_seconds: int = 120,
) -> tuple[LegacyDetectionAdapter, _Metrics]:
    metrics = _Metrics()
    guard = DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=maximum_age_seconds),
        metrics=metrics,
        clock=lambda: evaluated_at,
    )
    adapter = object.__new__(LegacyDetectionAdapter)
    adapter._delivery_recovery_guard = guard
    adapter._policy = DeliveryPolicy(RuntimeSettings.from_env())
    return adapter, metrics


def test_fresh_event_at_exact_age_boundary_can_reach_delivery_policy() -> None:
    evaluated_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    adapter, metrics = _adapter(evaluated_at=evaluated_at)

    governed = adapter._govern_delivery(
        _signal(source_event_at=evaluated_at - timedelta(seconds=120))
    )

    assert governed.payload["delivery_status"] == "delivered"
    assert governed.payload["delivery_reason_code"] == "momentum_quality_and_z"
    assert metrics.suppressions == []


def test_old_event_is_retained_but_suppressed_with_explicit_reason() -> None:
    evaluated_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    adapter, metrics = _adapter(evaluated_at=evaluated_at)
    signal = _signal(source_event_at=evaluated_at - timedelta(seconds=121))

    first = adapter._govern_delivery(signal)
    second = adapter._govern_delivery(signal)

    assert first == second
    assert first.payload["delivery_status"] == "suppressed"
    assert first.payload["delivery_reason_code"] == DELIVERY_EVENT_AGE_EXCEEDED
    assert first.payload["delivery_rule"] == "delivery_recovery_freshness_v1"
    assert first.payload["delivery_recovery_only"] is True
    assert first.payload["delivery_event_age_seconds"] == 121.0
    assert first.payload["delivery_max_event_age_seconds"] == 120
    assert metrics.suppressions == [
        (DELIVERY_EVENT_AGE_EXCEEDED, "volume_spike"),
        (DELIVERY_EVENT_AGE_EXCEEDED, "volume_spike"),
    ]


def test_previous_moscow_session_is_suppressed_even_within_age_limit() -> None:
    # 20:59:55 UTC is 23:59:55 in Moscow; evaluation is in the next
    # local market date only ten seconds later.
    source_event_at = datetime(2026, 7, 22, 20, 59, 55, tzinfo=UTC)
    evaluated_at = source_event_at + timedelta(seconds=10)
    adapter, metrics = _adapter(
        evaluated_at=evaluated_at,
        maximum_age_seconds=300,
    )

    governed = adapter._govern_delivery(_signal(source_event_at=source_event_at))

    assert governed.payload["delivery_status"] == "suppressed"
    assert governed.payload["delivery_reason_code"] == (DELIVERY_EVENT_CROSSED_SESSION)
    assert governed.payload["delivery_source_session"] == "2026-07-22"
    assert governed.payload["delivery_evaluated_session"] == "2026-07-23"
    assert metrics.suppressions == [(DELIVERY_EVENT_CROSSED_SESSION, "volume_spike")]


def test_forced_stale_suppression_does_not_create_fresh_activity_context() -> None:
    evaluated_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    adapter, _ = _adapter(evaluated_at=evaluated_at)
    adapter._govern_delivery(_signal(source_event_at=evaluated_at - timedelta(hours=1)))
    fresh_liquidity = _signal(source_event_at=evaluated_at - timedelta(seconds=1))
    fresh_liquidity = replace(
        fresh_liquidity,
        signal_id="00000000-0000-4000-8000-000000000002",
        source_event_type="orderbook",
        signal_type="orderbook_imbalance",
        z_score=8.0,
        payload={"quality_score": 90},
    )

    governed = adapter._govern_delivery(fresh_liquidity)

    assert governed.payload["delivery_status"] == "suppressed"
    assert governed.payload["delivery_reason_code"] == "liquidity_without_context"


@pytest.mark.parametrize(
    ("source_event_at", "reason_code"),
    [
        (None, DELIVERY_EVENT_TIME_UNAVAILABLE),
        (
            datetime(2026, 7, 22, 12, 0, 6, tzinfo=UTC),
            DELIVERY_EVENT_TIME_IN_FUTURE,
        ),
    ],
)
def test_missing_or_future_source_time_fails_closed(
    source_event_at: datetime | None,
    reason_code: str,
) -> None:
    evaluated_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    metrics = _Metrics()
    guard = DeliveryRecoveryGuard(
        policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120),
        metrics=metrics,
        clock=lambda: evaluated_at,
    )

    decision = guard.evaluate(
        source_event_at=source_event_at,
        signal_type="price_jump",
    )

    assert decision.allow_external_delivery is False
    assert decision.reason_code == reason_code
    assert metrics.suppressions == [(reason_code, "price_jump")]


def test_domain_policy_rejects_naive_timestamps_and_invalid_limit() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        DeliveryFreshnessPolicy(maximum_event_age_seconds=0)
    policy = DeliveryFreshnessPolicy(maximum_event_age_seconds=120)
    with pytest.raises(ValueError, match="timezone-aware"):
        policy.decide(
            source_event_at=datetime(2026, 7, 22, 12, 0),
            evaluated_at=datetime(2026, 7, 22, 12, 0, tzinfo=UTC),
        )


def test_domain_reason_for_fresh_event_is_stable() -> None:
    at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)

    decision = DeliveryFreshnessPolicy(maximum_event_age_seconds=120).decide(
        source_event_at=at,
        evaluated_at=at,
    )

    assert decision.allow_external_delivery is True
    assert decision.reason_code == DELIVERY_EVENT_FRESH


def test_maximum_event_age_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIGNAL_DELIVERY_MAX_EVENT_AGE_SECONDS", "45")

    settings = RuntimeSettings.from_env()

    assert settings.signal_delivery_max_event_age_seconds == 45


def test_existing_outbox_payload_is_checked_again_before_delivery() -> None:
    evaluated_at = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)
    metrics = _Metrics()
    guard = QueuedDeliveryRecoveryAdapter(
        DeliveryRecoveryGuard(
            policy=DeliveryFreshnessPolicy(maximum_event_age_seconds=120),
            metrics=metrics,
            clock=lambda: evaluated_at,
        )
    )
    task = DeliveryTask(
        outbox_id="00000000-0000-4000-8000-000000000010",
        signal_id="00000000-0000-4000-8000-000000000001",
        destination_type="telegram",
        payload={
            "signal_type": "price_jump",
            "source_event_at": (evaluated_at - timedelta(minutes=10)).isoformat(),
        },
        attempt_count=1,
    )

    decision = guard.evaluate(task)

    assert decision.allow_external_delivery is False
    assert decision.reason_code == DELIVERY_EVENT_AGE_EXCEEDED
    assert metrics.suppressions == [(DELIVERY_EVENT_AGE_EXCEEDED, "price_jump")]
