from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from tinvest_signal_engine.application.signal_outcomes import (
    DirectionalSignalOutcomeProcessor,
    DirectionalSignalOutcomeRequest,
    evaluate_directional_signal_from_ticks,
)
from tinvest_signal_engine.domain.reference_ticks import ReferenceTick
from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    DirectionalSignalOutcome,
)


def _policy(**overrides: object) -> DirectionalOutcomePolicy:
    values = {
        "policy_version": "directional-outcome-v1",
        "cost_model_version": "cost-v1",
        "horizon_seconds": 300,
        "anchor_max_age_seconds": 5,
        "forward_grace_seconds": 30,
        "min_move_bps": Decimal("5"),
        "volatility_multiplier": Decimal("0"),
        "round_trip_cost_bps": Decimal("1"),
    }
    values.update(overrides)
    return DirectionalOutcomePolicy(**values)


def _request(at: datetime, **overrides: object) -> DirectionalSignalOutcomeRequest:
    values = {
        "signal_id": "signal-1",
        "instrument_id": "SBER",
        "signal_type": "price_jump",
        "source_event_at": at,
        "expected_direction": 1,
        "realized_volatility_bps": Decimal("1"),
        "policy": _policy(),
    }
    values.update(overrides)
    return DirectionalSignalOutcomeRequest(**values)


def _tick(
    *,
    instrument_id: str = "SBER",
    event_at: datetime,
    price: str,
) -> ReferenceTick:
    return ReferenceTick(
        instrument_id=instrument_id,
        event_at=event_at,
        received_at=event_at,
        event_id=uuid4(),
        source_kind="last_price",
        last_price=Decimal(price),
        has_last_price=True,
    )


@dataclass
class _OutcomeStore:
    outcome_id: str = "outcome-1"
    outcomes: list[DirectionalSignalOutcome] = field(default_factory=list)

    def persist(self, outcome: DirectionalSignalOutcome) -> str:
        self.outcomes.append(outcome)
        return self.outcome_id


def test_evaluate_directional_signal_selects_anchor_without_lookahead() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    ticks = [
        _tick(event_at=source_at - timedelta(seconds=4), price="100"),
        _tick(event_at=source_at + timedelta(seconds=1), price="500"),
        _tick(event_at=source_at + timedelta(seconds=300), price="102"),
    ]

    outcome = evaluate_directional_signal_from_ticks(
        request=_request(source_at),
        ticks=ticks,
    )

    assert outcome.verdict == "confirmed"
    assert outcome.anchor_price == Decimal("100")
    assert outcome.forward_price == Decimal("102")
    assert outcome.raw_return_bps == Decimal("200")


def test_evaluate_directional_signal_selects_forward_only_at_or_after_target() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    ticks = [
        _tick(event_at=source_at, price="100"),
        _tick(event_at=source_at + timedelta(seconds=299), price="500"),
        _tick(event_at=source_at + timedelta(seconds=301), price="98"),
    ]

    outcome = evaluate_directional_signal_from_ticks(
        request=_request(source_at),
        ticks=ticks,
    )

    assert outcome.verdict == "contradicted"
    assert outcome.forward_price == Decimal("98")
    assert outcome.inverse_hypothesis_candidate is True


def test_evaluate_directional_signal_filters_other_instruments() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    ticks = [
        _tick(instrument_id="GAZP", event_at=source_at, price="100"),
        _tick(
            instrument_id="GAZP",
            event_at=source_at + timedelta(seconds=300),
            price="500",
        ),
        _tick(event_at=source_at, price="100"),
        _tick(event_at=source_at + timedelta(seconds=300), price="101"),
    ]

    outcome = evaluate_directional_signal_from_ticks(
        request=_request(source_at),
        ticks=ticks,
    )

    assert outcome.instrument_id == "SBER"
    assert outcome.forward_price == Decimal("101")
    assert outcome.verdict == "confirmed"


def test_evaluate_directional_signal_returns_inconclusive_without_forward_price() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    ticks = [
        _tick(event_at=source_at, price="100"),
        _tick(event_at=source_at + timedelta(seconds=331), price="102"),
    ]

    outcome = evaluate_directional_signal_from_ticks(
        request=_request(source_at),
        ticks=ticks,
    )

    assert outcome.verdict == "inconclusive"
    assert outcome.reason_code == "forward_price_unavailable"
    assert outcome.anchor_price == Decimal("100")
    assert outcome.forward_price is None


def test_directional_signal_outcome_processor_waits_until_horizon_matures() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    store = _OutcomeStore()

    result = DirectionalSignalOutcomeProcessor(store).process(
        request=_request(source_at),
        ticks=[
            _tick(event_at=source_at, price="100"),
            _tick(event_at=source_at + timedelta(seconds=300), price="102"),
        ],
        now=source_at + timedelta(seconds=329),
    )

    assert result.status == "pending"
    assert result.reason_code == "outcome_horizon_not_mature"
    assert result.outcome is None
    assert result.outcome_id is None
    assert store.outcomes == []


def test_directional_signal_outcome_processor_persists_mature_outcome() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    store = _OutcomeStore(outcome_id="stored-outcome")

    result = DirectionalSignalOutcomeProcessor(store).process(
        request=_request(source_at),
        ticks=[
            _tick(event_at=source_at, price="100"),
            _tick(event_at=source_at + timedelta(seconds=300), price="102"),
        ],
        now=source_at + timedelta(seconds=330),
    )

    assert result.status == "stored"
    assert result.reason_code == "confirmed"
    assert result.outcome_id == "stored-outcome"
    assert result.outcome is store.outcomes[0]
    assert store.outcomes[0].verdict == "confirmed"


def test_directional_signal_outcome_processor_persists_mature_unavailable_outcome() -> None:
    source_at = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)
    store = _OutcomeStore()

    result = DirectionalSignalOutcomeProcessor(store).process(
        request=_request(source_at),
        ticks=[_tick(event_at=source_at, price="100")],
        now=source_at + timedelta(seconds=330),
    )

    assert result.status == "stored"
    assert result.reason_code == "forward_price_unavailable"
    assert result.outcome is not None
    assert result.outcome.verdict == "inconclusive"
    assert store.outcomes[0].reason_code == "forward_price_unavailable"
