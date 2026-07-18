from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from tinvest_signal_engine.adapters.tinvest_bond_convergence import (
    estimate_weekday_sessions,
)
from tinvest_signal_engine.application.bond_convergence import ScanBondConvergence
from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.signal_quality import compute_signal_quality
from tinvest_signal_engine.domain.bond_convergence import (
    BondConvergenceSnapshot,
    evaluate_bond_convergence,
)
from tinvest_signal_engine.models import NormalizedEvent


def _snapshot(**overrides: object) -> BondConvergenceSnapshot:
    values: dict[str, object] = {
        "instrument_id": "bond-uid",
        "ticker": "RU000A",
        "class_code": "TQOB",
        "alias": "Тестовая облигация",
        "figi": "BBGTEST",
        "uid": "bond-uid",
        "lot": 1,
        "source_time": datetime(2026, 7, 17, tzinfo=UTC),
        "maturity_date": date(2026, 10, 15),
        "clean_price": Decimal("96.5"),
        "sessions_to_maturity": 60,
        "currency": "rub",
        "country_of_risk": "RU",
        "risk_level": "moderate",
        "amortization": False,
        "subordinated": False,
        "perpetual": False,
        "api_trade_available": True,
        "buy_available": True,
        "sell_available": True,
    }
    values.update(overrides)
    return BondConvergenceSnapshot(**values)  # type: ignore[arg-type]


def test_domain_selects_direction_towards_par() -> None:
    below = evaluate_bond_convergence(_snapshot(clean_price=Decimal("96.5")))
    above = evaluate_bond_convergence(_snapshot(clean_price=Decimal("103.5")))

    assert below.eligible and below.direction == "up"
    assert above.eligible and above.direction == "down"
    assert below.expected_net_distance_bps > 0


def test_future_session_estimate_skips_weekends() -> None:
    assert estimate_weekday_sessions(
        after_date=date(2026, 7, 17),
        maturity_date=date(2026, 7, 20),
    ) == 1


def test_domain_rejects_noncomparable_bond() -> None:
    decision = evaluate_bond_convergence(_snapshot(amortization=True))

    assert not decision.eligible
    assert decision.reason == "amortizing_bond"


def test_application_publishes_only_eligible_snapshots() -> None:
    accepted = _snapshot()
    rejected = _snapshot(uid="other", instrument_id="other", risk_level="high")

    class Source:
        def load_snapshots(self):
            return (accepted, rejected)

    class Publisher:
        published: list[BondConvergenceSnapshot] = []

        def publish(self, snapshot: BondConvergenceSnapshot) -> None:
            self.published.append(snapshot)

    publisher = Publisher()
    receipt = ScanBondConvergence(source=Source(), publisher=publisher).execute()

    assert receipt.inspected == 2
    assert receipt.published == 1
    assert publisher.published == [accepted]


def test_detector_builds_explainable_signal_with_historical_evidence() -> None:
    snapshot = _snapshot()
    detector = SignalDetector(
        DetectorSettings(alert_cooldown_seconds=0),
        expectation_catalog_version="1.0.0",
        detector_config_version="detector-v1",
        delivery_config_version="delivery-v1",
        cost_model_version="cost-v1",
    )
    event = NormalizedEvent(
        event_id="7d2f64cc-e6e2-5d9d-8867-f50e6f00c001",
        event_type="bond_convergence_observation",
        instrument_id=snapshot.instrument_id,
        ticker=snapshot.ticker,
        class_code=snapshot.class_code,
        alias=snapshot.alias,
        figi=snapshot.figi,
        uid=snapshot.uid,
        lot=snapshot.lot,
        source_time=snapshot.source_time,
        received_at=snapshot.source_time,
        payload={
            "maturity_date": snapshot.maturity_date.isoformat(),
            "clean_price": float(snapshot.clean_price),
            "sessions_to_maturity": snapshot.sessions_to_maturity,
            "currency": snapshot.currency,
            "country_of_risk": snapshot.country_of_risk,
            "risk_level": snapshot.risk_level,
            "amortization": snapshot.amortization,
            "subordinated": snapshot.subordinated,
            "perpetual": snapshot.perpetual,
            "api_trade_available": snapshot.api_trade_available,
            "buy_available": snapshot.buy_available,
            "sell_available": snapshot.sell_available,
        },
    )

    signal = detector.process(event)[0]

    assert signal.signal_type == "bond_maturity_convergence"
    assert signal.payload["price_direction"] == "up"
    assert signal.payload["historical_success_rate"] > 0.93
    assert signal.payload["outcome_schedule"] == "maturity_only"
    assert signal.provenance_status == "complete"

    quality = compute_signal_quality(signal)
    assert quality["quality_score"] >= 90
    assert quality["quality_tier"] == "high"
    assert quality["quality_factors"]["method"] == (
        "historical_wilson_lower_bound_v1"
    )
