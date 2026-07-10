from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.config import DetectorSettings
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.domain.signal_identity import deterministic_signal_id
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal


def _event(event_id: str) -> NormalizedEvent:
    timestamp = datetime(2026, 7, 1, 10, 0, tzinfo=timezone.utc)
    return NormalizedEvent(
        event_id=event_id,
        event_type="trading_status",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="figi",
        uid="uid",
        lot=10,
        source_time=timestamp,
        received_at=timestamp,
        payload={"trading_status": "normal"},
    )


def test_deterministic_signal_id_is_stable_and_type_scoped() -> None:
    first = deterministic_signal_id("event-1", "price_jump")
    assert first == deterministic_signal_id("event-1", "price_jump")
    assert first != deterministic_signal_id("event-1", "volume_spike")


def test_detector_attaches_source_and_config_versions() -> None:
    detector = SignalDetector(
        DetectorSettings(alert_cooldown_seconds=0),
        expectation_catalog_version="catalog-1",
        detector_config_version="detector-7",
        delivery_config_version="delivery-3",
        cost_model_version="cost-2",
    )
    detector.process(_event("event-before"))
    event = _event("event-after")
    event = NormalizedEvent(
        **{
            **event.__dict__,
            "payload": {"trading_status": "not_available"},
        }
    )

    signal = detector.process(event)[0]

    assert signal.signal_id == deterministic_signal_id(
        "event-after", "trading_status_changed"
    )
    assert signal.source_event_id == "event-after"
    assert signal.source_event_at == event.source_time
    assert signal.provenance_status == "complete"
    assert signal.expectation_catalog_version == "catalog-1"
    assert signal.detector_config_version == "detector-7"
    assert signal.delivery_config_version == "delivery-3"
    assert signal.cost_model_version == "cost-2"


def test_old_json_wire_defaults_to_legacy() -> None:
    old_wire = {
        "signal_id": "00000000-0000-0000-0000-000000000001",
        "detected_at": "2026-01-01T00:00:00+00:00",
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "alias": "sber",
        "source_event_type": "trade",
        "signal_type": "price_jump",
        "severity": 2,
        "metric_value": 1.0,
        "baseline_value": 0.5,
        "z_score": 3.0,
        "window_seconds": 60,
        "summary": "legacy",
        "payload": {},
    }

    signal = TriggerSignal.from_dict(old_wire)

    assert signal.source_event_id is None
    assert signal.source_event_at is None
    assert signal.provenance_status == "legacy"
    assert signal.signal_schema_version == "1.0.0"
