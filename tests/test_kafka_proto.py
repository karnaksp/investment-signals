from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.kafka_proto import (
    confluent_unwrap,
    confluent_wrap,
    normalized_event_dict_from_proto_bytes,
    normalized_event_to_proto_bytes,
    trigger_signal_dict_from_proto_bytes,
    trigger_signal_to_proto_bytes,
)
from tinvest_signal_engine.models import NormalizedEvent, TriggerSignal


def test_normalized_event_proto_roundtrip() -> None:
    ev = NormalizedEvent(
        event_id="e1",
        event_type="trade",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        figi="f",
        uid="u",
        lot=10,
        source_time=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        received_at=datetime(2026, 1, 2, 3, 4, 6, tzinfo=timezone.utc),
        payload={"quantity": 1.0, "price": {"units": 100, "nano": 0}},
    )
    raw = normalized_event_to_proto_bytes(ev)
    d = normalized_event_dict_from_proto_bytes(raw)
    ev2 = NormalizedEvent.from_dict(d)
    assert ev2.instrument_id == ev.instrument_id
    assert ev2.lot == 10
    assert ev2.payload.get("quantity") == 1.0


def test_confluent_wrap_unwrap_roundtrip() -> None:
    ev = NormalizedEvent(
        event_id="e2",
        event_type="trade",
        instrument_id="X_TQBR",
        ticker="X",
        class_code="TQBR",
        alias="x",
        figi="",
        uid="",
        lot=1,
        source_time=datetime(2026, 5, 1, tzinfo=timezone.utc),
        received_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        payload={"quantity": 2.0, "price": {"units": 1, "nano": 0}},
    )
    inner = normalized_event_to_proto_bytes(ev)
    wire = confluent_wrap(42, inner)
    un = confluent_unwrap(wire)
    assert un is not None
    sid, body = un
    assert sid == 42
    d = normalized_event_dict_from_proto_bytes(body)
    assert NormalizedEvent.from_dict(d).event_id == "e2"


def test_trigger_signal_proto_roundtrip() -> None:
    sig = TriggerSignal(
        signal_id="s1",
        detected_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="volume_spike",
        severity=2,
        metric_value=1.0,
        baseline_value=0.5,
        z_score=3.0,
        window_seconds=60,
        summary="test",
        payload={"k": 1},
        source_event_id="event-1",
        source_event_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        signal_schema_version="1.0.0",
        expectation_catalog_version="catalog-1",
        detector_config_version="detector-1",
        delivery_config_version="delivery-1",
        cost_model_version="cost-1",
        provenance_status="complete",
    )
    raw = trigger_signal_to_proto_bytes(sig)
    d = trigger_signal_dict_from_proto_bytes(raw)
    sig2 = TriggerSignal.from_dict(d)
    assert sig2.signal_id == sig.signal_id
    assert sig2.payload == {"k": 1}
    assert sig2.source_event_id == "event-1"
    assert sig2.source_event_at == sig.source_event_at
    assert sig2.detector_config_version == "detector-1"
    assert sig2.provenance_status == "complete"


def test_old_trigger_signal_proto_defaults_to_legacy() -> None:
    from tinvest_signal_engine.proto_gen.trigger_signal_pb2 import TriggerSignalV1

    old = TriggerSignalV1(
        signal_id="00000000-0000-0000-0000-000000000001",
        detected_at_rfc3339="2026-01-01T00:00:00+00:00",
        instrument_id="SBER_TQBR",
        ticker="SBER",
        class_code="TQBR",
        alias="sber",
        source_event_type="trade",
        signal_type="price_jump",
        severity=2,
        metric_value=1.0,
        baseline_value=0.5,
        z_score=3.0,
        window_seconds=60,
        summary="old",
        payload_json="{}",
    )

    signal = TriggerSignal.from_dict(
        trigger_signal_dict_from_proto_bytes(old.SerializeToString())
    )

    assert signal.source_event_id is None
    assert signal.provenance_status == "legacy"
