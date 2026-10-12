"""Protobuf (опционально Confluent wire) для значений Kafka."""

from __future__ import annotations

import struct
from typing import Any, Callable

from .models import NormalizedEvent, TriggerSignal
from .serialization import json_dumps, json_loads

_CONFLUENT_MAGIC = 0


def confluent_wrap(schema_id: int, protobuf_payload: bytes) -> bytes:
    return bytes([_CONFLUENT_MAGIC]) + struct.pack(">I", schema_id) + protobuf_payload


def confluent_unwrap(raw: bytes) -> tuple[int, bytes] | None:
    if len(raw) < 5 or raw[0] != _CONFLUENT_MAGIC:
        return None
    schema_id = struct.unpack(">I", raw[1:5])[0]
    return schema_id, raw[5:]


def normalized_event_to_proto_bytes(event: NormalizedEvent) -> bytes:
    from .proto_gen.normalized_event_pb2 import NormalizedEventV1

    msg = NormalizedEventV1()
    msg.event_id = event.event_id
    msg.event_type = event.event_type
    msg.instrument_id = event.instrument_id
    msg.ticker = event.ticker
    msg.class_code = event.class_code
    msg.alias = event.alias
    msg.figi = event.figi
    msg.uid = event.uid
    msg.lot = int(event.lot)
    msg.source_time_rfc3339 = event.source_time.isoformat()
    msg.received_at_rfc3339 = event.received_at.isoformat()
    msg.payload_json = json_dumps(event.payload)
    return msg.SerializeToString()


def normalized_event_dict_from_proto_bytes(data: bytes) -> dict[str, Any]:
    from .proto_gen.normalized_event_pb2 import NormalizedEventV1

    msg = NormalizedEventV1()
    msg.ParseFromString(data)
    payload = json_loads(msg.payload_json) if msg.payload_json else {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "event_id": msg.event_id,
        "event_type": msg.event_type,
        "instrument_id": msg.instrument_id,
        "ticker": msg.ticker,
        "class_code": msg.class_code,
        "alias": msg.alias,
        "figi": msg.figi,
        "uid": msg.uid,
        "lot": int(msg.lot),
        "source_time": msg.source_time_rfc3339,
        "received_at": msg.received_at_rfc3339,
        "payload": payload,
    }


def trigger_signal_to_proto_bytes(signal: TriggerSignal) -> bytes:
    from .proto_gen.trigger_signal_pb2 import TriggerSignalV1

    msg = TriggerSignalV1()
    msg.signal_id = signal.signal_id
    msg.detected_at_rfc3339 = signal.detected_at.isoformat()
    msg.instrument_id = signal.instrument_id
    msg.ticker = signal.ticker
    msg.class_code = signal.class_code
    msg.alias = signal.alias
    msg.source_event_type = signal.source_event_type
    msg.signal_type = signal.signal_type
    msg.severity = int(signal.severity)
    msg.metric_value = float(signal.metric_value)
    msg.baseline_value = float(signal.baseline_value)
    msg.z_score = float(signal.z_score)
    msg.window_seconds = int(signal.window_seconds)
    msg.summary = signal.summary
    msg.payload_json = json_dumps(signal.payload)
    msg.source_event_id = signal.source_event_id or ""
    msg.source_event_at_rfc3339 = (
        signal.source_event_at.isoformat() if signal.source_event_at else ""
    )
    msg.signal_schema_version = signal.signal_schema_version
    msg.expectation_catalog_version = signal.expectation_catalog_version or ""
    msg.detector_config_version = signal.detector_config_version or ""
    msg.delivery_config_version = signal.delivery_config_version or ""
    msg.cost_model_version = signal.cost_model_version or ""
    msg.provenance_status = signal.provenance_status
    return msg.SerializeToString()


def trigger_signal_dict_from_proto_bytes(data: bytes) -> dict[str, Any]:
    from .proto_gen.trigger_signal_pb2 import TriggerSignalV1

    msg = TriggerSignalV1()
    msg.ParseFromString(data)
    payload = json_loads(msg.payload_json) if msg.payload_json else {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "signal_id": msg.signal_id,
        "detected_at": msg.detected_at_rfc3339,
        "instrument_id": msg.instrument_id,
        "ticker": msg.ticker,
        "class_code": msg.class_code,
        "alias": msg.alias,
        "source_event_type": msg.source_event_type,
        "signal_type": msg.signal_type,
        "severity": int(msg.severity),
        "metric_value": float(msg.metric_value),
        "baseline_value": float(msg.baseline_value),
        "z_score": float(msg.z_score),
        "window_seconds": int(msg.window_seconds),
        "summary": msg.summary,
        "payload": payload,
        "source_event_id": msg.source_event_id or None,
        "source_event_at": msg.source_event_at_rfc3339 or None,
        "signal_schema_version": msg.signal_schema_version or "1.0.0",
        "expectation_catalog_version": msg.expectation_catalog_version or None,
        "detector_config_version": msg.detector_config_version or None,
        "delivery_config_version": msg.delivery_config_version or None,
        "cost_model_version": msg.cost_model_version or None,
        "provenance_status": msg.provenance_status or "legacy",
    }


def build_raw_value_serializer(
    *,
    format_name: str,
    schema_id: int | None,
    register_schema: Callable[[], int] | None,
) -> Callable[[Any], bytes]:
    fmt = (format_name or "json").strip().lower()
    if fmt == "json":
        from .serialization import kafka_json_serializer

        return kafka_json_serializer  # type: ignore[return-value]

    if fmt != "protobuf":
        raise ValueError(f"Unsupported KAFKA_RAW_VALUE_FORMAT: {format_name!r}")

    def _serialize(value: Any) -> bytes:
        if not isinstance(value, NormalizedEvent):
            raise TypeError("protobuf raw serializer expects NormalizedEvent")
        sid = schema_id
        if sid is None:
            if register_schema is None:
                raise RuntimeError("schema_id or register_schema required for protobuf")
            sid = register_schema()
        body = normalized_event_to_proto_bytes(value)
        return confluent_wrap(sid, body)

    return _serialize


def build_raw_value_deserializer(
    *, format_name: str
) -> Callable[[bytes], Any]:
    fmt = (format_name or "json").strip().lower()
    if fmt == "json":
        from .serialization import kafka_json_deserializer

        return kafka_json_deserializer

    if fmt != "protobuf":

        def _bad(raw: bytes) -> Any:
            raise ValueError(f"Unsupported KAFKA_RAW_VALUE_FORMAT: {format_name!r}")

        return _bad

    def _deserialize(raw: bytes) -> Any:
        from .serialization import kafka_json_deserializer as _json_dec

        unwrapped = confluent_unwrap(raw)
        if unwrapped is None:
            return _json_dec(raw)
        _, payload = unwrapped
        return normalized_event_dict_from_proto_bytes(payload)

    return _deserialize


def build_signal_value_serializer(
    *,
    format_name: str,
    schema_id: int | None,
    register_schema: Callable[[], int] | None,
) -> Callable[[Any], bytes]:
    fmt = (format_name or "json").strip().lower()
    if fmt == "json":
        from .serialization import kafka_json_serializer

        return kafka_json_serializer  # type: ignore[return-value]

    if fmt != "protobuf":
        raise ValueError(f"Unsupported KAFKA_SIGNAL_VALUE_FORMAT: {format_name!r}")

    def _serialize(value: Any) -> bytes:
        if isinstance(value, TriggerSignal):
            sig = value
        elif isinstance(value, dict):
            sig = TriggerSignal.from_dict(value)
        else:
            raise TypeError("signal serializer expects TriggerSignal or dict")
        sid = schema_id
        if sid is None:
            if register_schema is None:
                raise RuntimeError("schema_id or register_schema required for protobuf")
            sid = register_schema()
        body = trigger_signal_to_proto_bytes(sig)
        return confluent_wrap(sid, body)

    return _serialize


def build_signal_value_deserializer(
    *, format_name: str
) -> Callable[[bytes], Any]:
    fmt = (format_name or "json").strip().lower()
    if fmt == "json":
        from .serialization import kafka_json_deserializer

        return kafka_json_deserializer

    if fmt != "protobuf":

        def _bad(raw: bytes) -> Any:
            raise ValueError(f"Unsupported KAFKA_SIGNAL_VALUE_FORMAT: {format_name!r}")

        return _bad

    def _deserialize(raw: bytes) -> Any:
        from .serialization import kafka_json_deserializer as _json_dec

        unwrapped = confluent_unwrap(raw)
        if unwrapped is None:
            return _json_dec(raw)
        _, payload = unwrapped
        return trigger_signal_dict_from_proto_bytes(payload)

    return _deserialize
