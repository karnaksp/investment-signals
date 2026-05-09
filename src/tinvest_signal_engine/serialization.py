"""Утилиты времени, JSON-совместимых структур и котировок из payload."""

from __future__ import annotations

import enum
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import orjson


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def to_plain_data(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, enum.Enum):
        return value.name
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {
            str(key): to_plain_data(item)
            for key, item in value.items()
            if not _is_empty(item)
        }
    if isinstance(value, (list, tuple)):
        return [to_plain_data(item) for item in value if not _is_empty(item)]
    if is_dataclass(value):
        result: dict[str, Any] = {}
        for field in fields(value):
            field_value = getattr(value, field.name)
            if _is_empty(field_value):
                continue
            result[field.name] = to_plain_data(field_value)
        return result
    return str(value)


def quotation_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        units = int(value.get("units", 0))
        nano = int(value.get("nano", 0))
        return units + nano / 1_000_000_000
    return None


def _orjson_default(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.isoformat()
    if isinstance(value, enum.Enum):
        return value.name
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_dumps(payload: dict[str, Any]) -> str:
    return orjson.dumps(payload, default=_orjson_default).decode("utf-8")


def json_dumps_bytes(payload: dict[str, Any]) -> bytes:
    return orjson.dumps(payload, default=_orjson_default)


def json_loads(raw: str | bytes | bytearray | memoryview) -> Any:
    return orjson.loads(raw)


def kafka_json_serializer(value: Any) -> bytes:
    """Kafka ``value_serializer`` for dict payloads on the hot path."""
    if isinstance(value, dict):
        return orjson.dumps(value, default=_orjson_default)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return orjson.dumps(value, default=_orjson_default)


def kafka_json_deserializer(raw: bytes) -> Any:
    return orjson.loads(raw)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False
