from __future__ import annotations

import enum
import json
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import Any


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


def json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False

