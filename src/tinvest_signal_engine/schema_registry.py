"""Регистрация protobuf-схем в Confluent-совместимом Schema Registry (Redpanda)."""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Lock
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_lock = Lock()
_schema_id_cache: dict[tuple[str, str], int] = {}


def schema_subject_for_topic(topic: str) -> str:
    return f"{topic}-value"


def register_protobuf_schema(
    base_url: str,
    subject: str,
    proto_path: Path,
    *,
    timeout: float = 30.0,
) -> int:
    """Регистрирует текст ``.proto``; возвращает numeric schema id."""
    key = (subject, str(proto_path.resolve()))
    with _lock:
        cached = _schema_id_cache.get(key)
        if cached is not None:
            return cached
    schema_text = proto_path.read_text(encoding="utf-8")
    url = base_url.rstrip("/") + f"/subjects/{subject}/versions"
    body: dict[str, Any] = {
        "schemaType": "PROTOBUF",
        "schema": schema_text,
    }
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    with httpx.Client(timeout=timeout) as client:
        response = client.post(url, json=body, headers=headers)
        response.raise_for_status()
        payload = response.json()
    schema_id = int(payload["id"])
    with _lock:
        _schema_id_cache[key] = schema_id
    logger.info("Registered protobuf schema subject=%s id=%s", subject, schema_id)
    return schema_id


def clear_schema_id_cache() -> None:
    with _lock:
        _schema_id_cache.clear()
