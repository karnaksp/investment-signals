"""Чтение сырых событий из ClickHouse по HTTP (JSONEachRow), без отдельного драйвера."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx


def _escape_ch_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def fetch_raw_events_window(
    base_url: str,
    *,
    instrument_id: str,
    start: datetime,
    end: datetime,
    limit: int = 500,
    username: str | None = None,
    password: str | None = None,
) -> list[dict[str, Any]]:
    """События из ``signal_engine.market_raw_events`` в полуинтервале ``[start, end)``."""
    bu = base_url.rstrip("/")
    inst = _escape_ch_string(instrument_id.strip())
    if not inst:
        return []
    st = start if start.tzinfo else start.replace(tzinfo=timezone.utc)
    en = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
    t0 = st.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t1 = en.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    lim = max(1, min(int(limit), 2_000))
    query = (
        "SELECT event_type, source_time, received_at, "
        "length(payload_json) AS payload_len "
        "FROM signal_engine.market_raw_events "
        f"WHERE instrument_id = '{inst}' "
        f"AND source_time >= toDateTime64('{t0}', 3, 'UTC') "
        f"AND source_time < toDateTime64('{t1}', 3, 'UTC') "
        "ORDER BY source_time ASC "
        f"LIMIT {lim} FORMAT JSONEachRow"
    )
    auth = None
    if username and password:
        auth = (username.strip(), password)
    with httpx.Client(timeout=20.0, auth=auth) as client:
        response = client.post(bu, content=query.encode("utf-8"))
        response.raise_for_status()
    text = response.text.strip()
    if not text:
        return []
    rows: list[dict[str, Any]] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def fetch_source_health(
    base_url: str,
    *,
    minutes: int = 1440,
    username: str | None = None,
    password: str | None = None,
) -> list[dict[str, Any]]:
    """Latest raw-event timestamp per instrument/source from ClickHouse."""
    bu = base_url.rstrip("/")
    m = max(1, min(int(minutes), 10_080))
    query = (
        "SELECT instrument_id, event_type, max(source_time) AS last_source_time, "
        "count() AS event_count "
        "FROM signal_engine.market_raw_events "
        f"WHERE source_time >= now() - INTERVAL {m} MINUTE "
        "GROUP BY instrument_id, event_type "
        "ORDER BY instrument_id ASC, event_type ASC "
        "FORMAT JSONEachRow"
    )
    auth = None
    if username and password:
        auth = (username.strip(), password)
    with httpx.Client(timeout=20.0, auth=auth) as client:
        response = client.post(bu, content=query.encode("utf-8"))
        response.raise_for_status()
    text = response.text.strip()
    if not text:
        return []
    rows: list[dict[str, Any]] = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows
