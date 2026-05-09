"""Полное состояние детектора в Redis: cooldown сигналов + скользящие окна."""

from __future__ import annotations

import base64
import gzip
import json
import logging
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

FULL_STATE_KEY = "tinvest:detector:v1:full_state"
LEGACY_ALERT_KEY = "tinvest:detector:v1:last_alert_state"

if TYPE_CHECKING:
    from .detector_core import SignalDetector


def _pack_json(obj: Any) -> str:
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if len(raw) > 100_000:
        compressed = gzip.compress(raw, compresslevel=6)
        return "gz:" + base64.b64encode(compressed).decode("ascii")
    return "js:" + raw.decode("utf-8")


def _unpack_json(blob: str) -> Any:
    if blob.startswith("gz:"):
        raw = gzip.decompress(base64.b64decode(blob[3:].encode("ascii")))
        return json.loads(raw.decode("utf-8"))
    if blob.startswith("js:"):
        raw = blob[3:].encode("utf-8")
        return json.loads(raw.decode("utf-8"))
    return json.loads(blob)


def load_detector_redis_state(redis_url: str) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    """Возвращает ``(alerts, windows)``. Совместимость: только legacy alerts."""
    try:
        import redis
    except ImportError:
        logger.warning("redis package not installed")
        return {}, {}
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        packed = client.get(FULL_STATE_KEY)
        if packed:
            data = _unpack_json(packed)
            if isinstance(data, dict) and int(data.get("v", 0)) == 1:
                alerts = data.get("alerts") or {}
                windows = data.get("windows") or {}
                if isinstance(alerts, dict) and isinstance(windows, dict):
                    return _coerce_alerts(alerts), windows
        legacy = client.get(LEGACY_ALERT_KEY)
        if legacy:
            parsed = json.loads(legacy)
            if isinstance(parsed, dict):
                return _coerce_alerts(parsed), {}
    except Exception:
        logger.exception("Failed to load detector state from Redis")
    return {}, {}


def _coerce_alerts(data: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for iid, m in data.items():
        if not isinstance(iid, str) or not isinstance(m, dict):
            continue
        inner = {str(k): str(v) for k, v in m.items() if isinstance(k, str)}
        if inner:
            out[iid] = inner
    return out


def save_detector_redis_state(
    redis_url: str,
    *,
    alerts: dict[str, dict[str, str]],
    windows: dict[str, Any],
) -> None:
    try:
        import redis
    except ImportError:
        return
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        body = {"v": 1, "alerts": alerts, "windows": windows}
        client.set(FULL_STATE_KEY, _pack_json(body))
        client.set(LEGACY_ALERT_KEY, json.dumps(alerts, separators=(",", ":"), ensure_ascii=True))
    except Exception:
        logger.exception("Failed to save detector state to Redis")


def hydrate_detector_from_redis(detector: SignalDetector, redis_url: str | None) -> None:
    if not redis_url:
        return
    from .detector_state_persist import hydrate_window_state

    alerts, windows = load_detector_redis_state(redis_url)
    if alerts:
        detector.hydrate_alert_state(alerts)
        logger.info(
            "Restored alert cooldown state from Redis for %d instrument(s)",
            len(alerts),
        )
    if windows:
        hydrate_window_state(detector, windows)
        n_inst = len(windows.get("instruments", {}))
        n_mid = len(windows.get("mid_track", {}))
        logger.info(
            "Restored detector windows from Redis (%d instruments, %d mid tracks)",
            n_inst,
            n_mid,
        )


def flush_detector_to_redis(detector: SignalDetector, redis_url: str | None) -> None:
    if not redis_url:
        return
    from .detector_state_persist import export_window_state

    save_detector_redis_state(
        redis_url,
        alerts=detector.export_alert_state(),
        windows=export_window_state(detector),
    )
