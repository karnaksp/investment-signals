"""Container healthcheck for the prospective live-shadow worker."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Mapping

from tinvest_signal_engine.domain.live_shadow_health import (
    LIVE_SHADOW_ACTIVE,
    LIVE_SHADOW_HEALTH_SCHEMA_VERSION,
)


def validate_live_shadow_health(
    path: str | Path,
    *,
    now: datetime | None = None,
) -> str | None:
    """Return a stable failure reason, or ``None`` for a healthy heartbeat."""

    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "live_shadow_health_unreadable"
    if not isinstance(payload, Mapping):
        return "live_shadow_health_invalid"
    if payload.get("schema_version") != LIVE_SHADOW_HEALTH_SCHEMA_VERSION:
        return "live_shadow_health_schema_unsupported"
    if (
        payload.get("state") != "active"
        or payload.get("reason_code") != LIVE_SHADOW_ACTIVE
        or payload.get("consecutive_failures") != 0
    ):
        return "live_shadow_latest_pass_failed"
    last_success_at = _timestamp(payload.get("last_success_at"))
    stale_after_seconds = payload.get("stale_after_seconds")
    if (
        last_success_at is None
        or isinstance(stale_after_seconds, bool)
        or not isinstance(stale_after_seconds, int)
        or stale_after_seconds <= 0
    ):
        return "live_shadow_health_invalid"
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    age_seconds = (checked_at - last_success_at).total_seconds()
    if age_seconds < -30:
        return "live_shadow_health_from_future"
    if age_seconds > stale_after_seconds:
        return "live_shadow_health_stale"
    return None


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()
    failure = validate_live_shadow_health(args.path)
    if failure is not None:
        print(failure)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by Docker
    raise SystemExit(main())
