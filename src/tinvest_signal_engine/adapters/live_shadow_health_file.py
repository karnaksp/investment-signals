"""Atomic JSON persistence for the live-shadow heartbeat."""

from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

from tinvest_signal_engine.domain.live_shadow_health import (
    LIVE_SHADOW_HEALTH_SCHEMA_VERSION,
    LiveShadowHealthSnapshot,
)


class AtomicJsonLiveShadowHealthStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def save(self, snapshot: LiveShadowHealthSnapshot) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": LIVE_SHADOW_HEALTH_SCHEMA_VERSION,
            "state": snapshot.state.value,
            "started_at": _timestamp(snapshot.started_at),
            "last_success_at": _optional_timestamp(snapshot.last_success_at),
            "last_error_at": _optional_timestamp(snapshot.last_error_at),
            "reason_code": snapshot.reason_code,
            "consecutive_failures": snapshot.consecutive_failures,
            "observations_processed": snapshot.observations_processed,
            "outcomes_processed": snapshot.outcomes_processed,
            "outcomes_unavailable": snapshot.outcomes_unavailable,
            "stale_after_seconds": snapshot.stale_after_seconds,
        }
        temporary: Path | None = None
        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o644)
            os.replace(temporary, self._path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)
