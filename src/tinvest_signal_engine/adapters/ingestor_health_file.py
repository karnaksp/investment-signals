"""Atomic allow-list JSON persistence for ingestor health."""

from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Mapping

from tinvest_signal_engine.domain.ingestor_health import (
    INGESTOR_HEALTH_SCHEMA_VERSION,
    IngestorHealthSnapshot,
)


class AtomicJsonIngestorHealthStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def save(self, snapshot: IngestorHealthSnapshot) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = _payload(snapshot)
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
                json.dump(
                    payload,
                    handle,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o644)
            os.replace(temporary, self._path)
            _fsync_directory(self._path.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def _payload(snapshot: IngestorHealthSnapshot) -> Mapping[str, object]:
    return {
        "schema_version": INGESTOR_HEALTH_SCHEMA_VERSION,
        "state": snapshot.state.value,
        "started_at": _timestamp(snapshot.started_at),
        "last_market_event_at": _optional_timestamp(
            snapshot.last_market_event_at
        ),
        "last_success_at": _optional_timestamp(snapshot.last_success_at),
        "last_error_at": _optional_timestamp(snapshot.last_error_at),
        "reason_code": snapshot.reason_code,
        "consecutive_failures": snapshot.consecutive_failures,
        "configured_instruments": snapshot.configured_instruments,
        "stale_after_seconds": snapshot.stale_after_seconds,
    }


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
