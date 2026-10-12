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

    def load(self) -> IngestorHealthSnapshot | None:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        payload = json.loads(raw)
        if not isinstance(payload, Mapping):
            raise ValueError("ingestor health snapshot must be an object")
        if payload.get("schema_version") != INGESTOR_HEALTH_SCHEMA_VERSION:
            raise ValueError("unsupported ingestor health snapshot")
        return IngestorHealthSnapshot(
            state=_stream_state(payload.get("state")),
            started_at=_required_timestamp(payload.get("started_at")),
            last_market_event_at=_optional_parsed_timestamp(
                payload.get("last_market_event_at")
            ),
            last_success_at=_optional_parsed_timestamp(
                payload.get("last_success_at")
            ),
            last_error_at=_optional_parsed_timestamp(
                payload.get("last_error_at")
            ),
            reason_code=str(payload.get("reason_code") or ""),
            consecutive_failures=int(payload.get("consecutive_failures", -1)),
            configured_instruments=int(payload.get("configured_instruments", -1)),
            stale_after_seconds=int(payload.get("stale_after_seconds", 0)),
        )


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


def _stream_state(value: object):
    from tinvest_signal_engine.domain.ingestor_health import IngestorStreamState

    if not isinstance(value, str):
        raise ValueError("state must be text")
    return IngestorStreamState(value)


def _required_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


def _optional_parsed_timestamp(value: object) -> datetime | None:
    return None if value is None else _required_timestamp(value)


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
