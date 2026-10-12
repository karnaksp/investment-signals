"""Atomic local-file adapter for worker heartbeat snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from typing import Mapping

from tinvest_signal_engine.domain.worker_health import (
    WorkerHealthSnapshot,
    WorkerState,
)


class WorkerHealthFileSink:
    def __init__(self, path: Path) -> None:
        self._path = path

    def persist(self, snapshot: WorkerHealthSnapshot) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            _snapshot_to_dict(snapshot),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self._path.parent,
                prefix=f".{self._path.name}.",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                os.chmod(temporary.name, 0o600)
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, self._path)
            directory_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


def read_worker_health_snapshot(path: Path) -> WorkerHealthSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("worker health file must contain an object")
    return _snapshot_from_dict(payload)


def _snapshot_to_dict(snapshot: WorkerHealthSnapshot) -> dict[str, object]:
    return {
        "schema_version": snapshot.schema_version,
        "worker_id": snapshot.worker_id,
        "state": snapshot.state.value,
        "started_at": _format_timestamp(snapshot.started_at),
        "last_heartbeat_at": _format_timestamp(snapshot.last_heartbeat_at),
        "last_success_at": _format_timestamp(snapshot.last_success_at),
        "last_error_at": _format_timestamp(snapshot.last_error_at),
        "reason_code": snapshot.reason_code,
        "consecutive_failures": snapshot.consecutive_failures,
        "stale_after_seconds": snapshot.stale_after_seconds,
    }


def _snapshot_from_dict(payload: Mapping[str, object]) -> WorkerHealthSnapshot:
    return WorkerHealthSnapshot(
        schema_version=str(payload.get("schema_version") or ""),
        worker_id=str(payload.get("worker_id") or ""),
        state=WorkerState(str(payload.get("state") or "")),
        started_at=_parse_timestamp(payload.get("started_at"), required=True),
        last_heartbeat_at=_parse_timestamp(
            payload.get("last_heartbeat_at"), required=True
        ),
        last_success_at=_parse_timestamp(payload.get("last_success_at")),
        last_error_at=_parse_timestamp(payload.get("last_error_at")),
        reason_code=(
            str(payload["reason_code"])
            if payload.get("reason_code") is not None
            else None
        ),
        consecutive_failures=int(payload.get("consecutive_failures") or 0),
        stale_after_seconds=int(payload.get("stale_after_seconds") or 0),
    )


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: object, *, required: bool = False) -> datetime | None:
    if value is None:
        if required:
            raise ValueError("worker health timestamp is required")
        return None
    if not isinstance(value, str):
        raise ValueError("worker health timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("worker health timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)
