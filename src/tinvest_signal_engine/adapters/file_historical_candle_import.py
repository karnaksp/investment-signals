"""Checksummed atomic progress and aggregate result artifacts."""

from __future__ import annotations

from datetime import date, datetime
from contextlib import contextmanager
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Mapping
from uuid import uuid4

from tinvest_signal_engine.domain.historical_candle_import import (
    HistoricalCandleImportProgress,
    HistoricalCandleImportResult,
    HistoricalCandlePartitionKey,
    HistoricalImportPartitionProgress,
    HistoricalImportState,
)


class AtomicFileHistoricalCandleImportProgress:
    SCHEMA_VERSION = 1

    def __init__(self, state_dir: str | Path) -> None:
        self._state_dir = Path(state_dir).expanduser().resolve()
        self._progress_path = self._state_dir / "progress.json"
        self._result_path = self._state_dir / "last-result.json"

    def load(self) -> HistoricalCandleImportProgress | None:
        if not self._progress_path.is_file():
            return None
        envelope = json.loads(self._progress_path.read_text(encoding="utf-8"))
        if not isinstance(envelope, Mapping):
            raise ValueError("historical import progress envelope is invalid")
        if envelope.get("schema_version") != self.SCHEMA_VERSION:
            raise ValueError("historical import progress schema is unsupported")
        payload = envelope.get("payload")
        if not isinstance(payload, Mapping):
            raise ValueError("historical import progress payload is invalid")
        expected = _checksum(payload)
        if envelope.get("payload_checksum") != expected:
            raise ValueError("historical import progress checksum does not match")
        return _progress(payload)

    def save(self, progress: HistoricalCandleImportProgress) -> None:
        payload = _progress_payload(progress)
        _atomic_json(
            self._progress_path,
            {
                "schema_version": self.SCHEMA_VERSION,
                "payload_checksum": _checksum(payload),
                "payload": payload,
            },
        )

    def publish_result(self, result: HistoricalCandleImportResult) -> None:
        additional = result.total_partitions - result.manifest_covered_partitions
        _atomic_json(
            self._result_path,
            {
                "schema_version": self.SCHEMA_VERSION,
                "operation": "run",
                "status": result.state.value,
                "dry_run": result.dry_run,
                "run_id": result.run_id,
                "source": {
                    "inventory_fingerprint": result.inventory_fingerprint,
                    "partitions_total": result.total_partitions,
                    "manifest_partitions": result.manifest_covered_partitions,
                    "additional_validated_partitions": additional,
                    "rows": result.source_rows,
                    "gap_markers": result.gap_rows,
                },
                "destination": {
                    "partitions_completed": result.completed_partitions,
                    "inserted_rows": result.inserted_rows,
                    "existing_rows": result.existing_rows,
                    "insert_batches": result.insert_batches,
                    "query_batches": result.query_batches,
                },
            },
        )

    def status_payload(self) -> dict[str, object]:
        progress = self.load()
        if progress is None:
            return {
                "schema_version": self.SCHEMA_VERSION,
                "operation": "status",
                "status": HistoricalImportState.NOT_STARTED.value,
            }
        return {
            "schema_version": self.SCHEMA_VERSION,
            "operation": "status",
            "status": progress.state.value,
            "run_id": progress.run_id,
            "inventory_fingerprint": progress.inventory_fingerprint,
            "started_at": progress.started_at.isoformat(),
            "updated_at": progress.updated_at.isoformat(),
            "partitions_completed": len(progress.partitions),
            "partitions_total": progress.total_partitions,
            "manifest_partitions": progress.manifest_covered_partitions,
            "additional_validated_partitions": (
                progress.total_partitions - progress.manifest_covered_partitions
            ),
            "rows": sum(item.source_rows for item in progress.partitions),
            "gap_markers": sum(item.gap_rows for item in progress.partitions),
            "inserted_rows": sum(item.inserted_rows for item in progress.partitions),
            "existing_rows": sum(item.existing_rows for item in progress.partitions),
            "failure_reason_code": progress.failure_reason_code,
        }

    @contextmanager
    def exclusive_run(self):
        """Reject concurrent writers while status readers remain lock-free."""

        self._state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._state_dir / "import.lock"
        with lock_path.open("a+", encoding="utf-8") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise HistoricalCandleImportAlreadyRunning(
                    "another historical candle import holds the state lock"
                ) from error
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            os.fsync(handle.fileno())
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class HistoricalCandleImportAlreadyRunning(RuntimeError):
    pass


def result_payload(result: HistoricalCandleImportResult) -> dict[str, object]:
    additional = result.total_partitions - result.manifest_covered_partitions
    return {
        "schema_version": 1,
        "operation": "run",
        "status": result.state.value,
        "dry_run": result.dry_run,
        "run_id": result.run_id,
        "source": {
            "inventory_fingerprint": result.inventory_fingerprint,
            "partitions_total": result.total_partitions,
            "manifest_partitions": result.manifest_covered_partitions,
            "additional_validated_partitions": additional,
            "rows": result.source_rows,
            "gap_markers": result.gap_rows,
        },
        "destination": {
            "partitions_completed": result.completed_partitions,
            "inserted_rows": result.inserted_rows,
            "existing_rows": result.existing_rows,
            "insert_batches": result.insert_batches,
            "query_batches": result.query_batches,
        },
    }


def _progress_payload(progress: HistoricalCandleImportProgress) -> dict[str, object]:
    return {
        "run_id": progress.run_id,
        "state": progress.state.value,
        "inventory_fingerprint": progress.inventory_fingerprint,
        "source_manifest_checksum": progress.source_manifest_checksum,
        "started_at": progress.started_at.isoformat(),
        "updated_at": progress.updated_at.isoformat(),
        "total_partitions": progress.total_partitions,
        "manifest_covered_partitions": progress.manifest_covered_partitions,
        "failure_reason_code": progress.failure_reason_code,
        "partitions": [
            {
                "ticker": item.key.ticker,
                "trading_day": item.key.trading_day.isoformat(),
                "file_checksum": item.file_checksum,
                "content_fingerprint": item.content_fingerprint,
                "source_rows": item.source_rows,
                "inserted_rows": item.inserted_rows,
                "existing_rows": item.existing_rows,
                "gap_rows": item.gap_rows,
                "completed_at": item.completed_at.isoformat(),
            }
            for item in progress.partitions
        ],
    }


def _progress(payload: Mapping[str, object]) -> HistoricalCandleImportProgress:
    raw_partitions = payload.get("partitions", ())
    if not isinstance(raw_partitions, list):
        raise ValueError("historical import progress partitions are invalid")
    partitions = tuple(
        HistoricalImportPartitionProgress(
            key=HistoricalCandlePartitionKey(
                str(item["ticker"]), date.fromisoformat(str(item["trading_day"]))
            ),
            file_checksum=str(item["file_checksum"]),
            content_fingerprint=str(item["content_fingerprint"]),
            source_rows=int(item["source_rows"]),
            inserted_rows=int(item["inserted_rows"]),
            existing_rows=int(item["existing_rows"]),
            gap_rows=int(item.get("gap_rows", 0)),
            completed_at=datetime.fromisoformat(str(item["completed_at"])),
        )
        for item in raw_partitions
        if isinstance(item, Mapping)
    )
    failure = payload.get("failure_reason_code")
    return HistoricalCandleImportProgress(
        run_id=str(payload["run_id"]),
        state=HistoricalImportState(str(payload["state"])),
        inventory_fingerprint=str(payload["inventory_fingerprint"]),
        source_manifest_checksum=str(payload["source_manifest_checksum"]),
        started_at=datetime.fromisoformat(str(payload["started_at"])),
        updated_at=datetime.fromisoformat(str(payload["updated_at"])),
        total_partitions=int(payload["total_partitions"]),
        manifest_covered_partitions=int(payload["manifest_covered_partitions"]),
        partitions=partitions,
        failure_reason_code=str(failure) if failure is not None else None,
    )


def _checksum(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
