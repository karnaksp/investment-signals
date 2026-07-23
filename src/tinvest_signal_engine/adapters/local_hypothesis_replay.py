"""Local-only candle cache reader and immutable replay artifact store."""

from __future__ import annotations

import csv
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import json
import os
from pathlib import Path
from collections.abc import Iterator
from typing import Any, Iterable, Mapping

from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    CompletedReplay,
    HistoricalCandle,
    HistoricalReplayReport,
)
from tinvest_signal_engine.domain.hypothesis_formulas import HypothesisId


class LocalCandleCache:
    """Read an immutable cache; this adapter contains no HTTP/broker client."""

    def __init__(self, cache_dir: str | Path) -> None:
        self._cache_dir = Path(cache_dir)
        self._manifest_path = self._cache_dir / "manifest.json"
        self._descriptor_cache: CandleCacheDescriptor | None = None

    def describe(self) -> CandleCacheDescriptor:
        if self._descriptor_cache is not None:
            return self._descriptor_cache
        manifest = self._manifest()
        scope = _mapping(manifest.get("scope"), "manifest.scope")
        quality = _mapping(manifest.get("quality"), "manifest.quality")
        fingerprint = str(manifest.get("content_fingerprint", "")).strip()
        if len(fingerprint) != 64:
            raise ValueError("cache manifest content_fingerprint must be sha256 hex")
        files = self._partition_files()
        declared_count = int(quality.get("partition_count", 0))
        if len(files) == declared_count:
            tickers = tuple(
                str(item).upper()
                for item in _sequence(scope.get("tickers"), "scope.tickers")
            )
            start_day = date.fromisoformat(str(scope["from"]))
            end_day = date.fromisoformat(str(scope["to"]))
            resolved_fingerprint = fingerprint
        else:
            tickers = tuple(
                sorted(
                    {path.parent.name.removeprefix("ticker=").upper() for path in files}
                )
            )
            days = tuple(sorted(_partition_day(path) for path in files))
            if not days:
                raise ValueError("candle cache has no partition files")
            start_day, end_day = days[0], days[-1]
            resolved_fingerprint = _partition_fingerprint(self._cache_dir, files)
        self._descriptor_cache = CandleCacheDescriptor(
            dataset_fingerprint=f"sha256:{resolved_fingerprint}",
            partition_count=len(files),
            tickers=tickers,
            start_day=start_day,
            end_day=end_day,
        )
        return self._descriptor_cache

    def load(self) -> tuple[HistoricalCandle, ...]:
        return tuple(
            candle
            for partition in self.iter_ticker_partitions()
            for candle in partition
        )

    def iter_ticker_partitions(self) -> Iterator[tuple[HistoricalCandle, ...]]:
        """Yield one ordered ticker at a time instead of sealing the full cache."""

        descriptor = self.describe()
        files = self._partition_files()
        if len(files) != descriptor.partition_count:
            raise ValueError(
                "cache partition count differs from immutable manifest: "
                f"expected {descriptor.partition_count}, found {len(files)}"
            )
        by_ticker: dict[str, list[Path]] = {}
        for path in files:
            ticker = path.parent.name.removeprefix("ticker=").upper()
            by_ticker.setdefault(ticker, []).append(path)
        for ticker in sorted(by_ticker):
            records = self._read_partition_files(tuple(by_ticker[ticker]))
            candles = tuple(
                sorted(
                    (_candle(record) for record in records),
                    key=lambda item: item.at,
                )
            )
            if candles:
                yield candles

    def _read_partition_files(
        self,
        files: tuple[Path, ...],
    ) -> list[Mapping[str, object]]:
        records: list[Mapping[str, object]] = []
        parquet = [path for path in files if path.suffix == ".parquet"]
        records.extend(self._read_parquet(parquet))
        for path in files:
            if path.suffix == ".csv":
                with path.open(encoding="utf-8", newline="") as handle:
                    records.extend(dict(row) for row in csv.DictReader(handle))
            elif path.suffix == ".jsonl":
                records.extend(
                    json.loads(line)
                    for line in path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                )
        return records

    def _manifest(self) -> Mapping[str, object]:
        if not self._manifest_path.is_file():
            raise FileNotFoundError(
                f"candle cache manifest not found: {self._manifest_path}"
            )
        payload = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("cache manifest root must be an object")
        if payload.get("kind") != "tinvest_research_candle_cache":
            raise ValueError("cache manifest kind is not a candle cache")
        privacy = _mapping(payload.get("privacy"), "manifest.privacy")
        if any(
            privacy.get(field) is not False
            for field in (
                "tokens_persisted",
                "account_identifiers_persisted",
                "instrument_uids_persisted",
            )
        ):
            raise ValueError("cache privacy declaration is incomplete")
        return payload

    def _partition_files(self) -> tuple[Path, ...]:
        files = [
            path
            for suffix in (".parquet", ".csv", ".jsonl")
            for path in self._cache_dir.glob(f"ticker=*/date=*{suffix}")
        ]
        return tuple(sorted(files))

    @staticmethod
    def _read_parquet(paths: Iterable[Path]) -> list[Mapping[str, object]]:
        files = tuple(paths)
        if not files:
            return []
        try:
            import duckdb  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "DuckDB is required to read the existing Parquet candle cache; "
                "install the research extra"
            ) from exc
        connection = duckdb.connect(database=":memory:")
        try:
            rows = connection.execute(
                "SELECT * FROM read_parquet(?)",
                [[str(path) for path in files]],
            ).fetchall()
            columns = [column[0] for column in connection.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            connection.close()


class ImmutableReplayArtifactStore:
    """Write deterministic files once and verify them before resume."""

    _ARTIFACT_NAMES = (
        "manifest.json",
        "split.json",
        "summaries.json",
        "outcomes.jsonl",
        "evidence.json",
    )

    def __init__(self, output_dir: str | Path) -> None:
        self._output_dir = Path(output_dir)

    def load_completed(self, run_id: str) -> CompletedReplay | None:
        run_dir = self._run_dir(run_id)
        completion_path = run_dir / "completion.json"
        if not completion_path.is_file():
            return None
        completion = json.loads(completion_path.read_text(encoding="utf-8"))
        if completion.get("run_id") != run_id:
            raise ValueError("replay completion identity mismatch")
        hashes = _mapping(
            completion.get("artifact_hashes"), "completion.artifact_hashes"
        )
        for name in self._ARTIFACT_NAMES:
            path = run_dir / name
            if not path.is_file() or _file_hash(path) != hashes.get(name):
                raise ValueError(
                    f"immutable replay artifact failed verification: {name}"
                )
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        selected = tuple(HypothesisId(item) for item in manifest["selected_hypotheses"])
        return CompletedReplay(
            run_id=run_id,
            artifact_fingerprint=str(completion["artifact_fingerprint"]),
            dataset_fingerprint=str(manifest["dataset_fingerprint"]),
            selected_hypotheses=selected,
            resumed=True,
        )

    def save(self, report: HistoricalReplayReport) -> CompletedReplay:
        run_dir = self._run_dir(report.run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        payloads = {
            "manifest.json": _json_bytes(
                {
                    "run_id": report.run_id,
                    "engine_version": report.engine_version,
                    "dataset_fingerprint": report.dataset_fingerprint,
                    "cache_partition_count": report.cache_partition_count,
                    "selected_hypotheses": [
                        item.value for item in report.selected_hypotheses
                    ],
                    "cost_model": _json_value(report.cost_model),
                }
            ),
            "split.json": _json_bytes(_json_value(report.split)),
            "summaries.json": _json_bytes(_json_value(report.summaries)),
            "evidence.json": _json_bytes(_json_value(report.evidence)),
        }
        hashes: dict[str, str] = {}
        for name in self._ARTIFACT_NAMES:
            path = run_dir / name
            if name == "outcomes.jsonl":
                hashes[name] = _write_jsonl_once_or_verify(
                    path,
                    report.outcomes,
                )
            else:
                _write_once_or_verify(path, payloads[name])
                hashes[name] = _file_hash(path)
        artifact_fingerprint = (
            "sha256:"
            + sha256(
                json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        completion = {
            "run_id": report.run_id,
            "artifact_hashes": hashes,
            "artifact_fingerprint": artifact_fingerprint,
        }
        _write_once_or_verify(run_dir / "completion.json", _json_bytes(completion))
        return CompletedReplay(
            run_id=report.run_id,
            artifact_fingerprint=artifact_fingerprint,
            dataset_fingerprint=report.dataset_fingerprint,
            selected_hypotheses=report.selected_hypotheses,
            resumed=False,
        )

    def _run_dir(self, run_id: str) -> Path:
        if not run_id.startswith("sha256:") or len(run_id) != 71:
            raise ValueError("run_id must be a sha256 fingerprint")
        return self._output_dir / run_id.removeprefix("sha256:")


def _candle(record: Mapping[str, object]) -> HistoricalCandle:
    raw_at = record["at"]
    at = (
        raw_at
        if isinstance(raw_at, datetime)
        else datetime.fromisoformat(str(raw_at).replace("Z", "+00:00"))
    )
    return HistoricalCandle(
        ticker=str(record["ticker"]).upper(),
        at=at,
        open=float(record["open"]),
        high=float(record["high"]),
        low=float(record["low"]),
        close=float(record["close"]),
        volume=float(record["volume"]),
        complete=str(record.get("complete", True)).lower() in {"1", "true", "yes"},
    )


def _mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{location} must be an object")
    return value


def _sequence(value: object, location: str) -> tuple[object, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{location} must be a list")
    return tuple(value)


def _json_value(value: object) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if is_dataclass(value):
        return {
            field.name: _json_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise TypeError(f"cannot serialize replay value {type(value)!r}")


def _json_bytes(value: object, *, newline: bool = False) -> bytes:
    suffix = "\n" if newline else ""
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + suffix
    ).encode("utf-8")


def _write_once_or_verify(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(
                f"refusing to overwrite immutable replay artifact: {path.name}"
            )
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _write_jsonl_once_or_verify(
    path: Path,
    rows: Iterable[object],
) -> str:
    """Write deterministic JSON Lines without building one giant byte string."""

    if path.exists():
        expected = sha256()
        expected_size = 0
        for row in rows:
            payload = _json_bytes(_json_value(row), newline=True)
            expected.update(payload)
            expected_size += len(payload)
        expected_hash = "sha256:" + expected.hexdigest()
        if path.stat().st_size != expected_size or _file_hash(path) != expected_hash:
            raise ValueError(
                f"refusing to overwrite immutable replay artifact: {path.name}"
            )
        return expected_hash

    digest = sha256()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            for row in rows:
                payload = _json_bytes(_json_value(row), newline=True)
                handle.write(payload)
                digest.update(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return "sha256:" + digest.hexdigest()


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _partition_day(path: Path) -> date:
    return date.fromisoformat(path.stem.removeprefix("date="))


def _partition_fingerprint(cache_dir: Path, files: Iterable[Path]) -> str:
    digest = sha256()
    for path in files:
        digest.update(str(path.relative_to(cache_dir)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(_file_hash(path).removeprefix("sha256:")))
        digest.update(b"\n")
    return digest.hexdigest()
