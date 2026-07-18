"""Filesystem adapters for immutable candle-cache replay artifacts."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from tinvest_signal_engine.application.jump_activity_replay import JumpReplayResult
from tinvest_signal_engine.domain.jump_activity_replay import CandleBar


class ParquetCandleCacheAdapter:
    """Read existing Parquet partitions without broker access or mutation."""

    def __init__(self, cache_dir: Path) -> None:
        self._cache_dir = cache_dir

    def fingerprint(self, tickers: Sequence[str] | None = None) -> str:
        files = self._partition_files(tickers)
        digest = sha256()
        for path in files:
            digest.update(str(path.relative_to(self._cache_dir)).encode("utf-8"))
            digest.update(b"\0")
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
            digest.update(b"\n")
        return f"sha256:{digest.hexdigest()}"

    def load(self, tickers: Sequence[str] | None = None) -> tuple[CandleBar, ...]:
        files = self._partition_files(tickers)
        try:
            import duckdb  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "DuckDB is required to read the local Parquet candle cache; "
                "install the research extra"
            ) from exc
        connection = duckdb.connect(database=":memory:")
        candles: list[CandleBar] = []
        try:
            cursor = connection.execute(
                """
                SELECT
                    ticker,
                    CAST("at" AS VARCHAR) AS at_text,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    COALESCE(complete, true) AS complete
                FROM read_parquet(?)
                ORDER BY ticker, "at"
                """,
                [[str(path) for path in files]],
            )
            while rows := cursor.fetchmany(20_000):
                candles.extend(_candle_from_row(row) for row in rows)
        finally:
            connection.close()
        return tuple(candles)

    def _partition_files(self, tickers: Sequence[str] | None) -> tuple[Path, ...]:
        if tickers:
            files = tuple(
                path
                for ticker in sorted({item.strip().upper() for item in tickers})
                for path in sorted(
                    (self._cache_dir / f"ticker={ticker}").glob("date=*.parquet")
                )
            )
        else:
            files = tuple(sorted(self._cache_dir.glob("ticker=*/date=*.parquet")))
        if not files:
            raise RuntimeError(f"No candle partitions found in {self._cache_dir}")
        return files


class JsonJumpReplayArtifactAdapter:
    """Persist deterministic evidence, controls, and observations atomically."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None:
        complete = self._output_dir / run_id / "complete.json"
        if not complete.is_file():
            return None
        try:
            payload = json.loads(complete.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if (
            payload.get("run_id") != run_id
            or payload.get("input_fingerprint") != input_fingerprint
            or payload.get("status") != "completed"
        ):
            return None
        return str(complete.parent.resolve())

    def persist(self, result: JumpReplayResult) -> str:
        run_dir = self._output_dir / result.run_id
        complete_path = run_dir / "complete.json"
        if complete_path.is_file():
            try:
                existing = json.loads(complete_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise RuntimeError(
                    f"completed replay marker is unreadable: {complete_path}"
                ) from exc
            if (
                existing.get("input_fingerprint") == result.input_fingerprint
                and existing.get("policy_fingerprint") == result.policy_fingerprint
                and existing.get("run_id") == result.run_id
            ):
                return str(run_dir.resolve())
            raise RuntimeError(f"refusing to overwrite immutable replay {result.run_id}")
        run_dir.mkdir(parents=True, exist_ok=True)
        evidence_rows = [
            {
                "hypothesis": item.hypothesis.value,
                "horizon_seconds": item.horizon_seconds,
                "bundle": _json_value(item.bundle),
                "unmatched_event_ids": item.matched_controls.unmatched_event_ids,
                "matched_sample_summary": {
                    "mean_lift_bps": item.matched_mean_lift_bps,
                    "positive_lift_rate": item.matched_positive_lift_rate,
                },
            }
            for item in result.evidence
        ]
        control_rows = (
            {
                "hypothesis": item.hypothesis.value,
                "horizon_seconds": item.horizon_seconds,
                "event_id": group.event.point_id,
                "event_at": group.event.occurred_at,
                "event_net_effect_bps": group.event.net_effect_bps,
                "control_ids": tuple(control.point_id for control in group.controls),
                "control_net_effect_bps": tuple(
                    control.net_effect_bps for control in group.controls
                ),
                "lift_bps": group.lift_bps,
            }
            for item in result.evidence
            for group in item.matched_controls.groups
        )
        observation_rows = (
            _json_value(observation) for observation in result.observations
        )
        manifest = {
            "schema_version": 1,
            "kind": "h3_h4_historical_replay",
            "source": "existing_local_parquet_cache_no_download",
            "run_id": result.run_id,
            "input_fingerprint": result.input_fingerprint,
            "policy_fingerprint": result.policy_fingerprint,
            "policy": _json_value(result.policy),
            "split": _json_value(result.split),
            "counts": {
                "candles": result.candle_count,
                "raw_features": result.raw_feature_count,
                "classified_observations": len(result.observations),
                "evidence_tests": len(result.evidence),
            },
            "thresholds": _json_value(result.thresholds),
            "artifacts": {
                "evidence": "evidence.json",
                "observations": "observations.jsonl",
                "matched_controls": "matched-controls.jsonl",
            },
        }
        _atomic_json(run_dir / "manifest.json", manifest)
        _atomic_json(run_dir / "evidence.json", evidence_rows)
        _atomic_json_lines(run_dir / "observations.jsonl", observation_rows)
        _atomic_json_lines(run_dir / "matched-controls.jsonl", control_rows)
        _atomic_json(
            complete_path,
            {
                "status": "completed",
                "run_id": result.run_id,
                "input_fingerprint": result.input_fingerprint,
                "policy_fingerprint": result.policy_fingerprint,
            },
        )
        return str(run_dir.resolve())


def _candle_from_row(row: Sequence[object]) -> CandleBar:
    raw_at = str(row[1]).replace(" ", "T", 1)
    opened_at = datetime.fromisoformat(raw_at)
    if opened_at.tzinfo is None or opened_at.utcoffset() is None:
        raise ValueError("cached candle timestamp must be timezone-aware")
    return CandleBar(
        ticker=str(row[0]).upper(),
        opened_at=opened_at,
        open_price=float(row[2]),
        high_price=float(row[3]),
        low_price=float(row[4]),
        close_price=float(row[5]),
        volume=float(row[6]),
        complete=bool(row[7]),
    )


def _json_value(value: object) -> Any:
    if is_dataclass(value):
        return _json_value(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _atomic_json(path: Path, payload: object) -> None:
    _atomic_text(
        path,
        json.dumps(
            _json_value(payload),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    )


def _atomic_json_lines(path: Path, rows: Iterable[object]) -> None:
    _atomic_text(
        path,
        "".join(
            json.dumps(_json_value(row), ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ),
    )


def _atomic_text(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
