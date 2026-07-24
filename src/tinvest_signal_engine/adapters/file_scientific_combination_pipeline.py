"""Bounded and resume-safe file adapters for C1-C5 historical evidence."""

from __future__ import annotations

from dataclasses import asdict, fields
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import heapq
from itertools import groupby
import json
import os
from pathlib import Path
import shutil
from typing import Any, Iterable, Mapping

from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.application.scientific_combination_evidence import (
    ProspectiveScientificPartition,
    ProspectiveScientificPartitionSourceDescriptor,
    ScientificCombinationArtifactReference,
    CombinationEvidenceResult,
    ScientificCombinationPartitionArtifact,
    ScientificCombinationStreamingCompletion,
)
from tinvest_signal_engine.domain.hypothesis_evidence import ChronologicalSplit
from tinvest_signal_engine.domain.prospective_scientific_models import (
    MetricUnit,
    MetricValue,
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    TargetMetric,
)


_SOURCE_SCHEMA = "prospective-scientific-partitions-v1"
_COMBINATION_SCHEMA = "scientific-combination-stream-v1"
_COMBINATION_RETENTION_SCHEMA = "scientific-combination-retention-v1"


class FileProspectiveScientificPartitionStage:
    """Stage sequential one-hypothesis reports and expose day partitions."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._references: dict[ProspectiveHypothesis, Path] = {}

    def stage(
        self,
        report: ProspectiveScientificReport,
        *,
        cost_model_version: str,
    ) -> None:
        if len(report.selected_hypotheses) != 1:
            raise ValueError("partition staging requires one-hypothesis reports")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        hypothesis = report.selected_hypotheses[0]
        existing = self._references.get(hypothesis)
        run_dir = self._root / report.report_fingerprint.removeprefix("sha256:")
        if existing is not None and existing != run_dir:
            raise ValueError("one hypothesis cannot stage two report versions")
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema": _SOURCE_SCHEMA,
            "dataset_fingerprint": report.dataset_fingerprint,
            "report_fingerprint": report.report_fingerprint,
            "hypothesis": hypothesis.value,
            "hypothesis_version": hypothesis.version,
            "cost_model_version": cost_model_version,
            "split": _json_value(report.split),
            "policy": _json_value(report.policy),
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        partition_hashes: dict[str, str] = {}
        pairs = zip(report.features, report.outcomes, strict=True)
        previous_day: date | None = None
        for trading_day, rows in groupby(pairs, key=lambda item: item[0].trading_day):
            if previous_day is not None and trading_day <= previous_day:
                raise ValueError("staged report must be ordered by trading day")
            previous_day = trading_day
            payload = [
                {
                    "feature": _json_value(feature),
                    "outcome": _json_value(outcome),
                }
                for feature, outcome in rows
            ]
            relative = f"partitions/{trading_day.isoformat()}.json"
            path = run_dir / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            _write_once_or_verify(path, _json_bytes(payload))
            partition_hashes[relative] = _file_hash(path)
        completion = {
            "schema": _SOURCE_SCHEMA,
            "manifest_hash": _file_hash(run_dir / "manifest.json"),
            "partition_hashes": partition_hashes,
            "partition_count": len(partition_hashes),
            "observation_count": len(report.features),
        }
        _write_once_or_verify(
            run_dir / "completion.json", _json_bytes(completion)
        )
        self._verify_source(run_dir)
        self._references[hypothesis] = run_dir

    def stage_rows(
        self,
        *,
        dataset_fingerprint: str,
        report_fingerprint: str,
        split: ChronologicalSplit,
        policy: ProspectiveScientificPolicy,
        hypothesis: ProspectiveHypothesis,
        cost_model_version: str,
        rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
    ) -> None:
        """Stage a globally ordered report without retaining a whole day."""

        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        existing = self._references.get(hypothesis)
        run_dir = self._root / report_fingerprint.removeprefix("sha256:")
        if existing is not None and existing != run_dir:
            raise ValueError("one hypothesis cannot stage two report versions")
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema": _SOURCE_SCHEMA,
            "dataset_fingerprint": dataset_fingerprint,
            "report_fingerprint": report_fingerprint,
            "hypothesis": hypothesis.value,
            "hypothesis_version": hypothesis.version,
            "cost_model_version": cost_model_version,
            "split": _json_value(split),
            "policy": _json_value(policy),
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        partition_hashes: dict[str, str] = {}
        observation_count = 0
        previous_day: date | None = None
        for trading_day, day_rows in groupby(rows, key=lambda item: item[0].trading_day):
            if previous_day is not None and trading_day <= previous_day:
                raise ValueError("staged report must be ordered by trading day")
            previous_day = trading_day
            relative = f"partitions/{trading_day.isoformat()}.json"
            path = run_dir / relative
            count = _write_row_array_once_or_verify(path, day_rows)
            observation_count += count
            partition_hashes[relative] = _file_hash(path)
        completion = {
            "schema": _SOURCE_SCHEMA,
            "manifest_hash": _file_hash(run_dir / "manifest.json"),
            "partition_hashes": partition_hashes,
            "partition_count": len(partition_hashes),
            "observation_count": observation_count,
        }
        _write_once_or_verify(
            run_dir / "completion.json", _json_bytes(completion)
        )
        self._verify_source(run_dir)
        self._references[hypothesis] = run_dir

    def describe(self) -> ProspectiveScientificPartitionSourceDescriptor:
        manifests = self._manifests()
        first = next(iter(manifests.values()), None)
        if first is None:
            raise ValueError("no prospective reports were staged")
        for manifest in manifests.values():
            if (
                manifest["dataset_fingerprint"] != first["dataset_fingerprint"]
                or manifest["split"] != first["split"]
                or manifest["policy"] != first["policy"]
                or manifest["cost_model_version"] != first["cost_model_version"]
            ):
                raise ValueError("staged prospective reports are incompatible")
        hypotheses = tuple(sorted(manifests, key=lambda item: item.value))
        report_fingerprint = _fingerprint(
            {
                "schema": _SOURCE_SCHEMA,
                "reports": [
                    {
                        "hypothesis": hypothesis.value,
                        "report_fingerprint": manifests[hypothesis][
                            "report_fingerprint"
                        ],
                    }
                    for hypothesis in hypotheses
                ],
            }
        )
        return ProspectiveScientificPartitionSourceDescriptor(
            dataset_fingerprint=str(first["dataset_fingerprint"]),
            source_report_fingerprint=report_fingerprint,
            split=_split_from_json(first["split"]),
            policy=_policy_from_json(first["policy"]),
            selected_hypotheses=hypotheses,
        )

    def iter_partitions(self) -> Iterable[ProspectiveScientificPartition]:
        self.describe()
        by_day: dict[date, list[Path]] = {}
        for run_dir in self._references.values():
            completion = self._verify_source(run_dir)
            for relative in completion["partition_hashes"]:
                trading_day = date.fromisoformat(Path(relative).stem)
                by_day.setdefault(trading_day, []).append(run_dir / relative)
        for trading_day in sorted(by_day):
            pairs: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
            for path in sorted(by_day[trading_day]):
                payload = json.loads(path.read_text(encoding="utf-8"))
                pairs.extend(
                    (_feature_from_json(item["feature"]), _outcome_from_json(item["outcome"]))
                    for item in payload
                )
            pairs.sort(
                key=lambda item: (
                    item[0].observed_at,
                    item[0].ticker,
                    item[0].hypothesis.value,
                    item[0].horizon_seconds,
                )
            )
            yield ProspectiveScientificPartition(
                trading_day=trading_day,
                features=tuple(item[0] for item in pairs),
                outcomes=tuple(item[1] for item in pairs),
            )

    def _manifests(self) -> dict[ProspectiveHypothesis, Mapping[str, Any]]:
        result = {}
        for hypothesis, run_dir in sorted(
            self._references.items(), key=lambda item: item[0].value
        ):
            self._verify_source(run_dir)
            result[hypothesis] = json.loads(
                (run_dir / "manifest.json").read_text(encoding="utf-8")
            )
        return result

    @staticmethod
    def _verify_source(run_dir: Path) -> Mapping[str, Any]:
        completion_path = run_dir / "completion.json"
        if not completion_path.is_file():
            raise ValueError("prospective partition stage is incomplete")
        completion = json.loads(completion_path.read_text(encoding="utf-8"))
        if completion.get("schema") != _SOURCE_SCHEMA:
            raise ValueError("unsupported prospective partition schema")
        if _file_hash(run_dir / "manifest.json") != completion.get("manifest_hash"):
            raise ValueError("prospective partition manifest failed verification")
        partition_hashes = completion.get("partition_hashes")
        if not isinstance(partition_hashes, Mapping):
            raise ValueError("prospective partition hashes are invalid")
        for relative, expected in partition_hashes.items():
            path = run_dir / str(relative)
            if not path.is_file() or _file_hash(path) != expected:
                raise ValueError("prospective partition failed verification")
        return completion


class FileProspectiveScientificRowSpool:
    """External merge spool retaining at most one derived row per ticker."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        if self._root.exists():
            shutil.rmtree(self._root)
        self._root.mkdir(parents=True, exist_ok=True)
        self._paths: list[Path] = []
        self._observation_count = 0
        self._latest_target_at: datetime | None = None

    @property
    def observation_count(self) -> int:
        return self._observation_count

    @property
    def latest_target_at(self) -> datetime | None:
        return self._latest_target_at

    def stage_partition(
        self,
        rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
    ) -> None:
        path = self._root / f"{len(self._paths):04d}.jsonl"
        previous_key: tuple[object, ...] | None = None
        count = 0
        with path.open("xb") as handle:
            for feature, outcome in rows:
                if feature.observation_id != outcome.observation_id:
                    raise ValueError("feature and outcome identities must remain aligned")
                key = _prospective_row_key((feature, outcome))
                if previous_key is not None and key < previous_key:
                    raise ValueError("spooled ticker rows must be ordered")
                previous_key = key
                handle.write(
                    json.dumps(
                        {
                            "feature": _json_value(feature),
                            "outcome": _json_value(outcome),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
                count += 1
                if (
                    self._latest_target_at is None
                    or outcome.target_at > self._latest_target_at
                ):
                    self._latest_target_at = outcome.target_at
        if count:
            self._paths.append(path)
            self._observation_count += count
        else:
            path.unlink()

    def iter_rows(
        self,
    ) -> Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]]:
        iterators = tuple(self._iter_file(path) for path in self._paths)
        yield from heapq.merge(*iterators, key=_prospective_row_key)

    def close(self) -> None:
        shutil.rmtree(self._root, ignore_errors=True)

    def __enter__(self) -> FileProspectiveScientificRowSpool:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @staticmethod
    def _iter_file(
        path: Path,
    ) -> Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]]:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                payload = json.loads(line)
                yield (
                    _feature_from_json(payload["feature"]),
                    _outcome_from_json(payload["outcome"]),
                )


class FileScientificCombinationStreamingArtifacts:
    """Checkpoint every composed day and atomically seal final evidence."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def load_completed(
        self, run_id: str
    ) -> ScientificCombinationStreamingCompletion | None:
        run_dir = self._run_dir(run_id)
        completion_path = run_dir / "completion.json"
        if not completion_path.is_file():
            return None
        payload = json.loads(completion_path.read_text(encoding="utf-8"))
        self._verify_completion(run_dir, payload)
        return _completion(run_dir, payload, resumed=True)

    def compact_completed(self, artifact_dir: str | Path) -> bool:
        """Remove replayable day checkpoints after sealing aggregate evidence.

        ``results.json`` and the original completion envelope remain byte
        identical.  A retention seal records the original hashes, allowing a
        later resume to verify the same artifact fingerprint without retaining
        multi-gigabyte observation checkpoints.
        """

        run_dir = Path(artifact_dir).resolve(strict=False)
        root = self._root.resolve(strict=False)
        if run_dir.parent != root or run_dir.is_symlink():
            raise ValueError("combination artifact is outside its retention root")
        completion_path = run_dir / "completion.json"
        if not completion_path.is_file() or completion_path.is_symlink():
            raise ValueError("combination completion is missing or unsafe")
        payload = json.loads(completion_path.read_text(encoding="utf-8"))
        self._verify_completion(run_dir, payload)
        retention_path = run_dir / "retention.json"
        already_compacted = retention_path.is_file() and not any(
            (run_dir / str(relative)).exists()
            for relative in payload.get("hashes", {})
            if str(relative).startswith("partitions/")
        )
        if already_compacted:
            return False
        hashes = payload.get("hashes")
        if not isinstance(hashes, Mapping):
            raise ValueError("combination completion hashes are invalid")
        removed_hashes = {
            str(relative): str(expected)
            for relative, expected in hashes.items()
            if str(relative).startswith("partitions/")
        }
        if not removed_hashes:
            return False
        retention = {
            "schema": _COMBINATION_RETENTION_SCHEMA,
            "completion_hash": _file_hash(completion_path),
            "artifact_fingerprint": payload.get("artifact_fingerprint"),
            "removed_hashes": removed_hashes,
        }
        _write_once_or_verify(retention_path, _json_bytes(retention))
        # The retention seal is written only after every original hash has
        # passed verification.  Deleting partitions one-by-one is therefore
        # crash safe and idempotent.
        for relative, expected in sorted(removed_hashes.items()):
            path = run_dir / relative
            if not path.exists():
                continue
            if path.is_symlink() or not path.is_file():
                raise ValueError("combination checkpoint is unsafe")
            if _file_hash(path) != expected:
                raise ValueError("combination checkpoint changed before compaction")
            path.unlink()
        partitions = run_dir / "partitions"
        if partitions.exists():
            if partitions.is_symlink() or not partitions.is_dir():
                raise ValueError("combination partitions directory is unsafe")
            partitions.rmdir()
        self._verify_completion(run_dir, payload)
        return True

    def stage_partition(
        self,
        run_id: str,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
        partition: ScientificCombinationPartitionArtifact,
    ) -> None:
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema": _COMBINATION_SCHEMA,
            "run_id": run_id,
            "dataset_fingerprint": descriptor.dataset_fingerprint,
            "source_report_fingerprint": descriptor.source_report_fingerprint,
            "split": _json_value(descriptor.split),
            "policy": _json_value(descriptor.policy),
            "selected_hypotheses": [
                item.value for item in descriptor.selected_hypotheses
            ],
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        payload = {
            "trading_day": partition.trading_day.isoformat(),
            "observations": [_json_value(item) for item in partition.observations],
            "outcomes": [_json_value(item) for item in partition.outcomes],
        }
        path = run_dir / "partitions" / f"{partition.trading_day.isoformat()}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        _write_once_or_verify(path, _json_bytes(payload))

    def complete(
        self,
        run_id: str,
        descriptor: ProspectiveScientificPartitionSourceDescriptor,
        results: tuple[CombinationEvidenceResult, ...],
        *,
        cost_model_version: str,
        partition_count: int,
        observation_count: int,
    ) -> ScientificCombinationStreamingCompletion:
        del descriptor
        run_dir = self._run_dir(run_id)
        partition_paths = tuple(sorted((run_dir / "partitions").glob("*.json")))
        if len(partition_paths) != partition_count:
            raise ValueError("combination checkpoint partition count drifted")
        results_path = run_dir / "results.json"
        _write_once_or_verify(
            results_path,
            _json_bytes([_json_value(item) for item in results]),
        )
        hashes = {
            str(path.relative_to(run_dir)): _file_hash(path)
            for path in partition_paths
        }
        hashes["manifest.json"] = _file_hash(run_dir / "manifest.json")
        hashes["results.json"] = _file_hash(results_path)
        artifact_fingerprint = _fingerprint(hashes)
        completion = {
            "schema": _COMBINATION_SCHEMA,
            "run_id": run_id,
            "artifact_fingerprint": artifact_fingerprint,
            "cost_model_version": cost_model_version,
            "hashes": hashes,
            "partition_count": partition_count,
            "observation_count": observation_count,
            "result_count": len(results),
        }
        _write_once_or_verify(
            run_dir / "completion.json", _json_bytes(completion)
        )
        self._verify_completion(run_dir, completion)
        return _completion(run_dir, completion, resumed=False)

    def _run_dir(self, run_id: str) -> Path:
        if not run_id.startswith("sha256:") or len(run_id) != 71:
            raise ValueError("combination run_id must be a sha256 fingerprint")
        return self._root / run_id.removeprefix("sha256:")

    @staticmethod
    def _verify_completion(run_dir: Path, payload: Mapping[str, Any]) -> None:
        if payload.get("schema") != _COMBINATION_SCHEMA:
            raise ValueError("unsupported combination completion schema")
        hashes = payload.get("hashes")
        if not isinstance(hashes, Mapping):
            raise ValueError("combination completion hashes are invalid")
        removed_hashes: Mapping[str, Any] = {}
        retention_path = run_dir / "retention.json"
        if retention_path.exists():
            if retention_path.is_symlink() or not retention_path.is_file():
                raise ValueError("combination retention seal is unsafe")
            retention = json.loads(retention_path.read_text(encoding="utf-8"))
            if (
                not isinstance(retention, Mapping)
                or retention.get("schema") != _COMBINATION_RETENTION_SCHEMA
                or retention.get("completion_hash")
                != _file_hash(run_dir / "completion.json")
                or retention.get("artifact_fingerprint")
                != payload.get("artifact_fingerprint")
            ):
                raise ValueError("combination retention seal failed verification")
            candidate_removed = retention.get("removed_hashes")
            if not isinstance(candidate_removed, Mapping):
                raise ValueError("combination retention hashes are invalid")
            if any(
                not str(relative).startswith("partitions/")
                or hashes.get(relative) != expected
                for relative, expected in candidate_removed.items()
            ):
                raise ValueError("combination retention scope is invalid")
            partition_hashes = {
                relative: expected
                for relative, expected in hashes.items()
                if str(relative).startswith("partitions/")
            }
            if dict(candidate_removed) != partition_hashes:
                raise ValueError("combination retention is incomplete")
            removed_hashes = candidate_removed
        for relative, expected in hashes.items():
            path = run_dir / str(relative)
            if (
                path.is_file()
                and not path.is_symlink()
                and _file_hash(path) == expected
            ):
                continue
            if relative in removed_hashes and not path.exists():
                continue
            else:
                raise ValueError("combination artifact failed verification")
        if _fingerprint(hashes) != payload.get("artifact_fingerprint"):
            raise ValueError("combination artifact fingerprint drifted")


def _completion(
    run_dir: Path,
    payload: Mapping[str, Any],
    *,
    resumed: bool,
) -> ScientificCombinationStreamingCompletion:
    return ScientificCombinationStreamingCompletion(
        run_id=str(payload["run_id"]),
        artifact=ScientificCombinationArtifactReference(
            artifact_uri=str(run_dir.resolve()),
            artifact_fingerprint=str(payload["artifact_fingerprint"]),
        ),
        partition_count=int(payload["partition_count"]),
        observation_count=int(payload["observation_count"]),
        result_count=int(payload["result_count"]),
        resumed=resumed,
    )


def _feature_from_json(payload: Mapping[str, Any]) -> ProspectiveFeature:
    return ProspectiveFeature(
        observation_id=str(payload["observation_id"]),
        hypothesis=ProspectiveHypothesis(str(payload["hypothesis"])),
        ticker=str(payload["ticker"]),
        trading_day=date.fromisoformat(str(payload["trading_day"])),
        observed_at=datetime.fromisoformat(str(payload["observed_at"])),
        feature_max_observed_at=datetime.fromisoformat(
            str(payload["feature_max_observed_at"])
        ),
        history_observed_until=(
            datetime.fromisoformat(str(payload["history_observed_until"]))
            if payload.get("history_observed_until") is not None
            else None
        ),
        model_trained_until=(
            datetime.fromisoformat(str(payload["model_trained_until"]))
            if payload.get("model_trained_until") is not None
            else None
        ),
        horizon_seconds=int(payload["horizon_seconds"]),
        target=TargetMetric(str(payload["target"])),
        decision=ProspectiveDecision(str(payload["decision"])),
        reason=ProspectiveReason(str(payload["reason"])),
        expected_direction=int(payload["expected_direction"]),
        forecast=(
            _metric_from_json(payload["forecast"])
            if payload.get("forecast") is not None
            else None
        ),
        feature_values=tuple(
            _metric_from_json(item) for item in payload["feature_values"]
        ),
    )


def _outcome_from_json(payload: Mapping[str, Any]) -> ProspectiveOutcome:
    return ProspectiveOutcome(
        observation_id=str(payload["observation_id"]),
        target_at=datetime.fromisoformat(str(payload["target_at"])),
        available=bool(payload["available"]),
        reason=ProspectiveReason(str(payload["reason"])),
        target=TargetMetric(str(payload["target"])),
        measurements=tuple(
            _metric_from_json(item) for item in payload["measurements"]
        ),
    )


def _metric_from_json(payload: Mapping[str, Any]) -> MetricValue:
    return MetricValue(
        name=str(payload["name"]),
        unit=MetricUnit(str(payload["unit"])),
        value=float(payload["value"]),
    )


def _split_from_json(payload: Mapping[str, Any]) -> ChronologicalSplit:
    return ChronologicalSplit(
        train_days=tuple(date.fromisoformat(item) for item in payload["train_days"]),
        validation_days=tuple(
            date.fromisoformat(item) for item in payload["validation_days"]
        ),
        holdout_days=tuple(
            date.fromisoformat(item) for item in payload["holdout_days"]
        ),
    )


def _policy_from_json(payload: Mapping[str, Any]) -> ProspectiveScientificPolicy:
    values = dict(payload)
    defaults = ProspectiveScientificPolicy()
    for item in fields(ProspectiveScientificPolicy):
        if isinstance(getattr(defaults, item.name), tuple):
            values[item.name] = tuple(values[item.name])
    return ProspectiveScientificPolicy(**values)


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return {key: _json_value(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    raise TypeError(f"cannot serialize combination pipeline value {type(value)!r}")


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _prospective_row_key(
    row: tuple[ProspectiveFeature, ProspectiveOutcome],
) -> tuple[object, ...]:
    feature = row[0]
    return (
        feature.observed_at,
        feature.ticker,
        feature.hypothesis.value,
        feature.horizon_seconds,
    )


def _write_row_array_once_or_verify(
    path: Path,
    rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    count = 0
    try:
        with temporary.open("xb") as handle:
            handle.write(b"[")
            for feature, outcome in rows:
                if feature.observation_id != outcome.observation_id:
                    raise ValueError("feature and outcome identities must remain aligned")
                if count:
                    handle.write(b",")
                handle.write(
                    json.dumps(
                        {
                            "feature": _json_value(feature),
                            "outcome": _json_value(outcome),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                )
                count += 1
            handle.write(b"]\n")
            handle.flush()
            os.fsync(handle.fileno())
        if path.exists():
            if _file_hash(path) != _file_hash(temporary):
                raise ValueError(f"immutable evidence artifact differs: {path.name}")
            temporary.unlink()
        else:
            os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return count


def _write_once_or_verify(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable evidence artifact differs: {path.name}")
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()
