"""Fail-closed filesystem retention for local replay artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import stat
from typing import Any, Callable, Mapping, Sequence

from tinvest_signal_engine.adapters.file_scientific_combination_pipeline import (
    FileScientificCombinationStreamingArtifacts,
)
from tinvest_signal_engine.application.replay_artifact_retention import (
    ReplayArtifactKind,
    ReplayRetentionArtifact,
    ReplayRetentionJob,
    ReplayRetentionPolicy,
    ReplayRetentionStatus,
    plan_replay_artifact_retention,
)


@dataclass(frozen=True, slots=True)
class ReplayArtifactRetentionResult:
    deleted_paths: tuple[str, ...]
    compacted_paths: tuple[str, ...]
    preserved_paths: tuple[str, ...]
    bytes_before: int
    bytes_after: int
    budget_satisfied: bool
    skipped_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _ValidatedTree:
    path: Path
    size_bytes: int
    modified_at: datetime


class LocalReplayArtifactRetention:
    """Collect only paths whose ownership is proven by immutable job state."""

    def __init__(
        self,
        *,
        state_root: str | Path,
        artifact_root: str | Path,
        policy: ReplayRetentionPolicy = ReplayRetentionPolicy(),
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._state_root = Path(state_root)
        self._artifact_root = Path(artifact_root)
        self._policy = policy
        self._clock = clock

    def collect(
        self,
        *,
        safe_to_remove_working: bool,
    ) -> ReplayArtifactRetentionResult:
        """Compact completed evidence and remove deterministic safe candidates.

        A malformed state/result, an escaping artifact URI, or a symlink in a
        candidate causes a fail-closed no-op for owned artifacts.  A working
        tree is process-private and may be removed independently when the
        composition root proves that no replay is using it.
        """

        bytes_before = self._root_size_fail_closed()
        if self._artifact_root.is_symlink():
            return ReplayArtifactRetentionResult(
                deleted_paths=(),
                compacted_paths=(),
                preserved_paths=(),
                bytes_before=bytes_before,
                bytes_after=bytes_before,
                budget_satisfied=False,
                skipped_reason="unsafe_retention_state:ValueError",
            )
        deleted: list[str] = []
        if safe_to_remove_working:
            try:
                working_paths = self._validated_working_children()
                for item in sorted(
                    working_paths,
                    key=lambda row: (row.modified_at, self._relative(row.path)),
                ):
                    self._remove_validated_tree(item.path)
                    deleted.append(self._relative(item.path))
            except (OSError, ValueError):
                # Never follow or unlink an unsafe working-tree symlink.
                pass
        try:
            jobs, _ = self._jobs()
            references = self._sealed_references(jobs)
            has_active_jobs = any(
                item.status
                in {
                    ReplayRetentionStatus.QUEUED,
                    ReplayRetentionStatus.RUNNING,
                }
                for item in jobs
            )
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            current = self._root_size_fail_closed()
            return ReplayArtifactRetentionResult(
                deleted_paths=tuple(deleted),
                compacted_paths=(),
                preserved_paths=(),
                bytes_before=bytes_before,
                bytes_after=current,
                budget_satisfied=current <= self._policy.hard_byte_budget,
                skipped_reason=f"unsafe_retention_state:{type(exc).__name__}",
            )

        if not has_active_jobs:
            try:
                orphan_paths = self._expired_orphan_raw_paths(jobs)
                for item in orphan_paths:
                    self._remove_validated_tree(item.path)
                    deleted.append(self._relative(item.path))
            except (OSError, ValueError):
                # An unowned path is never important enough to make cleanup
                # unsafe for the known, validated job inventory.
                pass

        compacted: list[str] = []
        try:
            combination_paths = (
                ()
                if has_active_jobs
                else self._completed_combination_artifacts(references)
            )
            for path in combination_paths:
                adapter = FileScientificCombinationStreamingArtifacts(path.parent)
                if adapter.compact_completed(path):
                    compacted.append(self._relative(path))
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            # Compaction validates the complete immutable artifact before the
            # first partition is removed.  If validation fails, do not proceed
            # to unrelated deletions during the same pass.
            current = self._root_size_fail_closed()
            return ReplayArtifactRetentionResult(
                deleted_paths=tuple(deleted),
                compacted_paths=tuple(compacted),
                preserved_paths=(),
                bytes_before=bytes_before,
                bytes_after=current,
                budget_satisfied=current <= self._policy.hard_byte_budget,
                skipped_reason=f"unsafe_compaction_state:{type(exc).__name__}",
            )

        try:
            # Size and select only after compaction: otherwise the original
            # checkpoint size could evict one of the two newest sealed sets
            # even though its retained aggregate is small.
            inventory = self._inventory(jobs, references)
            plan = plan_replay_artifact_retention(
                jobs=jobs,
                artifacts=inventory,
                now=self._clock(),
                policy=self._policy,
            )
            deletion_paths = self._paths_for_ids(
                inventory,
                plan.delete_artifact_ids,
            )
            # Validate every destructive unit before touching any of them.
            for item in deletion_paths:
                self._validated_tree(item.path)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            current = self._root_size_fail_closed()
            return ReplayArtifactRetentionResult(
                deleted_paths=tuple(deleted),
                compacted_paths=tuple(compacted),
                preserved_paths=(),
                bytes_before=bytes_before,
                bytes_after=current,
                budget_satisfied=current <= self._policy.hard_byte_budget,
                skipped_reason=f"unsafe_retention_state:{type(exc).__name__}",
            )

        ordered = sorted(
            deletion_paths,
            key=lambda item: (item.modified_at, self._relative(item.path)),
        )
        for item in ordered:
            if not item.path.exists():
                continue
            self._remove_validated_tree(item.path)
            deleted.append(self._relative(item.path))

        bytes_after = self._root_size_fail_closed()
        protected = tuple(
            sorted(
                item.artifact_id
                for item in inventory
                if item.artifact_id in plan.protected_artifact_ids
            )
        )
        return ReplayArtifactRetentionResult(
            deleted_paths=tuple(deleted),
            compacted_paths=tuple(compacted),
            preserved_paths=protected,
            bytes_before=bytes_before,
            bytes_after=bytes_after,
            budget_satisfied=bytes_after <= self._policy.hard_byte_budget,
        )

    def _jobs(
        self,
    ) -> tuple[tuple[ReplayRetentionJob, ...], dict[str, Mapping[str, Any]]]:
        if not self._state_root.exists():
            return (), {}
        if self._state_root.is_symlink() or not self._state_root.is_dir():
            raise ValueError("replay state root must be a real directory")
        jobs: list[ReplayRetentionJob] = []
        states: dict[str, Mapping[str, Any]] = {}
        for job_dir in sorted(self._state_root.iterdir()):
            if job_dir.name.startswith("."):
                continue
            if job_dir.is_symlink() or not job_dir.is_dir():
                raise ValueError("unknown replay state entry")
            state_path = job_dir / "state.json"
            if not state_path.is_file() or state_path.is_symlink():
                raise ValueError("replay job state is missing or unsafe")
            payload = _read_mapping(state_path)
            job_id = str(payload["job_id"])
            if job_id != job_dir.name:
                raise ValueError("replay state ownership mismatch")
            status = ReplayRetentionStatus(str(payload["status"]))
            finished_at = (
                _timestamp(payload["finished_at"])
                if payload.get("finished_at") is not None
                else None
            )
            jobs.append(
                ReplayRetentionJob(
                    job_id=job_id,
                    status=status,
                    run_fingerprint=str(payload["run_fingerprint"]),
                    created_at=_timestamp(payload["created_at"]),
                    finished_at=finished_at,
                )
            )
            states[job_id] = payload
        return tuple(jobs), states

    def _sealed_references(
        self,
        jobs: Sequence[ReplayRetentionJob],
    ) -> dict[Path, list[ReplayRetentionJob]]:
        references: dict[Path, list[ReplayRetentionJob]] = {}
        for job in jobs:
            if job.status is not ReplayRetentionStatus.COMPLETED:
                continue
            result_path = self._state_root / job.job_id / "result.json"
            if not result_path.is_file() or result_path.is_symlink():
                raise ValueError("completed replay result is missing or unsafe")
            result = _read_mapping(result_path)
            if (
                result.get("job_id") != job.job_id
                or result.get("run_fingerprint") != job.run_fingerprint
                or result.get("status") != "completed"
            ):
                raise ValueError("completed replay result identity mismatch")
            engines = result.get("engines")
            if not isinstance(engines, list):
                raise ValueError("completed replay engines must be a list")
            for engine in engines:
                if not isinstance(engine, Mapping):
                    raise ValueError("completed replay engine is malformed")
                uri = engine.get("artifact_uri")
                if uri is None:
                    continue
                path = self._contained_path(str(uri))
                references.setdefault(path, []).append(job)
        return references

    def _inventory(
        self,
        jobs: Sequence[ReplayRetentionJob],
        references: Mapping[Path, Sequence[ReplayRetentionJob]],
    ) -> tuple[ReplayRetentionArtifact, ...]:
        by_fingerprint = {item.run_fingerprint: item for item in jobs}
        rows: list[ReplayRetentionArtifact] = []
        for kind, base in (
            (
                ReplayArtifactKind.RAW_COMBINATION_SOURCE,
                self._artifact_root / "scientific-combinations" / "source",
            ),
            (
                ReplayArtifactKind.INTERMEDIATE_SPOOL,
                self._artifact_root / "prospective-spool",
            ),
        ):
            if not base.exists():
                continue
            if base.is_symlink() or not base.is_dir():
                raise ValueError("raw replay root is unsafe")
            for path in sorted(base.iterdir()):
                if path.is_symlink() or not path.is_dir():
                    raise ValueError("raw replay entry is unsafe")
                fingerprint = "sha256:" + path.name
                owner = by_fingerprint.get(fingerprint)
                if owner is None:
                    # Unknown ownership is explicitly preserved.
                    continue
                tree = self._validated_tree(path)
                rows.append(
                    ReplayRetentionArtifact(
                        artifact_id=self._relative(path),
                        kind=kind,
                        owner_run_fingerprint=owner.run_fingerprint,
                        size_bytes=tree.size_bytes,
                        modified_at=tree.modified_at,
                    )
                )

        for path, owners in sorted(references.items(), key=lambda item: str(item[0])):
            if not path.exists():
                # An older result may already have been retained only as its
                # sealed aggregate JSON and fingerprints.
                continue
            tree = self._validated_tree(path)
            newest_owner = max(
                owners,
                key=lambda item: (
                    item.finished_at or item.created_at,
                    item.job_id,
                ),
            )
            rows.append(
                ReplayRetentionArtifact(
                    artifact_id=self._relative(path),
                    kind=ReplayArtifactKind.SEALED_ARTIFACT,
                    owner_run_fingerprint=newest_owner.run_fingerprint,
                    size_bytes=tree.size_bytes,
                    modified_at=tree.modified_at,
                    referenced_by_job_ids=tuple(
                        sorted({item.job_id for item in owners})
                    ),
                )
            )
        return tuple(rows)

    def _completed_combination_artifacts(
        self,
        references: Mapping[Path, Sequence[ReplayRetentionJob]],
    ) -> tuple[Path, ...]:
        expected = (
            self._artifact_root / "scientific-combinations" / "evidence"
        ).resolve(strict=False)
        if not expected.exists():
            return ()
        if expected.is_symlink() or not expected.is_dir():
            raise ValueError("combination evidence root is unsafe")
        result: list[Path] = []
        referenced = {path.resolve(strict=False) for path in references}
        for path in sorted(expected.iterdir()):
            if path.is_symlink() or not path.is_dir():
                raise ValueError("combination evidence entry is unsafe")
            if len(path.name) != 64 or any(
                character not in "0123456789abcdef" for character in path.name
            ):
                # Unknown names are preserved fail-closed.  Canonically named
                # completed artifacts are safe to compact even when an old
                # job state no longer references them: the retention seal
                # keeps every original partition hash for resume validation.
                continue
            completion_path = path / "completion.json"
            if completion_path.is_file() and not completion_path.is_symlink():
                result.append(path)
            elif path.resolve(strict=False) in referenced:
                raise ValueError("referenced combination completion is missing")
        return tuple(sorted(result))

    def _validated_working_children(self) -> tuple[_ValidatedTree, ...]:
        root = self._artifact_root / ".replay-working"
        if not root.exists():
            return ()
        if root.is_symlink() or not root.is_dir():
            raise ValueError("working root is unsafe")
        return tuple(self._validated_tree(path) for path in sorted(root.iterdir()))

    def _expired_orphan_raw_paths(
        self,
        jobs: Sequence[ReplayRetentionJob],
    ) -> tuple[_ValidatedTree, ...]:
        """Return old derived trees that cannot belong to any persisted job.

        Orphans are considered only when no replay is active.  Their directory
        name must be a canonical SHA-256 fingerprint and they must remain
        untouched for the full grace period before becoming removable.
        """

        known = {item.run_fingerprint.removeprefix("sha256:") for item in jobs}
        cutoff = self._clock() - self._policy.orphan_ttl
        rows: list[_ValidatedTree] = []
        for base in (
            self._artifact_root / "scientific-combinations" / "source",
            self._artifact_root / "prospective-spool",
        ):
            if not base.exists():
                continue
            if base.is_symlink() or not base.is_dir():
                raise ValueError("orphan replay root is unsafe")
            for path in sorted(base.iterdir()):
                if path.name in known:
                    continue
                if len(path.name) != 64 or any(
                    character not in "0123456789abcdef" for character in path.name
                ):
                    continue
                tree = self._validated_tree(path)
                if tree.modified_at <= cutoff:
                    rows.append(tree)
        return tuple(sorted(rows, key=lambda item: (item.modified_at, str(item.path))))

    def _paths_for_ids(
        self,
        inventory: Sequence[ReplayRetentionArtifact],
        artifact_ids: Sequence[str],
    ) -> tuple[_ValidatedTree, ...]:
        by_id = {item.artifact_id: item for item in inventory}
        rows: list[_ValidatedTree] = []
        for artifact_id in artifact_ids:
            item = by_id[artifact_id]
            rows.append(
                _ValidatedTree(
                    path=self._contained_path(artifact_id),
                    size_bytes=item.size_bytes,
                    modified_at=item.modified_at,
                )
            )
        return tuple(rows)

    def _validated_tree(self, path: Path) -> _ValidatedTree:
        path = self._contained_path(path)
        if path == self._artifact_root.resolve(strict=False):
            raise ValueError("artifact root cannot be a deletion unit")
        size = 0
        latest_ns = 0
        for root, directories, files in os.walk(path, followlinks=False):
            root_path = Path(root)
            root_stat = root_path.lstat()
            if stat.S_ISLNK(root_stat.st_mode):
                raise ValueError("symlinked replay directory is unsafe")
            latest_ns = max(latest_ns, root_stat.st_mtime_ns)
            for name in (*directories, *files):
                child = root_path / name
                child_stat = child.lstat()
                if stat.S_ISLNK(child_stat.st_mode):
                    raise ValueError("symlink inside replay artifact is unsafe")
                latest_ns = max(latest_ns, child_stat.st_mtime_ns)
                if stat.S_ISREG(child_stat.st_mode):
                    size += child_stat.st_size
                elif not stat.S_ISDIR(child_stat.st_mode):
                    raise ValueError("special replay artifact file is unsafe")
        return _ValidatedTree(
            path=path,
            size_bytes=size,
            modified_at=datetime.fromtimestamp(
                latest_ns / 1_000_000_000,
                tz=timezone.utc,
            ),
        )

    def _contained_path(self, value: str | Path) -> Path:
        raw = Path(value)
        if not raw.is_absolute():
            raw = self._artifact_root / raw
        resolved = raw.resolve(strict=False)
        root = self._artifact_root.resolve(strict=False)
        if not resolved.is_relative_to(root):
            raise ValueError("replay artifact path escapes its root")
        return resolved

    def _relative(self, path: Path) -> str:
        return str(
            self._contained_path(path).relative_to(
                self._artifact_root.resolve(strict=False)
            )
        )

    def _remove_validated_tree(self, path: Path) -> None:
        tree = self._validated_tree(path)
        if tree.path.is_dir():
            shutil.rmtree(tree.path)
        else:
            tree.path.unlink()

    def _root_size_fail_closed(self) -> int:
        if not self._artifact_root.exists():
            return 0
        try:
            return self._tree_size(self._artifact_root.resolve(strict=False))[0]
        except (OSError, ValueError):
            return 0

    def _tree_size(self, path: Path) -> tuple[int, int]:
        if path.is_symlink() or not path.is_dir():
            raise ValueError("replay artifact tree root is unsafe")
        size = 0
        latest_ns = path.lstat().st_mtime_ns
        for root, directories, files in os.walk(path, followlinks=False):
            root_path = Path(root)
            root_stat = root_path.lstat()
            if stat.S_ISLNK(root_stat.st_mode):
                raise ValueError("symlinked replay directory is unsafe")
            latest_ns = max(latest_ns, root_stat.st_mtime_ns)
            for name in (*directories, *files):
                child = root_path / name
                child_stat = child.lstat()
                if stat.S_ISLNK(child_stat.st_mode):
                    raise ValueError("symlink inside replay artifact is unsafe")
                latest_ns = max(latest_ns, child_stat.st_mtime_ns)
                if stat.S_ISREG(child_stat.st_mode):
                    size += child_stat.st_size
                elif not stat.S_ISDIR(child_stat.st_mode):
                    raise ValueError("special replay artifact file is unsafe")
        return size, latest_ns


def _read_mapping(path: Path) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("replay JSON root must be an object")
    return payload


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("replay timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)
