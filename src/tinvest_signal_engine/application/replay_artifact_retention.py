"""Policy for bounded local hypothesis-replay artifacts.

The application layer deliberately reasons about opaque artifact identifiers.
Filesystem discovery, ownership validation, symlink checks, and deletion are
adapter responsibilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable


class ReplayRetentionStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ReplayArtifactKind(str, Enum):
    RAW_COMBINATION_SOURCE = "raw_combination_source"
    INTERMEDIATE_SPOOL = "intermediate_spool"
    SEALED_ARTIFACT = "sealed_artifact"


@dataclass(frozen=True, slots=True)
class ReplayRetentionPolicy:
    """Limits for derived replay files; job state is outside this policy."""

    keep_completed_sets: int = 2
    raw_ttl: timedelta = timedelta(days=7)
    orphan_ttl: timedelta = timedelta(days=7)
    hard_byte_budget: int = 16 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.keep_completed_sets < 1:
            raise ValueError("keep_completed_sets must be positive")
        if self.raw_ttl <= timedelta(0):
            raise ValueError("raw_ttl must be positive")
        if self.orphan_ttl <= timedelta(0):
            raise ValueError("orphan_ttl must be positive")
        if self.hard_byte_budget <= 0:
            raise ValueError("hard_byte_budget must be positive")


@dataclass(frozen=True, slots=True)
class ReplayRetentionJob:
    job_id: str
    status: ReplayRetentionStatus
    run_fingerprint: str
    created_at: datetime
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        if (
            not self.job_id.startswith("job-")
            or len(self.job_id) != 36
            or not _is_lower_hex(self.job_id.removeprefix("job-"))
        ):
            raise ValueError("invalid replay retention job id")
        if not _is_sha256(self.run_fingerprint):
            raise ValueError("invalid replay run fingerprint")
        _aware(self.created_at, "created_at")
        if self.status in {
            ReplayRetentionStatus.COMPLETED,
            ReplayRetentionStatus.FAILED,
        }:
            if self.finished_at is None:
                raise ValueError("terminal replay job must have finished_at")
            _aware(self.finished_at, "finished_at")
        elif self.finished_at is not None:
            raise ValueError("active replay job cannot have finished_at")


@dataclass(frozen=True, slots=True)
class ReplayRetentionArtifact:
    """An adapter-validated deletion unit."""

    artifact_id: str
    kind: ReplayArtifactKind
    owner_run_fingerprint: str
    size_bytes: int
    modified_at: datetime
    referenced_by_job_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.artifact_id:
            raise ValueError("artifact_id must not be empty")
        if not _is_sha256(self.owner_run_fingerprint):
            raise ValueError("invalid artifact owner fingerprint")
        if self.size_bytes < 0:
            raise ValueError("artifact size cannot be negative")
        _aware(self.modified_at, "modified_at")


@dataclass(frozen=True, slots=True)
class ReplayRetentionPlan:
    delete_artifact_ids: tuple[str, ...]
    protected_artifact_ids: tuple[str, ...]
    projected_bytes: int
    budget_satisfied: bool


def plan_replay_artifact_retention(
    *,
    jobs: Iterable[ReplayRetentionJob],
    artifacts: Iterable[ReplayRetentionArtifact],
    now: datetime,
    policy: ReplayRetentionPolicy = ReplayRetentionPolicy(),
) -> ReplayRetentionPlan:
    """Select validated units without ever selecting active-job artifacts.

    Raw and intermediate data belonging to a terminal job can be discarded
    immediately after the aggregate result has been sealed.  The TTL is a
    maximum lifetime for raw data, not a minimum retention promise.
    """

    _aware(now, "now")
    job_rows = tuple(jobs)
    artifact_rows = tuple(artifacts)
    by_fingerprint = {item.run_fingerprint: item for item in job_rows}
    if len(by_fingerprint) != len(job_rows):
        raise ValueError("one replay run fingerprint must have one owner")

    active = {
        item.run_fingerprint
        for item in job_rows
        if item.status
        in {ReplayRetentionStatus.QUEUED, ReplayRetentionStatus.RUNNING}
    }
    completed = sorted(
        (
            item
            for item in job_rows
            if item.status is ReplayRetentionStatus.COMPLETED
        ),
        key=lambda item: (
            _aware(item.finished_at, "finished_at"),
            item.job_id,
        ),
        reverse=True,
    )
    retained_completed_jobs = {
        item.job_id for item in completed[: policy.keep_completed_sets]
    }

    protected: set[str] = set()
    mandatory_candidates: list[ReplayRetentionArtifact] = []
    budget_candidates: list[ReplayRetentionArtifact] = []
    for artifact in artifact_rows:
        owner = by_fingerprint.get(artifact.owner_run_fingerprint)
        if owner is None:
            raise ValueError("artifact ownership is unknown")
        if artifact.owner_run_fingerprint in active:
            protected.add(artifact.artifact_id)
            continue
        if artifact.kind in {
            ReplayArtifactKind.RAW_COMBINATION_SOURCE,
            ReplayArtifactKind.INTERMEDIATE_SPOOL,
        }:
            if owner.status in {
                ReplayRetentionStatus.COMPLETED,
                ReplayRetentionStatus.FAILED,
            }:
                # Terminal raw rows are no longer required for recovery and
                # are removed immediately; this is stricter than the maximum
                # raw TTL and is necessary to honour the hard disk budget.
                mandatory_candidates.append(artifact)
            else:
                protected.add(artifact.artifact_id)
            continue
        if active:
            # A running use case may reuse any content-addressed sealed
            # artifact before it has produced its own result envelope.  Its
            # exact references are therefore unknowable until completion.
            protected.add(artifact.artifact_id)
            continue
        if any(
            job_id in retained_completed_jobs
            for job_id in artifact.referenced_by_job_ids
        ):
            budget_candidates.append(artifact)
        else:
            mandatory_candidates.append(artifact)

    ordered_mandatory = sorted(
        mandatory_candidates,
        key=lambda item: (
            _aware(item.modified_at, "modified_at"),
            item.artifact_id,
        ),
    )
    delete_ids = {item.artifact_id for item in ordered_mandatory}
    projected = sum(
        item.size_bytes
        for item in artifact_rows
        if item.artifact_id not in delete_ids
    )
    ordered_budget = sorted(
        budget_candidates,
        key=lambda item: (
            _aware(item.modified_at, "modified_at"),
            item.artifact_id,
        ),
    )
    budget_deletions: list[ReplayRetentionArtifact] = []
    for artifact in ordered_budget:
        if projected <= policy.hard_byte_budget:
            protected.add(artifact.artifact_id)
            continue
        budget_deletions.append(artifact)
        delete_ids.add(artifact.artifact_id)
        projected -= artifact.size_bytes

    return ReplayRetentionPlan(
        delete_artifact_ids=tuple(
            item.artifact_id for item in (*ordered_mandatory, *budget_deletions)
        ),
        protected_artifact_ids=tuple(sorted(protected)),
        projected_bytes=projected,
        budget_satisfied=projected <= policy.hard_byte_budget,
    )


def _aware(value: datetime | None, field: str) -> datetime:
    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _is_sha256(value: str) -> bool:
    return (
        value.startswith("sha256:")
        and len(value) == 71
        and _is_lower_hex(value.removeprefix("sha256:"))
    )


def _is_lower_hex(value: str) -> bool:
    return bool(value) and all(character in "0123456789abcdef" for character in value)
