from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path

from tinvest_signal_engine.adapters.replay_artifact_retention import (
    LocalReplayArtifactRetention,
)
from tinvest_signal_engine.application.replay_artifact_retention import (
    ReplayArtifactKind,
    ReplayRetentionArtifact,
    ReplayRetentionJob,
    ReplayRetentionPolicy,
    ReplayRetentionStatus,
    plan_replay_artifact_retention,
)


NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)


def test_active_artifacts_are_preserved_while_terminal_raw_is_removed(
    tmp_path: Path,
) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    active = _job(1, ReplayRetentionStatus.RUNNING)
    completed = _job(2, ReplayRetentionStatus.COMPLETED)
    _write_job(state_root, active)
    _write_job(state_root, completed, engines=[])
    active_raw = _raw(artifact_root, active, "scientific-combinations/source")
    completed_raw = _raw(artifact_root, completed, "scientific-combinations/source")

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=False
    )

    assert active_raw.is_dir()
    assert not completed_raw.exists()
    assert result.skipped_reason is None
    assert completed.job_id in (state_root / completed.job_id / "state.json").read_text()
    assert (state_root / completed.job_id / "result.json").is_file()


def test_malformed_state_preserves_owned_artifacts_but_stale_working_is_always_cleaned(
    tmp_path: Path,
) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    malformed = state_root / ("job-" + "f" * 32)
    malformed.mkdir(parents=True)
    (malformed / "state.json").write_text("{", encoding="utf-8")
    raw = artifact_root / "scientific-combinations/source" / ("a" * 64)
    _payload(raw)
    working = artifact_root / ".replay-working" / "candle-partitions-stale"
    _payload(working)

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=True
    )

    assert raw.is_dir()
    assert not working.exists()
    assert result.skipped_reason == "unsafe_retention_state:JSONDecodeError"


def test_success_and_failure_cleanup_is_idempotent(tmp_path: Path) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    completed = _job(1, ReplayRetentionStatus.COMPLETED)
    failed = _job(2, ReplayRetentionStatus.FAILED)
    _write_job(state_root, completed, engines=[])
    _write_job(state_root, failed)
    completed_raw = _raw(
        artifact_root, completed, "scientific-combinations/source"
    )
    failed_spool = _raw(artifact_root, failed, "prospective-spool")

    collector = _collector(state_root, artifact_root)
    first = collector.collect(safe_to_remove_working=True)
    second = collector.collect(safe_to_remove_working=True)

    assert not completed_raw.exists()
    assert not failed_spool.exists()
    assert len(first.deleted_paths) == 2
    assert second.deleted_paths == ()
    assert (state_root / completed.job_id / "state.json").is_file()
    assert (state_root / failed.job_id / "state.json").is_file()


def test_only_two_newest_completed_sealed_sets_are_kept(tmp_path: Path) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    jobs = tuple(_job(index, ReplayRetentionStatus.COMPLETED) for index in range(3))
    sealed = []
    for index, job in enumerate(jobs):
        path = artifact_root / "sealed" / f"set-{index}"
        _payload(path, size=32)
        sealed.append(path)
        _write_job(
            state_root,
            job,
            engines=[{"engine": "fixture", "artifact_uri": str(path.resolve())}],
        )

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=False
    )

    assert not sealed[0].exists()
    assert sealed[1].is_dir()
    assert sealed[2].is_dir()
    assert result.deleted_paths == ("sealed/set-0",)


def test_policy_is_oldest_first_and_reports_hard_budget() -> None:
    jobs = tuple(_job(index, ReplayRetentionStatus.COMPLETED) for index in range(3))
    artifacts = tuple(
        ReplayRetentionArtifact(
            artifact_id=f"sealed/{index}",
            kind=ReplayArtifactKind.SEALED_ARTIFACT,
            owner_run_fingerprint=job.run_fingerprint,
            size_bytes=8,
            modified_at=NOW + timedelta(minutes=index),
            referenced_by_job_ids=(job.job_id,),
        )
        for index, job in enumerate(jobs)
    )

    plan = plan_replay_artifact_retention(
        jobs=jobs,
        artifacts=artifacts,
        now=NOW + timedelta(days=1),
        policy=ReplayRetentionPolicy(
            keep_completed_sets=2,
            hard_byte_budget=16,
        ),
    )

    assert plan.delete_artifact_ids == ("sealed/0",)
    assert plan.projected_bytes == 16
    assert plan.budget_satisfied is True

    constrained = plan_replay_artifact_retention(
        jobs=jobs,
        artifacts=artifacts,
        now=NOW + timedelta(days=1),
        policy=ReplayRetentionPolicy(
            keep_completed_sets=2,
            hard_byte_budget=8,
        ),
    )
    assert constrained.delete_artifact_ids == ("sealed/0", "sealed/1")
    assert constrained.protected_artifact_ids == ("sealed/2",)
    assert constrained.projected_bytes == 8
    assert constrained.budget_satisfied is True


def test_active_raw_remains_protected_after_ttl() -> None:
    active = _job(1, ReplayRetentionStatus.RUNNING)
    artifact = ReplayRetentionArtifact(
        artifact_id="source/active",
        kind=ReplayArtifactKind.RAW_COMBINATION_SOURCE,
        owner_run_fingerprint=active.run_fingerprint,
        size_bytes=10,
        modified_at=NOW - timedelta(days=30),
    )

    plan = plan_replay_artifact_retention(
        jobs=(active,),
        artifacts=(artifact,),
        now=NOW,
        policy=ReplayRetentionPolicy(raw_ttl=timedelta(days=7)),
    )

    assert plan.delete_artifact_ids == ()
    assert plan.protected_artifact_ids == ("source/active",)


def test_any_active_job_protects_content_addressed_sealed_artifacts() -> None:
    active = _job(1, ReplayRetentionStatus.RUNNING)
    old_completed = _job(2, ReplayRetentionStatus.COMPLETED)
    old = ReplayRetentionArtifact(
        artifact_id="sealed/old",
        kind=ReplayArtifactKind.SEALED_ARTIFACT,
        owner_run_fingerprint=old_completed.run_fingerprint,
        size_bytes=10,
        modified_at=NOW - timedelta(days=30),
        referenced_by_job_ids=(old_completed.job_id,),
    )

    plan = plan_replay_artifact_retention(
        jobs=(active, old_completed),
        artifacts=(old,),
        now=NOW,
    )

    assert plan.delete_artifact_ids == ()
    assert plan.protected_artifact_ids == ("sealed/old",)


def test_escaping_result_uri_fails_closed(tmp_path: Path) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    completed = _job(1, ReplayRetentionStatus.COMPLETED)
    outside = tmp_path / "outside"
    _payload(outside)
    raw = _raw(artifact_root, completed, "scientific-combinations/source")
    _write_job(
        state_root,
        completed,
        engines=[{"engine": "fixture", "artifact_uri": str(outside.resolve())}],
    )

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=False
    )

    assert raw.is_dir()
    assert outside.is_dir()
    assert result.skipped_reason == "unsafe_retention_state:ValueError"


def test_symlink_inside_candidate_is_never_followed_or_removed(
    tmp_path: Path,
) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    completed = _job(1, ReplayRetentionStatus.COMPLETED)
    _write_job(state_root, completed, engines=[])
    outside = tmp_path / "outside.txt"
    outside.write_text("preserve", encoding="utf-8")
    raw = _raw(artifact_root, completed, "scientific-combinations/source")
    os.symlink(outside, raw / "unsafe-link")

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=False
    )

    assert raw.is_dir()
    assert outside.read_text(encoding="utf-8") == "preserve"
    assert result.skipped_reason == "unsafe_retention_state:ValueError"


def test_unknown_raw_ownership_is_preserved(tmp_path: Path) -> None:
    state_root, artifact_root = tmp_path / "jobs", tmp_path / "artifacts"
    completed = _job(1, ReplayRetentionStatus.COMPLETED)
    _write_job(state_root, completed, engines=[])
    unknown = artifact_root / "scientific-combinations/source" / ("f" * 64)
    _payload(unknown)

    result = _collector(state_root, artifact_root).collect(
        safe_to_remove_working=False
    )

    assert unknown.is_dir()
    assert result.deleted_paths == ()


def _collector(
    state_root: Path,
    artifact_root: Path,
) -> LocalReplayArtifactRetention:
    return LocalReplayArtifactRetention(
        state_root=state_root,
        artifact_root=artifact_root,
        clock=lambda: NOW,
        policy=ReplayRetentionPolicy(hard_byte_budget=1024 * 1024),
    )


def _job(index: int, status: ReplayRetentionStatus) -> ReplayRetentionJob:
    terminal = status in {
        ReplayRetentionStatus.COMPLETED,
        ReplayRetentionStatus.FAILED,
    }
    return ReplayRetentionJob(
        job_id="job-" + f"{index:032x}",
        status=status,
        run_fingerprint="sha256:" + f"{index + 1:064x}",
        created_at=NOW + timedelta(minutes=index),
        finished_at=(NOW + timedelta(minutes=index + 1) if terminal else None),
    )


def _write_job(
    root: Path,
    job: ReplayRetentionJob,
    *,
    engines: list[dict[str, str]] | None = None,
) -> None:
    directory = root / job.job_id
    directory.mkdir(parents=True, exist_ok=True)
    state = {
        "job_id": job.job_id,
        "status": job.status.value,
        "run_fingerprint": job.run_fingerprint,
        "created_at": job.created_at.isoformat(),
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }
    (directory / "state.json").write_text(
        json.dumps(state),
        encoding="utf-8",
    )
    if job.status is ReplayRetentionStatus.COMPLETED:
        result = {
            "job_id": job.job_id,
            "status": "completed",
            "run_fingerprint": job.run_fingerprint,
            "engines": engines or [],
        }
        (directory / "result.json").write_text(
            json.dumps(result),
            encoding="utf-8",
        )


def _raw(root: Path, job: ReplayRetentionJob, prefix: str) -> Path:
    path = root / prefix / job.run_fingerprint.removeprefix("sha256:")
    _payload(path)
    return path


def _payload(path: Path, *, size: int = 16) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "payload.bin").write_bytes(b"x" * size)
