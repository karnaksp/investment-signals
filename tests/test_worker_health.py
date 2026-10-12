from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from tinvest_signal_engine.adapters.worker_health_file import (
    WorkerHealthFileSink,
    read_worker_health_snapshot,
)
from tinvest_signal_engine.application.worker_health import WorkerHealthTracker
from tinvest_signal_engine.domain.worker_health import WorkerState


class _Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now


def test_worker_health_file_is_atomic_and_records_recovery(tmp_path: Path) -> None:
    clock = _Clock()
    path = tmp_path / "reference-ticks.json"
    tracker = WorkerHealthTracker(
        worker_id="reference_tick_writer",
        sink=WorkerHealthFileSink(path),
        stale_after_seconds=90,
        minimum_write_interval_seconds=15,
        clock=clock,
    )

    starting = read_worker_health_snapshot(path)
    assert starting.state is WorkerState.STARTING
    assert list(tmp_path.glob(".reference-ticks.json.*")) == []

    clock.now += timedelta(seconds=2)
    tracker.failed("clickhouse_unavailable")
    degraded = read_worker_health_snapshot(path)
    assert degraded.state is WorkerState.DEGRADED
    assert degraded.reason_code == "clickhouse_unavailable"
    assert degraded.consecutive_failures == 1

    clock.now += timedelta(seconds=2)
    tracker.succeeded(force=True)
    active = read_worker_health_snapshot(path)
    assert active.state is WorkerState.ACTIVE
    assert active.reason_code is None
    assert active.consecutive_failures == 0
    assert active.last_success_at == clock.now


def test_worker_health_heartbeat_throttles_idle_disk_writes(tmp_path: Path) -> None:
    clock = _Clock()
    path = tmp_path / "worker.json"
    tracker = WorkerHealthTracker(
        worker_id="worker",
        sink=WorkerHealthFileSink(path),
        minimum_write_interval_seconds=15,
        clock=clock,
    )
    first = path.read_text(encoding="utf-8")

    clock.now += timedelta(seconds=14)
    tracker.heartbeat()
    assert path.read_text(encoding="utf-8") == first

    clock.now += timedelta(seconds=1)
    tracker.heartbeat()
    assert path.read_text(encoding="utf-8") != first
