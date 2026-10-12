from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json

from tinvest_signal_engine.adapters.live_shadow_health_file import (
    AtomicJsonLiveShadowHealthStore,
)
from tinvest_signal_engine.application.live_shadow_health import (
    LiveShadowHealthTracker,
)
from tinvest_signal_engine.services.live_shadow_healthcheck import (
    validate_live_shadow_health,
)


def test_live_shadow_tracker_persists_success_and_failure(tmp_path) -> None:
    now = datetime(2026, 8, 5, 7, 0, tzinfo=UTC)
    path = tmp_path / "live-shadow.json"
    tracker = LiveShadowHealthTracker(
        store=AtomicJsonLiveShadowHealthStore(path),
        clock=lambda: now,
        stale_after_seconds=180,
    )

    tracker.succeeded(
        observations_processed=11,
        outcomes_processed=7,
        outcomes_unavailable=2,
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "active"
    assert payload["last_success_at"] == "2026-08-05T07:00:00Z"
    assert payload["observations_processed"] == 11
    assert payload["outcomes_processed"] == 7
    assert payload["outcomes_unavailable"] == 2

    now += timedelta(seconds=30)
    tracker.heartbeat()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["last_success_at"] == "2026-08-05T07:00:30Z"
    assert payload["observations_processed"] == 11
    assert payload["outcomes_processed"] == 7
    assert payload["outcomes_unavailable"] == 2

    now += timedelta(seconds=30)
    tracker.failed()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["state"] == "degraded"
    assert payload["reason_code"] == "live_shadow_pass_failed"
    assert payload["consecutive_failures"] == 1
    assert payload["last_error_at"] == "2026-08-05T07:01:00Z"


def test_live_shadow_container_health_requires_fresh_success(tmp_path) -> None:
    now = datetime(2026, 8, 5, 7, 0, tzinfo=UTC)
    path = tmp_path / "live-shadow.json"
    tracker = LiveShadowHealthTracker(
        store=AtomicJsonLiveShadowHealthStore(path),
        clock=lambda: now,
        stale_after_seconds=180,
    )

    assert validate_live_shadow_health(path, now=now) == (
        "live_shadow_latest_pass_failed"
    )
    tracker.succeeded(
        observations_processed=0,
        outcomes_processed=0,
        outcomes_unavailable=0,
    )
    assert validate_live_shadow_health(path, now=now) is None
    assert validate_live_shadow_health(
        path,
        now=now + timedelta(seconds=181),
    ) == "live_shadow_health_stale"

    tracker.failed()
    assert validate_live_shadow_health(path, now=now) == (
        "live_shadow_latest_pass_failed"
    )


def test_live_shadow_container_health_fails_closed_for_unreadable_snapshot(
    tmp_path,
) -> None:
    path = tmp_path / "live-shadow.json"
    path.write_text("not-json", encoding="utf-8")

    assert validate_live_shadow_health(path) == "live_shadow_health_unreadable"
