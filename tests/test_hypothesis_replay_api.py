"""Internal HTTP boundary for recoverable local hypothesis replay."""

from __future__ import annotations

from pathlib import Path
import sys
from threading import Event
import time
from types import SimpleNamespace
from typing import Any, Mapping

from fastapi.testclient import TestClient

from tinvest_signal_engine.services.hypothesis_replay_api import (
    LocalHypothesisPortfolioRunner,
    LocalReplayJobStore,
    ReplayJobManager,
    StartReplayRequest,
    create_app,
)
from tinvest_signal_engine.domain.scientific_replay_contract import (
    scientific_replay_definition,
)
import tinvest_signal_engine.services.hypothesis_replay_api as replay_api


class FakeReplayRunner:
    def __init__(self, *, blocker: Event | None = None, failure: str | None = None) -> None:
        self.blocker = blocker
        self.failure = failure
        self.calls: list[tuple[StartReplayRequest, str]] = []
        self.fingerprint = "sha256:" + "a" * 64

    def dataset_fingerprint(self) -> str:
        return self.fingerprint

    def readiness(self) -> tuple[bool, str | None]:
        return True, None

    def execute(
        self,
        request: StartReplayRequest,
        *,
        run_fingerprint: str,
    ) -> Mapping[str, Any]:
        self.calls.append((request, run_fingerprint))
        if self.blocker is not None:
            self.blocker.wait(timeout=2)
        if self.failure is not None:
            raise RuntimeError(self.failure)
        return {
            "engines": ({
                "engine": "fake_application_use_case",
                "hypothesis_ids": request.hypothesis_ids,
                "artifact_uri": "/local/immutable/artifacts/run",
                "resumed": False,
            },),
            "evidence": tuple(_fake_evidence(item) for item in request.hypothesis_ids),
        }


def _fake_evidence(hypothesis_id: str) -> Mapping[str, Any]:
    fingerprint = "sha256:" + "b" * 64
    definition = scientific_replay_definition(hypothesis_id)
    return {
        "hypothesis_id": hypothesis_id,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": definition.market_phase,
        "source_data_state": "insufficient_history",
        "decision": "blocked_by_data",
        "independent_validation": True,
        "cost_adjusted": True,
        "sample_count": 0,
        "trading_days": 0,
        "generated_at": "2026-07-19T10:00:00+00:00",
        "artifact_fingerprint": fingerprint,
        "dataset_fingerprint": "sha256:" + "a" * 64,
        "formula_fingerprint": fingerprint,
        "cost_model_version": "fixture-cost-v1",
        "primary_metric_value": None,
        "matched_control_lift_ci95_lower": None,
        "matched_control_lift_ci95_upper": None,
        "matched_controls": 0,
        "controls_per_event": 5,
        "adjusted_p_value": None,
        "stable_blocks": 0,
        "total_blocks": 0,
        "maximum_ticker_share": None,
        "maximum_period_share": None,
        "abstention_rate": None,
        "horizons": tuple({
            "horizon_seconds": horizon,
            "evidence_scope": "not_evaluated",
            "source_data_state": "insufficient_history",
            "decision": "blocked_by_data",
            "sample_count": 0,
            "primary_metric_value": None,
        } for horizon in definition.horizons_seconds),
    }


def _client(
    tmp_path: Path,
    runner: FakeReplayRunner,
) -> tuple[TestClient, LocalReplayJobStore]:
    store = LocalReplayJobStore(tmp_path / "jobs")
    manager = ReplayJobManager(runner=runner, store=store)
    return TestClient(create_app(manager=manager, close_manager=True)), store


def _wait_for_status(client: TestClient, job_id: str, expected: str) -> dict[str, Any]:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        payload = client.get(f"/internal/v1/hypothesis-replays/{job_id}").json()
        if payload["status"] == expected:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach {expected}")


def test_health_readiness_and_completed_result(tmp_path: Path) -> None:
    runner = FakeReplayRunner()
    client, _ = _client(tmp_path, runner)

    with client:
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/ready").json() == {"status": "ready"}

        accepted = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "first-local-replay"},
            json={"hypothesis_ids": ["H7", "H1", "H4", "H3"]},
        )
        assert accepted.status_code == 202
        submission = accepted.json()
        assert submission["run_fingerprint"].startswith("sha256:")
        assert submission["idempotency_key_hash"].startswith("sha256:")
        assert submission["reused"] is False

        final = _wait_for_status(client, submission["job_id"], "completed")
        assert final["hypothesis_ids"] == ["H1", "H3", "H4", "H7"]
        result = client.get(submission["result_url"])
        assert result.status_code == 200
        assert result.json()["network_download_performed"] is False
        assert result.json()["engines"][0]["engine"] == "fake_application_use_case"
        assert [item["hypothesis_id"] for item in result.json()["evidence"]] == [
            "H1",
            "H3",
            "H4",
            "H7",
        ]

    assert len(runner.calls) == 1


def test_pending_result_returns_202_and_same_key_reuses_job(tmp_path: Path) -> None:
    blocker = Event()
    runner = FakeReplayRunner(blocker=blocker)
    client, _ = _client(tmp_path, runner)

    with client:
        first = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "stable-key"},
            json={"hypothesis_ids": ["H1"]},
        ).json()
        pending = client.get(first["result_url"])
        assert pending.status_code == 202
        assert pending.json()["status"] in {"queued", "running"}

        second_response = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "stable-key"},
            json={"hypothesis_ids": ["H1"]},
        )
        assert second_response.status_code == 202
        second = second_response.json()
        assert second["job_id"] == first["job_id"]
        assert second["run_fingerprint"] == first["run_fingerprint"]
        assert second["reused"] is True
        blocker.set()
        _wait_for_status(client, first["job_id"], "completed")

    assert len(runner.calls) == 1


def test_same_idempotency_key_rejects_different_input(tmp_path: Path) -> None:
    runner = FakeReplayRunner()
    client, _ = _client(tmp_path, runner)

    with client:
        assert client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "collision"},
            json={"hypothesis_ids": ["H1"]},
        ).status_code == 202
        conflict = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "collision"},
            json={"hypothesis_ids": ["H2"]},
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"]["code"] == "idempotency_key_conflict"


def test_same_key_and_request_reuses_original_job_after_cache_changes(
    tmp_path: Path,
) -> None:
    runner = FakeReplayRunner()
    client, store = _client(tmp_path, runner)

    with client:
        first = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "stable-across-cache-change"},
            json={"hypothesis_ids": ["H1"]},
        ).json()
        _wait_for_status(client, first["job_id"], "completed")
        runner.fingerprint = "sha256:" + "f" * 64
        repeated = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "stable-across-cache-change"},
            json={"hypothesis_ids": ["H1"]},
        )

    assert repeated.status_code == 202
    assert repeated.json()["reused"] is True
    assert repeated.json()["job_id"] == first["job_id"]
    assert repeated.json()["run_fingerprint"] == first["run_fingerprint"]
    original = store.load(first["job_id"])
    assert original is not None
    assert original["dataset_fingerprint"] == "sha256:" + "a" * 64
    assert len(runner.calls) == 1


def test_transport_rejects_token_and_unknown_fields(tmp_path: Path) -> None:
    client, _ = _client(tmp_path, FakeReplayRunner())

    with client:
        rejected = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "must-not-accept-token"},
            json={"hypothesis_ids": ["H1"], "tinvest_token": "secret"},
        )
        assert rejected.status_code == 422

        unpaired_jump = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "unpaired-jump"},
            json={"hypothesis_ids": ["H3"]},
        )
        assert unpaired_jump.status_code == 422

    assert StartReplayRequest().cost_model.version == "research-cost-v1.0.0"
    assert StartReplayRequest().cost_model.round_trip_bps == 10.0


def test_orderbook_catalog_hypotheses_are_explicitly_blocked_without_live_data(
    tmp_path: Path,
) -> None:
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
    )
    runner._descriptor_cache = SimpleNamespace(  # type: ignore[assignment]
        describe=lambda: SimpleNamespace(
            dataset_fingerprint="sha256:" + "a" * 64,
        )
    )

    result = runner.execute(
        StartReplayRequest(hypothesis_ids=("H8", "H9")),
        run_fingerprint="sha256:" + "c" * 64,
    )

    assert result["engines"] == ({
        "engine": "live_orderbook_replay",
        "hypothesis_ids": ("H8", "H9"),
        "availability": "requires_live_orderbook",
        "resumed": False,
    },)
    evidence = result["evidence"]
    assert [item["hypothesis_id"] for item in evidence] == ["H8", "H9"]
    assert all(item["decision"] == "blocked_by_data" for item in evidence)
    assert all(
        item["source_data_state"] == "requires_live_orderbook"
        for item in evidence
    )
    assert [
        row["horizon_seconds"] for row in evidence[1]["horizons"]
    ] == [1, 5, 60, 300, 900]
    assert all(
        row["evidence_scope"] == "not_evaluated"
        for item in evidence for row in item["horizons"]
    )


def test_failure_is_persisted_without_traceback(tmp_path: Path) -> None:
    client, store = _client(tmp_path, FakeReplayRunner(failure="sealed failure"))

    with client:
        submitted = client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "failed-run"},
            json={"hypothesis_ids": ["H3", "H4"]},
        ).json()
        final = _wait_for_status(client, submitted["job_id"], "failed")
        assert final["error"] == {
            "code": "replay_execution_failed",
            "message": "sealed failure",
        }
        result = client.get(submitted["result_url"])
        assert result.status_code == 409

    state_text = (store.root / submitted["job_id"] / "state.json").read_text()
    assert "Traceback" not in state_text
    assert "failed-run" not in state_text


def test_queued_job_is_recovered_from_disk_after_restart(tmp_path: Path) -> None:
    store = LocalReplayJobStore(tmp_path / "jobs")
    first_runner = FakeReplayRunner(blocker=Event())
    first_manager = ReplayJobManager(runner=first_runner, store=store)
    submission = first_manager.submit(
        StartReplayRequest(hypothesis_ids=("H5",)),
        "restartable",
    )
    job_id = str(submission.record["job_id"])
    record = store.load(job_id)
    assert record is not None
    record["status"] = "running"
    record["attempt"] = max(1, int(record["attempt"]))
    store.save(record)
    # Simulate a killed process: its persisted running envelope is the source of truth.
    first_runner.blocker.set()
    first_manager.close()
    record = store.load(job_id)
    assert record is not None
    record["status"] = "running"
    store.save(record)

    recovered_runner = FakeReplayRunner()
    recovered_manager = ReplayJobManager(runner=recovered_runner, store=store)
    client = TestClient(create_app(manager=recovered_manager, close_manager=True))
    with client:
        final = _wait_for_status(client, job_id, "completed")
        assert final["recovered_after_restart"] is True
        assert final["attempt"] >= 2
        assert client.get(
            f"/internal/v1/hypothesis-replays/{job_id}/result"
        ).status_code == 200

    assert len(recovered_runner.calls) == 1


def test_completed_result_is_available_after_restart(tmp_path: Path) -> None:
    store = LocalReplayJobStore(tmp_path / "jobs")
    first_manager = ReplayJobManager(runner=FakeReplayRunner(), store=store)
    first_client = TestClient(create_app(manager=first_manager, close_manager=True))
    with first_client:
        submission = first_client.post(
            "/internal/v1/hypothesis-replays",
            headers={"Idempotency-Key": "completed-before-restart"},
            json={"hypothesis_ids": ["H1", "H2"]},
        ).json()
        _wait_for_status(first_client, submission["job_id"], "completed")

    second_manager = ReplayJobManager(runner=FakeReplayRunner(), store=store)
    second_client = TestClient(create_app(manager=second_manager, close_manager=True))
    with second_client:
        status_response = second_client.get(submission["status_url"])
        result_response = second_client.get(submission["result_url"])
        assert status_response.json()["status"] == "completed"
        assert result_response.status_code == 200
        assert result_response.json()["job_id"] == submission["job_id"]


def test_single_general_request_executes_fixed_portfolio_but_returns_requested_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}

    class FakeGeneralUseCase:
        def __init__(self, **_: Any) -> None:
            pass

        def execute(self, request: Any) -> Any:
            captured["selected"] = tuple(
                item.value for item in request.selected_hypotheses
            )
            return SimpleNamespace(completion=SimpleNamespace(
                run_id="sha256:" + "c" * 64,
                artifact_fingerprint="sha256:" + "d" * 64,
                resumed=False,
            ))

    class FakeEvidenceReader:
        def read_general(
            self,
            _: object,
            requested: tuple[str, ...],
            *,
            generated_at: str,
        ) -> tuple[Mapping[str, Any], ...]:
            captured["evidence_requested"] = requested
            row = dict(_fake_evidence("H1"))
            row["generated_at"] = generated_at
            return (row,)

    monkeypatch.setattr(replay_api, "RunHistoricalHypothesisReplay", FakeGeneralUseCase)
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "unused-cache",
        artifact_root=tmp_path / "artifacts",
        evidence_reader=FakeEvidenceReader(),
    )

    result = runner.execute(
        StartReplayRequest(hypothesis_ids=("H1",)),
        run_fingerprint="sha256:" + "e" * 64,
    )

    assert captured["selected"] == ("H1", "H2", "H5", "H6", "H7")
    assert captured["evidence_requested"] == ("H1",)
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == ("H1",)
    engine = result["engines"][0]
    assert engine["requested_hypothesis_ids"] == ("H1",)
    assert engine["executed_hypothesis_ids"] == ("H1", "H2", "H5", "H6", "H7")


def test_cli_host_defaults_to_loopback_and_allows_explicit_internal_bind(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    calls: list[dict[str, Any]] = []
    fake_uvicorn = SimpleNamespace(
        run=lambda app, **kwargs: calls.append({"app": app, **kwargs})
    )
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)
    monkeypatch.setattr(replay_api, "build_app", lambda **_: "app")

    assert replay_api.main([
        "--cache-dir", str(tmp_path / "cache"),
        "--state-dir", str(tmp_path / "state"),
        "--artifact-dir", str(tmp_path / "artifacts"),
    ]) == 0
    assert calls[-1]["host"] == "127.0.0.1"

    assert replay_api.main([
        "--host", "0.0.0.0",
        "--cache-dir", str(tmp_path / "cache"),
        "--state-dir", str(tmp_path / "state"),
        "--artifact-dir", str(tmp_path / "artifacts"),
    ]) == 0
    assert calls[-1]["host"] == "0.0.0.0"


def test_cli_rejects_non_internal_host(tmp_path: Path) -> None:
    try:
        replay_api.main([
            "--host", "192.0.2.1",
            "--cache-dir", str(tmp_path / "cache"),
        ])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("unsupported bind host must be rejected")
