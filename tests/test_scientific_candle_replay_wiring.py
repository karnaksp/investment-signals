from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from tinvest_signal_engine.services.hypothesis_replay_api import (
    LocalHypothesisPortfolioRunner,
    StartReplayRequest,
)
import tinvest_signal_engine.services.hypothesis_replay_api as replay_api


def test_internal_runner_wires_all_next_candle_hypotheses(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}
    report = SimpleNamespace(report_fingerprint="sha256:" + "a" * 64)

    class FakeScientificUseCase:
        def __init__(self, cache: object) -> None:
            captured["cache"] = cache

        def execute(self, request: object) -> object:
            captured["request"] = request
            return report

    class FakeScientificArtifacts:
        def save(
            self,
            actual_report: object,
            requested: tuple[object, ...],
            *,
            cost_model_version: str,
        ) -> object:
            captured["artifact_report"] = actual_report
            captured["artifact_requested"] = tuple(
                getattr(item, "value") for item in requested
            )
            captured["cost_model_version"] = cost_model_version
            return SimpleNamespace(
                artifact_uri=str(tmp_path / "immutable-artifact"),
                artifact_fingerprint="sha256:" + "c" * 64,
                evidence=tuple(
                    {"hypothesis_id": getattr(item, "value")} for item in requested
                ),
            )

    monkeypatch.setattr(
        replay_api,
        "BuildScientificCandleModelResearch",
        FakeScientificUseCase,
    )
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        scientific_artifacts=FakeScientificArtifacts(),  # type: ignore[arg-type]
    )
    request = StartReplayRequest(
        hypothesis_ids=("H7V2", "H15", "H10", "H11"),
        cost_model={
            "version": "cost-v2",
            "commission_bps": 1.0,
            "slippage_bps": 2.0,
            "entry_half_spread_bps": 3.0,
            "exit_half_spread_bps": 4.0,
        },
    )

    result = runner.execute(request, run_fingerprint="sha256:" + "d" * 64)

    scientific_request = captured["request"]
    assert tuple(item.value for item in scientific_request.selected_hypotheses) == (
        "H10",
        "H11",
        "H15",
        "H7V2",
    )
    assert scientific_request.policy.round_trip_cost_bps == 10.0
    assert captured["artifact_report"] is report
    assert captured["artifact_requested"] == ("H10", "H11", "H15", "H7V2")
    assert captured["cost_model_version"] == "cost-v2"
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == (
        "H10",
        "H11",
        "H15",
        "H7V2",
    )
    assert result["engines"] == (
        {
            "engine": "next_scientific_candle_replay",
            "hypothesis_ids": ("H10", "H11", "H15", "H7V2"),
            "application_run_id": "sha256:" + "a" * 64,
            "artifact_fingerprint": "sha256:" + "c" * 64,
            "artifact_uri": str(tmp_path / "immutable-artifact"),
            "resumed": False,
        },
    )


def test_internal_request_keeps_legacy_default_but_accepts_next_ids() -> None:
    assert StartReplayRequest().hypothesis_ids == tuple(
        item.short_id for item in replay_api.SCIENTIFIC_REPLAY_CONTRACT_V1
    )
    assert StartReplayRequest(
        hypothesis_ids=("H10", "H11", "H15", "H7V2")
    ).hypothesis_ids == ("H10", "H11", "H15", "H7V2")
