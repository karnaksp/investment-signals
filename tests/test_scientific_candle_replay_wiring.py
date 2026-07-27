from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from tinvest_signal_engine.services.hypothesis_replay_api import (
    DerivedReplayEvidenceResponse,
    LocalHypothesisPortfolioRunner,
    StartReplayRequest,
)
from tinvest_signal_engine.application.derived_combination_evidence import (
    CombinationEvidenceArtifactSnapshot,
    CombinationHorizonArtifactSnapshot,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    preregistered_combination_definition,
)
import tinvest_signal_engine.services.hypothesis_replay_api as replay_api


def test_internal_runner_closes_materialized_cache_after_portfolio(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, object]] = []

    class FakeMaterializedCache:
        def describe(self) -> CandleCacheDescriptor:
            return CandleCacheDescriptor(
                dataset_fingerprint="sha256:" + "b" * 64,
                partition_count=1,
                tickers=("SBER",),
                start_day=date(2026, 1, 1),
                end_day=date(2026, 1, 1),
            )

        def load(self) -> tuple[object, ...]:
            raise AssertionError("order-book-only replay must not load candles")

        def materialize_ticker_partitions(self, root: Path) -> None:
            calls.append(("materialize", root))

        def close_materialized_partitions(self) -> None:
            calls.append(("close", None))

    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
    )
    runner._descriptor_cache = FakeMaterializedCache()  # type: ignore[assignment]

    result = runner.execute(
        StartReplayRequest(hypothesis_ids=("H8",)),
        run_fingerprint="sha256:" + "d" * 64,
    )

    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == ("H8",)
    assert calls == [
        ("materialize", tmp_path / "artifacts" / ".replay-working"),
        ("close", None),
    ]


def test_internal_runner_wires_non_r2_next_candle_hypotheses(
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
        hypothesis_ids=("H7V2", "H15"),
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
        "H15",
        "H7V2",
    )
    assert scientific_request.policy.round_trip_cost_bps == 10.0
    assert captured["artifact_report"] is report
    assert captured["artifact_requested"] == ("H15", "H7V2")
    assert captured["cost_model_version"] == "cost-v2"
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == (
        "H15",
        "H7V2",
    )
    assert result["engines"] == (
        {
            "engine": "next_scientific_candle_replay",
            "hypothesis_ids": ("H15", "H7V2"),
            "application_run_id": "sha256:" + "a" * 64,
            "artifact_fingerprint": "sha256:" + "c" * 64,
            "artifact_uri": str(tmp_path / "immutable-artifact"),
            "resumed": False,
        },
    )


def test_internal_runner_routes_h10_h11_only_to_causal_r2(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}
    report = SimpleNamespace(report_fingerprint="sha256:" + "a" * 64)

    class FakeR2UseCase:
        def __init__(self, cache: object) -> None:
            captured["cache"] = cache

        def execute(self, request: object) -> object:
            captured["request"] = request
            return report

    class FakeR2Artifacts:
        def save(
            self,
            actual_report: object,
            requested: tuple[object, ...],
            *,
            cost_model_version: str,
            blocking_reason_codes: tuple[str, ...],
        ) -> object:
            captured["artifact_report"] = actual_report
            captured["artifact_requested"] = tuple(
                getattr(item, "value") for item in requested
            )
            captured["cost_model_version"] = cost_model_version
            captured["blocking_reason_codes"] = blocking_reason_codes
            return SimpleNamespace(
                artifact_uri=str(tmp_path / "immutable-r2-artifact"),
                artifact_fingerprint="sha256:" + "c" * 64,
                evidence=tuple(
                    {"hypothesis_id": getattr(item, "value")} for item in requested
                ),
            )

    class ForbiddenLegacyUseCase:
        def __init__(self, _cache: object) -> None:
            raise AssertionError("H10/H11 must not use the legacy candle engine")

    monkeypatch.setattr(replay_api, "BuildR2ExtensionReplay", FakeR2UseCase)
    monkeypatch.setattr(
        replay_api,
        "BuildScientificCandleModelResearch",
        ForbiddenLegacyUseCase,
    )
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        r2_artifacts=FakeR2Artifacts(),  # type: ignore[arg-type]
    )

    result = runner.execute(
        StartReplayRequest(hypothesis_ids=("H10", "H11")),
        run_fingerprint="sha256:" + "d" * 64,
    )

    request = captured["request"]
    assert tuple(item.value for item in request.selected_hypotheses) == (
        "H10",
        "H11",
    )
    assert request.policy.round_trip_cost_bps == 10.0
    assert request.policy.cost_model_version == "research-cost-v1.0.0"
    assert request.target_universe == request.market_universe
    assert captured["artifact_report"] is report
    assert captured["artifact_requested"] == ("H10", "H11")
    assert captured["blocking_reason_codes"] == (
        "independent_evidence_gate_unavailable",
        "r2_reference_data_unavailable",
    )
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == (
        "H10",
        "H11",
    )
    assert result["engines"] == (
        {
            "engine": "causal_h10_h11_r2_replay",
            "hypothesis_ids": ("H10", "H11"),
            "application_run_id": "sha256:" + "a" * 64,
            "artifact_fingerprint": "sha256:" + "c" * 64,
            "artifact_uri": str(tmp_path / "immutable-r2-artifact"),
            "resumed": False,
            "evidence_state": "blocked_by_data",
        },
    )


def test_internal_request_keeps_legacy_default_but_accepts_next_ids() -> None:
    assert StartReplayRequest().hypothesis_ids == replay_api.LEGACY_DEFAULT_HYPOTHESES
    assert StartReplayRequest(
        hypothesis_ids=("H10", "H11", "H15", "H7V2")
    ).hypothesis_ids == ("H10", "H11", "H15", "H7V2")


def test_internal_runner_wires_prospective_portfolio_as_one_evidence_family(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}
    report = SimpleNamespace(
        report_fingerprint="sha256:" + "a" * 64,
        dataset_fingerprint="sha256:" + "b" * 64,
    )

    class FakeCache:
        def describe(self) -> object:
            return SimpleNamespace(dataset_fingerprint=report.dataset_fingerprint)

        def load(self) -> tuple[object, ...]:
            return (object(),)

    class FakeArtifacts:
        def save_portfolio(
            self,
            actual_reports: object,
            requested: tuple[object, ...],
            *,
            cost_model_version: str,
        ) -> object:
            captured["artifact_reports"] = tuple(actual_reports)
            captured["requested"] = tuple(item.value for item in requested)
            captured["cost_model_version"] = cost_model_version
            return SimpleNamespace(
                artifact_uri=str(tmp_path / "prospective-artifact"),
                artifact_fingerprint="sha256:" + "c" * 64,
                evidence=tuple({"hypothesis_id": item.value} for item in requested),
            )

    def fake_build(
        candles: object,
        *,
        dataset_fingerprint: str,
        request: object,
    ) -> object:
        captured["candles"] = candles
        captured["dataset_fingerprint"] = dataset_fingerprint
        captured.setdefault("requests", []).append(request)
        return report

    monkeypatch.setattr(
        replay_api,
        "build_prospective_scientific_research",
        fake_build,
    )
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        prospective_artifacts=FakeArtifacts(),  # type: ignore[arg-type]
    )
    runner._descriptor_cache = FakeCache()  # type: ignore[assignment]
    request = StartReplayRequest(
        hypothesis_ids=("H3V2", "H4V2", "H7V3", "H15V2", "H16", "H17"),
        cost_model={"version": "cost-v3"},
    )

    result = runner.execute(request, run_fingerprint="sha256:" + "d" * 64)

    requests = captured["requests"]
    assert (
        tuple(item.selected_hypotheses[0].value for item in requests)
        == request.hypothesis_ids
    )
    assert all(item.policy.round_trip_cost_bps == 10.0 for item in requests)
    assert len(captured["artifact_reports"]) == len(request.hypothesis_ids)
    assert captured["dataset_fingerprint"] == report.dataset_fingerprint
    assert captured["requested"] == request.hypothesis_ids
    assert captured["cost_model_version"] == "cost-v3"
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == (
        request.hypothesis_ids
    )
    assert result["engines"][0]["engine"] == "prospective_scientific_replay"


def test_internal_runner_streams_dense_non_jump_model_without_cache_load(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {"load_calls": 0, "row_factory_calls": 0}
    dataset_fingerprint = "sha256:" + "b" * 64
    derived_pair = (object(), object())

    class FakeCache:
        def describe(self) -> object:
            return SimpleNamespace(dataset_fingerprint=dataset_fingerprint)

        def iter_ticker_partitions(self):
            yield (object(),)

        def load(self) -> tuple[object, ...]:
            captured["load_calls"] += 1
            raise AssertionError("bounded replay must not materialize the cache")

    class FakeSpool:
        def __init__(self, root: object) -> None:
            captured["spool_root"] = root
            self.rows: list[object] = []
            self.latest_target_at = datetime(2026, 1, 5, tzinfo=timezone.utc)

        def __enter__(self):
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def stage_partition(self, rows: object) -> None:
            self.rows.extend(rows)  # type: ignore[arg-type]

        def iter_rows(self):
            captured["row_factory_calls"] += 1
            return iter(self.rows)

    class FakeArtifacts:
        def prepare_replayable_rows(
            self,
            rows: object,
            **kwargs: object,
        ) -> object:
            assert callable(rows)
            assert tuple(rows()) == (derived_pair,)  # type: ignore[operator]
            captured["prepared_hypothesis"] = kwargs["hypothesis"]
            return SimpleNamespace(hypothesis=kwargs["hypothesis"])

        def save_prepared_portfolio(
            self,
            entries: object,
            requested: tuple[object, ...],
            *,
            cost_model_version: str,
        ) -> object:
            captured["entries"] = tuple(entries)  # type: ignore[arg-type]
            return SimpleNamespace(
                artifact_uri=str(tmp_path / "prospective"),
                artifact_fingerprint="sha256:" + "c" * 64,
                evidence=tuple({"hypothesis_id": item.value} for item in requested),
            )

    monkeypatch.setattr(replay_api, "FileProspectiveScientificRowSpool", FakeSpool)
    monkeypatch.setattr(
        replay_api,
        "partitioned_prospective_split",
        lambda cache: SimpleNamespace(),
    )
    monkeypatch.setattr(
        replay_api,
        "iter_independent_prospective_row_partitions",
        lambda cache, *, request: ((derived_pair,),),
    )

    def fingerprint_from_rows(*, rows: object, **_: object) -> str:
        assert callable(rows)
        assert tuple(rows()) == (derived_pair,)  # type: ignore[operator]
        return "sha256:" + "a" * 64

    monkeypatch.setattr(
        replay_api,
        "prospective_report_fingerprint_from_rows",
        fingerprint_from_rows,
    )
    monkeypatch.setattr(
        replay_api,
        "build_partitioned_prospective_scientific_research",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("dense model must use the external spool")
        ),
    )
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        prospective_artifacts=FakeArtifacts(),  # type: ignore[arg-type]
    )
    runner._descriptor_cache = FakeCache()  # type: ignore[assignment]

    result = runner.execute(
        StartReplayRequest(
            hypothesis_ids=("H7V3",),
            cost_model={"version": "cost-v3"},
        ),
        run_fingerprint="sha256:" + "d" * 64,
    )

    assert captured["load_calls"] == 0
    assert captured["prepared_hypothesis"].value == "H7V3"
    assert captured["row_factory_calls"] >= 2
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == ("H7V3",)


def test_internal_runner_stages_full_combination_sources_and_wires_bounded_evidence(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {"staged": []}
    source_ids = tuple(
        hypothesis.value for hypothesis in replay_api.COMBINATION_SOURCE_HYPOTHESES
    )
    report = SimpleNamespace(
        report_fingerprint="sha256:" + "a" * 64,
        dataset_fingerprint="sha256:" + "b" * 64,
    )

    class FakeCache:
        def describe(self) -> object:
            return SimpleNamespace(dataset_fingerprint=report.dataset_fingerprint)

        def load(self) -> tuple[object, ...]:
            return (object(),)

    class FakeProspectiveArtifacts:
        def save_portfolio(
            self,
            actual_reports: object,
            requested: tuple[object, ...],
            *,
            cost_model_version: str,
        ) -> object:
            captured["reports"] = tuple(actual_reports)
            return SimpleNamespace(
                artifact_uri=str(tmp_path / "prospective"),
                artifact_fingerprint="sha256:" + "c" * 64,
                evidence=tuple({"hypothesis_id": item.value} for item in requested),
            )

    class FakeStage:
        def __init__(self, root: object) -> None:
            captured["stage_root"] = root

        def stage(self, actual_report: object, *, cost_model_version: str) -> None:
            captured["staged"].append((actual_report, cost_model_version))

    class FakeStreamingArtifacts:
        def __init__(self, root: object) -> None:
            captured["combination_root"] = root

    class FakeCombinationEvaluator:
        def __init__(self, *, artifacts: object) -> None:
            captured["streaming_artifacts"] = artifacts

        def execute(
            self,
            source: object,
            *,
            cost_model_version: str,
        ) -> object:
            captured["combination_source"] = source
            captured["combination_cost_model"] = cost_model_version
            return SimpleNamespace(
                run_id="sha256:" + "d" * 64,
                artifact=SimpleNamespace(
                    artifact_uri=str(tmp_path / "combinations"),
                    artifact_fingerprint="sha256:" + "e" * 64,
                ),
                partition_count=180,
                observation_count=1234,
                result_count=8,
                resumed=False,
            )

    class FakeCombinationEvidenceReader:
        def read(
            self,
            artifact_uri: str,
            *,
            expected_artifact_fingerprint: str,
        ) -> CombinationEvidenceArtifactSnapshot:
            captured["derived_artifact"] = (
                artifact_uri,
                expected_artifact_fingerprint,
            )
            rows = []
            for combination_id in replay_api.ScientificCombinationId:
                definition = preregistered_combination_definition(combination_id)
                for horizon in definition.horizons_seconds:
                    rows.append(
                        CombinationHorizonArtifactSnapshot(
                            combination_id=combination_id,
                            combination_version=definition.version,
                            horizon_seconds=horizon,
                            dataset_fingerprint="sha256:" + "b" * 64,
                            decision="blocked_by_data",
                            reason_codes=("minimum_eligible_events_not_met",),
                            total_observations=10,
                            abstained_observations=2,
                            eligible_events=8,
                            matched_events=0,
                            matched_controls=0,
                            trading_days=2,
                            cost_model_version="cost-v4",
                            mean_lift_bps=None,
                            lift_interval=None,
                            adjusted_q_value=None,
                            positive_stability_blocks=0,
                            total_stability_blocks=0,
                            maximum_instrument_share=None,
                            diagnostics=None,
                        )
                    )
            return CombinationEvidenceArtifactSnapshot(
                artifact_fingerprint=expected_artifact_fingerprint,
                dataset_fingerprint="sha256:" + "b" * 64,
                cost_model_version="cost-v4",
                horizons=tuple(rows),
            )

    def fake_build(
        candles: object,
        *,
        dataset_fingerprint: str,
        request: object,
    ) -> object:
        captured.setdefault("requests", []).append(request)
        return report

    monkeypatch.setattr(
        replay_api,
        "PROSPECTIVE_SCIENTIFIC_HYPOTHESES",
        replay_api.PROSPECTIVE_SCIENTIFIC_HYPOTHESES | {"H1", "H2"},
    )
    monkeypatch.setattr(replay_api, "GENERAL_HYPOTHESES", frozenset())
    monkeypatch.setattr(replay_api, "build_prospective_scientific_research", fake_build)
    monkeypatch.setattr(
        replay_api, "FileProspectiveScientificPartitionStage", FakeStage
    )
    monkeypatch.setattr(
        replay_api,
        "FileScientificCombinationStreamingArtifacts",
        FakeStreamingArtifacts,
    )
    monkeypatch.setattr(
        replay_api, "EvaluateScientificCombinationPartitions", FakeCombinationEvaluator
    )
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        prospective_artifacts=FakeProspectiveArtifacts(),  # type: ignore[arg-type]
        combination_evidence_reader=FakeCombinationEvidenceReader(),
    )
    runner._descriptor_cache = FakeCache()  # type: ignore[assignment]

    result = runner.execute(
        StartReplayRequest(
            hypothesis_ids=source_ids,
            cost_model={"version": "cost-v4"},
        ),
        run_fingerprint="sha256:" + "f" * 64,
    )

    assert len(captured["reports"]) == len(source_ids)
    assert len(captured["staged"]) == len(source_ids)
    assert all(item[1] == "cost-v4" for item in captured["staged"])
    assert captured["combination_source"].__class__ is FakeStage
    assert captured["combination_cost_model"] == "cost-v4"
    assert result["engines"][1] == {
        "engine": "scientific_combination_evidence",
        "combination_ids": ("C1", "C2", "C3", "C4", "C5"),
        "application_run_id": "sha256:" + "d" * 64,
        "artifact_fingerprint": "sha256:" + "e" * 64,
        "artifact_uri": str(tmp_path / "combinations"),
        "partition_count": 180,
        "observation_count": 1234,
        "result_count": 8,
        "resumed": False,
    }
    assert tuple(item["hypothesis_id"] for item in result["derived_evidence"]) == (
        "C1",
        "C2",
        "C3",
        "C4",
        "C5",
    )
    assert all(
        DerivedReplayEvidenceResponse.model_validate(item)
        for item in result["derived_evidence"]
    )
    json.dumps(result)
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == tuple(
        sorted(source_ids)
    )


def test_internal_runner_reclaims_transient_heap_at_materialized_boundaries(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class FakeCache:
        def describe(self) -> object:
            return SimpleNamespace(dataset_fingerprint="sha256:" + "a" * 64)

        def materialize_ticker_partitions(self, root: object) -> None:
            assert root == tmp_path / "artifacts" / ".replay-working"
            events.append("materialized")

        def close_materialized_partitions(self) -> None:
            events.append("closed")

    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
        memory_reclaimer=lambda: events.append("reclaimed"),
    )
    runner._descriptor_cache = FakeCache()  # type: ignore[assignment]

    result = runner.execute(
        StartReplayRequest(
            hypothesis_ids=("H8",),
            cost_model={"version": "cost-v1"},
        ),
        run_fingerprint="sha256:" + "b" * 64,
    )

    assert events == ["materialized", "reclaimed", "closed", "reclaimed"]
    assert result["engines"][0]["availability"] == "requires_live_orderbook"


def test_replay_memory_reclaimer_collects_and_trims_glibc(
    monkeypatch: Any,
) -> None:
    calls: list[object] = []

    class FakeTrim:
        argtypes: object = None
        restype: object = None

        def __call__(self, padding: int) -> int:
            calls.append(("trim", padding))
            return 1

    trim = FakeTrim()
    monkeypatch.setattr(replay_api.gc, "collect", lambda: calls.append("collect"))
    monkeypatch.setattr(
        replay_api.ctypes,
        "CDLL",
        lambda _: SimpleNamespace(malloc_trim=trim),
    )

    replay_api._reclaim_replay_memory()

    assert calls == ["collect", ("trim", 0)]
    assert trim.argtypes == (replay_api.ctypes.c_size_t,)
    assert trim.restype is replay_api.ctypes.c_int
