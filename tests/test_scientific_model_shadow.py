from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Sequence

import pytest

from tinvest_signal_engine.adapters.scientific_model_shadow import (
    ImmutableJsonShadowArtifactAdapter,
    ImmutableJsonShadowDatasetSource,
    build_shadow_dataset_from_sealed_portfolio,
    seal_shadow_dataset,
)
from tinvest_signal_engine.application.scientific_model_shadow import (
    FeatureRow,
    RunScientificModelShadowComparison,
    ShadowComparisonPolicy,
)
from tinvest_signal_engine.domain.scientific_model_shadow import (
    SealedShadowDataset,
    ShadowModelExample,
    ShadowModelKind,
    ShadowResultState,
    ShadowStudyKind,
    ShadowStudyScope,
)


UTC = timezone.utc


class _Estimator:
    def predict_probabilities(self, rows: Sequence[FeatureRow]) -> tuple[float, ...]:
        return tuple(0.8 if row[0] > 0.0 else 0.2 for row in rows)


class _RecordingFactory:
    def __init__(self) -> None:
        self.fit_rows: list[tuple[FeatureRow, ...]] = []

    def fit(
        self,
        *,
        model_kind: ShadowModelKind,
        feature_names: tuple[str, ...],
        rows: Sequence[FeatureRow],
        labels: Sequence[int],
        seed: int,
    ) -> _Estimator:
        del model_kind, feature_names, labels, seed
        self.fit_rows.append(tuple(rows))
        return _Estimator()


def _scope(study_id: str, *, horizon: int = 300) -> ShadowStudyScope:
    return ShadowStudyScope(
        study_id=study_id,
        study_version="1.0.0",
        study_kind=(
            ShadowStudyKind.COMBINATION
            if study_id.startswith("C")
            else ShadowStudyKind.HYPOTHESIS
        ),
        horizon_seconds=horizon,
        effect_unit="basis_points",
        cost_model_version="cost-v1",
        costs_applied=True,
    )


def _dataset(study_ids: tuple[str, ...], *, days: int = 10) -> SealedShadowDataset:
    scopes = tuple(
        sorted((_scope(item) for item in study_ids), key=lambda item: item.key)
    )
    examples: list[ShadowModelExample] = []
    for scope in scopes:
        for day_index in range(days):
            trading_day = date(2026, 1, 1) + timedelta(days=day_index)
            for row_index in range(4):
                positive = (day_index + row_index) % 2 == 0
                observed_at = datetime(
                    trading_day.year,
                    trading_day.month,
                    trading_day.day,
                    10,
                    row_index,
                    tzinfo=UTC,
                )
                examples.append(
                    ShadowModelExample(
                        scope=scope,
                        observation_id=(
                            f"{scope.study_id}-{scope.horizon_seconds}-"
                            f"{day_index}-{row_index}"
                        ),
                        instrument_id="SBER",
                        trading_day=trading_day,
                        observed_at=observed_at,
                        feature_max_observed_at=observed_at,
                        feature_values=(
                            ("directional_feature", 1.0 if positive else -1.0),
                            ("trading_day_index", float(day_index)),
                        ),
                        effect_value=5.0 if positive else -5.0,
                    )
                )
    return SealedShadowDataset(
        dataset_fingerprint="sha256:" + "1" * 64,
        source_artifact_fingerprints=("sha256:" + "2" * 64,),
        scopes=scopes,
        examples=tuple(examples),
    )


def _policy(study_ids: tuple[str, ...]) -> ShadowComparisonPolicy:
    return ShadowComparisonPolicy(
        required_study_ids=study_ids,
        minimum_train_examples=20,
        minimum_validation_examples=8,
        minimum_holdout_examples=8,
        minimum_total_trading_days=10,
        minimum_holdout_trading_days=2,
        action_probability_threshold=0.60,
        calibration_bins=2,
    )


def _run(
    tmp_path: Path,
    dataset: SealedShadowDataset,
    policy: ShadowComparisonPolicy,
    factory: _RecordingFactory | None = None,
):
    input_dir = tmp_path / "input"
    seal_shadow_dataset(input_dir, dataset)
    return RunScientificModelShadowComparison(
        source=ImmutableJsonShadowDatasetSource(input_dir),
        estimators=factory or _RecordingFactory(),
        artifacts=ImmutableJsonShadowArtifactAdapter(tmp_path / "runs"),
        policy=policy,
    ).execute()


def test_future_feature_is_rejected_before_model_comparison() -> None:
    scope = _scope("H1")
    observed_at = datetime(2026, 1, 1, 10, tzinfo=UTC)
    with pytest.raises(ValueError, match="future data"):
        ShadowModelExample(
            scope=scope,
            observation_id="leak",
            instrument_id="SBER",
            trading_day=observed_at.date(),
            observed_at=observed_at,
            feature_max_observed_at=observed_at + timedelta(seconds=1),
            feature_values=(("x", 1.0),),
            effect_value=1.0,
        )


def test_fit_uses_only_earliest_chronological_trading_days(tmp_path: Path) -> None:
    factory = _RecordingFactory()
    execution = _run(tmp_path, _dataset(("H1",)), _policy(("H1",)), factory)

    assert execution.result is not None
    study = execution.result.results[0]
    assert (study.train_days, study.validation_days, study.holdout_days) == (6, 2, 2)
    assert (
        study.train_examples,
        study.validation_examples,
        study.holdout_examples,
    ) == (
        24,
        8,
        8,
    )
    assert len(factory.fit_rows) == 2
    assert all(max(row[1] for row in fitted) == 5.0 for fitted in factory.fit_rows)


def test_same_sealed_input_reuses_byte_identical_artifact(tmp_path: Path) -> None:
    dataset = _dataset(("H1",))
    first = _run(tmp_path, dataset, _policy(("H1",)))
    report = Path(first.artifact_uri, "report.md").read_bytes()
    second = RunScientificModelShadowComparison(
        source=ImmutableJsonShadowDatasetSource(tmp_path / "input"),
        estimators=_RecordingFactory(),
        artifacts=ImmutableJsonShadowArtifactAdapter(tmp_path / "runs"),
        policy=_policy(("H1",)),
    ).execute()

    assert second.reused is True
    assert second.run_id == first.run_id
    assert Path(second.artifact_uri, "report.md").read_bytes() == report


def test_c1_c4_are_compared_without_changing_causal_gate(tmp_path: Path) -> None:
    studies = ("C1", "C2", "C3", "C4")
    execution = _run(tmp_path, _dataset(studies), _policy(studies))

    assert execution.result is not None
    assert execution.result.state is ShadowResultState.READY
    assert {item.scope.study_id for item in execution.result.results} == set(studies)
    assert all(
        item.scope.study_kind is ShadowStudyKind.COMBINATION
        for item in execution.result.results
    )
    assert execution.result.causal_evidence_gate_unchanged is True
    assert execution.result.claim_allowed is False
    assert all(
        {model.model_kind for model in item.models} == set(ShadowModelKind)
        for item in execution.result.results
    )


def test_small_sample_is_persisted_as_blocked_by_data(tmp_path: Path) -> None:
    execution = _run(
        tmp_path,
        _dataset(("H1",), days=3),
        _policy(("H1",)),
    )

    assert execution.result is not None
    assert execution.result.state is ShadowResultState.BLOCKED_BY_DATA
    study = execution.result.results[0]
    assert study.state is ShadowResultState.BLOCKED_BY_DATA
    assert "minimum_total_trading_days_not_met" in study.reason_codes
    assert all(model.metrics is None for model in study.models)
    assert (
        "blocked_by_data" in Path(execution.artifact_uri, "leaderboard.csv").read_text()
    )


def test_missing_required_study_blocks_portfolio_without_overclaim(
    tmp_path: Path,
) -> None:
    policy = replace(_policy(("H1",)), required_study_ids=("H1", "C1"))
    execution = _run(tmp_path, _dataset(("H1",)), policy)

    assert execution.result is not None
    assert execution.result.state is ShadowResultState.BLOCKED_BY_DATA
    assert execution.result.missing_study_ids == ("C1",)


def test_input_checksum_tampering_is_rejected(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    seal_shadow_dataset(input_dir, _dataset(("H1",)))
    with (input_dir / "examples.jsonl").open("a", encoding="utf-8") as handle:
        handle.write("{}\n")

    with pytest.raises(ValueError, match="checksum failed"):
        ImmutableJsonShadowDatasetSource(input_dir).load()


def test_artifact_contains_accuracy_calibration_cost_and_abstention(
    tmp_path: Path,
) -> None:
    execution = _run(tmp_path, _dataset(("H1",)), _policy(("H1",)))

    leaderboard = Path(execution.artifact_uri, "leaderboard.csv").read_text()
    calibration = Path(execution.artifact_uri, "calibration.csv").read_text()
    manifest = json.loads(Path(execution.artifact_uri, "manifest.json").read_text())
    assert "accuracy" in leaderboard
    assert "abstention_rate" in leaderboard
    assert "cost_model_version" in leaderboard
    assert "observed_useful_rate" in calibration
    assert manifest["causal_evidence_gate_unchanged"] is True
    assert manifest["claim_allowed"] is False


def test_existing_sealed_hypothesis_and_combination_artifacts_are_mapped(
    tmp_path: Path,
) -> None:
    dataset_fingerprint = "sha256:" + "a" * 64
    observed_at = datetime(2026, 1, 5, 10, tzinfo=UTC)
    prospective = tmp_path / "prospective" / ("b" * 64)
    prospective_partition = prospective / "partitions" / "2026-01-05.json"
    prospective_manifest = {
        "schema": "prospective-scientific-partitions-v1",
        "dataset_fingerprint": dataset_fingerprint,
        "report_fingerprint": "sha256:" + "b" * 64,
        "hypothesis": "H1",
        "hypothesis_version": "1.0.0",
        "cost_model_version": "cost-v1",
    }
    feature = {
        "observation_id": "sha256:" + "c" * 64,
        "hypothesis": "H1",
        "ticker": "SBER",
        "trading_day": "2026-01-05",
        "observed_at": observed_at.isoformat(),
        "feature_max_observed_at": observed_at.isoformat(),
        "horizon_seconds": 300,
        "target": "forward_return",
        "decision": "matched",
        "expected_direction": 1,
        "feature_values": [{"name": "deviation", "value": 2.5}],
        "forecast": None,
    }
    outcome = {
        "observation_id": feature["observation_id"],
        "available": True,
        "measurements": [{"name": "cost_adjusted_directional_return", "value": 7.0}],
    }
    _write_json(prospective / "manifest.json", prospective_manifest)
    _write_json(prospective_partition, [{"feature": feature, "outcome": outcome}])
    _write_json(
        prospective / "completion.json",
        {
            "schema": "prospective-scientific-partitions-v1",
            "manifest_hash": _hash(prospective / "manifest.json"),
            "partition_hashes": {
                "partitions/2026-01-05.json": _hash(prospective_partition)
            },
        },
    )

    combination = tmp_path / "combination"
    combination_partition = combination / "partitions" / "2026-01-05.json"
    combination_manifest = {
        "schema": "scientific-combination-stream-v1",
        "dataset_fingerprint": dataset_fingerprint,
    }
    component = {
        "requirement_key": "H4V2@2.0.0@300@primary",
        "observed_at": observed_at.isoformat(),
        "expected_direction": 1,
        "decision": "matched",
    }
    combination_observation = {
        "observation_id": "sha256:" + "d" * 64,
        "combination_id": "C1",
        "combination_version": "1.0.0",
        "primary_scope": "SBER",
        "trading_day": "2026-01-05",
        "observed_at": observed_at.isoformat(),
        "max_used_observed_at": observed_at.isoformat(),
        "horizon_seconds": 300,
        "decision": "matched",
        "expected_direction": 1,
        "components": [component],
    }
    combination_outcome = {
        "observation_id": combination_observation["observation_id"],
        "net_directional_return_bps": 8.0,
    }
    _write_json(combination / "manifest.json", combination_manifest)
    _write_json(
        combination_partition,
        {
            "observations": [combination_observation],
            "outcomes": [combination_outcome],
        },
    )
    hashes = {
        "manifest.json": _hash(combination / "manifest.json"),
        "partitions/2026-01-05.json": _hash(combination_partition),
    }
    combination_fingerprint = (
        "sha256:"
        + sha256(
            json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    _write_json(
        combination / "completion.json",
        {
            "schema": "scientific-combination-stream-v1",
            "artifact_fingerprint": combination_fingerprint,
            "cost_model_version": "cost-v1",
            "hashes": hashes,
        },
    )

    dataset = build_shadow_dataset_from_sealed_portfolio(
        prospective_artifact_root=tmp_path / "prospective",
        combination_artifact_dir=combination,
    )

    assert {scope.study_id for scope in dataset.scopes} == {"H1", "C1"}
    assert len(dataset.examples) == 2
    assert all(
        item.feature_max_observed_at <= item.observed_at for item in dataset.examples
    )
    assert {item.effect_value for item in dataset.examples} == {7.0, 8.0}


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _hash(path: Path) -> str:
    return "sha256:" + sha256(path.read_bytes()).hexdigest()
