from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

import pytest

from tinvest_signal_engine.adapters.scientific_model_shadow import (
    seal_shadow_dataset,
)
from tinvest_signal_engine.adapters.scientific_portfolio_selector import (
    ImmutableJsonScientificPortfolioReportAdapter,
    PORTFOLIO_REPORT_SCHEMA,
    PORTFOLIO_REPORT_VERSION,
    SealedScientificPortfolioSelectorExampleSource,
    selector_examples_from_shadow_dataset,
)
from tinvest_signal_engine.application.scientific_portfolio_selector import (
    RunScientificPortfolioSelector,
    ScientificPortfolioSelectorPolicy,
)
from tinvest_signal_engine.domain.scientific_model_shadow import (
    SealedShadowDataset,
    ShadowModelExample,
    ShadowStudyKind,
    ShadowStudyScope,
)
from tinvest_signal_engine.domain.scientific_portfolio_selector import (
    PortfolioAction,
    PortfolioSelectorModel,
    PortfolioSelectorState,
)


UTC = timezone.utc


def _scope(
    study_id: str,
    *,
    direction: int,
    kind: ShadowStudyKind = ShadowStudyKind.HYPOTHESIS,
) -> tuple[ShadowStudyScope, int]:
    directional = direction != 0
    return (
        ShadowStudyScope(
            study_id=study_id,
            study_version="1.0.0",
            study_kind=kind,
            horizon_seconds=300,
            effect_unit=(
                "basis_points" if directional else "variance_uplift_ratio_x_10000"
            ),
            cost_model_version="cost-v1",
            costs_applied=directional,
        ),
        direction,
    )


def _dataset(*, days: int = 40) -> SealedShadowDataset:
    scoped = (
        _scope("H-UP", direction=1),
        _scope("H-DOWN", direction=-1),
        _scope("H-RISK", direction=0),
        _scope("H-FLAT", direction=0),
    )
    examples = []
    for day_index in range(days):
        trading_day = date(2026, 1, 1) + timedelta(days=day_index)
        for scope_index, (scope, direction) in enumerate(scoped):
            effect = -10.0 if scope.study_id == "H-FLAT" else 10.0
            for row_index in range(2):
                observed_at = datetime(
                    trading_day.year,
                    trading_day.month,
                    trading_day.day,
                    10 + scope_index,
                    row_index,
                    tzinfo=UTC,
                )
                examples.append(
                    ShadowModelExample(
                        scope=scope,
                        observation_id=(
                            f"{scope.study_id}-{day_index:03d}-{row_index}"
                        ),
                        instrument_id=("SBER", "GAZP")[row_index],
                        trading_day=trading_day,
                        observed_at=observed_at,
                        feature_max_observed_at=observed_at,
                        feature_values=(
                            ("sealed_expected_direction", float(direction)),
                            ("signal_strength", float(scope_index + 1)),
                        ),
                        effect_value=effect,
                    )
                )
    scopes = tuple(
        sorted((scope for scope, _ in scoped), key=lambda item: item.key)
    )
    return SealedShadowDataset(
        dataset_fingerprint="sha256:" + "1" * 64,
        source_artifact_fingerprints=(
            "sha256:" + "2" * 64,
            "sha256:" + "3" * 64,
        ),
        scopes=scopes,
        examples=tuple(examples),
    )


def _policy() -> ScientificPortfolioSelectorPolicy:
    return ScientificPortfolioSelectorPolicy(
        minimum_train_examples=100,
        minimum_validation_examples=40,
        minimum_holdout_examples=40,
        minimum_total_trading_days=30,
        minimum_acted_examples=8,
        minimum_coverage=0.20,
        gap_trading_days=1,
        confidence_thresholds=(0.35, 0.50, 0.65, 0.80),
        minimum_accuracy_lift=0.05,
        logistic_complexity_premium=0.03,
        walk_forward_folds=4,
        minimum_positive_walk_forward_folds=3,
        calibration_bins=5,
        logistic_iterations=120,
        logistic_learning_rate=0.12,
    )


def test_fixture_artifact_runs_to_versioned_json_report(tmp_path: Path) -> None:
    input_dir = tmp_path / "sealed-input"
    seal_shadow_dataset(input_dir, _dataset())
    output_dir = tmp_path / "reports"
    source = SealedScientificPortfolioSelectorExampleSource(input_dir)
    artifacts = ImmutableJsonScientificPortfolioReportAdapter(output_dir)

    execution = RunScientificPortfolioSelector(
        source=source,
        artifacts=artifacts,
        policy=_policy(),
    ).execute()

    assert execution.reused is False
    assert execution.result is not None
    assert execution.result.state is PortfolioSelectorState.READY
    assert (
        execution.result.selected_model
        is PortfolioSelectorModel.BAYESIAN_FREQUENCY
    )
    report_path = Path(execution.artifact_uri) / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["schema"] == PORTFOLIO_REPORT_SCHEMA
    assert report["report_version"] == PORTFOLIO_REPORT_VERSION
    assert report["selected_model"] == "bayesian_frequency"
    assert report["claim_allowed"] is False
    assert report["causal_evidence_gate_unchanged"] is True
    assert report["summary"]["holdout_coverage"] == 0.75
    assert report["summary"]["holdout_abstention_rate"] == 0.25
    assert report["summary"]["holdout_accuracy_when_acted"] == 1.0
    assert len(report["decisions"]) == 64
    assert {item["action"] for item in report["decisions"]} == {
        "up",
        "down",
        "risk",
        "abstain",
    }
    assert all(
        "calibration" in model["holdout"]
        and "coverage" in model["holdout"]
        and "abstention_rate" in model["holdout"]
        for model in report["models"]
    )
    completion = json.loads(
        (Path(execution.artifact_uri) / "completion.json").read_text(
            encoding="utf-8"
        )
    )
    assert completion["schema"] == PORTFOLIO_REPORT_SCHEMA
    assert set(completion["hashes"]) == {"manifest.json", "report.json"}
    assert artifacts.completed_uri(
        execution.run_id, execution.result.input_fingerprint
    ) == execution.artifact_uri

    reused = RunScientificPortfolioSelector(
        source=source,
        artifacts=artifacts,
        policy=_policy(),
    ).execute()
    assert reused.reused is True
    assert reused.result is None
    assert reused.artifact_uri == execution.artifact_uri


def test_mapping_uses_only_sealed_features_and_mature_result_time() -> None:
    examples = selector_examples_from_shadow_dataset(_dataset(days=1))

    assert {item.target_action for item in examples} == {
        PortfolioAction.UP,
        PortfolioAction.DOWN,
        PortfolioAction.RISK,
        PortfolioAction.ABSTAIN,
    }
    assert all(
        item.feature_max_observed_at <= item.observed_at
        and item.label_observed_at
        == item.observed_at + timedelta(seconds=item.horizon_seconds)
        for item in examples
    )
    assert all(
        name != "effect_value"
        for item in examples
        for name, _ in item.feature_values
    )
    assert len({tuple(name for name, _ in item.feature_values) for item in examples}) == 1
    assert all(
        item.source_artifact_fingerprints
        == ("sha256:" + "2" * 64, "sha256:" + "3" * 64)
        for item in examples
    )


def test_directional_source_without_costs_is_rejected() -> None:
    source = _dataset(days=1)
    first = source.examples[0]
    broken_scope = replace(first.scope, costs_applied=False)
    broken = replace(
        source,
        scopes=tuple(
            broken_scope if item.key == first.scope.key else item
            for item in source.scopes
        ),
        examples=tuple(
            replace(item, scope=broken_scope)
            if item.scope.key == first.scope.key
            else item
            for item in source.examples
        ),
    )

    with pytest.raises(ValueError, match="require sealed costs"):
        selector_examples_from_shadow_dataset(broken)


def test_research_script_is_a_runnable_composition_root(tmp_path: Path) -> None:
    input_dir = tmp_path / "sealed-input"
    output_dir = tmp_path / "reports"
    seal_shadow_dataset(input_dir, _dataset())
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "research_select_scientific_portfolio.py"
    )

    completed = subprocess.run(
        (
            sys.executable,
            str(script),
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--minimum-train-examples",
            "100",
            "--minimum-validation-examples",
            "40",
            "--minimum-holdout-examples",
            "40",
            "--minimum-trading-days",
            "30",
            "--minimum-acted-examples",
            "8",
            "--minimum-coverage",
            "0.2",
            "--minimum-accuracy-lift",
            "0.05",
        ),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "completed"
    assert payload["network_used"] is False
    assert payload["selected_model"] == "bayesian_frequency"
    assert payload["holdout_coverage"] == 0.75
    assert Path(payload["artifact_uri"], "report.json").is_file()
