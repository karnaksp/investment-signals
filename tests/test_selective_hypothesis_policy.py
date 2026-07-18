from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.selective_hypothesis_policy import (
    JsonSelectiveResearchArtifactAdapter,
    SklearnLightgbmEstimatorFactory,
)
from tinvest_signal_engine.application.selective_hypothesis_policy import (
    ResearchSelectiveHypothesisPolicy,
    SelectivePortfolioResult,
)
from tinvest_signal_engine.application.selective_jump_policy_research import (
    RunSelectiveJumpPolicyResearch,
)
from tinvest_signal_engine.domain.selective_hypothesis_policy import (
    SelectiveExample,
    SelectiveModelKind,
    SelectiveResearchDecision,
    SelectiveResearchPolicy,
)


class _ScoreEstimator:
    def predict_probabilities(self, rows):
        return tuple(row[0] for row in rows)


class _ScoreFactory:
    def __init__(self) -> None:
        self.fit_labels: list[tuple[int, ...]] = []

    def available_model_kinds(self):
        return (SelectiveModelKind.LOGISTIC_REGRESSION,)

    def fit(self, *, model_kind, feature_names, rows, labels, seed):
        del model_kind, feature_names, rows, seed
        self.fit_labels.append(tuple(labels))
        return _ScoreEstimator()


class _ConstantEstimator:
    def predict_probabilities(self, rows):
        return tuple(0.5 for _ in rows)


class _ConstantFactory(_ScoreFactory):
    def fit(self, **kwargs):
        self.fit_labels.append(tuple(kwargs["labels"]))
        return _ConstantEstimator()


def _policy() -> SelectiveResearchPolicy:
    return SelectiveResearchPolicy(
        probability_thresholds=(0.5, 0.7, 0.9),
        minimum_train_examples=20,
        minimum_tune_examples=10,
        minimum_holdout_examples=10,
        minimum_acted_examples=5,
        minimum_coverage=0.25,
        minimum_lift_bps=1.0,
        bootstrap_samples=200,
        calibration_bins=5,
    )


def _examples(*, reverse_holdout: bool = False) -> tuple[SelectiveExample, ...]:
    start = date(2026, 1, 1)
    examples: list[SelectiveExample] = []
    for day_index in range(50):
        trading_day = start + timedelta(days=day_index)
        for index, (score, result) in enumerate(((0.9, 10.0), (0.1, -10.0))):
            if reverse_holdout and day_index >= 40:
                result = -result
            observed_at = datetime.combine(
                trading_day, time(10, index), tzinfo=timezone.utc
            )
            examples.append(
                SelectiveExample(
                    hypothesis_id="H3",
                    hypothesis_version="1.0.0",
                    observation_id=f"{day_index}-{index}",
                    instrument_id="SBER",
                    horizon_seconds=300,
                    trading_day=trading_day,
                    observed_at=observed_at,
                    feature_max_observed_at=observed_at,
                    feature_values=(("score", score),),
                    cost_adjusted_result_bps=result,
                    cost_model_version="cost-v1",
                )
            )
    return tuple(examples)


def test_selective_example_rejects_future_feature() -> None:
    example = _examples()[0]
    with pytest.raises(ValueError, match="future"):
        replace(
            example,
            feature_max_observed_at=example.observed_at + timedelta(seconds=1),
        )


def test_model_is_selected_on_tune_then_passes_independent_holdout() -> None:
    factory = _ScoreFactory()
    result = ResearchSelectiveHypothesisPolicy(
        estimator_factory=factory, policy=_policy()
    ).execute(_examples())

    assert result.tune_selected_model is SelectiveModelKind.LOGISTIC_REGRESSION
    assert result.tune_selected_threshold == 0.5
    assert result.decision is SelectiveResearchDecision.IMPROVED
    assert result.deployment_model is SelectiveModelKind.LOGISTIC_REGRESSION
    assert result.claim_allowed is True
    assert result.hypothesis_changed is False
    assert result.holdout_selected_metrics.coverage == 0.5
    assert result.holdout_selected_metrics.abstention_rate == 0.5
    assert result.holdout_selected_metrics.useful_rate_when_acted == 1.0
    assert result.holdout_selected_metrics.mean_cost_adjusted_result_bps == 10.0
    assert result.holdout_lift_interval is not None
    assert result.holdout_lift_interval.lower > 0.0
    assert len(factory.fit_labels[0]) == 60


def test_holdout_cannot_change_tune_selection_but_can_block_claim() -> None:
    passing = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ScoreFactory(), policy=_policy()
    ).execute(_examples())
    failing = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ScoreFactory(), policy=_policy()
    ).execute(_examples(reverse_holdout=True))

    assert failing.tune_selected_model is passing.tune_selected_model
    assert failing.tune_selected_threshold == passing.tune_selected_threshold
    assert failing.decision is SelectiveResearchDecision.NO_IMPROVEMENT
    assert failing.deployment_model is SelectiveModelKind.SEALED_RULE
    assert failing.claim_allowed is False


def test_no_tune_improvement_retains_sealed_rule() -> None:
    result = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ConstantFactory(), policy=_policy()
    ).execute(_examples())

    assert result.tune_selected_model is SelectiveModelKind.SEALED_RULE
    assert result.decision is SelectiveResearchDecision.NO_IMPROVEMENT
    assert result.deployment_model is SelectiveModelKind.SEALED_RULE
    assert result.claim_allowed is False


def test_generic_hypothesis_contract_and_calibration_are_supported() -> None:
    examples = tuple(
        replace(item, hypothesis_id="OTHER-SCIENTIFIC-HYPOTHESIS")
        for item in _examples()
    )
    result = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ScoreFactory(), policy=_policy()
    ).execute(examples)

    assert result.hypothesis_id == "OTHER-SCIENTIFIC-HYPOTHESIS"
    assert result.holdout_selected_metrics.brier_score == pytest.approx(0.01)
    assert result.holdout_selected_metrics.expected_calibration_error == pytest.approx(0.1)
    assert sum(
        item.observations for item in result.holdout_selected_metrics.calibration
    ) == result.holdout_selected_metrics.observations


def test_too_little_data_returns_explicit_insufficient_decision() -> None:
    result = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ScoreFactory(), policy=_policy()
    ).execute(_examples()[:10])
    assert result.decision is SelectiveResearchDecision.INSUFFICIENT_DATA
    assert result.deployment_model is SelectiveModelKind.SEALED_RULE
    assert result.claim_allowed is False


def test_optional_estimators_are_deterministic() -> None:
    pytest.importorskip("sklearn")
    factory = SklearnLightgbmEstimatorFactory()
    rows = tuple((float(index), float(index % 3)) for index in range(40))
    labels = tuple(index % 2 for index in range(40))
    kinds = factory.available_model_kinds()
    assert SelectiveModelKind.LOGISTIC_REGRESSION in kinds
    for kind in kinds:
        first = factory.fit(
            model_kind=kind,
            feature_names=("a", "b"),
            rows=rows,
            labels=labels,
            seed=123,
        )
        second = factory.fit(
            model_kind=kind,
            feature_names=("a", "b"),
            rows=rows,
            labels=labels,
            seed=123,
        )
        assert first is not None and second is not None
        assert first.predict_probabilities(rows) == pytest.approx(
            second.predict_probabilities(rows)
        )


def test_artifact_is_immutable_and_reusable(tmp_path: Path) -> None:
    study = ResearchSelectiveHypothesisPolicy(
        estimator_factory=_ScoreFactory(), policy=_policy()
    ).execute(_examples())
    result = SelectivePortfolioResult(
        run_id="selective-fixture",
        input_fingerprint="sha256:data",
        policy_fingerprint="sha256:policy",
        results=(study,),
        examples=100,
    )
    adapter = JsonSelectiveResearchArtifactAdapter(tmp_path)
    uri = adapter.persist(result)

    assert adapter.completed_uri(result.run_id, result.input_fingerprint) == uri
    assert (Path(uri) / "model-results.json").is_file()
    assert (Path(uri) / "leaderboard.csv").is_file()
    assert (Path(uri) / "report.md").is_file()
    assert adapter.persist(result) == uri


def test_completed_portfolio_skips_candle_loading() -> None:
    class Cache:
        def fingerprint(self, tickers=None):
            assert tickers is None
            return "sha256:cached"

        def load(self, tickers=None):
            raise AssertionError("completed research must not load candles")

    class Artifacts:
        def completed_uri(self, run_id, input_fingerprint):
            assert run_id.startswith("selective-h3-h4-")
            assert input_fingerprint == "sha256:cached"
            return "/already/completed"

        def persist(self, result):
            raise AssertionError("completed research must not persist again")

    execution = RunSelectiveJumpPolicyResearch(
        candle_cache=Cache(),
        artifacts=Artifacts(),
        estimator_factory=_ScoreFactory(),
    ).execute()

    assert execution.reused is True
    assert execution.result is None
    assert execution.artifact_uri == "/already/completed"
