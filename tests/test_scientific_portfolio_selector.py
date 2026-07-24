from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

import pytest

from tinvest_signal_engine.application.scientific_portfolio_selector import (
    EvaluateScientificPortfolioSelector,
    ScientificPortfolioSelectorPolicy,
)
from tinvest_signal_engine.domain.scientific_portfolio_selector import (
    PortfolioAction,
    PortfolioSelectorExample,
    PortfolioSelectorModel,
    PortfolioSelectorState,
)


def _policy(**overrides: object) -> ScientificPortfolioSelectorPolicy:
    values: dict[str, object] = {
        "minimum_train_examples": 100,
        "minimum_validation_examples": 40,
        "minimum_holdout_examples": 40,
        "minimum_total_trading_days": 30,
        "minimum_acted_examples": 8,
        "minimum_coverage": 0.20,
        "gap_trading_days": 1,
        "confidence_thresholds": (0.35, 0.50, 0.65, 0.80),
        "minimum_accuracy_lift": 0.05,
        "logistic_complexity_premium": 0.03,
        "walk_forward_folds": 4,
        "minimum_positive_walk_forward_folds": 3,
        "calibration_bins": 5,
        "logistic_iterations": 180,
        "logistic_learning_rate": 0.12,
    }
    values.update(overrides)
    return ScientificPortfolioSelectorPolicy(**values)


def _examples(
    *,
    informative_strata: bool,
    reverse_holdout: bool = False,
    days: int = 40,
) -> tuple[PortfolioSelectorExample, ...]:
    start = date(2026, 1, 1)
    actions = (
        PortfolioAction.UP,
        PortfolioAction.UP,
        PortfolioAction.DOWN,
        PortfolioAction.DOWN,
        PortfolioAction.RISK,
        PortfolioAction.RISK,
        PortfolioAction.ABSTAIN,
        PortfolioAction.ABSTAIN,
    )
    feature_by_action = {
        PortfolioAction.UP: (2.0, 0.0),
        PortfolioAction.DOWN: (-2.0, 0.0),
        PortfolioAction.RISK: (0.0, 2.0),
        PortfolioAction.ABSTAIN: (0.0, -2.0),
    }
    reverse = {
        PortfolioAction.UP: PortfolioAction.DOWN,
        PortfolioAction.DOWN: PortfolioAction.UP,
        PortfolioAction.RISK: PortfolioAction.UP,
        PortfolioAction.ABSTAIN: PortfolioAction.RISK,
    }
    result = []
    for day_index in range(days):
        trading_day = start + timedelta(days=day_index)
        for row_index, target in enumerate(actions):
            if reverse_holdout and day_index >= int(days * 0.80):
                target = reverse[target]
            trend, risk = feature_by_action[actions[row_index]]
            observed_at = datetime.combine(
                trading_day,
                time(10, row_index),
                tzinfo=timezone.utc,
            )
            result.append(
                PortfolioSelectorExample(
                    event_id=f"event-{day_index:03d}-{row_index}",
                    instrument_id=("SBER", "GAZP")[row_index % 2],
                    source_study_ids=("study:intraday-reversal",),
                    source_artifact_fingerprints=("sha256:" + "a" * 64,),
                    trading_day=trading_day,
                    observed_at=observed_at,
                    feature_max_observed_at=observed_at,
                    label_observed_at=observed_at + timedelta(minutes=5),
                    horizon_seconds=300,
                    sealed_action=PortfolioAction.UP,
                    target_action=target,
                    probability_stratum=(
                        f"target:{actions[row_index].value}"
                        if informative_strata
                        else "common"
                    ),
                    feature_values=(
                        ("risk_score", risk),
                        ("trend_score", trend),
                    ),
                    cost_model_version="cost-v1",
                )
            )
    return tuple(result)


def test_example_rejects_future_feature_and_immature_label() -> None:
    example = _examples(informative_strata=True, days=1)[0]

    with pytest.raises(ValueError, match="future"):
        replace(
            example,
            feature_max_observed_at=example.observed_at + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="not mature"):
        replace(
            example,
            label_observed_at=example.observed_at + timedelta(seconds=299),
        )


def test_bayesian_frequency_selects_explainable_scientific_portfolio() -> None:
    result = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=True)
    )

    assert result.state is PortfolioSelectorState.READY
    assert result.selected_model is PortfolioSelectorModel.BAYESIAN_FREQUENCY
    assert result.claim_allowed is False
    assert result.causal_evidence_gate_unchanged is True
    assert tuple(item.model_kind for item in result.evaluations) == tuple(
        PortfolioSelectorModel
    )
    bayesian = result.evaluations[1]
    assert bayesian.eligible is True
    assert bayesian.validation_metrics.accuracy_when_acted == 1.0
    assert bayesian.holdout_metrics.accuracy_when_acted == 1.0
    assert bayesian.validation_metrics.coverage >= 0.70
    assert bayesian.positive_walk_forward_folds >= 3
    assert {item.action for item in result.holdout_decisions} >= {
        PortfolioAction.UP,
        PortfolioAction.DOWN,
        PortfolioAction.RISK,
        PortfolioAction.ABSTAIN,
    }


def test_regularized_logistic_uses_features_when_frequency_is_uninformative() -> None:
    result = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=False)
    )

    assert result.state is PortfolioSelectorState.READY
    assert result.selected_model is PortfolioSelectorModel.REGULARIZED_LOGISTIC
    logistic = result.evaluations[2]
    assert logistic.eligible is True
    assert logistic.validation_metrics.accuracy_when_acted == 1.0
    assert logistic.validation_accuracy_lift is not None
    assert logistic.validation_accuracy_lift >= 0.70
    assert any(
        feature == "trend_score"
        for _, feature, _ in logistic.explanation.terms
    )


def test_complex_model_cannot_displace_equal_simpler_model() -> None:
    result = EvaluateScientificPortfolioSelector(
        _policy(logistic_complexity_premium=0.05)
    ).execute(_examples(informative_strata=True))

    assert result.selected_model is PortfolioSelectorModel.BAYESIAN_FREQUENCY
    logistic = result.evaluations[2]
    assert logistic.eligible is False
    assert "complex_model_does_not_beat_simpler_model" in logistic.reason_codes


def test_failed_independent_holdout_falls_back_to_fixed_rule() -> None:
    result = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=False, reverse_holdout=True)
    )

    assert result.state is PortfolioSelectorState.NO_STABLE_IMPROVEMENT
    assert result.selected_model is PortfolioSelectorModel.FIXED_RULE
    assert result.reason_codes[0] == "tuned_candidate_failed_independent_holdout"
    assert all(
        item.model_kind is PortfolioSelectorModel.FIXED_RULE
        for item in result.holdout_decisions
    )


def test_holdout_labels_do_not_change_tuning_or_thresholds() -> None:
    normal = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=False)
    )
    reversed_holdout = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=False, reverse_holdout=True)
    )

    assert tuple(
        (
            item.selected_confidence_threshold,
            item.validation_metrics,
            item.walk_forward,
        )
        for item in normal.evaluations
    ) == tuple(
        (
            item.selected_confidence_threshold,
            item.validation_metrics,
            item.walk_forward,
        )
        for item in reversed_holdout.evaluations
    )
    assert normal.selected_model is PortfolioSelectorModel.REGULARIZED_LOGISTIC
    assert reversed_holdout.selected_model is PortfolioSelectorModel.FIXED_RULE


def test_split_has_explicit_gap_and_is_by_trading_day() -> None:
    examples = _examples(informative_strata=True)
    result = EvaluateScientificPortfolioSelector(_policy()).execute(examples)
    split = result.split

    assert split.gap_trading_days == 1
    assert len(split.embargo_days) == 2
    assert set(split.train_days).isdisjoint(split.validation_days)
    assert set(split.validation_days).isdisjoint(split.holdout_days)
    assert max(split.train_days) < min(split.validation_days)
    assert max(split.validation_days) < min(split.holdout_days)
    assert all(
        item.feature_max_observed_at <= item.observed_at for item in examples
    )


def test_insufficient_data_fails_closed_and_retains_fixed_rule() -> None:
    result = EvaluateScientificPortfolioSelector(_policy()).execute(
        _examples(informative_strata=True, days=10)
    )

    assert result.state is PortfolioSelectorState.BLOCKED_BY_DATA
    assert result.selected_model is PortfolioSelectorModel.FIXED_RULE
    assert result.claim_allowed is False
    assert "insufficient_train_examples" in result.reason_codes
    assert result.evaluations[1].eligible is False
    assert result.evaluations[2].eligible is False


def test_input_order_does_not_change_reproducible_result() -> None:
    examples = _examples(informative_strata=False)
    evaluator = EvaluateScientificPortfolioSelector(_policy())

    ordered = evaluator.execute(examples)
    reversed_input = evaluator.execute(tuple(reversed(examples)))

    assert reversed_input == ordered
    assert reversed_input.run_id == ordered.run_id
    assert reversed_input.input_fingerprint == ordered.input_fingerprint
