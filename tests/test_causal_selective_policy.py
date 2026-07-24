from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.application.causal_selective_policy import (
    BuildCausalSelectiveEpisodes,
    CausalSelectiveEvidencePartition,
    EvaluateCausalSelectivePolicy,
)
from tinvest_signal_engine.application.scientific_combination_evidence import (
    CombinationOutcomeRecord,
)
from tinvest_signal_engine.domain.causal_selective_policy import (
    CausalSelectiveDecision,
    CausalSelectivePolicy,
    CausalSelectiveReason,
    causal_selective_policy_fingerprint,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveReason,
    TargetMetric,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    CombinationReason,
    ScientificCombinationId,
    compose_preregistered_combination,
)


MOSCOW = ZoneInfo("Europe/Moscow")


def _at(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=MOSCOW)


def _feature(
    hypothesis: ProspectiveHypothesis,
    *,
    ticker: str,
    observed_at: datetime,
    direction: int = 0,
    decision: ProspectiveDecision = ProspectiveDecision.MATCHED,
    suffix: str = "0",
) -> ProspectiveFeature:
    directional = hypothesis in {
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
    }
    return ProspectiveFeature(
        observation_id=f"sha256:{hypothesis.value.lower()}-{suffix}",
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=observed_at.date(),
        observed_at=observed_at,
        feature_max_observed_at=observed_at,
        history_observed_until=observed_at - timedelta(days=1),
        model_trained_until=(
            observed_at - timedelta(days=1) if directional else None
        ),
        horizon_seconds=(
            900 if directional else 1800
        ),
        target=(
            TargetMetric.FORWARD_RETURN
            if directional
            else TargetMetric.FUTURE_VARIANCE_UPLIFT
        ),
        decision=decision,
        reason=(
            ProspectiveReason.CONDITIONS_MATCHED
            if decision is ProspectiveDecision.MATCHED
            else ProspectiveReason.CONDITIONS_NOT_MET
        ),
        expected_direction=direction if directional else 0,
        forecast=None,
        feature_values=(),
    )


def _c5(
    day: date,
    *,
    minute: int,
    direction: int = 1,
    pair_direction: int | None = None,
):
    observed_at = _at(day, 12, minute)
    market = _feature(
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ticker="SBER",
        observed_at=observed_at,
        direction=direction,
        suffix=f"{day}-{minute}-market",
    )
    pair = _feature(
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
        ticker="SBER/SBERP",
        observed_at=observed_at,
        direction=direction if pair_direction is None else pair_direction,
        suffix=f"{day}-{minute}-pair",
    )
    return compose_preregistered_combination(
        combination_id=ScientificCombinationId.C5,
        primary_scope="SBER/SBERP",
        market_context_scope="SBER",
        trading_day=day,
        observed_at=observed_at,
        horizon_seconds=900,
        components=(market, pair),
    )


def _risk(
    day: date,
    *,
    minute: int,
    hypothesis: ProspectiveHypothesis,
    decision: ProspectiveDecision = ProspectiveDecision.NOT_MATCHED,
):
    return _feature(
        hypothesis,
        ticker="SBER",
        observed_at=_at(day, 12, minute),
        decision=decision,
        suffix=f"{day}-{minute}-risk",
    )


def _raw_inputs(
    *,
    days: int = 10,
    risk_override: tuple[int, ProspectiveHypothesis, ProspectiveDecision] | None = None,
    result_bps: float = 12.0,
):
    start = date(2026, 6, 1)
    c5_rows = []
    outcomes = []
    risks = []
    for day_index in range(days):
        day = start + timedelta(days=day_index)
        for minute in (0, 10):
            item = _c5(day, minute=minute)
            c5_rows.append(item)
            outcomes.append(
                CombinationOutcomeRecord(
                    observation_id=item.observation_id,
                    combination_id=ScientificCombinationId.C5,
                    horizon_seconds=900,
                    target_at=item.target_at,
                    available=True,
                    reason_code="available",
                    source_observation_id=item.observation_id,
                    forward_return_bps=result_bps + 10.0,
                    net_directional_return_bps=result_bps,
                )
            )
            for hypothesis in (
                ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
                ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2,
            ):
                decision = ProspectiveDecision.NOT_MATCHED
                if (
                    risk_override is not None
                    and day_index == risk_override[0]
                    and hypothesis is risk_override[1]
                ):
                    decision = risk_override[2]
                risks.append(
                    _risk(
                        day,
                        minute=minute,
                        hypothesis=hypothesis,
                        decision=decision,
                    )
                )
    return tuple(c5_rows), tuple(outcomes), tuple(risks)


def _episodes(**kwargs):
    c5_rows, outcomes, risks = _raw_inputs(**kwargs)
    return BuildCausalSelectiveEpisodes().execute(
        c5_observations=c5_rows,
        outcomes=outcomes,
        risk_features=risks,
        cost_model_version="cost-v1",
    )


def _policy(**kwargs) -> CausalSelectivePolicy:
    values = {
        "minimum_training_examples": 3,
        "minimum_confidence_lower_bound": 0.40,
        "minimum_mean_cost_adjusted_return_bps": 0.0,
        "maximum_risk_evidence_age_seconds": 1800,
    }
    values.update(kwargs)
    return CausalSelectivePolicy(
        **values,
    )


def test_c5_is_only_direction_source_and_risk_can_only_force_abstention() -> None:
    report = EvaluateCausalSelectivePolicy(_policy()).execute(_episodes())
    holdout = [
        item
        for item in report.decisions
        if item.trading_day >= date(2026, 6, 9)
    ]
    assert holdout
    assert {item.decision for item in holdout} == {
        CausalSelectiveDecision.EXPECTED_UP
    }
    assert all(
        item.reason_codes == (CausalSelectiveReason.ELIGIBLE_C5_AGREEMENT,)
        for item in holdout
    )

    risky = EvaluateCausalSelectivePolicy(_policy()).execute(
        _episodes(
            risk_override=(
                9,
                ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
                ProspectiveDecision.MATCHED,
            )
        )
    )
    last = risky.decisions[-1]
    assert last.decision is CausalSelectiveDecision.ABSTAIN
    assert last.reason_codes == (CausalSelectiveReason.H16V2_ELEVATED_RISK,)
    assert last.risk_elevated is True


def test_conflict_missing_risk_and_small_training_sample_abstain_explicitly() -> None:
    day = date(2026, 6, 1)
    conflict = _c5(day, minute=0, direction=1, pair_direction=-1)
    assert conflict.reason is CombinationReason.DIRECTION_DISAGREEMENT
    c5_rows, outcomes, risks = _raw_inputs()
    modified = (conflict, *c5_rows[1:])
    episodes = BuildCausalSelectiveEpisodes().execute(
        c5_observations=modified,
        outcomes=outcomes[1:],
        risk_features=risks,
        cost_model_version="cost-v1",
    )
    report = EvaluateCausalSelectivePolicy(_policy()).execute(episodes)
    assert report.decisions[0].reason_codes == (
        CausalSelectiveReason.DIRECTION_CONFLICT,
    )
    assert CausalSelectiveReason.INSUFFICIENT_TRAINING_SAMPLE in (
        report.decisions[1].reason_codes
    )

    missing_risk = replace(episodes[-1], h17v2=None)
    missing_report = EvaluateCausalSelectivePolicy(_policy()).execute(
        (*episodes[:-1], missing_risk)
    )
    assert missing_report.decisions[-1].reason_codes == (
        CausalSelectiveReason.RISK_EVIDENCE_MISSING,
    )


def test_low_train_confidence_blocks_direction_and_never_uses_holdout_labels() -> None:
    negative = _episodes(result_bps=-5.0)
    report = EvaluateCausalSelectivePolicy(
        _policy(minimum_confidence_lower_bound=0.10)
    ).execute(negative)
    validation_holdout = report.decisions[12:]
    assert validation_holdout
    assert all(
        item.decision is CausalSelectiveDecision.ABSTAIN
        for item in validation_holdout
    )
    assert all(
        item.reason_codes
        == (CausalSelectiveReason.CONFIDENCE_LOWER_BOUND_TOO_LOW,)
        for item in validation_holdout
    )

    # Replacing every non-training outcome cannot change frozen decisions.
    changed = tuple(
        replace(
            item,
            outcome=(
                replace(item.outcome, net_directional_return_bps=500.0)
                if item.outcome is not None and item.context.trading_day >= date(2026, 6, 7)
                else item.outcome
            ),
        )
        for item in negative
    )
    changed_report = EvaluateCausalSelectivePolicy(
        _policy(minimum_confidence_lower_bound=0.10)
    ).execute(changed)
    assert tuple(item.decision for item in report.decisions) == tuple(
        item.decision for item in changed_report.decisions
    )
    assert tuple(item.reason_codes for item in report.decisions) == tuple(
        item.reason_codes for item in changed_report.decisions
    )


def test_future_risk_feature_is_rejected_at_the_domain_boundary() -> None:
    episode = _episodes()[0]
    assert episode.h16v2 is not None
    with pytest.raises(ValueError, match="future"):
        replace(
            episode,
            h16v2=replace(
                episode.h16v2,
                observed_at=episode.context.observed_at + timedelta(seconds=1),
                feature_max_observed_at=(
                    episode.context.observed_at + timedelta(seconds=1)
                ),
            ),
        )


def test_policy_and_report_fingerprints_are_deterministic_and_versioned() -> None:
    episodes = _episodes()
    evaluator = EvaluateCausalSelectivePolicy(_policy())
    first = evaluator.execute(episodes)
    second = evaluator.execute(tuple(reversed(episodes)))
    assert first == second
    assert first.report_fingerprint == second.report_fingerprint
    assert first.policy_version == _policy().version
    assert first.cost_model_version == "cost-v1"
    assert len(first.split.train_days) == 6
    assert first.product_claim_allowed is False
    assert first.automatic_execution_allowed is False
    assert causal_selective_policy_fingerprint(_policy()) != (
        causal_selective_policy_fingerprint(
            replace(_policy(), minimum_training_examples=4)
        )
    )


def test_partitioned_and_monolithic_reports_are_exactly_equivalent() -> None:
    episodes = _episodes()
    partitions = tuple(
        CausalSelectiveEvidencePartition(
            trading_day=day,
            episodes=tuple(
                item for item in episodes if item.context.trading_day == day
            ),
        )
        for day in sorted({item.context.trading_day for item in episodes})
    )
    evaluator = EvaluateCausalSelectivePolicy(_policy())
    batch = evaluator.execute(episodes)
    partitioned = evaluator.execute_partitions(reversed(partitions))
    assert partitioned == batch
    holdout = next(
        item for item in batch.metrics if item.partition.value == "holdout"
    )
    assert holdout.coverage == 1.0
    assert holdout.selective_accuracy == 1.0
    assert holdout.mean_cost_adjusted_return_bps == 12.0
