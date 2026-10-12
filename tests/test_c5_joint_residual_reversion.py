from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from hashlib import sha256
import gzip
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.adapters.file_scientific_combination_pipeline import (
    FileProspectiveScientificPartitionStage,
    FileScientificCombinationStreamingArtifacts,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.application.relative_value_live_shadow import (
    BuildJointResidualLiveShadow,
    MarketResidualLiveInput,
    PairResidualLiveInput,
    RelativeValueLiveSnapshot,
)
from tinvest_signal_engine.application.scientific_combination_evidence import (
    EvaluateScientificCombinationPartitions,
    EvaluateScientificCombinationPortfolio,
    EvaluateScientificCombinationPortfolioRequest,
)
from tinvest_signal_engine.domain.hypothesis_evidence import ChronologicalSplit
from tinvest_signal_engine.domain.prospective_scientific_models import (
    FrozenMarketResidualParameters,
    FrozenPairParameters,
    MetricUnit,
    MetricValue,
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveReason,
    ProspectiveScientificPolicy,
    TargetMetric,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    CombinationReason,
    ScientificCombinationId,
    combination_formula_fingerprint,
    compose_preregistered_combination,
)


MOSCOW = ZoneInfo("Europe/Moscow")
OBSERVED_AT = datetime(2026, 7, 24, 11, 29, tzinfo=MOSCOW)


def test_c5_matches_only_when_both_residual_models_agree_on_direction() -> None:
    market = _feature(
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ticker="SBER",
        direction=-1,
    )
    pair = _feature(
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
        ticker="SBER/SBERP",
        direction=-1,
    )

    matched = _compose(market, pair)
    disagreement = _compose(replace(market, expected_direction=1), pair)
    one_not_matched = _compose(
        market,
        replace(
            pair,
            decision=ProspectiveDecision.NOT_MATCHED,
            reason=ProspectiveReason.CONDITIONS_NOT_MET,
        ),
    )

    assert matched.decision is ProspectiveDecision.MATCHED
    assert matched.reason is CombinationReason.CONDITIONS_MATCHED
    assert matched.expected_direction == -1
    assert disagreement.decision is ProspectiveDecision.ABSTAIN
    assert disagreement.reason is CombinationReason.DIRECTION_DISAGREEMENT
    assert disagreement.expected_direction == 0
    assert one_not_matched.decision is ProspectiveDecision.NOT_MATCHED
    assert one_not_matched.expected_direction == 0


def test_c5_abstains_for_missing_future_or_misaligned_component() -> None:
    market = _feature(
        ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
        ticker="SBER",
        direction=-1,
    )
    pair = _feature(
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
        ticker="SBER/SBERP",
        direction=-1,
    )

    missing = _compose(None, pair)
    future = _compose(
        replace(
            market,
            observed_at=OBSERVED_AT + timedelta(minutes=1),
            feature_max_observed_at=OBSERVED_AT + timedelta(minutes=1),
        ),
        pair,
    )
    wrong_member = compose_preregistered_combination(
        combination_id=ScientificCombinationId.C5,
        primary_scope="SBER/SBERP",
        market_context_scope="GAZP",
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        horizon_seconds=900,
        components=(market, pair),
    )

    assert missing.decision is ProspectiveDecision.ABSTAIN
    assert missing.reason is CombinationReason.INCOMPLETE_COMPONENT_SET
    assert future.decision is ProspectiveDecision.ABSTAIN
    assert future.reason is CombinationReason.FUTURE_COMPONENT
    assert future.max_used_observed_at is None
    assert wrong_member.decision is ProspectiveDecision.ABSTAIN
    assert wrong_member.reason is CombinationReason.INCOMPLETE_COMPONENT_SET


def test_live_c5_reuses_the_sealed_historical_formula_and_past_only_parameters() -> None:
    trained_until = OBSERVED_AT - timedelta(days=1)
    policy = ProspectiveScientificPolicy(
        market_residual_horizons_seconds=(900, 1800),
        pair_horizons_seconds=(900, 1800, 3600),
    )
    snapshot = RelativeValueLiveSnapshot(
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        market=MarketResidualLiveInput(
            ticker="SBER",
            stock_return_bps=30.0,
            basket_return_bps=0.0,
            basket_coverage=1.0,
            parameters=FrozenMarketResidualParameters(
                ticker="SBER",
                beta=1.0,
                absolute_residual_threshold_bps=10.0,
                training_points=200,
                trained_until=trained_until,
                basket_members=("GAZP", "LKOH"),
            ),
        ),
        pair=PairResidualLiveInput(
            left_ticker="SBER",
            right_ticker="SBERP",
            left_price=100.0,
            right_price=90.0,
            parameters=FrozenPairParameters(
                left_ticker="SBER",
                right_ticker="SBERP",
                intercept=0.0,
                hedge_ratio=1.0,
                spread_mean=0.0,
                spread_std=0.01,
                correlation=0.95,
                training_points=1000,
                trained_until=trained_until,
            ),
        ),
    )

    rows = BuildJointResidualLiveShadow(policy).execute(snapshot)

    assert tuple(item.horizon_seconds for item in rows) == (900, 1800)
    assert all(item.decision is ProspectiveDecision.MATCHED for item in rows)
    assert all(item.expected_direction == -1 for item in rows)
    assert all(
        item.formula_fingerprint
        == combination_formula_fingerprint(ScientificCombinationId.C5)
        for item in rows
    )
    with pytest.raises(ValueError, match="left ticker"):
        BuildJointResidualLiveShadow(policy).execute(
            replace(
                snapshot,
                market=replace(snapshot.market, ticker="GAZP"),
            )
        )
    with pytest.raises(ValueError, match="precede"):
        BuildJointResidualLiveShadow(policy).execute(
            replace(
                snapshot,
                market=replace(
                    snapshot.market,
                    parameters=replace(
                        snapshot.market.parameters,
                        trained_until=OBSERVED_AT,
                    ),
                ),
            )
        )


def test_materialized_and_partitioned_c5_replay_have_identical_payloads(
    tmp_path: Path,
) -> None:
    report = _report()
    policy = EvidenceGatePolicy(
        minimum_trading_days=2,
        minimum_eligible_events=2,
        controls_per_event=5,
        bootstrap_samples=100,
        maximum_instrument_share=0.99,
    )
    materialized = EvaluateScientificCombinationPortfolio(policy).execute(
        EvaluateScientificCombinationPortfolioRequest(
            report=report,
            cost_model_version="cost-v1",
            combination_ids=(ScientificCombinationId.C5,),
        )
    )
    source = _stage_by_hypothesis(report, tmp_path / "source")
    completion = EvaluateScientificCombinationPartitions(
        artifacts=FileScientificCombinationStreamingArtifacts(
            tmp_path / "partitioned"
        ),
        policy=policy,
    ).execute(
        source,
        cost_model_version="cost-v1",
        combination_ids=(ScientificCombinationId.C5,),
    )
    partitioned_observations: list[dict[str, object]] = []
    partitioned_outcomes: list[dict[str, object]] = []
    for path in sorted(Path(completion.artifact.artifact_uri).glob("partitions/*.json*")):
        if path.name.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        else:
            payload = json.loads(path.read_text(encoding="utf-8"))
        partitioned_observations.extend(payload["observations"])
        partitioned_outcomes.extend(payload["outcomes"])

    assert [item["payload_fingerprint"] for item in partitioned_observations] == [
        item.payload_fingerprint for item in materialized.observations
    ]
    assert [item["observation_id"] for item in partitioned_outcomes] == [
        item.observation_id for item in materialized.outcomes
    ]
    assert completion.observation_count == len(materialized.observations)
    assert {
        item["formula_fingerprint"] for item in partitioned_observations
    } == {combination_formula_fingerprint(ScientificCombinationId.C5)}


def _compose(
    market: ProspectiveFeature | None,
    pair: ProspectiveFeature,
):
    return compose_preregistered_combination(
        combination_id=ScientificCombinationId.C5,
        primary_scope="SBER/SBERP",
        market_context_scope="SBER",
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        horizon_seconds=900,
        components=tuple(item for item in (market, pair) if item is not None),
    )


def _feature(
    hypothesis: ProspectiveHypothesis,
    *,
    ticker: str,
    direction: int,
    observed_at: datetime = OBSERVED_AT,
    horizon_seconds: int = 900,
) -> ProspectiveFeature:
    identity = "|".join(
        (hypothesis.value, ticker, observed_at.isoformat(), str(horizon_seconds))
    )
    return ProspectiveFeature(
        observation_id="sha256:" + sha256(identity.encode()).hexdigest(),
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=observed_at.date(),
        observed_at=observed_at,
        feature_max_observed_at=observed_at,
        history_observed_until=observed_at - timedelta(days=1),
        model_trained_until=observed_at - timedelta(days=1),
        horizon_seconds=horizon_seconds,
        target=TargetMetric.FORWARD_RETURN,
        decision=ProspectiveDecision.MATCHED,
        reason=ProspectiveReason.CONDITIONS_MATCHED,
        expected_direction=direction,
        forecast=None,
        feature_values=(),
    )


def _report() -> ProspectiveScientificReport:
    days = tuple(date(2026, 7, day) for day in range(1, 11))
    features: list[ProspectiveFeature] = []
    outcomes: list[ProspectiveOutcome] = []
    for trading_day in days:
        observed_at = datetime.combine(trading_day, time(11, 29), MOSCOW)
        for horizon in (900, 1800):
            market = _feature(
                ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
                ticker="SBER",
                direction=-1,
                observed_at=observed_at,
                horizon_seconds=horizon,
            )
            pair = _feature(
                ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
                ticker="SBER/SBERP",
                direction=-1,
                observed_at=observed_at,
                horizon_seconds=horizon,
            )
            for feature in (market, pair):
                features.append(feature)
                outcomes.append(
                    ProspectiveOutcome(
                        observation_id=feature.observation_id,
                        target_at=observed_at + timedelta(seconds=horizon),
                        available=True,
                        reason=ProspectiveReason.CONDITIONS_MATCHED,
                        target=TargetMetric.FORWARD_RETURN,
                        measurements=(
                            MetricValue(
                                "forward_return",
                                MetricUnit.BASIS_POINTS,
                                -20.0,
                            ),
                        ),
                    )
                )
    ordered = tuple(
        sorted(
            zip(features, outcomes, strict=True),
            key=lambda item: (
                item[0].observed_at,
                item[0].ticker,
                item[0].hypothesis.value,
                item[0].horizon_seconds,
            ),
        )
    )
    return ProspectiveScientificReport(
        dataset_fingerprint="sha256:" + "c" * 64,
        report_fingerprint="sha256:" + "d" * 64,
        split=ChronologicalSplit(
            train_days=days[:6],
            validation_days=days[6:8],
            holdout_days=days[8:],
        ),
        policy=ProspectiveScientificPolicy(round_trip_cost_bps=1.0),
        selected_hypotheses=(
            ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
        ),
        har_v2_parameters=None,
        features=tuple(item[0] for item in ordered),
        outcomes=tuple(item[1] for item in ordered),
    )


def _stage_by_hypothesis(
    report: ProspectiveScientificReport,
    root: Path,
) -> FileProspectiveScientificPartitionStage:
    source = FileProspectiveScientificPartitionStage(root)
    for hypothesis in report.selected_hypotheses:
        pairs = tuple(
            pair
            for pair in zip(report.features, report.outcomes, strict=True)
            if pair[0].hypothesis is hypothesis
        )
        source.stage(
            replace(
                report,
                report_fingerprint=(
                    "sha256:"
                    + sha256(
                        f"{report.report_fingerprint}|{hypothesis.value}".encode()
                    ).hexdigest()
                ),
                selected_hypotheses=(hypothesis,),
                features=tuple(item[0] for item in pairs),
                outcomes=tuple(item[1] for item in pairs),
            ),
            cost_model_version="cost-v1",
        )
    return source
