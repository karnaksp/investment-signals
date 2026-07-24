from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from tinvest_signal_engine.application.scientific_hypothesis_combinations import (
    ComposeScientificCombination,
    ComposeScientificCombinationBatch,
    ComposeScientificCombinationBatchRequest,
    ComposeScientificCombinationRequest,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveReason,
    TargetMetric,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    PREREGISTERED_COMBINATION_DEFINITIONS,
    CombinationReason,
    ComponentHorizonBinding,
    ScientificCombinationId,
    combination_formula_fingerprint,
    compose_preregistered_combination,
)


MOSCOW = ZoneInfo("Europe/Moscow")
OBSERVED_AT = datetime(2026, 7, 22, 11, 15, tzinfo=MOSCOW)
FIXTURE = (
    Path(__file__).parent / "fixtures" / "scientific-hypothesis-combinations-v1.json"
)
REGISTRY = (
    Path(__file__).parents[1] / "config" / "scientific_hypotheses" / "registry-v1.yaml"
)
NON_DIRECTIONAL = {
    ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
    ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
}


def test_catalog_is_closed_to_exactly_five_preregistered_combinations() -> None:
    assert tuple(item.value for item in ScientificCombinationId) == (
        "C1",
        "C2",
        "C3",
        "C4",
        "C5",
    )
    assert tuple(
        definition.combination_id
        for definition in PREREGISTERED_COMBINATION_DEFINITIONS
    ) == tuple(ScientificCombinationId)
    assert all(
        combination_formula_fingerprint(item).startswith("sha256:")
        for item in ScientificCombinationId
    )
    assert (
        len({combination_formula_fingerprint(item) for item in ScientificCombinationId})
        == 5
    )


def test_standalone_fixture_matches_the_domain_registration() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))

    assert fixture["contract_version"] == "1.0.0"
    assert fixture["multiple_testing_family"] == "preregistered-combinations-v1"
    for row, definition in zip(
        fixture["combinations"],
        PREREGISTERED_COMBINATION_DEFINITIONS,
        strict=True,
    ):
        assert row["id"] == definition.combination_id.value
        assert row["version"] == definition.version
        assert row["formula_fingerprint"] == combination_formula_fingerprint(
            definition.combination_id
        )
        assert tuple(row["horizons_seconds"]) == definition.horizons_seconds
        assert row["primary_horizon_seconds"] == definition.primary_horizon_seconds
        assert len(row["components"]) == len(definition.requirements)
        for component, requirement in zip(
            row["components"], definition.requirements, strict=True
        ):
            assert component["hypothesis"] == requirement.hypothesis.value
            assert component["version"] == requirement.hypothesis.version
            assert component["role"] == requirement.role.value
            if requirement.horizon_binding is ComponentHorizonBinding.OUTPUT_HORIZON:
                assert component["horizon"] == "output_horizon"
            else:
                assert component["horizon_seconds"] == (
                    requirement.fixed_horizon_seconds
                )
            assert component.get("max_age_seconds", 0) == requirement.max_age_seconds


def test_every_combination_scientific_source_exists_in_the_versioned_registry() -> None:
    registry = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    source_ids = {item["source_id"] for item in registry["sources"]}

    assert all(
        set(definition.scientific_source_ids) <= source_ids
        for definition in PREREGISTERED_COMBINATION_DEFINITIONS
    )


def test_c1_is_causal_deterministic_and_independent_of_input_order() -> None:
    h4 = _feature(
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        decision=ProspectiveDecision.MATCHED,
        direction=1,
        horizon_seconds=300,
    )
    h7 = _feature(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
    )
    h17 = _feature(
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
    )

    first = _compose(ScientificCombinationId.C1, 300, (h4, h7, h17))
    reordered = _compose(
        ScientificCombinationId.C1,
        300,
        (h17, h4, h7, h4),
    )

    assert first == reordered
    assert first.decision is ProspectiveDecision.MATCHED
    assert first.reason is CombinationReason.CONDITIONS_MATCHED
    assert first.expected_direction == 1
    assert first.max_used_observed_at == OBSERVED_AT
    assert first.target_at == OBSERVED_AT + timedelta(minutes=5)
    assert all(
        item.source_payload_fingerprint.startswith("sha256:")
        for item in first.components
    )
    assert tuple(item.hypothesis for item in first.components) == (
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
    )


def test_incomplete_component_set_explicitly_abstains() -> None:
    result = _compose(
        ScientificCombinationId.C1,
        300,
        (
            _feature(
                ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
                decision=ProspectiveDecision.MATCHED,
                direction=1,
                horizon_seconds=300,
            ),
            _feature(
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                decision=ProspectiveDecision.MATCHED,
                horizon_seconds=1800,
            ),
        ),
    )

    assert result.decision is ProspectiveDecision.ABSTAIN
    assert result.reason is CombinationReason.INCOMPLETE_COMPONENT_SET
    assert result.expected_direction == 0
    assert result.missing_components == ("H17@1.0.0@1800@primary",)


def test_c1_uses_latest_causal_activity_windows_with_sealed_maximum_ages() -> None:
    h4 = _feature(
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        decision=ProspectiveDecision.MATCHED,
        direction=1,
        horizon_seconds=300,
    )
    recent_h7 = _feature(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
        observed_at=OBSERVED_AT - timedelta(minutes=10),
    )
    recent_h17 = _feature(
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
        observed_at=OBSERVED_AT - timedelta(minutes=20),
    )
    stale_h7 = _feature(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        decision=ProspectiveDecision.NOT_MATCHED,
        horizon_seconds=1800,
        observed_at=OBSERVED_AT - timedelta(minutes=20),
    )

    result = _compose(
        ScientificCombinationId.C1,
        300,
        (stale_h7, recent_h17, h4, recent_h7),
    )

    assert result.decision is ProspectiveDecision.MATCHED
    assert result.max_used_observed_at == OBSERVED_AT
    assert {item.observation_id for item in result.components} == {
        h4.observation_id,
        recent_h7.observation_id,
        recent_h17.observation_id,
    }


def test_c2_requires_explicit_not_matched_activity_and_jump_risk() -> None:
    h3 = _feature(
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        decision=ProspectiveDecision.MATCHED,
        direction=-1,
        horizon_seconds=900,
    )
    no_volume = _feature(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        decision=ProspectiveDecision.NOT_MATCHED,
        horizon_seconds=1800,
    )
    no_jump = _feature(
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        decision=ProspectiveDecision.NOT_MATCHED,
        horizon_seconds=1800,
    )

    matched = _compose(
        ScientificCombinationId.C2,
        900,
        (h3, no_volume, no_jump),
    )
    blocked = _compose(
        ScientificCombinationId.C2,
        900,
        (
            h3,
            replace(
                no_volume,
                decision=ProspectiveDecision.MATCHED,
                reason=ProspectiveReason.CONDITIONS_MATCHED,
            ),
            no_jump,
        ),
    )
    unknown = _compose(ScientificCombinationId.C2, 900, (h3, no_volume))

    assert matched.decision is ProspectiveDecision.MATCHED
    assert matched.expected_direction == -1
    assert blocked.decision is ProspectiveDecision.NOT_MATCHED
    assert blocked.expected_direction == 0
    assert unknown.decision is ProspectiveDecision.ABSTAIN
    assert unknown.reason is CombinationReason.INCOMPLETE_COMPONENT_SET


@pytest.mark.parametrize(
    ("h1_decision", "h2_decision", "expected_decision", "reason", "direction"),
    (
        (
            ProspectiveDecision.MATCHED,
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveDecision.MATCHED,
            CombinationReason.CONDITIONS_MATCHED,
            -1,
        ),
        (
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveDecision.MATCHED,
            ProspectiveDecision.MATCHED,
            CombinationReason.CONDITIONS_MATCHED,
            1,
        ),
        (
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveDecision.NOT_MATCHED,
            ProspectiveDecision.ABSTAIN,
            CombinationReason.REGIME_UNRESOLVED,
            0,
        ),
        (
            ProspectiveDecision.MATCHED,
            ProspectiveDecision.MATCHED,
            ProspectiveDecision.ABSTAIN,
            CombinationReason.CONFLICTING_REGIMES,
            0,
        ),
    ),
)
def test_c3_selects_one_mutually_exclusive_morning_regime_or_abstains(
    h1_decision: ProspectiveDecision,
    h2_decision: ProspectiveDecision,
    expected_decision: ProspectiveDecision,
    reason: CombinationReason,
    direction: int,
) -> None:
    result = _compose(
        ScientificCombinationId.C3,
        1800,
        (
            _feature(
                ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
                decision=h1_decision,
                direction=-1,
                horizon_seconds=1800,
            ),
            _feature(
                ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
                decision=h2_decision,
                direction=1,
                horizon_seconds=1800,
            ),
        ),
    )

    assert result.decision is expected_decision
    assert result.reason is reason
    assert result.expected_direction == direction


def test_c4_uses_market_context_scope_and_h12_liquidity_decision() -> None:
    pair = _feature(
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
        ticker="SBER/GAZP",
        decision=ProspectiveDecision.MATCHED,
        direction=1,
        horizon_seconds=1800,
    )
    calm_market = _feature(
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        ticker="MOEX_FIXED_BASKET",
        decision=ProspectiveDecision.NOT_MATCHED,
        horizon_seconds=1800,
    )
    request = dict(
        combination_id=ScientificCombinationId.C4,
        primary_scope="SBER/GAZP",
        market_context_scope="MOEX_FIXED_BASKET",
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        horizon_seconds=1800,
    )

    matched = compose_preregistered_combination(
        **request,
        components=(pair, calm_market),
    )
    no_market_context = compose_preregistered_combination(
        **{**request, "market_context_scope": None},
        components=(pair, calm_market),
    )
    illiquid = compose_preregistered_combination(
        **request,
        components=(
            replace(
                pair,
                decision=ProspectiveDecision.ABSTAIN,
                reason=ProspectiveReason.INSUFFICIENT_LIQUIDITY,
                expected_direction=0,
            ),
            calm_market,
        ),
    )

    assert matched.decision is ProspectiveDecision.MATCHED
    assert matched.expected_direction == 1
    assert no_market_context.decision is ProspectiveDecision.ABSTAIN
    assert no_market_context.reason is CombinationReason.INCOMPLETE_COMPONENT_SET
    assert illiquid.decision is ProspectiveDecision.ABSTAIN
    assert illiquid.reason is CombinationReason.COMPONENT_ABSTAINED


def test_future_or_conflicting_component_content_never_produces_a_match() -> None:
    h4 = _feature(
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        decision=ProspectiveDecision.MATCHED,
        direction=1,
        horizon_seconds=300,
    )
    h7 = _feature(
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
    )
    h17 = _feature(
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
        decision=ProspectiveDecision.MATCHED,
        horizon_seconds=1800,
    )
    future_h4 = _feature(
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        decision=ProspectiveDecision.MATCHED,
        direction=1,
        horizon_seconds=300,
        observed_at=OBSERVED_AT + timedelta(minutes=1),
    )

    future = _compose(
        ScientificCombinationId.C1,
        300,
        (future_h4, h7, h17),
    )
    ambiguous = _compose(
        ScientificCombinationId.C1,
        300,
        (
            h4,
            replace(
                h4,
                decision=ProspectiveDecision.NOT_MATCHED,
                reason=ProspectiveReason.CONDITIONS_NOT_MET,
            ),
            h7,
            h17,
        ),
    )

    assert future.decision is ProspectiveDecision.ABSTAIN
    assert future.reason is CombinationReason.FUTURE_COMPONENT
    assert future.max_used_observed_at is None
    assert ambiguous.decision is ProspectiveDecision.ABSTAIN
    assert ambiguous.reason is CombinationReason.AMBIGUOUS_COMPONENT_SET


def test_application_batch_is_canonically_ordered_and_rejects_duplicates() -> None:
    c1_components = (
        _feature(
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            decision=ProspectiveDecision.MATCHED,
            direction=1,
            horizon_seconds=300,
        ),
        _feature(
            ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
            decision=ProspectiveDecision.MATCHED,
            horizon_seconds=1800,
        ),
        _feature(
            ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
            decision=ProspectiveDecision.MATCHED,
            horizon_seconds=1800,
        ),
    )
    first = _request(ScientificCombinationId.C1, 300, c1_components)
    second = replace(first, combination_id=ScientificCombinationId.C2)

    rows = ComposeScientificCombinationBatch().execute(
        ComposeScientificCombinationBatchRequest((second, first))
    )

    assert tuple(item.combination_id for item in rows) == (
        ScientificCombinationId.C1,
        ScientificCombinationId.C2,
    )
    assert ComposeScientificCombination().execute(first) == rows[0]
    with pytest.raises(ValueError, match="must be unique"):
        ComposeScientificCombinationBatchRequest((first, first))


def _request(
    combination_id: ScientificCombinationId,
    horizon_seconds: int,
    components: tuple[ProspectiveFeature, ...],
) -> ComposeScientificCombinationRequest:
    return ComposeScientificCombinationRequest(
        combination_id=combination_id,
        primary_scope="SBER",
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        horizon_seconds=horizon_seconds,
        components=components,
    )


def _compose(
    combination_id: ScientificCombinationId,
    horizon_seconds: int,
    components: tuple[ProspectiveFeature, ...],
):
    return ComposeScientificCombination().execute(
        _request(combination_id, horizon_seconds, components)
    )


def _feature(
    hypothesis: ProspectiveHypothesis,
    *,
    decision: ProspectiveDecision,
    horizon_seconds: int,
    ticker: str = "SBER",
    direction: int = 0,
    observed_at: datetime = OBSERVED_AT,
) -> ProspectiveFeature:
    reason = {
        ProspectiveDecision.MATCHED: ProspectiveReason.CONDITIONS_MATCHED,
        ProspectiveDecision.NOT_MATCHED: ProspectiveReason.CONDITIONS_NOT_MET,
        ProspectiveDecision.ABSTAIN: ProspectiveReason.INSUFFICIENT_PRIOR_DAYS,
    }[decision]
    target = (
        TargetMetric.FUTURE_VARIANCE_UPLIFT
        if hypothesis in NON_DIRECTIONAL
        else TargetMetric.FORWARD_RETURN
    )
    identity = "|".join(
        (
            hypothesis.value,
            ticker,
            observed_at.isoformat(),
            str(horizon_seconds),
            decision.value,
        )
    )
    return ProspectiveFeature(
        observation_id="sha256:" + sha256(identity.encode()).hexdigest(),
        hypothesis=hypothesis,
        ticker=ticker,
        trading_day=observed_at.date(),
        observed_at=observed_at,
        feature_max_observed_at=observed_at,
        history_observed_until=observed_at - timedelta(days=1),
        model_trained_until=None,
        horizon_seconds=horizon_seconds,
        target=target,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        forecast=None,
        feature_values=(),
    )
