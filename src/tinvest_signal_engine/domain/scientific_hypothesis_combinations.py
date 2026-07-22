"""Preregistered causal compositions of prospective scientific observations.

Only C1-C4 are admitted by this module.  It deliberately composes already
sealed :class:`ProspectiveFeature` decisions instead of inspecting raw candles
or searching arbitrary feature combinations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from hashlib import sha256
import json
from typing import Iterable

from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveReason,
    TargetMetric,
)


class ScientificCombinationId(str, Enum):
    C1 = "C1"
    C2 = "C2"
    C3 = "C3"
    C4 = "C4"

    @property
    def version(self) -> str:
        return "1.0.0"


class CombinationExpectedEffect(str, Enum):
    CONTINUATION = "continuation"
    REVERSAL = "reversal"
    CONDITIONAL_MORNING_DIRECTION = "conditional_morning_direction"
    PAIR_RESIDUAL_REVERSION = "pair_residual_reversion"


class CombinationComponentRole(str, Enum):
    PRIMARY = "primary"
    MARKET_CONTEXT = "market_context"


class ComponentHorizonBinding(str, Enum):
    OUTPUT_HORIZON = "output_horizon"
    FIXED = "fixed"


class CombinationReason(str, Enum):
    CONDITIONS_MATCHED = "conditions_matched"
    CONDITIONS_NOT_MET = "conditions_not_met"
    INCOMPLETE_COMPONENT_SET = "incomplete_component_set"
    AMBIGUOUS_COMPONENT_SET = "ambiguous_component_set"
    COMPONENT_ABSTAINED = "component_abstained"
    FUTURE_COMPONENT = "future_component"
    COMPONENT_TIME_MISMATCH = "component_time_mismatch"
    CONFLICTING_REGIMES = "conflicting_regimes"
    REGIME_UNRESOLVED = "regime_unresolved"
    DIRECTION_UNAVAILABLE = "direction_unavailable"
    UNSUPPORTED_HORIZON = "unsupported_horizon"


@dataclass(frozen=True, slots=True)
class CombinationComponentRequirement:
    hypothesis: ProspectiveHypothesis
    role: CombinationComponentRole
    horizon_binding: ComponentHorizonBinding
    fixed_horizon_seconds: int | None = None
    max_age_seconds: int = 0

    def __post_init__(self) -> None:
        if self.horizon_binding is ComponentHorizonBinding.FIXED:
            if self.fixed_horizon_seconds is None or self.fixed_horizon_seconds <= 0:
                raise ValueError("a fixed component horizon must be positive")
        elif self.fixed_horizon_seconds is not None:
            raise ValueError("an output-bound component cannot have a fixed horizon")
        if self.max_age_seconds < 0:
            raise ValueError("component max age must not be negative")

    def horizon_for(self, output_horizon_seconds: int) -> int:
        if self.horizon_binding is ComponentHorizonBinding.OUTPUT_HORIZON:
            return output_horizon_seconds
        assert self.fixed_horizon_seconds is not None
        return self.fixed_horizon_seconds

    def key(self, output_horizon_seconds: int) -> str:
        return "@".join(
            (
                self.hypothesis.value,
                self.hypothesis.version,
                str(self.horizon_for(output_horizon_seconds)),
                self.role.value,
            )
        )


@dataclass(frozen=True, slots=True)
class PreregisteredCombinationDefinition:
    combination_id: ScientificCombinationId
    title: str
    expected_effect: CombinationExpectedEffect
    horizons_seconds: tuple[int, ...]
    primary_horizon_seconds: int
    requirements: tuple[CombinationComponentRequirement, ...]
    comparison_hypothesis_ids: tuple[ProspectiveHypothesis, ...]
    scientific_source_ids: tuple[str, ...]
    economic_mechanism: str
    falsification_criterion: str
    multiple_testing_family: str = "preregistered-combinations-v1"
    target: TargetMetric = TargetMetric.FORWARD_RETURN

    def __post_init__(self) -> None:
        if not self.title.strip() or not self.economic_mechanism.strip():
            raise ValueError("combination semantics must be documented")
        if not self.falsification_criterion.strip():
            raise ValueError("a combination requires a falsification criterion")
        if not self.multiple_testing_family.strip():
            raise ValueError("a combination requires a multiple-testing family")
        if not self.horizons_seconds or any(
            horizon <= 0 for horizon in self.horizons_seconds
        ):
            raise ValueError("combination horizons must be positive")
        if tuple(sorted(set(self.horizons_seconds))) != self.horizons_seconds:
            raise ValueError("combination horizons must be unique and sorted")
        if self.primary_horizon_seconds not in self.horizons_seconds:
            raise ValueError("primary horizon must be a supported horizon")
        if not self.requirements:
            raise ValueError("a combination requires sealed components")
        if not self.comparison_hypothesis_ids:
            raise ValueError("a combination requires a comparison hypothesis")
        if not self.scientific_source_ids:
            raise ValueError("a combination requires scientific sources")

    @property
    def version(self) -> str:
        return self.combination_id.version


def _requirement(
    hypothesis: ProspectiveHypothesis,
    *,
    role: CombinationComponentRole = CombinationComponentRole.PRIMARY,
    fixed_horizon_seconds: int | None = None,
    max_age_seconds: int = 0,
) -> CombinationComponentRequirement:
    return CombinationComponentRequirement(
        hypothesis=hypothesis,
        role=role,
        horizon_binding=(
            ComponentHorizonBinding.FIXED
            if fixed_horizon_seconds is not None
            else ComponentHorizonBinding.OUTPUT_HORIZON
        ),
        fixed_horizon_seconds=fixed_horizon_seconds,
        max_age_seconds=max_age_seconds,
    )


PREREGISTERED_COMBINATION_DEFINITIONS = (
    PreregisteredCombinationDefinition(
        combination_id=ScientificCombinationId.C1,
        title="Продолжение импульса при подтверждении объёмом и скачком риска",
        expected_effect=CombinationExpectedEffect.CONTINUATION,
        horizons_seconds=(300, 900),
        primary_horizon_seconds=300,
        requirements=(
            _requirement(ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2),
            _requirement(
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                fixed_horizon_seconds=1800,
                max_age_seconds=900,
            ),
            _requirement(
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
                fixed_horizon_seconds=1800,
                max_age_seconds=1800,
            ),
        ),
        comparison_hypothesis_ids=(
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ),
        scientific_source_ids=(
            "chiang-kirby-nie-2021",
            "graczyk-queiros-2018",
            "corsi-pirino-reno-2010",
        ),
        economic_mechanism=(
            "Широкая торговая активность и скачок изменчивости отличают "
            "информационный импульс от временного дисбаланса ликвидности."
        ),
        falsification_criterion=(
            "После издержек сочетание не улучшает H4V2 на более поздней "
            "проверочной выборке либо улучшение создаёт один инструмент."
        ),
    ),
    PreregisteredCombinationDefinition(
        combination_id=ScientificCombinationId.C2,
        title="Разворот импульса без подтверждения активностью",
        expected_effect=CombinationExpectedEffect.REVERSAL,
        horizons_seconds=(300, 900),
        primary_horizon_seconds=300,
        requirements=(
            _requirement(ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2),
            _requirement(
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
                fixed_horizon_seconds=1800,
                max_age_seconds=900,
            ),
            _requirement(
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
                fixed_horizon_seconds=1800,
                max_age_seconds=1800,
            ),
        ),
        comparison_hypothesis_ids=(
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ),
        scientific_source_ids=(
            "jegadeesh-titman-1995",
            "butt-hogholm-sadaqat-2021",
            "chiang-kirby-nie-2021",
            "graczyk-queiros-2018",
            "corsi-pirino-reno-2010",
        ),
        economic_mechanism=(
            "Импульс без необычного объёма и скачка риска может отражать "
            "временный дисбаланс ликвидности, который затем устраняется."
        ),
        falsification_criterion=(
            "После издержек сочетание не улучшает H3V2 на более поздней "
            "проверочной выборке либо эффект неустойчив по периодам."
        ),
    ),
    PreregisteredCombinationDefinition(
        combination_id=ScientificCombinationId.C3,
        title="Утренний выбор режима",
        expected_effect=CombinationExpectedEffect.CONDITIONAL_MORNING_DIRECTION,
        horizons_seconds=(1800,),
        primary_horizon_seconds=1800,
        requirements=(
            _requirement(ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION),
            _requirement(ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION),
        ),
        comparison_hypothesis_ids=(
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
        ),
        scientific_source_ids=(
            "heston-korajczyk-sadka-2010",
            "graczyk-queiros-2018",
        ),
        economic_mechanism=(
            "Относительный объём, диапазон и подтверждение рыночной корзиной "
            "разделяют утренний возврат и продолжение движения."
        ),
        falsification_criterion=(
            "Выбор режима не улучшает раздельные H1/H2 после издержек либо "
            "не сохраняет знак на более поздней проверочной выборке."
        ),
    ),
    PreregisteredCombinationDefinition(
        combination_id=ScientificCombinationId.C4,
        title="Возврат пары только в спокойном рыночном режиме",
        expected_effect=CombinationExpectedEffect.PAIR_RESIDUAL_REVERSION,
        horizons_seconds=(900, 1800, 3600),
        primary_horizon_seconds=1800,
        requirements=(
            _requirement(ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION),
            _requirement(
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
                role=CombinationComponentRole.MARKET_CONTEXT,
                fixed_horizon_seconds=1800,
            ),
        ),
        comparison_hypothesis_ids=(ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,),
        scientific_source_ids=(
            "gatev-goetzmann-rouwenhorst-2006",
            "corsi-pirino-reno-2010",
        ),
        economic_mechanism=(
            "Возврат остатка устойчивой пары проверяется только вне общего "
            "скачка рыночного риска и после проверки ликвидности в H12."
        ),
        falsification_criterion=(
            "Фильтр спокойного рынка не улучшает H12 после издержек либо "
            "результат исчезает при исключении одного инструмента или периода."
        ),
    ),
)


def preregistered_combination_definition(
    combination_id: ScientificCombinationId,
) -> PreregisteredCombinationDefinition:
    return next(
        definition
        for definition in PREREGISTERED_COMBINATION_DEFINITIONS
        if definition.combination_id is combination_id
    )


@dataclass(frozen=True, slots=True)
class CombinationComponentEvidence:
    requirement_key: str
    hypothesis: ProspectiveHypothesis
    observation_id: str
    ticker: str
    observed_at: datetime
    feature_max_observed_at: datetime
    horizon_seconds: int
    decision: ProspectiveDecision
    reason: ProspectiveReason
    expected_direction: int
    source_payload_fingerprint: str

    def __post_init__(self) -> None:
        if not self.requirement_key.strip() or not self.observation_id.startswith(
            "sha256:"
        ):
            raise ValueError("component evidence identity is invalid")
        if not self.ticker.strip():
            raise ValueError("component evidence ticker is required")
        if not self.source_payload_fingerprint.startswith("sha256:"):
            raise ValueError("component source payload fingerprint is invalid")
        _require_aware(self.observed_at, "component observed_at")
        _require_aware(
            self.feature_max_observed_at,
            "component feature_max_observed_at",
        )
        if self.feature_max_observed_at > self.observed_at:
            raise ValueError("component evidence uses future market data")


@dataclass(frozen=True, slots=True)
class ScientificCombinationObservation:
    observation_id: str
    combination_id: ScientificCombinationId
    combination_version: str
    formula_fingerprint: str
    primary_scope: str
    market_context_scope: str | None
    trading_day: date
    observed_at: datetime
    max_used_observed_at: datetime | None
    horizon_seconds: int
    target: TargetMetric
    decision: ProspectiveDecision
    reason: CombinationReason
    expected_direction: int
    components: tuple[CombinationComponentEvidence, ...]
    missing_components: tuple[str, ...]
    payload_fingerprint: str

    def __post_init__(self) -> None:
        definition = preregistered_combination_definition(self.combination_id)
        if self.combination_version != definition.version:
            raise ValueError("combination version does not match registration")
        if self.formula_fingerprint != combination_formula_fingerprint(
            self.combination_id
        ):
            raise ValueError("combination formula fingerprint drifted")
        if not self.primary_scope.strip():
            raise ValueError("combination primary scope is required")
        _require_aware(self.observed_at, "observed_at")
        if self.max_used_observed_at is not None:
            _require_aware(self.max_used_observed_at, "max_used_observed_at")
            if self.max_used_observed_at > self.observed_at:
                raise ValueError("combination uses future component data")
        if self.horizon_seconds <= 0:
            raise ValueError("combination horizon must be positive")
        if self.expected_direction not in {-1, 0, 1}:
            raise ValueError("combination direction must be -1, 0, or 1")
        if (
            self.decision is ProspectiveDecision.MATCHED
            and self.target is TargetMetric.FORWARD_RETURN
            and self.expected_direction == 0
        ):
            raise ValueError("a matched directional combination needs a direction")
        keys = tuple(component.requirement_key for component in self.components)
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("combination components must be unique and sorted")
        if self.missing_components != tuple(sorted(set(self.missing_components))):
            raise ValueError("missing combination components must be unique and sorted")
        expected_id = deterministic_combination_observation_id(
            combination_id=self.combination_id,
            primary_scope=self.primary_scope,
            market_context_scope=self.market_context_scope,
            trading_day=self.trading_day,
            observed_at=self.observed_at,
            horizon_seconds=self.horizon_seconds,
        )
        if self.observation_id != expected_id:
            raise ValueError("combination observation identity does not match content")
        expected_payload = combination_observation_payload_fingerprint(
            observation_id=self.observation_id,
            decision=self.decision,
            reason=self.reason,
            expected_direction=self.expected_direction,
            components=self.components,
            missing_components=self.missing_components,
        )
        if self.payload_fingerprint != expected_payload:
            raise ValueError("combination payload fingerprint does not match content")

    @property
    def target_at(self) -> datetime:
        return self.observed_at + timedelta(seconds=self.horizon_seconds)


def compose_preregistered_combination(
    *,
    combination_id: ScientificCombinationId,
    primary_scope: str,
    market_context_scope: str | None,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
    components: Iterable[ProspectiveFeature],
) -> ScientificCombinationObservation:
    """Compose one sealed C1-C4 decision without looking beyond ``observed_at``."""

    if not primary_scope.strip():
        raise ValueError("primary_scope must not be empty")
    _require_aware(observed_at, "observed_at")
    definition = preregistered_combination_definition(combination_id)
    inputs = tuple(components)
    resolved, missing, resolution_reason = _resolve_components(
        definition=definition,
        primary_scope=primary_scope,
        market_context_scope=market_context_scope,
        trading_day=trading_day,
        observed_at=observed_at,
        output_horizon_seconds=horizon_seconds,
        components=inputs,
    )
    evidence = tuple(
        sorted(
            (
                _component_evidence(requirement, horizon_seconds, feature)
                for requirement, feature in resolved
            ),
            key=lambda item: item.requirement_key,
        )
    )
    missing_keys = tuple(sorted(missing))
    if horizon_seconds not in definition.horizons_seconds:
        decision, reason, direction = (
            ProspectiveDecision.ABSTAIN,
            CombinationReason.UNSUPPORTED_HORIZON,
            0,
        )
    elif resolution_reason is not None:
        decision, reason, direction = (
            ProspectiveDecision.ABSTAIN,
            resolution_reason,
            0,
        )
    elif missing_keys:
        decision, reason, direction = (
            ProspectiveDecision.ABSTAIN,
            CombinationReason.INCOMPLETE_COMPONENT_SET,
            0,
        )
    elif any(item.decision is ProspectiveDecision.ABSTAIN for item in evidence):
        decision, reason, direction = (
            ProspectiveDecision.ABSTAIN,
            CombinationReason.COMPONENT_ABSTAINED,
            0,
        )
    else:
        decision, reason, direction = _evaluate_complete_combination(
            combination_id,
            evidence,
        )
    max_used = max(
        (item.feature_max_observed_at for item in evidence),
        default=None,
    )
    observation_id = deterministic_combination_observation_id(
        combination_id=combination_id,
        primary_scope=primary_scope,
        market_context_scope=market_context_scope,
        trading_day=trading_day,
        observed_at=observed_at,
        horizon_seconds=horizon_seconds,
    )
    payload_fingerprint = combination_observation_payload_fingerprint(
        observation_id=observation_id,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        components=evidence,
        missing_components=missing_keys,
    )
    return ScientificCombinationObservation(
        observation_id=observation_id,
        combination_id=combination_id,
        combination_version=definition.version,
        formula_fingerprint=combination_formula_fingerprint(combination_id),
        primary_scope=primary_scope,
        market_context_scope=market_context_scope,
        trading_day=trading_day,
        observed_at=observed_at,
        max_used_observed_at=max_used,
        horizon_seconds=horizon_seconds,
        target=definition.target,
        decision=decision,
        reason=reason,
        expected_direction=direction,
        components=evidence,
        missing_components=missing_keys,
        payload_fingerprint=payload_fingerprint,
    )


def combination_formula_fingerprint(
    combination_id: ScientificCombinationId,
) -> str:
    definition = preregistered_combination_definition(combination_id)
    payload = {
        "combination_id": definition.combination_id.value,
        "version": definition.version,
        "expected_effect": definition.expected_effect.value,
        "target": definition.target.value,
        "horizons_seconds": definition.horizons_seconds,
        "primary_horizon_seconds": definition.primary_horizon_seconds,
        "requirements": [
            {
                "hypothesis": item.hypothesis.value,
                "version": item.hypothesis.version,
                "role": item.role.value,
                "horizon_binding": item.horizon_binding.value,
                "fixed_horizon_seconds": item.fixed_horizon_seconds,
                "max_age_seconds": item.max_age_seconds,
            }
            for item in definition.requirements
        ],
        "comparison_hypothesis_ids": [
            item.value for item in definition.comparison_hypothesis_ids
        ],
        "scientific_source_ids": definition.scientific_source_ids,
        "economic_mechanism": definition.economic_mechanism,
        "falsification_criterion": definition.falsification_criterion,
        "multiple_testing_family": definition.multiple_testing_family,
    }
    return _fingerprint(payload)


def deterministic_combination_observation_id(
    *,
    combination_id: ScientificCombinationId,
    primary_scope: str,
    market_context_scope: str | None,
    trading_day: date,
    observed_at: datetime,
    horizon_seconds: int,
) -> str:
    _require_aware(observed_at, "observed_at")
    return _fingerprint(
        {
            "combination_id": combination_id.value,
            "version": combination_id.version,
            "primary_scope": primary_scope,
            "market_context_scope": market_context_scope,
            "trading_day": trading_day.isoformat(),
            "observed_at": observed_at.isoformat(),
            "horizon_seconds": horizon_seconds,
        }
    )


def combination_observation_payload_fingerprint(
    *,
    observation_id: str,
    decision: ProspectiveDecision,
    reason: CombinationReason,
    expected_direction: int,
    components: tuple[CombinationComponentEvidence, ...],
    missing_components: tuple[str, ...],
) -> str:
    return _fingerprint(
        {
            "observation_id": observation_id,
            "decision": decision.value,
            "reason": reason.value,
            "expected_direction": expected_direction,
            "components": [
                {
                    "requirement_key": item.requirement_key,
                    "hypothesis": item.hypothesis.value,
                    "observation_id": item.observation_id,
                    "ticker": item.ticker,
                    "observed_at": item.observed_at.isoformat(),
                    "feature_max_observed_at": (
                        item.feature_max_observed_at.isoformat()
                    ),
                    "horizon_seconds": item.horizon_seconds,
                    "decision": item.decision.value,
                    "reason": item.reason.value,
                    "expected_direction": item.expected_direction,
                    "source_payload_fingerprint": item.source_payload_fingerprint,
                }
                for item in components
            ],
            "missing_components": missing_components,
        }
    )


def _resolve_components(
    *,
    definition: PreregisteredCombinationDefinition,
    primary_scope: str,
    market_context_scope: str | None,
    trading_day: date,
    observed_at: datetime,
    output_horizon_seconds: int,
    components: tuple[ProspectiveFeature, ...],
) -> tuple[
    tuple[tuple[CombinationComponentRequirement, ProspectiveFeature], ...],
    tuple[str, ...],
    CombinationReason | None,
]:
    resolved: list[tuple[CombinationComponentRequirement, ProspectiveFeature]] = []
    missing: list[str] = []
    for requirement in definition.requirements:
        expected_scope = (
            primary_scope
            if requirement.role is CombinationComponentRole.PRIMARY
            else market_context_scope
        )
        key = requirement.key(output_horizon_seconds)
        if expected_scope is None or not expected_scope.strip():
            missing.append(key)
            continue
        expected_horizon = requirement.horizon_for(output_horizon_seconds)
        candidates = tuple(
            feature
            for feature in components
            if feature.hypothesis is requirement.hypothesis
            and feature.horizon_seconds == expected_horizon
            and feature.ticker == expected_scope
        )
        timely = tuple(
            feature
            for feature in candidates
            if feature.observed_at <= observed_at
            and (observed_at - feature.observed_at).total_seconds()
            <= requirement.max_age_seconds
            and feature.trading_day == trading_day
            and feature.feature_max_observed_at <= observed_at
        )
        if not timely:
            if candidates:
                reason = (
                    CombinationReason.FUTURE_COMPONENT
                    if all(feature.observed_at > observed_at for feature in candidates)
                    else CombinationReason.COMPONENT_TIME_MISMATCH
                )
                return (), tuple(missing), reason
            missing.append(key)
            continue
        latest_at = max(feature.observed_at for feature in timely)
        timely = tuple(
            feature for feature in timely if feature.observed_at == latest_at
        )
        unique = {
            _prospective_feature_content_fingerprint(feature): feature
            for feature in timely
        }
        if len(unique) > 1:
            return (), tuple(missing), CombinationReason.AMBIGUOUS_COMPONENT_SET
        resolved.append((requirement, next(iter(unique.values()))))
    return tuple(resolved), tuple(missing), None


def _component_evidence(
    requirement: CombinationComponentRequirement,
    output_horizon_seconds: int,
    feature: ProspectiveFeature,
) -> CombinationComponentEvidence:
    return CombinationComponentEvidence(
        requirement_key=requirement.key(output_horizon_seconds),
        hypothesis=feature.hypothesis,
        observation_id=feature.observation_id,
        ticker=feature.ticker,
        observed_at=feature.observed_at,
        feature_max_observed_at=feature.feature_max_observed_at,
        horizon_seconds=feature.horizon_seconds,
        decision=feature.decision,
        reason=feature.reason,
        expected_direction=feature.expected_direction,
        source_payload_fingerprint=_prospective_feature_content_fingerprint(feature),
    )


def _evaluate_complete_combination(
    combination_id: ScientificCombinationId,
    components: tuple[CombinationComponentEvidence, ...],
) -> tuple[ProspectiveDecision, CombinationReason, int]:
    by_hypothesis = {item.hypothesis: item for item in components}
    if combination_id is ScientificCombinationId.C1:
        primary = by_hypothesis[
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2
        ]
        matched = all(
            item.decision is ProspectiveDecision.MATCHED for item in components
        )
        return _directional_result(matched, primary.expected_direction)
    if combination_id is ScientificCombinationId.C2:
        primary = by_hypothesis[ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2]
        matched = (
            primary.decision is ProspectiveDecision.MATCHED
            and by_hypothesis[
                ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3
            ].decision
            is ProspectiveDecision.NOT_MATCHED
            and by_hypothesis[
                ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE
            ].decision
            is ProspectiveDecision.NOT_MATCHED
        )
        return _directional_result(matched, primary.expected_direction)
    if combination_id is ScientificCombinationId.C3:
        reversion = by_hypothesis[ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION]
        continuation = by_hypothesis[
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION
        ]
        matched = tuple(
            item
            for item in (reversion, continuation)
            if item.decision is ProspectiveDecision.MATCHED
        )
        if len(matched) > 1:
            return (
                ProspectiveDecision.ABSTAIN,
                CombinationReason.CONFLICTING_REGIMES,
                0,
            )
        if not matched:
            return (
                ProspectiveDecision.ABSTAIN,
                CombinationReason.REGIME_UNRESOLVED,
                0,
            )
        return _directional_result(True, matched[0].expected_direction)
    if combination_id is ScientificCombinationId.C4:
        primary = by_hypothesis[ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION]
        calm_market = (
            by_hypothesis[ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE].decision
            is ProspectiveDecision.NOT_MATCHED
        )
        matched = primary.decision is ProspectiveDecision.MATCHED and calm_market
        return _directional_result(matched, primary.expected_direction)
    raise AssertionError("unregistered scientific combination")


def _directional_result(
    matched: bool,
    expected_direction: int,
) -> tuple[ProspectiveDecision, CombinationReason, int]:
    if matched and expected_direction == 0:
        return (
            ProspectiveDecision.ABSTAIN,
            CombinationReason.DIRECTION_UNAVAILABLE,
            0,
        )
    if matched:
        return (
            ProspectiveDecision.MATCHED,
            CombinationReason.CONDITIONS_MATCHED,
            expected_direction,
        )
    return (
        ProspectiveDecision.NOT_MATCHED,
        CombinationReason.CONDITIONS_NOT_MET,
        0,
    )


def _prospective_feature_content_fingerprint(feature: ProspectiveFeature) -> str:
    return _fingerprint(
        {
            "observation_id": feature.observation_id,
            "hypothesis": feature.hypothesis.value,
            "ticker": feature.ticker,
            "trading_day": feature.trading_day.isoformat(),
            "observed_at": feature.observed_at.isoformat(),
            "feature_max_observed_at": feature.feature_max_observed_at.isoformat(),
            "history_observed_until": (
                feature.history_observed_until.isoformat()
                if feature.history_observed_until is not None
                else None
            ),
            "model_trained_until": (
                feature.model_trained_until.isoformat()
                if feature.model_trained_until is not None
                else None
            ),
            "horizon_seconds": feature.horizon_seconds,
            "target": feature.target.value,
            "decision": feature.decision.value,
            "reason": feature.reason.value,
            "expected_direction": feature.expected_direction,
            "forecast": (
                (
                    feature.forecast.name,
                    feature.forecast.unit.value,
                    feature.forecast.value,
                )
                if feature.forecast is not None
                else None
            ),
            "feature_values": [
                (item.name, item.unit.value, item.value)
                for item in feature.feature_values
            ],
        }
    )


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()


def _require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
