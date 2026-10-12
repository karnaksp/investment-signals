"""Research use cases for a causal C5-based selective decision policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hashlib import sha256
import json
from typing import Iterable, Sequence

from tinvest_signal_engine.application.scientific_combination_evidence import (
    CombinationOutcomeRecord,
)
from tinvest_signal_engine.domain.causal_selective_policy import (
    CausalSelectiveContext,
    CausalSelectiveDecision,
    CausalSelectiveDecisionRecord,
    CausalSelectiveEpisode,
    CausalSelectiveOutcome,
    CausalSelectivePartitionMetrics,
    CausalSelectivePolicy,
    CausalSelectiveReason,
    CausalSelectiveReport,
    CausalTrainingConfidence,
    causal_selective_policy_fingerprint,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    chronological_split_60_20_20,
    wilson_interval,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    CombinationReason,
    ScientificCombinationId,
    ScientificCombinationObservation,
)


@dataclass(frozen=True, slots=True)
class CausalSelectiveEvidencePartition:
    trading_day: date
    episodes: tuple[CausalSelectiveEpisode, ...]

    def __post_init__(self) -> None:
        if any(item.context.trading_day != self.trading_day for item in self.episodes):
            raise ValueError("selective partition must contain one trading day")


class BuildCausalSelectiveEpisodes:
    """Align C5 outcomes with the latest causal H16V2/H17V2 risk evidence."""

    def execute(
        self,
        *,
        c5_observations: Sequence[ScientificCombinationObservation],
        outcomes: Sequence[CombinationOutcomeRecord],
        risk_features: Sequence[ProspectiveFeature],
        cost_model_version: str,
    ) -> tuple[CausalSelectiveEpisode, ...]:
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        outcome_by_id = {item.observation_id: item for item in outcomes}
        if len(outcome_by_id) != len(outcomes):
            raise ValueError("combination outcomes must be unique")
        risk = tuple(
            item
            for item in risk_features
            if item.hypothesis
            in {
                ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2,
                ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2,
            }
        )
        episodes: list[CausalSelectiveEpisode] = []
        for c5 in sorted(
            c5_observations,
            key=lambda item: (
                item.trading_day,
                item.observed_at,
                item.primary_scope,
                item.horizon_seconds,
                item.observation_id,
            ),
        ):
            if c5.combination_id is not ScientificCombinationId.C5:
                raise ValueError("episode builder accepts only C5 observations")
            instrument = c5.market_context_scope or c5.primary_scope.partition("/")[0]
            context = CausalSelectiveContext(
                episode_id=c5.observation_id,
                instrument_id=instrument,
                primary_scope=c5.primary_scope,
                trading_day=c5.trading_day,
                observed_at=c5.observed_at,
                horizon_seconds=c5.horizon_seconds,
            )
            outcome = outcome_by_id.get(c5.observation_id)
            episodes.append(
                CausalSelectiveEpisode(
                    context=context,
                    c5=c5,
                    h16v2=self._latest_risk(
                        risk,
                        context=context,
                        hypothesis=(
                            ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_CONTRAST_V2
                        ),
                    ),
                    h17v2=self._latest_risk(
                        risk,
                        context=context,
                        hypothesis=(
                            ProspectiveHypothesis.VOLATILITY_JUMP_CONTRAST_V2
                        ),
                    ),
                    outcome=(
                        CausalSelectiveOutcome(
                            episode_id=outcome.observation_id,
                            target_at=outcome.target_at,
                            available=(
                                outcome.available
                                and outcome.net_directional_return_bps is not None
                            ),
                            net_directional_return_bps=(
                                outcome.net_directional_return_bps
                                if outcome.available
                                else None
                            ),
                            cost_model_version=cost_model_version,
                        )
                        if outcome is not None
                        else None
                    ),
                )
            )
        return tuple(episodes)

    def _latest_risk(
        self,
        features: Sequence[ProspectiveFeature],
        *,
        context: CausalSelectiveContext,
        hypothesis: ProspectiveHypothesis,
    ) -> ProspectiveFeature | None:
        candidates = tuple(
            item
            for item in features
            if item.hypothesis is hypothesis
            and item.ticker == context.instrument_id
            and item.trading_day == context.trading_day
            and item.observed_at <= context.observed_at
            and item.feature_max_observed_at <= context.observed_at
        )
        if not candidates:
            return None
        latest = max(item.observed_at for item in candidates)
        same_time = tuple(item for item in candidates if item.observed_at == latest)
        if len(same_time) > 1:
            contents = {
                (
                    item.observation_id,
                    item.decision.value,
                    item.reason.value,
                    tuple((metric.name, metric.value) for metric in item.feature_values),
                ): item
                for item in same_time
            }
            if len(contents) > 1:
                raise ValueError("ambiguous risk evidence at the same timestamp")
        return sorted(same_time, key=lambda item: item.observation_id)[0]


class EvaluateCausalSelectivePolicy:
    """Evaluate decisions using labels matured on training days only."""

    def __init__(
        self,
        policy: CausalSelectivePolicy = CausalSelectivePolicy(),
    ) -> None:
        self._policy = policy
        self._policy_fingerprint = causal_selective_policy_fingerprint(policy)

    def execute(
        self,
        episodes: Sequence[CausalSelectiveEpisode],
        *,
        split: ChronologicalSplit | None = None,
    ) -> CausalSelectiveReport:
        ordered = _canonical_episodes(episodes)
        if not ordered:
            raise ValueError("selective research requires evidence episodes")
        split = split or chronological_split_60_20_20(
            tuple(item.context.trading_day for item in ordered)
        )
        expected_days = set(split.train_days + split.validation_days + split.holdout_days)
        actual_days = {item.context.trading_day for item in ordered}
        if actual_days - expected_days:
            raise ValueError("selective evidence contains days outside the split")
        cost_models = {
            item.outcome.cost_model_version
            for item in ordered
            if item.outcome is not None
        }
        if len(cost_models) > 1:
            raise ValueError("selective report cannot mix cost model versions")

        train = tuple(
            item
            for item in ordered
            if split.partition_for(item.context.trading_day)
            is DatasetPartition.TRAIN
        )
        decisions = tuple(
            self._decide(
                episode,
                self._training_confidence(
                    train,
                    horizon_seconds=episode.context.horizon_seconds,
                    cutoff=episode.context.observed_at,
                ),
            )
            for episode in ordered
        )
        metrics = tuple(
            self._metrics(
                partition,
                ordered=ordered,
                decisions=decisions,
                split=split,
            )
            for partition in DatasetPartition
        )
        dataset_fingerprint = _dataset_fingerprint(ordered, split)
        report_fingerprint = _report_fingerprint(
            self._policy.version,
            self._policy_fingerprint,
            dataset_fingerprint,
            next(iter(cost_models)) if cost_models else None,
            decisions,
            metrics,
        )
        return CausalSelectiveReport(
            policy_version=self._policy.version,
            policy_fingerprint=self._policy_fingerprint,
            dataset_fingerprint=dataset_fingerprint,
            report_fingerprint=report_fingerprint,
            split=split,
            cost_model_version=next(iter(cost_models)) if cost_models else None,
            decisions=decisions,
            metrics=metrics,
        )

    def execute_partitions(
        self,
        partitions: Iterable[CausalSelectiveEvidencePartition],
        *,
        split: ChronologicalSplit | None = None,
    ) -> CausalSelectiveReport:
        ordered_partitions = tuple(sorted(partitions, key=lambda item: item.trading_day))
        days = tuple(item.trading_day for item in ordered_partitions)
        if len(days) != len(set(days)):
            raise ValueError("selective partitions must be unique by trading day")
        if split is None:
            split = chronological_split_60_20_20(days)
        return self.execute(
            tuple(
                episode
                for partition in ordered_partitions
                for episode in partition.episodes
            ),
            split=split,
        )

    def _training_confidence(
        self,
        train: Sequence[CausalSelectiveEpisode],
        *,
        horizon_seconds: int,
        cutoff,
    ) -> CausalTrainingConfidence | None:
        matured = tuple(
            item
            for item in train
            if item.context.horizon_seconds == horizon_seconds
            and item.context.observed_at < cutoff
            and not self._base_abstain_reasons(item)
            and item.outcome is not None
            and item.outcome.available
            and item.outcome.target_at < cutoff
            and item.outcome.net_directional_return_bps is not None
        )
        if not matured:
            return None
        cost_models = {item.outcome.cost_model_version for item in matured if item.outcome}
        if len(cost_models) != 1:
            raise ValueError("training confidence cannot mix cost models")
        values = tuple(
            item.outcome.net_directional_return_bps
            for item in matured
            if item.outcome is not None
            and item.outcome.net_directional_return_bps is not None
        )
        successes = sum(
            value > self._policy.success_threshold_bps for value in values
        )
        return CausalTrainingConfidence(
            horizon_seconds=horizon_seconds,
            examples=len(values),
            successes=successes,
            success_interval=wilson_interval(successes, len(values)),
            mean_cost_adjusted_return_bps=sum(values) / len(values),
            trained_until=max(
                item.outcome.target_at for item in matured if item.outcome is not None
            ),
            cost_model_version=next(iter(cost_models)),
        )

    def _decide(
        self,
        episode: CausalSelectiveEpisode,
        confidence: CausalTrainingConfidence | None,
    ) -> CausalSelectiveDecisionRecord:
        reasons = self._base_abstain_reasons(episode)
        if not reasons:
            if (
                confidence is None
                or confidence.examples < self._policy.minimum_training_examples
            ):
                reasons = (CausalSelectiveReason.INSUFFICIENT_TRAINING_SAMPLE,)
            elif confidence.trained_until >= episode.context.observed_at:
                raise ValueError("selective confidence uses a future outcome")
            elif (
                confidence.success_interval.lower
                < self._policy.minimum_confidence_lower_bound
            ):
                reasons = (
                    CausalSelectiveReason.CONFIDENCE_LOWER_BOUND_TOO_LOW,
                )
            elif (
                confidence.mean_cost_adjusted_return_bps
                <= self._policy.minimum_mean_cost_adjusted_return_bps
            ):
                reasons = (
                    CausalSelectiveReason.TRAINING_NET_RESULT_NOT_POSITIVE,
                )
        if reasons:
            decision = CausalSelectiveDecision.ABSTAIN
        else:
            assert episode.c5 is not None
            decision = (
                CausalSelectiveDecision.EXPECTED_UP
                if episode.c5.expected_direction == 1
                else CausalSelectiveDecision.EXPECTED_DOWN
            )
            reasons = (CausalSelectiveReason.ELIGIBLE_C5_AGREEMENT,)
        source_ids = tuple(
            item
            for item in (
                episode.c5.observation_id if episode.c5 else None,
                episode.h16v2.observation_id if episode.h16v2 else None,
                episode.h17v2.observation_id if episode.h17v2 else None,
            )
            if item is not None
        )
        return CausalSelectiveDecisionRecord(
            episode_id=episode.context.episode_id,
            trading_day=episode.context.trading_day,
            observed_at=episode.context.observed_at,
            horizon_seconds=episode.context.horizon_seconds,
            decision=decision,
            reason_codes=reasons,
            policy_fingerprint=self._policy_fingerprint,
            confidence=confidence,
            source_observation_ids=source_ids,
            risk_elevated=any(
                reason
                in {
                    CausalSelectiveReason.H16V2_ELEVATED_RISK,
                    CausalSelectiveReason.H17V2_ELEVATED_RISK,
                }
                for reason in reasons
            ),
        )

    def _base_abstain_reasons(
        self, episode: CausalSelectiveEpisode
    ) -> tuple[CausalSelectiveReason, ...]:
        c5 = episode.c5
        if c5 is None:
            return (CausalSelectiveReason.C5_MISSING,)
        if c5.missing_components or c5.reason is CombinationReason.INCOMPLETE_COMPONENT_SET:
            return (CausalSelectiveReason.C5_COMPONENT_MISSING,)
        components = {
            item.hypothesis: item
            for item in c5.components
            if item.hypothesis
            in {
                ProspectiveHypothesis.MARKET_RESIDUAL_REVERSION_V2,
                ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION_V2,
            }
        }
        if len(components) != 2:
            return (CausalSelectiveReason.C5_COMPONENT_MISSING,)
        if c5.reason is CombinationReason.DIRECTION_DISAGREEMENT:
            return (CausalSelectiveReason.DIRECTION_CONFLICT,)
        if any(
            item.decision is ProspectiveDecision.ABSTAIN
            for item in components.values()
        ):
            return (CausalSelectiveReason.C5_COMPONENT_ABSTAINED,)
        if c5.decision is not ProspectiveDecision.MATCHED:
            return (CausalSelectiveReason.C5_NOT_MATCHED,)
        directions = {item.expected_direction for item in components.values()}
        if len(directions) > 1:
            return (CausalSelectiveReason.DIRECTION_CONFLICT,)
        if directions != {c5.expected_direction} or c5.expected_direction == 0:
            return (CausalSelectiveReason.DIRECTION_UNAVAILABLE,)

        risks = (episode.h16v2, episode.h17v2)
        if any(item is None for item in risks):
            return (CausalSelectiveReason.RISK_EVIDENCE_MISSING,)
        assert episode.h16v2 is not None and episode.h17v2 is not None
        stale = any(
            (episode.context.observed_at - item.observed_at).total_seconds()
            > self._policy.maximum_risk_evidence_age_seconds
            for item in (episode.h16v2, episode.h17v2)
        )
        if stale:
            return (CausalSelectiveReason.RISK_EVIDENCE_STALE,)
        unavailable = any(
            item.decision is ProspectiveDecision.ABSTAIN
            for item in (episode.h16v2, episode.h17v2)
        )
        if unavailable:
            return (CausalSelectiveReason.RISK_EVIDENCE_UNAVAILABLE,)
        elevated: list[CausalSelectiveReason] = []
        if episode.h16v2.decision is ProspectiveDecision.MATCHED:
            elevated.append(CausalSelectiveReason.H16V2_ELEVATED_RISK)
        if episode.h17v2.decision is ProspectiveDecision.MATCHED:
            elevated.append(CausalSelectiveReason.H17V2_ELEVATED_RISK)
        return tuple(elevated)

    def _metrics(
        self,
        partition: DatasetPartition,
        *,
        ordered: Sequence[CausalSelectiveEpisode],
        decisions: Sequence[CausalSelectiveDecisionRecord],
        split: ChronologicalSplit,
    ) -> CausalSelectivePartitionMetrics:
        rows = tuple(
            (episode, decision)
            for episode, decision in zip(ordered, decisions, strict=True)
            if split.partition_for(episode.context.trading_day) is partition
        )
        acted = tuple(
            row
            for row in rows
            if row[1].decision is not CausalSelectiveDecision.ABSTAIN
        )
        resolved = tuple(
            row
            for row in acted
            if row[0].outcome is not None
            and row[0].outcome.available
            and row[0].outcome.net_directional_return_bps is not None
        )
        values = tuple(
            row[0].outcome.net_directional_return_bps
            for row in resolved
            if row[0].outcome is not None
            and row[0].outcome.net_directional_return_bps is not None
        )
        correct = sum(
            value > self._policy.success_threshold_bps for value in values
        )
        return CausalSelectivePartitionMetrics(
            partition=partition,
            observations=len(rows),
            acted_observations=len(acted),
            resolved_acted_outcomes=len(resolved),
            correct_acted_outcomes=correct,
            coverage=len(acted) / len(rows) if rows else 0.0,
            selective_accuracy=(correct / len(resolved) if resolved else None),
            mean_cost_adjusted_return_bps=(
                sum(values) / len(values) if values else None
            ),
        )


def _canonical_episodes(
    episodes: Sequence[CausalSelectiveEpisode],
) -> tuple[CausalSelectiveEpisode, ...]:
    ordered = tuple(
        sorted(
            episodes,
            key=lambda item: (
                item.context.trading_day,
                item.context.observed_at,
                item.context.primary_scope,
                item.context.horizon_seconds,
                item.context.episode_id,
            ),
        )
    )
    identities = tuple(item.context.episode_id for item in ordered)
    if len(identities) != len(set(identities)):
        raise ValueError("selective episode ids must be unique")
    return ordered


def _dataset_fingerprint(
    episodes: Sequence[CausalSelectiveEpisode],
    split: ChronologicalSplit,
) -> str:
    payload = {
        "split": {
            "train": [item.isoformat() for item in split.train_days],
            "validation": [item.isoformat() for item in split.validation_days],
            "holdout": [item.isoformat() for item in split.holdout_days],
        },
        "episodes": [
            {
                "episode_id": item.context.episode_id,
                "observed_at": item.context.observed_at.isoformat(),
                "horizon_seconds": item.context.horizon_seconds,
                "c5_payload": item.c5.payload_fingerprint if item.c5 else None,
                "h16v2": item.h16v2.observation_id if item.h16v2 else None,
                "h17v2": item.h17v2.observation_id if item.h17v2 else None,
                "outcome": (
                    {
                        "target_at": item.outcome.target_at.isoformat(),
                        "available": item.outcome.available,
                        "net_bps": item.outcome.net_directional_return_bps,
                        "cost_model_version": item.outcome.cost_model_version,
                    }
                    if item.outcome
                    else None
                ),
            }
            for item in episodes
        ],
    }
    return _fingerprint(payload)


def _report_fingerprint(
    policy_version: str,
    policy_fingerprint: str,
    dataset_fingerprint: str,
    cost_model_version: str | None,
    decisions: Sequence[CausalSelectiveDecisionRecord],
    metrics: Sequence[CausalSelectivePartitionMetrics],
) -> str:
    return _fingerprint(
        {
            "policy_version": policy_version,
            "policy": policy_fingerprint,
            "dataset": dataset_fingerprint,
            "cost_model_version": cost_model_version,
            "decisions": [
                {
                    "episode_id": item.episode_id,
                    "decision": item.decision.value,
                    "reason_codes": [reason.value for reason in item.reason_codes],
                    "confidence_examples": (
                        item.confidence.examples if item.confidence else None
                    ),
                    "confidence_lower": (
                        item.confidence.success_interval.lower
                        if item.confidence
                        else None
                    ),
                }
                for item in decisions
            ],
            "metrics": [
                {
                    "partition": item.partition.value,
                    "observations": item.observations,
                    "acted": item.acted_observations,
                    "resolved": item.resolved_acted_outcomes,
                    "correct": item.correct_acted_outcomes,
                    "coverage": item.coverage,
                    "selective_accuracy": item.selective_accuracy,
                    "mean_net_bps": item.mean_cost_adjusted_return_bps,
                }
                for item in metrics
            ],
            "product_claim_allowed": False,
            "automatic_execution_allowed": False,
        }
    )


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return f"sha256:{sha256(encoded.encode('utf-8')).hexdigest()}"
