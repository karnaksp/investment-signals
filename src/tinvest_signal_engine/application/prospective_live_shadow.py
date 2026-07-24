"""Application use cases for the six-model prospective live-shadow portfolio."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol

from tinvest_signal_engine.domain.prospective_live_shadow import (
    ProspectiveLiveObservation,
    ProspectiveLiveOutcome,
    ProspectiveLiveShadowStatistics,
    aggregate_live_shadow_statistics,
    build_live_observation,
    build_live_outcome,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    HarV2Parameters,
    JumpHistoryPoint,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveScientificPolicy,
    TargetMetric,
    directional_outcome,
    downside_semivariance_feature,
    har_v2_feature,
    har_v2_outcome,
    jump_regime_features,
    relative_volume_volatility_feature,
    variance_uplift_outcome,
    volatility_jump_feature,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
)


DEFAULT_LIVE_OUTCOME_POLICY_VERSION = "prospective-live-outcomes-v2"
DEFAULT_LIVE_OUTCOME_CONFIRMATION_INTERVAL = timedelta(minutes=1)
DEFAULT_LIVE_OUTCOME_RETRY_TIMEOUT = timedelta(hours=1)
LIVE_SHADOW_HYPOTHESES = frozenset(
    {
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3,
        ProspectiveHypothesis.HAR_VOLATILITY_V2,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK,
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE,
    }
)


class ProspectiveLiveShadowStore(Protocol):
    def existing_observation_ids(
        self, observation_ids: tuple[str, ...]
    ) -> frozenset[str]: ...

    def persist_observation(
        self, observation: ProspectiveLiveObservation
    ) -> PersistenceDisposition: ...

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveLiveObservation, ...]: ...

    def persist_outcome(
        self, outcome: ProspectiveLiveOutcome
    ) -> PersistenceDisposition: ...

    def observations(self) -> tuple[ProspectiveLiveObservation, ...]: ...

    def outcomes(
        self, *, outcome_policy_version: str
    ) -> tuple[ProspectiveLiveOutcome, ...]: ...


class ProspectiveLiveOutcomeSource(Protocol):
    def load(
        self,
        observation: ProspectiveLiveObservation,
        *,
        as_of: datetime,
    ) -> "ProspectiveLiveOutcomeEvidence": ...


@dataclass(frozen=True, slots=True)
class JumpFeatureInput:
    signed_return_bps: float
    volume: float
    range_bps: float
    illiquidity: float
    prior_history: tuple[JumpHistoryPoint, ...]
    history_observed_until: datetime | None


@dataclass(frozen=True, slots=True)
class RelativeVolumeFeatureInput:
    current_volume: float
    historical_volumes: tuple[float, ...]
    baseline_future_variance: float
    history_observed_until: datetime | None


@dataclass(frozen=True, slots=True)
class HarFeatureInput:
    short_variance: float
    medium_variance: float
    long_variance: float
    parameters: HarV2Parameters | None


@dataclass(frozen=True, slots=True)
class SemivarianceFeatureInput:
    downside_share: float
    historical_downside_shares: tuple[float, ...]
    baseline_future_variance: float
    history_observed_until: datetime | None


@dataclass(frozen=True, slots=True)
class VolatilityJumpFeatureInput:
    jump_share: float
    continuous_variance: float
    historical_jump_shares: tuple[float, ...]
    baseline_future_variance: float
    history_observed_until: datetime | None


@dataclass(frozen=True, slots=True)
class ProspectivePortfolioSnapshot:
    instrument_id: str
    ticker: str
    trading_day: date
    observed_at: datetime
    recorded_at: datetime
    source_event_ids: tuple[str, ...]
    dataset_fingerprint: str
    input_fingerprint: str
    trading_gap: bool
    jump: JumpFeatureInput
    relative_volume: RelativeVolumeFeatureInput
    har: HarFeatureInput
    semivariance: SemivarianceFeatureInput
    volatility_jump: VolatilityJumpFeatureInput

    def __post_init__(self) -> None:
        for name, value in (
            ("observed_at", self.observed_at),
            ("recorded_at", self.recorded_at),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.recorded_at < self.observed_at:
            raise ValueError("recorded_at must not precede observed_at")
        if not self.instrument_id.strip() or not self.ticker.strip():
            raise ValueError("instrument_id and ticker are required")


@dataclass(frozen=True, slots=True)
class ProspectiveLiveOutcomeEvidence:
    observation_id: str
    target_at: datetime
    available: bool
    actual_value: float | None
    evidence_fingerprint: str
    ewma_baseline: float | None = None
    phase_baseline: float | None = None

    def __post_init__(self) -> None:
        if self.target_at.tzinfo is None or self.target_at.utcoffset() is None:
            raise ValueError("target_at must be timezone-aware")
        if self.available and self.actual_value is None:
            raise ValueError("available outcome evidence requires actual_value")
        if not self.available and self.actual_value is not None:
            raise ValueError("unavailable outcome evidence cannot contain actual_value")


@dataclass(frozen=True, slots=True)
class ProspectiveLiveShadowEvent:
    event_type: str
    emitted_at: datetime
    observation_ids: tuple[str, ...]
    outcome_ids: tuple[str, ...]
    statistics: ProspectiveLiveShadowStatistics

    def __post_init__(self) -> None:
        if self.event_type != "prospective_live_shadow_updated":
            raise ValueError("unsupported prospective live-shadow event")
        if self.emitted_at.tzinfo is None or self.emitted_at.utcoffset() is None:
            raise ValueError("live-shadow emitted_at must be timezone-aware")
        if self.statistics.generated_at != self.emitted_at:
            raise ValueError("live-shadow event and statistics timestamps must match")


@dataclass(frozen=True, slots=True)
class ProspectivePortfolioIngestResult:
    stored: int
    replayed: int
    observation_ids: tuple[str, ...]
    event: ProspectiveLiveShadowEvent


@dataclass(frozen=True, slots=True)
class ProspectiveLiveOutcomeBatchResult:
    scanned: int
    stored: int
    replayed: int
    pending: int
    unavailable: int
    outcome_ids: tuple[str, ...]
    event: ProspectiveLiveShadowEvent


class RecordProspectivePortfolioSnapshot:
    """Build the full portfolio once and seal every decision, including abstentions."""

    def __init__(
        self,
        *,
        store: ProspectiveLiveShadowStore,
        policy: ProspectiveScientificPolicy = ProspectiveScientificPolicy(),
        outcome_policy_version: str = DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
    ) -> None:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        self._store = store
        self._policy = policy
        self._outcome_policy_version = outcome_policy_version

    def execute(
        self, snapshot: ProspectivePortfolioSnapshot
    ) -> ProspectivePortfolioIngestResult:
        features = _portfolio_features(snapshot, self._policy)
        observations = tuple(
            build_live_observation(
                instrument_id=snapshot.instrument_id,
                policy_version=self._policy.version,
                feature=feature,
                source_event_ids=snapshot.source_event_ids,
                dataset_fingerprint=snapshot.dataset_fingerprint,
                input_fingerprint=snapshot.input_fingerprint,
                recorded_at=snapshot.recorded_at,
            )
            for feature in features
        )
        existing_ids = self._store.existing_observation_ids(
            tuple(item.observation_id for item in observations)
        )
        stored = replayed = 0
        observation_ids: list[str] = []
        for observation in observations:
            if observation.observation_id in existing_ids:
                replayed += 1
                observation_ids.append(observation.observation_id)
                continue
            disposition = self._store.persist_observation(observation)
            if disposition is PersistenceDisposition.INSERTED:
                stored += 1
            else:
                replayed += 1
            observation_ids.append(observation.observation_id)
        event = _event(
            store=self._store,
            emitted_at=snapshot.recorded_at,
            observation_ids=tuple(observation_ids),
            outcome_ids=(),
            outcome_policy_version=self._outcome_policy_version,
        )
        return ProspectivePortfolioIngestResult(
            stored=stored,
            replayed=replayed,
            observation_ids=tuple(observation_ids),
            event=event,
        )


class ProcessProspectiveLiveOutcomes:
    """Accumulate immutable outcomes only after their sealed horizons mature."""

    def __init__(
        self,
        *,
        store: ProspectiveLiveShadowStore,
        source: ProspectiveLiveOutcomeSource,
        policy: ProspectiveScientificPolicy = ProspectiveScientificPolicy(),
        outcome_policy_version: str = DEFAULT_LIVE_OUTCOME_POLICY_VERSION,
        unavailable_confirmation_interval: timedelta = (
            DEFAULT_LIVE_OUTCOME_CONFIRMATION_INTERVAL
        ),
        unavailable_retry_timeout: timedelta = DEFAULT_LIVE_OUTCOME_RETRY_TIMEOUT,
    ) -> None:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if unavailable_confirmation_interval <= timedelta(0):
            raise ValueError(
                "unavailable_confirmation_interval must be positive"
            )
        if unavailable_retry_timeout < unavailable_confirmation_interval:
            raise ValueError(
                "unavailable_retry_timeout must not be below confirmation interval"
            )
        self._store = store
        self._source = source
        self._policy = policy
        self._outcome_policy_version = outcome_policy_version
        self._unavailable_confirmation_interval = (
            unavailable_confirmation_interval
        )
        self._unavailable_retry_timeout = unavailable_retry_timeout
        self._unavailable_candidates: dict[str, tuple[str, datetime]] = {}

    def run_once(
        self, *, now: datetime, limit: int = 100
    ) -> ProspectiveLiveOutcomeBatchResult:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if limit <= 0:
            raise ValueError("limit must be positive")
        candidates = self._store.pending_observations(
            outcome_policy_version=self._outcome_policy_version,
            limit=limit,
        )
        stored = replayed = pending = unavailable = 0
        outcome_ids: list[str] = []
        for observation in candidates:
            if now < observation.target_at:
                pending += 1
                continue
            evidence = self._source.load(observation, as_of=now)
            if evidence.observation_id != observation.observation_id:
                raise ValueError("outcome evidence belongs to a different observation")
            if evidence.target_at != observation.target_at:
                raise ValueError("outcome evidence target differs from observation")
            if not evidence.available:
                deadline = observation.target_at + self._unavailable_retry_timeout
                candidate = self._unavailable_candidates.get(
                    observation.observation_id
                )
                if now < deadline:
                    if (
                        candidate is None
                        or candidate[0] != evidence.evidence_fingerprint
                    ):
                        self._unavailable_candidates[observation.observation_id] = (
                            evidence.evidence_fingerprint,
                            now,
                        )
                        pending += 1
                        continue
                    if (
                        now
                        < candidate[1] + self._unavailable_confirmation_interval
                    ):
                        pending += 1
                        continue
            self._unavailable_candidates.pop(observation.observation_id, None)
            outcome = _evaluate_outcome(
                observation.feature,
                evidence,
                policy=self._policy,
            )
            record = build_live_outcome(
                observation=observation,
                outcome=outcome,
                outcome_policy_version=self._outcome_policy_version,
                evidence_fingerprint=evidence.evidence_fingerprint,
                evaluated_at=now,
            )
            disposition = self._store.persist_outcome(record)
            if disposition is PersistenceDisposition.INSERTED:
                stored += 1
            else:
                replayed += 1
            unavailable += int(not outcome.available)
            outcome_ids.append(record.outcome_id)
        event = _event(
            store=self._store,
            emitted_at=now,
            observation_ids=(),
            outcome_ids=tuple(outcome_ids),
            outcome_policy_version=self._outcome_policy_version,
        )
        return ProspectiveLiveOutcomeBatchResult(
            scanned=len(candidates),
            stored=stored,
            replayed=replayed,
            pending=pending,
            unavailable=unavailable,
            outcome_ids=tuple(outcome_ids),
            event=event,
        )


def _portfolio_features(
    snapshot: ProspectivePortfolioSnapshot,
    policy: ProspectiveScientificPolicy,
) -> tuple[ProspectiveFeature, ...]:
    features: list[ProspectiveFeature] = []
    for horizon in policy.jump_horizons_seconds:
        features.extend(
            jump_regime_features(
                ticker=snapshot.ticker,
                trading_day=snapshot.trading_day,
                observed_at=snapshot.observed_at,
                horizon_seconds=horizon,
                signed_return_bps=snapshot.jump.signed_return_bps,
                volume=snapshot.jump.volume,
                range_bps=snapshot.jump.range_bps,
                illiquidity=snapshot.jump.illiquidity,
                prior_history=snapshot.jump.prior_history,
                history_observed_until=snapshot.jump.history_observed_until,
                trading_gap=snapshot.trading_gap,
                policy=policy,
            )
        )
    features.extend(
        (
            relative_volume_volatility_feature(
                ticker=snapshot.ticker,
                trading_day=snapshot.trading_day,
                observed_at=snapshot.observed_at,
                current_volume=snapshot.relative_volume.current_volume,
                historical_volumes=snapshot.relative_volume.historical_volumes,
                baseline_future_variance=(
                    snapshot.relative_volume.baseline_future_variance
                ),
                history_observed_until=(
                    snapshot.relative_volume.history_observed_until
                ),
                trading_gap=snapshot.trading_gap,
                policy=policy,
            ),
            har_v2_feature(
                ticker=snapshot.ticker,
                trading_day=snapshot.trading_day,
                observed_at=snapshot.observed_at,
                short_variance=snapshot.har.short_variance,
                medium_variance=snapshot.har.medium_variance,
                long_variance=snapshot.har.long_variance,
                parameters=snapshot.har.parameters,
                horizon_seconds=policy.har_horizon_seconds,
            ),
            downside_semivariance_feature(
                ticker=snapshot.ticker,
                trading_day=snapshot.trading_day,
                observed_at=snapshot.observed_at,
                downside_share=snapshot.semivariance.downside_share,
                historical_downside_shares=(
                    snapshot.semivariance.historical_downside_shares
                ),
                baseline_future_variance=(
                    snapshot.semivariance.baseline_future_variance
                ),
                history_observed_until=(snapshot.semivariance.history_observed_until),
                trading_gap=snapshot.trading_gap,
                policy=policy,
            ),
            volatility_jump_feature(
                ticker=snapshot.ticker,
                trading_day=snapshot.trading_day,
                observed_at=snapshot.observed_at,
                jump_share=snapshot.volatility_jump.jump_share,
                continuous_variance=snapshot.volatility_jump.continuous_variance,
                historical_jump_shares=(
                    snapshot.volatility_jump.historical_jump_shares
                ),
                baseline_future_variance=(
                    snapshot.volatility_jump.baseline_future_variance
                ),
                history_observed_until=(
                    snapshot.volatility_jump.history_observed_until
                ),
                trading_gap=snapshot.trading_gap,
                policy=policy,
            ),
        )
    )
    return tuple(
        sorted(
            features,
            key=lambda item: (item.hypothesis.value, item.horizon_seconds),
        )
    )


def _evaluate_outcome(
    feature: ProspectiveFeature,
    evidence: ProspectiveLiveOutcomeEvidence,
    *,
    policy: ProspectiveScientificPolicy,
):
    actual = evidence.actual_value if evidence.available else None
    if feature.target is TargetMetric.FORWARD_RETURN:
        return directional_outcome(
            feature,
            target_at=evidence.target_at,
            forward_return_bps=actual,
            round_trip_cost_bps=policy.round_trip_cost_bps,
        )
    if feature.target is TargetMetric.FUTURE_VARIANCE_UPLIFT:
        return variance_uplift_outcome(
            feature,
            target_at=evidence.target_at,
            actual_future_variance=actual,
        )
    return har_v2_outcome(
        feature,
        target_at=evidence.target_at,
        actual_future_variance=actual,
        ewma_baseline=evidence.ewma_baseline,
        phase_baseline=evidence.phase_baseline,
    )


def _event(
    *,
    store: ProspectiveLiveShadowStore,
    emitted_at: datetime,
    observation_ids: tuple[str, ...],
    outcome_ids: tuple[str, ...],
    outcome_policy_version: str,
) -> ProspectiveLiveShadowEvent:
    return ProspectiveLiveShadowEvent(
        event_type="prospective_live_shadow_updated",
        emitted_at=emitted_at,
        observation_ids=observation_ids,
        outcome_ids=outcome_ids,
        statistics=aggregate_live_shadow_statistics(
            store.observations(),
            store.outcomes(outcome_policy_version=outcome_policy_version),
            generated_at=emitted_at,
        ),
    )
