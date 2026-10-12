"""Causal historical replay use case for preregistered H3 and H4."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from math import sqrt
from typing import Protocol, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildChronologicalSplit,
    BuildMatchedControls,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    EvidenceBundle,
    MatchedControlsResult,
    StudyPoint,
)
from tinvest_signal_engine.domain.jump_activity_replay import (
    CandleBar,
    ClassifiedJumpFeature,
    FeatureThresholds,
    HorizonOutcome,
    JumpHypothesis,
    JumpObservation,
    JumpReplayPolicy,
    RawJumpFeature,
)


MOSCOW = ZoneInfo("Europe/Moscow")


class CandleCachePort(Protocol):
    def fingerprint(self, tickers: Sequence[str] | None = None) -> str: ...

    def load(self, tickers: Sequence[str] | None = None) -> tuple[CandleBar, ...]: ...


class JumpReplayArtifactPort(Protocol):
    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None: ...

    def persist(self, result: "JumpReplayResult") -> str: ...


@dataclass(frozen=True)
class TrainingProfile:
    thresholds: FeatureThresholds
    volumes: tuple[float, ...]
    ranges_bps: tuple[float, ...]
    illiquidity: tuple[float, ...]


@dataclass(frozen=True)
class HorizonEvidence:
    hypothesis: JumpHypothesis
    horizon_seconds: int
    matched_controls: MatchedControlsResult
    bundle: EvidenceBundle

    @property
    def matched_mean_lift_bps(self) -> float | None:
        groups = self.matched_controls.groups
        if not groups:
            return None
        return sum(group.lift_bps for group in groups) / len(groups)

    @property
    def matched_positive_lift_rate(self) -> float | None:
        groups = self.matched_controls.groups
        if not groups:
            return None
        return sum(group.lift_bps > 0.0 for group in groups) / len(groups)


@dataclass(frozen=True)
class JumpReplayResult:
    run_id: str
    input_fingerprint: str
    policy_fingerprint: str
    policy: JumpReplayPolicy
    split: ChronologicalSplit
    thresholds: tuple[FeatureThresholds, ...]
    observations: tuple[JumpObservation, ...]
    evidence: tuple[HorizonEvidence, ...]
    candle_count: int
    raw_feature_count: int


@dataclass(frozen=True)
class JumpReplayExecution:
    run_id: str
    reused: bool
    artifact_uri: str
    result: JumpReplayResult | None


class RunJumpActivityReplay:
    """Run or reuse a deterministic H3/H4 replay from an immutable cache."""

    def __init__(
        self,
        *,
        candle_cache: CandleCachePort,
        artifacts: JumpReplayArtifactPort,
        evidence_policy: EvidenceGatePolicy = EvidenceGatePolicy(),
    ) -> None:
        self._candle_cache = candle_cache
        self._artifacts = artifacts
        self._evidence_policy = evidence_policy

    def execute(
        self,
        *,
        policy: JumpReplayPolicy = JumpReplayPolicy(),
        tickers: Sequence[str] | None = None,
    ) -> JumpReplayExecution:
        normalized_tickers = (
            tuple(sorted({ticker.strip().upper() for ticker in tickers if ticker.strip()}))
            if tickers
            else ()
        )
        input_fingerprint = self._candle_cache.fingerprint(normalized_tickers or None)
        policy_fingerprint = _policy_fingerprint(policy)
        run_id = _run_id(input_fingerprint, policy_fingerprint, normalized_tickers)
        if uri := self._artifacts.completed_uri(run_id, input_fingerprint):
            return JumpReplayExecution(
                run_id=run_id,
                reused=True,
                artifact_uri=uri,
                result=None,
            )

        candles = self._candle_cache.load(normalized_tickers or None)
        result = replay_jump_activity_hypotheses(
            candles,
            input_fingerprint=input_fingerprint,
            policy_fingerprint=policy_fingerprint,
            run_id=run_id,
            policy=policy,
            evidence_policy=self._evidence_policy,
        )
        uri = self._artifacts.persist(result)
        return JumpReplayExecution(
            run_id=run_id,
            reused=False,
            artifact_uri=uri,
            result=result,
        )


def replay_jump_activity_hypotheses(
    candles: Sequence[CandleBar],
    *,
    input_fingerprint: str,
    policy_fingerprint: str,
    run_id: str,
    policy: JumpReplayPolicy = JumpReplayPolicy(),
    evidence_policy: EvidenceGatePolicy = EvidenceGatePolicy(),
) -> JumpReplayResult:
    complete = tuple(
        sorted(
            (candle for candle in candles if candle.complete),
            key=lambda candle: (candle.ticker, candle.opened_at),
        )
    )
    if not complete:
        raise ValueError("replay requires at least one complete candle")
    identities = {(candle.ticker, candle.opened_at) for candle in complete}
    if len(identities) != len(complete):
        raise ValueError("candle cache contains duplicate ticker/timestamp rows")

    raw_features = build_raw_jump_features(complete, policy)
    trading_days = tuple(sorted({feature.trading_day for feature in raw_features}))
    split = BuildChronologicalSplit().execute(trading_days)
    profiles = build_training_profiles(raw_features, split, policy)
    classified = tuple(
        item
        for feature in raw_features
        if (item := classify_jump_feature(feature, profiles, policy)) is not None
    )
    bar_lookup = {candle.observed_at: candle for candle in complete}
    ticker_bar_lookup = {
        (candle.ticker, candle.observed_at): candle for candle in complete
    }
    observations = build_jump_observations(
        classified,
        ticker_bar_lookup,
        policy,
    )
    event_times = _event_times(observations)

    requests: list[EvidenceRequest] = []
    matched_by_test: dict[str, MatchedControlsResult] = {}
    evidence_identity: dict[str, tuple[JumpHypothesis, int]] = {}
    holdout_features = tuple(
        feature
        for feature in classified
        if split.partition_for(feature.raw.trading_day) is DatasetPartition.HOLDOUT
        and feature.raw.direction != 0
    )
    for hypothesis in JumpHypothesis:
        scenario_observations = tuple(
            observation
            for observation in observations
            if observation.hypothesis is hypothesis
            and split.partition_for(observation.feature.raw.trading_day)
            is DatasetPartition.HOLDOUT
        )
        for horizon in policy.horizons_seconds:
            events = tuple(
                _study_point(
                    observation.feature,
                    hypothesis=hypothesis,
                    horizon_seconds=horizon,
                    ticker_bar_lookup=ticker_bar_lookup,
                    split=split,
                    policy=policy,
                    event_times=event_times,
                    is_event=True,
                )
                for observation in scenario_observations
                if _outcome_for(observation, horizon).available
            )
            candidates = tuple(
                point
                for feature in holdout_features
                if (
                    point := _study_point(
                        feature,
                        hypothesis=hypothesis,
                        horizon_seconds=horizon,
                        ticker_bar_lookup=ticker_bar_lookup,
                        split=split,
                        policy=policy,
                        event_times=event_times,
                        is_event=False,
                    )
                )
                is not None
            )
            matched = _build_matched_controls_by_strata(
                tuple(point for point in events if point is not None),
                candidates,
                controls_per_event=evidence_policy.controls_per_event,
            )
            test_hypothesis_id = f"{hypothesis.value}-{horizon}s"
            request = EvidenceRequest(
                hypothesis_id=test_hypothesis_id,
                hypothesis_version="1.0.0",
                dataset_fingerprint=input_fingerprint,
                groups=matched.groups,
                expected_eligible_events=len(events),
                unmatched_event_ids=matched.unmatched_event_ids,
            )
            requests.append(request)
            matched_by_test[request.test_id] = matched
            evidence_identity[request.test_id] = (hypothesis, horizon)

    bundles = AssessEvidencePortfolio(evidence_policy).execute(requests)
    evidence = tuple(
        HorizonEvidence(
            hypothesis=evidence_identity[request.test_id][0],
            horizon_seconds=evidence_identity[request.test_id][1],
            matched_controls=matched_by_test[request.test_id],
            bundle=bundle,
        )
        for request, bundle in zip(requests, bundles, strict=True)
    )
    return JumpReplayResult(
        run_id=run_id,
        input_fingerprint=input_fingerprint,
        policy_fingerprint=policy_fingerprint,
        policy=policy,
        split=split,
        thresholds=tuple(
            sorted(
                (profile.thresholds for profile in profiles.values()),
                key=lambda item: (item.ticker, item.session_bucket),
            )
        ),
        observations=observations,
        evidence=evidence,
        candle_count=len(complete),
        raw_feature_count=len(raw_features),
    )


def build_raw_jump_features(
    candles: Sequence[CandleBar],
    policy: JumpReplayPolicy,
) -> tuple[RawJumpFeature, ...]:
    by_ticker_day: dict[tuple[str, date], list[CandleBar]] = defaultdict(list)
    for candle in candles:
        local_day = candle.opened_at.astimezone(MOSCOW).date()
        by_ticker_day[(candle.ticker, local_day)].append(candle)
    features: list[RawJumpFeature] = []
    history = policy.history_window_minutes
    for (ticker, trading_day), unsorted in sorted(by_ticker_day.items()):
        bars = sorted(unsorted, key=lambda item: item.opened_at)
        for index in range(max(history, policy.volatility_window_minutes), len(bars)):
            current = bars[index]
            base = bars[index - history]
            feature_bars = bars[index - history + 1 : index + 1]
            if not _continuous((base, *feature_bars)):
                continue
            prior_start = max(1, index - policy.volatility_window_minutes)
            prior_returns = [
                _return_bps(bars[position - 1].close_price, bars[position].close_price)
                for position in range(prior_start, index)
                if bars[position].opened_at - bars[position - 1].opened_at
                == timedelta(minutes=1)
            ]
            if len(prior_returns) < policy.volatility_window_minutes - 1:
                continue
            movement = _return_bps(base.close_price, current.close_price)
            volume = sum(bar.volume for bar in feature_bars)
            high = max(bar.high_price for bar in feature_bars)
            low = min(bar.low_price for bar in feature_bars)
            range_bps = _return_bps(low, high)
            turnover = sum(bar.close_price * bar.volume for bar in feature_bars)
            illiquidity = abs(movement) / max(turnover, 1.0) * 1_000_000_000.0
            prior_mean = sum(prior_returns) / len(prior_returns)
            prior_volatility = sqrt(
                sum((value - prior_mean) ** 2 for value in prior_returns)
                / len(prior_returns)
            )
            observed_at = current.observed_at
            feature_id = _stable_id(
                "jump-feature",
                ticker,
                observed_at.isoformat(),
                policy.version,
            )
            features.append(
                RawJumpFeature(
                    feature_id=feature_id,
                    ticker=ticker,
                    observed_at=observed_at,
                    trading_day=trading_day,
                    session_bucket=_session_bucket(current.opened_at),
                    anchor_price=current.close_price,
                    five_minute_return_bps=movement,
                    absolute_return_bps=abs(movement),
                    five_minute_volume=volume,
                    five_minute_range_bps=range_bps,
                    illiquidity_proxy=illiquidity,
                    prior_volatility_bps=prior_volatility,
                    feature_max_observed_at=observed_at,
                )
            )
    return tuple(sorted(features, key=lambda item: (item.ticker, item.observed_at)))


def build_training_profiles(
    features: Sequence[RawJumpFeature],
    split: ChronologicalSplit,
    policy: JumpReplayPolicy,
) -> dict[tuple[str, str], TrainingProfile]:
    grouped: dict[tuple[str, str], list[RawJumpFeature]] = defaultdict(list)
    for feature in features:
        if split.partition_for(feature.trading_day) is DatasetPartition.TRAIN:
            grouped[(feature.ticker, feature.session_bucket)].append(feature)
    profiles: dict[tuple[str, str], TrainingProfile] = {}
    for key, samples in sorted(grouped.items()):
        if len(samples) < policy.minimum_training_observations:
            continue
        absolute_returns = tuple(sorted(item.absolute_return_bps for item in samples))
        volumes = tuple(sorted(item.five_minute_volume for item in samples))
        ranges = tuple(sorted(item.five_minute_range_bps for item in samples))
        illiquidity = tuple(sorted(item.illiquidity_proxy for item in samples))
        volatility = tuple(sorted(item.prior_volatility_bps for item in samples))
        profiles[key] = TrainingProfile(
            thresholds=FeatureThresholds(
                ticker=key[0],
                session_bucket=key[1],
                training_observations=len(samples),
                jump_absolute_return_bps=_quantile(
                    absolute_returns, policy.jump_quantile
                ),
                median_volume=_quantile(volumes, policy.low_volume_quantile),
                high_volume=_quantile(volumes, policy.high_activity_quantile),
                high_range_bps=_quantile(ranges, policy.high_activity_quantile),
                high_illiquidity=_quantile(
                    illiquidity, policy.high_illiquidity_quantile
                ),
                volatility_low_bps=_quantile(volatility, 1.0 / 3.0),
                volatility_high_bps=_quantile(volatility, 2.0 / 3.0),
            ),
            volumes=volumes,
            ranges_bps=ranges,
            illiquidity=illiquidity,
        )
    if not profiles:
        raise ValueError("training split has no feature strata with enough observations")
    return profiles


def classify_jump_feature(
    feature: RawJumpFeature,
    profiles: dict[tuple[str, str], TrainingProfile],
    policy: JumpReplayPolicy,
) -> ClassifiedJumpFeature | None:
    profile = profiles.get((feature.ticker, feature.session_bucket))
    if profile is None:
        return None
    thresholds = profile.thresholds
    volume_percentile = _percentile_rank(profile.volumes, feature.five_minute_volume)
    range_percentile = _percentile_rank(
        profile.ranges_bps, feature.five_minute_range_bps
    )
    illiquidity_percentile = _percentile_rank(
        profile.illiquidity, feature.illiquidity_proxy
    )
    hypothesis: JumpHypothesis | None = None
    jump = feature.absolute_return_bps >= thresholds.jump_absolute_return_bps
    if (
        jump
        and volume_percentile < policy.low_volume_quantile
        and illiquidity_percentile >= policy.high_illiquidity_quantile
    ):
        hypothesis = JumpHypothesis.LOW_ACTIVITY_REVERSAL
    elif (
        jump
        and volume_percentile >= policy.high_activity_quantile
        and range_percentile >= policy.high_activity_quantile
    ):
        hypothesis = JumpHypothesis.HIGH_ACTIVITY_CONTINUATION
    volatility_bucket = (
        "low"
        if feature.prior_volatility_bps <= thresholds.volatility_low_bps
        else "high"
        if feature.prior_volatility_bps >= thresholds.volatility_high_bps
        else "medium"
    )
    liquidity_bucket = (
        "low"
        if illiquidity_percentile >= 2.0 / 3.0
        else "high"
        if illiquidity_percentile <= 1.0 / 3.0
        else "medium"
    )
    return ClassifiedJumpFeature(
        raw=feature,
        thresholds=thresholds,
        volume_ratio=feature.five_minute_volume / max(thresholds.median_volume, 1.0),
        volume_percentile=volume_percentile,
        range_percentile=range_percentile,
        illiquidity_percentile=illiquidity_percentile,
        volatility_bucket=volatility_bucket,
        liquidity_bucket=liquidity_bucket,
        hypothesis=hypothesis,
    )


def build_jump_observations(
    features: Sequence[ClassifiedJumpFeature],
    ticker_bar_lookup: dict[tuple[str, datetime], CandleBar],
    policy: JumpReplayPolicy,
) -> tuple[JumpObservation, ...]:
    observations: list[JumpObservation] = []
    last_event: dict[tuple[str, JumpHypothesis], datetime] = {}
    for feature in sorted(features, key=lambda item: (item.raw.observed_at, item.raw.ticker)):
        if feature.hypothesis is None or feature.raw.direction == 0:
            continue
        key = (feature.raw.ticker, feature.hypothesis)
        previous = last_event.get(key)
        if previous is not None and feature.raw.observed_at - previous <= timedelta(
            minutes=policy.event_cooldown_minutes
        ):
            continue
        last_event[key] = feature.raw.observed_at
        outcomes = tuple(
            _calculate_outcome(
                feature,
                hypothesis=feature.hypothesis,
                horizon_seconds=horizon,
                ticker_bar_lookup=ticker_bar_lookup,
                policy=policy,
            )
            for horizon in policy.horizons_seconds
        )
        observations.append(
            JumpObservation(
                observation_id=_stable_id(
                    "jump-observation",
                    feature.hypothesis.value,
                    feature.raw.feature_id,
                    policy.version,
                ),
                hypothesis=feature.hypothesis,
                feature=feature,
                outcomes=outcomes,
            )
        )
    return tuple(
        sorted(
            observations,
            key=lambda item: (
                item.feature.raw.observed_at,
                item.feature.raw.ticker,
                item.hypothesis.value,
            ),
        )
    )


def _study_point(
    feature: ClassifiedJumpFeature,
    *,
    hypothesis: JumpHypothesis,
    horizon_seconds: int,
    ticker_bar_lookup: dict[tuple[str, datetime], CandleBar],
    split: ChronologicalSplit,
    policy: JumpReplayPolicy,
    event_times: dict[tuple[str, JumpHypothesis], tuple[datetime, ...]],
    is_event: bool,
) -> StudyPoint | None:
    outcome = _calculate_outcome(
        feature,
        hypothesis=hypothesis,
        horizon_seconds=horizon_seconds,
        ticker_bar_lookup=ticker_bar_lookup,
        policy=policy,
    )
    if not outcome.available or outcome.net_effect_bps is None:
        return None
    nearby = tuple(
        scenario.value
        for scenario in JumpHypothesis
        if _has_nearby(
            event_times.get((feature.raw.ticker, scenario), ()),
            feature.raw.observed_at,
            timedelta(minutes=5),
        )
    )
    return StudyPoint(
        point_id=_stable_id(
            "event" if is_event else "control-candidate",
            hypothesis.value,
            str(horizon_seconds),
            feature.raw.feature_id,
            policy.version,
        ),
        scenario_id=hypothesis.value if is_event else None,
        instrument_id=feature.raw.ticker,
        occurred_at=feature.raw.observed_at,
        trading_day=feature.raw.trading_day,
        session_bucket=feature.raw.session_bucket,
        volatility_bucket=feature.volatility_bucket,
        liquidity_bucket=feature.liquidity_bucket,
        features_observed_at=feature.raw.feature_max_observed_at,
        partition=split.partition_for(feature.raw.trading_day),
        net_effect_bps=outcome.net_effect_bps,
        cost_model_version=policy.cost_model.version,
        nearby_scenario_ids=nearby,
    )


def _calculate_outcome(
    feature: ClassifiedJumpFeature,
    *,
    hypothesis: JumpHypothesis,
    horizon_seconds: int,
    ticker_bar_lookup: dict[tuple[str, datetime], CandleBar],
    policy: JumpReplayPolicy,
) -> HorizonOutcome:
    target_at = feature.raw.observed_at + timedelta(seconds=horizon_seconds)
    expected_direction = (
        -feature.raw.direction
        if hypothesis is JumpHypothesis.LOW_ACTIVITY_REVERSAL
        else feature.raw.direction
    )
    target = ticker_bar_lookup.get((feature.raw.ticker, target_at))
    if target is None or target.opened_at.astimezone(MOSCOW).date() != feature.raw.trading_day:
        return HorizonOutcome(
            horizon_seconds=horizon_seconds,
            target_observed_at=target_at,
            available=False,
            reason_code="missing_exact_forward_candle",
            forward_return_bps=None,
            expected_direction=expected_direction,
            net_effect_bps=None,
            cost_model_version=policy.cost_model.version,
        )
    minute_count = horizon_seconds // 60
    for minute in range(1, minute_count + 1):
        observed_at = feature.raw.observed_at + timedelta(minutes=minute)
        if (feature.raw.ticker, observed_at) not in ticker_bar_lookup:
            return HorizonOutcome(
                horizon_seconds=horizon_seconds,
                target_observed_at=target_at,
                available=False,
                reason_code="trading_gap_in_horizon",
                forward_return_bps=None,
                expected_direction=expected_direction,
                net_effect_bps=None,
                cost_model_version=policy.cost_model.version,
            )
    forward = _return_bps(feature.raw.anchor_price, target.close_price)
    return HorizonOutcome(
        horizon_seconds=horizon_seconds,
        target_observed_at=target_at,
        available=True,
        reason_code="ready",
        forward_return_bps=forward,
        expected_direction=expected_direction,
        net_effect_bps=expected_direction * forward - policy.cost_model.round_trip_bps,
        cost_model_version=policy.cost_model.version,
    )


def _outcome_for(observation: JumpObservation, horizon: int) -> HorizonOutcome:
    return next(item for item in observation.outcomes if item.horizon_seconds == horizon)


def _event_times(
    observations: Sequence[JumpObservation],
) -> dict[tuple[str, JumpHypothesis], tuple[datetime, ...]]:
    grouped: dict[tuple[str, JumpHypothesis], list[datetime]] = defaultdict(list)
    for observation in observations:
        grouped[(observation.feature.raw.ticker, observation.hypothesis)].append(
            observation.feature.raw.observed_at
        )
    return {key: tuple(sorted(values)) for key, values in grouped.items()}


def _has_nearby(
    times: Sequence[datetime],
    at: datetime,
    window: timedelta,
) -> bool:
    index = bisect_left(times, at - window)
    return index < len(times) and times[index] <= at + window


def _continuous(candles: Sequence[CandleBar]) -> bool:
    return all(
        later.opened_at - earlier.opened_at == timedelta(minutes=1)
        for earlier, later in zip(candles, candles[1:])
    )


def _return_bps(start: float, end: float) -> float:
    return (end / start - 1.0) * 10_000.0


def _session_bucket(at: datetime) -> str:
    local = at.astimezone(MOSCOW)
    return f"{local.hour:02d}:00-{local.hour:02d}:59"


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("quantile requires observations")
    position = (len(sorted_values) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def _percentile_rank(sorted_values: Sequence[float], value: float) -> float:
    if not sorted_values:
        raise ValueError("percentile rank requires observations")
    left = bisect_left(sorted_values, value)
    right = bisect_right(sorted_values, value)
    return ((left + right) / 2.0) / len(sorted_values)


def _stable_id(*parts: str) -> str:
    encoded = "\0".join(parts).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"


def _build_matched_controls_by_strata(
    events: Sequence[StudyPoint],
    candidates: Sequence[StudyPoint],
    *,
    controls_per_event: int,
) -> MatchedControlsResult:
    """Preserve exact matching while avoiding a full pool scan per event."""

    events_by_key: dict[tuple[object, ...], list[StudyPoint]] = defaultdict(list)
    candidates_by_key: dict[tuple[object, ...], list[StudyPoint]] = defaultdict(list)
    for event in events:
        events_by_key[event.matching_key].append(event)
    for candidate in candidates:
        candidates_by_key[candidate.matching_key].append(candidate)
    groups = []
    unmatched: list[str] = []
    builder = BuildMatchedControls(controls_per_event=controls_per_event)
    for key in sorted(events_by_key, key=repr):
        result = builder.execute(
            events_by_key[key],
            candidates_by_key.get(key, ()),
        )
        groups.extend(result.groups)
        unmatched.extend(result.unmatched_event_ids)
    return MatchedControlsResult(
        groups=tuple(
            sorted(groups, key=lambda group: (group.event.occurred_at, group.event.point_id))
        ),
        unmatched_event_ids=tuple(sorted(unmatched)),
        controls_per_event=controls_per_event,
    )


def _policy_fingerprint(policy: JumpReplayPolicy) -> str:
    payload = {
        "version": policy.version,
        "history_window_minutes": policy.history_window_minutes,
        "volatility_window_minutes": policy.volatility_window_minutes,
        "event_cooldown_minutes": policy.event_cooldown_minutes,
        "minimum_training_observations": policy.minimum_training_observations,
        "jump_quantile": policy.jump_quantile,
        "low_volume_quantile": policy.low_volume_quantile,
        "high_activity_quantile": policy.high_activity_quantile,
        "high_illiquidity_quantile": policy.high_illiquidity_quantile,
        "horizons_seconds": policy.horizons_seconds,
        "cost_model": {
            "version": policy.cost_model.version,
            "round_trip_bps": policy.cost_model.round_trip_bps,
        },
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{sha256(encoded).hexdigest()}"


def _run_id(
    input_fingerprint: str,
    policy_fingerprint: str,
    tickers: Sequence[str],
) -> str:
    encoded = json.dumps(
        {
            "input": input_fingerprint,
            "policy": policy_fingerprint,
            "tickers": tuple(tickers),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"h3-h4-{sha256(encoded).hexdigest()[:16]}"
