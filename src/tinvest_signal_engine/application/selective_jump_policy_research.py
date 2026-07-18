"""H3/H4 composition-neutral application workflow for selective research."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from math import log1p
from typing import Protocol, Sequence

from tinvest_signal_engine.application.jump_activity_replay import (
    CandleCachePort,
    build_jump_observations,
    build_raw_jump_features,
    build_training_profiles,
    classify_jump_feature,
)
from tinvest_signal_engine.application.hypothesis_evidence import BuildChronologicalSplit
from tinvest_signal_engine.application.selective_hypothesis_policy import (
    ProbabilityEstimatorFactory,
    ResearchSelectiveHypothesisPolicy,
    SelectivePortfolioExecution,
    SelectivePortfolioResult,
    SelectiveResearchArtifactPort,
    selective_policy_fingerprint,
)
from tinvest_signal_engine.domain.jump_activity_replay import (
    CandleBar,
    JumpHypothesis,
    JumpReplayPolicy,
)
from tinvest_signal_engine.domain.selective_hypothesis_policy import (
    SelectiveExample,
    SelectiveResearchPolicy,
)


IMPLEMENTATION_VERSION = "selective-h3-h4-research-v1.0.0"


class SelectiveCandleCachePort(CandleCachePort, Protocol):
    pass


@dataclass(frozen=True)
class SelectiveJumpDataset:
    examples: tuple[SelectiveExample, ...]
    candle_count: int
    raw_feature_count: int
    classified_feature_count: int


class RunSelectiveJumpPolicyResearch:
    """Run or reuse six H3/H4 × horizon studies without broker access."""

    def __init__(
        self,
        *,
        candle_cache: SelectiveCandleCachePort,
        artifacts: SelectiveResearchArtifactPort,
        estimator_factory: ProbabilityEstimatorFactory,
    ) -> None:
        self._candle_cache = candle_cache
        self._artifacts = artifacts
        self._estimators = estimator_factory

    def execute(
        self,
        *,
        replay_policy: JumpReplayPolicy = JumpReplayPolicy(),
        research_policy: SelectiveResearchPolicy = SelectiveResearchPolicy(),
        tickers: Sequence[str] | None = None,
    ) -> SelectivePortfolioExecution:
        normalized = (
            tuple(sorted({item.strip().upper() for item in tickers if item.strip()}))
            if tickers
            else ()
        )
        input_fingerprint = self._candle_cache.fingerprint(normalized or None)
        policy_fingerprint = _combined_policy_fingerprint(
            replay_policy, research_policy
        )
        run_id = _run_id(input_fingerprint, policy_fingerprint, normalized)
        if uri := self._artifacts.completed_uri(run_id, input_fingerprint):
            return SelectivePortfolioExecution(
                run_id=run_id,
                reused=True,
                artifact_uri=uri,
                result=None,
            )
        candles = self._candle_cache.load(normalized or None)
        dataset = build_selective_jump_dataset(candles, replay_policy)
        grouped: dict[tuple[str, int], list[SelectiveExample]] = defaultdict(list)
        for example in dataset.examples:
            grouped[(example.hypothesis_id, example.horizon_seconds)].append(example)
        use_case = ResearchSelectiveHypothesisPolicy(
            estimator_factory=self._estimators,
            policy=research_policy,
        )
        results = tuple(
            use_case.execute(grouped[key]) for key in sorted(grouped)
        )
        result = SelectivePortfolioResult(
            run_id=run_id,
            input_fingerprint=input_fingerprint,
            policy_fingerprint=policy_fingerprint,
            results=results,
            examples=len(dataset.examples),
        )
        uri = self._artifacts.persist(result)
        return SelectivePortfolioExecution(
            run_id=run_id,
            reused=False,
            artifact_uri=uri,
            result=result,
        )


def build_selective_jump_dataset(
    candles: Sequence[CandleBar],
    policy: JumpReplayPolicy = JumpReplayPolicy(),
) -> SelectiveJumpDataset:
    complete = tuple(
        sorted(
            (item for item in candles if item.complete),
            key=lambda item: (item.ticker, item.opened_at),
        )
    )
    if not complete:
        raise ValueError("selective jump research requires complete candles")
    identities = {(item.ticker, item.opened_at) for item in complete}
    if len(identities) != len(complete):
        raise ValueError("candle cache contains duplicate ticker/timestamp rows")
    raw = build_raw_jump_features(complete, policy)
    split = BuildChronologicalSplit().execute(
        tuple(sorted({item.trading_day for item in raw}))
    )
    profiles = build_training_profiles(raw, split, policy)
    classified = tuple(
        item
        for feature in raw
        if (item := classify_jump_feature(feature, profiles, policy)) is not None
    )
    lookup = {(item.ticker, item.observed_at): item for item in complete}
    observations = build_jump_observations(classified, lookup, policy)
    examples: list[SelectiveExample] = []
    for observation in observations:
        feature = observation.feature
        values = _feature_values(feature)
        for outcome in observation.outcomes:
            if not outcome.available or outcome.net_effect_bps is None:
                continue
            examples.append(
                SelectiveExample(
                    hypothesis_id=observation.hypothesis.value,
                    hypothesis_version="1.0.0",
                    observation_id=(
                        f"{observation.observation_id}:{outcome.horizon_seconds}"
                    ),
                    instrument_id=feature.raw.ticker,
                    horizon_seconds=outcome.horizon_seconds,
                    trading_day=feature.raw.trading_day,
                    observed_at=feature.raw.observed_at,
                    feature_max_observed_at=feature.raw.feature_max_observed_at,
                    feature_values=values,
                    cost_adjusted_result_bps=outcome.net_effect_bps,
                    cost_model_version=outcome.cost_model_version,
                )
            )
    return SelectiveJumpDataset(
        examples=tuple(
            sorted(
                examples,
                key=lambda item: (
                    item.hypothesis_id,
                    item.horizon_seconds,
                    item.trading_day,
                    item.observed_at,
                    item.observation_id,
                ),
            )
        ),
        candle_count=len(complete),
        raw_feature_count=len(raw),
        classified_feature_count=len(classified),
    )


def _feature_values(feature) -> tuple[tuple[str, float], ...]:
    session_hour = int(feature.raw.session_bucket[:2])
    values = {
        "absolute_return_bps": feature.raw.absolute_return_bps,
        "direction": float(feature.raw.direction),
        "five_minute_range_bps": feature.raw.five_minute_range_bps,
        "illiquidity_log": log1p(feature.raw.illiquidity_proxy),
        "illiquidity_percentile": feature.illiquidity_percentile,
        "jump_excess_ratio": (
            feature.raw.absolute_return_bps
            / max(feature.thresholds.jump_absolute_return_bps, 1e-12)
        ),
        "liquidity_high": float(feature.liquidity_bucket == "high"),
        "liquidity_low": float(feature.liquidity_bucket == "low"),
        "prior_volatility_bps": feature.raw.prior_volatility_bps,
        "range_percentile": feature.range_percentile,
        "session_hour_fraction": session_hour / 23.0,
        "signed_return_bps": feature.raw.five_minute_return_bps,
        "volatility_high": float(feature.volatility_bucket == "high"),
        "volatility_low": float(feature.volatility_bucket == "low"),
        "volume_percentile": feature.volume_percentile,
        "volume_ratio": feature.volume_ratio,
    }
    return tuple(sorted(values.items()))


def _combined_policy_fingerprint(
    replay: JumpReplayPolicy, research: SelectiveResearchPolicy
) -> str:
    payload = {
        "implementation_version": IMPLEMENTATION_VERSION,
        "replay": asdict(replay),
        "selective": selective_policy_fingerprint(research),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return f"sha256:{sha256(encoded.encode('utf-8')).hexdigest()}"


def _run_id(
    input_fingerprint: str,
    policy_fingerprint: str,
    tickers: tuple[str, ...],
) -> str:
    encoded = json.dumps(
        {
            "input": input_fingerprint,
            "policy": policy_fingerprint,
            "tickers": tickers,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"selective-h3-h4-{sha256(encoded.encode('utf-8')).hexdigest()[:16]}"
