"""Independent evidence gate for the next candle hypothesis portfolio.

The workflow converts sealed, causal candle observations into the generic
matched-control evidence vocabulary.  Thresholds and matching strata are
derived before the holdout partition is opened; holdout outcomes are used only
for the final effect calculation.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildMatchedControls,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.application.scientific_candle_models import (
    ScientificCandleResearchReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    DatasetPartition,
    EvidenceBundle,
    StudyPoint,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    CausalFeatureVector,
    FeatureDecision,
    ScientificCandleHypothesis,
    ScientificModelOutcome,
)


_VERSIONS = {
    ScientificCandleHypothesis.OPENING_GAP_REVERSION: "1.0.0",
    ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION: "1.0.0",
    ScientificCandleHypothesis.HAR_VOLATILITY: "1.0.0",
    ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2: "2.0.0",
}
_MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True, slots=True)
class ScientificCandleEvidenceAssessment:
    """One multiple-testing family evaluated on the sealed holdout."""

    bundles: tuple[EvidenceBundle, ...]

    def for_hypothesis(
        self,
        hypothesis: ScientificCandleHypothesis,
    ) -> EvidenceBundle:
        return next(
            item for item in self.bundles if item.hypothesis_id == hypothesis.value
        )


class AssessScientificCandleHoldoutEvidence:
    """Build causal controls and assess all requested candle hypotheses at once."""

    def __init__(
        self,
        policy: EvidenceGatePolicy = EvidenceGatePolicy(),
    ) -> None:
        self.policy = policy
        self._controls = BuildMatchedControls(
            controls_per_event=policy.controls_per_event,
            scenario_exclusion_window=timedelta(minutes=5),
        )
        self._portfolio = AssessEvidencePortfolio(policy)

    def execute(
        self,
        report: ScientificCandleResearchReport,
        requested_hypotheses: Sequence[ScientificCandleHypothesis],
        *,
        cost_model_version: str,
    ) -> ScientificCandleEvidenceAssessment:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        if not selected:
            raise ValueError("at least one scientific hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")

        paired = tuple(zip(report.features, report.outcomes, strict=True))
        requests = tuple(
            self._request(
                report,
                hypothesis,
                paired,
                cost_model_version=cost_model_version,
            )
            for hypothesis in selected
        )
        return ScientificCandleEvidenceAssessment(
            bundles=self._portfolio.execute(requests)
        )

    def _request(
        self,
        report: ScientificCandleResearchReport,
        hypothesis: ScientificCandleHypothesis,
        paired: Sequence[tuple[CausalFeatureVector, ScientificModelOutcome]],
        *,
        cost_model_version: str,
    ) -> EvidenceRequest:
        hypothesis_pairs = tuple(
            pair for pair in paired if pair[0].hypothesis is hypothesis
        )
        validation = tuple(
            pair
            for pair in hypothesis_pairs
            if report.split.partition_for(pair[0].trading_day)
            is DatasetPartition.VALIDATION
            and pair[1].available
        )
        holdout = tuple(
            pair
            for pair in hypothesis_pairs
            if report.split.partition_for(pair[0].trading_day)
            is DatasetPartition.HOLDOUT
            and pair[1].available
        )
        event_threshold = _event_threshold(hypothesis, validation)
        volatility_cutoffs = _tertile_cutoffs(
            tuple(_volatility_proxy(feature) for feature, _ in validation)
        )

        events: list[StudyPoint] = []
        candidates: list[StudyPoint] = []
        for feature, outcome in holdout:
            treated = _is_event(feature, event_threshold=event_threshold)
            effect = _net_effect(
                feature,
                outcome,
                round_trip_cost_bps=report.policy.round_trip_cost_bps,
            )
            if effect is None:
                continue
            point = _study_point(
                feature,
                net_effect=effect,
                cost_model_version=cost_model_version,
                volatility_cutoffs=volatility_cutoffs,
                treated=treated,
            )
            if treated:
                events.append(point)
            else:
                candidates.append(point)

        candidates_with_exclusions = tuple(
            replace(
                candidate,
                nearby_scenario_ids=tuple(
                    event.scenario_id
                    for event in events
                    if event.scenario_id is not None
                    and event.instrument_id == candidate.instrument_id
                    and abs(event.occurred_at - candidate.occurred_at)
                    <= timedelta(minutes=5)
                ),
            )
            for candidate in candidates
        )
        matched = self._controls.execute(events, candidates_with_exclusions)
        return EvidenceRequest(
            hypothesis_id=hypothesis.value,
            hypothesis_version=_VERSIONS[hypothesis],
            dataset_fingerprint=report.dataset_fingerprint,
            groups=matched.groups,
            expected_eligible_events=len(events),
            unmatched_event_ids=matched.unmatched_event_ids,
        )


def _event_threshold(
    hypothesis: ScientificCandleHypothesis,
    validation: Sequence[tuple[CausalFeatureVector, ScientificModelOutcome]],
) -> float | None:
    if hypothesis is not ScientificCandleHypothesis.HAR_VOLATILITY:
        return None
    forecasts = tuple(
        feature.forecast_value
        for feature, _ in validation
        if feature.forecast_value is not None
    )
    return _quantile(forecasts, 0.90) if forecasts else None


def _is_event(
    feature: CausalFeatureVector,
    *,
    event_threshold: float | None,
) -> bool:
    if feature.hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        return (
            event_threshold is not None
            and feature.forecast_value is not None
            and feature.forecast_value >= event_threshold
        )
    return feature.decision is FeatureDecision.MATCHED


def _net_effect(
    feature: CausalFeatureVector,
    outcome: ScientificModelOutcome,
    *,
    round_trip_cost_bps: float,
) -> float | None:
    hypothesis = feature.hypothesis
    if hypothesis in {
        ScientificCandleHypothesis.OPENING_GAP_REVERSION,
        ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION,
    }:
        if outcome.actual_value is None:
            return None
        direction = _direction(feature)
        return direction * outcome.actual_value - round_trip_cost_bps
    if hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        if outcome.model_loss is None or outcome.benchmark_loss is None:
            return None
        return outcome.benchmark_loss - outcome.model_loss
    if outcome.actual_value is None:
        return None
    return (outcome.actual_value - 1.0) * 10_000.0


def _direction(feature: CausalFeatureVector) -> int:
    if feature.hypothesis is ScientificCandleHypothesis.OPENING_GAP_REVERSION:
        return -1
    residual = feature.value("market_residual_bps")
    return -1 if residual > 0.0 else 1


def _study_point(
    feature: CausalFeatureVector,
    *,
    net_effect: float,
    cost_model_version: str,
    volatility_cutoffs: tuple[float, float] | None,
    treated: bool,
) -> StudyPoint:
    local = feature.observed_at.astimezone(_MOSCOW)
    direction = _direction(feature) if feature.hypothesis in {
        ScientificCandleHypothesis.OPENING_GAP_REVERSION,
        ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION,
    } else 0
    return StudyPoint(
        point_id=feature.observation_id,
        scenario_id=feature.observation_id if treated else None,
        instrument_id=feature.ticker,
        occurred_at=feature.observed_at,
        trading_day=feature.trading_day,
        session_bucket=f"{local.hour:02d}:{(local.minute // 15) * 15:02d}",
        volatility_bucket=_bucket(
            _volatility_proxy(feature), volatility_cutoffs
        ),
        # The same instrument is an explicit liquidity fixed effect.  The
        # direction suffix prevents opposite residual moves from sharing a
        # control while avoiding post-event liquidity information.
        liquidity_bucket=f"same_instrument_direction_{direction:+d}",
        features_observed_at=feature.feature_max_observed_at,
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net_effect,
        cost_model_version=cost_model_version,
    )


def _volatility_proxy(feature: CausalFeatureVector) -> float:
    if feature.hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        return feature.value("long_realized_variance")
    if feature.hypothesis is ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2:
        return feature.value("baseline_future_variance")
    if feature.hypothesis is ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION:
        return abs(feature.value("market_return_bps"))
    return 0.0


def _tertile_cutoffs(values: Sequence[float]) -> tuple[float, float] | None:
    if not values:
        return None
    return (_quantile(values, 1.0 / 3.0), _quantile(values, 2.0 / 3.0))


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("quantile requires observations")
    ordered = tuple(sorted(values))
    index = int((len(ordered) - 1) * probability)
    return ordered[index]


def _bucket(value: float, cutoffs: tuple[float, float] | None) -> str:
    if cutoffs is None:
        return "pre_holdout_distribution_unavailable"
    low, high = cutoffs
    if value <= low:
        return "low"
    if value <= high:
        return "medium"
    return "high"
