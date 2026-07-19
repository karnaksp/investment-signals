"""Independent evidence gate for the next candle hypothesis portfolio.

The workflow converts sealed, causal candle observations into the generic
matched-control evidence vocabulary.  Thresholds and matching strata are
derived before the holdout partition is opened; holdout outcomes are used only
for the final effect calculation.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from collections import defaultdict
from typing import Mapping, Sequence
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
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
)


_VERSIONS = {
    ScientificCandleHypothesis.OPENING_GAP_REVERSION: "1.0.0",
    ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION: "1.0.0",
    ScientificCandleHypothesis.HAR_VOLATILITY: "1.0.0",
    ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2: "2.0.0",
}


@dataclass(frozen=True, slots=True)
class ScientificCandleEvidenceCoverage:
    hypothesis_id: str
    available_holdout_observations: int
    triggered_events: int
    eligible_common_support_events: int
    unmatched_events: int
    control_candidates: int
    matched_event_ids: tuple[str, ...]

    @property
    def common_support_rate(self) -> float | None:
        if self.triggered_events == 0:
            return None
        return self.eligible_common_support_events / self.triggered_events


@dataclass(frozen=True, slots=True)
class ScientificCandleEvidenceAssessment:
    """One multiple-testing family evaluated on the sealed holdout."""

    bundles: tuple[EvidenceBundle, ...]
    coverage: tuple[ScientificCandleEvidenceCoverage, ...]

    def for_hypothesis(
        self,
        hypothesis: ScientificCandleHypothesis,
    ) -> EvidenceBundle:
        return next(
            item for item in self.bundles if item.hypothesis_id == hypothesis.value
        )

    def coverage_for(
        self,
        hypothesis: ScientificCandleHypothesis,
    ) -> ScientificCandleEvidenceCoverage:
        return next(
            item for item in self.coverage if item.hypothesis_id == hypothesis.value
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
        prepared = tuple(
            self._request(
                report,
                hypothesis,
                paired,
                cost_model_version=cost_model_version,
            )
            for hypothesis in selected
        )
        return ScientificCandleEvidenceAssessment(
            bundles=self._portfolio.execute(tuple(item[0] for item in prepared)),
            coverage=tuple(item[1] for item in prepared),
        )

    def _request(
        self,
        report: ScientificCandleResearchReport,
        hypothesis: ScientificCandleHypothesis,
        paired: Sequence[tuple[CausalFeatureVector, ScientificModelOutcome]],
        *,
        cost_model_version: str,
    ) -> tuple[EvidenceRequest, ScientificCandleEvidenceCoverage]:
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
        event_thresholds = _event_thresholds(hypothesis, validation)
        volatility_cutoffs = _volatility_cutoffs(
            hypothesis,
            validation,
        )

        events: list[StudyPoint] = []
        candidates: list[StudyPoint] = []
        for feature, outcome in holdout:
            treated = _is_event(feature, event_thresholds=event_thresholds)
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
        coverage = ScientificCandleEvidenceCoverage(
            hypothesis_id=hypothesis.value,
            available_holdout_observations=len(holdout),
            triggered_events=len(events),
            eligible_common_support_events=len(matched.groups),
            unmatched_events=len(matched.unmatched_event_ids),
            control_candidates=len(candidates_with_exclusions),
            matched_event_ids=tuple(group.event.point_id for group in matched.groups),
        )
        # Common-support eligibility is fixed solely by pre-outcome strata and
        # deterministic control availability. Discarded triggers remain in the
        # coverage record; they are not silently counted as evaluated events.
        return (
            EvidenceRequest(
                hypothesis_id=hypothesis.value,
                hypothesis_version=_VERSIONS[hypothesis],
                dataset_fingerprint=report.dataset_fingerprint,
                groups=matched.groups,
                expected_eligible_events=len(matched.groups),
                unmatched_event_ids=(),
                total_available_observations=len(holdout),
            ),
            coverage,
        )


def _event_thresholds(
    hypothesis: ScientificCandleHypothesis,
    validation: Sequence[tuple[CausalFeatureVector, ScientificModelOutcome]],
) -> Mapping[tuple[str, str], float]:
    if hypothesis is not ScientificCandleHypothesis.HAR_VOLATILITY:
        return {}
    forecasts: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    for feature, _ in validation:
        if feature.forecast_value is not None:
            forecasts[_stratum_key(feature)].append(feature.forecast_value)
    return {
        key: _quantile(values, 0.90)
        for key, values in sorted(forecasts.items())
    }


def _is_event(
    feature: CausalFeatureVector,
    *,
    event_thresholds: Mapping[tuple[str, str], float],
) -> bool:
    if feature.hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        event_threshold = event_thresholds.get(_stratum_key(feature))
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
        session_bucket=_session_phase(feature),
        volatility_bucket=_bucket(
            _volatility_proxy(feature),
            volatility_cutoffs.get(_stratum_key(feature)),
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
        # Every HAR variance window contributes to the forecast and therefore
        # defines treatment. Matching on one of those windows destroys common
        # support by construction, so H15 uses an explicit neutral stratum.
        return 0.0
    if feature.hypothesis is ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2:
        return feature.value("baseline_future_variance")
    if feature.hypothesis is ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION:
        return abs(feature.value("market_return_bps"))
    return 0.0


def _volatility_cutoffs(
    hypothesis: ScientificCandleHypothesis,
    validation: Sequence[tuple[CausalFeatureVector, ScientificModelOutcome]],
) -> Mapping[tuple[str, str], tuple[float, float]]:
    if hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        return {}
    values: defaultdict[tuple[str, str], list[float]] = defaultdict(list)
    for feature, _ in validation:
        values[_stratum_key(feature)].append(_volatility_proxy(feature))
    return {
        key: (
            _quantile(group, 1.0 / 3.0),
            _quantile(group, 2.0 / 3.0),
        )
        for key, group in sorted(values.items())
    }


def _stratum_key(feature: CausalFeatureVector) -> tuple[str, str]:
    return (feature.ticker, _session_phase(feature))


def _session_phase(feature: CausalFeatureVector) -> str:
    if feature.hypothesis in {
        ScientificCandleHypothesis.HAR_VOLATILITY,
        ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
    }:
        return MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(feature.observed_at).value
    local = feature.observed_at.astimezone(
        ZoneInfo(MOEX_EQUITY_PHASE_SCHEDULE_V1.timezone_name)
    )
    return f"{local.hour:02d}:{(local.minute // 15) * 15:02d}"


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
