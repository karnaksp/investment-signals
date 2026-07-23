"""Causal historical replay for the first candle-based hypothesis portfolio."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from statistics import fmean, pstdev
from typing import Iterable, Protocol, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.hypothesis_evidence import (
    AssessEvidencePortfolio,
    BuildMatchedControls,
    EvidenceGatePolicy,
    EvidenceRequest,
)
from tinvest_signal_engine.application.hypothesis_observations import (
    EvaluateHypothesisObservation,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    CompletedReplay,
    HistoricalCandle,
    HistoricalReplayReport,
    HypothesisReplaySummary,
    ReplayCostModel,
    ReplayOutcome,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    StudyPoint,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.hypothesis_formulas import (
    FeatureName,
    HypothesisFeatureSet,
    HypothesisId,
    HypothesisObservation,
    ObservationVerdict,
    ObservedFeature,
    default_rule,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
    TradingPhase,
)


REPLAY_ENGINE_VERSION = "scientific-candle-replay-v1.0.0"
SUPPORTED_HYPOTHESES = (
    HypothesisId.H1,
    HypothesisId.H2,
    HypothesisId.H5,
    HypothesisId.H6,
    HypothesisId.H7,
)
CANONICAL_IDS = {
    HypothesisId.H1: "h1-morning-low-volume-reversion",
    HypothesisId.H2: "h2-morning-high-volume-continuation",
    HypothesisId.H5: "h5-same-phase-return-recurrence",
    HypothesisId.H6: "h6-open-close-market-continuation",
    HypothesisId.H7: "h7-relative-volume-future-activity",
}
DEFAULT_LIQUID_UNIVERSE = (
    "SBER",
    "GAZP",
    "LKOH",
    "YDEX",
    "T",
    "ROSN",
    "NVTK",
    "GMKN",
    "MOEX",
    "TATN",
)


class HistoricalCandleCachePort(Protocol):
    def describe(self) -> CandleCacheDescriptor: ...

    def load(self) -> tuple[HistoricalCandle, ...]: ...


class PartitionedHistoricalCandleCachePort(HistoricalCandleCachePort, Protocol):
    """Application-owned port for repeatable, ticker-bounded candle reads."""

    def iter_ticker_partitions(
        self,
    ) -> Iterable[tuple[HistoricalCandle, ...]]: ...


class HistoricalReplayArtifactPort(Protocol):
    def load_completed(self, run_id: str) -> CompletedReplay | None: ...

    def save(self, report: HistoricalReplayReport) -> CompletedReplay: ...


@dataclass(frozen=True, slots=True)
class HistoricalReplayRequest:
    selected_hypotheses: tuple[HypothesisId, ...]
    cost_model: ReplayCostModel
    liquid_universe: tuple[str, ...] = DEFAULT_LIQUID_UNIVERSE
    resume: bool = True

    def __post_init__(self) -> None:
        if not self.selected_hypotheses:
            raise ValueError("at least one hypothesis must be selected")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("selected hypotheses must be unique")
        unsupported = set(self.selected_hypotheses) - set(SUPPORTED_HYPOTHESES)
        if unsupported:
            raise ValueError(f"historical replay does not support {sorted(unsupported)}")
        if not self.liquid_universe:
            raise ValueError("liquid universe must not be empty")


@dataclass(frozen=True, slots=True)
class HistoricalReplayExecution:
    completion: CompletedReplay
    report: HistoricalReplayReport | None


@dataclass(frozen=True, slots=True)
class _DaySeries:
    ticker: str
    trading_day: date
    rows: tuple[HistoricalCandle, ...]
    by_minute: dict[int, HistoricalCandle]
    cumulative_volume: dict[int, float]


@dataclass(frozen=True, slots=True)
class _CandidateValue:
    ticker: str
    event_at: datetime
    trading_day: date
    phase: TradingPhase
    local_minute: int
    raw_effect_bps: float
    activity_effect_bps: float | None
    volatility_rank: float
    liquidity_rank: float
    feature_cutoff_at: datetime


class RunHistoricalHypothesisReplay:
    """Replay local candles; this use case has no broker or network port."""

    def __init__(
        self,
        *,
        cache: HistoricalCandleCachePort,
        artifacts: HistoricalReplayArtifactPort,
        gate_policy: EvidenceGatePolicy = EvidenceGatePolicy(),
    ) -> None:
        self._cache = cache
        self._artifacts = artifacts
        self._gate_policy = gate_policy
        self._evaluator = EvaluateHypothesisObservation()

    def execute(self, request: HistoricalReplayRequest) -> HistoricalReplayExecution:
        descriptor = self._cache.describe()
        selected = tuple(sorted(request.selected_hypotheses, key=lambda item: item.value))
        run_id = _run_id(descriptor, selected, request)
        if request.resume:
            completed = self._artifacts.load_completed(run_id)
            if completed is not None:
                return HistoricalReplayExecution(
                    completion=CompletedReplay(
                        run_id=completed.run_id,
                        artifact_fingerprint=completed.artifact_fingerprint,
                        dataset_fingerprint=completed.dataset_fingerprint,
                        selected_hypotheses=completed.selected_hypotheses,
                        resumed=True,
                    ),
                    report=None,
                )
        partition_reader = getattr(self._cache, "iter_ticker_partitions", None)
        if callable(partition_reader):
            report = self._build_partitioned_report(
                run_id=run_id,
                descriptor=descriptor,
                selected=selected,
                request=request,
                cache=self._cache,  # type: ignore[arg-type]
            )
        else:
            report = self._build_report(
                run_id=run_id,
                descriptor=descriptor,
                selected=selected,
                request=request,
                candles=self._cache.load(),
            )
        completion = self._artifacts.save(report)
        return HistoricalReplayExecution(completion=completion, report=report)

    def _build_partitioned_report(
        self,
        *,
        run_id: str,
        descriptor: CandleCacheDescriptor,
        selected: tuple[HypothesisId, ...],
        request: HistoricalReplayRequest,
        cache: PartitionedHistoricalCandleCachePort,
    ) -> HistoricalReplayReport:
        """Replay per ticker and retain full candles only for fixed-basket H6.

        The first bounded pass seals the exact chronological split.  The
        second pass evaluates ticker-local hypotheses.  H6 is the sole
        cross-sectional formula in this engine, so only its preregistered
        liquid universe is retained until that formula is evaluated.
        """

        trading_days: set[date] = set()
        moscow = ZoneInfo("Europe/Moscow")
        for partition in cache.iter_ticker_partitions():
            trading_days.update(
                candle.at.astimezone(moscow).date()
                for candle in partition
                if candle.complete
                and MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(candle.at)
            )
        ordered_days = tuple(sorted(trading_days))
        split = (
            chronological_split_60_20_20(ordered_days)
            if len(ordered_days) >= 5
            else None
        )
        outcomes: list[ReplayOutcome] = []
        event_points: dict[HypothesisId, list[StudyPoint]] = defaultdict(list)
        candidate_values: dict[HypothesisId, list[_CandidateValue]] = defaultdict(list)
        retained_h6: dict[tuple[str, date], _DaySeries] = {}
        seen_tickers: set[str] = set()
        ticker_local = tuple(
            item for item in selected if item is not HypothesisId.H6
        )
        h6_universe = frozenset(request.liquid_universe)

        for partition in cache.iter_ticker_partitions():
            series, _ = _build_series(partition)
            if not series:
                continue
            tickers = {ticker for ticker, _ in series}
            if len(tickers) != 1:
                raise ValueError(
                    "partitioned historical replay requires one ticker per partition"
                )
            ticker = next(iter(tickers))
            if ticker in seen_tickers:
                raise ValueError(
                    "partitioned historical replay requires unique ticker partitions"
                )
            seen_tickers.add(ticker)
            for hypothesis_id in ticker_local:
                self._replay_one(
                    hypothesis_id,
                    series,
                    split,
                    request,
                    outcomes,
                    event_points[hypothesis_id],
                    candidate_values[hypothesis_id],
                )
            if (
                HypothesisId.H6 in selected
                and ticker in h6_universe
            ):
                retained_h6.update(series)

        if HypothesisId.H6 in selected:
            self._replay_one(
                HypothesisId.H6,
                retained_h6,
                split,
                request,
                outcomes,
                event_points[HypothesisId.H6],
                candidate_values[HypothesisId.H6],
            )
        return self._complete_report(
            run_id=run_id,
            descriptor=descriptor,
            selected=selected,
            request=request,
            split=split,
            outcomes=outcomes,
            event_points=event_points,
            candidate_values=candidate_values,
        )

    def _build_report(
        self,
        *,
        run_id: str,
        descriptor: CandleCacheDescriptor,
        selected: tuple[HypothesisId, ...],
        request: HistoricalReplayRequest,
        candles: Sequence[HistoricalCandle],
    ) -> HistoricalReplayReport:
        series, trading_days = _build_series(candles)
        split = (
            chronological_split_60_20_20(trading_days)
            if len(trading_days) >= 5
            else None
        )
        outcomes: list[ReplayOutcome] = []
        event_points: dict[HypothesisId, list[StudyPoint]] = defaultdict(list)
        candidate_values: dict[HypothesisId, list[_CandidateValue]] = defaultdict(list)

        for hypothesis_id in selected:
            self._replay_one(
                hypothesis_id,
                series,
                split,
                request,
                outcomes,
                event_points[hypothesis_id],
                candidate_values[hypothesis_id],
            )
        return self._complete_report(
            run_id=run_id,
            descriptor=descriptor,
            selected=selected,
            request=request,
            split=split,
            outcomes=outcomes,
            event_points=event_points,
            candidate_values=candidate_values,
        )

    def _replay_one(
        self,
        hypothesis_id: HypothesisId,
        series: dict[tuple[str, date], _DaySeries],
        split: ChronologicalSplit | None,
        request: HistoricalReplayRequest,
        outcomes: list[ReplayOutcome],
        events: list[StudyPoint],
        candidates: list[_CandidateValue],
    ) -> None:
        if hypothesis_id in (HypothesisId.H1, HypothesisId.H2):
            self._replay_morning(
                hypothesis_id,
                series,
                split,
                request.cost_model,
                outcomes,
                events,
                candidates,
            )
        elif hypothesis_id is HypothesisId.H5:
            self._replay_phase_recurrence(
                series,
                split,
                request.cost_model,
                outcomes,
                events,
                candidates,
            )
        elif hypothesis_id is HypothesisId.H6:
            self._replay_open_close(
                series,
                split,
                request.cost_model,
                request.liquid_universe,
                outcomes,
                events,
                candidates,
            )
        else:
            self._replay_activity(
                series,
                split,
                request.cost_model,
                outcomes,
                events,
                candidates,
            )

    def _complete_report(
        self,
        *,
        run_id: str,
        descriptor: CandleCacheDescriptor,
        selected: tuple[HypothesisId, ...],
        request: HistoricalReplayRequest,
        split: ChronologicalSplit | None,
        outcomes: list[ReplayOutcome],
        event_points: dict[HypothesisId, list[StudyPoint]],
        candidate_values: dict[HypothesisId, list[_CandidateValue]],
    ) -> HistoricalReplayReport:
        evidence_requests: list[EvidenceRequest] = []
        holdout_eligible: dict[HypothesisId, int] = {}
        for hypothesis_id in selected:
            events = tuple(event_points[hypothesis_id])
            candidates = _study_candidates(
                hypothesis_id,
                candidate_values[hypothesis_id],
                events,
                split,
                request.cost_model,
            )
            controls = BuildMatchedControls(
                controls_per_event=self._gate_policy.controls_per_event,
            ).execute(events, candidates)
            holdout_eligible[hypothesis_id] = len(events)
            evidence_requests.append(EvidenceRequest(
                hypothesis_id=CANONICAL_IDS[hypothesis_id],
                hypothesis_version=default_rule(hypothesis_id).version,
                dataset_fingerprint=descriptor.dataset_fingerprint,
                groups=controls.groups,
                expected_eligible_events=len(events),
                unmatched_event_ids=controls.unmatched_event_ids,
            ))
        evidence = AssessEvidencePortfolio(self._gate_policy).execute(evidence_requests)
        summaries = tuple(
            _summary(hypothesis_id, outcomes, holdout_eligible[hypothesis_id])
            for hypothesis_id in selected
        )
        return HistoricalReplayReport(
            run_id=run_id,
            engine_version=REPLAY_ENGINE_VERSION,
            dataset_fingerprint=descriptor.dataset_fingerprint,
            cache_partition_count=descriptor.partition_count,
            selected_hypotheses=selected,
            cost_model=request.cost_model,
            split=split,
            summaries=summaries,
            outcomes=tuple(sorted(outcomes, key=lambda item: (
                item.hypothesis_id.value,
                item.event_at,
                item.ticker,
                item.horizon_seconds,
            ))),
            evidence=evidence,
        )

    def _replay_morning(
        self,
        hypothesis_id: HypothesisId,
        series: dict[tuple[str, date], _DaySeries],
        split: ChronologicalSplit | None,
        costs: ReplayCostModel,
        outcomes: list[ReplayOutcome],
        events: list[StudyPoint],
        candidates: list[_CandidateValue],
    ) -> None:
        for current in _ordered_series(series):
            if _series_position(series, current) < 20:
                continue
            for local_minute in _morning_checkpoints(current):
                row = current.by_minute[local_minute]
                previous_close = _previous_main_close(series, current)
                history = _prior_series(series, current, 20)
                same_minute = [item.by_minute.get(local_minute) for item in history]
                if previous_close is None or any(item is None for item in same_minute):
                    continue
                historical_deviations: list[float] = []
                historical_cumulative: list[float] = []
                historical_ranges: list[float] = []
                for prior in history:
                    prior_close = _previous_main_close(series, prior)
                    prior_row = prior.by_minute.get(local_minute)
                    if prior_close is None or prior_row is None:
                        continue
                    historical_deviations.append(_return_bps(prior_close.close, prior_row.close))
                    historical_cumulative.append(prior.cumulative_volume[local_minute])
                    historical_ranges.append(_range_bps(prior_row))
                if len(historical_deviations) < 20 or not all(historical_cumulative):
                    continue
                deviation = _return_bps(previous_close.close, row.close)
                deviation_z = _z_score(historical_deviations, deviation)
                cumulative_relative_volume = (
                    current.cumulative_volume[local_minute] / fmean(historical_cumulative)
                )
                range_rank = _percentile_rank(historical_ranges, _range_bps(row))
                features = _feature_set(row.at, {
                    FeatureName.PREVIOUS_CLOSE: previous_close.close,
                    FeatureName.EVENT_PRICE: row.close,
                    FeatureName.MORNING_DEVIATION_Z: deviation_z,
                    FeatureName.CUMULATIVE_RELATIVE_VOLUME: cumulative_relative_volume,
                    FeatureName.RANGE_PERCENTILE: range_rank,
                })
                observation = self._evaluator.execute(
                    hypothesis_id=hypothesis_id,
                    ticker=current.ticker,
                    event_at=row.at,
                    features=features,
                    has_trading_gap=_has_gap(current, 7 * 60, local_minute),
                )
                raw_by_horizon = {
                    horizon: _directional_return_from_minutes(
                        current,
                        10 * 60,
                        10 * 60 + horizon // 60,
                        1,
                    )
                    for horizon in observation.horizons_seconds
                }
                _append_outcomes(outcomes, observation, current.trading_day, raw_by_horizon, costs)
                primary_raw = raw_by_horizon[observation.horizons_seconds[0]]
                if primary_raw is None:
                    continue
                candidate = _CandidateValue(
                    ticker=current.ticker,
                    event_at=row.at,
                    trading_day=current.trading_day,
                    phase=observation.phase,
                    local_minute=local_minute,
                    raw_effect_bps=primary_raw,
                    activity_effect_bps=None,
                    volatility_rank=range_rank,
                    liquidity_rank=_ratio_rank(cumulative_relative_volume),
                    feature_cutoff_at=row.at,
                )
                candidates.append(candidate)
                if observation.verdict is ObservationVerdict.MATCHED and split is not None:
                    if split.partition_for(current.trading_day) is DatasetPartition.HOLDOUT:
                        events.append(_event_point(
                            observation,
                            current.trading_day,
                            local_minute,
                            primary_raw,
                            range_rank,
                            _ratio_rank(cumulative_relative_volume),
                            costs,
                        ))

    def _replay_phase_recurrence(
        self,
        series: dict[tuple[str, date], _DaySeries],
        split: ChronologicalSplit | None,
        costs: ReplayCostModel,
        outcomes: list[ReplayOutcome],
        events: list[StudyPoint],
        candidates: list[_CandidateValue],
    ) -> None:
        for current in _ordered_series(series):
            history = _prior_series(series, current, 20)
            if len(history) < 20:
                continue
            for local_minute in sorted(current.by_minute):
                if local_minute % 30 != 0:
                    continue
                row = current.by_minute[local_minute]
                historical_returns = [
                    value
                    for prior in history
                    if (value := _directional_return_from_minutes(
                        prior, local_minute, local_minute + 30, 1
                    )) is not None
                ]
                if len(historical_returns) < 20:
                    continue
                features = _feature_set(row.at, {
                    FeatureName.SAME_PHASE_MEAN_RETURN_BPS_20D: fmean(historical_returns),
                    FeatureName.SAME_PHASE_HISTORY_DAYS: float(len(historical_returns)),
                })
                observation = self._evaluator.execute(
                    hypothesis_id=HypothesisId.H5,
                    ticker=current.ticker,
                    event_at=row.at,
                    features=features,
                )
                raw = _directional_return_from_minutes(
                    current, local_minute, local_minute + 30, 1
                )
                _append_outcomes(outcomes, observation, current.trading_day, {1800: raw}, costs)
                if raw is None:
                    continue
                magnitude_rank = _percentile_rank(
                    [abs(item) for item in historical_returns],
                    abs(fmean(historical_returns)),
                )
                candidates.append(_CandidateValue(
                    current.ticker, row.at, current.trading_day,
                    observation.phase, local_minute, raw, None,
                    magnitude_rank, 0.5, row.at,
                ))
                if observation.verdict is ObservationVerdict.MATCHED and split is not None:
                    if split.partition_for(current.trading_day) is DatasetPartition.HOLDOUT:
                        events.append(_event_point(
                            observation, current.trading_day, local_minute, raw,
                            magnitude_rank, 0.5, costs,
                        ))

    def _replay_open_close(
        self,
        series: dict[tuple[str, date], _DaySeries],
        split: ChronologicalSplit | None,
        costs: ReplayCostModel,
        liquid_universe: tuple[str, ...],
        outcomes: list[ReplayOutcome],
        events: list[StudyPoint],
        candidates: list[_CandidateValue],
    ) -> None:
        days = sorted({day for _, day in series})
        for trading_day in days:
            members = [series[(ticker, trading_day)] for ticker in liquid_universe if (ticker, trading_day) in series]
            if len(members) / len(liquid_universe) < 0.8:
                continue
            opening = [
                value for item in members
                if (value := _directional_return_from_minutes(item, 10 * 60, 10 * 60 + 29, 1)) is not None
            ]
            closing = [
                value for item in members
                if (value := _directional_return_from_minutes(item, 18 * 60 + 10, 18 * 60 + 39, 1)) is not None
            ]
            if len(opening) / len(liquid_universe) < 0.8 or len(closing) / len(liquid_universe) < 0.8:
                continue
            event_row = next((item.by_minute.get(10 * 60 + 30) for item in members if item.by_minute.get(10 * 60 + 30)), None)
            if event_row is None:
                continue
            opening_basket = fmean(opening)
            closing_basket = fmean(closing)
            observation = self._evaluator.execute(
                hypothesis_id=HypothesisId.H6,
                ticker="MOEX_LIQUID_BASKET",
                event_at=event_row.at,
                features=_feature_set(event_row.at, {
                    FeatureName.OPENING_BASKET_RETURN_BPS: opening_basket,
                }),
            )
            _append_outcomes(outcomes, observation, trading_day, {1800: closing_basket}, costs)
            opening_rank = _ratio_rank(abs(opening_basket) / 10.0)
            for candidate_minute in range(10 * 60 + 30, 17 * 60 + 31, 30):
                candidate_returns = [
                    value for item in members
                    if (value := _directional_return_from_minutes(
                        item, candidate_minute, candidate_minute + 30, 1
                    )) is not None
                ]
                candidate_row = next((
                    item.by_minute.get(candidate_minute)
                    for item in members if item.by_minute.get(candidate_minute)
                ), None)
                if candidate_row is None or len(candidate_returns) / len(liquid_universe) < 0.8:
                    continue
                candidates.append(_CandidateValue(
                    "MOEX_LIQUID_BASKET", candidate_row.at, trading_day,
                    TradingPhase.MAIN_CONTINUOUS,
                    10 * 60 + 30,
                    fmean(candidate_returns), None, opening_rank, 1.0,
                    candidate_row.at,
                ))
            if observation.verdict is ObservationVerdict.MATCHED and split is not None:
                if split.partition_for(trading_day) is DatasetPartition.HOLDOUT:
                    events.append(_event_point(
                        observation, trading_day, 10 * 60 + 30, closing_basket,
                        opening_rank, 1.0, costs,
                    ))

    def _replay_activity(
        self,
        series: dict[tuple[str, date], _DaySeries],
        split: ChronologicalSplit | None,
        costs: ReplayCostModel,
        outcomes: list[ReplayOutcome],
        events: list[StudyPoint],
        candidates: list[_CandidateValue],
    ) -> None:
        for current in _ordered_series(series):
            history = _prior_series(series, current, 20)
            if len(history) < 20:
                continue
            for local_minute in sorted(current.by_minute):
                if local_minute % 15 != 14:
                    continue
                row = current.by_minute[local_minute]
                start = local_minute - 14
                current_volume = _window_volume(current, start, local_minute)
                prior_volumes = [_window_volume(item, start, local_minute) for item in history]
                prior_volumes = [value for value in prior_volumes if value is not None]
                if current_volume is None or len(prior_volumes) < 20:
                    continue
                volume_rank = _percentile_rank(prior_volumes, current_volume)
                observation = self._evaluator.execute(
                    hypothesis_id=HypothesisId.H7,
                    ticker=current.ticker,
                    event_at=row.at,
                    features=_feature_set(row.at, {
                        FeatureName.PHASE_VOLUME_PERCENTILE: volume_rank,
                        FeatureName.PHASE_HISTORY_DAYS: float(len(prior_volumes)),
                    }),
                    has_trading_gap=_has_gap(current, start, local_minute),
                )
                activity_by_horizon: dict[int, float | None] = {}
                for horizon in observation.horizons_seconds:
                    future_minutes = horizon // 60
                    current_activity = _realized_activity(current, local_minute, future_minutes)
                    prior_activity = [
                        value for item in history
                        if (value := _realized_activity(item, local_minute, future_minutes)) is not None
                    ]
                    activity_by_horizon[horizon] = (
                        current_activity - fmean(prior_activity)
                        if current_activity is not None and len(prior_activity) >= 20
                        else None
                    )
                _append_activity_outcomes(
                    outcomes, observation, current.trading_day, activity_by_horizon
                )
                primary = activity_by_horizon[observation.horizons_seconds[0]]
                if primary is None:
                    continue
                candidates.append(_CandidateValue(
                    current.ticker, row.at, current.trading_day, observation.phase,
                    local_minute, 0.0, primary, 0.5, volume_rank, row.at,
                ))
                if observation.verdict is ObservationVerdict.MATCHED and split is not None:
                    if split.partition_for(current.trading_day) is DatasetPartition.HOLDOUT:
                        events.append(_event_point(
                            observation, current.trading_day, local_minute, primary,
                            0.5, volume_rank, costs, activity=True,
                        ))


def _build_series(
    candles: Sequence[HistoricalCandle],
) -> tuple[dict[tuple[str, date], _DaySeries], tuple[date, ...]]:
    grouped: dict[tuple[str, date], list[HistoricalCandle]] = defaultdict(list)
    moscow = ZoneInfo("Europe/Moscow")
    for candle in candles:
        if candle.complete and MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(candle.at):
            grouped[(candle.ticker, candle.at.astimezone(moscow).date())].append(candle)
    result: dict[tuple[str, date], _DaySeries] = {}
    for (ticker, trading_day), rows in sorted(grouped.items()):
        rows.sort(key=lambda item: item.at)
        by_minute: dict[int, HistoricalCandle] = {}
        cumulative: dict[int, float] = {}
        running = 0.0
        for row in rows:
            local = row.at.astimezone(moscow)
            minute = local.hour * 60 + local.minute
            if minute in by_minute:
                continue
            by_minute[minute] = row
            if 7 * 60 <= minute <= 9 * 60 + 49:
                running += row.volume
                cumulative[minute] = running
        result[(ticker, trading_day)] = _DaySeries(
            ticker=ticker,
            trading_day=trading_day,
            rows=tuple(rows),
            by_minute=by_minute,
            cumulative_volume=cumulative,
        )
    return result, tuple(sorted({day for _, day in result}))


def _ordered_series(series: dict[tuple[str, date], _DaySeries]) -> tuple[_DaySeries, ...]:
    return tuple(series[key] for key in sorted(series))


def _ticker_days(series: dict[tuple[str, date], _DaySeries], ticker: str) -> tuple[date, ...]:
    return tuple(sorted(day for item_ticker, day in series if item_ticker == ticker))


def _series_position(series: dict[tuple[str, date], _DaySeries], current: _DaySeries) -> int:
    return _ticker_days(series, current.ticker).index(current.trading_day)


def _prior_series(
    series: dict[tuple[str, date], _DaySeries],
    current: _DaySeries,
    count: int,
) -> tuple[_DaySeries, ...]:
    days = _ticker_days(series, current.ticker)
    position = days.index(current.trading_day)
    return tuple(series[(current.ticker, day)] for day in days[max(0, position - count):position])


def _previous_main_close(
    series: dict[tuple[str, date], _DaySeries],
    current: _DaySeries,
) -> HistoricalCandle | None:
    prior = _prior_series(series, current, 1)
    if not prior:
        return None
    eligible = [row for minute, row in prior[0].by_minute.items() if minute >= 10 * 60]
    return max(eligible, key=lambda item: item.at) if eligible else None


def _morning_checkpoints(series: _DaySeries) -> tuple[int, ...]:
    return tuple(
        minute for minute in sorted(series.by_minute)
        if 7 * 60 <= minute <= 9 * 60 + 49
        and (minute % 15 == 14 or minute == 9 * 60 + 49)
    )


def _feature_set(at: datetime, values: dict[FeatureName, float]) -> HypothesisFeatureSet:
    return HypothesisFeatureSet.from_iterable(
        ObservedFeature(
            name=name,
            value=value,
            observed_at=at,
            window_start=at - timedelta(days=180),
            window_end=at,
        )
        for name, value in values.items()
    )


def _has_gap(series: _DaySeries, start_minute: int, end_minute: int) -> bool:
    if start_minute > end_minute:
        return True
    return any(minute not in series.by_minute for minute in range(start_minute, end_minute + 1))


def _return_bps(start: float, end: float) -> float:
    return 10_000.0 * (end / start - 1.0)


def _range_bps(candle: HistoricalCandle) -> float:
    return _return_bps(candle.low, candle.high)


def _z_score(history: Sequence[float], value: float) -> float:
    sigma = pstdev(history)
    if sigma <= 1e-12:
        return 999.0 if value > fmean(history) else -999.0 if value < fmean(history) else 0.0
    return (value - fmean(history)) / sigma


def _percentile_rank(history: Sequence[float], value: float) -> float:
    if not history:
        return 0.0
    below = sum(item < value for item in history)
    equal = sum(item == value for item in history)
    return (below + 0.5 * equal) / len(history)


def _ratio_rank(value: float) -> float:
    return min(1.0, max(0.0, value / 2.0))


def _directional_return_from_minutes(
    series: _DaySeries,
    start_minute: int,
    end_minute: int,
    direction: int,
) -> float | None:
    start = series.by_minute.get(start_minute)
    end = series.by_minute.get(end_minute)
    if start is None or end is None or _has_gap(series, start_minute, end_minute):
        return None
    return direction * _return_bps(start.close, end.close)


def _window_volume(series: _DaySeries, start: int, end: int) -> float | None:
    if _has_gap(series, start, end):
        return None
    return sum(series.by_minute[minute].volume for minute in range(start, end + 1))


def _realized_activity(series: _DaySeries, event_minute: int, horizon_minutes: int) -> float | None:
    end = event_minute + horizon_minutes
    if _has_gap(series, event_minute, end):
        return None
    return sum(
        abs(_return_bps(series.by_minute[minute - 1].close, series.by_minute[minute].close))
        for minute in range(event_minute + 1, end + 1)
    )


def _append_outcomes(
    outcomes: list[ReplayOutcome],
    observation: HypothesisObservation,
    trading_day: date,
    raw_by_horizon: dict[int, float | None],
    costs: ReplayCostModel,
) -> None:
    for horizon, raw in raw_by_horizon.items():
        gross = raw * observation.expected_direction if raw is not None else None
        net = (
            gross - costs.round_trip_bps
            if gross is not None and observation.verdict is ObservationVerdict.MATCHED
            else None
        )
        outcomes.append(_outcome(observation, trading_day, horizon, gross, net))


def _append_activity_outcomes(
    outcomes: list[ReplayOutcome],
    observation: HypothesisObservation,
    trading_day: date,
    activity_by_horizon: dict[int, float | None],
) -> None:
    for horizon, effect in activity_by_horizon.items():
        net = effect if observation.verdict is ObservationVerdict.MATCHED else None
        outcomes.append(_outcome(observation, trading_day, horizon, effect, net))


def _outcome(
    observation: HypothesisObservation,
    trading_day: date,
    horizon: int,
    gross: float | None,
    net: float | None,
) -> ReplayOutcome:
    return ReplayOutcome(
        observation_id=observation.observation_id,
        hypothesis_id=observation.hypothesis_id,
        hypothesis_version=observation.hypothesis_version,
        ticker=observation.ticker,
        event_at=observation.event_at,
        trading_day=trading_day,
        phase=observation.phase,
        verdict=observation.verdict,
        reason=observation.reason,
        expected_effect=observation.expected_effect,
        expected_direction=observation.expected_direction,
        outcome_anchor=observation.outcome_anchor,
        horizon_seconds=horizon,
        feature_cutoff_at=observation.feature_cutoff_at,
        gross_effect_bps=gross,
        net_effect_bps=net,
        label_available=net is not None,
    )


def _event_point(
    observation: HypothesisObservation,
    trading_day: date,
    local_minute: int,
    raw_effect: float,
    volatility_rank: float,
    liquidity_rank: float,
    costs: ReplayCostModel,
    *,
    activity: bool = False,
) -> StudyPoint:
    net = raw_effect if activity else observation.expected_direction * raw_effect - costs.round_trip_bps
    return StudyPoint(
        point_id=f"event:{observation.observation_id}",
        scenario_id=observation.observation_id,
        instrument_id=observation.ticker,
        occurred_at=observation.event_at,
        trading_day=trading_day,
        session_bucket=_matching_session(observation.phase, local_minute, observation.expected_direction),
        volatility_bucket=_bucket(volatility_rank),
        liquidity_bucket=_bucket(liquidity_rank),
        features_observed_at=observation.feature_cutoff_at,
        partition=DatasetPartition.HOLDOUT,
        net_effect_bps=net,
        cost_model_version=costs.version,
    )


def _study_candidates(
    hypothesis_id: HypothesisId,
    values: Sequence[_CandidateValue],
    events: Sequence[StudyPoint],
    split: ChronologicalSplit | None,
    costs: ReplayCostModel,
) -> tuple[StudyPoint, ...]:
    if split is None:
        return ()
    event_times = defaultdict(list)
    for event in events:
        event_times[event.instrument_id].append((event.occurred_at, event.scenario_id))
    result: list[StudyPoint] = []
    directional = hypothesis_id is not HypothesisId.H7
    for candidate in values:
        if split.partition_for(candidate.trading_day) is not DatasetPartition.HOLDOUT:
            continue
        directions = (-1, 1) if directional else (0,)
        nearby = tuple(
            scenario for at, scenario in event_times[candidate.ticker]
            if scenario is not None and abs(at - candidate.event_at) <= timedelta(minutes=5)
        )
        for direction in directions:
            effect = (
                candidate.activity_effect_bps
                if hypothesis_id is HypothesisId.H7
                else direction * candidate.raw_effect_bps - costs.round_trip_bps
            )
            if effect is None:
                continue
            result.append(StudyPoint(
                point_id=(
                    f"control:{hypothesis_id.value}:{candidate.ticker}:"
                    f"{candidate.event_at.isoformat()}:{direction}"
                ),
                scenario_id=None,
                instrument_id=candidate.ticker,
                occurred_at=candidate.event_at,
                trading_day=candidate.trading_day,
                session_bucket=_matching_session(
                    candidate.phase, candidate.local_minute, direction
                ),
                volatility_bucket=_bucket(candidate.volatility_rank),
                liquidity_bucket=_bucket(candidate.liquidity_rank),
                features_observed_at=candidate.feature_cutoff_at,
                partition=DatasetPartition.HOLDOUT,
                net_effect_bps=effect,
                cost_model_version=costs.version,
                nearby_scenario_ids=nearby,
            ))
    return tuple(result)


def _matching_session(phase: TradingPhase, local_minute: int, direction: int) -> str:
    return f"{phase.value}:{local_minute // 30}:direction={direction}"


def _bucket(rank: float) -> str:
    return "low" if rank < 1 / 3 else "medium" if rank < 2 / 3 else "high"


def _summary(
    hypothesis_id: HypothesisId,
    outcomes: Sequence[ReplayOutcome],
    holdout_eligible_events: int,
) -> HypothesisReplaySummary:
    rows = [item for item in outcomes if item.hypothesis_id is hypothesis_id]
    observation_ids = {item.observation_id for item in rows}
    matched = {
        item.observation_id for item in rows
        if item.verdict is ObservationVerdict.MATCHED
    }
    abstained = {
        item.observation_id for item in rows
        if item.verdict is ObservationVerdict.ABSTAIN
    }
    return HypothesisReplaySummary(
        hypothesis_id=hypothesis_id,
        hypothesis_version=default_rule(hypothesis_id).version,
        evaluated_observations=len(observation_ids),
        matched_observations=len(matched),
        abstained_observations=len(abstained),
        available_labels=sum(item.label_available for item in rows),
        holdout_eligible_events=holdout_eligible_events,
    )


def _run_id(
    descriptor: CandleCacheDescriptor,
    selected: tuple[HypothesisId, ...],
    request: HistoricalReplayRequest,
) -> str:
    payload = {
        "engine_version": REPLAY_ENGINE_VERSION,
        "dataset_fingerprint": descriptor.dataset_fingerprint,
        "selected": tuple(item.value for item in selected),
        "cost_model": {
            "version": request.cost_model.version,
            "commission_bps": request.cost_model.commission_bps,
            "slippage_bps": request.cost_model.slippage_bps,
            "half_spread_entry_bps": request.cost_model.half_spread_entry_bps,
            "half_spread_exit_bps": request.cost_model.half_spread_exit_bps,
        },
        "liquid_universe": request.liquid_universe,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{sha256(encoded).hexdigest()}"
