"""Chronological candle calculations for the prospective scientific portfolio."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from math import log, pi, sqrt
from statistics import fmean, median, pstdev
from typing import Iterable, Protocol, Sequence, TypeVar
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    FrozenPairParameters,
    HarV2Parameters,
    HarV2TrainingPoint,
    JumpHistoryPoint,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveScientificPolicy,
    directional_outcome,
    downside_semivariance_feature,
    fit_har_v2_parameters,
    har_v2_feature,
    har_v2_outcome,
    jump_regime_features,
    morning_regime_features,
    open_close_basket_feature,
    pair_residual_reversion_feature,
    phase_recurrence_feature,
    relative_volume_volatility_feature,
    variance_uplift_outcome,
    volatility_jump_feature,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
    TradingPhase,
)


MOSCOW = ZoneInfo("Europe/Moscow")
_ELIGIBLE_PHASES = {
    TradingPhase.MAIN_OPENING,
    TradingPhase.MAIN_CONTINUOUS,
    TradingPhase.PRE_CLOSE,
}
DEFAULT_FIXED_MARKET_UNIVERSE = (
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
DEFAULT_PAIR_CANDIDATES = (
    ("SBER", "SBERP"),
    ("TATN", "TATNP"),
    ("SNGS", "SNGSP"),
)


@dataclass(frozen=True, slots=True)
class ProspectiveScientificRequest:
    selected_hypotheses: tuple[ProspectiveHypothesis, ...] = tuple(
        ProspectiveHypothesis
    )
    policy: ProspectiveScientificPolicy = ProspectiveScientificPolicy()
    market_universe: tuple[str, ...] = DEFAULT_FIXED_MARKET_UNIVERSE
    pair_candidates: tuple[tuple[str, str], ...] = DEFAULT_PAIR_CANDIDATES

    def __post_init__(self) -> None:
        if not self.selected_hypotheses:
            raise ValueError("at least one prospective hypothesis is required")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("prospective hypotheses must be unique")
        if len(set(self.market_universe)) != len(self.market_universe):
            raise ValueError("market universe must be unique")
        if any(not ticker.strip() for ticker in self.market_universe):
            raise ValueError("market universe tickers must not be empty")
        normalized_pairs = tuple(tuple(pair) for pair in self.pair_candidates)
        if any(
            len(pair) != 2 or not pair[0].strip() or not pair[1].strip()
            or pair[0] == pair[1]
            for pair in normalized_pairs
        ):
            raise ValueError("pair candidates require two distinct tickers")
        if len(set(normalized_pairs)) != len(normalized_pairs):
            raise ValueError("pair candidates must be unique")


@dataclass(frozen=True, slots=True)
class ProspectiveScientificReport:
    dataset_fingerprint: str
    report_fingerprint: str
    split: ChronologicalSplit
    policy: ProspectiveScientificPolicy
    selected_hypotheses: tuple[ProspectiveHypothesis, ...]
    har_v2_parameters: HarV2Parameters | None
    features: tuple[ProspectiveFeature, ...]
    outcomes: tuple[ProspectiveOutcome, ...]
    pair_parameters: tuple[FrozenPairParameters, ...] = ()

    def __post_init__(self) -> None:
        if not self.dataset_fingerprint.startswith("sha256:"):
            raise ValueError("dataset_fingerprint must use sha256")
        if not self.report_fingerprint.startswith("sha256:"):
            raise ValueError("report_fingerprint must use sha256")
        if len(self.features) != len(self.outcomes):
            raise ValueError("features and outcomes must remain aligned")
        if any(
            feature.observation_id != outcome.observation_id
            for feature, outcome in zip(self.features, self.outcomes, strict=True)
        ):
            raise ValueError("feature and outcome identities must remain aligned")


class PartitionedProspectiveCandleCachePort(Protocol):
    """Application-owned port for bounded, ticker-partitioned candle access."""

    def iter_ticker_partitions(self) -> Iterable[tuple[HistoricalCandle, ...]]: ...


def build_prospective_scientific_research(
    candles: Iterable[HistoricalCandle],
    *,
    dataset_fingerprint: str,
    request: ProspectiveScientificRequest = ProspectiveScientificRequest(),
) -> ProspectiveScientificReport:
    """Build deterministic causal features and sealed outcomes for all versions."""

    if not dataset_fingerprint.startswith("sha256:"):
        raise ValueError("dataset_fingerprint must use sha256")
    complete = tuple(
        sorted(
            (item for item in candles if item.complete),
            key=lambda item: (item.ticker, item.at),
        )
    )
    if not complete:
        raise ValueError("prospective research requires complete candles")
    identities = {(item.ticker, item.at) for item in complete}
    if len(identities) != len(complete):
        raise ValueError("prospective research candles must be unique")
    split = chronological_split_60_20_20(
        tuple(sorted({_trading_day(item.at) for item in complete}))
    )
    by_ticker: defaultdict[str, list[HistoricalCandle]] = defaultdict(list)
    for item in complete:
        by_ticker[item.ticker].append(item)
    ordered = {key: tuple(value) for key, value in sorted(by_ticker.items())}

    rows: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    selected = frozenset(request.selected_hypotheses)
    if selected & {
        ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
        ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
    }:
        rows.extend(
            _morning_rows(
                ordered,
                selected,
                request.policy,
                request.market_universe,
            )
        )
    if selected & {
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
    }:
        rows.extend(_jump_rows(ordered, selected, request.policy))
    if ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3 in selected:
        rows.extend(_relative_volume_rows(ordered, request.policy))
    if ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE in selected:
        rows.extend(_phase_recurrence_rows(ordered, request.policy))
    if ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION in selected:
        rows.extend(
            _open_close_rows(ordered, request.policy, request.market_universe)
        )
    if ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK in selected:
        rows.extend(_semivariance_rows(ordered, request.policy))
    if ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE in selected:
        rows.extend(_volatility_jump_rows(ordered, request.policy))
    har_parameters: HarV2Parameters | None = None
    if ProspectiveHypothesis.HAR_VOLATILITY_V2 in selected:
        har_rows, har_parameters = _har_v2_rows(ordered, split, request.policy)
        rows.extend(har_rows)
    pair_parameters: tuple[FrozenPairParameters, ...] = ()
    if ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION in selected:
        pair_rows, pair_parameters = _pair_reversion_rows(
            ordered,
            split,
            request.policy,
            request.pair_candidates,
        )
        rows.extend(pair_rows)

    rows.sort(
        key=lambda item: (
            item[0].observed_at,
            item[0].ticker,
            item[0].hypothesis.value,
            item[0].horizon_seconds,
        )
    )
    features = tuple(item[0] for item in rows)
    outcomes = tuple(item[1] for item in rows)
    fingerprint = _report_fingerprint(
        dataset_fingerprint,
        request,
        features,
        outcomes,
        har_parameters,
        pair_parameters,
    )
    return ProspectiveScientificReport(
        dataset_fingerprint=dataset_fingerprint,
        report_fingerprint=fingerprint,
        split=split,
        policy=request.policy,
        selected_hypotheses=request.selected_hypotheses,
        har_v2_parameters=har_parameters,
        pair_parameters=pair_parameters,
        features=features,
        outcomes=outcomes,
    )


def build_partitioned_prospective_scientific_research(
    cache: PartitionedProspectiveCandleCachePort,
    *,
    dataset_fingerprint: str,
    request: ProspectiveScientificRequest,
) -> ProspectiveScientificReport:
    """Build one model while retaining at most one ordinary ticker partition.

    Cross-sectional models retain only their fixed, pre-registered universe or
    pair members.  All other models emit feature/outcome rows as soon as one
    ticker has been evaluated, so the input candle graph is never global.
    """

    if len(request.selected_hypotheses) != 1:
        raise ValueError("partitioned replay requires exactly one hypothesis")
    if not dataset_fingerprint.startswith("sha256:"):
        raise ValueError("dataset_fingerprint must use sha256")
    hypothesis = request.selected_hypotheses[0]
    selected = frozenset((hypothesis,))
    all_days: set[date] = set()
    seen_tickers: set[str] = set()
    feature_outcomes: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    retained: dict[str, tuple[HistoricalCandle, ...]] = {}
    morning_candidates: list[_MorningCandidate] = []
    har_candidates: list[_HarCandidate] = []
    pair_tickers = frozenset(
        ticker for pair in request.pair_candidates for ticker in pair
    )
    universe = frozenset(request.market_universe)

    for raw_partition in cache.iter_ticker_partitions():
        rows = tuple(item for item in raw_partition if item.complete)
        if not rows:
            continue
        ticker = rows[0].ticker
        if ticker in seen_tickers or any(item.ticker != ticker for item in rows):
            raise ValueError("partitioned candles require one unique ticker partition")
        if any(left.at >= right.at for left, right in zip(rows, rows[1:])):
            raise ValueError("partitioned candles must be strictly time ordered")
        seen_tickers.add(ticker)
        all_days.update(_trading_day(item.at) for item in rows)
        by_ticker = {ticker: rows}
        if hypothesis in {
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
        }:
            morning_candidates.extend(
                _build_morning_candidates(
                    ticker,
                    rows,
                    tuple(
                        sorted(
                            set(request.policy.morning_reversion_horizons_seconds)
                            | set(
                                request.policy.morning_continuation_horizons_seconds
                            )
                        )
                    ),
                )
            )
        elif hypothesis in {
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        }:
            feature_outcomes.extend(
                _jump_rows(by_ticker, selected, request.policy)
            )
        elif hypothesis is ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3:
            feature_outcomes.extend(_relative_volume_rows(by_ticker, request.policy))
        elif hypothesis is ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE:
            feature_outcomes.extend(_phase_recurrence_rows(by_ticker, request.policy))
        elif hypothesis is ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK:
            feature_outcomes.extend(_semivariance_rows(by_ticker, request.policy))
        elif hypothesis is ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE:
            feature_outcomes.extend(_volatility_jump_rows(by_ticker, request.policy))
        elif hypothesis is ProspectiveHypothesis.HAR_VOLATILITY_V2:
            har_candidates.extend(_build_har_candidates(ticker, rows, request.policy))
        elif (
            hypothesis is ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION
            and ticker in universe
        ):
            retained[ticker] = rows
        elif (
            hypothesis is ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION
            and ticker in pair_tickers
        ):
            retained[ticker] = rows

    if not all_days:
        raise ValueError("prospective research requires complete candles")
    split = chronological_split_60_20_20(tuple(sorted(all_days)))
    pair_parameters: tuple[FrozenPairParameters, ...] = ()
    har_parameters: HarV2Parameters | None = None
    if morning_candidates:
        feature_outcomes.extend(
            _evaluate_morning_candidates(
                morning_candidates,
                selected,
                request.policy,
                request.market_universe,
            )
        )
    if hypothesis is ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION:
        feature_outcomes.extend(
            _open_close_rows(retained, request.policy, request.market_universe)
        )
    if hypothesis is ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION:
        pair_rows, pair_parameters = _pair_reversion_rows(
            retained,
            split,
            request.policy,
            request.pair_candidates,
        )
        feature_outcomes.extend(pair_rows)
    if hypothesis is ProspectiveHypothesis.HAR_VOLATILITY_V2:
        har_rows, har_parameters = _evaluate_har_candidates(
            har_candidates,
            split,
            request.policy,
        )
        feature_outcomes.extend(har_rows)

    feature_outcomes.sort(
        key=lambda item: (
            item[0].observed_at,
            item[0].ticker,
            item[0].hypothesis.value,
            item[0].horizon_seconds,
        )
    )
    features = tuple(item[0] for item in feature_outcomes)
    outcomes = tuple(item[1] for item in feature_outcomes)
    fingerprint = _report_fingerprint(
        dataset_fingerprint,
        request,
        features,
        outcomes,
        har_parameters,
        pair_parameters,
    )
    return ProspectiveScientificReport(
        dataset_fingerprint=dataset_fingerprint,
        report_fingerprint=fingerprint,
        split=split,
        policy=request.policy,
        selected_hypotheses=request.selected_hypotheses,
        har_v2_parameters=har_parameters,
        pair_parameters=pair_parameters,
        features=features,
        outcomes=outcomes,
    )


@dataclass(frozen=True, slots=True)
class _MorningCandidate:
    ticker: str
    trading_day: date
    observed_at: datetime
    feature_max_observed_at: datetime
    deviation_bps: float
    cumulative_volume: float
    range_bps: float
    trading_gap: bool
    forward_returns_bps: tuple[tuple[int, float | None], ...]


def _morning_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    selected: frozenset[ProspectiveHypothesis],
    policy: ProspectiveScientificPolicy,
    market_universe: tuple[str, ...],
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_MorningCandidate] = []
    all_horizons = tuple(
        sorted(
            set(policy.morning_reversion_horizons_seconds)
            | set(policy.morning_continuation_horizons_seconds)
        )
    )
    for ticker, rows in by_ticker.items():
        candidates.extend(_build_morning_candidates(ticker, rows, all_horizons))
    return _evaluate_morning_candidates(
        candidates,
        selected,
        policy,
        market_universe,
    )


def _build_morning_candidates(
    ticker: str,
    rows: tuple[HistoricalCandle, ...],
    horizons: tuple[int, ...],
) -> list[_MorningCandidate]:
    candidates: list[_MorningCandidate] = []
    by_day = _indexed_rows_by_day(rows)
    previous_close: float | None = None
    for trading_day, day_rows in sorted(by_day.items()):
        opening = day_rows.get(10 * 60)
        morning = tuple(
            candle
            for minute, (_, candle) in sorted(day_rows.items())
            if 7 * 60 <= minute <= 9 * 60 + 49
        )
        if previous_close is not None and opening is not None and morning:
            opening_index, opening_candle = opening
            high = max(item.high for item in morning)
            low = min(item.low for item in morning)
            candidates.append(
                _MorningCandidate(
                    ticker=ticker,
                    trading_day=trading_day,
                    observed_at=opening_candle.at,
                    feature_max_observed_at=_observed_at(morning[-1]),
                    deviation_bps=(morning[-1].close / previous_close - 1.0)
                    * 10_000.0,
                    cumulative_volume=sum(item.volume for item in morning),
                    range_bps=(high / low - 1.0) * 10_000.0,
                    trading_gap=len(morning) != 170 or not _continuous(morning),
                    forward_returns_bps=tuple(
                        (
                            horizon,
                            _forward_return_from_open(
                                rows, opening_index, horizon // 60
                            ),
                        )
                        for horizon in horizons
                    ),
                )
            )
        main_closes = tuple(
            candle.close
            for minute, (_, candle) in sorted(day_rows.items())
            if 10 * 60 <= minute <= 18 * 60 + 39
        )
        if main_closes:
            previous_close = main_closes[-1]
    return candidates


def _evaluate_morning_candidates(
    candidates: Sequence[_MorningCandidate],
    selected: frozenset[ProspectiveHypothesis],
    policy: ProspectiveScientificPolicy,
    market_universe: tuple[str, ...],
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    universe = frozenset(market_universe)
    market_by_day: dict[date, tuple[float, float]] = {}
    for trading_day, day_items in _items_by_day(candidates):
        member_returns = tuple(
            item.deviation_bps for item in day_items if item.ticker in universe
        )
        market_by_day[trading_day] = (
            fmean(member_returns) if member_returns else 0.0,
            len(member_returns) / len(universe) if universe else 0.0,
        )

    histories: defaultdict[
        str, deque[tuple[float, float, float, datetime]]
    ] = defaultdict(lambda: deque(maxlen=policy.morning_history_days))
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    all_horizons = tuple(
        sorted(
            set(policy.morning_reversion_horizons_seconds)
            | set(policy.morning_continuation_horizons_seconds)
        )
    )
    for trading_day, day_items in _items_by_day(candidates):
        market_return, market_coverage = market_by_day[trading_day]
        for item in day_items:
            history = histories[item.ticker]
            deviations = tuple(value[0] for value in history)
            volumes = tuple(value[1] for value in history)
            ranges = tuple(value[2] for value in history)
            deviation_std = pstdev(deviations) if len(deviations) > 1 else 0.0
            deviation_z = (
                (item.deviation_bps - fmean(deviations)) / deviation_std
                if deviation_std > 0.0
                else 0.0
            )
            volume_mean = fmean(volumes) if volumes else 0.0
            relative_volume = (
                item.cumulative_volume / volume_mean if volume_mean > 0.0 else 0.0
            )
            range_percentile = _percentile_rank(ranges, item.range_bps)
            valid_baseline = deviation_std > 0.0 and volume_mean > 0.0
            for horizon in all_horizons:
                h1, h2 = morning_regime_features(
                    ticker=item.ticker,
                    trading_day=trading_day,
                    observed_at=item.observed_at,
                    feature_max_observed_at=item.feature_max_observed_at,
                    horizon_seconds=horizon,
                    morning_deviation_bps=item.deviation_bps,
                    morning_deviation_z=deviation_z,
                    cumulative_relative_volume=relative_volume,
                    morning_range_percentile=range_percentile,
                    market_return_bps=market_return,
                    market_coverage=market_coverage,
                    history_count=len(history),
                    history_observed_until=history[-1][3] if history else None,
                    trading_gap=item.trading_gap,
                    valid_baseline=valid_baseline,
                    policy=policy,
                )
                for feature, allowed_horizons in (
                    (h1, policy.morning_reversion_horizons_seconds),
                    (h2, policy.morning_continuation_horizons_seconds),
                ):
                    if (
                        feature.hypothesis not in selected
                        or horizon not in allowed_horizons
                    ):
                        continue
                    forward = dict(item.forward_returns_bps)[horizon]
                    result.append(
                        (
                            feature,
                            directional_outcome(
                                feature,
                                target_at=item.observed_at
                                + timedelta(seconds=horizon),
                                forward_return_bps=forward,
                                round_trip_cost_bps=policy.round_trip_cost_bps,
                            ),
                        )
                    )
        for item in day_items:
            if not item.trading_gap:
                histories[item.ticker].append(
                    (
                        item.deviation_bps,
                        item.cumulative_volume,
                        item.range_bps,
                        item.feature_max_observed_at,
                    )
                )
    return result


@dataclass(frozen=True, slots=True)
class _DirectionalCandidate:
    ticker: str
    trading_day: date
    bucket: int
    observed_at: datetime
    index: int
    rows: tuple[HistoricalCandle, ...]
    raw_return_bps: float | None


def _phase_recurrence_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ProspectiveScientificPolicy,
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_DirectionalCandidate] = []
    minutes = policy.phase_recurrence_horizon_seconds // 60
    for ticker, rows in by_ticker.items():
        for index, candle in enumerate(rows):
            local = candle.at.astimezone(MOSCOW)
            observed_at = candle.at
            if local.minute % 30 or not _eligible(observed_at):
                continue
            candidates.append(
                _DirectionalCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(candle.at),
                    bucket=_clock_bucket(candle.at, 30),
                    observed_at=observed_at,
                    index=index,
                    rows=rows,
                    raw_return_bps=_forward_return_from_open(rows, index, minutes),
                )
            )
    histories: defaultdict[
        tuple[str, int], deque[tuple[float, datetime]]
    ] = defaultdict(lambda: deque(maxlen=policy.phase_recurrence_history_days))
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day, day_items in _items_by_day(candidates):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            feature = phase_recurrence_feature(
                ticker=item.ticker,
                trading_day=trading_day,
                observed_at=item.observed_at,
                historical_same_phase_returns_bps=(value[0] for value in history),
                history_observed_until=history[-1][1] if history else None,
                trading_gap=False,
                policy=policy,
            )
            result.append(
                (
                    feature,
                    directional_outcome(
                        feature,
                        target_at=item.observed_at
                        + timedelta(seconds=policy.phase_recurrence_horizon_seconds),
                        forward_return_bps=item.raw_return_bps,
                        round_trip_cost_bps=policy.round_trip_cost_bps,
                    ),
                )
            )
        for item in day_items:
            if item.raw_return_bps is not None:
                histories[(item.ticker, item.bucket)].append(
                    (
                        item.raw_return_bps,
                        item.observed_at
                        + timedelta(seconds=policy.phase_recurrence_horizon_seconds),
                    )
                )
    return result


def _open_close_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ProspectiveScientificPolicy,
    market_universe: tuple[str, ...],
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    indexed = {
        ticker: _indexed_rows_by_day(rows)
        for ticker, rows in by_ticker.items()
        if ticker in frozenset(market_universe)
    }
    trading_days = sorted(
        {trading_day for by_day in indexed.values() for trading_day in by_day}
    )
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day in trading_days:
        opening_returns: list[float] = []
        closing_returns: list[float] = []
        opening_cutoffs: list[datetime] = []
        closing_starts: list[datetime] = []
        complete_members = 0
        for ticker in market_universe:
            day_rows = indexed.get(ticker, {}).get(trading_day)
            if day_rows is None:
                continue
            opening = _minute_window(day_rows, 10 * 60, 30)
            closing = _minute_window(day_rows, 18 * 60 + 10, 30)
            if opening is None or closing is None:
                continue
            complete_members += 1
            opening_returns.append(
                (opening[-1].close / opening[0].open - 1.0) * 10_000.0
            )
            closing_returns.append(
                (closing[-1].close / closing[0].open - 1.0) * 10_000.0
            )
            opening_cutoffs.append(_observed_at(opening[-1]))
            closing_starts.append(closing[0].at)
        coverage = (
            complete_members / len(market_universe) if market_universe else 0.0
        )
        if not opening_cutoffs or not closing_starts:
            continue
        observed_at = max(closing_starts)
        feature = open_close_basket_feature(
            trading_day=trading_day,
            observed_at=observed_at,
            feature_max_observed_at=max(opening_cutoffs),
            opening_basket_return_bps=fmean(opening_returns),
            basket_coverage=coverage,
            shortened_session=complete_members == 0,
            policy=policy,
        )
        result.append(
            (
                feature,
                directional_outcome(
                    feature,
                    target_at=observed_at
                    + timedelta(seconds=policy.open_close_horizon_seconds),
                    forward_return_bps=fmean(closing_returns),
                    round_trip_cost_bps=policy.round_trip_cost_bps,
                ),
            )
        )
    return result


def _pair_reversion_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    split: ChronologicalSplit,
    policy: ProspectiveScientificPolicy,
    pair_candidates: tuple[tuple[str, str], ...],
) -> tuple[
    list[tuple[ProspectiveFeature, ProspectiveOutcome]],
    tuple[FrozenPairParameters, ...],
]:
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    fitted: list[FrozenPairParameters] = []
    for left_ticker, right_ticker in pair_candidates:
        left = {item.at: item for item in by_ticker.get(left_ticker, ())}
        right = {item.at: item for item in by_ticker.get(right_ticker, ())}
        shared = tuple(sorted(set(left) & set(right)))
        training_at = tuple(
            at
            for at in shared
            if split.partition_for(_trading_day(at)) is DatasetPartition.TRAIN
            and _eligible(_observed_at(left[at]))
            and at.astimezone(MOSCOW).minute % 30 == 29
        )
        parameters = _fit_pair_parameters(
            left_ticker,
            right_ticker,
            left,
            right,
            training_at,
            policy,
        )
        if parameters is not None:
            fitted.append(parameters)
        evaluation_at = tuple(
            at
            for at in shared
            if split.partition_for(_trading_day(at)) is not DatasetPartition.TRAIN
            and _eligible(_observed_at(left[at]))
            and at.astimezone(MOSCOW).minute % 30 == 29
        )
        positions = {at: index for index, at in enumerate(shared)}
        for at in evaluation_at:
            index = positions[at]
            previous_at = shared[index - 1] if index else None
            current_left, current_right = left[at], right[at]
            corporate_action = False
            if previous_at is not None:
                corporate_action = (
                    abs(current_left.close / left[previous_at].close - 1.0) >= 0.20
                    or abs(current_right.close / right[previous_at].close - 1.0) >= 0.20
                )
            observed_at = _observed_at(current_left)
            for horizon in policy.pair_horizons_seconds:
                feature = pair_residual_reversion_feature(
                    left_ticker=left_ticker,
                    right_ticker=right_ticker,
                    trading_day=_trading_day(at),
                    observed_at=observed_at,
                    left_price=current_left.close,
                    right_price=current_right.close,
                    parameters=parameters,
                    corporate_action_suspected=corporate_action,
                    liquid=current_left.volume > 0.0 and current_right.volume > 0.0,
                    policy=policy,
                    horizon_seconds=horizon,
                )
                target_at = observed_at + timedelta(seconds=horizon)
                future_left = left.get(target_at - timedelta(minutes=1))
                future_right = right.get(target_at - timedelta(minutes=1))
                forward: float | None = None
                expected_times = tuple(
                    at + timedelta(minutes=minute)
                    for minute in range(1, horizon // 60 + 1)
                )
                if (
                    parameters is not None
                    and future_left is not None
                    and future_right is not None
                    and _trading_day(future_left.at) == _trading_day(at)
                    and all(
                        expected_at in left and expected_at in right
                        for expected_at in expected_times
                    )
                ):
                    current_spread = parameters.spread(
                        current_left.close, current_right.close
                    )
                    future_spread = parameters.spread(
                        future_left.close, future_right.close
                    )
                    forward = (future_spread - current_spread) * 10_000.0
                result.append(
                    (
                        feature,
                        directional_outcome(
                            feature,
                            target_at=target_at,
                            forward_return_bps=forward,
                            round_trip_cost_bps=policy.round_trip_cost_bps,
                        ),
                    )
                )
    return result, tuple(fitted)


def _fit_pair_parameters(
    left_ticker: str,
    right_ticker: str,
    left: dict[datetime, HistoricalCandle],
    right: dict[datetime, HistoricalCandle],
    training_at: tuple[datetime, ...],
    policy: ProspectiveScientificPolicy,
) -> FrozenPairParameters | None:
    if len(training_at) < policy.pair_min_training_points:
        return None
    x = tuple(log(right[at].close) for at in training_at)
    y = tuple(log(left[at].close) for at in training_at)
    x_mean, y_mean = fmean(x), fmean(y)
    x_variance = sum((value - x_mean) ** 2 for value in x)
    y_variance = sum((value - y_mean) ** 2 for value in y)
    if x_variance <= 0.0 or y_variance <= 0.0:
        return None
    covariance = sum(
        (left_value - x_mean) * (right_value - y_mean)
        for left_value, right_value in zip(x, y, strict=True)
    )
    hedge_ratio = covariance / x_variance
    intercept = y_mean - hedge_ratio * x_mean
    spreads = tuple(
        y_value - intercept - hedge_ratio * x_value
        for x_value, y_value in zip(x, y, strict=True)
    )
    spread_std = pstdev(spreads)
    if spread_std <= 0.0:
        return None
    return FrozenPairParameters(
        left_ticker=left_ticker,
        right_ticker=right_ticker,
        intercept=intercept,
        hedge_ratio=hedge_ratio,
        spread_mean=fmean(spreads),
        spread_std=spread_std,
        correlation=covariance / sqrt(x_variance * y_variance),
        training_points=len(training_at),
        trained_until=max(_observed_at(left[at]) for at in training_at),
    )


@dataclass(frozen=True, slots=True)
class _JumpCandidate:
    ticker: str
    trading_day: date
    bucket: int
    observed_at: datetime
    index: int
    rows: tuple[HistoricalCandle, ...]
    signed_return_bps: float
    volume: float
    range_bps: float
    illiquidity: float

    @property
    def history_point(self) -> JumpHistoryPoint:
        return JumpHistoryPoint(
            absolute_return_bps=abs(self.signed_return_bps),
            volume=self.volume,
            range_bps=self.range_bps,
            illiquidity=self.illiquidity,
        )


def _jump_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    selected: frozenset[ProspectiveHypothesis],
    policy: ProspectiveScientificPolicy,
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_JumpCandidate] = []
    for ticker, rows in by_ticker.items():
        for index, candle in enumerate(rows):
            observed_at = _observed_at(candle)
            if observed_at.minute % policy.jump_window_minutes or not _eligible(
                observed_at
            ):
                continue
            window = _window_ending(rows, index, policy.jump_window_minutes)
            if window is None:
                continue
            signed_return = (window[-1].close / window[0].open - 1.0) * 10_000.0
            volume = sum(item.volume for item in window)
            high = max(item.high for item in window)
            low = min(item.low for item in window)
            range_bps = (high / low - 1.0) * 10_000.0
            turnover = sum(item.close * item.volume for item in window)
            candidates.append(
                _JumpCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(candle.at),
                    bucket=_clock_bucket(candle.at, policy.jump_window_minutes),
                    observed_at=observed_at,
                    index=index,
                    rows=rows,
                    signed_return_bps=signed_return,
                    volume=volume,
                    range_bps=range_bps,
                    illiquidity=abs(signed_return)
                    / max(turnover, 1.0)
                    * 1_000_000_000.0,
                )
            )
    histories: defaultdict[
        tuple[str, int], deque[tuple[JumpHistoryPoint, datetime]]
    ] = defaultdict(lambda: deque(maxlen=policy.jump_history_days))
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day, day_items in _items_by_day(candidates):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            values = tuple(value[0] for value in history)
            history_until = history[-1][1] if history else None
            for horizon in policy.jump_horizons_seconds:
                h3, h4 = jump_regime_features(
                    ticker=item.ticker,
                    trading_day=trading_day,
                    observed_at=item.observed_at,
                    horizon_seconds=horizon,
                    signed_return_bps=item.signed_return_bps,
                    volume=item.volume,
                    range_bps=item.range_bps,
                    illiquidity=item.illiquidity,
                    prior_history=values,
                    history_observed_until=history_until,
                    trading_gap=False,
                    policy=policy,
                )
                forward = _forward_return(item.rows, item.index, horizon // 60)
                for feature in (h3, h4):
                    if feature.hypothesis not in selected:
                        continue
                    result.append(
                        (
                            feature,
                            directional_outcome(
                                feature,
                                target_at=item.observed_at + timedelta(seconds=horizon),
                                forward_return_bps=forward,
                                round_trip_cost_bps=policy.round_trip_cost_bps,
                            ),
                        )
                    )
        for item in day_items:
            histories[(item.ticker, item.bucket)].append(
                (item.history_point, item.observed_at)
            )
    return result


@dataclass(frozen=True, slots=True)
class _VarianceEventCandidate:
    ticker: str
    trading_day: date
    bucket: int
    observed_at: datetime
    target_at: datetime
    trigger_value: float
    secondary_value: float
    future_variance: float | None


def _relative_volume_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ProspectiveScientificPolicy,
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_VarianceEventCandidate] = []
    for ticker, rows in by_ticker.items():
        for index, candle in enumerate(rows):
            observed_at = _observed_at(candle)
            if observed_at.minute % policy.volume_window_minutes or not _eligible(
                observed_at
            ):
                continue
            window = _window_ending(rows, index, policy.volume_window_minutes)
            if window is None:
                continue
            candidates.append(
                _VarianceEventCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(candle.at),
                    bucket=_clock_bucket(candle.at, policy.volume_window_minutes),
                    observed_at=observed_at,
                    target_at=observed_at
                    + timedelta(seconds=policy.volume_horizon_seconds),
                    trigger_value=sum(item.volume for item in window),
                    secondary_value=0.0,
                    future_variance=_future_variance(
                        rows,
                        index,
                        policy.volume_horizon_seconds // 60,
                    ),
                )
            )
    histories: defaultdict[tuple[str, int], deque[tuple[float, float, datetime]]] = (
        defaultdict(lambda: deque(maxlen=policy.volume_history_days))
    )
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day, day_items in _items_by_day(candidates):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            volumes = tuple(value[0] for value in history)
            variances = tuple(value[1] for value in history)
            feature = relative_volume_volatility_feature(
                ticker=item.ticker,
                trading_day=trading_day,
                observed_at=item.observed_at,
                current_volume=item.trigger_value,
                historical_volumes=volumes,
                baseline_future_variance=median(variances) if variances else 0.0,
                history_observed_until=history[-1][2] if history else None,
                trading_gap=False,
                policy=policy,
            )
            result.append(
                (
                    feature,
                    variance_uplift_outcome(
                        feature,
                        target_at=item.target_at,
                        actual_future_variance=item.future_variance,
                    ),
                )
            )
        for item in day_items:
            if item.future_variance is not None:
                histories[(item.ticker, item.bucket)].append(
                    (item.trigger_value, item.future_variance, item.target_at)
                )
    return result


def _semivariance_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ProspectiveScientificPolicy,
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_VarianceEventCandidate] = []
    for ticker, rows in by_ticker.items():
        for index, candle in enumerate(rows):
            observed_at = _observed_at(candle)
            if observed_at.minute % policy.semivariance_window_minutes or not _eligible(
                observed_at
            ):
                continue
            window = _window_ending(rows, index, policy.semivariance_window_minutes)
            if window is None:
                continue
            returns = _log_returns(window)
            variance = sum(value * value for value in returns)
            downside = sum(value * value for value in returns if value < 0.0)
            candidates.append(
                _VarianceEventCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(candle.at),
                    bucket=_clock_bucket(candle.at, policy.semivariance_window_minutes),
                    observed_at=observed_at,
                    target_at=observed_at
                    + timedelta(seconds=policy.semivariance_horizon_seconds),
                    trigger_value=downside / variance if variance > 0.0 else 0.0,
                    secondary_value=variance,
                    future_variance=_future_variance(
                        rows,
                        index,
                        policy.semivariance_horizon_seconds // 60,
                    ),
                )
            )
    histories: defaultdict[tuple[str, int], deque[tuple[float, float, datetime]]] = (
        defaultdict(lambda: deque(maxlen=policy.semivariance_history_days))
    )
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day, day_items in _items_by_day(candidates):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            shares = tuple(value[0] for value in history)
            variances = tuple(value[1] for value in history)
            feature = downside_semivariance_feature(
                ticker=item.ticker,
                trading_day=trading_day,
                observed_at=item.observed_at,
                downside_share=item.trigger_value,
                historical_downside_shares=shares,
                baseline_future_variance=median(variances) if variances else 0.0,
                history_observed_until=history[-1][2] if history else None,
                trading_gap=False,
                policy=policy,
            )
            result.append(
                (
                    feature,
                    variance_uplift_outcome(
                        feature,
                        target_at=item.target_at,
                        actual_future_variance=item.future_variance,
                    ),
                )
            )
        for item in day_items:
            if item.future_variance is not None:
                histories[(item.ticker, item.bucket)].append(
                    (item.trigger_value, item.future_variance, item.target_at)
                )
    return result


def _volatility_jump_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ProspectiveScientificPolicy,
) -> list[tuple[ProspectiveFeature, ProspectiveOutcome]]:
    candidates: list[_VarianceEventCandidate] = []
    for ticker, rows in by_ticker.items():
        for index, candle in enumerate(rows):
            observed_at = _observed_at(candle)
            if (
                observed_at.minute % policy.jump_variance_window_minutes
                or not _eligible(observed_at)
            ):
                continue
            window = _window_ending(rows, index, policy.jump_variance_window_minutes)
            if window is None:
                continue
            returns = _log_returns(window)
            variance = sum(value * value for value in returns)
            bipower = (pi / 2.0) * sum(
                abs(previous) * abs(current)
                for previous, current in zip(returns, returns[1:])
            )
            jump_variance = max(variance - bipower, 0.0)
            candidates.append(
                _VarianceEventCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(candle.at),
                    bucket=_clock_bucket(
                        candle.at, policy.jump_variance_window_minutes
                    ),
                    observed_at=observed_at,
                    target_at=observed_at
                    + timedelta(seconds=policy.jump_variance_horizon_seconds),
                    trigger_value=jump_variance / variance if variance > 0.0 else 0.0,
                    secondary_value=bipower,
                    future_variance=_future_variance(
                        rows,
                        index,
                        policy.jump_variance_horizon_seconds // 60,
                    ),
                )
            )
    histories: defaultdict[tuple[str, int], deque[tuple[float, float, datetime]]] = (
        defaultdict(lambda: deque(maxlen=policy.jump_variance_history_days))
    )
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    for trading_day, day_items in _items_by_day(candidates):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            shares = tuple(value[0] for value in history)
            variances = tuple(value[1] for value in history)
            feature = volatility_jump_feature(
                ticker=item.ticker,
                trading_day=trading_day,
                observed_at=item.observed_at,
                jump_share=item.trigger_value,
                continuous_variance=item.secondary_value,
                historical_jump_shares=shares,
                baseline_future_variance=median(variances) if variances else 0.0,
                history_observed_until=history[-1][2] if history else None,
                trading_gap=False,
                policy=policy,
            )
            result.append(
                (
                    feature,
                    variance_uplift_outcome(
                        feature,
                        target_at=item.target_at,
                        actual_future_variance=item.future_variance,
                    ),
                )
            )
        for item in day_items:
            if item.future_variance is not None:
                histories[(item.ticker, item.bucket)].append(
                    (item.trigger_value, item.future_variance, item.target_at)
                )
    return result


@dataclass(frozen=True, slots=True)
class _HarCandidate:
    ticker: str
    trading_day: date
    bucket: int
    observed_at: datetime
    target_at: datetime
    short_variance: float
    medium_variance: float
    long_variance: float
    future_variance: float | None


def _har_v2_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    split: ChronologicalSplit,
    policy: ProspectiveScientificPolicy,
) -> tuple[list[tuple[ProspectiveFeature, ProspectiveOutcome]], HarV2Parameters | None]:
    candidates: list[_HarCandidate] = []
    for ticker, rows in by_ticker.items():
        candidates.extend(_build_har_candidates(ticker, rows, policy))
    return _evaluate_har_candidates(candidates, split, policy)


def _build_har_candidates(
    ticker: str,
    rows: tuple[HistoricalCandle, ...],
    policy: ProspectiveScientificPolicy,
) -> list[_HarCandidate]:
    candidates: list[_HarCandidate] = []
    short, medium, long_window = policy.har_windows_minutes
    for index, candle in enumerate(rows):
        observed_at = _observed_at(candle)
        if observed_at.minute % 30 or not _eligible(observed_at):
            continue
        window = _window_ending(rows, index, long_window)
        if window is None:
            continue
        candidates.append(
            _HarCandidate(
                ticker=ticker,
                trading_day=_trading_day(candle.at),
                bucket=_clock_bucket(candle.at, 30),
                observed_at=observed_at,
                target_at=observed_at
                + timedelta(seconds=policy.har_horizon_seconds),
                short_variance=_realized_variance(window[-short:]),
                medium_variance=_realized_variance(window[-medium:]),
                long_variance=_realized_variance(window),
                future_variance=_future_variance(
                    rows,
                    index,
                    policy.har_horizon_seconds // 60,
                ),
            )
        )
    return candidates


def _evaluate_har_candidates(
    candidates: Sequence[_HarCandidate],
    split: ChronologicalSplit,
    policy: ProspectiveScientificPolicy,
) -> tuple[list[tuple[ProspectiveFeature, ProspectiveOutcome]], HarV2Parameters | None]:
    training = tuple(
        HarV2TrainingPoint(
            feature_at=item.observed_at,
            target_at=item.target_at,
            short_variance=item.short_variance,
            medium_variance=item.medium_variance,
            long_variance=item.long_variance,
            target_variance=item.future_variance,
        )
        for item in candidates
        if split.partition_for(item.trading_day) is DatasetPartition.TRAIN
        and item.future_variance is not None
    )
    parameters = (
        fit_har_v2_parameters(
            training,
            minimum_points=policy.har_minimum_training_points,
            ridge_penalty=policy.har_ridge_penalty,
        )
        if len(training) >= policy.har_minimum_training_points
        else None
    )
    histories: defaultdict[tuple[str, int], list[tuple[float, datetime]]] = defaultdict(
        list
    )
    for item in candidates:
        if (
            split.partition_for(item.trading_day) is DatasetPartition.TRAIN
            and item.future_variance is not None
        ):
            histories[(item.ticker, item.bucket)].append(
                (item.future_variance, item.target_at)
            )
    result: list[tuple[ProspectiveFeature, ProspectiveOutcome]] = []
    evaluation = tuple(
        item
        for item in candidates
        if split.partition_for(item.trading_day) is not DatasetPartition.TRAIN
    )
    for _, day_items in _items_by_day(evaluation):
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            history_values = tuple(value[0] for value in history)
            feature = har_v2_feature(
                ticker=item.ticker,
                trading_day=item.trading_day,
                observed_at=item.observed_at,
                short_variance=item.short_variance,
                medium_variance=item.medium_variance,
                long_variance=item.long_variance,
                parameters=parameters,
                horizon_seconds=policy.har_horizon_seconds,
            )
            result.append(
                (
                    feature,
                    har_v2_outcome(
                        feature,
                        target_at=item.target_at,
                        actual_future_variance=item.future_variance,
                        ewma_baseline=(
                            _ewma(history_values, policy.har_ewma_alpha)
                            if history_values
                            else None
                        ),
                        phase_baseline=(
                            median(history_values) if history_values else None
                        ),
                    ),
                )
            )
        for item in day_items:
            if item.future_variance is not None:
                histories[(item.ticker, item.bucket)].append(
                    (item.future_variance, item.target_at)
                )
    return result, parameters


class _DatedCandidate(Protocol):
    ticker: str
    trading_day: date
    observed_at: datetime


_Candidate = TypeVar("_Candidate", bound=_DatedCandidate)


def _items_by_day(
    items: Sequence[_Candidate],
) -> tuple[tuple[date, tuple[_Candidate, ...]], ...]:
    grouped: defaultdict[date, list[_Candidate]] = defaultdict(list)
    for item in items:
        grouped[item.trading_day].append(item)
    return tuple(
        (
            trading_day,
            tuple(
                sorted(
                    day_items,
                    key=lambda item: (item.observed_at, item.ticker),
                )
            ),
        )
        for trading_day, day_items in sorted(grouped.items())
    )


def _window_ending(
    rows: Sequence[HistoricalCandle],
    end_index: int,
    minutes: int,
) -> tuple[HistoricalCandle, ...] | None:
    start = end_index - minutes + 1
    if start < 0:
        return None
    window = tuple(rows[start : end_index + 1])
    if len(window) != minutes or _trading_day(window[0].at) != _trading_day(
        window[-1].at
    ):
        return None
    if not _continuous(window):
        return None
    if any(not _eligible(_observed_at(item)) for item in window):
        return None
    return window


def _future_variance(
    rows: Sequence[HistoricalCandle],
    anchor_index: int,
    minutes: int,
) -> float | None:
    future = _future_window(rows, anchor_index, minutes)
    return _realized_variance(future) if future is not None else None


def _forward_return(
    rows: Sequence[HistoricalCandle],
    anchor_index: int,
    minutes: int,
) -> float | None:
    future = _future_window(rows, anchor_index, minutes)
    if future is None:
        return None
    return (future[-1].close / future[0].close - 1.0) * 10_000.0


def _future_window(
    rows: Sequence[HistoricalCandle],
    anchor_index: int,
    minutes: int,
) -> tuple[HistoricalCandle, ...] | None:
    end = anchor_index + minutes
    if end >= len(rows):
        return None
    future = tuple(rows[anchor_index : end + 1])
    if len(future) != minutes + 1:
        return None
    if _trading_day(future[0].at) != _trading_day(future[-1].at):
        return None
    if not _continuous(future):
        return None
    if any(not _eligible(_observed_at(item)) for item in future):
        return None
    return future


def _continuous(rows: Sequence[HistoricalCandle]) -> bool:
    return all(
        current.at - previous.at == timedelta(minutes=1)
        for previous, current in zip(rows, rows[1:])
    )


def _indexed_rows_by_day(
    rows: Sequence[HistoricalCandle],
) -> dict[date, dict[int, tuple[int, HistoricalCandle]]]:
    grouped: defaultdict[date, dict[int, tuple[int, HistoricalCandle]]] = defaultdict(
        dict
    )
    for index, candle in enumerate(rows):
        local = candle.at.astimezone(MOSCOW)
        grouped[local.date()][local.hour * 60 + local.minute] = (index, candle)
    return dict(grouped)


def _minute_window(
    day_rows: dict[int, tuple[int, HistoricalCandle]],
    start_minute: int,
    minutes: int,
) -> tuple[HistoricalCandle, ...] | None:
    window = tuple(
        day_rows[minute][1]
        for minute in range(start_minute, start_minute + minutes)
        if minute in day_rows
    )
    if len(window) != minutes or not _continuous(window):
        return None
    return window


def _forward_return_from_open(
    rows: Sequence[HistoricalCandle],
    anchor_index: int,
    minutes: int,
) -> float | None:
    if minutes <= 0:
        raise ValueError("forward-return horizon must be positive")
    end_index = anchor_index + minutes - 1
    if end_index >= len(rows):
        return None
    future = tuple(rows[anchor_index : end_index + 1])
    if len(future) != minutes or not _continuous(future):
        return None
    if _trading_day(future[0].at) != _trading_day(future[-1].at):
        return None
    if any(not _eligible(_observed_at(item)) for item in future):
        return None
    return (future[-1].close / future[0].open - 1.0) * 10_000.0


def _percentile_rank(values: Sequence[float], current: float) -> float:
    if not values:
        return 0.0
    return sum(value <= current for value in values) / len(values)


def _log_returns(rows: Sequence[HistoricalCandle]) -> tuple[float, ...]:
    return tuple(
        log(current.close / previous.close) * 10_000.0
        for previous, current in zip(rows, rows[1:])
    )


def _realized_variance(rows: Sequence[HistoricalCandle]) -> float:
    return sum(value * value for value in _log_returns(rows))


def _ewma(values: Sequence[float], alpha: float) -> float:
    estimate = values[0]
    for value in values[1:]:
        estimate = alpha * value + (1.0 - alpha) * estimate
    return estimate


def _eligible(at: datetime) -> bool:
    return MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(at) in _ELIGIBLE_PHASES


def _observed_at(candle: HistoricalCandle) -> datetime:
    return candle.at + timedelta(minutes=1)


def _trading_day(at: datetime) -> date:
    return at.astimezone(MOSCOW).date()


def _clock_bucket(at: datetime, minutes: int) -> int:
    local = at.astimezone(MOSCOW)
    return (local.hour * 60 + local.minute) // minutes


def _report_fingerprint(
    dataset_fingerprint: str,
    request: ProspectiveScientificRequest,
    features: tuple[ProspectiveFeature, ...],
    outcomes: tuple[ProspectiveOutcome, ...],
    parameters: HarV2Parameters | None,
    pair_parameters: tuple[FrozenPairParameters, ...],
) -> str:
    payload = {
        "dataset_fingerprint": dataset_fingerprint,
        "policy": request.policy.version,
        "selected": [item.value for item in request.selected_hypotheses],
        "market_universe": request.market_universe,
        "pair_candidates": request.pair_candidates,
        "features": [
            (
                item.observation_id,
                item.decision.value,
                tuple(
                    (value.name, value.unit.value, value.value)
                    for value in item.feature_values
                ),
            )
            for item in features
        ],
        "outcomes": [
            (
                item.observation_id,
                item.available,
                tuple(
                    (value.name, value.unit.value, value.value)
                    for value in item.measurements
                ),
            )
            for item in outcomes
        ],
        "har_v2": (
            (
                parameters.intercept,
                parameters.short_weight,
                parameters.medium_weight,
                parameters.long_weight,
                parameters.training_points,
                parameters.trained_until.isoformat(),
            )
            if parameters is not None
            else None
        ),
        "pairs": [
            (
                item.pair_id,
                item.intercept,
                item.hedge_ratio,
                item.spread_mean,
                item.spread_std,
                item.correlation,
                item.training_points,
                item.trained_until.isoformat(),
            )
            for item in pair_parameters
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()
