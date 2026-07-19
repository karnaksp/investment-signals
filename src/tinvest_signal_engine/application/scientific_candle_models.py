"""Chronological research workflow for H10, H11, H15, and H7 v2."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from math import log
from statistics import fmean, median
from typing import Protocol, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    HistoricalCandle,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    CausalFeatureVector,
    HarParameters,
    HarTrainingPoint,
    ScientificCandleHypothesis,
    ScientificCandlePolicy,
    ScientificModelOutcome,
    directional_outcome,
    fit_har_parameters,
    har_volatility_feature,
    opening_gap_feature,
    relative_volume_activity_feature,
    residual_reversal_feature,
    variance_outcome,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
    TradingPhase,
)


MOSCOW = ZoneInfo("Europe/Moscow")


class ScientificCandleCachePort(Protocol):
    def describe(self) -> CandleCacheDescriptor: ...

    def load(self) -> tuple[HistoricalCandle, ...]: ...


@dataclass(frozen=True, slots=True)
class ScientificCandleResearchRequest:
    selected_hypotheses: tuple[ScientificCandleHypothesis, ...] = tuple(
        ScientificCandleHypothesis
    )
    market_universe: tuple[str, ...] = ()
    policy: ScientificCandlePolicy = ScientificCandlePolicy()

    def __post_init__(self) -> None:
        if not self.selected_hypotheses:
            raise ValueError("at least one scientific hypothesis must be selected")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("selected hypotheses must be unique")
        normalized = tuple(ticker.strip().upper() for ticker in self.market_universe)
        if any(not ticker for ticker in normalized) or len(set(normalized)) != len(
            normalized
        ):
            raise ValueError("market_universe must contain unique non-empty tickers")
        object.__setattr__(self, "market_universe", normalized)


@dataclass(frozen=True, slots=True)
class ScientificCandleResearchReport:
    dataset_fingerprint: str
    report_fingerprint: str
    split: ChronologicalSplit
    policy: ScientificCandlePolicy
    selected_hypotheses: tuple[ScientificCandleHypothesis, ...]
    har_parameters: HarParameters | None
    features: tuple[CausalFeatureVector, ...]
    outcomes: tuple[ScientificModelOutcome, ...]

    def __post_init__(self) -> None:
        if not self.dataset_fingerprint.startswith("sha256:"):
            raise ValueError("dataset_fingerprint must use sha256")
        if not self.report_fingerprint.startswith("sha256:"):
            raise ValueError("report_fingerprint must use sha256")
        feature_ids = tuple(item.observation_id for item in self.features)
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("research report contains duplicate observations")
        if tuple(item.observation_id for item in self.outcomes) != feature_ids:
            raise ValueError("every feature must have one outcome in the same order")


class BuildScientificCandleModelResearch:
    """Build a deterministic research dataset without broker or storage imports."""

    def __init__(self, cache: ScientificCandleCachePort) -> None:
        self._cache = cache

    def execute(
        self,
        request: ScientificCandleResearchRequest = ScientificCandleResearchRequest(),
    ) -> ScientificCandleResearchReport:
        descriptor = self._cache.describe()
        return build_scientific_candle_model_research(
            self._cache.load(),
            dataset_fingerprint=descriptor.dataset_fingerprint,
            request=request,
        )


def build_scientific_candle_model_research(
    candles: Sequence[HistoricalCandle],
    *,
    dataset_fingerprint: str,
    request: ScientificCandleResearchRequest = ScientificCandleResearchRequest(),
) -> ScientificCandleResearchReport:
    complete = tuple(
        sorted(
            (item for item in candles if item.complete),
            key=lambda item: (item.ticker, item.at),
        )
    )
    if not complete:
        raise ValueError("scientific candle research requires complete candles")
    identities = {(item.ticker, item.at) for item in complete}
    if len(identities) != len(complete):
        raise ValueError("candle input contains duplicate ticker/timestamp rows")
    days = tuple(sorted({_trading_day(item.at) for item in complete}))
    split = chronological_split_60_20_20(days)
    by_ticker: dict[str, tuple[HistoricalCandle, ...]] = {}
    grouped: defaultdict[str, list[HistoricalCandle]] = defaultdict(list)
    for candle in complete:
        grouped[candle.ticker].append(candle)
    for ticker, rows in grouped.items():
        by_ticker[ticker] = tuple(rows)

    policy = request.policy
    selected = set(request.selected_hypotheses)
    feature_outcomes: list[tuple[CausalFeatureVector, ScientificModelOutcome]] = []

    if ScientificCandleHypothesis.OPENING_GAP_REVERSION in selected:
        feature_outcomes.extend(_opening_gap_rows(by_ticker, policy))
    if ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION in selected:
        feature_outcomes.extend(
            _residual_reversal_rows(
                by_ticker,
                policy,
                request.market_universe or tuple(sorted(by_ticker)),
            )
        )

    har_parameters: HarParameters | None = None
    if ScientificCandleHypothesis.HAR_VOLATILITY in selected:
        har_rows, har_parameters = _har_rows(by_ticker, split, policy)
        feature_outcomes.extend(har_rows)
    if ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2 in selected:
        feature_outcomes.extend(_relative_volume_rows(by_ticker, policy))

    feature_outcomes.sort(
        key=lambda pair: (
            pair[0].observed_at,
            pair[0].ticker,
            pair[0].hypothesis.value,
        )
    )
    features = tuple(pair[0] for pair in feature_outcomes)
    outcomes = tuple(pair[1] for pair in feature_outcomes)
    report_fingerprint = _report_fingerprint(
        dataset_fingerprint,
        request,
        features,
        outcomes,
        har_parameters,
    )
    return ScientificCandleResearchReport(
        dataset_fingerprint=dataset_fingerprint,
        report_fingerprint=report_fingerprint,
        split=split,
        policy=policy,
        selected_hypotheses=request.selected_hypotheses,
        har_parameters=har_parameters,
        features=features,
        outcomes=outcomes,
    )


def _opening_gap_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ScientificCandlePolicy,
) -> list[tuple[CausalFeatureVector, ScientificModelOutcome]]:
    result: list[tuple[CausalFeatureVector, ScientificModelOutcome]] = []
    for ticker, rows in by_ticker.items():
        days = _rows_by_day(rows)
        previous_close: float | None = None
        for trading_day in sorted(days):
            day_rows = days[trading_day]
            opening = next(
                (item for item in day_rows if _local_minute(item.at) == 10 * 60),
                None,
            )
            if previous_close is not None and opening is not None:
                feature = opening_gap_feature(
                    ticker=ticker,
                    trading_day=trading_day,
                    observed_at=opening.at,
                    previous_close=previous_close,
                    opening_price=opening.open,
                    policy=policy,
                )
                target_at = opening.at + timedelta(seconds=feature.horizon_seconds)
                target = _candle_observed_at(day_rows, target_at)
                forward = (
                    (target.close / opening.open - 1.0) * 10_000.0
                    if target is not None
                    else None
                )
                result.append(
                    (
                        feature,
                        directional_outcome(
                            feature,
                            target_at=target_at,
                            forward_return_bps=forward,
                            policy=policy,
                        ),
                    )
                )
            previous_close = day_rows[-1].close
    return result


@dataclass(frozen=True, slots=True)
class _WindowReturn:
    ticker: str
    trading_day: date
    observed_at: datetime
    anchor_price: float
    return_bps: float
    rows: tuple[HistoricalCandle, ...]


def _residual_reversal_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ScientificCandlePolicy,
    market_universe: tuple[str, ...],
) -> list[tuple[CausalFeatureVector, ScientificModelOutcome]]:
    returns_by_time: defaultdict[datetime, list[_WindowReturn]] = defaultdict(list)
    observed_candles = {
        ticker: {_observed_at(candle): candle for candle in rows}
        for ticker, rows in by_ticker.items()
    }
    universe = set(market_universe)
    for ticker, rows in by_ticker.items():
        for index in range(len(rows)):
            observed_at = _observed_at(rows[index])
            if observed_at.minute % policy.residual_window_minutes:
                continue
            if not _eligible_phase(observed_at):
                continue
            window = _window_ending(rows, index, policy.residual_window_minutes)
            if window is None:
                continue
            returns_by_time[observed_at].append(
                _WindowReturn(
                    ticker=ticker,
                    trading_day=_trading_day(rows[index].at),
                    observed_at=observed_at,
                    anchor_price=rows[index].close,
                    return_bps=(rows[index].close / window[0].open - 1.0) * 10_000.0,
                    rows=rows,
                )
            )

    result: list[tuple[CausalFeatureVector, ScientificModelOutcome]] = []
    for observed_at in sorted(returns_by_time):
        items = returns_by_time[observed_at]
        market_items = tuple(item for item in items if item.ticker in universe)
        market_return = (
            fmean(item.return_bps for item in market_items) if market_items else 0.0
        )
        for item in items:
            feature = residual_reversal_feature(
                ticker=item.ticker,
                trading_day=item.trading_day,
                observed_at=item.observed_at,
                instrument_return_bps=item.return_bps,
                market_return_bps=market_return,
                market_members=len(market_items),
                policy=policy,
            )
            target_at = observed_at + timedelta(seconds=feature.horizon_seconds)
            target = observed_candles[item.ticker].get(target_at)
            forward = (
                (target.close / item.anchor_price - 1.0) * 10_000.0
                if target is not None
                else None
            )
            result.append(
                (
                    feature,
                    directional_outcome(
                        feature,
                        target_at=target_at,
                        forward_return_bps=forward,
                        policy=policy,
                    ),
                )
            )
    return result


@dataclass(frozen=True, slots=True)
class _VarianceCandidate:
    ticker: str
    trading_day: date
    observed_at: datetime
    target_at: datetime
    short_variance: float
    medium_variance: float
    long_variance: float
    future_variance: float


def _har_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    split: ChronologicalSplit,
    policy: ScientificCandlePolicy,
) -> tuple[
    list[tuple[CausalFeatureVector, ScientificModelOutcome]], HarParameters | None
]:
    candidates: list[_VarianceCandidate] = []
    short, medium, long_window = policy.har_windows_minutes
    horizon_minutes = policy.har_horizon_seconds // 60
    for ticker, rows in by_ticker.items():
        for index in range(len(rows)):
            observed_at = _observed_at(rows[index])
            if observed_at.minute % 30 or not _eligible_phase(observed_at):
                continue
            long_rows = _window_ending(rows, index, long_window)
            if long_rows is None:
                continue
            future_variance = _future_variance(rows, index, horizon_minutes)
            if future_variance is None:
                continue
            candidates.append(
                _VarianceCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(rows[index].at),
                    observed_at=observed_at,
                    target_at=observed_at
                    + timedelta(seconds=policy.har_horizon_seconds),
                    short_variance=_realized_variance(long_rows[-short:]),
                    medium_variance=_realized_variance(long_rows[-medium:]),
                    long_variance=_realized_variance(long_rows),
                    future_variance=future_variance,
                )
            )
    training = tuple(
        HarTrainingPoint(
            feature_at=item.observed_at,
            target_at=item.target_at,
            short_variance=item.short_variance,
            medium_variance=item.medium_variance,
            long_variance=item.long_variance,
            target_variance=item.future_variance,
        )
        for item in candidates
        if split.partition_for(item.trading_day) is DatasetPartition.TRAIN
    )
    parameters = (
        fit_har_parameters(
            training,
            minimum_points=policy.har_minimum_training_points,
            ridge_penalty=policy.har_ridge_penalty,
        )
        if len(training) >= policy.har_minimum_training_points
        else None
    )
    result: list[tuple[CausalFeatureVector, ScientificModelOutcome]] = []
    for item in candidates:
        if split.partition_for(item.trading_day) is DatasetPartition.TRAIN:
            continue
        feature = har_volatility_feature(
            ticker=item.ticker,
            trading_day=item.trading_day,
            observed_at=item.observed_at,
            short_variance=item.short_variance,
            medium_variance=item.medium_variance,
            long_variance=item.long_variance,
            parameters=parameters,
            policy=policy,
        )
        result.append(
            (
                feature,
                variance_outcome(
                    feature,
                    target_at=item.target_at,
                    actual_future_variance=item.future_variance,
                    policy=policy,
                ),
            )
        )
    return result, parameters


@dataclass(frozen=True, slots=True)
class _ActivityCandidate:
    ticker: str
    trading_day: date
    bucket: int
    observed_at: datetime
    target_at: datetime
    volume: float
    future_variance: float


def _relative_volume_rows(
    by_ticker: dict[str, tuple[HistoricalCandle, ...]],
    policy: ScientificCandlePolicy,
) -> list[tuple[CausalFeatureVector, ScientificModelOutcome]]:
    candidates: list[_ActivityCandidate] = []
    horizon_minutes = policy.activity_horizon_seconds // 60
    for ticker, rows in by_ticker.items():
        for index in range(len(rows)):
            observed_at = _observed_at(rows[index])
            if observed_at.minute % policy.activity_window_minutes:
                continue
            if not _eligible_phase(observed_at):
                continue
            window = _window_ending(rows, index, policy.activity_window_minutes)
            if window is None:
                continue
            future_variance = _future_variance(rows, index, horizon_minutes)
            if future_variance is None:
                continue
            candidates.append(
                _ActivityCandidate(
                    ticker=ticker,
                    trading_day=_trading_day(rows[index].at),
                    bucket=_local_minute(rows[index].at)
                    // policy.activity_window_minutes,
                    observed_at=observed_at,
                    target_at=observed_at
                    + timedelta(seconds=policy.activity_horizon_seconds),
                    volume=sum(item.volume for item in window),
                    future_variance=future_variance,
                )
            )
    histories: defaultdict[tuple[str, int], list[tuple[float, float]]] = defaultdict(
        list
    )
    result: list[tuple[CausalFeatureVector, ScientificModelOutcome]] = []
    for trading_day in sorted({item.trading_day for item in candidates}):
        day_items = tuple(
            item for item in candidates if item.trading_day == trading_day
        )
        for item in day_items:
            history = histories[(item.ticker, item.bucket)]
            historical_volumes = tuple(value[0] for value in history)
            historical_variances = tuple(value[1] for value in history)
            baseline = median(historical_variances) if historical_variances else 0.0
            feature = relative_volume_activity_feature(
                ticker=item.ticker,
                trading_day=item.trading_day,
                observed_at=item.observed_at,
                current_volume=item.volume,
                historical_phase_volumes=historical_volumes,
                baseline_future_variance=baseline,
                policy=policy,
            )
            result.append(
                (
                    feature,
                    variance_outcome(
                        feature,
                        target_at=item.target_at,
                        actual_future_variance=item.future_variance,
                        policy=policy,
                    ),
                )
            )
        for item in day_items:
            histories[(item.ticker, item.bucket)].append(
                (item.volume, item.future_variance)
            )
    return result


def _rows_by_day(
    rows: Sequence[HistoricalCandle],
) -> dict[date, tuple[HistoricalCandle, ...]]:
    grouped: defaultdict[date, list[HistoricalCandle]] = defaultdict(list)
    for row in rows:
        grouped[_trading_day(row.at)].append(row)
    return {day: tuple(values) for day, values in grouped.items()}


def _window_ending(
    rows: Sequence[HistoricalCandle],
    end_index: int,
    minutes: int,
) -> tuple[HistoricalCandle, ...] | None:
    start = end_index - minutes + 1
    if start < 0:
        return None
    window = tuple(rows[start : end_index + 1])
    if _trading_day(window[0].at) != _trading_day(window[-1].at):
        return None
    if any(
        current.at - previous.at != timedelta(minutes=1)
        for previous, current in zip(window, window[1:])
    ):
        return None
    return window


def _future_variance(
    rows: Sequence[HistoricalCandle],
    anchor_index: int,
    minutes: int,
) -> float | None:
    end = anchor_index + minutes
    if end >= len(rows):
        return None
    future = tuple(rows[anchor_index : end + 1])
    if _trading_day(future[0].at) != _trading_day(future[-1].at):
        return None
    if any(
        current.at - previous.at != timedelta(minutes=1)
        for previous, current in zip(future, future[1:])
    ):
        return None
    returns = tuple(
        log(current.close / previous.close) * 10_000.0
        for previous, current in zip(future, future[1:])
    )
    return sum(value * value for value in returns)


def _realized_variance(rows: Sequence[HistoricalCandle]) -> float:
    returns = tuple(
        log(current.close / previous.close) * 10_000.0
        for previous, current in zip(rows, rows[1:])
    )
    return sum(value * value for value in returns)


def _candle_observed_at(
    rows: Sequence[HistoricalCandle],
    observed_at: datetime,
) -> HistoricalCandle | None:
    return next((item for item in rows if _observed_at(item) == observed_at), None)


def _eligible_phase(at: datetime) -> bool:
    return MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(at) in {
        TradingPhase.MAIN_OPENING,
        TradingPhase.MAIN_CONTINUOUS,
        TradingPhase.PRE_CLOSE,
    }


def _observed_at(candle: HistoricalCandle) -> datetime:
    return candle.at + timedelta(minutes=1)


def _trading_day(at: datetime) -> date:
    return at.astimezone(MOSCOW).date()


def _local_minute(at: datetime) -> int:
    local = at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute


def _report_fingerprint(
    dataset_fingerprint: str,
    request: ScientificCandleResearchRequest,
    features: tuple[CausalFeatureVector, ...],
    outcomes: tuple[ScientificModelOutcome, ...],
    har_parameters: HarParameters | None,
) -> str:
    payload = {
        "dataset_fingerprint": dataset_fingerprint,
        "policy_version": request.policy.version,
        "selected": [item.value for item in request.selected_hypotheses],
        "market_universe": request.market_universe,
        "features": [item.observation_id for item in features],
        "outcomes": [
            (item.observation_id, item.available, item.actual_value)
            for item in outcomes
        ],
        "har": (
            (
                har_parameters.intercept,
                har_parameters.short_weight,
                har_parameters.medium_weight,
                har_parameters.long_weight,
                har_parameters.training_points,
                har_parameters.trained_until.isoformat(),
            )
            if har_parameters is not None
            else None
        ),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()
