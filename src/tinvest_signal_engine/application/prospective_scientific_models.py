"""Chronological candle calculations for the prospective scientific portfolio."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from math import log, pi
from statistics import median
from typing import Iterable, Protocol, Sequence, TypeVar
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    DatasetPartition,
    chronological_split_60_20_20,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
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


@dataclass(frozen=True, slots=True)
class ProspectiveScientificRequest:
    selected_hypotheses: tuple[ProspectiveHypothesis, ...] = tuple(
        ProspectiveHypothesis
    )
    policy: ProspectiveScientificPolicy = ProspectiveScientificPolicy()

    def __post_init__(self) -> None:
        if not self.selected_hypotheses:
            raise ValueError("at least one prospective hypothesis is required")
        if len(set(self.selected_hypotheses)) != len(self.selected_hypotheses):
            raise ValueError("prospective hypotheses must be unique")


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
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
    }:
        rows.extend(_jump_rows(ordered, selected, request.policy))
    if ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3 in selected:
        rows.extend(_relative_volume_rows(ordered, request.policy))
    if ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK in selected:
        rows.extend(_semivariance_rows(ordered, request.policy))
    if ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE in selected:
        rows.extend(_volatility_jump_rows(ordered, request.policy))
    har_parameters: HarV2Parameters | None = None
    if ProspectiveHypothesis.HAR_VOLATILITY_V2 in selected:
        har_rows, har_parameters = _har_v2_rows(ordered, split, request.policy)
        rows.extend(har_rows)

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
    )
    return ProspectiveScientificReport(
        dataset_fingerprint=dataset_fingerprint,
        report_fingerprint=fingerprint,
        split=split,
        policy=request.policy,
        selected_hypotheses=request.selected_hypotheses,
        har_v2_parameters=har_parameters,
        features=features,
        outcomes=outcomes,
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
    short, medium, long_window = policy.har_windows_minutes
    for ticker, rows in by_ticker.items():
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
) -> str:
    payload = {
        "dataset_fingerprint": dataset_fingerprint,
        "policy": request.policy.version,
        "selected": [item.value for item in request.selected_hypotheses],
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
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()
