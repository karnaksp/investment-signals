"""Causal ClickHouse candle inputs for the prospective live-shadow worker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
import json
from math import ceil, log, pi
from statistics import median
from typing import Iterator, Literal, Mapping, Protocol, Sequence

from tinvest_signal_engine.application.prospective_live_shadow import (
    HarFeatureInput,
    JumpFeatureInput,
    ProspectiveLiveOutcomeEvidence,
    ProspectivePortfolioSnapshot,
    RelativeVolumeFeatureInput,
    SemivarianceFeatureInput,
    VolatilityJumpFeatureInput,
)
from tinvest_signal_engine.domain.prospective_live_shadow import (
    ProspectiveLiveObservation,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    HarV2Parameters,
    HarV2TrainingPoint,
    JumpVarianceContrastHistoryPoint,
    JumpHistoryPoint,
    ProspectiveScientificPolicy,
    SemivarianceContrastHistoryPoint,
    TargetMetric,
    fit_har_v2_parameters,
)
from tinvest_signal_engine.domain.scientific_candles import (
    ScientificCandle,
    scientific_candle_fingerprint,
)


_CANDLE_COLUMNS = (
    "instrument_id",
    "ticker",
    "exchange",
    "trading_day",
    "candle_at",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "is_complete",
    "source_kind",
    "source_at",
    "received_at",
    "source_event_id",
    "payload_fingerprint",
    "has_gap",
    "schema_version",
    "record_version",
    "same_version_fingerprint_count",
)
_TRADING_DAYS_PER_WEEK = 5
_CALENDAR_DAYS_PER_WEEK = 7
_EXCHANGE_HOLIDAY_BUFFER_DAYS = 14
_HISTORY_RESULT_ROW_LIMIT = 75_000
_HISTORY_RESULT_BYTE_LIMIT = 32 * 1024 * 1024
_SHORT_RESULT_ROW_LIMIT = 4_096
_SHORT_RESULT_BYTE_LIMIT = 8 * 1024 * 1024

_INSTRUMENT_CANDLES_SQL = """
SELECT
    instrument_id,
    ticker,
    exchange,
    trading_day,
    candle_at,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    is_complete,
    source_kind,
    source_at,
    received_at,
    source_event_id,
    payload_fingerprint,
    has_gap,
    schema_version,
    record_version,
    same_version_fingerprint_count
FROM
(
    SELECT
        instrument_id,
        ticker,
        exchange,
        trading_day,
        candle_at,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        is_complete,
        source_kind,
        source_at,
        received_at,
        source_event_id,
        payload_fingerprint,
        has_gap,
        schema_version,
        record_version,
        row_number() OVER
        (
            PARTITION BY instrument_id, candle_at
            ORDER BY record_version DESC, payload_fingerprint DESC
        ) AS physical_rank,
        uniqExact(payload_fingerprint) OVER
        (
            PARTITION BY instrument_id, candle_at, record_version
        ) AS same_version_fingerprint_count
    FROM scientific_candles_1m
    PREWHERE instrument_id = {instrument_id:String}
      AND trading_day >= toDate(parseDateTime64BestEffort({lookback_start:String}, 6, 'UTC'))
    WHERE candle_at >= parseDateTime64BestEffort({lookback_start:String}, 6, 'UTC')
      AND candle_at < parseDateTime64BestEffort({candle_until:String}, 6, 'UTC')
      AND source_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
      AND received_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
)
WHERE physical_rank = 1
ORDER BY trading_day DESC, candle_at DESC, record_version DESC
LIMIT 75001
SETTINGS max_execution_time = 30,
         max_rows_to_read = 200000,
         max_bytes_to_read = 268435456,
         max_result_rows = 75001,
         max_result_bytes = 33554432,
         result_overflow_mode = 'throw',
         max_memory_usage = 100663296,
         max_threads = 1,
         output_format_parallel_formatting = 0,
         timeout_before_checking_execution_speed = 0
FORMAT JSONCompactEachRow
""".strip()

_SHORT_OUTCOME_CANDLES_SQL = (
    _INSTRUMENT_CANDLES_SQL.replace("LIMIT 75001", "LIMIT 4097")
    .replace("max_rows_to_read = 200000", "max_rows_to_read = 32768")
    .replace("max_bytes_to_read = 268435456", "max_bytes_to_read = 33554432")
    .replace("max_result_rows = 75001", "max_result_rows = 4097")
    .replace("max_result_bytes = 33554432", "max_result_bytes = 8388608")
    .replace("max_memory_usage = 100663296", "max_memory_usage = 33554432")
)


class _ClickHouseRequester(Protocol):
    def _request(self, sql: str, *, parameters: Mapping[str, str]) -> bytes: ...


@dataclass(frozen=True, slots=True)
class _SeriesPoint:
    candle: ScientificCandle
    record_version: int


class ClickHouseProspectiveLiveSnapshotSource:
    """Build the latest 30-minute portfolio cutoff per instrument as-of a poll."""

    def __init__(
        self,
        client: _ClickHouseRequester,
        *,
        instrument_ids: tuple[str, ...],
    ) -> None:
        normalized = tuple(dict.fromkeys(item.strip() for item in instrument_ids))
        if not normalized or any(not item for item in normalized):
            raise ValueError("snapshot instrument_ids must be non-empty and unique")
        self._client = client
        self._instrument_ids = normalized

    def load_snapshots(
        self,
        *,
        as_of: datetime,
        policy: ProspectiveScientificPolicy,
        limit: int,
        instrument_ids: tuple[str, ...] | None = None,
    ) -> tuple[ProspectivePortfolioSnapshot, ...]:
        cutoff = _aware_utc(as_of, "as_of")
        if limit <= 0:
            raise ValueError("snapshot limit must be positive")
        selected = (
            self._instrument_ids
            if instrument_ids is None
            else tuple(dict.fromkeys(item.strip() for item in instrument_ids))
        )
        if not selected or any(
            not item or item not in self._instrument_ids for item in selected
        ):
            raise ValueError("snapshot instruments must be configured and non-empty")
        snapshots: list[ProspectivePortfolioSnapshot] = []
        lookback_days = _calendar_lookback_days(policy)
        # One strictly bounded compact response is materialized at a time.
        for instrument_id in selected[:limit]:
            candles = tuple(
                item
                for item in _load_candles(
                    self._client,
                    as_of=cutoff,
                    lookback_start=cutoff - timedelta(days=lookback_days),
                    candle_until=cutoff,
                    instrument_id=instrument_id,
                    query_kind="history",
                )
                if item.complete
            )
            ordered = tuple(sorted(candles, key=lambda item: item.candle_at))
            candidates = tuple(
                index
                for index, candle in enumerate(ordered)
                if _observed_at(candle).minute % 30 == 0
            )
            if not candidates:
                continue
            snapshots.append(
                _snapshot(ordered, candidates[-1], as_of=cutoff, policy=policy)
            )
        return tuple(
            sorted(snapshots, key=lambda item: (item.observed_at, item.instrument_id))
        )[:limit]


class ClickHouseProspectiveLiveOutcomeSource:
    """Calculate sealed outcome evidence from candles no later than target_at."""

    def __init__(
        self,
        client: _ClickHouseRequester,
        *,
        ewma_alpha: float = 0.10,
        policy: ProspectiveScientificPolicy | None = None,
    ) -> None:
        if not 0.0 < ewma_alpha < 1.0:
            raise ValueError("ewma_alpha must be in (0, 1)")
        self._client = client
        self._ewma_alpha = ewma_alpha
        self._history_lookback_days = _calendar_lookback_days(
            policy or ProspectiveScientificPolicy()
        )

    def load(
        self,
        observation: ProspectiveLiveObservation,
        *,
        as_of: datetime,
    ) -> ProspectiveLiveOutcomeEvidence:
        cutoff = _aware_utc(as_of, "as_of")
        if cutoff < observation.target_at:
            raise ValueError("outcome evidence cannot be loaded before target_at")
        rows = tuple(
            item
            for item in _load_candles(
                self._client,
                as_of=cutoff,
                lookback_start=(
                    observation.feature.observed_at
                    - (
                        timedelta(days=self._history_lookback_days)
                        if observation.feature.target
                        is TargetMetric.FUTURE_REALIZED_VARIANCE
                        else timedelta(minutes=1)
                    )
                ),
                candle_until=observation.target_at,
                instrument_id=observation.instrument_id,
                query_kind=(
                    "history"
                    if observation.feature.target
                    is TargetMetric.FUTURE_REALIZED_VARIANCE
                    else "short_outcome"
                ),
            )
            if item.instrument_id == observation.instrument_id
            and item.complete
            and item.candle_at < observation.target_at
        )
        actual = _outcome_value(rows, observation)
        ewma = phase = None
        if observation.feature.target is TargetMetric.FUTURE_REALIZED_VARIANCE:
            history = _prior_phase_future_variances(
                rows,
                observed_at=observation.feature.observed_at,
                horizon_seconds=observation.feature.horizon_seconds,
            )
            if history:
                ewma = _ewma(history, self._ewma_alpha)
                phase = median(history)
        fingerprint = _candle_fingerprint(
            tuple(item for item in rows if item.candle_at < observation.target_at)
        )
        return ProspectiveLiveOutcomeEvidence(
            observation_id=observation.observation_id,
            target_at=observation.target_at,
            available=actual is not None,
            actual_value=actual,
            evidence_fingerprint=fingerprint,
            ewma_baseline=ewma,
            phase_baseline=phase,
        )


def _load_candles(
    client: _ClickHouseRequester,
    *,
    as_of: datetime,
    lookback_start: datetime,
    candle_until: datetime,
    instrument_id: str,
    query_kind: Literal["history", "short_outcome"],
) -> tuple[ScientificCandle, ...]:
    if not instrument_id.strip():
        raise ValueError("instrument_id must not be empty")
    parameters = {
        "as_of": _clickhouse_datetime(as_of),
        "candle_until": _clickhouse_datetime(candle_until),
        "lookback_start": _clickhouse_datetime(lookback_start),
        "instrument_id": instrument_id,
    }
    sql = (
        _INSTRUMENT_CANDLES_SQL
        if query_kind == "history"
        else _SHORT_OUTCOME_CANDLES_SQL
    )
    payload = client._request(
        sql,
        parameters=parameters,
    )
    payload_limit, row_limit = (
        (_HISTORY_RESULT_BYTE_LIMIT, _HISTORY_RESULT_ROW_LIMIT)
        if query_kind == "history"
        else (_SHORT_RESULT_BYTE_LIMIT, _SHORT_RESULT_ROW_LIMIT)
    )
    selected: dict[tuple[str, datetime], _SeriesPoint] = {}
    for row in _compact_json_each_row(
        payload,
        max_payload_bytes=payload_limit,
        max_rows=row_limit,
    ):
        point = _series_point(row, cutoff=as_of)
        key = (point.candle.instrument_id, point.candle.candle_at)
        existing = selected.get(key)
        if existing is None or point.record_version > existing.record_version:
            selected[key] = point
        elif (
            point.record_version == existing.record_version
            and point.candle.payload_fingerprint != existing.candle.payload_fingerprint
        ):
            raise ValueError("conflicting scientific candle physical versions")
    return tuple(
        item.candle
        for item in sorted(
            selected.values(),
            key=lambda item: (item.candle.instrument_id, item.candle.candle_at),
        )
    )


def _calendar_lookback_days(policy: ProspectiveScientificPolicy) -> int:
    """Convert the sealed trading-day requirement into a bounded query span."""

    trading_days = policy.required_history_trading_days
    weekday_span = ceil(trading_days * _CALENDAR_DAYS_PER_WEEK / _TRADING_DAYS_PER_WEEK)
    return weekday_span + _EXCHANGE_HOLIDAY_BUFFER_DAYS


def _compact_json_each_row(
    payload: bytes,
    *,
    max_payload_bytes: int,
    max_rows: int,
) -> Iterator[dict[str, object]]:
    """Decode a bounded JSONCompactEachRow candle response."""

    if max_payload_bytes <= 0 or max_rows <= 0:
        raise ValueError("compact response bounds must be positive")
    if len(payload) > max_payload_bytes:
        raise ValueError("ClickHouse candle response exceeds byte limit")
    row_count = 0
    for raw_line in BytesIO(payload):
        if not raw_line.strip():
            continue
        row_count += 1
        if row_count > max_rows:
            raise ValueError("ClickHouse candle response exceeds row limit")
        values = json.loads(raw_line)
        if not isinstance(values, list) or len(values) != len(_CANDLE_COLUMNS):
            raise ValueError("invalid JSONCompactEachRow candle record")
        yield dict(zip(_CANDLE_COLUMNS, values, strict=True))


def _snapshot(
    candles: tuple[ScientificCandle, ...],
    index: int,
    *,
    as_of: datetime,
    policy: ProspectiveScientificPolicy,
) -> ProspectivePortfolioSnapshot:
    current = candles[index]
    observed_at = _observed_at(current)
    jump_window = _window_ending(candles, index, policy.jump_window_minutes)
    volume_window = _window_ending(candles, index, policy.volume_window_minutes)
    variance_window = _window_ending(candles, index, policy.semivariance_window_minutes)
    jump_variance_window = _window_ending(
        candles, index, policy.jump_variance_window_minutes
    )
    long_window = _window_ending(candles, index, policy.har_windows_minutes[-1])
    trading_gap = any(
        window is None or any(item.has_gap for item in window)
        for window in (
            jump_window,
            volume_window,
            variance_window,
            jump_variance_window,
            long_window,
        )
    )
    jump_window = jump_window or (current,)
    volume_window = volume_window or (current,)
    variance_window = variance_window or (current,)
    jump_variance_window = jump_variance_window or (current,)
    long_window = long_window or (current,)
    prior_days = _prior_daily_inputs(candles, index, policy=policy)
    jump_history = tuple(item.jump for item in prior_days)[-policy.jump_history_days :]
    volume_history = tuple(item.volume for item in prior_days)[
        -policy.volume_history_days :
    ]
    semivariance_history = tuple(item.downside_share for item in prior_days)[
        -policy.semivariance_history_days :
    ]
    jump_share_history = tuple(item.jump_share for item in prior_days)[
        -policy.jump_variance_history_days :
    ]
    semivariance_contrast_history = tuple(
        item.semivariance_contrast for item in prior_days
    )[-policy.semivariance_history_days :]
    jump_contrast_history = tuple(item.jump_contrast for item in prior_days)[
        -policy.jump_variance_history_days :
    ]
    volume_baseline = _median_tail(
        tuple(item.future_variance for item in prior_days), policy.volume_history_days
    )
    semivariance_baseline = _median_tail(
        tuple(item.future_variance for item in prior_days),
        policy.semivariance_history_days,
    )
    jump_baseline = _median_tail(
        tuple(item.future_variance for item in prior_days),
        policy.jump_variance_history_days,
    )
    returns = _log_returns(variance_window)
    variance = sum(value * value for value in returns)
    downside = sum(value * value for value in returns if value < 0.0)
    upside = sum(value * value for value in returns if value > 0.0)
    legacy_bipower = (pi / 2.0) * sum(
        abs(previous) * abs(current_value)
        for previous, current_value in zip(returns, returns[1:])
    )
    legacy_jump_variance = max(variance - legacy_bipower, 0.0)
    jump_returns = _log_returns(jump_variance_window)
    jump_total_variance = sum(value * value for value in jump_returns)
    bipower = (pi / 2.0) * sum(
        abs(previous) * abs(current_value)
        for previous, current_value in zip(jump_returns, jump_returns[1:])
    )
    continuous_variance = min(max(bipower, 0.0), jump_total_variance)
    jump_variance = max(jump_total_variance - continuous_variance, 0.0)
    history_until = max((item.target_at for item in prior_days), default=None)
    parameters = (
        _har_parameters(candles, index, policy=policy) if not trading_gap else None
    )
    short, medium, _ = policy.har_windows_minutes
    current_ids = tuple(dict.fromkeys(item.source_event_id for item in long_window))
    fingerprint = _candle_fingerprint(candles[: index + 1])
    return ProspectivePortfolioSnapshot(
        instrument_id=current.instrument_id,
        ticker=current.ticker,
        trading_day=current.trading_day,
        observed_at=observed_at,
        recorded_at=as_of,
        source_event_ids=current_ids,
        dataset_fingerprint=fingerprint,
        input_fingerprint=fingerprint,
        trading_gap=trading_gap,
        jump=JumpFeatureInput(
            signed_return_bps=(
                float(jump_window[-1].close_price / jump_window[0].open_price - 1)
                * 10_000.0
            ),
            volume=float(sum(item.volume for item in jump_window)),
            range_bps=(
                float(
                    max(item.high_price for item in jump_window)
                    / min(item.low_price for item in jump_window)
                    - 1
                )
                * 10_000.0
            ),
            illiquidity=_illiquidity(jump_window),
            prior_history=jump_history,
            history_observed_until=history_until,
        ),
        relative_volume=RelativeVolumeFeatureInput(
            current_volume=float(sum(item.volume for item in volume_window)),
            historical_volumes=volume_history,
            baseline_future_variance=volume_baseline,
            history_observed_until=history_until,
        ),
        har=HarFeatureInput(
            short_variance=_realized_variance(long_window[-short:]),
            medium_variance=_realized_variance(long_window[-medium:]),
            long_variance=_realized_variance(long_window),
            parameters=parameters,
        ),
        semivariance=SemivarianceFeatureInput(
            downside_share=downside / variance if variance > 0.0 else 0.0,
            historical_downside_shares=semivariance_history,
            baseline_future_variance=semivariance_baseline,
            history_observed_until=history_until,
            downside_variance=downside,
            upside_variance=upside,
            contrast_history=semivariance_contrast_history,
            contrast_history_observed_until=(
                semivariance_contrast_history[-1].target_at
                if semivariance_contrast_history
                else None
            ),
        ),
        volatility_jump=VolatilityJumpFeatureInput(
            jump_share=(legacy_jump_variance / variance if variance > 0.0 else 0.0),
            continuous_variance=legacy_bipower,
            historical_jump_shares=jump_share_history,
            baseline_future_variance=jump_baseline,
            history_observed_until=history_until,
            jump_variance=jump_variance,
            contrast_history=jump_contrast_history,
            contrast_history_observed_until=(
                jump_contrast_history[-1].target_at if jump_contrast_history else None
            ),
        ),
    )


@dataclass(frozen=True, slots=True)
class _DailyInput:
    trading_day: date
    target_at: datetime
    jump: JumpHistoryPoint
    volume: float
    downside_share: float
    jump_share: float
    future_variance: float
    semivariance_contrast: SemivarianceContrastHistoryPoint
    jump_contrast: JumpVarianceContrastHistoryPoint


def _prior_daily_inputs(
    candles: tuple[ScientificCandle, ...],
    current_index: int,
    *,
    policy: ProspectiveScientificPolicy,
) -> tuple[_DailyInput, ...]:
    current = candles[current_index]
    cutoff_clock = _observed_at(current).time()
    result: list[_DailyInput] = []
    for index, candle in enumerate(candles[:current_index]):
        if candle.trading_day >= current.trading_day:
            continue
        if _observed_at(candle).time() != cutoff_clock:
            continue
        jump = _window_ending(candles, index, policy.jump_window_minutes)
        volume = _window_ending(candles, index, policy.volume_window_minutes)
        variance = _window_ending(candles, index, policy.semivariance_window_minutes)
        jump_variance_window = _window_ending(
            candles, index, policy.jump_variance_window_minutes
        )
        future = _future_window(candles, index, policy.volume_horizon_seconds // 60)
        semivariance_future = _future_window(
            candles, index, policy.semivariance_horizon_seconds // 60
        )
        jump_future = _future_window(
            candles, index, policy.jump_variance_horizon_seconds // 60
        )
        if (
            jump is None
            or volume is None
            or variance is None
            or jump_variance_window is None
            or future is None
            or semivariance_future is None
            or jump_future is None
        ):
            continue
        returns = _log_returns(variance)
        total_variance = sum(value * value for value in returns)
        downside = sum(value * value for value in returns if value < 0.0)
        upside = sum(value * value for value in returns if value > 0.0)
        legacy_bipower = (pi / 2.0) * sum(
            abs(previous) * abs(current_value)
            for previous, current_value in zip(returns, returns[1:])
        )
        legacy_jump_variance = max(total_variance - legacy_bipower, 0.0)
        jump_returns = _log_returns(jump_variance_window)
        jump_total_variance = sum(value * value for value in jump_returns)
        bipower = (pi / 2.0) * sum(
            abs(previous) * abs(current_value)
            for previous, current_value in zip(jump_returns, jump_returns[1:])
        )
        continuous_variance = min(max(bipower, 0.0), jump_total_variance)
        jump_variance = max(jump_total_variance - continuous_variance, 0.0)
        observed_at = _observed_at(candle)
        semivariance_target_at = observed_at + timedelta(
            seconds=policy.semivariance_horizon_seconds
        )
        jump_target_at = observed_at + timedelta(
            seconds=policy.jump_variance_horizon_seconds
        )
        signed_return = float(jump[-1].close_price / jump[0].open_price - 1) * 10_000.0
        result.append(
            _DailyInput(
                trading_day=candle.trading_day,
                target_at=max(
                    observed_at + timedelta(seconds=policy.volume_horizon_seconds),
                    semivariance_target_at,
                    jump_target_at,
                ),
                jump=JumpHistoryPoint(
                    absolute_return_bps=abs(signed_return),
                    volume=float(sum(item.volume for item in jump)),
                    range_bps=float(
                        max(item.high_price for item in jump)
                        / min(item.low_price for item in jump)
                        - 1
                    )
                    * 10_000.0,
                    illiquidity=_illiquidity(jump),
                ),
                volume=float(sum(item.volume for item in volume)),
                downside_share=(
                    downside / total_variance if total_variance > 0.0 else 0.0
                ),
                jump_share=(
                    legacy_jump_variance / total_variance
                    if total_variance > 0.0
                    else 0.0
                ),
                future_variance=_realized_variance(future),
                semivariance_contrast=SemivarianceContrastHistoryPoint(
                    downside_variance=downside,
                    upside_variance=upside,
                    future_variance=_realized_variance(semivariance_future),
                    target_at=semivariance_target_at,
                ),
                jump_contrast=JumpVarianceContrastHistoryPoint(
                    jump_variance=jump_variance,
                    continuous_variance=continuous_variance,
                    future_variance=_realized_variance(jump_future),
                    target_at=jump_target_at,
                ),
            )
        )
    return tuple(sorted(result, key=lambda item: item.trading_day))


def _har_parameters(
    candles: tuple[ScientificCandle, ...],
    current_index: int,
    *,
    policy: ProspectiveScientificPolicy,
) -> HarV2Parameters | None:
    short, medium, long_window = policy.har_windows_minutes
    training: list[HarV2TrainingPoint] = []
    for index, candle in enumerate(candles[:current_index]):
        if _observed_at(candle).minute % 30:
            continue
        window = _window_ending(candles, index, long_window)
        future = _future_window(candles, index, policy.har_horizon_seconds // 60)
        if window is None or future is None:
            continue
        training.append(
            HarV2TrainingPoint(
                feature_at=_observed_at(candle),
                target_at=_observed_at(candle)
                + timedelta(seconds=policy.har_horizon_seconds),
                short_variance=_realized_variance(window[-short:]),
                medium_variance=_realized_variance(window[-medium:]),
                long_variance=_realized_variance(window),
                target_variance=_realized_variance(future),
            )
        )
    if len(training) < policy.har_minimum_training_points:
        return None
    return fit_har_v2_parameters(
        training,
        minimum_points=policy.har_minimum_training_points,
        ridge_penalty=policy.har_ridge_penalty,
    )


def _prior_phase_future_variances(
    candles: tuple[ScientificCandle, ...],
    *,
    observed_at: datetime,
    horizon_seconds: int,
) -> tuple[float, ...]:
    result: list[float] = []
    for index, candle in enumerate(candles):
        candle_observed_at = _observed_at(candle)
        if candle_observed_at >= observed_at:
            break
        if candle_observed_at.time() != observed_at.time():
            continue
        future = _future_window(candles, index, horizon_seconds // 60)
        if future is not None:
            result.append(_realized_variance(future))
    return tuple(result)


def _outcome_value(
    candles: tuple[ScientificCandle, ...], observation: ProspectiveLiveObservation
) -> float | None:
    try:
        index = next(
            index
            for index, item in enumerate(candles)
            if _observed_at(item) == observation.feature.observed_at
        )
    except StopIteration:
        return None
    future = _future_window(candles, index, observation.feature.horizon_seconds // 60)
    if future is None:
        return None
    if observation.feature.target is TargetMetric.FORWARD_RETURN:
        return float(future[-1].close_price / future[0].close_price - 1) * 10_000.0
    return _realized_variance(future)


def _window_ending(
    candles: Sequence[ScientificCandle], index: int, minutes: int
) -> tuple[ScientificCandle, ...] | None:
    start = index - minutes + 1
    if start < 0:
        return None
    window = tuple(candles[start : index + 1])
    return window if _continuous(window, expected=minutes) else None


def _future_window(
    candles: Sequence[ScientificCandle], index: int, minutes: int
) -> tuple[ScientificCandle, ...] | None:
    end = index + minutes
    if end >= len(candles):
        return None
    window = tuple(candles[index : end + 1])
    return window if _continuous(window, expected=minutes + 1) else None


def _continuous(candles: Sequence[ScientificCandle], *, expected: int) -> bool:
    if len(candles) != expected or not candles:
        return False
    if len({item.instrument_id for item in candles}) != 1:
        return False
    if candles[0].trading_day != candles[-1].trading_day:
        return False
    return all(
        current.candle_at - previous.candle_at == timedelta(minutes=1)
        for previous, current in zip(candles, candles[1:])
    )


def _log_returns(candles: Sequence[ScientificCandle]) -> tuple[float, ...]:
    return tuple(
        log(float(current.close_price / previous.close_price)) * 10_000.0
        for previous, current in zip(candles, candles[1:])
    )


def _realized_variance(candles: Sequence[ScientificCandle]) -> float:
    return sum(value * value for value in _log_returns(candles))


def _illiquidity(candles: Sequence[ScientificCandle]) -> float:
    signed_return = float(candles[-1].close_price / candles[0].open_price - 1)
    turnover = sum(float(item.close_price) * item.volume for item in candles)
    return abs(signed_return * 10_000.0) / max(turnover, 1.0) * 1_000_000_000.0


def _median_tail(values: tuple[float, ...], size: int) -> float:
    tail = values[-size:]
    return median(tail) if len(tail) == size else 0.0


def _ewma(values: tuple[float, ...], alpha: float) -> float:
    estimate = values[0]
    for value in values[1:]:
        estimate = alpha * value + (1.0 - alpha) * estimate
    return estimate


def _candle_fingerprint(candles: Sequence[ScientificCandle]) -> str:
    payload = tuple(
        (item.instrument_id, item.candle_at.isoformat(), item.payload_fingerprint)
        for item in candles
    )
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + sha256(encoded).hexdigest()


def _series_point(row: Mapping[str, object], *, cutoff: datetime) -> _SeriesPoint:
    candle_at = _datetime(row["candle_at"])
    source_at = _datetime(row["source_at"])
    received_at = _datetime(row["received_at"])
    if max(candle_at, source_at, received_at) > cutoff:
        raise ValueError("ClickHouse returned a scientific candle beyond cutoff")
    if int(row["same_version_fingerprint_count"]) != 1:
        raise ValueError("conflicting scientific candle physical versions")
    fingerprint = str(row["payload_fingerprint"])
    if not fingerprint.startswith("sha256:"):
        fingerprint = "sha256:" + fingerprint
    raw_prices = tuple(
        Decimal(str(row[column]))
        for column in ("open_price", "high_price", "low_price", "close_price")
    )
    source_kind = str(row["source_kind"])
    # Decimal(18, 9) is rendered by ClickHouse with its storage scale. Stream
    # fingerprints were sealed from T-Invest quotations before persistence, so
    # e.g. Decimal("280.1") returns as Decimal("280.100000000"). Backfill was
    # sealed from Parquet floats, whose integer values retain one decimal place
    # (`263.0`). Try only those two known, source-aware transport reversals.
    # A fingerprint must still match a complete payload; corrupted rows fail.
    normalized_prices = tuple(_without_storage_scale(value) for value in raw_prices)
    price_candidates = (raw_prices, normalized_prices)
    if source_kind == "backfill":
        price_candidates += (
            tuple(_parquet_float_decimal(value) for value in raw_prices),
        )
    instrument_id = str(row["instrument_id"])
    ticker = str(row["ticker"])
    exchange = str(row["exchange"])
    volume = int(row["volume"])
    complete = _boolean(row["is_complete"])
    source_event_id = str(row["source_event_id"])
    has_gap = _boolean(row["has_gap"])
    schema_version = str(row["schema_version"])
    prices = next(
        (
            candidate
            for candidate in price_candidates
            if scientific_candle_fingerprint(
                instrument_id=instrument_id,
                ticker=ticker,
                exchange=exchange,
                candle_at=candle_at,
                open_price=candidate[0],
                high_price=candidate[1],
                low_price=candidate[2],
                close_price=candidate[3],
                volume=volume,
                complete=complete,
                source_kind=source_kind,
                source_at=source_at,
                source_event_id=source_event_id,
                has_gap=has_gap,
                schema_version=schema_version,
            )
            == fingerprint
        ),
        None,
    )
    if prices is None:
        raise ValueError("candle payload fingerprint does not match content")
    return _SeriesPoint(
        candle=ScientificCandle(
            instrument_id=instrument_id,
            ticker=ticker,
            exchange=exchange,
            trading_day=date.fromisoformat(str(row["trading_day"])),
            candle_at=candle_at,
            open_price=prices[0],
            high_price=prices[1],
            low_price=prices[2],
            close_price=prices[3],
            volume=volume,
            complete=complete,
            source_kind=source_kind,
            source_at=source_at,
            received_at=received_at,
            source_event_id=source_event_id,
            payload_fingerprint=fingerprint,
            has_gap=has_gap,
            schema_version=schema_version,
        ),
        record_version=int(row["record_version"]),
    )


def _without_storage_scale(value: Decimal) -> Decimal:
    """Remove only insignificant ClickHouse Decimal trailing zeroes."""

    return Decimal(format(value.normalize(), "f"))


def _parquet_float_decimal(value: Decimal) -> Decimal:
    """Reproduce ``Decimal(str(float_value))`` used by the backfill importer."""

    normalized = _without_storage_scale(value)
    if normalized == normalized.to_integral_value():
        return Decimal(f"{format(normalized, 'f')}.0")
    return normalized


def _observed_at(candle: ScientificCandle) -> datetime:
    return candle.candle_at + timedelta(minutes=1)


def _datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    raise ValueError(f"invalid ClickHouse boolean: {value!r}")


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _clickhouse_datetime(value: datetime) -> str:
    return _aware_utc(value, "timestamp").strftime("%Y-%m-%d %H:%M:%S.%f")
