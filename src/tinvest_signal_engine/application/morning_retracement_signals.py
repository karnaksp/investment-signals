"""Generate live morning-retracement recommendations from mapped candle data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.morning_retracement_research import (
    causal_morning_feature_values,
    causal_previous_session_feature_values,
    estimate_tick_size,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import build_snapshot
from tinvest_signal_engine.domain.morning_retracement_signal import (
    MorningRetracementRecommendation,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
    build_recommendation,
)


MOSCOW = ZoneInfo("Europe/Moscow")
MAX_RECOMMENDATION_STALENESS = timedelta(minutes=2)


@dataclass(frozen=True, slots=True)
class MorningRetracementMarketSeries:
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    trading_day: date
    previous_session: tuple[HistoricalCandle, ...]
    current_session: tuple[HistoricalCandle, ...]
    historical_cumulative_volume: float | None


class GenerateMorningRetracementRecommendations:
    """Apply the sealed model and owner filters without external dependencies."""

    def __init__(self, policy: MorningRetracementRuntimePolicy) -> None:
        self._policy = policy

    def execute(
        self,
        series: Sequence[MorningRetracementMarketSeries],
        *,
        settings: MorningRetracementRuntimeSettings,
        already_emitted_instruments: frozenset[str] = frozenset(),
        as_of: datetime | None = None,
    ) -> tuple[tuple[MorningRetracementMarketSeries, MorningRetracementRecommendation], ...]:
        if not settings.enabled:
            return ()
        result: list[
            tuple[MorningRetracementMarketSeries, MorningRetracementRecommendation]
        ] = []
        for item in sorted(series, key=lambda value: value.ticker):
            if item.instrument_id in already_emitted_instruments:
                continue
            if settings.enabled_tickers and item.ticker not in settings.enabled_tickers:
                continue
            recommendation = self._score(item, settings, as_of=as_of)
            if recommendation is None:
                continue
            result.append((item, recommendation))
            if len(result) >= settings.maximum_signals_per_day:
                break
        return tuple(result)

    def _score(
        self,
        series: MorningRetracementMarketSeries,
        settings: MorningRetracementRuntimeSettings,
        *,
        as_of: datetime | None,
    ) -> MorningRetracementRecommendation | None:
        if not series.previous_session or not series.current_session:
            return None
        decision_rows = tuple(
            row
            for row in series.current_session
            if (
                settings.first_decision_local_minute
                <= _local_minute(row)
                <= settings.last_decision_local_minute
                and _local_minute(row) % 5 == 0
            )
        )
        if not decision_rows:
            return None
        latest = decision_rows[-1]
        effective_as_of = as_of or series.current_session[-1].at
        if (
            effective_as_of < latest.at
            or effective_as_of - latest.at > MAX_RECOMMENDATION_STALENESS
        ):
            return None
        local_minute = _local_minute(latest)
        morning_rows = tuple(
            row
            for row in series.current_session
            if 7 * 60 <= _local_minute(row) <= local_minute
        )
        if not morning_rows:
            return None
        snapshot = build_snapshot(
            ticker=series.ticker,
            observed_at=latest.at,
            previous_close=series.previous_session[-1].close,
            observed_candles=morning_rows,
            analytical_floor_bps=10.0,
            tick_size=estimate_tick_size(series.previous_session + morning_rows),
        )
        if snapshot is None or snapshot.excursion_bps < settings.minimum_excursion_bps:
            return None
        morning_features = causal_morning_feature_values(
            snapshot,
            morning_rows,
            historical_cumulative_volume=series.historical_cumulative_volume,
        )
        if self._policy.require_volume_baseline and not bool(
            morning_features["morning_volume_baseline_available"]
        ):
            return None
        relative_volume = float(morning_features["morning_relative_volume"])
        active_ratio = float(morning_features["morning_active_minute_ratio"])
        if (
            relative_volume > settings.maximum_relative_volume
            or active_ratio < settings.minimum_active_minute_ratio
        ):
            return None
        features: dict[str, float | str] = {
            **causal_previous_session_feature_values(series.previous_session),
            **morning_features,
            "ticker": series.ticker,
        }
        probability = self._policy.model.probability(features)
        if probability < settings.probability_threshold:
            return None
        target_price = snapshot.target_price(self._policy.target_fraction)
        remaining_move_bps = (
            int(snapshot.direction)
            * (target_price / snapshot.current_price - 1.0)
            * 10_000.0
        )
        if remaining_move_bps < settings.minimum_remaining_move_bps:
            return None
        return build_recommendation(
            snapshot=snapshot,
            probability=probability,
            relative_volume=relative_volume,
            active_minute_ratio=active_ratio,
            policy=self._policy,
        )


def _local_minute(candle: HistoricalCandle) -> int:
    local = candle.at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute
