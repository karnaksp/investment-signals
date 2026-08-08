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
    MorningRetracementLiveAssessment,
    MorningRetracementRecommendation,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
    build_recommendation,
)


MOSCOW = ZoneInfo("Europe/Moscow")
COMPLETED_CANDLE_DURATION = timedelta(minutes=1)
MAX_COMPLETED_CANDLE_DELAY = timedelta(minutes=5)


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
            if settings.enabled_tickers and item.ticker not in settings.enabled_tickers:
                continue
            if item.instrument_id in already_emitted_instruments:
                continue
            assessment = self._assess(
                item,
                settings,
                as_of=as_of,
                formal_signal=True,
            )
            if assessment is None:
                continue
            if not assessment.eligible_for_signal:
                continue
            result.append((item, assessment.recommendation))
            if len(result) >= settings.maximum_signals_per_day:
                break
        return tuple(result)

    def assess(
        self,
        series: Sequence[MorningRetracementMarketSeries],
        *,
        settings: MorningRetracementRuntimeSettings,
        as_of: datetime | None = None,
    ) -> tuple[
        tuple[MorningRetracementMarketSeries, MorningRetracementLiveAssessment],
        ...,
    ]:
        """Return every causal live assessment, including abstentions."""

        if not settings.enabled:
            return ()
        result: list[
            tuple[MorningRetracementMarketSeries, MorningRetracementLiveAssessment]
        ] = []
        for item in sorted(series, key=lambda value: value.ticker):
            if settings.enabled_tickers and item.ticker not in settings.enabled_tickers:
                continue
            assessment = self._assess(
                item,
                settings,
                as_of=as_of,
                formal_signal=False,
            )
            if assessment is not None:
                result.append((item, assessment))
        return tuple(result)

    def _assess(
        self,
        series: MorningRetracementMarketSeries,
        settings: MorningRetracementRuntimeSettings,
        *,
        as_of: datetime | None,
        formal_signal: bool,
    ) -> MorningRetracementLiveAssessment | None:
        if not series.previous_session or not series.current_session:
            return None
        decision_rows = tuple(
            row
            for row in series.current_session
            if (
                settings.first_decision_local_minute
                <= _local_minute(row)
                <= (
                    settings.last_decision_local_minute
                    if formal_signal
                    else settings.monitor_until_local_minute
                )
            )
        )
        if not decision_rows:
            return None
        latest = decision_rows[-1]
        effective_as_of = as_of or _completed_at(series.current_session[-1])
        latest_completed_at = _completed_at(latest)
        if (
            effective_as_of < latest_completed_at
            or effective_as_of - latest_completed_at > MAX_COMPLETED_CANDLE_DELAY
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
        if snapshot is None:
            return None
        morning_features = causal_morning_feature_values(
            snapshot,
            morning_rows,
            historical_cumulative_volume=series.historical_cumulative_volume,
        )
        baseline_available = bool(
            morning_features["morning_volume_baseline_available"]
        )
        relative_volume = float(morning_features["morning_relative_volume"])
        active_ratio = float(morning_features["morning_active_minute_ratio"])
        current_retracement_fraction = float(
            morning_features["current_retracement_fraction"]
        )
        features: dict[str, float | str] = {
            **causal_previous_session_feature_values(series.previous_session),
            **morning_features,
            "ticker": series.ticker,
        }
        probability = self._policy.model.probability(features)
        non_loss_probability = (
            self._policy.effective_non_loss_model.probability(features)
        )
        target_price = snapshot.target_price(self._policy.target_fraction)
        remaining_move_bps = max(
            0.0,
            int(snapshot.direction)
            * (target_price / snapshot.current_price - 1.0)
            * 10_000.0,
        )
        recommendation = build_recommendation(
            snapshot=snapshot,
            probability=probability,
            relative_volume=relative_volume,
            active_minute_ratio=active_ratio,
            policy=self._policy,
            non_loss_probability=non_loss_probability,
        )
        reasons: list[str] = []
        if snapshot.excursion_bps < settings.minimum_excursion_bps:
            reasons.append("excursion_below_minimum")
        if (
            self._policy.require_volume_baseline
            or settings.minimum_relative_volume > 0.0
        ) and not baseline_available:
            reasons.append("volume_baseline_unavailable")
        if baseline_available and relative_volume > settings.maximum_relative_volume:
            reasons.append("relative_volume_above_maximum")
        if baseline_available and relative_volume < settings.minimum_relative_volume:
            reasons.append("relative_volume_below_minimum")
        if active_ratio < settings.minimum_active_minute_ratio:
            reasons.append("active_minute_ratio_below_minimum")
        if (
            current_retracement_fraction
            < settings.minimum_current_retracement_fraction
        ):
            reasons.append("current_retracement_below_minimum")
        if probability < settings.probability_threshold:
            reasons.append("probability_below_threshold")
        non_loss_threshold = (
            settings.effective_non_loss_probability_threshold(self._policy)
        )
        if non_loss_probability < non_loss_threshold:
            reasons.append("non_loss_probability_below_threshold")
        if remaining_move_bps < settings.minimum_remaining_move_bps:
            reasons.append("remaining_move_below_minimum")
        if local_minute > settings.last_decision_local_minute:
            reasons.append("outside_signal_window")
        return MorningRetracementLiveAssessment(
            instrument_id=series.instrument_id,
            ticker=series.ticker,
            trading_day=series.trading_day.isoformat(),
            recommendation=recommendation,
            eligible_for_signal=not reasons,
            reason_codes=tuple(reasons),
            settings_revision=settings.revision,
            policy_version=self._policy.policy_version,
            hypothesis_version=self._policy.hypothesis_version,
            model_fingerprint=self._policy.model.fingerprint,
            probability_threshold=settings.probability_threshold,
            maximum_relative_volume=settings.maximum_relative_volume,
            minimum_excursion_bps=settings.minimum_excursion_bps,
            minimum_remaining_move_bps=settings.minimum_remaining_move_bps,
            remaining_move_bps=remaining_move_bps,
            deadline_local_minute=self._policy.deadline_local_minute,
            expected_hit_minutes_p25=self._policy.expected_hit_minutes_p25,
            expected_hit_minutes_median=self._policy.expected_hit_minutes_median,
            expected_hit_minutes_p75=self._policy.expected_hit_minutes_p75,
            training_window_ended=local_minute > 10 * 60,
            non_loss_probability_threshold=non_loss_threshold,
            non_loss_model_fingerprint=(
                self._policy.effective_non_loss_model.fingerprint
            ),
            target_fraction=self._policy.target_fraction,
            current_retracement_fraction=current_retracement_fraction,
            minimum_current_retracement_fraction=(
                settings.minimum_current_retracement_fraction
            ),
            minimum_relative_volume=settings.minimum_relative_volume,
        )


def _local_minute(candle: HistoricalCandle) -> int:
    local = candle.at.astimezone(MOSCOW)
    return local.hour * 60 + local.minute


def _completed_at(candle: HistoricalCandle) -> datetime:
    """Return when the complete one-minute candle first became observable."""

    return candle.at + COMPLETED_CANDLE_DURATION
