"""File and ClickHouse adapters for the live morning-retracement worker."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from statistics import median
from threading import Lock
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.morning_retracement_signals import (
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.application.morning_retracement_tracking import (
    MorningRetracementTrackingStore,
    StoredMorningRetracementAssessment,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveScientificStore,
)
from tinvest_signal_engine.config import InstrumentSubscriptionConfig
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement import (
    MorningSnapshot,
    RetracementDirection,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    LinearProbabilityModel,
    MorningRetracementLiveAssessment,
    MorningRetracementRecommendation,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
    MorningRetracementTrackedOutcome,
)


MOSCOW = ZoneInfo("Europe/Moscow")
_MORNING_START_LOCAL_MINUTE = 7 * 60
_MORNING_MONITOR_UNTIL_LOCAL_MINUTE = 11 * 60
_LATEST_SESSION_CANDLES_SQL = """
SELECT
    instrument_id,
    ticker,
    trading_day,
    candle_at,
    argMax(open_price, record_version) AS open_price,
    argMax(high_price, record_version) AS high_price,
    argMax(low_price, record_version) AS low_price,
    argMax(close_price, record_version) AS close_price,
    argMax(volume, record_version) AS volume,
    argMax(is_complete, record_version) AS is_complete
FROM scientific_candles_1m
PREWHERE instrument_id IN {instrument_ids:Array(String)}
WHERE trading_day >= today() - 10
  AND candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
  AND trading_day IN
  (
      SELECT trading_day
      FROM scientific_candles_1m
      PREWHERE instrument_id IN {instrument_ids:Array(String)}
      WHERE trading_day >= today() - 10
        AND candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
      GROUP BY trading_day
      ORDER BY trading_day DESC
      LIMIT 2
  )
GROUP BY instrument_id, ticker, trading_day, candle_at
ORDER BY ticker, trading_day, candle_at
LIMIT 100000
SETTINGS max_execution_time = 10,
         timeout_before_checking_execution_speed = 0,
         max_threads = 2,
         max_rows_to_read = 2000000,
         max_result_rows = 100000,
         result_overflow_mode = 'throw'
FORMAT JSONEachRow
""".strip()

_VOLUME_HISTORY_SQL = """
SELECT ticker, trading_day, sum(volume) AS cumulative_volume
FROM
(
    SELECT
        ticker,
        trading_day,
        candle_at,
        argMax(volume, record_version) AS volume,
        argMax(is_complete, record_version) AS is_complete
    FROM scientific_candles_1m
    PREWHERE instrument_id IN {instrument_ids:Array(String)}
    WHERE trading_day >= today() - 45
      AND trading_day < toDate(parseDateTime64BestEffort({as_of:String}, 6, 'UTC'))
      AND toHour(toTimeZone(candle_at, 'Europe/Moscow')) * 60
          + toMinute(toTimeZone(candle_at, 'Europe/Moscow')) BETWEEN 420
          AND toUInt16({local_minute:UInt16})
    GROUP BY ticker, trading_day, candle_at
)
WHERE is_complete = 1
GROUP BY ticker, trading_day
ORDER BY ticker, trading_day DESC
LIMIT 2000
SETTINGS max_execution_time = 10,
         timeout_before_checking_execution_speed = 0,
         max_threads = 2,
         max_rows_to_read = 3000000,
         max_result_rows = 2000,
         result_overflow_mode = 'throw'
FORMAT JSONEachRow
""".strip()

MORNING_RETRACEMENT_LIVE_RECORD_VERSION = "morning-retracement-live-v1"
_MORNING_SOURCE_IDS = (
    "heston-korajczyk-sadka-2010",
    "jegadeesh-titman-1995",
    "kitapbayev-leung-2017",
    "lipton-lopez-de-prado-2020",
)
_PENDING_ASSESSMENTS_SQL = """
SELECT
    observation_id,
    feature_values_json
FROM scientific_hypothesis_observations FINAL
WHERE record_schema_version = {record_schema_version:String}
  AND observation_id NOT IN
  (
      SELECT observation_id
      FROM scientific_hypothesis_outcomes FINAL
      WHERE record_schema_version = {record_schema_version:String}
        AND outcome_policy_version = {outcome_policy_version:String}
  )
ORDER BY target_at ASC, observed_at ASC
LIMIT {limit:UInt32}
FORMAT JSONEachRow
""".strip()


def load_morning_retracement_policy(path: Path) -> MorningRetracementRuntimePolicy:
    raw = json.loads(path.read_text(encoding="utf-8"))
    model = raw["runtime_model"]
    recommendation = raw["recommendation"]
    operational = raw["operational_filter"]
    window = raw["expected_hit_window"]
    return MorningRetracementRuntimePolicy(
        policy_version=f"{raw['hypothesis_version']}:runtime-v1",
        hypothesis_id="h1-selective-morning-retracement",
        hypothesis_version="2.2.0",
        model=LinearProbabilityModel(
            feature_names=tuple(str(item) for item in model["feature_names"]),
            coefficients=tuple(float(item) for item in model["coefficients"]),
            intercept=float(model["intercept"]),
            fingerprint=str(model["fingerprint"]),
        ),
        target_fraction=float(raw["target_fraction"]),
        default_probability_threshold=float(raw["probability_threshold"]),
        stop_extension_fraction=float(raw["stop_extension_fraction"]),
        break_even_trigger_fraction=float(raw["break_even_trigger_fraction"]),
        deadline_local_minute=int(raw["deadline_local_minute"]),
        round_trip_cost_bps=float(raw["round_trip_cost_bps"]),
        require_volume_baseline=bool(operational["require_volume_baseline"]),
        default_maximum_relative_volume=float(
            operational["maximum_relative_volume"]
        ),
        default_minimum_active_minute_ratio=float(
            operational["minimum_active_minute_ratio"]
        ),
        historical_target_probability=float(recommendation["target_probability"]),
        historical_target_probability_lower=float(
            recommendation["target_probability_lower"]
        ),
        historical_non_loss_probability=float(
            recommendation["non_loss_probability"]
        ),
        historical_non_loss_probability_lower=float(
            recommendation["non_loss_probability_lower"]
        ),
        historical_sample_count=int(recommendation["sample_count"]),
        historical_trading_days=int(recommendation["trading_days"]),
        expected_hit_minutes_p25=round(float(window["p25_minutes"])),
        expected_hit_minutes_median=round(float(window["median_minutes"])),
        expected_hit_minutes_p75=round(float(window["p75_minutes"])),
    )


def load_morning_retracement_settings(
    path: Path,
    policy: MorningRetracementRuntimePolicy,
) -> MorningRetracementRuntimeSettings:
    config: Mapping[str, Any] = {}
    if path.exists():
        decoded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(decoded, dict):
            candidate = decoded.get("morning_retracement")
            if isinstance(candidate, dict):
                config = candidate
    return MorningRetracementRuntimeSettings(
        enabled=bool(config.get("enabled", True)),
        revision=int(config.get("revision", 1)),
        probability_threshold=float(
            config.get("probability_threshold", policy.default_probability_threshold)
        ),
        maximum_relative_volume=float(
            config.get(
                "maximum_relative_volume",
                policy.default_maximum_relative_volume,
            )
        ),
        minimum_active_minute_ratio=float(
            config.get(
                "minimum_active_minute_ratio",
                policy.default_minimum_active_minute_ratio,
            )
        ),
        minimum_excursion_bps=float(config.get("minimum_excursion_bps", 40.0)),
        minimum_remaining_move_bps=float(
            config.get(
                "minimum_remaining_move_bps",
                2.0 * policy.round_trip_cost_bps,
            )
        ),
        first_decision_local_minute=int(
            config.get("first_decision_local_minute", 7 * 60 + 15)
        ),
        last_decision_local_minute=int(
            config.get("last_decision_local_minute", 10 * 60)
        ),
        monitor_until_local_minute=int(
            config.get("monitor_until_local_minute", 11 * 60)
        ),
        maximum_signals_per_day=int(
            config.get("maximum_signals_per_day", 10)
        ),
        enabled_tickers=frozenset(
            str(item).strip().upper()
            for item in config.get("enabled_tickers", ())
            if str(item).strip()
        ),
        telegram_enabled=bool(config.get("telegram_enabled", False)),
    )


class ClickHouseMorningRetracementSource:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds
        self._market_cache_lock = Lock()
        self._market_cache_key: tuple[date, int, tuple[str, ...]] | None = None
        self._market_cache_rows: tuple[MorningRetracementMarketSeries, ...] = ()
        self._volume_cache_lock = Lock()
        self._volume_cache_key: tuple[date, int, tuple[str, ...]] | None = None
        self._volume_cache_rows: tuple[dict[str, Any], ...] = ()

    def load(
        self,
        *,
        as_of: datetime,
        instruments: Sequence[InstrumentSubscriptionConfig],
    ) -> tuple[MorningRetracementMarketSeries, ...]:
        cutoff = as_of.astimezone(timezone.utc)
        local = cutoff.astimezone(MOSCOW)
        local_minute = local.hour * 60 + local.minute
        if local_minute < _MORNING_START_LOCAL_MINUTE:
            return ()
        instrument_ids = tuple(
            dict.fromkeys(
                item.instrument_id.strip()
                for item in instruments
                if item.instrument_id.strip()
            )
        )
        if not instrument_ids:
            return ()
        effective_local_minute = min(
            local_minute,
            _MORNING_MONITOR_UNTIL_LOCAL_MINUTE,
        )
        if local_minute > _MORNING_MONITOR_UNTIL_LOCAL_MINUTE:
            cutoff = local.replace(
                hour=_MORNING_MONITOR_UNTIL_LOCAL_MINUTE // 60,
                minute=_MORNING_MONITOR_UNTIL_LOCAL_MINUTE % 60,
                second=59,
                microsecond=999_999,
            ).astimezone(timezone.utc)
        market_cache_key = (
            local.date(),
            effective_local_minute,
            instrument_ids,
        )
        with self._market_cache_lock:
            if self._market_cache_key == market_cache_key:
                return self._market_cache_rows
        parameters = {
            "as_of": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "local_minute": str(effective_local_minute),
            "instrument_ids": _array_parameter(instrument_ids),
        }
        candles = self._rows(_LATEST_SESSION_CANDLES_SQL, parameters)
        volumes = self._volume_rows(
            parameters=parameters,
            local_day=local.date(),
            local_minute=effective_local_minute,
            instrument_ids=instrument_ids,
        )
        volume_history: defaultdict[str, list[float]] = defaultdict(list)
        for row in volumes:
            values = volume_history[str(row["ticker"]).upper()]
            if len(values) < 20:
                values.append(float(row["cumulative_volume"]))
        baseline = {
            ticker: median(values)
            for ticker, values in volume_history.items()
            if values
        }
        by_instrument: defaultdict[
            tuple[str, str], defaultdict[date, list[HistoricalCandle]]
        ] = defaultdict(lambda: defaultdict(list))
        for row in candles:
            if not bool(int(row["is_complete"])):
                continue
            ticker = str(row["ticker"]).upper()
            instrument_id = str(row["instrument_id"])
            trading_day = date.fromisoformat(str(row["trading_day"]))
            by_instrument[(instrument_id, ticker)][trading_day].append(
                HistoricalCandle(
                    ticker=ticker,
                    at=_timestamp(row["candle_at"]),
                    open=float(row["open_price"]),
                    high=float(row["high_price"]),
                    low=float(row["low_price"]),
                    close=float(row["close_price"]),
                    volume=float(row["volume"]),
                    complete=True,
                )
            )
        configured = {item.instrument_id: item for item in instruments}
        result: list[MorningRetracementMarketSeries] = []
        for (instrument_id, ticker), sessions in by_instrument.items():
            instrument = configured.get(instrument_id)
            days = sorted(sessions)
            if instrument is None or len(days) < 2:
                continue
            current_day = days[-1]
            if current_day != local.date():
                continue
            previous_day = days[-2]
            result.append(
                MorningRetracementMarketSeries(
                    instrument_id=instrument_id,
                    ticker=ticker,
                    class_code=instrument.class_code,
                    alias=instrument.alias,
                    trading_day=current_day,
                    previous_session=tuple(sessions[previous_day]),
                    current_session=tuple(sessions[current_day]),
                    historical_cumulative_volume=baseline.get(ticker),
                )
            )
        market = tuple(result)
        if market or local_minute <= _MORNING_MONITOR_UNTIL_LOCAL_MINUTE:
            with self._market_cache_lock:
                self._market_cache_key = market_cache_key
                self._market_cache_rows = market
        return market

    def _volume_rows(
        self,
        *,
        parameters: Mapping[str, str],
        local_day: date,
        local_minute: int,
        instrument_ids: tuple[str, ...],
    ) -> tuple[dict[str, Any], ...]:
        cache_key = (local_day, local_minute // 5, instrument_ids)
        with self._volume_cache_lock:
            if self._volume_cache_key == cache_key:
                return self._volume_cache_rows
        rows = self._rows(_VOLUME_HISTORY_SQL, parameters)
        with self._volume_cache_lock:
            self._volume_cache_key = cache_key
            self._volume_cache_rows = rows
        return rows

    def _rows(
        self,
        sql: str,
        parameters: Mapping[str, str],
    ) -> tuple[dict[str, Any], ...]:
        query_id = f"morning-retracement-{uuid4()}"
        query = {
            "database": self._database,
            "query_id": query_id,
            "cancel_http_readonly_queries_on_client_close": "1",
            **{f"param_{key}": value for key, value in parameters.items()},
        }
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=(sql + "\n").encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        with urlopen(request, timeout=self._timeout_seconds) as response:
            return tuple(
                json.loads(line)
                for line in response.read().decode("utf-8").splitlines()
                if line.strip()
            )


def _array_parameter(values: Sequence[str]) -> str:
    escaped = (
        "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        for value in values
    )
    return "[" + ",".join(escaped) + "]"


class ClickHouseMorningRetracementTrackingStore(
    ClickHouseProspectiveScientificStore,
    MorningRetracementTrackingStore,
):
    """Map the hypothesis-specific domain records to shared evidence tables."""

    def persist_assessment(
        self,
        assessment: MorningRetracementLiveAssessment,
        *,
        recorded_at: datetime,
    ) -> str:
        observation_id = _assessment_id(assessment)
        row = _assessment_row(
            observation_id=observation_id,
            assessment=assessment,
            recorded_at=recorded_at,
        )
        self._persist_immutable(
            table="scientific_hypothesis_observations",
            identity_column="observation_id",
            identity=observation_id,
            payload_fingerprint=str(row["payload_fingerprint"]),
            row=row,
        )
        return observation_id

    def pending_assessments(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[StoredMorningRetracementAssessment, ...]:
        payload = self._request(
            _PENDING_ASSESSMENTS_SQL,
            parameters={
                "record_schema_version": MORNING_RETRACEMENT_LIVE_RECORD_VERSION,
                "outcome_policy_version": outcome_policy_version,
                "limit": str(limit),
            },
        )
        rows = tuple(
            json.loads(line)
            for line in payload.decode("utf-8").splitlines()
            if line.strip()
        )
        return tuple(
            StoredMorningRetracementAssessment(
                observation_id=str(row["observation_id"]),
                assessment=_assessment_from_payload(
                    json.loads(str(row["feature_values_json"]))
                ),
            )
            for row in rows
        )

    def persist_outcome(
        self,
        outcome: MorningRetracementTrackedOutcome,
        *,
        assessment: MorningRetracementLiveAssessment,
    ) -> None:
        row = _outcome_row(outcome=outcome, assessment=assessment)
        self._persist_immutable(
            table="scientific_hypothesis_outcomes",
            identity_column="outcome_id",
            identity=str(row["outcome_id"]),
            payload_fingerprint=str(row["payload_fingerprint"]),
            row=row,
        )


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace(" ", "T").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _assessment_id(assessment: MorningRetracementLiveAssessment) -> str:
    return "morning-retracement:" + sha256(
        assessment.observation_key.encode("utf-8")
    ).hexdigest()


def _assessment_payload(
    assessment: MorningRetracementLiveAssessment,
) -> dict[str, Any]:
    recommendation = assessment.recommendation
    snapshot = recommendation.snapshot
    return {
        "instrument_id": assessment.instrument_id,
        "ticker": assessment.ticker,
        "trading_day": assessment.trading_day,
        "observed_at": assessment.observed_at.isoformat(),
        "previous_close": snapshot.previous_close,
        "current_price": snapshot.current_price,
        "running_extreme": snapshot.running_extreme,
        "extreme_at": snapshot.extreme_at.isoformat(),
        "expected_direction": recommendation.expected_direction,
        "excursion_bps": snapshot.excursion_bps,
        "tick_size": snapshot.tick_size,
        "model_probability": recommendation.model_probability,
        "target_price": recommendation.target_price,
        "initial_stop_price": recommendation.initial_stop_price,
        "break_even_trigger_price": recommendation.break_even_trigger_price,
        "break_even_stop_price": recommendation.break_even_stop_price,
        "relative_volume": recommendation.relative_volume,
        "active_minute_ratio": recommendation.active_minute_ratio,
        "eligible_for_signal": assessment.eligible_for_signal,
        "reason_codes": list(assessment.reason_codes),
        "settings_revision": assessment.settings_revision,
        "policy_version": assessment.policy_version,
        "hypothesis_version": assessment.hypothesis_version,
        "model_fingerprint": assessment.model_fingerprint,
        "probability_threshold": assessment.probability_threshold,
        "maximum_relative_volume": assessment.maximum_relative_volume,
        "minimum_excursion_bps": assessment.minimum_excursion_bps,
        "minimum_remaining_move_bps": assessment.minimum_remaining_move_bps,
        "remaining_move_bps": assessment.remaining_move_bps,
        "deadline_local_minute": assessment.deadline_local_minute,
        "expected_hit_minutes_p25": assessment.expected_hit_minutes_p25,
        "expected_hit_minutes_median": assessment.expected_hit_minutes_median,
        "expected_hit_minutes_p75": assessment.expected_hit_minutes_p75,
        "training_window_ended": assessment.training_window_ended,
    }


def _assessment_row(
    *,
    observation_id: str,
    assessment: MorningRetracementLiveAssessment,
    recorded_at: datetime,
) -> dict[str, object]:
    payload = _assessment_payload(assessment)
    observed = assessment.observed_at
    local = observed.astimezone(MOSCOW)
    deadline = local.replace(
        hour=assessment.deadline_local_minute // 60,
        minute=assessment.deadline_local_minute % 60,
        second=0,
        microsecond=0,
    )
    input_fingerprint = _fingerprint(payload)
    row: dict[str, object] = {
        "observation_id": observation_id,
        "hypothesis_id": "h1-morning-low-volume-reversion",
        "hypothesis_version": assessment.hypothesis_version,
        "policy_version": assessment.policy_version,
        "formula_version": f"morning-retracement-logit-{assessment.hypothesis_version}",
        "formula_fingerprint": assessment.model_fingerprint,
        "scientific_source_ids": list(_MORNING_SOURCE_IDS),
        "instrument_id": assessment.instrument_id,
        "ticker": assessment.ticker,
        "trading_day": assessment.trading_day,
        "observed_at": observed.isoformat(),
        "feature_max_observed_at": observed.isoformat(),
        "model_trained_until": None,
        "market_phase": "morning_live_monitoring",
        "phase_bucket": local.strftime("%H:%M"),
        "decision": "matched" if assessment.eligible_for_signal else "abstain",
        "reason_code": (
            "conditions_matched"
            if assessment.eligible_for_signal
            else assessment.reason_codes[0]
        ),
        "expected_direction": (
            1 if assessment.recommendation.expected_direction == "up" else -1
        ),
        "forecast_value": assessment.recommendation.model_probability,
        "target_metric": "r50_hit",
        "effect_unit": "probability",
        "claim_scope": "research_recommendation",
        "horizon_seconds": max(1, int((deadline - observed).total_seconds())),
        "target_at": deadline.isoformat(),
        "feature_values_json": json.dumps(
            payload, allow_nan=False, ensure_ascii=True, sort_keys=True
        ),
        "thresholds_json": json.dumps(
            {
                "probability": assessment.probability_threshold,
                "maximum_relative_volume": assessment.maximum_relative_volume,
                "minimum_excursion_bps": assessment.minimum_excursion_bps,
                "minimum_remaining_move_bps": assessment.minimum_remaining_move_bps,
            },
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
        ),
        "input_window_start": local.replace(
            hour=7, minute=0, second=0, microsecond=0
        ).isoformat(),
        "input_window_end": observed.isoformat(),
        "source_kind": "stream",
        "source_max_observed_at": observed.isoformat(),
        "has_gap": 0,
        "source_event_ids": [
            "scientific-candle:" + sha256(
                f"{assessment.instrument_id}:{observed.isoformat()}".encode("utf-8")
            ).hexdigest()
        ],
        "input_fingerprint": input_fingerprint,
        "dataset_fingerprint": _fingerprint(
            {
                "instrument_id": assessment.instrument_id,
                "trading_day": assessment.trading_day,
            }
        ),
        "config_fingerprint": _fingerprint(
            {
                "policy": assessment.policy_version,
                "settings_revision": assessment.settings_revision,
            }
        ),
        "payload_fingerprint": "",
        "recorded_at": recorded_at.isoformat(),
        "record_version": int(recorded_at.timestamp() * 1_000_000),
        "record_schema_version": MORNING_RETRACEMENT_LIVE_RECORD_VERSION,
    }
    row["payload_fingerprint"] = _fingerprint(
        {
            key: value
            for key, value in row.items()
            if key not in {"payload_fingerprint", "recorded_at", "record_version"}
        }
    )
    return row


def _assessment_from_payload(payload: Mapping[str, Any]) -> MorningRetracementLiveAssessment:
    observed_at = _timestamp(payload["observed_at"])
    expected_direction = str(payload["expected_direction"])
    direction = (
        RetracementDirection.RETURN_UP
        if expected_direction == "up"
        else RetracementDirection.RETURN_DOWN
    )
    snapshot = MorningSnapshot(
        ticker=str(payload["ticker"]),
        observed_at=observed_at,
        previous_close=float(payload["previous_close"]),
        current_price=float(payload["current_price"]),
        running_extreme=float(payload["running_extreme"]),
        extreme_at=_timestamp(payload["extreme_at"]),
        direction=direction,
        excursion_bps=float(payload["excursion_bps"]),
        tick_size=float(payload["tick_size"]),
    )
    recommendation = MorningRetracementRecommendation(
        snapshot=snapshot,
        model_probability=float(payload["model_probability"]),
        target_price=float(payload["target_price"]),
        initial_stop_price=float(payload["initial_stop_price"]),
        break_even_trigger_price=float(payload["break_even_trigger_price"]),
        break_even_stop_price=float(payload["break_even_stop_price"]),
        relative_volume=float(payload["relative_volume"]),
        active_minute_ratio=float(payload["active_minute_ratio"]),
        observed_at=observed_at,
    )
    return MorningRetracementLiveAssessment(
        instrument_id=str(payload["instrument_id"]),
        ticker=str(payload["ticker"]),
        trading_day=str(payload["trading_day"]),
        recommendation=recommendation,
        eligible_for_signal=bool(payload["eligible_for_signal"]),
        reason_codes=tuple(str(item) for item in payload["reason_codes"]),
        settings_revision=int(payload["settings_revision"]),
        policy_version=str(payload["policy_version"]),
        hypothesis_version=str(payload["hypothesis_version"]),
        model_fingerprint=str(payload["model_fingerprint"]),
        probability_threshold=float(payload["probability_threshold"]),
        maximum_relative_volume=float(payload["maximum_relative_volume"]),
        minimum_excursion_bps=float(payload["minimum_excursion_bps"]),
        minimum_remaining_move_bps=float(payload["minimum_remaining_move_bps"]),
        remaining_move_bps=float(payload["remaining_move_bps"]),
        deadline_local_minute=int(payload["deadline_local_minute"]),
        expected_hit_minutes_p25=int(payload["expected_hit_minutes_p25"]),
        expected_hit_minutes_median=int(payload["expected_hit_minutes_median"]),
        expected_hit_minutes_p75=int(payload["expected_hit_minutes_p75"]),
        training_window_ended=bool(payload["training_window_ended"]),
    )


def _outcome_row(
    *,
    outcome: MorningRetracementTrackedOutcome,
    assessment: MorningRetracementLiveAssessment,
) -> dict[str, object]:
    actual = (
        None if outcome.target_hit is None else float(outcome.target_hit)
    )
    probability = assessment.recommendation.model_probability
    deadline = assessment.observed_at.astimezone(MOSCOW).replace(
        hour=assessment.deadline_local_minute // 60,
        minute=assessment.deadline_local_minute % 60,
        second=0,
        microsecond=0,
    )
    measurements = {
        "observed_at": assessment.observed_at.isoformat(),
        "decision_local_minute": (
            assessment.observed_at.astimezone(MOSCOW).hour * 60
            + assessment.observed_at.astimezone(MOSCOW).minute
        ),
        "model_probability": probability,
        "eligible_for_signal": assessment.eligible_for_signal,
        "target_hit": outcome.target_hit,
        "non_loss": outcome.non_loss,
        "exit_reason": outcome.exit_reason,
        "entry_at": outcome.entry_at.isoformat() if outcome.entry_at else None,
        "exit_at": outcome.exit_at.isoformat() if outcome.exit_at else None,
        "entry_price": outcome.entry_price,
        "exit_price": outcome.exit_price,
        "net_result_bps": outcome.net_result_bps,
        "minutes_to_exit": outcome.minutes_to_exit,
    }
    evidence_fingerprint = _fingerprint(measurements)
    outcome_id = "morning-retracement-outcome:" + sha256(
        f"{outcome.observation_id}:{outcome.outcome_policy_version}".encode("utf-8")
    ).hexdigest()
    row: dict[str, object] = {
        "outcome_id": outcome_id,
        "observation_id": outcome.observation_id,
        "hypothesis_id": "h1-morning-low-volume-reversion",
        "hypothesis_version": assessment.hypothesis_version,
        "instrument_id": assessment.instrument_id,
        "trading_day": assessment.trading_day,
        "target_at": deadline.isoformat(),
        "observed_range_start": assessment.observed_at.isoformat(),
        "observed_range_end": deadline.isoformat(),
        "available": int(outcome.target_hit is not None),
        "reason_code": outcome.exit_reason,
        "actual_value": actual,
        "cost_adjusted_value": outcome.net_result_bps,
        "model_loss": (
            None if actual is None else (probability - actual) ** 2
        ),
        "benchmark_loss": None,
        "supported": None if actual is None else int(bool(outcome.target_hit)),
        "target_metric": "r50_hit",
        "effect_unit": "probability",
        "outcome_policy_version": outcome.outcome_policy_version,
        "source_event_ids": [],
        "source_window_start": assessment.observed_at.isoformat(),
        "source_window_end": deadline.isoformat(),
        "source_max_observed_at": deadline.isoformat(),
        "input_fingerprint": evidence_fingerprint,
        "evaluated_at": outcome.evaluated_at.isoformat(),
        "payload_fingerprint": "",
        "record_version": int(outcome.evaluated_at.timestamp() * 1_000_000),
        "record_schema_version": MORNING_RETRACEMENT_LIVE_RECORD_VERSION,
        "measurements_json": json.dumps(
            measurements, allow_nan=False, ensure_ascii=True, sort_keys=True
        ),
        "evidence_fingerprint": evidence_fingerprint,
    }
    row["payload_fingerprint"] = _fingerprint(
        {
            key: value
            for key, value in row.items()
            if key not in {"payload_fingerprint", "evaluated_at", "record_version"}
        }
    )
    return row


def _fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + sha256(encoded).hexdigest()
