"""File and ClickHouse adapters for the live morning-retracement worker."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
import json
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from tinvest_signal_engine.application.morning_retracement_signals import (
    MorningRetracementMarketSeries,
)
from tinvest_signal_engine.config import InstrumentSubscriptionConfig
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    HistoricalCandle,
)
from tinvest_signal_engine.domain.morning_retracement_signal import (
    LinearProbabilityModel,
    MorningRetracementRuntimePolicy,
    MorningRetracementRuntimeSettings,
)


MOSCOW = ZoneInfo("Europe/Moscow")
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
WHERE trading_day >= today() - 10
  AND candle_at <= parseDateTime64BestEffort({as_of:String}, 6, 'UTC')
GROUP BY instrument_id, ticker, trading_day, candle_at
ORDER BY ticker, trading_day, candle_at
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

    def load(
        self,
        *,
        as_of: datetime,
        instruments: Sequence[InstrumentSubscriptionConfig],
    ) -> tuple[MorningRetracementMarketSeries, ...]:
        cutoff = as_of.astimezone(timezone.utc)
        local = cutoff.astimezone(MOSCOW)
        local_minute = local.hour * 60 + local.minute
        parameters = {
            "as_of": cutoff.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "local_minute": str(local_minute),
        }
        candles = self._rows(_LATEST_SESSION_CANDLES_SQL, parameters)
        volumes = self._rows(_VOLUME_HISTORY_SQL, parameters)
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
        return tuple(result)

    def _rows(
        self,
        sql: str,
        parameters: Mapping[str, str],
    ) -> tuple[dict[str, Any], ...]:
        query = {
            "database": self._database,
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


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace(" ", "T").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
