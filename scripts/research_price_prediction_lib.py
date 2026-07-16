#!/usr/bin/env python3
"""Offline research helpers for signal-triggered price prediction.

This module is deliberately script-level tooling, not production runtime code.
It keeps broker tokens and instrument UIDs out of persisted research artifacts
and provides deterministic feature/label helpers that can be tested without
T-Invest network access.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import re
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


MOSCOW = ZoneInfo("Europe/Moscow")
UTC = timezone.utc
REGULAR_SESSION_START = datetime_time(10, 5)
REGULAR_SESSION_END = datetime_time(18, 39)
ROUND_TRIP_COST_BPS = 10.0
MATERIALITY_BPS = 5.0
TRIPLE_BARRIER_BPS = 10.0
SECRET_VALUE_PATTERN = re.compile(
    r"(?i)(bearer|token|password|secret|api[-_ ]?key)([=: ]+)([^\s,;]+)"
)
DEFAULT_RESEARCH_TICKERS = (
    "SBER",
    "GAZP",
    "LKOH",
    "YDEX",
    "T",
    "VTBR",
    "ROSN",
    "NVTK",
    "GMKN",
    "PLZL",
    "MOEX",
    "MGNT",
    "TRNFP",
    "CHMF",
    "NLMK",
    "TATN",
    "AFKS",
    "ALRS",
    "OZON",
    "PIKK",
    "MTSS",
    "POSI",
    "IRAO",
    "PHOR",
    "RUAL",
)


@dataclass(frozen=True, slots=True)
class ResearchCandle:
    ticker: str
    at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    complete: bool = True


@dataclass(frozen=True, slots=True)
class SignalEvent:
    ticker: str
    signal_type: str
    family: str
    direction: int
    source_event_at: datetime
    trading_day: date
    session_bucket: int
    event_move_bps: float
    baseline_move_bps: float
    z_score: float
    volume_z_score: float
    range_z_score: float
    candle_range_bps: float
    baseline_volatility_bps: float
    anchor_price: float


@dataclass(frozen=True, slots=True)
class ReplayPolicy:
    version: str = "research-price-prediction-v1.0.0"
    detector_window_minutes: int = 3
    detector_baseline_points: int = 40
    detector_min_baseline_points: int = 6
    detector_z_score: float = 4.0
    activity_z_score: float = 3.0
    combo_confirmation_z_score: float = 2.0
    min_relative_metric_excursion: float = 0.12
    cooldown_minutes: int = 5
    volatility_lookback_points: int = 30
    volatility_min_points: int = 20
    volatility_floor_bps: float = 2.0
    round_trip_cost_bps: float = ROUND_TRIP_COST_BPS
    materiality_bps: float = MATERIALITY_BPS
    triple_barrier_bps: float = TRIPLE_BARRIER_BPS


DATASET_FIELDS = (
    "row_id",
    "ticker",
    "signal_type",
    "family",
    "direction",
    "source_event_at",
    "trading_day",
    "session_bucket",
    "horizon_seconds",
    "event_move_bps",
    "baseline_move_bps",
    "z_score",
    "volume_z_score",
    "range_z_score",
    "candle_range_bps",
    "baseline_volatility_bps",
    "day_volatility_bps",
    "day_volatility_quantile",
    "ticker_volatility_quantile",
    "recent_signal_count_60s",
    "recent_signal_count_300s",
    "recent_signal_count_900s",
    "recent_same_family_count_300s",
    "recent_price_jump_300s",
    "recent_volume_spike_300s",
    "recent_candle_range_spike_300s",
    "recent_directional_combo_300s",
    "combo_key_300s",
    "feature_max_observed_at",
    "feature_leakage_flag",
    "pre_return_bps_5m",
    "pre_volatility_bps_5m",
    "pre_range_bps_5m",
    "pre_volume_change_5m",
    "pre_return_bps_15m",
    "pre_volatility_bps_15m",
    "pre_range_bps_15m",
    "pre_volume_change_15m",
    "pre_return_bps_30m",
    "pre_volatility_bps_30m",
    "pre_range_bps_30m",
    "pre_volume_change_30m",
    "pre_return_bps_60m",
    "pre_volatility_bps_60m",
    "pre_range_bps_60m",
    "pre_volume_change_60m",
    "forward_available",
    "forward_reason_code",
    "forward_return_bps",
    "direction_label",
    "cost_adjusted_directional_bps",
    "reverse_directional_bps",
    "triple_barrier_label",
    "meta_label",
)

CANDLE_CACHE_FIELDS = (
    "ticker",
    "at",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "complete",
)


def load_env_value(path: Path, key: str) -> str:
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        candidate, value = line.split("=", 1)
        candidate = candidate.removeprefix("export ").strip()
        result = value.strip().strip('"').strip("'")
        if result:
            values[candidate] = result
    if direct := values.get(key):
        return direct
    if token_file := values.get(f"{key}_FILE"):
        token_path = Path(token_file)
        if not token_path.is_absolute():
            token_path = path.parent / token_path
        token = token_path.read_text(encoding="utf-8").strip()
        if token:
            return token
    raise RuntimeError(f"Required environment key {key} is absent")


def redact_diagnostic(value: object, *, limit: int = 360) -> str:
    text = str(value).replace("\n", " ").replace("\r", " ")
    text = SECRET_VALUE_PATTERN.sub(r"\1\2<redacted>", text)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def quotation(value: Mapping[str, Any] | None) -> float:
    item = value or {}
    return float(item.get("units", "0")) + float(item.get("nano", 0)) / 1_000_000_000


def is_regular_session(at: datetime) -> bool:
    local = at.astimezone(MOSCOW)
    return REGULAR_SESSION_START <= local.time().replace(tzinfo=None) <= REGULAR_SESSION_END


def trading_day(at: datetime) -> date:
    return at.astimezone(MOSCOW).date()


def session_bucket(at: datetime) -> int:
    local = at.astimezone(MOSCOW)
    minutes = local.hour * 60 + local.minute
    start = REGULAR_SESSION_START.hour * 60 + REGULAR_SESSION_START.minute
    return max(0, min(4, (minutes - start) // 104))


def study_window(calendar_days: int, end_day: date | None = None) -> tuple[date, date]:
    if calendar_days < 2:
        raise ValueError("calendar_days must be at least 2")
    resolved_end = end_day or (datetime.now(MOSCOW).date() - timedelta(days=1))
    return resolved_end - timedelta(days=calendar_days - 1), resolved_end


def partition_path(cache_dir: Path, ticker: str, day: date) -> Path:
    return cache_dir / f"ticker={ticker}" / f"date={day.isoformat()}.parquet"


def manifest_path(cache_dir: Path) -> Path:
    return cache_dir / "manifest.json"


def candle_rows_for_storage(candles: Sequence[ResearchCandle]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row.ticker,
            "at": row.at.astimezone(UTC).isoformat(),
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.volume,
            "complete": row.complete,
        }
        for row in sorted(candles, key=lambda item: (item.ticker, item.at))
    ]


def candles_from_records(records: Iterable[Mapping[str, Any]]) -> tuple[ResearchCandle, ...]:
    candles: list[ResearchCandle] = []
    for row in records:
        raw_at = row["at"]
        at = raw_at if isinstance(raw_at, datetime) else datetime.fromisoformat(str(raw_at))
        if at.tzinfo is None or at.utcoffset() is None:
            at = at.replace(tzinfo=UTC)
        candles.append(
            ResearchCandle(
                ticker=str(row["ticker"]),
                at=at.astimezone(UTC),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                complete=bool(row.get("complete", True)),
            )
        )
    return tuple(candles)


def fingerprint_records(records: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(records, key=lambda item: (str(item.get("ticker")), str(item.get("at")))):
        digest.update(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode(
                "utf-8"
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


def build_cache_manifest(
    *,
    tickers: Sequence[str],
    start_day: date,
    end_day: date,
    row_counts: Mapping[str, int],
    content_fingerprint: str,
    failures: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "tinvest_research_candle_cache",
        "created_at": datetime.now(UTC).isoformat(),
        "script_version": "research-cache-v1.0.0",
        "scope": {
            "tickers": list(tickers),
            "from": start_day.isoformat(),
            "to": end_day.isoformat(),
            "interval": "1m",
            "source_type": "CANDLE_SOURCE_EXCHANGE",
        },
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
        "quality": {
            "partition_count": len(row_counts),
            "rows_by_partition": dict(sorted(row_counts.items())),
            "failed_partitions": list(failures),
        },
        "content_fingerprint": content_fingerprint,
    }


def write_csv_records(
    path: Path,
    records: Sequence[Mapping[str, Any]],
    *,
    fields: Sequence[str] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is not None:
        resolved_fields = list(fields)
    elif records:
        seen: list[str] = []
        for row in records:
            for field in row:
                if field not in seen:
                    seen.append(field)
        resolved_fields = seen
    else:
        resolved_fields = list(DATASET_FIELDS)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=resolved_fields)
        writer.writeheader()
        writer.writerows(records)


def require_duckdb() -> Any:
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is required for Parquet research files. Install with "
            "pip install -e '.[research]' or use a .csv output where supported."
        ) from exc
    return duckdb


def _sql_path_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def write_table(
    path: Path,
    records: Sequence[Mapping[str, Any]],
    *,
    fields: Sequence[str] | None = None,
) -> None:
    if path.suffix.lower() == ".csv":
        write_csv_records(path, records, fields=fields)
        return
    duckdb = require_duckdb()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_csv = path.with_suffix(path.suffix + ".tmp.csv")
    resolved_fields = list(fields or (records[0].keys() if records else DATASET_FIELDS))
    write_csv_records(temp_csv, records, fields=resolved_fields)
    con = duckdb.connect(database=":memory:")
    try:
        output = _sql_path_literal(path)
        if records:
            source = _sql_path_literal(temp_csv)
            con.execute(
                f"COPY (SELECT * FROM read_csv_auto('{source}')) TO '{output}' (FORMAT PARQUET)"
            )
        else:
            projection = ", ".join(
                f"CAST(NULL AS VARCHAR) AS \"{field}\"" for field in resolved_fields
            )
            con.execute(
                f"COPY (SELECT {projection} WHERE false) TO '{output}' (FORMAT PARQUET)"
            )
    finally:
        con.close()
        temp_csv.unlink(missing_ok=True)


def read_table(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    duckdb = require_duckdb()
    con = duckdb.connect(database=":memory:")
    try:
        rows = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        columns = [item[0] for item in con.description]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()


def read_cache(cache_dir: Path) -> tuple[ResearchCandle, ...]:
    files = sorted(cache_dir.glob("ticker=*/date=*.parquet"))
    if not files:
        raise RuntimeError(f"No candle partitions found in {cache_dir}")
    records: list[dict[str, Any]] = []
    for file in files:
        records.extend(read_table(file))
    return candles_from_records(records)


def valid_partition(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        read_table(path)
        return True
    except Exception:
        return False


def _return_bps(start: float, end: float) -> float:
    if start <= 0 or end <= 0:
        return 0.0
    return 10_000.0 * (end / start - 1.0)


def _mean_and_z_score(values: Iterable[float], current: float) -> tuple[float, float]:
    samples = tuple(float(item) for item in values)
    if not samples:
        return 0.0, 0.0
    mean = statistics.fmean(samples)
    sigma = statistics.pstdev(samples)
    if sigma <= 1e-12:
        return mean, 999.0 if current > mean else 0.0
    return mean, (current - mean) / sigma


def _family(signal_type: str) -> str:
    if signal_type in {
        "price_jump",
        "price_volume_range_combo_long",
        "price_volume_range_combo_short",
    }:
        return "directional"
    return "activity"


def replay_signals(
    candles: Sequence[ResearchCandle],
    policy: ReplayPolicy | None = None,
    *,
    max_signals_per_instrument: int = 10_000,
) -> tuple[SignalEvent, ...]:
    policy = policy or ReplayPolicy()
    grouped: dict[tuple[str, date], list[ResearchCandle]] = defaultdict(list)
    for candle in candles:
        if candle.complete and is_regular_session(candle.at):
            grouped[(candle.ticker, trading_day(candle.at))].append(candle)
    detected_by_type: Counter[tuple[str, str]] = Counter()
    signals: list[SignalEvent] = []
    for (_, _), rows in sorted(grouped.items()):
        rows.sort(key=lambda item: item.at)
        by_time = {row.at: row for row in rows}
        move_history: deque[float] = deque(maxlen=policy.detector_baseline_points)
        volume_history: deque[float] = deque(maxlen=policy.detector_baseline_points)
        range_history: deque[float] = deque(maxlen=policy.detector_baseline_points)
        return_history: deque[float] = deque(maxlen=policy.volatility_lookback_points)
        last_event_by_type: dict[str, datetime] = {}
        for index, candle in enumerate(rows):
            if index == 0:
                continue
            previous = rows[index - 1]
            if candle.at - previous.at == timedelta(minutes=1):
                return_history.append(_return_bps(previous.close, candle.close))
            window_start = by_time.get(candle.at - timedelta(minutes=policy.detector_window_minutes))
            candle_range_bps = _return_bps(candle.low, candle.high)
            if window_start is None:
                continue
            absolute_move = abs(_return_bps(window_start.close, candle.close))
            if len(move_history) < policy.detector_min_baseline_points:
                move_history.append(absolute_move)
                volume_history.append(candle.volume)
                range_history.append(candle_range_bps)
                continue
            signed_move = _return_bps(window_start.close, candle.close)
            baseline_move, z_score = _mean_and_z_score(move_history, absolute_move)
            baseline_volume, volume_z_score = _mean_and_z_score(volume_history, candle.volume)
            baseline_range, range_z_score = _mean_and_z_score(range_history, candle_range_bps)
            relative_excursion = (
                abs(absolute_move - baseline_move) / abs(baseline_move)
                if abs(baseline_move) >= 1e-12
                else math.inf
            )
            eligible = len(return_history) >= policy.volatility_min_points and signed_move != 0
            price_detected = (
                eligible
                and z_score >= policy.detector_z_score
                and relative_excursion >= policy.min_relative_metric_excursion
            )
            volume_detected = eligible and baseline_volume > 0 and volume_z_score >= policy.activity_z_score
            range_detected = eligible and baseline_range > 0 and range_z_score >= policy.activity_z_score
            direction = 1 if signed_move > 0 else -1
            candidates: list[tuple[str, int]] = []
            if price_detected:
                candidates.append(("price_jump", direction))
            if volume_detected:
                candidates.append(("volume_spike", 0))
            if range_detected:
                candidates.append(("candle_range_spike", 0))
            if price_detected and volume_z_score >= policy.combo_confirmation_z_score and range_z_score >= policy.combo_confirmation_z_score:
                candidates.append(
                    (
                        "price_volume_range_combo_long"
                        if direction > 0
                        else "price_volume_range_combo_short",
                        direction,
                    )
                )
            baseline_volatility = max(
                policy.volatility_floor_bps,
                statistics.pstdev(return_history) if len(return_history) > 1 else 0.0,
            )
            for signal_type, candidate_direction in candidates:
                cap_key = (candle.ticker, signal_type)
                if detected_by_type[cap_key] >= max_signals_per_instrument:
                    continue
                prior = last_event_by_type.get(signal_type)
                if prior is not None and candle.at - prior < timedelta(minutes=policy.cooldown_minutes):
                    continue
                last_event_by_type[signal_type] = candle.at
                detected_by_type[cap_key] += 1
                signals.append(
                    SignalEvent(
                        ticker=candle.ticker,
                        signal_type=signal_type,
                        family=_family(signal_type),
                        direction=candidate_direction,
                        source_event_at=candle.at,
                        trading_day=trading_day(candle.at),
                        session_bucket=session_bucket(candle.at),
                        event_move_bps=signed_move,
                        baseline_move_bps=baseline_move,
                        z_score=z_score,
                        volume_z_score=volume_z_score,
                        range_z_score=range_z_score,
                        candle_range_bps=candle_range_bps,
                        baseline_volatility_bps=baseline_volatility,
                        anchor_price=candle.close,
                    )
                )
            move_history.append(absolute_move)
            volume_history.append(candle.volume)
            range_history.append(candle_range_bps)
    return tuple(sorted(signals, key=lambda item: (item.ticker, item.source_event_at, item.signal_type)))


def _history_before(rows: Sequence[ResearchCandle], at: datetime, minutes: int) -> tuple[ResearchCandle, ...]:
    start = at - timedelta(minutes=minutes)
    return tuple(row for row in rows if start <= row.at < at)


def _pre_signal_features(
    rows: Sequence[ResearchCandle],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    max_observed_at: datetime | None = None
    for window in lookback_windows:
        history = _history_before(rows, signal.source_event_at, window)
        if history:
            max_observed_at = max(max_observed_at or history[-1].at, history[-1].at)
        prefix = f"{window}m"
        if len(history) < 2:
            result[f"pre_return_bps_{prefix}"] = ""
            result[f"pre_volatility_bps_{prefix}"] = ""
            result[f"pre_range_bps_{prefix}"] = ""
            result[f"pre_volume_change_{prefix}"] = ""
            continue
        returns = [
            _return_bps(previous.close, current.close)
            for previous, current in zip(history, history[1:])
            if previous.close > 0 and current.close > 0
        ]
        first_volume = history[0].volume
        result[f"pre_return_bps_{prefix}"] = _fmt(_return_bps(history[0].close, history[-1].close))
        result[f"pre_volatility_bps_{prefix}"] = _fmt(statistics.pstdev(returns) if len(returns) > 1 else 0.0)
        result[f"pre_range_bps_{prefix}"] = _fmt(statistics.fmean(_return_bps(row.low, row.high) for row in history))
        result[f"pre_volume_change_{prefix}"] = _fmt(
            (history[-1].volume / first_volume - 1.0) if first_volume > 0 else 0.0
        )
    result["feature_max_observed_at"] = "" if max_observed_at is None else max_observed_at.isoformat()
    result["feature_leakage_flag"] = bool(max_observed_at is not None and max_observed_at >= signal.source_event_at)
    return result


def _forward_path(
    rows: Sequence[ResearchCandle],
    signal: SignalEvent,
    horizon_seconds: int,
) -> tuple[str, tuple[ResearchCandle, ...]]:
    by_time = {row.at: row for row in rows}
    step_count = horizon_seconds // 60
    if horizon_seconds <= 0 or horizon_seconds % 60 != 0:
        return "invalid_horizon", ()
    path: list[ResearchCandle] = []
    expected = signal.source_event_at + timedelta(minutes=1)
    for _ in range(step_count):
        row = by_time.get(expected)
        if row is None or trading_day(row.at) != signal.trading_day or not is_regular_session(row.at):
            return "forward_price_unavailable_or_session_gap", ()
        path.append(row)
        expected += timedelta(minutes=1)
    return "ok", tuple(path)


def _triple_barrier_label(
    signal: SignalEvent,
    path: Sequence[ResearchCandle],
    policy: ReplayPolicy,
) -> str:
    if not path:
        return "unavailable"
    direction = signal.direction if signal.direction else 1
    for row in path:
        signed = direction * _return_bps(signal.anchor_price, row.close)
        if signed >= policy.triple_barrier_bps:
            return "take_profit"
        if signed <= -policy.triple_barrier_bps:
            return "stop_loss"
    return "timeout"


def _outcome_fields(
    signal: SignalEvent,
    horizon_seconds: int,
    rows: Sequence[ResearchCandle],
    policy: ReplayPolicy,
) -> dict[str, Any]:
    reason, path = _forward_path(rows, signal, horizon_seconds)
    if reason != "ok" or not path:
        return {
            "forward_available": False,
            "forward_reason_code": reason,
            "forward_return_bps": "",
            "direction_label": "unavailable",
            "cost_adjusted_directional_bps": "",
            "reverse_directional_bps": "",
            "triple_barrier_label": "unavailable",
            "meta_label": "",
        }
    forward_return = _return_bps(signal.anchor_price, path[-1].close)
    direction_label = (
        "up"
        if forward_return >= policy.materiality_bps
        else "down"
        if forward_return <= -policy.materiality_bps
        else "flat"
    )
    direction = signal.direction if signal.direction else 1
    directional = direction * forward_return - policy.round_trip_cost_bps
    reverse = -direction * forward_return - policy.round_trip_cost_bps
    return {
        "forward_available": True,
        "forward_reason_code": "",
        "forward_return_bps": _fmt(forward_return),
        "direction_label": direction_label,
        "cost_adjusted_directional_bps": _fmt(directional),
        "reverse_directional_bps": _fmt(reverse),
        "triple_barrier_label": _triple_barrier_label(signal, path, policy),
        "meta_label": int(signal.direction != 0 and directional >= policy.materiality_bps),
    }


def _combination_features(signals: Sequence[SignalEvent]) -> dict[tuple[str, datetime, str], dict[str, Any]]:
    grouped: dict[str, list[SignalEvent]] = defaultdict(list)
    for signal in signals:
        grouped[signal.ticker].append(signal)
    result: dict[tuple[str, datetime, str], dict[str, Any]] = {}
    for ticker, group in grouped.items():
        recent: deque[SignalEvent] = deque()
        for signal in sorted(group, key=lambda item: (item.source_event_at, item.signal_type)):
            while recent and signal.source_event_at - recent[0].source_event_at > timedelta(seconds=900):
                recent.popleft()
            last_60 = [item for item in recent if signal.source_event_at - item.source_event_at <= timedelta(seconds=60)]
            last_300 = [item for item in recent if signal.source_event_at - item.source_event_at <= timedelta(seconds=300)]
            types_300 = Counter(item.signal_type for item in last_300)
            combo_types = sorted({signal.signal_type, *(item.signal_type for item in last_300)})
            result[(ticker, signal.source_event_at, signal.signal_type)] = {
                "recent_signal_count_60s": len(last_60),
                "recent_signal_count_300s": len(last_300),
                "recent_signal_count_900s": len(recent),
                "recent_same_family_count_300s": sum(1 for item in last_300 if item.family == signal.family),
                "recent_price_jump_300s": types_300["price_jump"],
                "recent_volume_spike_300s": types_300["volume_spike"],
                "recent_candle_range_spike_300s": types_300["candle_range_spike"],
                "recent_directional_combo_300s": sum(1 for item in last_300 if item.family == "directional"),
                "combo_key_300s": "+".join(combo_types),
            }
            recent.append(signal)
    return result


def _quantile_rank(values: Mapping[Any, float]) -> dict[Any, float]:
    if not values:
        return {}
    ordered = sorted(values.items(), key=lambda item: item[1])
    if len(ordered) == 1:
        return {ordered[0][0]: 1.0}
    return {key: index / (len(ordered) - 1) for index, (key, _) in enumerate(ordered)}


def _day_volatility(candles: Sequence[ResearchCandle]) -> dict[tuple[str, date], float]:
    grouped: dict[tuple[str, date], list[ResearchCandle]] = defaultdict(list)
    for candle in candles:
        if candle.complete and is_regular_session(candle.at):
            grouped[(candle.ticker, trading_day(candle.at))].append(candle)
    result: dict[tuple[str, date], float] = {}
    for key, rows in grouped.items():
        rows.sort(key=lambda item: item.at)
        returns = [_return_bps(a.close, b.close) for a, b in zip(rows, rows[1:])]
        result[key] = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    return result


def build_signal_price_dataset(
    candles: Sequence[ResearchCandle],
    *,
    horizons_seconds: Sequence[int],
    lookback_windows: Sequence[int],
    policy: ReplayPolicy | None = None,
    max_signals_per_instrument: int = 10_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    policy = policy or ReplayPolicy()
    normalized_horizons = tuple(sorted({int(item) for item in horizons_seconds}))
    normalized_windows = tuple(sorted({int(item) for item in lookback_windows}))
    signals = replay_signals(candles, policy, max_signals_per_instrument=max_signals_per_instrument)
    by_ticker_day: dict[tuple[str, date], list[ResearchCandle]] = defaultdict(list)
    for candle in candles:
        by_ticker_day[(candle.ticker, trading_day(candle.at))].append(candle)
    for rows in by_ticker_day.values():
        rows.sort(key=lambda item: item.at)
    combinations = _combination_features(signals)
    day_vol = _day_volatility(candles)
    day_quantiles = _quantile_rank(day_vol)
    ticker_vol = {
        ticker: statistics.fmean(values)
        for ticker, values in _group_values((key[0], value) for key, value in day_vol.items()).items()
    }
    ticker_quantiles = _quantile_rank(ticker_vol)
    rows: list[dict[str, Any]] = []
    for signal in signals:
        candles_for_day = by_ticker_day[(signal.ticker, signal.trading_day)]
        pre_features = _pre_signal_features(candles_for_day, signal, normalized_windows)
        combo = combinations[(signal.ticker, signal.source_event_at, signal.signal_type)]
        base = {
            "ticker": signal.ticker,
            "signal_type": signal.signal_type,
            "family": signal.family,
            "direction": signal.direction,
            "source_event_at": signal.source_event_at.isoformat(),
            "trading_day": signal.trading_day.isoformat(),
            "session_bucket": signal.session_bucket,
            "event_move_bps": _fmt(signal.event_move_bps),
            "baseline_move_bps": _fmt(signal.baseline_move_bps),
            "z_score": _fmt(signal.z_score),
            "volume_z_score": _fmt(signal.volume_z_score),
            "range_z_score": _fmt(signal.range_z_score),
            "candle_range_bps": _fmt(signal.candle_range_bps),
            "baseline_volatility_bps": _fmt(signal.baseline_volatility_bps),
            "day_volatility_bps": _fmt(day_vol.get((signal.ticker, signal.trading_day), 0.0)),
            "day_volatility_quantile": _fmt(day_quantiles.get((signal.ticker, signal.trading_day), 0.0)),
            "ticker_volatility_quantile": _fmt(ticker_quantiles.get(signal.ticker, 0.0)),
            **combo,
            **pre_features,
        }
        for horizon in normalized_horizons:
            row = {
                "row_id": _row_id(signal, horizon),
                **base,
                "horizon_seconds": horizon,
                **_outcome_fields(signal, horizon, candles_for_day, policy),
            }
            rows.append(row)
    manifest = {
        "schema_version": 1,
        "kind": "signal_price_prediction_dataset",
        "created_at": datetime.now(UTC).isoformat(),
        "policy": asdict(policy),
        "horizons_seconds": list(normalized_horizons),
        "lookback_windows_minutes": list(normalized_windows),
        "quality": {
            "candles": len(candles),
            "signals": len(signals),
            "rows": len(rows),
            "signals_by_type": dict(sorted(Counter(signal.signal_type for signal in signals).items())),
            "feature_leakage_rows": sum(1 for row in rows if row["feature_leakage_flag"]),
            "unavailable_rows": sum(1 for row in rows if not row["forward_available"]),
        },
        "fingerprint": fingerprint_records(rows),
    }
    return rows, manifest


def _group_values(pairs: Iterable[tuple[Any, float]]) -> dict[Any, list[float]]:
    result: dict[Any, list[float]] = defaultdict(list)
    for key, value in pairs:
        result[key].append(value)
    return result


def _row_id(signal: SignalEvent, horizon_seconds: int) -> str:
    raw = "|".join(
        (
            signal.ticker,
            signal.signal_type,
            signal.source_event_at.isoformat(),
            str(horizon_seconds),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _fmt(value: Any) -> str:
    if value == "":
        return ""
    return f"{float(value):.8f}"


def chronological_split(rows: Sequence[Mapping[str, Any]], train_fraction: float = 0.70) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    days = sorted({str(row["trading_day"]) for row in rows})
    if len(days) < 2:
        return [dict(row) for row in rows], []
    cut = min(len(days) - 1, max(1, math.floor(len(days) * train_fraction)))
    train_days = set(days[:cut])
    train = [dict(row) for row in rows if str(row["trading_day"]) in train_days]
    validation = [dict(row) for row in rows if str(row["trading_day"]) not in train_days]
    return train, validation


def day_bootstrap_interval(
    rows: Sequence[Mapping[str, Any]],
    selector: Callable[[Mapping[str, Any]], float | None],
    *,
    samples: int = 1000,
    seed: int = 20260716,
) -> list[float] | None:
    values_by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = selector(row)
        if value is not None and math.isfinite(value):
            values_by_day[str(row["trading_day"])].append(value)
    days = sorted(values_by_day)
    if not days:
        return None
    day_means = {day: statistics.fmean(values_by_day[day]) for day in days}
    rng = random.Random(seed)
    estimates = []
    for _ in range(samples):
        sample_days = [rng.choice(days) for _ in days]
        estimates.append(statistics.fmean(day_means[day] for day in sample_days))
    estimates.sort()
    return [estimates[max(0, int(samples * 0.025) - 1)], estimates[min(samples - 1, int(samples * 0.975))]]


def float_or_none(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def event_study_summary(rows: Sequence[Mapping[str, Any]], *, split: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("forward_available")).lower() in {"true", "1"}:
            groups[(str(row["signal_type"]), int(row["horizon_seconds"]))].append(row)
    result: list[dict[str, Any]] = []
    for (signal_type, horizon), group in sorted(groups.items()):
        directional = [float_or_none(row.get("cost_adjusted_directional_bps")) for row in group]
        reverse = [float_or_none(row.get("reverse_directional_bps")) for row in group]
        directional_values = [item for item in directional if item is not None]
        reverse_values = [item for item in reverse if item is not None]
        result.append(
            {
                "model": "event_study_baseline",
                "split": split,
                "signal_type": signal_type,
                "horizon_seconds": horizon,
                "n": len(group),
                "sessions": len({row["trading_day"] for row in group}),
                "mean_cost_adjusted_directional_bps": _maybe_mean(directional_values),
                "mean_reverse_directional_bps": _maybe_mean(reverse_values),
                "directional_ci95_by_day": day_bootstrap_interval(
                    group,
                    lambda row: float_or_none(row.get("cost_adjusted_directional_bps")),
                    samples=500,
                    seed=17,
                ),
                "inverse_candidate": (
                    _maybe_mean(reverse_values) is not None
                    and _maybe_mean(directional_values) is not None
                    and _maybe_mean(reverse_values) > _maybe_mean(directional_values)
                    and _maybe_mean(reverse_values) > 0
                ),
            }
        )
    return result


def bayesian_score_summary(rows: Sequence[Mapping[str, Any]], *, split: str) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if str(row.get("meta_label")) in {"0", "1"}:
            volatility_bucket = _bucket(float_or_none(row.get("day_volatility_quantile")))
            key = (
                str(row["signal_type"]),
                int(row["horizon_seconds"]),
                str(row["session_bucket"]),
                volatility_bucket,
            )
            groups[key].append(row)
    result: list[dict[str, Any]] = []
    for (signal_type, horizon, session, volatility_bucket), group in sorted(groups.items()):
        wins = sum(1 for row in group if str(row.get("meta_label")) == "1")
        losses = sum(1 for row in group if str(row.get("meta_label")) == "0")
        alpha = 1 + wins
        beta = 1 + losses
        result.append(
            {
                "model": "bayesian_score",
                "split": split,
                "signal_type": signal_type,
                "horizon_seconds": horizon,
                "session_bucket": session,
                "volatility_bucket": volatility_bucket,
                "n": wins + losses,
                "posterior_mean": alpha / (alpha + beta),
                "wins": wins,
                "losses": losses,
            }
        )
    return result


def _bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.33:
        return "low"
    if value < 0.66:
        return "mid"
    return "high"


def _maybe_mean(values: Sequence[float]) -> float | None:
    return statistics.fmean(values) if values else None


def dataset_feature_columns(rows: Sequence[Mapping[str, Any]]) -> tuple[list[str], list[str]]:
    numeric = [
        "horizon_seconds",
        "session_bucket",
        "event_move_bps",
        "baseline_move_bps",
        "z_score",
        "volume_z_score",
        "range_z_score",
        "candle_range_bps",
        "baseline_volatility_bps",
        "day_volatility_bps",
        "day_volatility_quantile",
        "ticker_volatility_quantile",
        "recent_signal_count_60s",
        "recent_signal_count_300s",
        "recent_signal_count_900s",
        "recent_same_family_count_300s",
        "recent_price_jump_300s",
        "recent_volume_spike_300s",
        "recent_candle_range_spike_300s",
        "recent_directional_combo_300s",
        "pre_return_bps_5m",
        "pre_volatility_bps_5m",
        "pre_range_bps_5m",
        "pre_volume_change_5m",
        "pre_return_bps_15m",
        "pre_volatility_bps_15m",
        "pre_range_bps_15m",
        "pre_volume_change_15m",
        "pre_return_bps_30m",
        "pre_volatility_bps_30m",
        "pre_range_bps_30m",
        "pre_volume_change_30m",
        "pre_return_bps_60m",
        "pre_volatility_bps_60m",
        "pre_range_bps_60m",
        "pre_volume_change_60m",
    ]
    categorical = ["ticker", "signal_type", "family", "direction", "combo_key_300s"]
    existing = set(rows[0]) if rows else set()
    return [item for item in numeric if item in existing], [item for item in categorical if item in existing]


def render_markdown_report(results: Mapping[str, Any]) -> str:
    leaderboard = results.get("leaderboard", [])
    accepted = [item for item in leaderboard if item.get("accepted")]
    inverse = [item for item in results.get("event_study", []) if item.get("inverse_candidate")]
    lines = [
        "# Signal-triggered price prediction research",
        "",
        f"- Dataset rows: {results.get('dataset_rows', 0)}",
        f"- Train rows: {results.get('train_rows', 0)}; validation rows: {results.get('validation_rows', 0)}",
        f"- Validation sessions: {results.get('validation_sessions', 0)}",
        "",
        "## Directional candidates",
        "",
    ]
    if not accepted:
        lines.append("No model/slice passed the first research acceptance gate.")
    else:
        lines.append("| Model | Signal | Horizon | n | Score |")
        lines.append("|---|---|---:|---:|---:|")
        for row in accepted[:20]:
            lines.append(
                f"| {row.get('model')} | {row.get('signal_type', 'all')} | "
                f"{row.get('horizon_seconds', '')} | {row.get('n', '')} | "
                f"{row.get('score', '')} |"
            )
    lines.extend(["", "## Inverse hypothesis candidates", ""])
    if not inverse:
        lines.append("No stable inverse candidate cleared the exploratory screen.")
    else:
        for row in inverse[:20]:
            lines.append(
                f"- {row['signal_type']} @ {row['horizon_seconds']}s: "
                f"reverse mean {row['mean_reverse_directional_bps']:.3f} bps vs "
                f"direct {row['mean_cost_adjusted_directional_bps']:.3f} bps."
            )
    lines.extend(
        [
            "",
            "## Product transfer note",
            "",
            "Treat every passing candidate as shadow/admin-only until an independent "
            "holdout and production tick/L2 outcomes confirm the same effect.",
            "",
        ]
    )
    return "\n".join(lines)


def write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
