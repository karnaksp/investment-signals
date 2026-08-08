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
from bisect import bisect_left
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
)


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

MARKET_CONTEXT_TICKERS = ("IMOEX", "RVI", "XAU", "BRENT", "CNYRUB")
TECHNICAL_FEATURE_NAMES = (
    "pre_rsi",
    "pre_macd_bps",
    "pre_bollinger_z",
    "pre_atr_bps",
    "pre_volume_z",
    "pre_price_position",
)
TECHNICAL_FEATURE_WINDOWS = (5, 15, 30, 60)


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
    volume_buy: float = 0.0
    volume_sell: float = 0.0


@dataclass(frozen=True, slots=True)
class ResearchOrderBookSnapshot:
    ticker: str
    at: datetime
    depth: int
    best_bid: float
    best_ask: float
    mid: float
    spread_bps: float
    bid_qty: float
    ask_qty: float
    total_qty: float
    imbalance_ratio: float
    imbalance_abs: float
    is_consistent: bool = True


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
    event_volume: float = 0.0
    event_volume_buy: float = 0.0
    event_volume_sell: float = 0.0
    event_aggressor_imbalance: float = 0.0
    event_classified_volume_share: float = 0.0
    baseline_volume: float = 0.0
    event_volume_ratio: float = 0.0
    event_range_ratio: float = 0.0
    event_strength_to_volatility: float = 0.0
    candle_close_position: float = 0.5
    event_body_bps: float = 0.0
    event_upper_wick_bps: float = 0.0
    event_lower_wick_bps: float = 0.0
    event_body_to_range: float = 0.0
    event_upper_wick_to_range: float = 0.0
    event_lower_wick_to_range: float = 0.0
    event_close_to_direction: float = 0.5
    event_reversal_pressure: float = 0.0


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
    "event_volume",
    "event_volume_buy",
    "event_volume_sell",
    "event_aggressor_imbalance",
    "event_aggressor_alignment",
    "event_classified_volume_share",
    "baseline_volume",
    "event_volume_ratio",
    "event_range_ratio",
    "event_strength_to_volatility",
    "candle_close_position",
    "event_body_bps",
    "event_upper_wick_bps",
    "event_lower_wick_bps",
    "event_body_to_range",
    "event_upper_wick_to_range",
    "event_lower_wick_to_range",
    "event_close_to_direction",
    "event_reversal_pressure",
    "orderbook_available",
    "orderbook_age_seconds",
    "orderbook_depth",
    "orderbook_spread_bps",
    "orderbook_bid_qty",
    "orderbook_ask_qty",
    "orderbook_total_qty",
    "orderbook_imbalance_ratio",
    "orderbook_imbalance_abs",
    "orderbook_is_consistent",
    "native_signal_available",
    "native_signal_active_count",
    "native_signal_strategy_count",
    "native_signal_technical_count",
    "native_signal_fundamental_count",
    "native_signal_buy_count",
    "native_signal_sell_count",
    "native_signal_probability_max",
    "native_signal_probability_mean",
    "native_signal_direction_score",
    "native_signal_consensus_direction",
    "native_signal_detector_alignment",
    "day_volatility_bps",
    "day_volatility_quantile",
    "ticker_volatility_quantile",
    "day_volume_quantile",
    "ticker_volume_quantile",
    "ticker_mean_daily_volume",
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
    *(f"context_{ticker.lower()}_return_bps_{window}m" for ticker in MARKET_CONTEXT_TICKERS for window in (5, 15, 30, 60)),
    *(f"{name}_{window}m" for name in TECHNICAL_FEATURE_NAMES for window in TECHNICAL_FEATURE_WINDOWS),
    "pre_return_bps_5m",
    "pre_abs_return_bps_5m",
    "pre_directional_return_bps_5m",
    "pre_volatility_bps_5m",
    "pre_return_to_volatility_5m",
    "event_to_pre_volatility_5m",
    "pre_range_bps_5m",
    "event_to_pre_range_5m",
    "pre_volume_change_5m",
    "pre_aggressor_imbalance_5m",
    "pre_aggressor_alignment_5m",
    "aggressor_imbalance_shift_5m",
    "pre_classified_volume_share_5m",
    "pre_consolidation_score_5m",
    "market_return_bps_5m",
    "market_abs_return_bps_5m",
    "market_volatility_bps_5m",
    "signal_vs_market_bps_5m",
    "signal_directional_vs_market_bps_5m",
    "signal_market_alignment_bps_5m",
    "pre_return_bps_15m",
    "pre_abs_return_bps_15m",
    "pre_directional_return_bps_15m",
    "pre_volatility_bps_15m",
    "pre_return_to_volatility_15m",
    "event_to_pre_volatility_15m",
    "pre_range_bps_15m",
    "event_to_pre_range_15m",
    "pre_volume_change_15m",
    "pre_aggressor_imbalance_15m",
    "pre_aggressor_alignment_15m",
    "aggressor_imbalance_shift_15m",
    "pre_classified_volume_share_15m",
    "pre_consolidation_score_15m",
    "market_return_bps_15m",
    "market_abs_return_bps_15m",
    "market_volatility_bps_15m",
    "signal_vs_market_bps_15m",
    "signal_directional_vs_market_bps_15m",
    "signal_market_alignment_bps_15m",
    "pre_return_bps_30m",
    "pre_abs_return_bps_30m",
    "pre_directional_return_bps_30m",
    "pre_volatility_bps_30m",
    "pre_return_to_volatility_30m",
    "event_to_pre_volatility_30m",
    "pre_range_bps_30m",
    "event_to_pre_range_30m",
    "pre_volume_change_30m",
    "pre_aggressor_imbalance_30m",
    "pre_aggressor_alignment_30m",
    "aggressor_imbalance_shift_30m",
    "pre_classified_volume_share_30m",
    "pre_consolidation_score_30m",
    "market_return_bps_30m",
    "market_abs_return_bps_30m",
    "market_volatility_bps_30m",
    "signal_vs_market_bps_30m",
    "signal_directional_vs_market_bps_30m",
    "signal_market_alignment_bps_30m",
    "pre_return_bps_60m",
    "pre_abs_return_bps_60m",
    "pre_directional_return_bps_60m",
    "pre_volatility_bps_60m",
    "pre_return_to_volatility_60m",
    "event_to_pre_volatility_60m",
    "pre_range_bps_60m",
    "event_to_pre_range_60m",
    "pre_volume_change_60m",
    "pre_aggressor_imbalance_60m",
    "pre_aggressor_alignment_60m",
    "aggressor_imbalance_shift_60m",
    "pre_classified_volume_share_60m",
    "pre_consolidation_score_60m",
    "market_return_bps_60m",
    "market_abs_return_bps_60m",
    "market_volatility_bps_60m",
    "signal_vs_market_bps_60m",
    "signal_directional_vs_market_bps_60m",
    "signal_market_alignment_bps_60m",
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
    "volume_buy",
    "volume_sell",
    "complete",
)

ORDERBOOK_CACHE_FIELDS = (
    "ticker",
    "at",
    "depth",
    "best_bid",
    "best_ask",
    "mid",
    "spread_bps",
    "bid_qty",
    "ask_qty",
    "total_qty",
    "imbalance_ratio",
    "imbalance_abs",
    "is_consistent",
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
    """Return whether the versioned research schedule admits this candle.

    The public name is kept for compatibility with existing research commands.
    Unlike the legacy 10:05 cutoff, the schedule includes the 07:00-09:49
    morning phase and explicitly excludes the 09:50-09:59 transition.
    """

    return MOEX_EQUITY_PHASE_SCHEDULE_V1.is_signal_eligible(at)


def trading_day(at: datetime) -> date:
    return at.astimezone(MOSCOW).date()


def session_bucket(at: datetime) -> int:
    return MOEX_EQUITY_PHASE_SCHEDULE_V1.research_bucket(at)


def study_window(calendar_days: int, end_day: date | None = None) -> tuple[date, date]:
    if calendar_days < 2:
        raise ValueError("calendar_days must be at least 2")
    resolved_end = end_day or (datetime.now(MOSCOW).date() - timedelta(days=1))
    return resolved_end - timedelta(days=calendar_days - 1), resolved_end


def partition_path(cache_dir: Path, ticker: str, day: date) -> Path:
    return cache_dir / f"ticker={ticker}" / f"date={day.isoformat()}.parquet"


def orderbook_partition_path(cache_dir: Path, ticker: str, day: date) -> Path:
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
            "volume_buy": row.volume_buy,
            "volume_sell": row.volume_sell,
            "complete": row.complete,
        }
        for row in sorted(candles, key=lambda item: (item.ticker, item.at))
    ]


def _level_price(level: Mapping[str, Any]) -> float:
    raw = level.get("price")
    if isinstance(raw, Mapping):
        return quotation(raw)
    return float(raw or 0.0)


def _level_quantity(level: Mapping[str, Any]) -> float:
    raw = level.get("quantity", level.get("qty", 0))
    if isinstance(raw, Mapping):
        return quotation(raw)
    return float(raw or 0.0)


def orderbook_snapshot_from_levels(
    *,
    ticker: str,
    at: datetime,
    bids: Sequence[Mapping[str, Any]],
    asks: Sequence[Mapping[str, Any]],
    depth: int,
    is_consistent: bool = True,
) -> ResearchOrderBookSnapshot | None:
    resolved_depth = max(1, int(depth))
    bid_levels = list(bids[:resolved_depth])
    ask_levels = list(asks[:resolved_depth])
    if not bid_levels or not ask_levels:
        return None
    best_bid = _level_price(bid_levels[0])
    best_ask = _level_price(ask_levels[0])
    if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
        return None
    bid_qty = sum(_level_quantity(level) for level in bid_levels)
    ask_qty = sum(_level_quantity(level) for level in ask_levels)
    total_qty = bid_qty + ask_qty
    if total_qty <= 0:
        return None
    mid = (best_bid + best_ask) / 2.0
    spread_bps = ((best_ask - best_bid) / mid) * 10_000.0 if mid > 0 else 0.0
    imbalance_ratio = bid_qty / total_qty
    return ResearchOrderBookSnapshot(
        ticker=ticker,
        at=at.astimezone(UTC),
        depth=resolved_depth,
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        spread_bps=spread_bps,
        bid_qty=bid_qty,
        ask_qty=ask_qty,
        total_qty=total_qty,
        imbalance_ratio=imbalance_ratio,
        imbalance_abs=abs(imbalance_ratio - 0.5) * 2.0,
        is_consistent=bool(is_consistent),
    )


def orderbook_rows_for_storage(
    snapshots: Sequence[ResearchOrderBookSnapshot],
) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row.ticker,
            "at": row.at.astimezone(UTC).isoformat(),
            "depth": row.depth,
            "best_bid": row.best_bid,
            "best_ask": row.best_ask,
            "mid": row.mid,
            "spread_bps": row.spread_bps,
            "bid_qty": row.bid_qty,
            "ask_qty": row.ask_qty,
            "total_qty": row.total_qty,
            "imbalance_ratio": row.imbalance_ratio,
            "imbalance_abs": row.imbalance_abs,
            "is_consistent": row.is_consistent,
        }
        for row in sorted(snapshots, key=lambda item: (item.ticker, item.at, item.depth))
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
                volume_buy=float(row.get("volume_buy", 0) or 0),
                volume_sell=float(row.get("volume_sell", 0) or 0),
                complete=bool(row.get("complete", True)),
            )
        )
    return tuple(candles)


def orderbook_snapshots_from_records(
    records: Iterable[Mapping[str, Any]],
) -> tuple[ResearchOrderBookSnapshot, ...]:
    snapshots: list[ResearchOrderBookSnapshot] = []
    for row in records:
        raw_at = row["at"]
        at = raw_at if isinstance(raw_at, datetime) else datetime.fromisoformat(str(raw_at).replace("Z", "+00:00"))
        if at.tzinfo is None or at.utcoffset() is None:
            at = at.replace(tzinfo=UTC)
        snapshots.append(
            ResearchOrderBookSnapshot(
                ticker=str(row["ticker"]),
                at=at.astimezone(UTC),
                depth=int(float(row.get("depth", 0) or 0)),
                best_bid=float(row.get("best_bid", 0) or 0),
                best_ask=float(row.get("best_ask", 0) or 0),
                mid=float(row.get("mid", 0) or 0),
                spread_bps=float(row.get("spread_bps", 0) or 0),
                bid_qty=float(row.get("bid_qty", 0) or 0),
                ask_qty=float(row.get("ask_qty", 0) or 0),
                total_qty=float(row.get("total_qty", 0) or 0),
                imbalance_ratio=float(row.get("imbalance_ratio", 0) or 0),
                imbalance_abs=float(row.get("imbalance_abs", 0) or 0),
                is_consistent=str(row.get("is_consistent", True)).lower() in {"1", "true", "yes"},
            )
        )
    return tuple(sorted(snapshots, key=lambda item: (item.ticker, item.at, item.depth)))


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
            "aggressor_volume_fields": ["volume_buy", "volume_sell"],
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


def build_orderbook_cache_manifest(
    *,
    tickers: Sequence[str],
    start_at: datetime,
    end_at: datetime,
    depth: int,
    row_counts: Mapping[str, int],
    content_fingerprint: str,
    failures: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "tinvest_research_orderbook_snapshot_cache",
        "created_at": datetime.now(UTC).isoformat(),
        "script_version": "research-orderbook-cache-v1.0.0",
        "scope": {
            "tickers": list(tickers),
            "from": start_at.astimezone(UTC).isoformat(),
            "to": end_at.astimezone(UTC).isoformat(),
            "depth": int(depth),
            "source_type": "MarketDataService/GetOrderBook",
        },
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
        "quality": {
            "partition_count": len(row_counts),
            "rows_by_partition": dict(sorted(row_counts.items())),
            "failed_snapshots": list(failures),
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


def read_native_signal_cache(
    cache_dir: Path,
    *,
    tickers: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Read the locally cached public T-Invest signal archive."""

    path = cache_dir / "signals.parquet"
    if not path.is_file():
        return []
    rows = read_table(path)
    if not tickers:
        return rows
    allowed = {str(item).upper() for item in tickers}
    return [row for row in rows if str(row.get("ticker", "")).upper() in allowed]


def _read_parquet_records(files: Sequence[Path]) -> list[dict[str, Any]]:
    if not files:
        return []
    duckdb = require_duckdb()
    con = duckdb.connect(database=":memory:")
    try:
        rows = con.execute("SELECT * FROM read_parquet(?)", [[str(file) for file in files]]).fetchall()
        columns = [item[0] for item in con.description]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        con.close()


def _read_parquet_candles(files: Sequence[Path]) -> tuple[ResearchCandle, ...]:
    if not files:
        return ()
    duckdb = require_duckdb()
    con = duckdb.connect(database=":memory:")
    result: list[ResearchCandle] = []
    try:
        cursor = con.execute(
            "SELECT * FROM read_parquet(?, union_by_name = true)",
            [[str(file) for file in files]],
        )
        columns = [item[0] for item in cursor.description]
        positions = {name: columns.index(name) for name in columns}
        while rows := cursor.fetchmany(100_000):
            for row in rows:
                raw_at = row[positions["at"]]
                at = (
                    raw_at
                    if isinstance(raw_at, datetime)
                    else datetime.fromisoformat(str(raw_at))
                )
                if at.tzinfo is None or at.utcoffset() is None:
                    at = at.replace(tzinfo=UTC)
                result.append(
                    ResearchCandle(
                        ticker=str(row[positions["ticker"]]),
                        at=at.astimezone(UTC),
                        open=float(row[positions["open"]]),
                        high=float(row[positions["high"]]),
                        low=float(row[positions["low"]]),
                        close=float(row[positions["close"]]),
                        volume=float(row[positions["volume"]]),
                        complete=bool(row[positions["complete"]]),
                        volume_buy=(
                            float(row[positions["volume_buy"]] or 0.0)
                            if "volume_buy" in positions
                            else 0.0
                        ),
                        volume_sell=(
                            float(row[positions["volume_sell"]] or 0.0)
                            if "volume_sell" in positions
                            else 0.0
                        ),
                    )
                )
        return tuple(result)
    finally:
        con.close()


def _cache_partition_files(cache_dir: Path, tickers: Sequence[str] | None = None) -> list[Path]:
    if tickers:
        return [
            file
            for ticker in sorted({item.upper() for item in tickers})
            for file in sorted((cache_dir / f"ticker={ticker}").glob("date=*.parquet"))
        ]
    return sorted(cache_dir.glob("ticker=*/date=*.parquet"))


def read_cache(
    cache_dir: Path,
    tickers: Sequence[str] | None = None,
    *,
    start_day: date | None = None,
    end_day: date | None = None,
) -> tuple[ResearchCandle, ...]:
    files = _cache_partition_files(cache_dir, tickers)
    if start_day is not None or end_day is not None:
        files = [
            file
            for file in files
            if (
                start_day is None
                or date.fromisoformat(file.stem.removeprefix("date="))
                >= start_day
            )
            and (
                end_day is None
                or date.fromisoformat(file.stem.removeprefix("date="))
                <= end_day
            )
        ]
    if not files:
        raise RuntimeError(f"No candle partitions found in {cache_dir}")
    return _read_parquet_candles(files)


def read_orderbook_cache(cache_dir: Path, tickers: Sequence[str] | None = None) -> tuple[ResearchOrderBookSnapshot, ...]:
    files = _cache_partition_files(cache_dir, tickers)
    if not files:
        raise RuntimeError(f"No order book partitions found in {cache_dir}")
    records = _read_parquet_records(files)
    return orderbook_snapshots_from_records(records)


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
    mean = sum(samples) / len(samples)
    variance = sum((item - mean) ** 2 for item in samples) / len(samples)
    sigma = math.sqrt(variance)
    if sigma <= 1e-12:
        return mean, 999.0 if current > mean else 0.0
    return mean, (current - mean) / sigma


def _pstdev_fast(values: Iterable[float]) -> float:
    samples = tuple(float(item) for item in values)
    if len(samples) <= 1:
        return 0.0
    mean = sum(samples) / len(samples)
    return math.sqrt(sum((item - mean) ** 2 for item in samples) / len(samples))


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
                _pstdev_fast(return_history),
            )
            event_volume_ratio = candle.volume / baseline_volume if baseline_volume > 0 else 0.0
            classified_volume = candle.volume_buy + candle.volume_sell
            event_aggressor_imbalance = (
                (candle.volume_buy - candle.volume_sell) / classified_volume
                if classified_volume > 0
                else 0.0
            )
            event_classified_volume_share = (
                min(1.0, classified_volume / candle.volume) if candle.volume > 0 else 0.0
            )
            event_range_ratio = candle_range_bps / baseline_range if baseline_range > 0 else 0.0
            event_strength_to_volatility = abs(signed_move) / baseline_volatility if baseline_volatility > 0 else 0.0
            candle_span = candle.high - candle.low
            candle_close_position = (
                (candle.close - candle.low) / candle_span if candle_span > 0 else 0.5
            )
            candle_close_position = min(1.0, max(0.0, candle_close_position))
            event_body = abs(candle.close - candle.open)
            event_upper_wick = max(0.0, candle.high - max(candle.open, candle.close))
            event_lower_wick = max(0.0, min(candle.open, candle.close) - candle.low)
            price_base = abs(candle.close) if abs(candle.close) > 1e-12 else 0.0
            event_body_bps = event_body / price_base * 10_000 if price_base else 0.0
            event_upper_wick_bps = event_upper_wick / price_base * 10_000 if price_base else 0.0
            event_lower_wick_bps = event_lower_wick / price_base * 10_000 if price_base else 0.0
            event_body_to_range = event_body / candle_span if candle_span > 0 else 0.0
            event_upper_wick_to_range = event_upper_wick / candle_span if candle_span > 0 else 0.0
            event_lower_wick_to_range = event_lower_wick / candle_span if candle_span > 0 else 0.0
            for signal_type, candidate_direction in candidates:
                if candidate_direction > 0:
                    event_close_to_direction = candle_close_position
                    event_reversal_pressure = event_upper_wick_to_range + (1.0 - candle_close_position)
                elif candidate_direction < 0:
                    event_close_to_direction = 1.0 - candle_close_position
                    event_reversal_pressure = event_lower_wick_to_range + candle_close_position
                else:
                    event_close_to_direction = 0.5
                    event_reversal_pressure = max(event_upper_wick_to_range, event_lower_wick_to_range)
                event_reversal_pressure = min(1.0, max(0.0, event_reversal_pressure))
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
                        event_volume=candle.volume,
                        event_volume_buy=candle.volume_buy,
                        event_volume_sell=candle.volume_sell,
                        event_aggressor_imbalance=event_aggressor_imbalance,
                        event_classified_volume_share=event_classified_volume_share,
                        baseline_volume=baseline_volume,
                        event_volume_ratio=event_volume_ratio,
                        event_range_ratio=event_range_ratio,
                        event_strength_to_volatility=event_strength_to_volatility,
                        candle_close_position=candle_close_position,
                        event_body_bps=event_body_bps,
                        event_upper_wick_bps=event_upper_wick_bps,
                        event_lower_wick_bps=event_lower_wick_bps,
                        event_body_to_range=event_body_to_range,
                        event_upper_wick_to_range=event_upper_wick_to_range,
                        event_lower_wick_to_range=event_lower_wick_to_range,
                        event_close_to_direction=event_close_to_direction,
                        event_reversal_pressure=event_reversal_pressure,
                    )
                )
            move_history.append(absolute_move)
            volume_history.append(candle.volume)
            range_history.append(candle_range_bps)
    return tuple(sorted(signals, key=lambda item: (item.ticker, item.source_event_at, item.signal_type)))


def _history_before(rows: Sequence[ResearchCandle], at: datetime, minutes: int) -> tuple[ResearchCandle, ...]:
    start = at - timedelta(minutes=minutes)
    return tuple(row for row in rows if start <= row.at < at)


def _ema(values: Sequence[float], span: int) -> float | None:
    if not values:
        return None
    alpha = 2.0 / (span + 1.0)
    value = float(values[0])
    for item in values[1:]:
        value = alpha * float(item) + (1.0 - alpha) * value
    return value


def _technical_history_features(history: Sequence[ResearchCandle]) -> dict[str, float | str]:
    """Calculate candle indicators from observations strictly before an event."""

    if len(history) < 2:
        return {name: "" for name in TECHNICAL_FEATURE_NAMES}
    closes = [float(row.close) for row in history]
    returns = [current - previous for previous, current in zip(closes, closes[1:])]
    recent_returns = returns[-14:]
    gains = [max(0.0, item) for item in recent_returns]
    losses = [max(0.0, -item) for item in recent_returns]
    average_gain = statistics.fmean(gains) if gains else 0.0
    average_loss = statistics.fmean(losses) if losses else 0.0
    if average_loss <= 0:
        rsi = 100.0 if average_gain > 0 else 50.0
    else:
        relative_strength = average_gain / average_loss
        rsi = 100.0 - 100.0 / (1.0 + relative_strength)

    last_close = closes[-1]
    ema_fast = _ema(closes[-12:], 12)
    ema_slow = _ema(closes[-26:], 26)
    macd_bps = (
        (ema_fast - ema_slow) / last_close * 10_000
        if ema_fast is not None and ema_slow is not None and last_close > 0
        else 0.0
    )
    bollinger_window = closes[-20:]
    bollinger_mean = statistics.fmean(bollinger_window)
    bollinger_std = statistics.pstdev(bollinger_window) if len(bollinger_window) > 1 else 0.0
    bollinger_z = (last_close - bollinger_mean) / bollinger_std if bollinger_std > 0 else 0.0

    atr_rows = history[-14:]
    true_ranges: list[float] = []
    previous_close: float | None = None
    for row in atr_rows:
        candidates = [float(row.high) - float(row.low)]
        if previous_close is not None:
            candidates.extend(
                [abs(float(row.high) - previous_close), abs(float(row.low) - previous_close)]
            )
        true_ranges.append(max(candidates))
        previous_close = float(row.close)
    atr_bps = statistics.fmean(true_ranges) / last_close * 10_000 if last_close > 0 else 0.0

    prior_volumes = [float(row.volume) for row in history[-21:-1]]
    prior_volume_mean = statistics.fmean(prior_volumes) if prior_volumes else 0.0
    prior_volume_std = statistics.pstdev(prior_volumes) if len(prior_volumes) > 1 else 0.0
    volume_z = (
        (float(history[-1].volume) - prior_volume_mean) / prior_volume_std
        if prior_volume_std > 0
        else 0.0
    )
    recent = history[-20:]
    recent_low = min(float(row.low) for row in recent)
    recent_high = max(float(row.high) for row in recent)
    price_position = (
        (last_close - recent_low) / (recent_high - recent_low)
        if recent_high > recent_low
        else 0.5
    )
    return {
        "pre_rsi": rsi,
        "pre_macd_bps": macd_bps,
        "pre_bollinger_z": bollinger_z,
        "pre_atr_bps": atr_bps,
        "pre_volume_z": volume_z,
        "pre_price_position": min(1.0, max(0.0, price_position)),
    }


def _pre_signal_features(
    rows: Sequence[ResearchCandle],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    max_observed_at: datetime | None = None
    ordered_rows = tuple(rows)
    times = [row.at for row in ordered_rows]
    for window in lookback_windows:
        start = signal.source_event_at - timedelta(minutes=window)
        start_index = bisect_left(times, start)
        end_index = bisect_left(times, signal.source_event_at)
        history = ordered_rows[start_index:end_index]
        if history:
            max_observed_at = max(max_observed_at or history[-1].at, history[-1].at)
        prefix = f"{window}m"
        if len(history) < 2:
            result[f"pre_return_bps_{prefix}"] = ""
            result[f"pre_abs_return_bps_{prefix}"] = ""
            result[f"pre_directional_return_bps_{prefix}"] = ""
            result[f"pre_volatility_bps_{prefix}"] = ""
            result[f"pre_return_to_volatility_{prefix}"] = ""
            result[f"event_to_pre_volatility_{prefix}"] = ""
            result[f"pre_range_bps_{prefix}"] = ""
            result[f"event_to_pre_range_{prefix}"] = ""
            result[f"pre_volume_change_{prefix}"] = ""
            result[f"pre_aggressor_imbalance_{prefix}"] = ""
            result[f"pre_aggressor_alignment_{prefix}"] = ""
            result[f"aggressor_imbalance_shift_{prefix}"] = ""
            result[f"pre_classified_volume_share_{prefix}"] = ""
            result[f"pre_consolidation_score_{prefix}"] = ""
            for name in TECHNICAL_FEATURE_NAMES:
                result[f"{name}_{prefix}"] = ""
            continue
        returns = [
            _return_bps(previous.close, current.close)
            for previous, current in zip(history, history[1:])
            if previous.close > 0 and current.close > 0
        ]
        first_volume = history[0].volume
        buy_volume = sum(row.volume_buy for row in history)
        sell_volume = sum(row.volume_sell for row in history)
        classified_volume = buy_volume + sell_volume
        total_volume = sum(row.volume for row in history)
        pre_return = _return_bps(history[0].close, history[-1].close)
        pre_volatility = statistics.pstdev(returns) if len(returns) > 1 else 0.0
        pre_range = statistics.fmean(_return_bps(row.low, row.high) for row in history)
        abs_return = abs(pre_return)
        direction = signal.direction if signal.direction else 0
        result[f"pre_return_bps_{prefix}"] = _fmt(pre_return)
        result[f"pre_abs_return_bps_{prefix}"] = _fmt(abs_return)
        result[f"pre_directional_return_bps_{prefix}"] = _fmt(direction * pre_return)
        result[f"pre_volatility_bps_{prefix}"] = _fmt(pre_volatility)
        result[f"pre_return_to_volatility_{prefix}"] = _fmt(abs_return / pre_volatility if pre_volatility > 0 else 0.0)
        result[f"event_to_pre_volatility_{prefix}"] = _fmt(abs(signal.event_move_bps) / pre_volatility if pre_volatility > 0 else 0.0)
        result[f"pre_range_bps_{prefix}"] = _fmt(pre_range)
        result[f"event_to_pre_range_{prefix}"] = _fmt(abs(signal.event_move_bps) / pre_range if pre_range > 0 else 0.0)
        result[f"pre_volume_change_{prefix}"] = _fmt(
            (history[-1].volume / first_volume - 1.0) if first_volume > 0 else 0.0
        )
        pre_aggressor_imbalance = (
            (buy_volume - sell_volume) / classified_volume if classified_volume > 0 else 0.0
        )
        result[f"pre_aggressor_imbalance_{prefix}"] = _fmt(pre_aggressor_imbalance)
        result[f"pre_aggressor_alignment_{prefix}"] = _fmt(signal.direction * pre_aggressor_imbalance)
        result[f"aggressor_imbalance_shift_{prefix}"] = _fmt(
            signal.event_aggressor_imbalance - pre_aggressor_imbalance
        )
        result[f"pre_classified_volume_share_{prefix}"] = _fmt(
            min(1.0, classified_volume / total_volume) if total_volume > 0 else 0.0
        )
        result[f"pre_consolidation_score_{prefix}"] = _fmt(
            abs_return / (pre_range * max(1, len(history))) if pre_range > 0 else 0.0
        )
        for name, value in _technical_history_features(history).items():
            result[f"{name}_{prefix}"] = _fmt(float(value)) if value != "" else ""
    result["feature_max_observed_at"] = "" if max_observed_at is None else max_observed_at.isoformat()
    result["feature_leakage_flag"] = bool(max_observed_at is not None and max_observed_at >= signal.source_event_at)
    return result


def _external_market_context_features(
    by_ticker_day: Mapping[
        tuple[str, date],
        tuple[Sequence[ResearchCandle], Sequence[datetime]],
    ],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    """Return only context values observed strictly before the signal."""

    result: dict[str, Any] = {}
    for ticker in MARKET_CONTEXT_TICKERS:
        rows, times = by_ticker_day.get((ticker, signal.trading_day), ((), ()))
        end_index = bisect_left(times, signal.source_event_at)
        for window in lookback_windows:
            key = f"context_{ticker.lower()}_return_bps_{window}m"
            start = signal.source_event_at - timedelta(minutes=window)
            start_index = bisect_left(times, start, hi=end_index)
            history = rows[start_index:end_index]
            result[key] = (
                _fmt(_return_bps(history[0].close, history[-1].close))
                if len(history) >= 2 and history[0].close > 0
                else ""
            )
    return result


def _forward_path(
    rows: Sequence[ResearchCandle],
    signal: SignalEvent,
    horizon_seconds: int,
    by_time: Mapping[datetime, ResearchCandle] | None = None,
) -> tuple[str, tuple[ResearchCandle, ...]]:
    resolved_by_time = by_time or {row.at: row for row in rows}
    step_count = horizon_seconds // 60
    if horizon_seconds <= 0 or horizon_seconds % 60 != 0:
        return "invalid_horizon", ()
    path: list[ResearchCandle] = []
    expected = signal.source_event_at + timedelta(minutes=1)
    for _ in range(step_count):
        row = resolved_by_time.get(expected)
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
    by_time: Mapping[datetime, ResearchCandle] | None = None,
) -> dict[str, Any]:
    reason, path = _forward_path(rows, signal, horizon_seconds, by_time=by_time)
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


def _day_volume(candles: Sequence[ResearchCandle]) -> dict[tuple[str, date], float]:
    grouped: dict[tuple[str, date], list[float]] = defaultdict(list)
    for candle in candles:
        if candle.complete and is_regular_session(candle.at):
            grouped[(candle.ticker, trading_day(candle.at))].append(candle.volume)
    return {key: statistics.fmean(values) if values else 0.0 for key, values in grouped.items()}


def _market_context_features(
    by_ticker_day: Mapping[tuple[str, date], Sequence[ResearchCandle]],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    index = _market_context_index(by_ticker_day, lookback_windows)
    features = _market_context_from_index(index, signal, lookback_windows)
    return _apply_signal_direction_to_market_context(features, signal, lookback_windows)


def _blank_market_context_features(lookback_windows: Sequence[int]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for window in lookback_windows:
        prefix = f"{window}m"
        result[f"market_return_bps_{prefix}"] = ""
        result[f"market_abs_return_bps_{prefix}"] = ""
        result[f"market_volatility_bps_{prefix}"] = ""
        result[f"signal_vs_market_bps_{prefix}"] = ""
        result[f"signal_directional_vs_market_bps_{prefix}"] = ""
        result[f"signal_market_alignment_bps_{prefix}"] = ""
    return result


def _market_context_from_index(
    index: Mapping[tuple[str, date, datetime], Mapping[str, Any]],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    features = dict(index.get((signal.ticker, signal.trading_day, signal.source_event_at), {}))
    if features:
        return features
    return _blank_market_context_features(lookback_windows)


def _market_context_index(
    by_ticker_day: Mapping[tuple[str, date], Sequence[ResearchCandle]],
    lookback_windows: Sequence[int],
    target_keys: Sequence[tuple[str, date, datetime]] | None = None,
) -> dict[tuple[str, date, datetime], dict[str, Any]]:
    normalized_windows = tuple(sorted({int(item) for item in lookback_windows}))
    if target_keys is None:
        resolved_target_keys = {
            (ticker, day, candle.at)
            for (ticker, day), rows in by_ticker_day.items()
            for candle in rows
        }
    else:
        resolved_target_keys = set(target_keys)
    target_times_by_day: dict[date, set[datetime]] = defaultdict(set)
    for _, day, at in resolved_target_keys:
        target_times_by_day[day].add(at)
    pre_returns: dict[tuple[str, date, datetime], dict[int, float]] = defaultdict(dict)
    by_day_window_time: dict[tuple[date, int, datetime], list[tuple[str, float]]] = defaultdict(list)
    for (ticker, day), raw_rows in by_ticker_day.items():
        day_target_times = sorted(target_times_by_day.get(day, ()))
        if not day_target_times:
            continue
        rows = tuple(sorted(raw_rows, key=lambda item: item.at))
        times = [row.at for row in rows]
        for window in normalized_windows:
            for target_at in day_target_times:
                start = target_at - timedelta(minutes=window)
                history_start = bisect_left(times, start)
                history_end = bisect_left(times, target_at) - 1
                if history_end <= history_start:
                    continue
                first = rows[history_start]
                last = rows[history_end]
                if first.close <= 0 or last.close <= 0:
                    continue
                pre_return = _return_bps(first.close, last.close)
                key = (ticker, day, target_at)
                if key in resolved_target_keys:
                    pre_returns[key][window] = pre_return
                by_day_window_time[(day, window, target_at)].append((ticker, pre_return))
    result: dict[tuple[str, date, datetime], dict[str, Any]] = {}
    for key in resolved_target_keys:
        ticker, day, at = key
        window_returns = pre_returns.get(key, {})
        features: dict[str, Any] = {}
        for window in normalized_windows:
            prefix = f"{window}m"
            all_returns = by_day_window_time.get((day, window, at), [])
            own_return = window_returns.get(window)
            market_returns = [value for item_ticker, value in all_returns if item_ticker != ticker]
            if not market_returns and own_return is not None:
                market_returns = [own_return]
            if not market_returns:
                features[f"market_return_bps_{prefix}"] = ""
                features[f"market_abs_return_bps_{prefix}"] = ""
                features[f"market_volatility_bps_{prefix}"] = ""
                features[f"signal_vs_market_bps_{prefix}"] = ""
                features[f"signal_directional_vs_market_bps_{prefix}"] = ""
                features[f"signal_market_alignment_bps_{prefix}"] = ""
                continue
            market_return = statistics.fmean(market_returns)
            market_volatility = statistics.pstdev(market_returns) if len(market_returns) > 1 else 0.0
            signal_pre_return = own_return if own_return is not None else 0.0
            relative_return = signal_pre_return - market_return
            # Direction-specific values are filled per signal later because
            # multiple signal types with different directions can share a
            # ticker/time row.
            features[f"market_return_bps_{prefix}"] = _fmt(market_return)
            features[f"market_abs_return_bps_{prefix}"] = _fmt(abs(market_return))
            features[f"market_volatility_bps_{prefix}"] = _fmt(market_volatility)
            features[f"signal_vs_market_bps_{prefix}"] = _fmt(relative_return)
            features[f"_raw_market_return_bps_{prefix}"] = market_return
            features[f"_raw_signal_vs_market_bps_{prefix}"] = relative_return
        result[key] = features
    public_result: dict[tuple[str, date, datetime], dict[str, Any]] = {}
    for key, features in result.items():
        public_result[key] = {
            item_key: item_value
            for item_key, item_value in features.items()
            if not item_key.startswith("_raw_")
        }
        for window in normalized_windows:
            prefix = f"{window}m"
            public_result[key].setdefault(f"signal_directional_vs_market_bps_{prefix}", "")
            public_result[key].setdefault(f"signal_market_alignment_bps_{prefix}", "")
    return public_result


def _apply_signal_direction_to_market_context(
    features: Mapping[str, Any],
    signal: SignalEvent,
    lookback_windows: Sequence[int],
) -> dict[str, Any]:
    result = dict(features)
    direction = signal.direction if signal.direction else 0
    for window in lookback_windows:
        prefix = f"{window}m"
        market_return = float_or_none(result.get(f"market_return_bps_{prefix}"))
        relative_return = float_or_none(result.get(f"signal_vs_market_bps_{prefix}"))
        result[f"signal_directional_vs_market_bps_{prefix}"] = (
            "" if relative_return is None else _fmt(direction * relative_return)
        )
        result[f"signal_market_alignment_bps_{prefix}"] = (
            "" if market_return is None else _fmt(direction * market_return)
        )
    return result


def _orderbooks_by_ticker(
    snapshots: Sequence[ResearchOrderBookSnapshot],
) -> dict[str, list[ResearchOrderBookSnapshot]]:
    result: dict[str, list[ResearchOrderBookSnapshot]] = defaultdict(list)
    for snapshot in snapshots:
        result[snapshot.ticker].append(snapshot)
    for rows in result.values():
        rows.sort(key=lambda item: item.at)
    return result


def _latest_orderbook_before(
    snapshots: Sequence[ResearchOrderBookSnapshot],
    at: datetime,
    *,
    max_age_seconds: int,
) -> ResearchOrderBookSnapshot | None:
    latest: ResearchOrderBookSnapshot | None = None
    for snapshot in snapshots:
        if snapshot.at > at:
            break
        latest = snapshot
    if latest is None:
        return None
    age = (at - latest.at).total_seconds()
    if age < 0 or age > max_age_seconds:
        return None
    return latest


def _orderbook_features(
    signal: SignalEvent,
    snapshots_by_ticker: Mapping[str, Sequence[ResearchOrderBookSnapshot]],
    *,
    max_age_seconds: int,
) -> dict[str, Any]:
    snapshot = _latest_orderbook_before(
        snapshots_by_ticker.get(signal.ticker, ()),
        signal.source_event_at,
        max_age_seconds=max_age_seconds,
    )
    if snapshot is None:
        return {
            "orderbook_available": False,
            "orderbook_age_seconds": "",
            "orderbook_depth": "",
            "orderbook_spread_bps": "",
            "orderbook_bid_qty": "",
            "orderbook_ask_qty": "",
            "orderbook_total_qty": "",
            "orderbook_imbalance_ratio": "",
            "orderbook_imbalance_abs": "",
            "orderbook_is_consistent": "",
        }
    return {
        "orderbook_available": True,
        "orderbook_age_seconds": _fmt((signal.source_event_at - snapshot.at).total_seconds()),
        "orderbook_depth": snapshot.depth,
        "orderbook_spread_bps": _fmt(snapshot.spread_bps),
        "orderbook_bid_qty": _fmt(snapshot.bid_qty),
        "orderbook_ask_qty": _fmt(snapshot.ask_qty),
        "orderbook_total_qty": _fmt(snapshot.total_qty),
        "orderbook_imbalance_ratio": _fmt(snapshot.imbalance_ratio),
        "orderbook_imbalance_abs": _fmt(snapshot.imbalance_abs),
        "orderbook_is_consistent": snapshot.is_consistent,
    }


def _native_signals_by_ticker(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        ticker = str(raw.get("ticker") or "").upper()
        if not ticker:
            continue
        created_raw = raw.get("create_at")
        if not created_raw:
            continue
        created = (
            created_raw
            if isinstance(created_raw, datetime)
            else datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
        )
        if created.tzinfo is None or created.utcoffset() is None:
            created = created.replace(tzinfo=UTC)
        closed_raw = raw.get("close_at")
        ended_raw = raw.get("end_at")
        inactive_at_raw = closed_raw or ended_raw
        inactive_at: datetime | None = None
        if inactive_at_raw:
            inactive_at = (
                inactive_at_raw
                if isinstance(inactive_at_raw, datetime)
                else datetime.fromisoformat(str(inactive_at_raw).replace("Z", "+00:00"))
            )
            if inactive_at.tzinfo is None or inactive_at.utcoffset() is None:
                inactive_at = inactive_at.replace(tzinfo=UTC)
            inactive_at = inactive_at.astimezone(UTC)
        item = dict(raw)
        item["_create_at"] = created.astimezone(UTC)
        item["_inactive_at"] = inactive_at
        result[ticker].append(item)
    for items in result.values():
        items.sort(key=lambda item: item["_create_at"])
    return result


def _native_signal_features(
    signal: SignalEvent,
    native_by_ticker: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    active: list[Mapping[str, Any]] = []
    for item in native_by_ticker.get(signal.ticker, ()):
        created = item.get("_create_at")
        if not isinstance(created, datetime) or created > signal.source_event_at:
            if isinstance(created, datetime) and created > signal.source_event_at:
                break
            continue
        inactive_at = item.get("_inactive_at")
        if isinstance(inactive_at, datetime) and inactive_at <= signal.source_event_at:
            continue
        active.append(item)
    if not active:
        return {
            "native_signal_available": False,
            "native_signal_active_count": 0,
            "native_signal_strategy_count": 0,
            "native_signal_technical_count": 0,
            "native_signal_fundamental_count": 0,
            "native_signal_buy_count": 0,
            "native_signal_sell_count": 0,
            "native_signal_probability_max": 0,
            "native_signal_probability_mean": 0,
            "native_signal_direction_score": 0,
            "native_signal_consensus_direction": "none",
            "native_signal_detector_alignment": 0,
        }
    probabilities = [max(0.0, float(item.get("probability") or 0.0)) for item in active]
    directions = [int(float(item.get("direction") or 0)) for item in active]
    total_weight = sum(probabilities)
    score = (
        sum(direction * probability for direction, probability in zip(directions, probabilities))
        / total_weight
        if total_weight > 0
        else 0.0
    )
    if score > 0.10:
        consensus = "buy"
        consensus_direction = 1
    elif score < -0.10:
        consensus = "sell"
        consensus_direction = -1
    else:
        consensus = "mixed"
        consensus_direction = 0
    return {
        "native_signal_available": True,
        "native_signal_active_count": len(active),
        "native_signal_strategy_count": len({str(item.get("strategy_key") or "") for item in active}),
        "native_signal_technical_count": sum(
            str(item.get("strategy_type")) == "STRATEGY_TYPE_TECHNICAL" for item in active
        ),
        "native_signal_fundamental_count": sum(
            str(item.get("strategy_type")) == "STRATEGY_TYPE_FUNDAMENTAL" for item in active
        ),
        "native_signal_buy_count": sum(direction > 0 for direction in directions),
        "native_signal_sell_count": sum(direction < 0 for direction in directions),
        "native_signal_probability_max": _fmt(max(probabilities, default=0.0)),
        "native_signal_probability_mean": _fmt(statistics.fmean(probabilities)),
        "native_signal_direction_score": _fmt(score),
        "native_signal_consensus_direction": consensus,
        "native_signal_detector_alignment": signal.direction * consensus_direction,
    }


def orderbook_signal_coverage_summary(
    candles: Sequence[ResearchCandle],
    orderbook_snapshots: Sequence[ResearchOrderBookSnapshot],
    *,
    max_age_seconds_options: Sequence[int],
    policy: ReplayPolicy | None = None,
    max_signals_per_instrument: int = 10_000,
) -> list[dict[str, Any]]:
    policy = policy or ReplayPolicy()
    signals = replay_signals(
        candles,
        policy,
        max_signals_per_instrument=max_signals_per_instrument,
    )
    snapshots_by_ticker = _orderbooks_by_ticker(orderbook_snapshots)
    signal_times = [signal.source_event_at for signal in signals]
    snapshot_times = [snapshot.at for snapshot in orderbook_snapshots]
    min_abs_gap: float | None = None
    min_prior_gap: float | None = None
    for signal in signals:
        for snapshot in snapshots_by_ticker.get(signal.ticker, ()):
            abs_gap = abs((signal.source_event_at - snapshot.at).total_seconds())
            min_abs_gap = abs_gap if min_abs_gap is None else min(min_abs_gap, abs_gap)
            if snapshot.at <= signal.source_event_at:
                prior_gap = (signal.source_event_at - snapshot.at).total_seconds()
                min_prior_gap = prior_gap if min_prior_gap is None else min(min_prior_gap, prior_gap)
    time_diagnostics = {
        "first_signal_at": min(signal_times).isoformat() if signal_times else "",
        "last_signal_at": max(signal_times).isoformat() if signal_times else "",
        "first_orderbook_at": min(snapshot_times).isoformat() if snapshot_times else "",
        "last_orderbook_at": max(snapshot_times).isoformat() if snapshot_times else "",
        "nearest_signal_orderbook_gap_seconds": "" if min_abs_gap is None else _fmt(min_abs_gap),
        "nearest_prior_orderbook_age_seconds": "" if min_prior_gap is None else _fmt(min_prior_gap),
    }
    result: list[dict[str, Any]] = []
    for max_age_seconds in sorted({int(item) for item in max_age_seconds_options}):
        covered: list[SignalEvent] = []
        covered_by_type: Counter[str] = Counter()
        for signal in signals:
            snapshot = _latest_orderbook_before(
                snapshots_by_ticker.get(signal.ticker, ()),
                signal.source_event_at,
                max_age_seconds=max_age_seconds,
            )
            if snapshot is None:
                continue
            covered.append(signal)
            covered_by_type[signal.signal_type] += 1
        result.append(
            {
                "max_age_seconds": max_age_seconds,
                "signals": len(signals),
                "covered_signals": len(covered),
                "coverage": len(covered) / len(signals) if signals else 0.0,
                "sessions": len({signal.trading_day for signal in signals}),
                "covered_sessions": len({signal.trading_day for signal in covered}),
                "orderbook_snapshots": len(orderbook_snapshots),
                "covered_by_type": dict(sorted(covered_by_type.items())),
                **time_diagnostics,
            }
        )
    return result


def orderbook_signal_coverage_by_ticker_day(
    candles: Sequence[ResearchCandle],
    orderbook_snapshots: Sequence[ResearchOrderBookSnapshot],
    *,
    max_age_seconds: int = 30,
    policy: ReplayPolicy | None = None,
    max_signals_per_instrument: int = 10_000,
) -> list[dict[str, Any]]:
    policy = policy or ReplayPolicy()
    signals = replay_signals(
        candles,
        policy,
        max_signals_per_instrument=max_signals_per_instrument,
    )
    snapshots_by_ticker = _orderbooks_by_ticker(orderbook_snapshots)
    snapshots_by_ticker_day: dict[tuple[str, date], list[ResearchOrderBookSnapshot]] = defaultdict(list)
    for snapshot in orderbook_snapshots:
        snapshots_by_ticker_day[(snapshot.ticker, snapshot.at.date())].append(snapshot)

    grouped_signals: dict[tuple[str, date], list[SignalEvent]] = defaultdict(list)
    for signal in signals:
        grouped_signals[(signal.ticker, signal.trading_day)].append(signal)

    rows: list[dict[str, Any]] = []
    for (ticker, trading_day_value), group in sorted(grouped_signals.items()):
        ticker_snapshots = snapshots_by_ticker.get(ticker, ())
        day_snapshots = snapshots_by_ticker_day.get((ticker, trading_day_value), ())
        covered: list[SignalEvent] = []
        prior_ages: list[float] = []
        for signal in group:
            snapshot = _latest_orderbook_before(
                ticker_snapshots,
                signal.source_event_at,
                max_age_seconds=max_age_seconds,
            )
            if snapshot is None:
                continue
            covered.append(signal)
            prior_ages.append((signal.source_event_at - snapshot.at).total_seconds())
        first_signal_at = min((signal.source_event_at for signal in group), default=None)
        last_signal_at = max((signal.source_event_at for signal in group), default=None)
        first_snapshot_at = min((snapshot.at for snapshot in day_snapshots), default=None)
        last_snapshot_at = max((snapshot.at for snapshot in day_snapshots), default=None)
        rows.append(
            {
                "ticker": ticker,
                "trading_day": trading_day_value.isoformat(),
                "max_age_seconds": max_age_seconds,
                "signals": len(group),
                "covered_signals": len(covered),
                "missing_signals": max(0, len(group) - len(covered)),
                "coverage": len(covered) / len(group) if group else 0.0,
                "orderbook_snapshots": len(day_snapshots),
                "first_signal_at": first_signal_at.isoformat() if first_signal_at else "",
                "last_signal_at": last_signal_at.isoformat() if last_signal_at else "",
                "first_orderbook_at": first_snapshot_at.isoformat() if first_snapshot_at else "",
                "last_orderbook_at": last_snapshot_at.isoformat() if last_snapshot_at else "",
                "min_prior_age_seconds": _fmt(min(prior_ages)) if prior_ages else "",
                "median_prior_age_seconds": _fmt(statistics.median(prior_ages)) if prior_ages else "",
                "status": "covered" if covered else "missing",
            }
        )
    return rows


def holdout_readiness_summary(
    coverage_rows: Sequence[Mapping[str, Any]],
    *,
    min_covered_signals: int = 300,
    min_covered_sessions: int = 30,
    min_coverage: float = 0.80,
    preferred_max_age_seconds: int | None = 30,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in coverage_rows:
        covered_signals = int(row.get("covered_signals", 0) or 0)
        covered_sessions = int(row.get("covered_sessions", 0) or 0)
        coverage = float(row.get("coverage", 0.0) or 0.0)
        max_age_seconds = int(row.get("max_age_seconds", 0) or 0)
        total_signals = int(row.get("signals", 0) or 0)
        coverage_target_signals = math.ceil(min_coverage * total_signals) if total_signals else min_covered_signals
        required_covered_signals = max(min_covered_signals, coverage_target_signals)
        missing_covered_signals = max(0, required_covered_signals - covered_signals)
        missing_covered_sessions = max(0, min_covered_sessions - covered_sessions)
        reasons: list[str] = []
        if preferred_max_age_seconds is not None and max_age_seconds > preferred_max_age_seconds:
            reasons.append("orderbook_window_too_wide")
        if covered_signals < min_covered_signals:
            reasons.append("not_enough_orderbook_covered_signals")
        if covered_sessions < min_covered_sessions:
            reasons.append("not_enough_orderbook_covered_sessions")
        if coverage < min_coverage:
            reasons.append("orderbook_coverage_too_sparse")
        result.append(
            {
                "max_age_seconds": max_age_seconds,
                "signals": total_signals,
                "covered_signals": covered_signals,
                "coverage": coverage,
                "sessions": int(row.get("sessions", 0) or 0),
                "covered_sessions": covered_sessions,
                "orderbook_snapshots": int(row.get("orderbook_snapshots", 0) or 0),
                "min_covered_signals": min_covered_signals,
                "min_covered_sessions": min_covered_sessions,
                "min_coverage": min_coverage,
                "coverage_target_signals": coverage_target_signals,
                "required_covered_signals": required_covered_signals,
                "missing_covered_signals": missing_covered_signals,
                "missing_covered_sessions": missing_covered_sessions,
                "preferred_max_age_seconds": preferred_max_age_seconds,
                "ready": not reasons,
                "reason_codes": reasons,
            }
        )
    return result


def build_signal_price_dataset(
    candles: Sequence[ResearchCandle],
    *,
    horizons_seconds: Sequence[int],
    lookback_windows: Sequence[int],
    policy: ReplayPolicy | None = None,
    max_signals_per_instrument: int = 10_000,
    orderbook_snapshots: Sequence[ResearchOrderBookSnapshot] = (),
    orderbook_max_age_seconds: int = 30,
    native_signal_rows: Sequence[Mapping[str, Any]] = (),
    market_context_candles: Sequence[ResearchCandle] = (),
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
    external_context_by_ticker_day: dict[tuple[str, date], list[ResearchCandle]] = defaultdict(list)
    for candle in market_context_candles:
        external_context_by_ticker_day[(candle.ticker, trading_day(candle.at))].append(candle)
    for context_rows in external_context_by_ticker_day.values():
        context_rows.sort(key=lambda item: item.at)
    external_context_time_index = {
        key: (tuple(context_rows), tuple(item.at for item in context_rows))
        for key, context_rows in external_context_by_ticker_day.items()
    }
    by_ticker_day_time = {
        key: {row.at: row for row in rows}
        for key, rows in by_ticker_day.items()
    }
    combinations = _combination_features(signals)
    day_vol = _day_volatility(candles)
    day_quantiles = _quantile_rank(day_vol)
    ticker_vol = {
        ticker: statistics.fmean(values)
        for ticker, values in _group_values((key[0], value) for key, value in day_vol.items()).items()
    }
    ticker_quantiles = _quantile_rank(ticker_vol)
    day_volume = _day_volume(candles)
    day_volume_quantiles = _quantile_rank(day_volume)
    ticker_volume = {
        ticker: statistics.fmean(values)
        for ticker, values in _group_values((key[0], value) for key, value in day_volume.items()).items()
    }
    ticker_volume_quantiles = _quantile_rank(ticker_volume)
    signal_market_context_keys = tuple(
        (signal.ticker, signal.trading_day, signal.source_event_at)
        for signal in signals
    )
    market_context_index = _market_context_index(
        by_ticker_day,
        normalized_windows,
        target_keys=signal_market_context_keys,
    )
    orderbooks_by_ticker = _orderbooks_by_ticker(orderbook_snapshots)
    native_by_ticker = _native_signals_by_ticker(native_signal_rows)
    rows: list[dict[str, Any]] = []
    for signal in signals:
        ticker_day_key = (signal.ticker, signal.trading_day)
        candles_for_day = by_ticker_day[ticker_day_key]
        candles_for_day_by_time = by_ticker_day_time[ticker_day_key]
        pre_features = _pre_signal_features(candles_for_day, signal, normalized_windows)
        market_features = _apply_signal_direction_to_market_context(
            _market_context_from_index(market_context_index, signal, normalized_windows),
            signal,
            normalized_windows,
        )
        combo = combinations[(signal.ticker, signal.source_event_at, signal.signal_type)]
        orderbook_features = _orderbook_features(
            signal,
            orderbooks_by_ticker,
            max_age_seconds=orderbook_max_age_seconds,
        )
        native_features = _native_signal_features(signal, native_by_ticker)
        external_context_features = _external_market_context_features(
            external_context_time_index,
            signal,
            normalized_windows,
        )
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
            "event_volume": _fmt(signal.event_volume),
            "event_volume_buy": _fmt(signal.event_volume_buy),
            "event_volume_sell": _fmt(signal.event_volume_sell),
            "event_aggressor_imbalance": _fmt(signal.event_aggressor_imbalance),
            "event_aggressor_alignment": _fmt(signal.direction * signal.event_aggressor_imbalance),
            "event_classified_volume_share": _fmt(signal.event_classified_volume_share),
            "baseline_volume": _fmt(signal.baseline_volume),
            "event_volume_ratio": _fmt(signal.event_volume_ratio),
            "event_range_ratio": _fmt(signal.event_range_ratio),
            "event_strength_to_volatility": _fmt(signal.event_strength_to_volatility),
            "candle_close_position": _fmt(signal.candle_close_position),
            "event_body_bps": _fmt(signal.event_body_bps),
            "event_upper_wick_bps": _fmt(signal.event_upper_wick_bps),
            "event_lower_wick_bps": _fmt(signal.event_lower_wick_bps),
            "event_body_to_range": _fmt(signal.event_body_to_range),
            "event_upper_wick_to_range": _fmt(signal.event_upper_wick_to_range),
            "event_lower_wick_to_range": _fmt(signal.event_lower_wick_to_range),
            "event_close_to_direction": _fmt(signal.event_close_to_direction),
            "event_reversal_pressure": _fmt(signal.event_reversal_pressure),
            **orderbook_features,
            **native_features,
            **external_context_features,
            "day_volatility_bps": _fmt(day_vol.get((signal.ticker, signal.trading_day), 0.0)),
            "day_volatility_quantile": _fmt(day_quantiles.get((signal.ticker, signal.trading_day), 0.0)),
            "ticker_volatility_quantile": _fmt(ticker_quantiles.get(signal.ticker, 0.0)),
            "day_volume_quantile": _fmt(day_volume_quantiles.get((signal.ticker, signal.trading_day), 0.0)),
            "ticker_volume_quantile": _fmt(ticker_volume_quantiles.get(signal.ticker, 0.0)),
            "ticker_mean_daily_volume": _fmt(ticker_volume.get(signal.ticker, 0.0)),
            **combo,
            **pre_features,
            **market_features,
        }
        for horizon in normalized_horizons:
            row = {
                "row_id": _row_id(signal, horizon),
                **base,
                "horizon_seconds": horizon,
                **_outcome_fields(signal, horizon, candles_for_day, policy, by_time=candles_for_day_by_time),
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
            "orderbook_snapshots": len(orderbook_snapshots),
            "orderbook_feature_rows": sum(1 for row in rows if row["orderbook_available"]),
            "orderbook_max_age_seconds": orderbook_max_age_seconds,
            "native_signal_cache_rows": len(native_signal_rows),
            "native_signal_feature_rows": sum(1 for row in rows if row["native_signal_available"]),
            "market_context_candles": len(market_context_candles),
            "market_context_feature_rows": sum(
                1
                for row in rows
                if any(row.get(f"context_{ticker.lower()}_return_bps_15m") not in ("", None) for ticker in MARKET_CONTEXT_TICKERS)
            ),
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


def wilson_lower_bound(successes: int, total: int, *, z: float = 1.96) -> float | None:
    """Return the lower 95% Wilson score bound for a binomial success rate."""

    if total <= 0:
        return None
    phat = successes / total
    denominator = 1.0 + z * z / total
    centre = phat + z * z / (2.0 * total)
    margin = z * math.sqrt((phat * (1.0 - phat) + z * z / (4.0 * total)) / total)
    return max(0.0, (centre - margin) / denominator)


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
        "event_volume",
        "event_volume_buy",
        "event_volume_sell",
        "event_aggressor_imbalance",
        "event_aggressor_alignment",
        "event_classified_volume_share",
        "baseline_volume",
        "event_volume_ratio",
        "event_range_ratio",
        "event_strength_to_volatility",
        "candle_close_position",
        "event_body_bps",
        "event_upper_wick_bps",
        "event_lower_wick_bps",
        "event_body_to_range",
        "event_upper_wick_to_range",
        "event_lower_wick_to_range",
        "event_close_to_direction",
        "event_reversal_pressure",
        "orderbook_age_seconds",
        "orderbook_depth",
        "orderbook_spread_bps",
        "orderbook_bid_qty",
        "orderbook_ask_qty",
        "orderbook_total_qty",
        "orderbook_imbalance_ratio",
        "orderbook_imbalance_abs",
        "native_signal_active_count",
        "native_signal_strategy_count",
        "native_signal_technical_count",
        "native_signal_fundamental_count",
        "native_signal_buy_count",
        "native_signal_sell_count",
        "native_signal_probability_max",
        "native_signal_probability_mean",
        "native_signal_direction_score",
        "native_signal_detector_alignment",
        "day_volatility_bps",
        "day_volatility_quantile",
        "ticker_volatility_quantile",
        "day_volume_quantile",
        "ticker_volume_quantile",
        "ticker_mean_daily_volume",
        "recent_signal_count_60s",
        "recent_signal_count_300s",
        "recent_signal_count_900s",
        "recent_same_family_count_300s",
        "recent_price_jump_300s",
        "recent_volume_spike_300s",
        "recent_candle_range_spike_300s",
        "recent_directional_combo_300s",
        "pre_return_bps_5m",
        "pre_abs_return_bps_5m",
        "pre_directional_return_bps_5m",
        "pre_volatility_bps_5m",
        "pre_return_to_volatility_5m",
        "event_to_pre_volatility_5m",
        "pre_range_bps_5m",
        "event_to_pre_range_5m",
        "pre_volume_change_5m",
        "pre_aggressor_imbalance_5m",
        "pre_aggressor_alignment_5m",
        "aggressor_imbalance_shift_5m",
        "pre_classified_volume_share_5m",
        "pre_consolidation_score_5m",
        "market_return_bps_5m",
        "market_abs_return_bps_5m",
        "market_volatility_bps_5m",
        "signal_vs_market_bps_5m",
        "signal_directional_vs_market_bps_5m",
        "signal_market_alignment_bps_5m",
        "pre_return_bps_15m",
        "pre_abs_return_bps_15m",
        "pre_directional_return_bps_15m",
        "pre_volatility_bps_15m",
        "pre_return_to_volatility_15m",
        "event_to_pre_volatility_15m",
        "pre_range_bps_15m",
        "event_to_pre_range_15m",
        "pre_volume_change_15m",
        "pre_aggressor_imbalance_15m",
        "pre_aggressor_alignment_15m",
        "aggressor_imbalance_shift_15m",
        "pre_classified_volume_share_15m",
        "pre_consolidation_score_15m",
        "market_return_bps_15m",
        "market_abs_return_bps_15m",
        "market_volatility_bps_15m",
        "signal_vs_market_bps_15m",
        "signal_directional_vs_market_bps_15m",
        "signal_market_alignment_bps_15m",
        "pre_return_bps_30m",
        "pre_abs_return_bps_30m",
        "pre_directional_return_bps_30m",
        "pre_volatility_bps_30m",
        "pre_return_to_volatility_30m",
        "event_to_pre_volatility_30m",
        "pre_range_bps_30m",
        "event_to_pre_range_30m",
        "pre_volume_change_30m",
        "pre_aggressor_imbalance_30m",
        "pre_aggressor_alignment_30m",
        "aggressor_imbalance_shift_30m",
        "pre_classified_volume_share_30m",
        "pre_consolidation_score_30m",
        "market_return_bps_30m",
        "market_abs_return_bps_30m",
        "market_volatility_bps_30m",
        "signal_vs_market_bps_30m",
        "signal_directional_vs_market_bps_30m",
        "signal_market_alignment_bps_30m",
        "pre_return_bps_60m",
        "pre_abs_return_bps_60m",
        "pre_directional_return_bps_60m",
        "pre_volatility_bps_60m",
        "pre_return_to_volatility_60m",
        "event_to_pre_volatility_60m",
        "pre_range_bps_60m",
        "event_to_pre_range_60m",
        "pre_volume_change_60m",
        "pre_aggressor_imbalance_60m",
        "pre_aggressor_alignment_60m",
        "aggressor_imbalance_shift_60m",
        "pre_classified_volume_share_60m",
        "pre_consolidation_score_60m",
        "market_return_bps_60m",
        "market_abs_return_bps_60m",
        "market_volatility_bps_60m",
        "signal_vs_market_bps_60m",
        "signal_directional_vs_market_bps_60m",
        "signal_market_alignment_bps_60m",
    ]
    categorical = [
        "ticker",
        "signal_type",
        "family",
        "direction",
        "combo_key_300s",
        "orderbook_available",
        "orderbook_is_consistent",
        "native_signal_available",
        "native_signal_consensus_direction",
    ]
    numeric.extend(
        f"context_{ticker.lower()}_return_bps_{window}m"
        for ticker in MARKET_CONTEXT_TICKERS
        for window in (5, 15, 30, 60)
    )
    numeric.extend(
        f"{name}_{window}m"
        for name in TECHNICAL_FEATURE_NAMES
        for window in TECHNICAL_FEATURE_WINDOWS
    )
    existing = set(rows[0]) if rows else set()
    return [item for item in numeric if item in existing], [item for item in categorical if item in existing]


def render_markdown_report(results: Mapping[str, Any]) -> str:
    leaderboard = results.get("leaderboard", [])
    accepted = [item for item in leaderboard if item.get("accepted")]
    inverse = [item for item in results.get("event_study", []) if item.get("inverse_candidate")]
    confidence_thresholds = results.get("confidence_thresholds", [])
    selective_frontier = results.get("selective_frontier", [])
    candidate_watchlist = results.get("candidate_watchlist", [])
    high_confidence_slices = results.get("high_confidence_slices", [])
    selective_rule_candidates = results.get("selective_rule_candidates", [])
    precision_scout_candidates = results.get("precision_scout_candidates", [])
    precision_scout_summary = results.get("precision_scout_summary", {})
    temporal_stability = results.get("temporal_stability", [])
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
    lines.extend(["", "## Confidence threshold triage", ""])
    if not confidence_thresholds:
        lines.append("No directional triage threshold report is available.")
    else:
        lines.append(
            "| Threshold | Selected | Skipped | Up | Down | Direct | Inverse | Neutral | Success rate | Wilson lower 95% | Mean result bps | Accepted |"
        )
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|")
        for row in confidence_thresholds:
            success_rate = row.get("success_rate")
            lower = row.get("wilson_lower_95")
            mean_result = row.get("mean_selected_result_bps")
            lines.append(
                "| {threshold:.2f} | {selected} | {skipped} | {up} | {down} | {direct} | {inverse} | {neutral} | {rate} | {lower} | {mean} | {accepted} |".format(
                    threshold=float(row.get("threshold") or 0.0),
                    selected=row.get("selected_rows", 0),
                    skipped=row.get("skipped_rows", 0),
                    up=row.get("up_decisions", 0),
                    down=row.get("down_decisions", 0),
                    direct=row.get("direct_decisions", 0),
                    inverse=row.get("inverse_decisions", 0),
                    neutral=row.get("neutral_decisions", 0),
                    rate="" if success_rate is None else f"{float(success_rate):.4f}",
                    lower="" if lower is None else f"{float(lower):.4f}",
                    mean="" if mean_result is None else f"{float(mean_result):.3f}",
                    accepted=row.get("accepted_research", False),
                )
            )
    lines.extend(["", "## Selective top-confidence frontier", ""])
    if not selective_frontier:
        lines.append("No selective frontier report is available.")
    else:
        lines.append(
            "| Scope | Rule | Selected | Sessions | Min confidence | Success rate | Wilson lower 95% | Mean result bps | Observed 90% | Accepted |"
        )
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---|---|")
        for row in selective_frontier[:20]:
            success_rate = row.get("success_rate")
            lower = row.get("wilson_lower_95")
            min_confidence = row.get("min_confidence")
            mean_result = row.get("mean_selected_result_bps")
            lines.append(
                "| {scope} | {rule} | {selected} | {sessions} | {confidence} | {rate} | {lower} | {mean} | {observed} | {accepted} |".format(
                    scope=str(row.get("scope", "")),
                    rule=str(row.get("rule", "")).replace("|", "\\|"),
                    selected=row.get("selected_rows", 0),
                    sessions=row.get("sessions", 0),
                    confidence="" if min_confidence is None else f"{float(min_confidence):.4f}",
                    rate="" if success_rate is None else f"{float(success_rate):.4f}",
                    lower="" if lower is None else f"{float(lower):.4f}",
                    mean="" if mean_result is None else f"{float(mean_result):.3f}",
                    observed=row.get("observed_90_success", False),
                    accepted=row.get("accepted_research", False),
                )
            )
    lines.extend(["", "## Candidate watchlist", ""])
    if not candidate_watchlist:
        lines.append("No underpowered 90% candidates are on the watchlist.")
    else:
        lines.append(
            "| Candidate | Rule | Selected | Sessions | Success rate | Wilson lower 95% | Missing rows | Missing days | Extra successes for 300-row 90% | Missing reasons |"
        )
        lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---|")
        for row in candidate_watchlist[:20]:
            lines.append(
                "| {candidate} | {rule} | {selected} | {sessions} | {rate:.4f} | {lower:.4f} | {missing_rows} | {missing_days} | {extra_successes} | {reasons} |".format(
                    candidate=row.get("candidate_id", ""),
                    rule=str(row.get("rule", "")).replace("|", "\\|"),
                    selected=row.get("selected_rows", 0),
                    sessions=row.get("sessions", 0),
                    rate=float(row.get("success_rate") or 0.0),
                    lower=float(row.get("wilson_lower_95") or 0.0),
                    missing_rows=row.get("missing_rows_to_shadow_gate", 0),
                    missing_days=row.get("missing_sessions_to_shadow_gate", 0),
                    extra_successes=row.get("additional_successes_needed_for_90pct_at_300", ""),
                    reasons=str(row.get("missing_reasons", "")).replace("|", "\\|"),
                )
            )
    lines.extend(["", "## High-confidence market-state slices", ""])
    if not high_confidence_slices:
        lines.append("No high-confidence slice report is available.")
    else:
        lines.append(
            "| Rule | Threshold | n | Sessions | Success rate | Wilson lower 95% | Mean result bps | Observed 90% | Reliable 90% | Shadow accepted |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---|---|---|")
        for row in high_confidence_slices[:20]:
            lines.append(
                "| {rule} | {threshold:.2f} | {n} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {observed} | {reliable} | {accepted} |".format(
                    rule=str(row.get("rule", "")).replace("|", "\\|"),
                    threshold=float(row.get("threshold") or 0.0),
                    n=row.get("selected_rows", 0),
                    sessions=row.get("sessions", 0),
                    rate=float(row.get("success_rate") or 0.0),
                    lower=float(row.get("wilson_lower_95") or 0.0),
                    mean=float(row.get("mean_result_bps") or 0.0),
                    observed=row.get("observed_90_success", False),
                    reliable=row.get("reliable_90_success", False),
                    accepted=row.get("accepted_shadow", False),
                )
            )
    lines.extend(["", "## Selective conjunction rules", ""])
    if not selective_rule_candidates:
        lines.append("No selective conjunction rule report is available.")
    else:
        lines.append(
            "| Rule | Terms | Eval rows | Eval sessions | Success rate | Wilson lower 95% | Mean result bps | Shadow accepted | Blocking reasons |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---|---|")
        for row in selective_rule_candidates[:20]:
            lines.append(
                "| {rule} | {terms} | {rows} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {accepted} | {reasons} |".format(
                    rule=str(row.get("rule", "")).replace("|", "\\|"),
                    terms=row.get("terms", 0),
                    rows=row.get("evaluation_rows", 0),
                    sessions=row.get("evaluation_sessions", 0),
                    rate=float(row.get("evaluation_success_rate") or 0.0),
                    lower=float(row.get("evaluation_wilson_lower_95") or 0.0),
                    mean=float(row.get("evaluation_mean_result_bps") or 0.0),
                    accepted=row.get("accepted_shadow", False),
                    reasons=str(row.get("blocking_reasons", "")).replace("|", "\\|"),
                )
            )
    lines.extend(["", "## Precision scout rules", ""])
    if not precision_scout_candidates:
        lines.append("No precision scout rule report is available.")
    else:
        if isinstance(precision_scout_summary, Mapping):
            lines.extend(
                [
                    "| Summary | Value |",
                    "|---|---:|",
                    f"| Candidates | {precision_scout_summary.get('candidate_rows', 0)} |",
                    f"| Watch-only | {precision_scout_summary.get('watch_only', 0)} |",
                    f"| Positive result rows | {precision_scout_summary.get('positive_result_rows', 0)} |",
                    f"| Can reach 90% at 300 rows | {precision_scout_summary.get('can_reach_90pct_at_min_rows', 0)} |",
                    "",
                ]
            )
            proof_counts = precision_scout_summary.get("proof_viability_counts")
            if isinstance(proof_counts, Mapping):
                lines.extend(["| Proof viability | Count |", "|---|---:|"])
                for category, count in proof_counts.items():
                    lines.append(f"| {category} | {count} |")
                lines.append("")
            action_counts = precision_scout_summary.get("next_action_counts")
            if isinstance(action_counts, Mapping):
                lines.extend(["| Next action | Count |", "|---|---:|"])
                for action, count in action_counts.items():
                    lines.append(f"| {action} | {count} |")
                lines.append("")
        lines.append(
            "| Rule | Direction | Hypothesis | Terms | Eval rows | Eval sessions | Success rate | Wilson lower 95% | Mean result bps | Missing rows | Missing days | Extra successes needed | Allowed future failures | Required future success | Proof viability | Next action | Status | Blocking reasons |"
        )
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|")
        for row in precision_scout_candidates[:20]:
            lines.append(
                "| {rule} | {direction} | {relation} | {terms} | {rows} | {sessions} | {rate:.4f} | {lower:.4f} | {mean:.3f} | {missing_rows} | {missing_days} | {extra_successes} | {allowed_failures} | {future_rate:.4f} | {viability} | {next_action} | {status} | {reasons} |".format(
                    rule=str(row.get("rule", "")).replace("|", "\\|"),
                    direction=row.get("dominant_decision", ""),
                    relation=row.get("dominant_relation", ""),
                    terms=row.get("terms", 0),
                    rows=row.get("evaluation_rows", 0),
                    sessions=row.get("evaluation_sessions", 0),
                    rate=float(row.get("evaluation_success_rate") or 0.0),
                    lower=float(row.get("evaluation_wilson_lower_95") or 0.0),
                    mean=float(row.get("evaluation_mean_result_bps") or 0.0),
                    missing_rows=row.get("missing_rows_to_shadow_gate", 0),
                    missing_days=row.get("missing_sessions_to_shadow_gate", 0),
                    extra_successes=row.get("additional_successes_needed_for_90pct_at_min_rows", 0),
                    allowed_failures=row.get("allowed_future_failures_for_90pct_at_min_rows", 0),
                    future_rate=float(row.get("required_future_success_rate_for_90pct_at_min_rows") or 0.0),
                    viability=row.get("proof_viability", ""),
                    next_action=row.get("proof_next_action", ""),
                    status=row.get("status", ""),
                    reasons=str(row.get("blocking_reasons", "")).replace("|", "\\|"),
                )
            )
    lines.extend(["", "## Temporal threshold stability", ""])
    if not temporal_stability:
        lines.append("No temporal stability report is available.")
    else:
        lines.append(
            "| Threshold | Block | Days | Selected | Success rate | Wilson lower 95% | Mean result bps | Observed 90% | Reliable 90% |"
        )
        lines.append("|---:|---:|---|---:|---:|---:|---:|---|---|")
        for row in temporal_stability[:30]:
            success_rate = row.get("success_rate")
            lower = row.get("wilson_lower_95")
            mean_result = row.get("mean_selected_result_bps")
            lines.append(
                "| {threshold:.2f} | {block}/{blocks} | {first_day}–{last_day} | {selected} | {rate} | {lower} | {mean} | {observed} | {reliable} |".format(
                    threshold=float(row.get("threshold") or 0.0),
                    block=row.get("block_index", ""),
                    blocks=row.get("block_count", ""),
                    first_day=row.get("first_day", ""),
                    last_day=row.get("last_day", ""),
                    selected=row.get("selected_rows", 0),
                    rate="" if success_rate is None else f"{float(success_rate):.4f}",
                    lower="" if lower is None else f"{float(lower):.4f}",
                    mean="" if mean_result is None else f"{float(mean_result):.3f}",
                    observed=row.get("observed_90_success", False),
                    reliable=row.get("reliable_90_success", False),
                )
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
