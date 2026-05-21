"""ClickHouse-backed seasonal baselines for trade bars (slot-of-day, multi-day lookback)."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _escape_ch_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


@dataclass(frozen=True)
class SlotBaseline:
    """Distribution for one (instrument, timeframe, metric, slot_minute)."""

    median: float
    p90: float
    p95: float
    p99: float
    sample_days: int


class HistoricalBaselineStore:
    """In-memory cache of precomputed baselines loaded from ClickHouse."""

    def __init__(
        self,
        *,
        base_url: str | None,
        username: str | None,
        password: str | None,
        refresh_seconds: float = 300.0,
    ) -> None:
        self._base_url = (base_url or "").strip().rstrip("/") or None
        self._username = (username or "").strip() or None
        self._password = (password or "").strip() or None
        self._refresh_seconds = max(30.0, float(refresh_seconds))
        self._last_refresh = 0.0
        self._cache: dict[tuple[str, str, str, int], SlotBaseline] = {}
        self._last_computed_at: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    def maybe_refresh(self, *, force: bool = False) -> None:
        if not self.enabled:
            return
        now = time.monotonic()
        if not force and (now - self._last_refresh) < self._refresh_seconds:
            return
        self._refresh_all()
        self._last_refresh = now

    def lookup(
        self,
        instrument_id: str,
        timeframe: str,
        metric: str,
        slot_minute: int,
    ) -> SlotBaseline | None:
        if not self.enabled:
            return None
        key = (instrument_id, timeframe, metric, int(slot_minute))
        return self._cache.get(key)

    def _ch_post(self, query: str) -> str:
        assert self._base_url is not None
        auth = None
        if self._username and self._password:
            auth = (self._username, self._password)
        with httpx.Client(timeout=120.0, auth=auth) as client:
            response = client.post(self._base_url, content=query.encode("utf-8"))
            response.raise_for_status()
        return response.text

    def _refresh_all(self) -> None:
        assert self._base_url is not None
        q_max = (
            "SELECT max(computed_at) AS m "
            "FROM signal_engine.historical_baseline_slot_stats "
            "FORMAT JSONEachRow"
        )
        try:
            raw = self._ch_post(q_max).strip()
        except Exception:
            logger.exception("ClickHouse baseline: failed to read max(computed_at)")
            return
        if not raw:
            logger.warning("ClickHouse baseline: empty max(computed_at)")
            return
        try:
            row = json.loads(raw.split("\n")[0])
        except json.JSONDecodeError:
            logger.warning("ClickHouse baseline: bad JSON for max(computed_at)")
            return
        ts = row.get("m")
        if ts is None:
            logger.warning(
                "ClickHouse baseline: no rows in historical_baseline_slot_stats"
            )
            return
        ts_str = str(ts).replace("T", " ")[:19]
        q_rows = (
            "SELECT instrument_id, timeframe, slot_minute, metric, "
            "median, p90, p95, p99, sample_days "
            "FROM signal_engine.historical_baseline_slot_stats "
            f"WHERE computed_at = toDateTime('{_escape_ch_string(ts_str)}', 'UTC') "
            "FORMAT JSONEachRow"
        )
        try:
            body = self._ch_post(q_rows).strip()
        except Exception:
            logger.exception("ClickHouse baseline: failed to load stats rows")
            return
        new_cache: dict[tuple[str, str, str, int], SlotBaseline] = {}
        for line in body.split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            iid = str(r.get("instrument_id", "")).strip()
            tf = str(r.get("timeframe", "")).strip()
            met = str(r.get("metric", "")).strip()
            if not iid or not tf or not met:
                continue
            slot = int(r.get("slot_minute", 0))
            new_cache[(iid, tf, met, slot)] = SlotBaseline(
                median=float(r.get("median", 0.0) or 0.0),
                p90=float(r.get("p90", 0.0) or 0.0),
                p95=float(r.get("p95", 0.0) or 0.0),
                p99=float(r.get("p99", 0.0) or 0.0),
                sample_days=int(r.get("sample_days", 0) or 0),
            )
        self._cache = new_cache
        self._last_computed_at = ts_str
        logger.info(
            "Loaded %s historical baseline slots (computed_at=%s)",
            len(self._cache),
            ts_str,
        )


def fetch_slot_baselines_for_admin(
    *,
    base_url: str,
    username: str | None,
    password: str | None,
    instrument_id: str,
    timeframe: str,
    slot_minute: int,
    limit_metrics: int = 32,
) -> dict[str, Any]:
    """Latest baseline row per metric for one slot (admin/debug)."""
    bu = base_url.rstrip("/")
    inst = _escape_ch_string(instrument_id.strip())
    tf = _escape_ch_string(timeframe.strip().lower())
    slot = max(0, min(1439, int(slot_minute)))
    lim = max(1, min(int(limit_metrics), 64))
    q_max = (
        "SELECT max(computed_at) AS m "
        "FROM signal_engine.historical_baseline_slot_stats "
        "FORMAT JSONEachRow"
    )
    auth = None
    if username and password:
        auth = (username.strip(), password)
    with httpx.Client(timeout=30.0, auth=auth) as client:
        r1 = client.post(bu, content=q_max.encode("utf-8"))
        r1.raise_for_status()
        raw = r1.text.strip()
        if not raw:
            return {"error": "no baseline batches in ClickHouse"}
        ts = json.loads(raw.split("\n")[0]).get("m")
        if ts is None:
            return {"error": "no baseline batches in ClickHouse"}
        ts_str = str(ts).replace("T", " ")[:19]
        q = (
            "SELECT metric, median, p90, p95, p99, sample_days "
            "FROM signal_engine.historical_baseline_slot_stats "
            f"WHERE computed_at = toDateTime('{_escape_ch_string(ts_str)}', 'UTC') "
            f"AND instrument_id = '{inst}' "
            f"AND timeframe = '{tf}' "
            f"AND slot_minute = {slot} "
            f"ORDER BY metric LIMIT {lim} FORMAT JSONEachRow"
        )
        r2 = client.post(bu, content=q.encode("utf-8"))
        r2.raise_for_status()
        body_text = r2.text
    rows: list[dict[str, Any]] = []
    for line in body_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return {
        "computed_at": ts_str,
        "instrument_id": instrument_id.strip(),
        "timeframe": timeframe.strip().lower(),
        "slot_minute": slot,
        "metrics": rows,
    }


def incremental_utc_day_range(*, n_calendar_days: int) -> tuple[date, date]:
    """Inclusive UTC calendar dates for the last ``n_calendar_days`` full days before today."""
    n = max(1, int(n_calendar_days))
    today = datetime.now(timezone.utc).date()
    end_d = today - timedelta(days=1)
    start_d = end_d - timedelta(days=n - 1)
    return start_d, end_d


def _decoded_trades_between_dates(start_d: date, end_d: date) -> str:
    s = _escape_ch_string(start_d.isoformat())
    e = _escape_ch_string(end_d.isoformat())
    return f"""
    (
        SELECT
            instrument_id,
            source_time,
            JSONExtractFloat(payload_json, 'quantity') AS qty,
            toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'units'), 0))
                + toFloat64(coalesce(JSONExtractInt(payload_json, 'price', 'nano'), 0))
                    / 1000000000. AS px
        FROM signal_engine.market_raw_events
        WHERE event_type = 'trade'
          AND JSONHas(payload_json, 'quantity')
          AND JSONHas(payload_json, 'price')
          AND toDate(toDateTime(source_time, 'UTC')) >= toDate('{s}')
          AND toDate(toDateTime(source_time, 'UTC')) <= toDate('{e}')
    )
    """


def _delete_trade_slot_for_day_range(start_d: date, end_d: date) -> str:
    s = _escape_ch_string(start_d.isoformat())
    e = _escape_ch_string(end_d.isoformat())
    return (
        "ALTER TABLE signal_engine.trade_slot_daily "
        "DELETE WHERE trading_day >= toDate('"
        f"{s}"
        "') AND trading_day <= toDate('"
        f"{e}"
        "') SETTINGS mutations_sync = 1"
    )


def _insert_trade_slot_for_tf_from_raw(
    *,
    tf: str,
    start_fn: str,
    slot_expr: str,
    decoded_subquery: str,
) -> str:
    return f"""
    INSERT INTO signal_engine.trade_slot_daily
    SELECT
        toDate(bucket, 'UTC') AS trading_day,
        instrument_id,
        '{tf}' AS timeframe,
        {slot_expr} AS slot_minute,
        sum(qty) AS sum_qty,
        toUInt64(count()) AS n_trades,
        sum(qty * px) AS sum_pv,
        argMin(px, source_time) AS open_px,
        max(px) AS max_px,
        min(px) AS min_px
    FROM
    (
        SELECT
            instrument_id,
            source_time,
            qty,
            px,
            {start_fn}(toDateTime(source_time, 'UTC')) AS bucket
        FROM {decoded_subquery} AS decoded
        WHERE qty > 0 AND px > 0 AND isFinite(qty) AND isFinite(px)
    ) AS t
    GROUP BY trading_day, instrument_id, slot_minute
    """


def _seed_trade_slot_from_feature_bars(*, tf: str, bar_table: str, slot_sql: str) -> str:
    return f"""
    INSERT INTO signal_engine.trade_slot_daily
    SELECT
        toDate(bucket, 'UTC') AS trading_day,
        instrument_id,
        '{tf}' AS timeframe,
        {slot_sql} AS slot_minute,
        sum_qty,
        n_trades,
        sum_pv,
        if(sum_qty > 0, sum_pv / sum_qty, 0.) AS open_px,
        if(sum_qty > 0, sum_pv / sum_qty, 0.) AS max_px,
        if(sum_qty > 0, sum_pv / sum_qty, 0.) AS min_px
    FROM
    (
        SELECT
            instrument_id,
            bucket,
            sum(sum_qty) AS sum_qty,
            sum(n_trades) AS n_trades,
            sum(sum_pv) AS sum_pv
        FROM signal_engine.{bar_table}
        GROUP BY instrument_id, bucket
    ) AS g
    WHERE sum_qty > 0 AND isFinite(sum_qty) AND isFinite(sum_pv)
    """


def _seed_statements_from_features() -> list[str]:
    return [
        _seed_trade_slot_from_feature_bars(
            tf="1m",
            bar_table="features_trade_bar_1m",
            slot_sql="toUInt16(toHour(bucket) * 60 + toMinute(bucket))",
        ),
        _seed_trade_slot_from_feature_bars(
            tf="5m",
            bar_table="features_trade_bar_5m",
            slot_sql=(
                "toUInt16(intDiv(toHour(bucket) * 60 + toMinute(bucket), 5) * 5)"
            ),
        ),
        _seed_trade_slot_from_feature_bars(
            tf="15m",
            bar_table="features_trade_bar_15m",
            slot_sql=(
                "toUInt16(intDiv(toHour(bucket) * 60 + toMinute(bucket), 15) * 15)"
            ),
        ),
    ]


def _trade_slot_daily_row_count(base_url: str, auth: tuple[str, str] | None) -> int:
    bu = base_url.rstrip("/")
    q = "SELECT count() AS c FROM signal_engine.trade_slot_daily FORMAT JSONEachRow"
    with httpx.Client(timeout=60.0, auth=auth) as client:
        r = client.post(bu, content=q.encode("utf-8"))
        r.raise_for_status()
        raw = r.text.strip()
    if not raw:
        return 0
    try:
        return int(json.loads(raw.split("\n")[0]).get("c", 0))
    except (json.JSONDecodeError, TypeError, ValueError):
        return 0


def _insert_slot_stat(metric_expr: str, metric_name: str, lb: int, where_extra: str) -> str:
    w = f"trading_day >= toDate(now('UTC')) - toIntervalDay({lb})"
    if where_extra.strip():
        w = f"({w}) AND ({where_extra})"
    return f"""
    INSERT INTO signal_engine.historical_baseline_slot_stats
    WITH base AS
    (
        SELECT
            instrument_id,
            timeframe,
            slot_minute,
            CAST({metric_expr} AS Float64) AS v
        FROM signal_engine.trade_slot_daily
        WHERE {w}
    )
    SELECT
        now('UTC') AS computed_at,
        instrument_id,
        timeframe,
        slot_minute,
        '{metric_name}' AS metric,
        quantileExact(0.5)(v) AS median,
        quantileExact(0.9)(v) AS p90,
        quantileExact(0.95)(v) AS p95,
        quantileExact(0.99)(v) AS p99,
        toUInt32(count()) AS sample_days
    FROM base
    WHERE isFinite(v)
    GROUP BY instrument_id, timeframe, slot_minute
    """


def run_historical_baseline_recalc(
    *,
    base_url: str,
    username: str | None,
    password: str | None,
    lookback_days: int = 35,
    incremental_days: int = 2,
    seed_trade_slot_if_empty: bool = True,
    force_truncate_and_seed_from_features: bool = False,
) -> None:
    """Refresh ``trade_slot_daily`` incrementally from raw, seed long history from bars if needed.

    - ``market_raw_events`` keeps a short TTL; ``trade_slot_daily`` holds long-horizon slot facts.
    - Default: if ``trade_slot_daily`` is empty, INSERT from ``features_trade_bar_*`` (VWAP-only OHLC).
    - Each run: DELETE last ``incremental_days`` UTC calendar days in ``trade_slot_daily``, then
      re-insert those days from ``market_raw_events`` (full tick OHLC), then append a new
      ``historical_baseline_slot_stats`` batch from ``trade_slot_daily`` over ``lookback_days``.
    """
    bu = base_url.rstrip("/")
    auth = None
    if username and password:
        auth = (username.strip(), password)
    lb = max(7, min(int(lookback_days), 120))
    inc = max(1, min(int(incremental_days), 30))
    start_d, end_d = incremental_utc_day_range(n_calendar_days=inc)

    statements: list[str] = []
    if force_truncate_and_seed_from_features:
        statements.append("TRUNCATE TABLE signal_engine.trade_slot_daily")
        statements.extend(_seed_statements_from_features())
    elif seed_trade_slot_if_empty and _trade_slot_daily_row_count(bu, auth) == 0:
        logger.info(
            "trade_slot_daily is empty: seeding from features_trade_bar_* (long history, VWAP OHLC)"
        )
        statements.extend(_seed_statements_from_features())

    decoded_range = _decoded_trades_between_dates(start_d, end_d)
    statements.append(_delete_trade_slot_for_day_range(start_d, end_d))
    statements.extend(
        [
            _insert_trade_slot_for_tf_from_raw(
                tf="1m",
                start_fn="toStartOfMinute",
                slot_expr="toUInt16(toHour(bucket) * 60 + toMinute(bucket))",
                decoded_subquery=decoded_range,
            ),
            _insert_trade_slot_for_tf_from_raw(
                tf="5m",
                start_fn="toStartOfFiveMinutes",
                slot_expr=(
                    "toUInt16(intDiv(toHour(bucket) * 60 + toMinute(bucket), 5) * 5)"
                ),
                decoded_subquery=decoded_range,
            ),
            _insert_trade_slot_for_tf_from_raw(
                tf="15m",
                start_fn="toStartOfFifteenMinutes",
                slot_expr=(
                    "toUInt16(intDiv(toHour(bucket) * 60 + toMinute(bucket), 15) * 15)"
                ),
                decoded_subquery=decoded_range,
            ),
        ]
    )
    statements.extend(
        [
            _insert_slot_stat("sum_qty", "volume_qty", lb, ""),
            _insert_slot_stat("toFloat64(n_trades)", "trade_count", lb, ""),
            _insert_slot_stat(
                "toFloat64(n_trades) / if(timeframe = '1m', 60, "
                "if(timeframe = '5m', 300, 900))",
                "trade_rate",
                lb,
                "",
            ),
            _insert_slot_stat(
                "abs((sum_pv / sum_qty - open_px) / open_px * 10000.0)",
                "return_bps_abs",
                lb,
                "timeframe IN ('5m', '15m') AND sum_qty > 0 AND open_px > 0",
            ),
            _insert_slot_stat(
                "(max_px - min_px) / ((max_px + min_px) / 2.0) * 10000.0",
                "range_abs_bps",
                lb,
                "timeframe IN ('5m', '15m') AND max_px > min_px AND (max_px + min_px) > 0",
            ),
        ]
    )

    with httpx.Client(timeout=600.0, auth=auth) as client:
        for sql in statements:
            resp = client.post(bu, content=sql.strip().encode("utf-8"))
            resp.raise_for_status()

    logger.info(
        "Historical baseline recalc finished (lookback_days=%s, incremental=%s..%s, utc=%s)",
        lb,
        start_d.isoformat(),
        end_d.isoformat(),
        datetime.now(timezone.utc).isoformat(),
    )
