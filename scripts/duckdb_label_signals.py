#!/usr/bin/env python3
"""
Offline **signal quality** labelling: join exported detector signals with exported 1m VWAP bars.

**Time alignment:**
- Signal time → ``anchor_bucket`` = start of that UTC minute.
- ``anchor_vwap`` = VWAP in ``bars`` for ``(instrument_id, anchor_bucket)``.
- ``forward_vwap`` = VWAP at ``anchor_bucket + N * INTERVAL '1 minute'``.

**Directional hit/miss (strict):**
- Types matching ``combo_long``, ``*_long``, ``long`` → hit if ``forward_vwap > anchor_vwap``.
- Types matching ``combo_short``, ``*_short``, ``short`` → hit if ``forward_vwap < anchor_vwap``.
- All other ``signal_type`` values → ``na_non_directional``.
- Missing bar → ``missing_bar``.

Inputs: Parquet or CSV. Signals need ``instrument_id``, ``signal_type``, and a timestamp column
(``detected_at`` | ``signal_time`` | ``ts`` | ``time`` | ``created_at`` or ``--signal-time-column``).

Bars need ``instrument_id``, ``bucket``, ``vwap`` (e.g. export from ``signal_engine.vw_trade_bar_1m_vwap``).

``--forward-bars`` may be a single integer or comma-separated horizons, e.g. ``1,5,15``.

Read-only; no trading.

Example::

  pip install -e ".[backtest]"
  python scripts/duckdb_label_signals.py --signals sig.parquet --bars bars.parquet --forward-bars 1
  python scripts/duckdb_label_signals.py --signals sig.parquet --bars bars.parquet --forward-bars 1,5,15 --output var/accuracy/signal_accuracy.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _from_file_sql(path: Path, table: str) -> str:
    p = path.name.lower()
    lit = _sql_path(path)
    if p.endswith(".parquet"):
        return f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{lit}')"
    return f"CREATE TABLE {table} AS SELECT * FROM read_csv_auto('{lit}')"


def _parse_forward_bars(raw: str) -> list[int]:
    s = raw.strip()
    if not s:
        raise ValueError("empty --forward-bars")
    horizons: list[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        n = int(part)
        if n < 1:
            raise ValueError("each forward bar count must be >= 1")
        horizons.append(n)
    if not horizons:
        raise ValueError("no forward horizons parsed")
    seen: set[int] = set()
    unique: list[int] = []
    for n in horizons:
        if n not in seen:
            seen.add(n)
            unique.append(n)
    return unique


def _optional_text_expr(cols: list[str], col: str, default: str) -> str:
    if col in cols:
        return f"coalesce(nullif(CAST(\"{col}\" AS VARCHAR), ''), '{default}')"
    return f"'{default}'"


def _optional_double_expr(cols: list[str], col: str) -> str:
    if col in cols:
        return f"TRY_CAST(\"{col}\" AS DOUBLE)"
    return "NULL"


def _metric_rows(con: Any, view_name: str, group_col: str) -> list[dict[str, Any]]:
    rows = con.execute(
        f"""
        SELECT
          CAST({group_col} AS VARCHAR) AS bucket,
          count(*)::BIGINT AS signal_count,
          sum(CASE WHEN outcome = 'hit' THEN 1 ELSE 0 END)::BIGINT AS hits,
          sum(CASE WHEN outcome = 'miss' THEN 1 ELSE 0 END)::BIGINT AS misses,
          avg(forward_return_pct) AS avg_forward_return_pct,
          median(forward_return_pct) AS median_forward_return_pct
        FROM {view_name}
        GROUP BY 1
        ORDER BY signal_count DESC, bucket
        LIMIT 200
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for bucket, count, hits, misses, avg_ret, median_ret in rows:
        decided = int(hits or 0) + int(misses or 0)
        out.append(
            {
                group_col: bucket or "unknown",
                "signal_count": int(count or 0),
                "directional_hits": int(hits or 0),
                "directional_misses": int(misses or 0),
                "directional_decided": decided,
                "directional_hit_rate": (int(hits or 0) / decided) if decided else None,
                "avg_forward_return_pct": float(avg_ret) if avg_ret is not None else None,
                "median_forward_return_pct": (
                    float(median_ret) if median_ret is not None else None
                ),
            }
        )
    return out


def _summarize_horizon(con: Any, fb: int) -> dict[str, Any]:
    view_name = f"scored_h{fb}"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
    WITH joined AS (
      SELECT
        s.signal_type,
        s.instrument_id,
        s.ticker,
        s.delivery_status,
        s.quality_tier,
        s.sig_ts,
        date_trunc('minute', s.sig_ts) AS anchor_bucket,
        b0.vwap AS anchor_vwap,
        b1.vwap AS forward_vwap
      FROM sig s
      LEFT JOIN bar b0
        ON s.instrument_id = b0.instrument_id
       AND b0.bucket = date_trunc('minute', s.sig_ts)
      LEFT JOIN bar b1
        ON s.instrument_id = b1.instrument_id
       AND b1.bucket = date_trunc('minute', s.sig_ts)
         + (CAST({fb} AS INTEGER) * INTERVAL '1 minute')
    ),
    scored AS (
      SELECT
        signal_type,
        instrument_id,
        ticker,
        delivery_status,
        quality_tier,
        anchor_vwap,
        forward_vwap,
        CASE
          WHEN anchor_vwap IS NULL OR forward_vwap IS NULL THEN 'missing_bar'
          WHEN (
            regexp_matches(lower(signal_type), '.*combo_long.*')
            OR regexp_matches(lower(signal_type), '.*_long$')
            OR lower(signal_type) = 'long'
          ) AND forward_vwap > anchor_vwap THEN 'hit'
          WHEN (
            regexp_matches(lower(signal_type), '.*combo_long.*')
            OR regexp_matches(lower(signal_type), '.*_long$')
            OR lower(signal_type) = 'long'
          ) AND forward_vwap <= anchor_vwap THEN 'miss'
          WHEN (
            regexp_matches(lower(signal_type), '.*combo_short.*')
            OR regexp_matches(lower(signal_type), '.*_short$')
            OR lower(signal_type) = 'short'
          ) AND forward_vwap < anchor_vwap THEN 'hit'
          WHEN (
            regexp_matches(lower(signal_type), '.*combo_short.*')
            OR regexp_matches(lower(signal_type), '.*_short$')
            OR lower(signal_type) = 'short'
          ) AND forward_vwap >= anchor_vwap THEN 'miss'
          ELSE 'na_non_directional'
        END AS outcome,
        CASE
          WHEN anchor_vwap IS NULL OR forward_vwap IS NULL OR anchor_vwap = 0
            THEN NULL
          ELSE ((forward_vwap - anchor_vwap) / anchor_vwap) * 100.0
        END AS forward_return_pct
      FROM joined
    )
    SELECT *
    FROM scored
    """
    )

    query = f"""
    SELECT outcome, signal_type, count(*)::BIGINT AS n
    FROM {view_name}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    rows = con.execute(query).fetchall()
    block: dict[str, Any] = {
        "forward_bars": fb,
        "by_outcome_and_type": [
            {"outcome": r[0], "signal_type": r[1], "count": int(r[2])} for r in rows
        ],
    }
    hits = sum(int(r[2]) for r in rows if r[0] == "hit")
    misses = sum(int(r[2]) for r in rows if r[0] == "miss")
    denom = hits + misses
    block["directional_hit_rate"] = (hits / denom) if denom else None
    block["directional_hits"] = hits
    block["directional_misses"] = misses
    block["directional_decided"] = denom
    block["by_type"] = _metric_rows(con, view_name, "signal_type")
    block["by_ticker"] = _metric_rows(con, view_name, "ticker")
    block["by_quality_tier"] = _metric_rows(con, view_name, "quality_tier")
    block["by_delivery_status"] = _metric_rows(con, view_name, "delivery_status")
    return block


def main() -> int:
    parser = argparse.ArgumentParser(description="Label signals vs forward VWAP (DuckDB).")
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--bars", type=Path, required=True)
    parser.add_argument(
        "--forward-bars",
        type=str,
        default="1,5,15",
        help="Minutes forward (single int or comma list, e.g. 1,5,15)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path, e.g. var/accuracy/signal_accuracy.json",
    )
    parser.add_argument(
        "--signal-time-column",
        default=None,
        help="Column name for signal timestamp (default: auto-detect)",
    )
    args = parser.parse_args()

    if not args.signals.exists():
        print(f"Signals file not found: {args.signals}", file=sys.stderr)
        return 2
    if not args.bars.exists():
        print(f"Bars file not found: {args.bars}", file=sys.stderr)
        return 2
    try:
        horizons = _parse_forward_bars(args.forward_bars)
    except ValueError as exc:
        print(f"Invalid --forward-bars: {exc}", file=sys.stderr)
        return 2

    try:
        import duckdb
    except ImportError:
        print(
            "Install duckdb: pip install duckdb  (or pip install -e '.[backtest]')",
            file=sys.stderr,
        )
        return 2

    con = duckdb.connect(database=":memory:")
    con.execute(_from_file_sql(args.signals, "signals"))
    con.execute(_from_file_sql(args.bars, "bars"))

    cols = [r[0] for r in con.execute("DESCRIBE signals").fetchall()]
    time_candidates = (
        "detected_at",
        "signal_time",
        "ts",
        "time",
        "created_at",
    )
    if args.signal_time_column:
        if args.signal_time_column not in cols:
            print(
                f"Column {args.signal_time_column!r} not in signals. Columns: {cols}",
                file=sys.stderr,
            )
            return 2
        ts_col = args.signal_time_column
    else:
        ts_col = next((c for c in time_candidates if c in cols), None)
        if ts_col is None:
            print(
                "Could not infer signal time column. "
                f"Columns: {cols}. Pass --signal-time-column.",
                file=sys.stderr,
            )
            return 2

    for req in ("instrument_id", "signal_type"):
        if req not in cols:
            print(f"Missing {req!r} in signals. Columns: {cols}", file=sys.stderr)
            return 2

    bar_cols = [r[0] for r in con.execute("DESCRIBE bars").fetchall()]
    for req in ("instrument_id", "bucket", "vwap"):
        if req not in bar_cols:
            print(f"Missing {req!r} in bars. Columns: {bar_cols}", file=sys.stderr)
            return 2

    ticker_expr = _optional_text_expr(cols, "ticker", "")
    delivery_expr = _optional_text_expr(cols, "delivery_status", "unknown")
    quality_expr = _optional_double_expr(cols, "quality_score")
    con.execute(
        f"""
        CREATE OR REPLACE VIEW sig AS
        SELECT
          instrument_id,
          CASE WHEN {ticker_expr} = '' THEN instrument_id ELSE {ticker_expr} END AS ticker,
          signal_type,
          {delivery_expr} AS delivery_status,
          CASE
            WHEN {quality_expr} IS NULL THEN 'unknown'
            WHEN {quality_expr} >= 80 THEN 'high'
            WHEN {quality_expr} >= 60 THEN 'medium'
            ELSE 'low'
          END AS quality_tier,
          CAST("{ts_col}" AS TIMESTAMP) AS sig_ts
        FROM signals
        """
    )
    con.execute(
        """
        CREATE OR REPLACE VIEW bar AS
        SELECT
          instrument_id,
          CAST(bucket AS TIMESTAMP) AS bucket,
          CAST(vwap AS DOUBLE) AS vwap
        FROM bars
        """
    )

    if len(horizons) == 1:
        summary = _summarize_horizon(con, horizons[0])
        summary["signal_time_column"] = ts_col
        text = json.dumps(summary, indent=2)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text + "\n", encoding="utf-8")
        print(text)
        return 0

    by_h: dict[str, Any] = {}
    for fb in horizons:
        by_h[str(fb)] = _summarize_horizon(con, fb)

    summary_multi: dict[str, Any] = {
        "forward_bars": horizons,
        "signal_time_column": ts_col,
        "by_horizon": by_h,
    }
    text = json.dumps(summary_multi, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
