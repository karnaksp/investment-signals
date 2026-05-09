#!/usr/bin/env python3
"""
Replay ``trade`` rows from ClickHouse through ``SignalDetector`` (read-only).

Crude summary: count signals and optional next-tick direction check — replace with
DuckDB / proper forward windows for production labelling.

  pip install -e ".[backtest]"
  set CLICKHOUSE_HTTP_URL=http://localhost:38123
  python scripts/backtest_from_clickhouse.py --instrument SBER_TQBR --hours 24
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.config import load_detector_config
from tinvest_signal_engine.detector_core import SignalDetector
from tinvest_signal_engine.models import NormalizedEvent
from tinvest_signal_engine.serialization import parse_timestamp


def _http_query(base: str, sql: str) -> str:
    url = f"{base.rstrip('/')}/?{urlencode({'query': sql + ' FORMAT JSONEachRow'})}"
    req = Request(url, method="GET")
    with urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8")


def _fetch_trades(base: str, instrument_id: str, since: datetime) -> list[dict[str, Any]]:
    since_s = since.strftime("%Y-%m-%d %H:%M:%S")
    safe_id = instrument_id.replace("'", "''")
    sql = (
        "SELECT event_id, instrument_id, source_time, payload_json "
        "FROM signal_engine.market_raw_events "
        f"WHERE event_type = 'trade' AND instrument_id = '{safe_id}' "
        f"AND source_time >= toDateTime64('{since_s}', 3, 'UTC') "
        "ORDER BY source_time ASC LIMIT 500000"
    )
    raw = _http_query(base, sql)
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _row_to_event(row: dict[str, Any], *, ticker: str, class_code: str, alias: str) -> NormalizedEvent:
    payload = json.loads(row["payload_json"])
    st = parse_timestamp(row["source_time"])
    return NormalizedEvent(
        event_id=str(row["event_id"]),
        event_type="trade",
        instrument_id=str(row["instrument_id"]),
        ticker=ticker,
        class_code=class_code,
        alias=alias,
        figi="",
        uid="",
        lot=1,
        source_time=st,
        received_at=st,
        payload=payload,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_HTTP_URL"))
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--detector-yaml", default="conf/detectors.yaml")
    args = parser.parse_args()
    base = args.clickhouse_url or os.getenv("CLICKHOUSE_HTTP_URL")
    if not base:
        print("Set CLICKHOUSE_HTTP_URL or pass --clickhouse-url", file=sys.stderr)
        return 2

    since = (datetime.now(tz=timezone.utc) - timedelta(hours=args.hours)).astimezone(
        timezone.utc
    )
    rows = _fetch_trades(base, args.instrument, since)
    if not rows:
        print("No trades in window.", file=sys.stderr)
        return 1

    parts = args.instrument.split("_", 1)
    ticker, class_code = parts[0], parts[1] if len(parts) > 1 else ("", "")

    loaded = load_detector_config(Path(args.detector_yaml), None)
    detector = SignalDetector(
        loaded.default,
        loaded.per_instrument,
        lead_lag_pairs=loaded.lead_lag_pairs,
    )

    events = [
        _row_to_event(r, ticker=ticker, class_code=class_code, alias=args.instrument.lower())
        for r in rows
    ]

    signal_count = 0
    for ev in events:
        signal_count += len(detector.process(ev))

    print(
        json.dumps(
            {
                "instrument": args.instrument,
                "trades_replayed": len(events),
                "signals_emitted": signal_count,
                "note": "Add DuckDB (optional) to join signals with vw_trade_bar_* forward windows.",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
