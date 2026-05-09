#!/usr/bin/env python3
"""
Lightweight local analytics on a Parquet/CSV export of ``features_trade_bar_*`` — keeps RAM
on a small machine compared to repeated heavy CH queries.

  clickhouse-client --host localhost --query \\
    "SELECT * FROM signal_engine.vw_trade_bar_1m_vwap FORMAT Parquet" \\
    > /tmp/bars.parquet

  python scripts/duckdb_feature_smoke.py /tmp/bars.parquet
"""

from __future__ import annotations

import argparse
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Parquet or CSV path")
    args = parser.parse_args()
    try:
        import duckdb
    except ImportError:
        print("Install duckdb: pip install duckdb", file=sys.stderr)
        return 2

    con = duckdb.connect(database=":memory:")
    rel = args.path.lower()
    if rel.endswith(".parquet"):
        sql = f"SELECT instrument_id, count(*) n, avg(vwap) avg_vwap FROM read_parquet('{args.path}') GROUP BY 1 ORDER BY n DESC LIMIT 20"
    else:
        sql = f"SELECT instrument_id, count(*) n, avg(vwap) avg_vwap FROM read_csv_auto('{args.path}') GROUP BY 1 ORDER BY n DESC LIMIT 20"
    print(con.execute(sql).fetchdf().to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
