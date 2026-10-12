#!/usr/bin/env python3
"""Collect local T-Invest order book snapshots for future price-signal research.

T-Invest does not provide the historical order book through the candle cache used
by the first research track. This script collects forward-looking local snapshots
so later experiments can test spread, depth, imbalance, and liquidity regimes
around detector events. It never persists the token, account identifiers, FIGIs,
or instrument UIDs.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_cache_tinvest_candles import api_post, resolve_instruments  # noqa: E402
from research_price_prediction_lib import (  # noqa: E402
    DEFAULT_RESEARCH_TICKERS,
    ORDERBOOK_CACHE_FIELDS,
    UTC,
    build_orderbook_cache_manifest,
    fingerprint_records,
    load_env_value,
    manifest_path,
    orderbook_partition_path,
    orderbook_rows_for_storage,
    orderbook_snapshot_from_levels,
    read_table,
    redact_diagnostic,
    require_duckdb,
    valid_partition,
    write_json,
    write_table,
)


def _parse_tickers(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-app-name": "investment-signals-orderbook-research-cache",
    }


def fetch_orderbook_snapshot(
    client: httpx.Client,
    *,
    ticker: str,
    instrument_uid: str,
    depth: int,
    attempts: int,
) -> dict[str, Any] | None:
    observed_at = datetime.now(UTC)
    body = api_post(
        client,
        "MarketDataService/GetOrderBook",
        {
            "instrumentId": instrument_uid,
            "depth": int(depth),
        },
        attempts=attempts,
    )
    snapshot = orderbook_snapshot_from_levels(
        ticker=ticker,
        at=observed_at,
        bids=body.get("bids") or [],
        asks=body.get("asks") or [],
        depth=depth,
        is_consistent=bool(body.get("isConsistent", True)),
    )
    if snapshot is None:
        return None
    return orderbook_rows_for_storage((snapshot,))[0]


def _existing_rows(path: Path) -> list[dict[str, Any]]:
    if not valid_partition(path):
        return []
    return read_table(path)


def _row_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    at = row.get("at")
    normalized_at = (
        at.astimezone(UTC).isoformat()
        if isinstance(at, datetime)
        else datetime.fromisoformat(str(at).replace("Z", "+00:00")).astimezone(UTC).isoformat()
    )
    return (str(row.get("ticker")), normalized_at, str(row.get("depth")))


def _write_partitions(cache_dir: Path, rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        at = datetime.fromisoformat(str(row["at"]).replace("Z", "+00:00")).astimezone(UTC)
        grouped[(str(row["ticker"]), at.date().isoformat())].append(dict(row))
    row_counts: dict[str, int] = {}
    for (ticker, day), new_rows in grouped.items():
        target = orderbook_partition_path(cache_dir, ticker, datetime.fromisoformat(day).date())
        existing = _existing_rows(target)
        by_key = {_row_key(row): dict(row) for row in existing}
        for row in new_rows:
            by_key[_row_key(row)] = dict(row)
        merged = sorted(by_key.values(), key=lambda item: (str(item["ticker"]), str(item["at"]), str(item["depth"])))
        write_table(target, merged, fields=ORDERBOOK_CACHE_FIELDS)
        row_counts[f"{ticker}/{day}"] = len(merged)
    return row_counts


def _progress_path(cache_dir: Path) -> Path:
    return cache_dir / "collection-progress.json"


def _collection_progress_payload(
    *,
    status: str,
    cache_dir: Path,
    tickers: Sequence[str],
    depth: int,
    started_at: datetime,
    sample_index: int,
    samples: int,
    rows_collected: int,
    rows_flushed: int,
    unflushed_rows: int,
    failures: Sequence[Mapping[str, Any]],
    row_counts: Mapping[str, int],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    completed_samples = max(0, min(sample_index, samples))
    return {
        "schema_version": 1,
        "kind": "tinvest_research_orderbook_collection_progress",
        "status": status,
        "updated_at": now.isoformat(),
        "cache_dir": str(cache_dir),
        "scope": {
            "tickers": list(tickers),
            "depth": int(depth),
            "started_at": started_at.astimezone(UTC).isoformat(),
        },
        "progress": {
            "completed_samples": completed_samples,
            "target_samples": int(samples),
            "completed_share": completed_samples / samples if samples > 0 else 0.0,
            "rows_collected": int(rows_collected),
            "rows_flushed": int(rows_flushed),
            "unflushed_rows": int(unflushed_rows),
            "failures": len(failures),
            "rows_by_partition": dict(sorted(row_counts.items())),
        },
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
    }


def _write_progress(
    *,
    cache_dir: Path,
    status: str,
    tickers: Sequence[str],
    depth: int,
    started_at: datetime,
    sample_index: int,
    samples: int,
    rows_collected: int,
    rows_flushed: int,
    unflushed_rows: int,
    failures: Sequence[Mapping[str, Any]],
    row_counts: Mapping[str, int],
) -> None:
    write_json(
        _progress_path(cache_dir),
        _collection_progress_payload(
            status=status,
            cache_dir=cache_dir,
            tickers=tickers,
            depth=depth,
            started_at=started_at,
            sample_index=sample_index,
            samples=samples,
            rows_collected=rows_collected,
            rows_flushed=rows_flushed,
            unflushed_rows=unflushed_rows,
            failures=failures,
            row_counts=row_counts,
        ),
    )


def collect_orderbook_cache(
    *,
    env_file: Path,
    cache_dir: Path,
    tickers: tuple[str, ...],
    depth: int,
    samples: int,
    interval_seconds: float,
    request_timeout: float,
    request_attempts: int,
    ca_cert: Path | None,
    insecure_skip_tls_verify: bool = False,
    flush_every_samples: int = 20,
) -> dict[str, Any]:
    require_duckdb()
    token = load_env_value(env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    if insecure_skip_tls_verify:
        verify = False
    timeout = httpx.Timeout(
        request_timeout,
        connect=min(request_timeout, 10.0),
        read=request_timeout,
        write=min(request_timeout, 10.0),
        pool=min(request_timeout, 10.0),
    )
    failures: list[dict[str, Any]] = []
    collected: list[dict[str, Any]] = []
    pending_flush: list[dict[str, Any]] = []
    row_counts: dict[str, int] = {}
    rows_flushed = 0
    total_samples = max(1, samples)
    flush_every = max(1, int(flush_every_samples)) if flush_every_samples > 0 else 0
    started_at = datetime.now(UTC)
    with httpx.Client(headers=_headers(token), timeout=timeout, verify=verify) as client:
        instruments = resolve_instruments(client, tickers, attempts=request_attempts)
        _write_progress(
            cache_dir=cache_dir,
            status="running",
            tickers=tickers,
            depth=depth,
            started_at=started_at,
            sample_index=0,
            samples=total_samples,
            rows_collected=0,
            rows_flushed=0,
            unflushed_rows=0,
            failures=failures,
            row_counts=row_counts,
        )
        for sample_index in range(total_samples):
            for ticker in tickers:
                try:
                    row = fetch_orderbook_snapshot(
                        client,
                        ticker=ticker,
                        instrument_uid=instruments[ticker],
                        depth=depth,
                        attempts=request_attempts,
                    )
                    if row is not None:
                        collected.append(row)
                        pending_flush.append(row)
                except Exception as exc:  # noqa: BLE001 - research diagnostics need redacted errors
                    failures.append(
                        {
                            "ticker": ticker,
                            "sample": sample_index,
                            "reason_code": "tinvest_orderbook_snapshot_failed",
                            "message": redact_diagnostic(exc),
                        }
                    )
            completed_samples = sample_index + 1
            if flush_every and completed_samples % flush_every == 0 and pending_flush:
                row_counts.update(_write_partitions(cache_dir, pending_flush))
                rows_flushed += len(pending_flush)
                pending_flush.clear()
            _write_progress(
                cache_dir=cache_dir,
                status="running",
                tickers=tickers,
                depth=depth,
                started_at=started_at,
                sample_index=completed_samples,
                samples=total_samples,
                rows_collected=len(collected),
                rows_flushed=rows_flushed,
                unflushed_rows=len(pending_flush),
                failures=failures,
                row_counts=row_counts,
            )
            if sample_index + 1 < total_samples and interval_seconds > 0:
                time.sleep(interval_seconds)
    if pending_flush:
        row_counts.update(_write_partitions(cache_dir, pending_flush))
        rows_flushed += len(pending_flush)
        pending_flush.clear()
    manifest_rows: list[dict[str, Any]] = []
    for ticker_day in row_counts:
        ticker, day = ticker_day.split("/", 1)
        manifest_rows.extend(read_table(orderbook_partition_path(cache_dir, ticker, datetime.fromisoformat(day).date())))
    finished_at = datetime.now(UTC)
    manifest = build_orderbook_cache_manifest(
        tickers=tickers,
        start_at=started_at,
        end_at=finished_at,
        depth=depth,
        row_counts=row_counts,
        content_fingerprint=fingerprint_records(manifest_rows),
        failures=failures,
    )
    write_json(manifest_path(cache_dir), manifest)
    _write_progress(
        cache_dir=cache_dir,
        status="completed",
        tickers=tickers,
        depth=depth,
        started_at=started_at,
        sample_index=total_samples,
        samples=total_samples,
        rows_collected=len(collected),
        rows_flushed=rows_flushed,
        unflushed_rows=0,
        failures=failures,
        row_counts=row_counts,
    )
    return {
        "status": "ok",
        "cache_dir": str(cache_dir),
        "rows_collected": len(collected),
        "rows_flushed": rows_flushed,
        "partitions_written": len(row_counts),
        "failures": len(failures),
        "manifest": str(manifest_path(cache_dir)),
        "progress": str(_progress_path(cache_dir)),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-collect-tinvest-orderbook-snapshots")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--tickers", type=_parse_tickers, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--samples", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=3)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument(
        "--flush-every-samples",
        type=int,
        default=20,
        help="Flush collected snapshots to parquet every N samples; use 0 to write only at the end.",
    )
    parser.add_argument(
        "--insecure-skip-tls-verify",
        action="store_true",
        help="Only for local research behind intercepting proxies; TLS verification stays enabled by default.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = collect_orderbook_cache(
        env_file=args.env_file,
        cache_dir=args.cache_dir,
        tickers=args.tickers,
        depth=args.depth,
        samples=args.samples,
        interval_seconds=args.interval_seconds,
        request_timeout=args.request_timeout,
        request_attempts=args.request_attempts,
        ca_cert=args.ca_cert,
        insecure_skip_tls_verify=args.insecure_skip_tls_verify,
        flush_every_samples=args.flush_every_samples,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
