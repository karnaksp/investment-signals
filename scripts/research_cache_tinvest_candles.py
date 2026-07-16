#!/usr/bin/env python3
"""Build a reusable local Parquet cache of T-Invest one-minute candles.

The cache is owner-local research input. It never persists the T-Invest token,
account identifiers, instrument UIDs, or FIGIs. Existing valid partitions are
kept and skipped so repeated experiments do not repeatedly call the API.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    DEFAULT_RESEARCH_TICKERS,
    MOSCOW,
    UTC,
    ResearchCandle,
    build_cache_manifest,
    candle_rows_for_storage,
    fingerprint_records,
    load_env_value,
    manifest_path,
    partition_path,
    quotation,
    redact_diagnostic,
    require_duckdb,
    study_window,
    valid_partition,
    write_json,
    write_table,
)


API_ROOT = "https://invest-public-api.tbank.ru/rest/"
API_SERVICE = "tinkoff.public.invest.api.contract.v1"


def api_post(
    client: httpx.Client,
    method: str,
    payload: Mapping[str, Any],
    *,
    attempts: int,
) -> Mapping[str, Any]:
    url = f"{API_ROOT}{API_SERVICE}.{method}"
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            response = client.post(url, json=payload)
            if response.status_code == 200:
                body = response.json()
                if not isinstance(body, Mapping):
                    raise RuntimeError("T-Invest response is not an object")
                return body
            if response.status_code not in {429, 500, 502, 503, 504}:
                try:
                    error = response.json()
                except ValueError:
                    error = {}
                raise RuntimeError(
                    f"T-Invest {method} failed with HTTP {response.status_code}; "
                    f"code={str(error.get('code', 'unknown'))[:80]}; "
                    f"message={str(error.get('message', 'unspecified'))[:240]}"
                )
        except (httpx.HTTPError, RuntimeError) as exc:
            last_error = exc
            if isinstance(exc, RuntimeError):
                raise
        if attempt + 1 < attempts:
            time.sleep(min(20.0, 0.75 * (2**attempt)))
    raise RuntimeError(f"T-Invest {method} failed: {redact_diagnostic(last_error)}")


def resolve_instruments(
    client: httpx.Client,
    tickers: Sequence[str],
    *,
    attempts: int,
) -> dict[str, str]:
    result: dict[str, str] = {}
    for ticker in tickers:
        body = api_post(
            client,
            "InstrumentsService/FindInstrument",
            {
                "query": ticker,
                "instrumentKind": "INSTRUMENT_TYPE_SHARE",
                "apiTradeAvailableFlag": True,
            },
            attempts=attempts,
        )
        matches = [
            item
            for item in body.get("instruments", [])
            if isinstance(item, Mapping)
            and item.get("ticker") == ticker
            and item.get("classCode") == "TQBR"
            and item.get("uid")
        ]
        if len(matches) != 1:
            raise RuntimeError(f"Expected one canonical TQBR share for {ticker}, got {len(matches)}")
        result[ticker] = str(matches[0]["uid"])
    return result


def fetch_day_candles(
    client: httpx.Client,
    *,
    ticker: str,
    instrument_uid: str,
    day: date,
    attempts: int,
) -> tuple[ResearchCandle, ...]:
    start = datetime.combine(day, datetime_time(10, 0), tzinfo=MOSCOW).astimezone(UTC)
    end = datetime.combine(day, datetime_time(19, 0), tzinfo=MOSCOW).astimezone(UTC)
    body = api_post(
        client,
        "MarketDataService/GetCandles",
        {
            "from": start.isoformat().replace("+00:00", "Z"),
            "to": end.isoformat().replace("+00:00", "Z"),
            "interval": "CANDLE_INTERVAL_1_MIN",
            "instrumentId": instrument_uid,
            "candleSourceType": "CANDLE_SOURCE_EXCHANGE",
        },
        attempts=attempts,
    )
    rows: list[ResearchCandle] = []
    for item in body.get("candles", []):
        rows.append(
            ResearchCandle(
                ticker=ticker,
                at=datetime.fromisoformat(str(item["time"]).replace("Z", "+00:00")).astimezone(UTC),
                open=quotation(item.get("open")),
                high=quotation(item.get("high")),
                low=quotation(item.get("low")),
                close=quotation(item.get("close")),
                volume=float(item.get("volume", 0) or 0),
                complete=bool(item.get("isComplete", False)),
            )
        )
    return tuple(rows)


def run_cache(
    *,
    env_file: Path,
    cache_dir: Path,
    tickers: tuple[str, ...],
    start_day: date,
    end_day: date,
    request_timeout: float,
    request_attempts: int,
    request_interval: float,
    ca_cert: Path | None,
) -> dict[str, Any]:
    require_duckdb()
    token = load_env_value(env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-app-name": "investment-signals-price-research-cache",
    }
    timeout = httpx.Timeout(
        request_timeout,
        connect=min(request_timeout, 10.0),
        read=request_timeout,
        write=min(request_timeout, 10.0),
        pool=min(request_timeout, 10.0),
    )
    row_counts: dict[str, int] = {}
    all_records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped = 0
    with httpx.Client(headers=headers, timeout=timeout, verify=verify) as client:
        instruments = resolve_instruments(client, tickers, attempts=request_attempts)
        day = start_day
        while day <= end_day:
            for ticker in tickers:
                target = partition_path(cache_dir, ticker, day)
                key = f"{ticker}/{day.isoformat()}"
                if valid_partition(target):
                    skipped += 1
                    rows = read_existing_records(target)
                    row_counts[key] = len(rows)
                    all_records.extend(rows)
                    continue
                try:
                    candles = fetch_day_candles(
                        client,
                        ticker=ticker,
                        instrument_uid=instruments[ticker],
                        day=day,
                        attempts=request_attempts,
                    )
                    records = candle_rows_for_storage(candles)
                    write_table(target, records)
                    row_counts[key] = len(records)
                    all_records.extend(records)
                except Exception as exc:
                    failures.append(
                        {
                            "ticker": ticker,
                            "date": day.isoformat(),
                            "reason_code": "tinvest_candle_partition_failed",
                            "diagnostic": redact_diagnostic(exc),
                        }
                    )
                time.sleep(request_interval)
            day += timedelta(days=1)
    manifest = build_cache_manifest(
        tickers=tickers,
        start_day=start_day,
        end_day=end_day,
        row_counts=row_counts,
        content_fingerprint=fingerprint_records(all_records),
        failures=failures,
    )
    manifest["quality"]["skipped_existing_partitions"] = skipped
    write_json(manifest_path(cache_dir), manifest)
    if failures:
        write_json(cache_dir / "failure-summary.json", {"failures": failures})
    return manifest


def read_existing_records(path: Path) -> list[dict[str, Any]]:
    from research_price_prediction_lib import read_table

    return read_table(path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-cache-tinvest-candles")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--tickers", default=",".join(DEFAULT_RESEARCH_TICKERS))
    parser.add_argument("--calendar-days", type=int, default=180)
    parser.add_argument("--end-day", type=date.fromisoformat)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--request-interval", type=float, default=0.05)
    parser.add_argument("--ca-cert", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = tuple(item.strip().upper() for item in args.tickers.split(",") if item.strip())
    if not tickers or len(tickers) > 25:
        raise SystemExit("--tickers must contain 1..25 symbols")
    start_day, end_day = study_window(args.calendar_days, args.end_day)
    try:
        manifest = run_cache(
            env_file=args.env_file,
            cache_dir=args.cache_dir,
            tickers=tickers,
            start_day=start_day,
            end_day=end_day,
            request_timeout=args.request_timeout,
            request_attempts=args.request_attempts,
            request_interval=args.request_interval,
            ca_cert=args.ca_cert,
        )
    except Exception as exc:
        failure = {
            "schema_version": 1,
            "kind": "tinvest_research_candle_cache_failure",
            "reason_code": "tinvest_research_cache_failed",
            "diagnostic": redact_diagnostic(exc),
        }
        write_json(args.cache_dir / "failure-summary.json", failure)
        raise
    print(
        json.dumps(
            {
                "status": "ok" if not manifest["quality"]["failed_partitions"] else "partial",
                "cache_dir": str(args.cache_dir),
                "partitions": manifest["quality"]["partition_count"],
                "failed_partitions": len(manifest["quality"]["failed_partitions"]),
                "manifest": str(manifest_path(args.cache_dir)),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
