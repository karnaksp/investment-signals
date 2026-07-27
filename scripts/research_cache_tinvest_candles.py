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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as datetime_time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    CANDLE_CACHE_FIELDS,
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
    candle_source_type: str = "CANDLE_SOURCE_EXCHANGE",
    session_start: datetime_time = datetime_time(7, 0),
    session_end: datetime_time = datetime_time(19, 0),
) -> tuple[ResearchCandle, ...]:
    if session_start >= session_end:
        raise ValueError("session_start must be earlier than session_end")
    start = datetime.combine(day, session_start, tzinfo=MOSCOW).astimezone(UTC)
    end = datetime.combine(day, session_end, tzinfo=MOSCOW).astimezone(UTC)
    payload: dict[str, Any] = {
        "from": start.isoformat().replace("+00:00", "Z"),
        "to": end.isoformat().replace("+00:00", "Z"),
        "interval": "CANDLE_INTERVAL_1_MIN",
        "instrumentId": instrument_uid,
    }
    if candle_source_type:
        payload["candleSourceType"] = candle_source_type
    body = api_post(
        client,
        "MarketDataService/GetCandles",
        payload,
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
                volume_buy=float(item.get("volumeBuy", 0) or 0),
                volume_sell=float(item.get("volumeSell", 0) or 0),
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
    max_workers: int,
    ca_cert: Path | None,
    refresh_days: frozenset[date] = frozenset(),
    insecure_skip_tls_verify: bool = False,
    resolved_instruments: Mapping[str, str] | None = None,
    candle_source_type: str = "CANDLE_SOURCE_EXCHANGE",
    session_start: datetime_time = datetime_time(7, 0),
    session_end: datetime_time = datetime_time(19, 0),
) -> dict[str, Any]:
    require_duckdb()
    token = load_env_value(env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    if insecure_skip_tls_verify:
        verify = False
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
    if resolved_instruments is None:
        with httpx.Client(headers=headers, timeout=timeout, verify=verify) as client:
            instruments = resolve_instruments(client, tickers, attempts=request_attempts)
    else:
        instruments = {ticker: str(resolved_instruments[ticker]) for ticker in tickers}
    row_counts: dict[str, int] = {}
    all_records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    skipped = 0
    session_backfills = 0
    tasks: list[tuple[str, date]] = []
    backfill_existing: dict[tuple[str, date], list[dict[str, Any]]] = {}
    existing_manifest = _read_existing_manifest(cache_dir)
    cache_session_is_current = _manifest_covers_session(
        existing_manifest,
        session_start=session_start,
        session_end=session_end,
    )
    day = start_day
    while day <= end_day:
        for ticker in tickers:
            target = partition_path(cache_dir, ticker, day)
            key = f"{ticker}/{day.isoformat()}"
            if valid_partition(target) and day not in refresh_days:
                rows = read_existing_records(target)
                if (
                    not cache_session_is_current
                    and _requires_morning_backfill(
                        rows,
                        requested_start=session_start,
                    )
                ):
                    tasks.append((ticker, day))
                    backfill_existing[(ticker, day)] = rows
                    session_backfills += 1
                    continue
                skipped += 1
                row_counts[key] = len(rows)
                all_records.extend(rows)
                continue
            tasks.append((ticker, day))
        day += timedelta(days=1)
    print(
        json.dumps(
            {
                "progress": "cache_plan",
                "tickers": len(tickers),
                "pending_partitions": len(tasks),
                "skipped_existing_partitions": skipped,
                "session_backfill_partitions": session_backfills,
                "max_workers": max_workers,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )
    completed = 0
    # httpx.Client is thread-safe. Reusing one connection pool avoids a new
    # TLS handshake for every ticker-day partition while preserving the same
    # bounded worker count and retry policy.
    with (
        httpx.Client(headers=headers, timeout=timeout, verify=verify) as shared_client,
        ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor,
    ):
        futures = {
            executor.submit(
                fetch_partition_records,
                token=token,
                ticker=ticker,
                instrument_uid=instruments[ticker],
                day=day,
                request_timeout=request_timeout,
                request_attempts=request_attempts,
                ca_cert=ca_cert,
                insecure_skip_tls_verify=insecure_skip_tls_verify,
                request_interval=request_interval,
                candle_source_type=candle_source_type,
                session_start=session_start,
                session_end=(
                    datetime_time(10, 0)
                    if (ticker, day) in backfill_existing
                    else session_end
                ),
                client=shared_client,
            ): (ticker, day)
            for ticker, day in tasks
        }
        for future in as_completed(futures):
            ticker, day = futures[future]
            target = partition_path(cache_dir, ticker, day)
            key = f"{ticker}/{day.isoformat()}"
            try:
                records = future.result()
                if existing := backfill_existing.get((ticker, day)):
                    records = _merge_partition_records(records, existing)
                write_table(target, records, fields=CANDLE_CACHE_FIELDS)
                canonical_records = read_existing_records(target)
                row_counts[key] = len(canonical_records)
                all_records.extend(canonical_records)
            except Exception as exc:
                failures.append(
                    {
                        "ticker": ticker,
                        "date": day.isoformat(),
                        "reason_code": "tinvest_candle_partition_failed",
                        "diagnostic": redact_diagnostic(exc),
                    }
                )
            completed += 1
            if completed == len(tasks) or completed % 100 == 0:
                print(
                    json.dumps(
                        {
                            "progress": "cache_partitions",
                            "completed": completed,
                            "pending": len(tasks),
                            "failures": len(failures),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
    manifest = build_cache_manifest(
        tickers=tickers,
        start_day=start_day,
        end_day=end_day,
        row_counts=row_counts,
        content_fingerprint=fingerprint_records(all_records),
        failures=failures,
    )
    manifest["quality"]["skipped_existing_partitions"] = skipped
    manifest["quality"]["session_backfill_partitions"] = session_backfills
    manifest["quality"]["refreshed_dates"] = [item.isoformat() for item in sorted(refresh_days)]
    manifest["scope"]["session_window"] = {
        "timezone": "Europe/Moscow",
        "from": session_start.strftime("%H:%M"),
        "to": session_end.strftime("%H:%M"),
    }
    manifest["script_version"] = "research-cache-v1.1.0"
    write_json(manifest_path(cache_dir), manifest)
    if failures:
        write_json(cache_dir / "failure-summary.json", {"failures": failures})
    return manifest


def fetch_partition_records(
    *,
    token: str,
    ticker: str,
    instrument_uid: str,
    day: date,
    request_timeout: float,
    request_attempts: int,
    request_interval: float,
    ca_cert: Path | None,
    insecure_skip_tls_verify: bool = False,
    candle_source_type: str = "CANDLE_SOURCE_EXCHANGE",
    session_start: datetime_time = datetime_time(7, 0),
    session_end: datetime_time = datetime_time(19, 0),
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    if insecure_skip_tls_verify:
        verify = False
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
    if client is None:
        with httpx.Client(headers=headers, timeout=timeout, verify=verify) as owned_client:
            return fetch_partition_records(
                token=token,
                ticker=ticker,
                instrument_uid=instrument_uid,
                day=day,
                request_timeout=request_timeout,
                request_attempts=request_attempts,
                request_interval=request_interval,
                ca_cert=ca_cert,
                insecure_skip_tls_verify=insecure_skip_tls_verify,
                candle_source_type=candle_source_type,
                session_start=session_start,
                session_end=session_end,
                client=owned_client,
            )
    candles = fetch_day_candles(
        client,
        ticker=ticker,
        instrument_uid=instrument_uid,
        day=day,
        attempts=request_attempts,
        candle_source_type=candle_source_type,
        session_start=session_start,
        session_end=session_end,
    )
    time.sleep(request_interval)
    return candle_rows_for_storage(candles)


def read_existing_records(path: Path) -> list[dict[str, Any]]:
    from research_price_prediction_lib import read_table

    return read_table(path)


def _read_existing_manifest(cache_dir: Path) -> Mapping[str, Any]:
    path = manifest_path(cache_dir)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _manifest_covers_session(
    manifest: Mapping[str, Any],
    *,
    session_start: datetime_time,
    session_end: datetime_time,
) -> bool:
    scope = manifest.get("scope")
    quality = manifest.get("quality")
    if not isinstance(scope, Mapping) or not isinstance(quality, Mapping):
        return False
    if quality.get("failed_partitions"):
        return False
    window = scope.get("session_window")
    if not isinstance(window, Mapping):
        return False
    try:
        cached_start = datetime_time.fromisoformat(str(window["from"]))
        cached_end = datetime_time.fromisoformat(str(window["to"]))
    except (KeyError, TypeError, ValueError):
        return False
    return cached_start <= session_start and cached_end >= session_end


def _requires_morning_backfill(
    records: Sequence[Mapping[str, Any]],
    *,
    requested_start: datetime_time,
) -> bool:
    """Detect legacy 10:00-only trading partitions without refetching empty days."""

    if requested_start >= datetime_time(10, 0) or not records:
        return False
    observed: list[datetime_time] = []
    for record in records:
        raw = record.get("at")
        try:
            at = (
                raw
                if isinstance(raw, datetime)
                else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            )
        except (TypeError, ValueError):
            continue
        if at.tzinfo is None:
            at = at.replace(tzinfo=UTC)
        observed.append(at.astimezone(MOSCOW).time().replace(tzinfo=None))
    return bool(observed) and min(observed) >= datetime_time(10, 0)


def _merge_partition_records(
    first: Sequence[Mapping[str, Any]],
    second: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Merge a morning repair with the immutable main-session partition."""

    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for record in (*first, *second):
        row = dict(record)
        key = (str(row.get("ticker", "")), str(row.get("at", "")))
        if not all(key):
            raise ValueError("candle partition row requires ticker and at")
        merged[key] = row
    return [merged[key] for key in sorted(merged, key=lambda item: (item[0], item[1]))]


def _clock(value: str) -> datetime_time:
    try:
        return datetime_time.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected local time as HH:MM") from exc


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-cache-tinvest-candles")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_candles/v1"))
    parser.add_argument("--tickers", default=",".join(DEFAULT_RESEARCH_TICKERS))
    parser.add_argument("--calendar-days", type=int, default=180)
    parser.add_argument("--start-day", type=date.fromisoformat)
    parser.add_argument("--end-day", type=date.fromisoformat)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--request-interval", type=float, default=0.05)
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--session-start", type=_clock, default=datetime_time(7, 0))
    parser.add_argument("--session-end", type=_clock, default=datetime_time(19, 0))
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument(
        "--refresh-days",
        default="",
        help="Comma-separated YYYY-MM-DD dates to re-fetch even when valid partitions already exist.",
    )
    parser.add_argument(
        "--insecure-skip-tls-verify",
        action="store_true",
        help="Only for local research behind intercepting proxies; TLS verification stays enabled by default.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = tuple(item.strip().upper() for item in args.tickers.split(",") if item.strip())
    if not tickers or len(tickers) > 25:
        raise SystemExit("--tickers must contain 1..25 symbols")
    if args.start_day and args.end_day:
        start_day, end_day = args.start_day, args.end_day
        if start_day > end_day:
            raise SystemExit("--start-day must not be after --end-day")
    elif args.start_day or args.end_day:
        raise SystemExit("--start-day and --end-day must be provided together, or use --calendar-days")
    else:
        start_day, end_day = study_window(args.calendar_days, args.end_day)
    try:
        refresh_days = frozenset(date.fromisoformat(item.strip()) for item in args.refresh_days.split(",") if item.strip())
        if args.session_start >= args.session_end:
            raise SystemExit("--session-start must be earlier than --session-end")
        manifest = run_cache(
            env_file=args.env_file,
            cache_dir=args.cache_dir,
            tickers=tickers,
            start_day=start_day,
            end_day=end_day,
            request_timeout=args.request_timeout,
            request_attempts=args.request_attempts,
            request_interval=args.request_interval,
            max_workers=args.max_workers,
            ca_cert=args.ca_cert,
            refresh_days=refresh_days,
            insecure_skip_tls_verify=args.insecure_skip_tls_verify,
            session_start=args.session_start,
            session_end=args.session_end,
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
