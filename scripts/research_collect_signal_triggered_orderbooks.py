#!/usr/bin/env python3
"""Collect order-book snapshots when fresh replayed detector signals appear.

This is offline research tooling for building a liquidity-aware holdout. A blind
time sampler can miss signal moments; this script polls recent one-minute
candles, replays the detector for the current trading day, and stores an
order-book snapshot as soon as a fresh, previously unseen signal appears.

Persisted artifacts contain only market data and local signal keys. They do not
store broker tokens, account identifiers, FIGIs, or instrument UIDs.
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_cache_tinvest_candles import fetch_day_candles, resolve_instruments  # noqa: E402
from research_collect_tinvest_orderbook_snapshots import (  # noqa: E402
    _headers,
    _write_partitions,
    fetch_orderbook_snapshot,
)
from research_price_prediction_lib import (  # noqa: E402
    DEFAULT_RESEARCH_TICKERS,
    MOSCOW,
    UTC,
    SignalEvent,
    build_orderbook_cache_manifest,
    fingerprint_records,
    load_env_value,
    manifest_path,
    orderbook_partition_path,
    read_table,
    redact_diagnostic,
    replay_signals,
    require_duckdb,
    write_json,
)


def _parse_tickers(raw: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in raw.split(",") if item.strip())


def signal_key(signal: SignalEvent) -> str:
    return "|".join(
        (
            signal.ticker,
            signal.signal_type,
            signal.source_event_at.astimezone(UTC).isoformat(),
            str(signal.direction),
        )
    )


def select_fresh_signal_tickers(
    signals: Sequence[SignalEvent],
    *,
    seen_signal_keys: set[str],
    now: datetime,
    max_signal_age_seconds: int,
) -> tuple[set[str], list[str]]:
    trigger_tickers: set[str] = set()
    fresh_keys: list[str] = []
    now_utc = now.astimezone(UTC)
    for signal in sorted(signals, key=lambda item: item.source_event_at):
        key = signal_key(signal)
        if key in seen_signal_keys:
            continue
        age_seconds = (now_utc - signal.source_event_at.astimezone(UTC)).total_seconds()
        if age_seconds < 0 or age_seconds > max_signal_age_seconds:
            continue
        trigger_tickers.add(signal.ticker)
        fresh_keys.append(key)
    return trigger_tickers, fresh_keys


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": 1,
            "kind": "signal_triggered_orderbook_state",
            "seen_signal_keys": [],
            "polls_completed": 0,
            "snapshots_collected": 0,
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, state: Mapping[str, Any]) -> None:
    write_json(path, state)


def _verify_context(ca_cert: Path | None, insecure_skip_tls_verify: bool) -> bool | ssl.SSLContext:
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    if insecure_skip_tls_verify:
        verify = False
    return verify


def _write_manifest(cache_dir: Path, *, tickers: Sequence[str], depth: int, failures: Sequence[Mapping[str, Any]]) -> None:
    rows: list[dict[str, Any]] = []
    row_counts: dict[str, int] = {}
    for file in sorted(cache_dir.glob("ticker=*/date=*.parquet")):
        ticker = file.parent.name.removeprefix("ticker=")
        day = file.name.removeprefix("date=").removesuffix(".parquet")
        partition_rows = read_table(file)
        row_counts[f"{ticker}/{day}"] = len(partition_rows)
        rows.extend(partition_rows)
    if not rows:
        return
    times = [
        datetime.fromisoformat(str(row["at"]).replace("Z", "+00:00")).astimezone(UTC)
        for row in rows
    ]
    manifest = build_orderbook_cache_manifest(
        tickers=tuple(tickers),
        start_at=min(times),
        end_at=max(times),
        depth=depth,
        row_counts=row_counts,
        content_fingerprint=fingerprint_records(rows),
        failures=failures,
    )
    write_json(manifest_path(cache_dir), manifest)


def collect_signal_triggered_orderbooks(
    *,
    env_file: Path,
    cache_dir: Path,
    state_file: Path,
    tickers: tuple[str, ...],
    target_day: date,
    depth: int,
    polls: int,
    interval_seconds: float,
    max_signal_age_seconds: int,
    request_timeout: float,
    request_attempts: int,
    ca_cert: Path | None,
    insecure_skip_tls_verify: bool = False,
) -> dict[str, Any]:
    require_duckdb()
    token = load_env_value(env_file, "TINVEST_TOKEN")
    timeout = httpx.Timeout(
        request_timeout,
        connect=min(request_timeout, 10.0),
        read=request_timeout,
        write=min(request_timeout, 10.0),
        pool=min(request_timeout, 10.0),
    )
    state = _load_state(state_file)
    seen_signal_keys = set(str(item) for item in state.get("seen_signal_keys", []))
    failures: list[dict[str, Any]] = []
    rows_collected = 0
    triggers_seen = 0
    with httpx.Client(
        headers=_headers(token),
        timeout=timeout,
        verify=_verify_context(ca_cert, insecure_skip_tls_verify),
    ) as client:
        instruments = resolve_instruments(client, tickers, attempts=request_attempts)
        for poll_index in range(max(1, polls)):
            now = datetime.now(UTC)
            candles = []
            for ticker in tickers:
                try:
                    candles.extend(
                        fetch_day_candles(
                            client,
                            ticker=ticker,
                            instrument_uid=instruments[ticker],
                            day=target_day,
                            attempts=request_attempts,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - diagnostics are redacted below
                    failures.append(
                        {
                            "ticker": ticker,
                            "poll": poll_index,
                            "reason_code": "tinvest_signal_candles_failed",
                            "message": redact_diagnostic(exc),
                        }
                    )
            signals = replay_signals(candles, max_signals_per_instrument=10_000)
            trigger_tickers, fresh_keys = select_fresh_signal_tickers(
                signals,
                seen_signal_keys=seen_signal_keys,
                now=now,
                max_signal_age_seconds=max_signal_age_seconds,
            )
            triggers_seen += len(fresh_keys)
            collected_rows: list[dict[str, Any]] = []
            for ticker in sorted(trigger_tickers):
                try:
                    row = fetch_orderbook_snapshot(
                        client,
                        ticker=ticker,
                        instrument_uid=instruments[ticker],
                        depth=depth,
                        attempts=request_attempts,
                    )
                    if row is not None:
                        collected_rows.append(row)
                except Exception as exc:  # noqa: BLE001 - diagnostics are redacted below
                    failures.append(
                        {
                            "ticker": ticker,
                            "poll": poll_index,
                            "reason_code": "tinvest_signal_orderbook_failed",
                            "message": redact_diagnostic(exc),
                        }
                    )
            if collected_rows:
                _write_partitions(cache_dir, collected_rows)
                rows_collected += len(collected_rows)
                seen_signal_keys.update(fresh_keys)
                state.update(
                    {
                        "schema_version": 1,
                        "kind": "signal_triggered_orderbook_state",
                        "updated_at": datetime.now(UTC).isoformat(),
                        "target_day": target_day.isoformat(),
                        "seen_signal_keys": sorted(seen_signal_keys)[-50_000:],
                        "polls_completed": int(state.get("polls_completed", 0)) + 1,
                        "snapshots_collected": int(state.get("snapshots_collected", 0)) + len(collected_rows),
                    }
                )
                _save_state(state_file, state)
            elif fresh_keys:
                # Do not mark keys as seen when no snapshot was stored; a later poll may succeed.
                state["polls_completed"] = int(state.get("polls_completed", 0)) + 1
                _save_state(state_file, state)
            else:
                state["polls_completed"] = int(state.get("polls_completed", 0)) + 1
                _save_state(state_file, state)
            if poll_index + 1 < polls and interval_seconds > 0:
                time.sleep(interval_seconds)
    _write_manifest(cache_dir, tickers=tickers, depth=depth, failures=failures)
    return {
        "status": "ok" if not failures else "partial",
        "cache_dir": str(cache_dir),
        "state_file": str(state_file),
        "target_day": target_day.isoformat(),
        "polls": polls,
        "triggers_seen": triggers_seen,
        "rows_collected": rows_collected,
        "failures": len(failures),
        "manifest": str(manifest_path(cache_dir)),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-collect-signal-triggered-orderbooks")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--state-file", type=Path, default=Path("var/research/tinvest_orderbooks/signal-triggered-state.json"))
    parser.add_argument("--tickers", type=_parse_tickers, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--target-day", type=date.fromisoformat)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--polls", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--max-signal-age-seconds", type=int, default=180)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=3)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    target_day = args.target_day or datetime.now(UTC).astimezone(MOSCOW).date()
    result = collect_signal_triggered_orderbooks(
        env_file=args.env_file,
        cache_dir=args.cache_dir,
        state_file=args.state_file,
        tickers=args.tickers,
        target_day=target_day,
        depth=args.depth,
        polls=args.polls,
        interval_seconds=args.interval_seconds,
        max_signal_age_seconds=args.max_signal_age_seconds,
        request_timeout=args.request_timeout,
        request_attempts=args.request_attempts,
        ca_cert=args.ca_cert,
        insecure_skip_tls_verify=args.insecure_skip_tls_verify,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
