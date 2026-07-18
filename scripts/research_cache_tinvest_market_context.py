#!/usr/bin/env python3
"""Cache reusable T-Invest cross-market candles without persisting identifiers."""

from __future__ import annotations

import argparse
import json
import ssl
import sys
from datetime import date
from pathlib import Path
from typing import Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_cache_tinvest_candles import api_post, run_cache  # noqa: E402
from research_price_prediction_lib import (  # noqa: E402
    load_env_value,
    manifest_path,
    redact_diagnostic,
    study_window,
    write_json,
)


DEFAULT_MARKET_UNIVERSE = (
    "IMOEX", "RTSI", "MOEXBC", "RVI", "RGBI", "RGBITR", "RUGBICP5Y7Y",
    "EURUSD", "CNYRUB", "BRENT", "NG", "XAU", "XAG", "XPD", "XPT",
    "BTC", "ETH", "MOEXBTC", "MOEXETH", "FTSE", "DAX", "STOXX", "VIX",
    "NDX", "SPX",
)

SOURCE_TICKER_BY_ALIAS = {
    "EURUSD": "EUR/USD",
    "CNYRUB": "CNYRUB_TOM",
    "BRENT": "LCOc1",
}


def resolve_context_instruments(
    client: httpx.Client,
    *,
    attempts: int,
    tickers: Sequence[str] = DEFAULT_MARKET_UNIVERSE,
) -> dict[str, str]:
    indicatives = api_post(client, "InstrumentsService/Indicatives", {}, attempts=attempts)
    currencies = api_post(
        client,
        "InstrumentsService/Currencies",
        {"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
        attempts=attempts,
    )
    indicative_by_ticker = {
        str(item.get("ticker")): str(item.get("uid"))
        for item in indicatives.get("instruments", [])
        if isinstance(item, Mapping) and item.get("ticker") and item.get("uid")
    }
    currency_by_ticker = {
        str(item.get("ticker")): str(item.get("uid"))
        for item in currencies.get("instruments", [])
        if isinstance(item, Mapping) and item.get("ticker") and item.get("uid")
    }
    resolved: dict[str, str] = {}
    for ticker in tickers:
        source_ticker = SOURCE_TICKER_BY_ALIAS.get(ticker, ticker)
        resolved[ticker] = (
            currency_by_ticker.get(source_ticker, "")
            if ticker == "CNYRUB"
            else indicative_by_ticker.get(source_ticker, "")
        )
    missing = sorted(ticker for ticker, uid in resolved.items() if not uid)
    if missing:
        raise RuntimeError(f"T-Invest market context instruments not found: {','.join(missing)}")
    return resolved


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-cache-tinvest-market-context")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_market_context/v1"))
    parser.add_argument("--tickers", default=",".join(DEFAULT_MARKET_UNIVERSE))
    parser.add_argument("--calendar-days", type=int, default=180)
    parser.add_argument("--start-day", type=date.fromisoformat)
    parser.add_argument("--end-day", type=date.fromisoformat)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--request-interval", type=float, default=0.01)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
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
        raise SystemExit("--start-day and --end-day must be provided together")
    else:
        start_day, end_day = study_window(args.calendar_days)

    token = load_env_value(args.env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if args.ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=args.ca_cert)
    if args.insecure_skip_tls_verify:
        verify = False
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-app-name": "investment-signals-market-context-research",
    }
    try:
        with httpx.Client(headers=headers, timeout=args.request_timeout, verify=verify) as client:
            instruments = resolve_context_instruments(
                client,
                attempts=args.request_attempts,
                tickers=tickers,
            )
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
            insecure_skip_tls_verify=args.insecure_skip_tls_verify,
            resolved_instruments=instruments,
            candle_source_type="",
        )
        manifest["kind"] = "tinvest_research_market_context_cache"
        manifest["privacy"]["instrument_uids_persisted"] = False
        write_json(manifest_path(args.cache_dir), manifest)
    except Exception as exc:
        write_json(
            args.cache_dir / "failure-summary.json",
            {
                "schema_version": 1,
                "kind": "tinvest_research_market_context_cache_failure",
                "reason_code": "tinvest_market_context_cache_failed",
                "diagnostic": redact_diagnostic(exc),
            },
        )
        raise
    print(
        json.dumps(
            {
                "status": "ok" if not manifest["quality"]["failed_partitions"] else "partial",
                "cache_dir": str(args.cache_dir),
                "partitions": manifest["quality"]["partition_count"],
                "failed_partitions": len(manifest["quality"]["failed_partitions"]),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
