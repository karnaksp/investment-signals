#!/usr/bin/env python3
"""Исследовать схождение поставочных фьючерсов с базовыми акциями.

Утилита относится только к локальному исследовательскому контуру. Она один
раз сохраняет обезличенные дневные цены по завершённым контрактам, а затем
повторно использует их без новых запросов к Т‑Инвест. Токен и идентификаторы
инструментов в кэш и результаты не записываются.
"""

from __future__ import annotations

import argparse
from bisect import bisect_right
import json
import re
import ssl
import statistics
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_cache_tinvest_candles import api_post  # noqa: E402
from research_price_prediction_lib import (  # noqa: E402
    load_env_value,
    quotation,
    read_table,
    redact_diagnostic,
    wilson_lower_bound,
    write_json,
    write_table,
)


CACHE_FIELDS = (
    "future_ticker",
    "basic_asset",
    "expiration",
    "day",
    "future_close",
    "share_close",
    "future_volume",
    "share_volume",
)
DIVIDEND_CACHE_FIELDS = (
    "basic_asset",
    "declared_date",
    "last_buy_date",
    "record_date",
    "dividend_amount",
    "currency",
)
KEY_RATE_CACHE_FIELDS = ("day", "key_rate_percent")
CBR_KEY_RATE_URL = "https://www.cbr.ru/hd_base/KeyRate/"
DEFAULT_DAYS_TO_EXIT = (10, 7, 5, 3, 2, 1)
DEFAULT_BASIS_THRESHOLDS_BPS = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 75.0, 100.0)
MAXIMUM_PLAUSIBLE_ENTRY_BASIS_BPS = 3_000.0


@dataclass(frozen=True)
class FutureContract:
    ticker: str
    basic_asset: str
    expiration: date
    last_trade_day: date
    basic_asset_size: float
    instrument_uid: str
    share_uid: str


def _iso_utc(day: date, *, end: bool = False) -> str:
    suffix = "T23:59:59Z" if end else "T00:00:00Z"
    return f"{day.isoformat()}{suffix}"


def _cache_path(cache_dir: Path, contract: FutureContract) -> Path:
    return (
        cache_dir
        / f"basic_asset={contract.basic_asset}"
        / f"expiration={contract.expiration.isoformat()}"
        / f"future={contract.ticker}.parquet"
    )


def _catalog_contracts(
    client: httpx.Client,
    *,
    attempts: int,
    first_expiration: date,
    last_expiration: date,
) -> list[FutureContract]:
    futures = api_post(
        client,
        "InstrumentsService/Futures",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
        attempts=attempts,
    ).get("instruments", [])
    shares = api_post(
        client,
        "InstrumentsService/Shares",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
        attempts=attempts,
    ).get("instruments", [])
    share_by_ticker = {
        str(item.get("ticker")): str(item.get("uid"))
        for item in shares
        if isinstance(item, Mapping)
        and item.get("classCode") == "TQBR"
        and item.get("ticker")
        and item.get("uid")
    }
    result: list[FutureContract] = []
    for item in futures:
        if not isinstance(item, Mapping):
            continue
        basic_asset = str(item.get("basicAsset", ""))
        share_uid = share_by_ticker.get(basic_asset)
        expiration_text = str(item.get("expirationDate", ""))[:10]
        last_trade_text = str(item.get("lastTradeDate", ""))[:10]
        size = quotation(item.get("basicAssetSize"))
        if (
            item.get("futuresType") != "DELIVERY_TYPE_PHYSICAL_DELIVERY"
            or not share_uid
            or not item.get("uid")
            or not item.get("ticker")
            or not expiration_text
            or not last_trade_text
            or size <= 0
        ):
            continue
        expiration = date.fromisoformat(expiration_text)
        if not first_expiration <= expiration <= last_expiration:
            continue
        result.append(
            FutureContract(
                ticker=str(item["ticker"]),
                basic_asset=basic_asset,
                expiration=expiration,
                last_trade_day=date.fromisoformat(last_trade_text),
                basic_asset_size=size,
                instrument_uid=str(item["uid"]),
                share_uid=share_uid,
            )
        )
    return sorted(result, key=lambda item: (item.expiration, item.basic_asset, item.ticker))


def _daily_candles(
    client: httpx.Client,
    *,
    instrument_uid: str,
    start_day: date,
    end_day: date,
    attempts: int,
) -> dict[date, tuple[float, float]]:
    body = api_post(
        client,
        "MarketDataService/GetCandles",
        {
            "from": _iso_utc(start_day),
            "to": _iso_utc(end_day, end=True),
            "interval": "CANDLE_INTERVAL_DAY",
            "instrumentId": instrument_uid,
            "candleSourceType": "CANDLE_SOURCE_EXCHANGE",
        },
        attempts=attempts,
    )
    result: dict[date, tuple[float, float]] = {}
    for item in body.get("candles", []):
        close = quotation(item.get("close"))
        day_text = str(item.get("time", ""))[:10]
        if close <= 0 or not day_text:
            continue
        result[date.fromisoformat(day_text)] = (close, float(item.get("volume", 0) or 0))
    return result


def fetch_or_read_dividends(
    client: httpx.Client,
    *,
    contracts: Sequence[FutureContract],
    cache_dir: Path,
    start_day: date,
    end_day: date,
    attempts: int,
) -> list[dict[str, Any]]:
    path = cache_dir / "dividends.parquet"
    if path.exists():
        return read_table(path)
    share_uid_by_asset = {contract.basic_asset: contract.share_uid for contract in contracts}
    rows: list[dict[str, Any]] = []
    for basic_asset, share_uid in sorted(share_uid_by_asset.items()):
        body = api_post(
            client,
            "InstrumentsService/GetDividends",
            {
                "from": _iso_utc(start_day),
                "to": _iso_utc(end_day, end=True),
                "instrumentId": share_uid,
            },
            attempts=attempts,
        )
        for item in body.get("dividends", []):
            dividend = item.get("dividendNet") or {}
            declared_date = str(item.get("declaredDate", ""))[:10]
            last_buy_date = str(item.get("lastBuyDate", ""))[:10]
            record_date = str(item.get("recordDate", ""))[:10]
            amount = quotation(dividend)
            if not declared_date or not last_buy_date or amount <= 0:
                continue
            rows.append(
                {
                    "basic_asset": basic_asset,
                    "declared_date": declared_date,
                    "last_buy_date": last_buy_date,
                    "record_date": record_date,
                    "dividend_amount": amount,
                    "currency": str(dividend.get("currency", "")),
                }
            )
    write_table(path, rows, fields=DIVIDEND_CACHE_FIELDS)
    return rows


def fetch_or_read_key_rate(
    *,
    cache_dir: Path,
    start_day: date,
    end_day: date,
    timeout: float,
    verify: bool | ssl.SSLContext,
) -> list[dict[str, Any]]:
    path = cache_dir / "cbr_key_rate.parquet"
    if path.exists():
        return read_table(path)
    params = {
        "UniDbQuery.Posted": "True",
        "UniDbQuery.From": start_day.strftime("%d.%m.%Y"),
        "UniDbQuery.To": end_day.strftime("%d.%m.%Y"),
    }
    with httpx.Client(timeout=timeout, verify=verify) as client:
        response = client.get(CBR_KEY_RATE_URL, params=params)
        response.raise_for_status()
    pairs = re.findall(
        r"<td>\s*(\d{2}\.\d{2}\.\d{4})\s*</td>\s*<td>\s*([\d,]+)\s*</td>",
        response.text,
        flags=re.IGNORECASE,
    )
    rows = [
        {
            "day": datetime.strptime(day_text, "%d.%m.%Y").date().isoformat(),
            "key_rate_percent": float(rate_text.replace(",", ".")),
        }
        for day_text, rate_text in pairs
    ]
    if not rows:
        raise RuntimeError("CBR key-rate history is empty")
    write_table(path, rows, fields=KEY_RATE_CACHE_FIELDS)
    return rows


def fetch_or_read_contract(
    client: httpx.Client,
    *,
    contract: FutureContract,
    cache_dir: Path,
    lookback_calendar_days: int,
    attempts: int,
) -> tuple[list[dict[str, Any]], bool]:
    path = _cache_path(cache_dir, contract)
    if path.exists():
        rows = read_table(path)
        if rows:
            return rows, True
    start_day = contract.last_trade_day - timedelta(days=lookback_calendar_days)
    futures = _daily_candles(
        client,
        instrument_uid=contract.instrument_uid,
        start_day=start_day,
        end_day=contract.last_trade_day,
        attempts=attempts,
    )
    shares = _daily_candles(
        client,
        instrument_uid=contract.share_uid,
        start_day=start_day,
        end_day=contract.last_trade_day,
        attempts=attempts,
    )
    rows = [
        {
            "future_ticker": contract.ticker,
            "basic_asset": contract.basic_asset,
            "expiration": contract.expiration.isoformat(),
            "day": day.isoformat(),
            "future_close": futures[day][0] / contract.basic_asset_size,
            "share_close": shares[day][0],
            "future_volume": futures[day][1],
            "share_volume": shares[day][1],
        }
        for day in sorted(set(futures) & set(shares))
    ]
    if rows:
        write_table(path, rows, fields=CACHE_FIELDS)
    return rows, False


def build_basis_observations(
    contract_rows: Sequence[Mapping[str, Any]],
    *,
    days_to_exit: Sequence[int] = DEFAULT_DAYS_TO_EXIT,
    dividend_events: Sequence[Mapping[str, Any]] = (),
    key_rate_rows: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    rows = sorted(contract_rows, key=lambda item: str(item["day"]))
    if len(rows) < max(days_to_exit, default=0) + 1:
        return []
    exit_row = rows[-1]
    exit_basis = (float(exit_row["future_close"]) / float(exit_row["share_close"]) - 1.0) * 10_000
    observations: list[dict[str, Any]] = []
    key_rates = sorted(
        (
            date.fromisoformat(str(row["day"])),
            float(row["key_rate_percent"]),
        )
        for row in key_rate_rows
        if row.get("day") and row.get("key_rate_percent") is not None
    )
    key_rate_days = [item[0] for item in key_rates]

    def basis(row: Mapping[str, Any]) -> float:
        return (float(row["future_close"]) / float(row["share_close"]) - 1.0) * 10_000

    for trading_days in sorted(set(int(value) for value in days_to_exit if int(value) > 0), reverse=True):
        if len(rows) <= trading_days:
            continue
        entry_index = len(rows) - 1 - trading_days
        entry_row = rows[entry_index]
        entry_basis = basis(entry_row)
        if abs(entry_basis) > MAXIMUM_PLAUSIBLE_ENTRY_BASIS_BPS:
            continue
        raw_basis_side = 1.0 if entry_basis >= 0 else -1.0
        prior_features: dict[str, float | None] = {}
        for window in (1, 3, 5, 10):
            if entry_index < window:
                prior_features[f"prior_basis_change_{window}d_bps"] = None
                prior_features[f"prior_future_return_{window}d_bps"] = None
                prior_features[f"prior_share_return_{window}d_bps"] = None
                continue
            prior_row = rows[entry_index - window]
            prior_features[f"prior_basis_change_{window}d_bps"] = entry_basis - basis(prior_row)
            prior_features[f"prior_future_return_{window}d_bps"] = (
                float(entry_row["future_close"]) / float(prior_row["future_close"]) - 1.0
            ) * 10_000
            prior_features[f"prior_share_return_{window}d_bps"] = (
                float(entry_row["share_close"]) / float(prior_row["share_close"]) - 1.0
            ) * 10_000
        trailing_basis = [basis(row) for row in rows[max(0, entry_index - 9) : entry_index + 1]]
        entry_day = date.fromisoformat(str(entry_row["day"]))
        expiration_day = date.fromisoformat(str(entry_row["expiration"]))
        known_dividends = [
            event
            for event in dividend_events
            if str(event.get("basic_asset", "")) == str(entry_row["basic_asset"])
            and str(event.get("declared_date", "")) <= entry_day.isoformat()
            and entry_day.isoformat() <= str(event.get("last_buy_date", "")) < str(exit_row["day"])
        ]
        known_dividend_bps = (
            sum(float(event.get("dividend_amount", 0) or 0) for event in known_dividends)
            / float(entry_row["share_close"])
            * 10_000
        )
        key_rate_index = bisect_right(key_rate_days, entry_day) - 1
        key_rate_percent = key_rates[key_rate_index][1] if key_rate_index >= 0 else 0.0
        calendar_days_to_exit = (date.fromisoformat(str(exit_row["day"])) - entry_day).days
        financing_bps = key_rate_percent / 100.0 * calendar_days_to_exit / 365.0 * 10_000
        fair_basis_bps = financing_bps - known_dividend_bps
        basis_residual_bps = entry_basis - fair_basis_bps
        side = 1.0 if basis_residual_bps >= 0 else -1.0
        raw_convergence_bps = side * (entry_basis - exit_basis)
        observations.append(
            {
                "future_ticker": str(entry_row["future_ticker"]),
                "basic_asset": str(entry_row["basic_asset"]),
                "expiration": str(entry_row["expiration"]),
                "entry_day": str(entry_row["day"]),
                "exit_day": str(exit_row["day"]),
                "trading_days_to_exit": trading_days,
                "entry_basis_bps": entry_basis,
                "absolute_entry_basis_bps": abs(entry_basis),
                "raw_basis_side": int(raw_basis_side),
                "basis_side": int(side),
                "key_rate_percent": key_rate_percent,
                "calendar_days_to_exit": calendar_days_to_exit,
                "financing_bps": financing_bps,
                "fair_basis_bps": fair_basis_bps,
                "basis_residual_bps": basis_residual_bps,
                "absolute_basis_residual_bps": abs(basis_residual_bps),
                "entry_day_of_week": entry_day.weekday(),
                "expiration_month": expiration_day.month,
                "entry_future_volume": float(entry_row.get("future_volume", 0) or 0),
                "entry_share_volume": float(entry_row.get("share_volume", 0) or 0),
                "trailing_basis_mean_10d_bps": statistics.fmean(trailing_basis),
                "trailing_basis_std_10d_bps": (
                    statistics.pstdev(trailing_basis) if len(trailing_basis) > 1 else 0.0
                ),
                **prior_features,
                "known_dividend_events": len(known_dividends),
                "known_dividend_bps": known_dividend_bps,
                "exit_basis_bps": exit_basis,
                "raw_convergence_bps": raw_convergence_bps,
                "dividend_adjusted_convergence_bps": raw_convergence_bps
                + side * known_dividend_bps,
                "carry_adjusted_convergence_bps": side
                * (entry_basis - exit_basis + known_dividend_bps - financing_bps),
                "future_return_bps": (
                    float(exit_row["future_close"]) / float(entry_row["future_close"]) - 1.0
                )
                * 10_000,
                "share_return_bps": (
                    float(exit_row["share_close"]) / float(entry_row["share_close"]) - 1.0
                )
                * 10_000,
            }
        )
    return observations


def chronological_expiration_split(
    rows: Sequence[Mapping[str, Any]],
    *,
    discovery_fraction: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    expirations = sorted({str(row["expiration"]) for row in rows})
    if len(expirations) < 2:
        return [dict(row) for row in rows], []
    split = max(1, min(len(expirations) - 1, int(len(expirations) * discovery_fraction)))
    discovery_expirations = set(expirations[:split])
    return (
        [dict(row) for row in rows if str(row["expiration"]) in discovery_expirations],
        [dict(row) for row in rows if str(row["expiration"]) not in discovery_expirations],
    )


def summarize_rule(
    rows: Sequence[Mapping[str, Any]],
    *,
    trading_days_to_exit: int,
    minimum_basis_bps: float,
    round_trip_cost_bps: float,
) -> dict[str, Any]:
    selected = [
        row
        for row in rows
        if int(row["trading_days_to_exit"]) == trading_days_to_exit
        and float(row.get("absolute_basis_residual_bps", abs(float(row["entry_basis_bps"]))))
        >= minimum_basis_bps
    ]
    net_results = [
        float(
            row.get(
                "carry_adjusted_convergence_bps",
                row.get("dividend_adjusted_convergence_bps", row["raw_convergence_bps"]),
            )
        )
        - round_trip_cost_bps
        for row in selected
    ]
    successes = sum(value > 0 for value in net_results)
    expiration_counts = Counter(str(row["expiration"]) for row in selected)
    base_counts = Counter(str(row["basic_asset"]) for row in selected)
    return {
        "rows": len(selected),
        "expirations": len(expiration_counts),
        "basic_assets": len(base_counts),
        "successes": successes,
        "success_rate": successes / len(selected) if selected else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, len(selected)) or 0.0,
        "mean_net_bps": statistics.fmean(net_results) if net_results else 0.0,
        "median_net_bps": statistics.median(net_results) if net_results else 0.0,
        "maximum_expiration_share": (
            max(expiration_counts.values()) / len(selected) if selected else 0.0
        ),
        "maximum_basic_asset_share": max(base_counts.values()) / len(selected) if selected else 0.0,
    }


def evaluate_grid(
    discovery: Sequence[Mapping[str, Any]],
    holdout: Sequence[Mapping[str, Any]],
    *,
    days_to_exit: Sequence[int],
    basis_thresholds_bps: Sequence[float],
    round_trip_cost_bps: float,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    grid: list[dict[str, Any]] = []
    for trading_days in days_to_exit:
        for threshold in basis_thresholds_bps:
            row = {
                "trading_days_to_exit": int(trading_days),
                "minimum_basis_bps": float(threshold),
                "round_trip_cost_bps": round_trip_cost_bps,
                "discovery": summarize_rule(
                    discovery,
                    trading_days_to_exit=int(trading_days),
                    minimum_basis_bps=float(threshold),
                    round_trip_cost_bps=round_trip_cost_bps,
                ),
                "holdout": summarize_rule(
                    holdout,
                    trading_days_to_exit=int(trading_days),
                    minimum_basis_bps=float(threshold),
                    round_trip_cost_bps=round_trip_cost_bps,
                ),
            }
            grid.append(row)
    eligible = [
        row
        for row in grid
        if row["discovery"]["rows"] >= 100
        and row["discovery"]["expirations"] >= 8
        and row["discovery"]["basic_assets"] >= 8
        and row["discovery"]["mean_net_bps"] > 0
        and row["discovery"]["maximum_expiration_share"] <= 0.25
        and row["discovery"]["maximum_basic_asset_share"] <= 0.15
    ]
    selected = max(
        eligible,
        key=lambda row: (
            row["discovery"]["wilson_lower_95"],
            row["discovery"]["success_rate"],
            row["discovery"]["rows"],
        ),
        default=None,
    )
    return grid, selected


def _parse_numbers(raw: str, *, integer: bool) -> tuple[int, ...] | tuple[float, ...]:
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    return tuple(int(item) for item in values) if integer else tuple(float(item) for item in values)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-future-basis-convergence")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/future_basis_convergence/v1"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/future_basis_convergence/current"))
    parser.add_argument("--first-expiration", type=date.fromisoformat, default=date(2022, 1, 1))
    parser.add_argument("--last-expiration", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    parser.add_argument("--lookback-calendar-days", type=int, default=60)
    parser.add_argument("--days-to-exit", default=",".join(map(str, DEFAULT_DAYS_TO_EXIT)))
    parser.add_argument(
        "--basis-thresholds-bps",
        default=",".join(f"{value:g}" for value in DEFAULT_BASIS_THRESHOLDS_BPS),
    )
    parser.add_argument("--round-trip-cost-bps", type=float, default=20.0)
    parser.add_argument("--discovery-fraction", type=float, default=0.70)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--request-timeout", type=float, default=60.0)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.first_expiration > args.last_expiration:
        raise SystemExit("--first-expiration must not be after --last-expiration")
    days_to_exit = _parse_numbers(args.days_to_exit, integer=True)
    thresholds = _parse_numbers(args.basis_thresholds_bps, integer=False)
    token = load_env_value(args.env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if args.ca_cert:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=args.ca_cert)
    if args.insecure_skip_tls_verify:
        verify = False
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-app-name": "investment-signals-future-basis-research",
    }
    failures: list[dict[str, str]] = []
    all_observations: list[dict[str, Any]] = []
    cached_contracts = 0
    key_rate_rows = fetch_or_read_key_rate(
        cache_dir=args.cache_dir,
        start_day=args.first_expiration - timedelta(days=args.lookback_calendar_days),
        end_day=args.last_expiration,
        timeout=args.request_timeout,
        verify=verify,
    )
    with httpx.Client(headers=headers, timeout=args.request_timeout, verify=verify) as client:
        contracts = _catalog_contracts(
            client,
            attempts=args.request_attempts,
            first_expiration=args.first_expiration,
            last_expiration=args.last_expiration,
        )
        dividend_events = fetch_or_read_dividends(
            client,
            contracts=contracts,
            cache_dir=args.cache_dir,
            start_day=args.first_expiration - timedelta(days=args.lookback_calendar_days),
            end_day=args.last_expiration,
            attempts=args.request_attempts,
        )

        def load(contract: FutureContract) -> tuple[list[dict[str, Any]], bool]:
            return fetch_or_read_contract(
                client,
                contract=contract,
                cache_dir=args.cache_dir,
                lookback_calendar_days=args.lookback_calendar_days,
                attempts=args.request_attempts,
            )

        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as pool:
            jobs = {pool.submit(load, contract): contract for contract in contracts}
            for future in as_completed(jobs):
                contract = jobs[future]
                try:
                    rows, was_cached = future.result()
                    cached_contracts += int(was_cached)
                    all_observations.extend(
                        build_basis_observations(
                            rows,
                            days_to_exit=days_to_exit,
                            dividend_events=dividend_events,
                            key_rate_rows=key_rate_rows,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - isolated research partition failure
                    failures.append(
                        {
                            "future_ticker": contract.ticker,
                            "reason_code": "contract_history_unavailable",
                            "diagnostic": redact_diagnostic(type(exc).__name__),
                        }
                    )
    discovery, holdout = chronological_expiration_split(
        all_observations,
        discovery_fraction=args.discovery_fraction,
    )
    grid, selected = evaluate_grid(
        discovery,
        holdout,
        days_to_exit=days_to_exit,
        basis_thresholds_bps=thresholds,
        round_trip_cost_bps=args.round_trip_cost_bps,
    )
    accepted = bool(
        selected
        and selected["holdout"]["rows"] >= 100
        and selected["holdout"]["expirations"] >= 5
        and selected["holdout"]["basic_assets"] >= 8
        and selected["holdout"]["success_rate"] >= 0.90
        and selected["holdout"]["wilson_lower_95"] >= 0.85
        and selected["holdout"]["mean_net_bps"] > 0
        and selected["holdout"]["maximum_expiration_share"] <= 0.25
        and selected["holdout"]["maximum_basic_asset_share"] <= 0.15
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_table(args.output_dir / "observations.parquet", all_observations)
    write_json(
        args.output_dir / "result.json",
        {
            "schema_version": 1,
            "kind": "future_basis_convergence_research",
            "as_of": datetime.now(timezone.utc).isoformat(),
            "source": "T-Invest daily exchange candles, completed futures catalog, dividends known at signal time, and Bank of Russia key-rate history",
            "contracts": len(contracts),
            "cached_contracts": cached_contracts,
            "failed_contracts": len(failures),
            "observations": len(all_observations),
            "discovery_expirations": len({row["expiration"] for row in discovery}),
            "holdout_expirations": len({row["expiration"] for row in holdout}),
            "selected_on_discovery": selected,
            "accepted": accepted,
            "acceptance_rule": {
                "minimum_holdout_rows": 100,
                "minimum_holdout_expirations": 5,
                "minimum_holdout_basic_assets": 8,
                "minimum_success_rate": 0.90,
                "minimum_wilson_lower_95": 0.85,
                "positive_mean_after_costs": True,
            },
            "privacy": {
                "token_persisted": False,
                "instrument_uids_persisted": False,
                "account_data_persisted": False,
            },
            "failures": failures,
        },
    )
    write_json(args.output_dir / "grid.json", {"schema_version": 1, "rows": grid})
    report = [
        "# Схождение поставочных фьючерсов с акциями",
        "",
        f"Проверено контрактов: {len(contracts)}; наблюдений: {len(all_observations)}.",
        f"Поздних дат исполнения в контрольной части: {len({row['expiration'] for row in holdout})}.",
        f"Кандидат прошёл критерий 90%: {'да' if accepted else 'нет'}.",
        "",
        "Правило выбиралось только на ранних датах исполнения. Поздняя часть не участвовала в выборе.",
        "Результат измеряет относительное схождение фьючерса и акции после заданных издержек, а не направление всей акции.",
        "",
        "## Выбранное правило",
        "",
        "```json",
        json.dumps(selected, ensure_ascii=False, indent=2) if selected else "null",
        "```",
        "",
    ]
    (args.output_dir / "report.md").write_text("\n".join(report), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "accepted" if accepted else "not_accepted",
                "contracts": len(contracts),
                "cached_contracts": cached_contracts,
                "failed_contracts": len(failures),
                "observations": len(all_observations),
                "result": str(args.output_dir / "result.json"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
