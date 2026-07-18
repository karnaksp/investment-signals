#!/usr/bin/env python3
"""Cache the public T-Invest signal archive for reproducible local research.

The cache stores public signal attributes and ticker metadata only. Broker
tokens, account data, instrument UIDs, FIGIs, and raw signal/strategy IDs are
never persisted. Existing valid cache files are reused unless ``--refresh`` is
passed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import ssl
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_cache_tinvest_candles import api_post  # noqa: E402
from research_price_prediction_lib import (  # noqa: E402
    UTC,
    fingerprint_records,
    load_env_value,
    quotation,
    read_table,
    redact_diagnostic,
    write_json,
    write_table,
)


SCRIPT_VERSION = "research-native-signals-cache-v1.0.0"
SIGNALS_FILE = "signals.parquet"
STRATEGIES_FILE = "strategies.parquet"
SIGNAL_FIELDS = (
    "signal_key",
    "strategy_key",
    "strategy_name",
    "strategy_type",
    "ticker",
    "class_code",
    "instrument_type",
    "instrument_name",
    "create_at",
    "end_at",
    "close_at",
    "direction",
    "probability",
    "initial_price",
    "target_price",
    "stop_loss",
    "close_price",
    "planned_duration_seconds",
    "target_distance_bps",
    "stop_distance_bps",
    "broker_signed_return_bps",
    "closed",
)
STRATEGY_FIELDS = (
    "strategy_key",
    "strategy_name",
    "strategy_type",
    "active_signals",
    "total_signals",
    "time_in_position_seconds",
)
CATALOG_METHODS = (
    ("Shares", "share"),
    ("Etfs", "etf"),
    ("Futures", "future"),
    ("Currencies", "currency"),
    ("Bonds", "bond"),
)


def stable_key(value: object) -> str:
    """Return a non-reversible local key for a public API identifier."""

    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:24]


def _parse_at(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _distance_bps(price: float, initial: float) -> float | None:
    if price <= 0 or initial <= 0:
        return None
    return ((price / initial) - 1.0) * 10_000.0


def _direction(value: object) -> int:
    if value == "SIGNAL_DIRECTION_BUY":
        return 1
    if value == "SIGNAL_DIRECTION_SELL":
        return -1
    return 0


def build_instrument_catalog(
    client: httpx.Client,
    *,
    attempts: int,
) -> dict[str, dict[str, Any]]:
    """Resolve API-only instrument UIDs in memory and return public metadata."""

    catalog: dict[str, dict[str, Any]] = {}
    for method, instrument_type in CATALOG_METHODS:
        body = api_post(
            client,
            f"InstrumentsService/{method}",
            {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
            attempts=attempts,
        )
        for item in body.get("instruments", []):
            if not isinstance(item, Mapping) or not item.get("uid"):
                continue
            catalog[str(item["uid"])] = {
                "ticker": str(item.get("ticker") or ""),
                "class_code": str(item.get("classCode") or ""),
                "instrument_type": instrument_type,
                "instrument_name": str(item.get("name") or ""),
            }
    return catalog


def fetch_strategies(
    client: httpx.Client,
    *,
    attempts: int,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    body = api_post(client, "SignalService/GetStrategies", {}, attempts=attempts)
    rows: list[dict[str, Any]] = []
    lookup: dict[str, dict[str, Any]] = {}
    for item in body.get("strategies", []):
        if not isinstance(item, Mapping) or not item.get("strategyId"):
            continue
        raw_id = str(item["strategyId"])
        public = {
            "strategy_key": stable_key(raw_id),
            "strategy_name": str(item.get("strategyName") or ""),
            "strategy_type": str(item.get("strategyType") or ""),
            "active_signals": int(item.get("activeSignals") or 0),
            "total_signals": int(item.get("totalSignals") or 0),
            "time_in_position_seconds": int(item.get("timeInPosition") or 0),
        }
        rows.append(public)
        lookup[raw_id] = public
    return sorted(rows, key=lambda row: (row["strategy_type"], row["strategy_name"])), lookup


def fetch_all_signals(
    client: httpx.Client,
    *,
    attempts: int,
    page_size: int,
) -> list[Mapping[str, Any]]:
    page = 0
    result: list[Mapping[str, Any]] = []
    while True:
        body = api_post(
            client,
            "SignalService/GetSignals",
            {
                "active": "SIGNAL_STATE_ALL",
                "paging": {"limit": page_size, "pageNumber": page},
            },
            attempts=attempts,
        )
        batch = [item for item in body.get("signals", []) if isinstance(item, Mapping)]
        result.extend(batch)
        paging = body.get("paging") if isinstance(body.get("paging"), Mapping) else {}
        total = int(paging.get("totalCount") or len(result))
        if not batch or len(result) >= total:
            break
        page += 1
    return result


def normalize_signal(
    item: Mapping[str, Any],
    *,
    instruments: Mapping[str, Mapping[str, Any]],
    strategies: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    strategy_id = str(item.get("strategyId") or "")
    strategy = strategies.get(strategy_id, {})
    instrument = instruments.get(str(item.get("instrumentUid") or ""), {})
    created = _parse_at(item.get("createDt"))
    ended = _parse_at(item.get("endDt"))
    closed_at = _parse_at(item.get("closeDt"))
    direction = _direction(item.get("direction"))
    initial = quotation(item.get("initialPrice"))
    target = quotation(item.get("targetPrice"))
    stop = quotation(item.get("stoploss"))
    close = quotation(item.get("closePrice"))
    target_distance = _distance_bps(target, initial)
    stop_distance = _distance_bps(stop, initial)
    signed_return = _distance_bps(close, initial)
    if signed_return is not None:
        signed_return *= direction
    duration = None
    if created is not None and ended is not None:
        duration = max(0, int((ended - created).total_seconds()))
    return {
        "signal_key": stable_key(item.get("signalId") or json.dumps(item, sort_keys=True, default=str)),
        "strategy_key": str(strategy.get("strategy_key") or stable_key(strategy_id)),
        "strategy_name": str(item.get("strategyName") or strategy.get("strategy_name") or ""),
        "strategy_type": str(strategy.get("strategy_type") or ""),
        "ticker": str(instrument.get("ticker") or ""),
        "class_code": str(instrument.get("class_code") or ""),
        "instrument_type": str(instrument.get("instrument_type") or "unknown"),
        "instrument_name": str(instrument.get("instrument_name") or ""),
        "create_at": created.isoformat() if created else "",
        "end_at": ended.isoformat() if ended else "",
        "close_at": closed_at.isoformat() if closed_at else "",
        "direction": direction,
        "probability": int(item.get("probability") or 0),
        "initial_price": initial,
        "target_price": target,
        "stop_loss": stop,
        "close_price": close,
        "planned_duration_seconds": duration,
        "target_distance_bps": target_distance,
        "stop_distance_bps": stop_distance,
        "broker_signed_return_bps": signed_return,
        "closed": bool(closed_at and close > 0),
    }


def valid_cache(cache_dir: Path) -> bool:
    manifest = cache_dir / "manifest.json"
    signals = cache_dir / SIGNALS_FILE
    strategies = cache_dir / STRATEGIES_FILE
    if not (manifest.is_file() and signals.is_file() and strategies.is_file()):
        return False
    try:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        return payload.get("kind") == "tinvest_native_signal_research_cache" and bool(read_table(signals))
    except (OSError, ValueError, RuntimeError):
        return False


def run_cache(
    *,
    env_file: Path,
    cache_dir: Path,
    refresh: bool,
    request_timeout: float,
    request_attempts: int,
    page_size: int,
    ca_cert: Path | None,
) -> dict[str, Any]:
    if not refresh and valid_cache(cache_dir):
        return json.loads((cache_dir / "manifest.json").read_text(encoding="utf-8"))

    token = load_env_value(env_file, "TINVEST_TOKEN")
    verify: bool | ssl.SSLContext = True
    if ca_cert is not None:
        verify = ssl.create_default_context()
        verify.load_verify_locations(cafile=ca_cert)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "x-app-name": "investment-signals-native-signal-research-cache",
    }
    timeout = httpx.Timeout(
        request_timeout,
        connect=min(request_timeout, 10.0),
        read=request_timeout,
        write=min(request_timeout, 10.0),
        pool=min(request_timeout, 10.0),
    )
    with httpx.Client(headers=headers, timeout=timeout, verify=verify) as client:
        instruments = build_instrument_catalog(client, attempts=request_attempts)
        strategy_rows, strategy_lookup = fetch_strategies(client, attempts=request_attempts)
        raw_signals = fetch_all_signals(
            client,
            attempts=request_attempts,
            page_size=page_size,
        )

    signal_rows = [
        normalize_signal(item, instruments=instruments, strategies=strategy_lookup)
        for item in raw_signals
    ]
    signal_rows.sort(key=lambda row: (row["create_at"], row["signal_key"]))
    write_table(cache_dir / SIGNALS_FILE, signal_rows, fields=SIGNAL_FIELDS)
    write_table(cache_dir / STRATEGIES_FILE, strategy_rows, fields=STRATEGY_FIELDS)
    dates = [row["create_at"] for row in signal_rows if row["create_at"]]
    mapped = sum(bool(row["ticker"]) for row in signal_rows)
    manifest = {
        "schema_version": 1,
        "kind": "tinvest_native_signal_research_cache",
        "created_at": datetime.now(UTC).isoformat(),
        "script_version": SCRIPT_VERSION,
        "scope": {
            "source_type": "SignalService/GetSignals",
            "from": min(dates) if dates else None,
            "to": max(dates) if dates else None,
        },
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
            "figis_persisted": False,
            "raw_signal_ids_persisted": False,
            "raw_strategy_ids_persisted": False,
        },
        "quality": {
            "signal_rows": len(signal_rows),
            "closed_signal_rows": sum(bool(row["closed"]) for row in signal_rows),
            "mapped_instrument_rows": mapped,
            "unmapped_instrument_rows": len(signal_rows) - mapped,
            "strategy_rows": len(strategy_rows),
        },
        "content_fingerprint": fingerprint_records(signal_rows),
    }
    write_json(cache_dir / "manifest.json", manifest)
    return manifest


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-cache-tinvest-native-signals")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_native_signals/v1"),
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=7)
    parser.add_argument("--ca-cert", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not 1 <= args.page_size <= 1000:
        raise SystemExit("--page-size must be between 1 and 1000")
    try:
        manifest = run_cache(
            env_file=args.env_file,
            cache_dir=args.cache_dir,
            refresh=args.refresh,
            request_timeout=args.request_timeout,
            request_attempts=args.request_attempts,
            page_size=args.page_size,
            ca_cert=args.ca_cert,
        )
    except Exception as exc:
        write_json(
            args.cache_dir / "failure-summary.json",
            {
                "schema_version": 1,
                "kind": "tinvest_native_signal_research_cache_failure",
                "reason_code": "tinvest_native_signal_cache_failed",
                "diagnostic": redact_diagnostic(exc),
            },
        )
        raise
    print(json.dumps({"status": "ok", "cache_dir": str(args.cache_dir), **manifest["quality"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
