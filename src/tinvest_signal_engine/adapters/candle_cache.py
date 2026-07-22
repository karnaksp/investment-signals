"""T-Invest, Parquet and filesystem adapters for the reusable candle cache."""

from __future__ import annotations

import csv
from datetime import UTC, datetime, time, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
import ssl
import time as clock
from typing import Any, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from tinvest_signal_engine.domain.candle_cache import (
    CachedCandle,
    CandleCacheInventory,
    CandleCacheReceipt,
    CandlePartitionKey,
    CandlePartitionState,
)


_API_ROOT = "https://invest-public-api.tbank.ru/rest/"
_API_SERVICE = "tinkoff.public.invest.api.contract.v1"
_MOSCOW = ZoneInfo("Europe/Moscow")
_FIELDS = (
    "ticker",
    "at",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "volume_buy",
    "volume_sell",
    "complete",
)


def _trusted_ssl_context(ca_bundle_path: str | Path | None) -> ssl.SSLContext:
    if ca_bundle_path is None:
        return ssl.create_default_context()
    path = Path(ca_bundle_path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"Trusted CA bundle does not exist: {path}")
    try:
        return ssl.create_default_context(cafile=str(path))
    except (OSError, ssl.SSLError) as error:
        raise ValueError(f"Trusted CA bundle could not be loaded: {path}") from error


class TInvestRestCandleHistorySource:
    """Read exchange candles without exposing broker identifiers to the cache."""

    def __init__(
        self,
        *,
        token: str,
        timeout_seconds: float = 30.0,
        attempts: int = 5,
        request_interval_seconds: float = 0.05,
        ca_bundle_path: str | Path | None = None,
        ssl_context: ssl.SSLContext | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = clock.sleep,
    ) -> None:
        if not token.strip():
            raise ValueError("T-Invest token must not be empty")
        if attempts < 1:
            raise ValueError("attempts must be positive")
        if ca_bundle_path is not None and ssl_context is not None:
            raise ValueError("Set only one of ca_bundle_path and ssl_context")
        self._attempts = attempts
        self._request_interval_seconds = max(0.0, request_interval_seconds)
        self._sleep = sleep
        self._instrument_uids: dict[str, str] = {}
        self._owns_client = client is None
        verify = (
            ssl_context
            if ssl_context is not None
            else _trusted_ssl_context(ca_bundle_path)
        )
        self._client = client or httpx.Client(
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "x-app-name": "investment-signals-candle-cache",
            },
            timeout=httpx.Timeout(timeout_seconds),
            verify=verify,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch(self, key: CandlePartitionKey) -> tuple[CachedCandle, ...]:
        instrument_uid = self._instrument_uid(key.ticker)
        start = datetime.combine(key.trading_day, time(6, 50), tzinfo=_MOSCOW)
        end = datetime.combine(
            key.trading_day + timedelta(days=1),
            time.min,
            tzinfo=_MOSCOW,
        )
        payload = self._post(
            "MarketDataService/GetCandles",
            {
                "from": _api_time(start),
                "to": _api_time(end),
                "interval": "CANDLE_INTERVAL_1_MIN",
                "instrumentId": instrument_uid,
                "candleSourceType": "CANDLE_SOURCE_EXCHANGE",
            },
        )
        rows = tuple(
            CachedCandle(
                ticker=key.ticker,
                at=datetime.fromisoformat(str(item["time"]).replace("Z", "+00:00")),
                open=_quotation(item.get("open")),
                high=_quotation(item.get("high")),
                low=_quotation(item.get("low")),
                close=_quotation(item.get("close")),
                volume=float(item.get("volume", 0) or 0),
                volume_buy=float(item.get("volumeBuy", 0) or 0),
                volume_sell=float(item.get("volumeSell", 0) or 0),
                complete=bool(item.get("isComplete", False)),
            )
            for item in payload.get("candles", ())
            if isinstance(item, Mapping)
        )
        if self._request_interval_seconds:
            self._sleep(self._request_interval_seconds)
        return rows

    def _instrument_uid(self, ticker: str) -> str:
        known = self._instrument_uids.get(ticker)
        if known is not None:
            return known
        payload = self._post(
            "InstrumentsService/FindInstrument",
            {
                "query": ticker,
                "instrumentKind": "INSTRUMENT_TYPE_SHARE",
                "apiTradeAvailableFlag": True,
            },
        )
        matches = [
            item
            for item in payload.get("instruments", ())
            if isinstance(item, Mapping)
            and item.get("ticker") == ticker
            and item.get("classCode") == "TQBR"
            and item.get("uid")
        ]
        if len(matches) != 1:
            raise RuntimeError("canonical TQBR instrument was not resolved")
        resolved = str(matches[0]["uid"])
        self._instrument_uids[ticker] = resolved
        return resolved

    def _post(self, method: str, payload: Mapping[str, object]) -> Mapping[str, Any]:
        url = f"{_API_ROOT}{_API_SERVICE}.{method}"
        for attempt in range(self._attempts):
            try:
                response = self._client.post(url, json=payload)
                if response.status_code == 200:
                    body = response.json()
                    if not isinstance(body, Mapping):
                        raise RuntimeError("T-Invest response is not an object")
                    return body
                if response.status_code not in {429, 500, 502, 503, 504}:
                    raise RuntimeError(
                        f"T-Invest request failed with HTTP {response.status_code}"
                    )
            except httpx.HTTPError:
                pass
            if attempt + 1 < self._attempts:
                self._sleep(min(20.0, 0.75 * (2**attempt)))
        raise RuntimeError("T-Invest request failed after retries")


class ParquetCandlePartitionRepository:
    """Validate and atomically replace cache partitions in the established layout."""

    def __init__(self, cache_dir: str | Path) -> None:
        self._cache_dir = Path(cache_dir)
        self._database: Any | None = None

    def close(self) -> None:
        database, self._database = self._database, None
        if database is not None:
            database.close()

    def inspect(self, key: CandlePartitionKey) -> CandlePartitionState:
        path = self._path(key)
        if not path.is_file() or path.stat().st_size <= 0:
            return CandlePartitionState(key=key, valid=False)
        try:
            records = self._read(path)
            self._validate(key, records)
        except Exception:
            return CandlePartitionState(key=key, valid=False)
        return CandlePartitionState(key=key, valid=True, row_count=len(records))

    def inspect_many(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> tuple[CandlePartitionState, ...]:
        try:
            summaries = self._inspect_many_batch(keys)
        except Exception:
            # A corrupt footer can fail the multi-file table function before
            # it identifies the offending path.  Isolate it on the slow path.
            return tuple(self.inspect(key) for key in keys)
        return tuple(
            summaries.get(key, CandlePartitionState(key, False)) for key in keys
        )

    def _inspect_many_batch(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> dict[CandlePartitionKey, CandlePartitionState]:
        key_by_path: dict[str, CandlePartitionKey] = {}
        for key in keys:
            path = self._path(key)
            if not path.is_file() or path.stat().st_size <= 0:
                continue
            resolved = str(path.resolve())
            if resolved in key_by_path:
                raise ValueError("partition paths must be unique")
            key_by_path[resolved] = key
        if not key_by_path:
            return {}

        # Initial cache discovery only needs structural invariants.  The
        # immutable content fingerprint is calculated once when the final
        # inventory is sealed, after missing or corrupt partitions are fixed.
        database = self._database_connection()
        paths = tuple(sorted(key_by_path))
        schema_rows = database.execute(
            "SELECT file_name, name FROM parquet_schema(?)",
            [list(paths)],
        ).fetchall()
        fields_by_path: dict[str, set[str]] = {}
        for raw_path, raw_name in schema_rows:
            path = str(Path(str(raw_path)).resolve())
            if path in key_by_path and raw_name is not None:
                fields_by_path.setdefault(path, set()).add(str(raw_name))
        valid_paths = tuple(
            path
            for path in paths
            if set(_FIELDS).issubset(fields_by_path.get(path, set()))
        )
        if not valid_paths:
            return {}

        metadata_rows = database.execute(
            "SELECT file_name, MAX(num_rows) "
            "FROM parquet_file_metadata(?) GROUP BY file_name",
            [list(valid_paths)],
        ).fetchall()
        expected_rows = {
            str(Path(str(raw_path)).resolve()): int(row_count)
            for raw_path, row_count in metadata_rows
        }
        valid_paths = tuple(path for path in valid_paths if path in expected_rows)
        if not valid_paths:
            return {}

        rows = database.execute(
            "SELECT file_name, path_in_schema, MIN(stats_min_value), "
            "MAX(stats_max_value), SUM(num_values), SUM(stats_null_count) "
            "FROM parquet_metadata(?) "
            "GROUP BY file_name, path_in_schema",
            [list(valid_paths)],
        ).fetchall()
        metadata_by_path: dict[str, dict[str, tuple[str, str, int, int]]] = {}
        for raw_path, raw_field, raw_min, raw_max, raw_values, raw_nulls in rows:
            path = str(Path(str(raw_path)).resolve())
            if path not in key_by_path or raw_field is None:
                continue
            metadata_by_path.setdefault(path, {})[str(raw_field)] = (
                str(raw_min),
                str(raw_max),
                int(raw_values),
                int(raw_nulls),
            )
        summaries: dict[CandlePartitionKey, CandlePartitionState] = {}
        for path in valid_paths:
            key = key_by_path[path]
            expected = expected_rows[path]
            if expected == 0:
                summaries[key] = CandlePartitionState(key, True, 0)
                continue
            fields = metadata_by_path.get(path, {})
            if not set(_FIELDS).issubset(fields):
                continue
            ticker_min, ticker_max, ticker_values, ticker_nulls = fields["ticker"]
            complete_min, complete_max, complete_values, complete_nulls = fields[
                "complete"
            ]
            at_min, at_max, at_values, at_nulls = fields["at"]
            min_day = _metadata_timestamp(at_min).astimezone(_MOSCOW).date()
            max_day = _metadata_timestamp(at_max).astimezone(_MOSCOW).date()
            if (
                ticker_values == expected
                and ticker_nulls == 0
                and ticker_min == key.ticker
                and ticker_max == key.ticker
                and complete_values == expected
                and complete_nulls == 0
                and complete_min.lower() == "true"
                and complete_max.lower() == "true"
                and at_values == expected
                and at_nulls == 0
                and min_day == key.trading_day
                and max_day == key.trading_day
            ):
                summaries[key] = CandlePartitionState(key, True, expected)
        return summaries

    def replace_atomically(
        self,
        key: CandlePartitionKey,
        candles: tuple[CachedCandle, ...],
    ) -> CandlePartitionState:
        records = tuple(_record(candle) for candle in candles)
        self._validate(key, records)
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        try:
            self._write(temporary, records)
            self._validate(key, self._read(temporary))
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
            temporary.with_suffix(temporary.suffix + ".csv").unlink(missing_ok=True)
        return self.inspect(key)

    def inventory(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> CandleCacheInventory:
        summaries, digest = self._scan_many(keys)
        row_counts = [
            (key.manifest_key, summaries[key][0])
            for key in sorted(
                summaries, key=lambda item: (item.ticker, item.trading_day)
            )
        ]
        morning_row_counts = [
            (key.manifest_key, summaries[key][1])
            for key in sorted(
                summaries, key=lambda item: (item.ticker, item.trading_day)
            )
            if summaries[key][1]
        ]
        return CandleCacheInventory(
            dataset_fingerprint=digest,
            rows_by_partition=tuple(sorted(row_counts)),
            morning_rows_by_partition=tuple(sorted(morning_row_counts)),
        )

    def _scan_many(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> tuple[dict[CandlePartitionKey, tuple[int, int]], str]:
        ordered = tuple(sorted(keys, key=lambda item: (item.ticker, item.trading_day)))
        key_by_path: dict[str, CandlePartitionKey] = {}
        for key in ordered:
            path = self._path(key)
            if not path.is_file() or path.stat().st_size <= 0:
                continue
            resolved = str(path.resolve())
            if resolved in key_by_path:
                return self._scan_many_fallback(ordered)
            key_by_path[resolved] = key
        if not key_by_path:
            return {}, sha256().hexdigest()
        try:
            return self._scan_many_batch(key_by_path)
        except Exception:
            # A corrupt Parquet footer can make a multi-file scan fail as a
            # whole.  The slow path isolates that file so every other valid
            # partition remains reusable and the corrupt one is re-fetched.
            return self._scan_many_fallback(ordered)

    def _scan_many_batch(
        self,
        key_by_path: Mapping[str, CandlePartitionKey],
    ) -> tuple[dict[CandlePartitionKey, tuple[int, int]], str]:
        database = self._database_connection()
        paths = tuple(sorted(key_by_path))
        schema_rows = database.execute(
            "SELECT file_name, name FROM parquet_schema(?)",
            [list(paths)],
        ).fetchall()
        fields_by_path: dict[str, set[str]] = {}
        for raw_path, raw_name in schema_rows:
            path = str(Path(str(raw_path)).resolve())
            if path in key_by_path and raw_name is not None:
                fields_by_path.setdefault(path, set()).add(str(raw_name))
        valid_paths = tuple(
            path
            for path in paths
            if set(_FIELDS).issubset(fields_by_path.get(path, set()))
        )
        if not valid_paths:
            return {}, sha256().hexdigest()

        metadata_rows = database.execute(
            "SELECT file_name, MAX(num_rows) "
            "FROM parquet_file_metadata(?) GROUP BY file_name",
            [list(valid_paths)],
        ).fetchall()
        expected_rows = {
            str(Path(str(raw_path)).resolve()): int(row_count)
            for raw_path, row_count in metadata_rows
        }
        valid_paths = tuple(path for path in valid_paths if path in expected_rows)
        if not valid_paths:
            return {}, sha256().hexdigest()

        quoted_fields = ", ".join(f'"{field}"' for field in _FIELDS)
        cursor = database.execute(
            f"SELECT filename, {quoted_fields} "
            "FROM read_parquet(?, filename=true, union_by_name=true) "
            'ORDER BY filename, "ticker", "at"',
            [list(valid_paths)],
        )
        columns = tuple(item[0] for item in cursor.description)
        if not {"filename", *_FIELDS}.issubset(columns):
            raise ValueError("candle partition schema is incomplete")

        digest = sha256()
        summaries: dict[CandlePartitionKey, tuple[int, int]] = {}
        current_path: str | None = None
        current_records: list[dict[str, object]] = []

        def finish() -> None:
            nonlocal current_path, current_records
            if current_path is None:
                return
            key = key_by_path[current_path]
            records = tuple(current_records)
            if len(records) != expected_rows[current_path]:
                raise ValueError("candle partition row count changed during scan")
            self._validate(key, records)
            _update_records_fingerprint(digest, records)
            summaries[key] = (
                len(records),
                sum(1 for record in records if _is_morning(_cached_candle(record).at)),
            )
            current_path = None
            current_records = []

        while rows := cursor.fetchmany(4096):
            for row in rows:
                record = dict(zip(columns, row))
                raw_path = str(Path(str(record.pop("filename"))).resolve())
                if raw_path not in key_by_path:
                    raise ValueError("Parquet scan returned an unexpected partition")
                if current_path is not None and raw_path != current_path:
                    finish()
                current_path = raw_path
                current_records.append(record)
        finish()

        for path in valid_paths:
            if expected_rows[path] == 0:
                key = key_by_path[path]
                summaries[key] = (0, 0)
            elif key_by_path[path] not in summaries:
                raise ValueError("non-empty candle partition was absent from scan")
        return summaries, digest.hexdigest()

    def _scan_many_fallback(
        self,
        keys: tuple[CandlePartitionKey, ...],
    ) -> tuple[dict[CandlePartitionKey, tuple[int, int]], str]:
        digest = sha256()
        summaries: dict[CandlePartitionKey, tuple[int, int]] = {}
        for key in keys:
            summary = self._inventory_partition(key, digest)
            if summary is not None:
                summaries[key] = summary
        return summaries, digest.hexdigest()

    def _inventory_partition(
        self,
        key: CandlePartitionKey,
        digest: Any,
    ) -> tuple[int, int] | None:
        path = self._path(key)
        if not path.is_file() or path.stat().st_size <= 0:
            return None
        try:
            records = self._read(path)
            self._validate(key, records)
        except Exception:
            return None
        morning_rows = sum(
            1 for record in records if _is_morning(_cached_candle(record).at)
        )
        _update_records_fingerprint(digest, records)
        return len(records), morning_rows

    def _path(self, key: CandlePartitionKey) -> Path:
        return (
            self._cache_dir
            / f"ticker={key.ticker}"
            / f"date={key.trading_day.isoformat()}.parquet"
        )

    def _read(self, path: Path) -> tuple[dict[str, object], ...]:
        database = self._database_connection()
        rows = database.execute(
            "SELECT * FROM read_parquet(?)",
            [str(path)],
        ).fetchall()
        columns = tuple(item[0] for item in database.description)
        if not set(_FIELDS).issubset(columns):
            raise ValueError("candle partition schema is incomplete")
        return tuple(dict(zip(columns, row)) for row in rows)

    def _write(
        self,
        path: Path,
        records: tuple[Mapping[str, object], ...],
    ) -> None:
        csv_path = path.with_suffix(path.suffix + ".csv")
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=_FIELDS)
            writer.writeheader()
            writer.writerows(records)
        database = self._database_connection()
        try:
            if records:
                source = _sql_literal(csv_path)
                output = _sql_literal(path)
                database.execute(
                    f"COPY (SELECT * FROM read_csv_auto('{source}')) "
                    f"TO '{output}' (FORMAT PARQUET)"
                )
            else:
                projection = ", ".join(
                    f'CAST(NULL AS VARCHAR) AS "{field}"' for field in _FIELDS
                )
                output = _sql_literal(path)
                database.execute(
                    f"COPY (SELECT {projection} WHERE false) "
                    f"TO '{output}' (FORMAT PARQUET)"
                )
        finally:
            csv_path.unlink(missing_ok=True)

    def _database_connection(self) -> Any:
        if self._database is None:
            self._database = _duckdb().connect(database=":memory:")
        return self._database

    @staticmethod
    def _validate(
        key: CandlePartitionKey,
        records: tuple[Mapping[str, object], ...],
    ) -> None:
        timestamps: set[datetime] = set()
        for record in records:
            candle = _cached_candle(record)
            if candle.ticker != key.ticker:
                raise ValueError("partition contains another ticker")
            if candle.at.astimezone(_MOSCOW).date() != key.trading_day:
                raise ValueError("partition contains another trading day")
            if not candle.complete:
                raise ValueError("historical partition contains an incomplete candle")
            if candle.at in timestamps:
                raise ValueError("partition contains duplicate timestamps")
            timestamps.add(candle.at)


class JsonCandleCacheManifest:
    """Publish the compatibility manifest and a redacted failure summary atomically."""

    def __init__(self, cache_dir: str | Path) -> None:
        self._cache_dir = Path(cache_dir)

    def publish(self, receipt: CandleCacheReceipt) -> None:
        failures = [
            {
                "ticker": item.key.ticker,
                "date": item.key.trading_day.isoformat(),
                "reason_code": item.reason_code,
            }
            for item in receipt.failures
        ]
        empty_partitions = [
            key for key, count in receipt.inventory.rows_by_partition if count == 0
        ]
        payload = {
            "schema_version": 1,
            "kind": "tinvest_research_candle_cache",
            "created_at": datetime.now(UTC).isoformat(),
            "script_version": "product-candle-cache-v1.0.0",
            "scope": {
                "tickers": list(receipt.scope.tickers),
                "from": receipt.scope.start_day.isoformat(),
                "to": receipt.scope.end_day.isoformat(),
                "interval": "1m",
                "source_type": "CANDLE_SOURCE_EXCHANGE",
                "session_window": "06:50-24:00 Europe/Moscow",
                "aggressor_volume_fields": ["volume_buy", "volume_sell"],
            },
            "privacy": {
                "tokens_persisted": False,
                "account_identifiers_persisted": False,
                "instrument_uids_persisted": False,
            },
            "quality": {
                "partition_count": len(receipt.inventory.rows_by_partition),
                "rows_by_partition": dict(receipt.inventory.rows_by_partition),
                "failed_partitions": failures,
                "empty_partitions": empty_partitions,
                "skipped_existing_partitions": receipt.skipped_partitions,
                "written_partitions": receipt.written_partitions,
                "morning_session": {
                    "window": "07:00-09:50 Europe/Moscow",
                    "partitions_with_rows": len(
                        receipt.inventory.morning_rows_by_partition
                    ),
                    "rows_by_partition": dict(
                        receipt.inventory.morning_rows_by_partition
                    ),
                    "rows_present": bool(receipt.inventory.morning_rows_by_partition),
                },
            },
            "content_fingerprint": receipt.inventory.dataset_fingerprint,
        }
        _atomic_json(self._cache_dir / "manifest.json", payload)
        failure_path = self._cache_dir / "failure-summary.json"
        if failures:
            _atomic_json(
                failure_path,
                {
                    "schema_version": 1,
                    "kind": "tinvest_research_candle_cache_failure",
                    "failures": failures,
                },
            )
        else:
            failure_path.unlink(missing_ok=True)


def _quotation(value: object) -> float:
    if not isinstance(value, Mapping):
        return 0.0
    return float(value.get("units", 0) or 0) + float(value.get("nano", 0) or 0) / 1e9


def _api_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _metadata_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _is_morning(value: datetime) -> bool:
    local = value.astimezone(_MOSCOW)
    minute = local.hour * 60 + local.minute
    return 7 * 60 <= minute < 9 * 60 + 50


def _record(candle: CachedCandle) -> dict[str, object]:
    return {
        "ticker": candle.ticker,
        "at": candle.at.astimezone(UTC).isoformat(),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "volume_buy": candle.volume_buy,
        "volume_sell": candle.volume_sell,
        "complete": candle.complete,
    }


def _cached_candle(record: Mapping[str, object]) -> CachedCandle:
    raw_at = record["at"]
    at = (
        raw_at
        if isinstance(raw_at, datetime)
        else datetime.fromisoformat(str(raw_at).replace("Z", "+00:00"))
    )
    if at.tzinfo is None or at.utcoffset() is None:
        at = at.replace(tzinfo=UTC)
    return CachedCandle(
        ticker=str(record["ticker"]),
        at=at,
        open=float(record["open"]),
        high=float(record["high"]),
        low=float(record["low"]),
        close=float(record["close"]),
        volume=float(record["volume"]),
        volume_buy=float(record.get("volume_buy", 0) or 0),
        volume_sell=float(record.get("volume_sell", 0) or 0),
        complete=str(record.get("complete", True)).lower() in {"1", "true", "yes"},
    )


def _update_records_fingerprint(
    digest: Any,
    records: tuple[Mapping[str, object], ...],
) -> None:
    canonical = (_record(_cached_candle(item)) for item in records)
    for record in sorted(
        canonical, key=lambda item: (str(item["ticker"]), str(item["at"]))
    ):
        digest.update(
            json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _duckdb() -> Any:
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError("DuckDB is required for the Parquet candle cache") from exc
    return duckdb


def _sql_literal(path: Path) -> str:
    return str(path).replace("'", "''")
