"""ClickHouse destination adapter for historical scientific candles."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.adapters.clickhouse_scientific_candles import (
    ClickHouseScientificCandleStore,
)
from tinvest_signal_engine.application.historical_candle_import import (
    HistoricalDestinationPartition,
)
from tinvest_signal_engine.domain.historical_candle_import import (
    PersistedCandleSnapshot,
)
from tinvest_signal_engine.domain.scientific_candles import ScientificCandle


SELECT_PARTITIONS_SQL = """
SELECT
    instrument_id,
    ticker,
    trading_day,
    candle_at,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    is_complete,
    source_kind,
    payload_fingerprint
FROM scientific_candles_1m FINAL
WHERE {partition_predicate}
ORDER BY candle_at
FORMAT JSONEachRow
""".strip()


class ClickHouseHistoricalCandleImportDestination:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 30.0,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if not database.strip() or not username.strip():
            raise ValueError("ClickHouse database and username are required")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds
        self._writer = ClickHouseScientificCandleStore(
            base_url=base_url,
            database=database,
            username=username,
            password=password,
            timeout_seconds=timeout_seconds,
        )

    def persist_many(self, candles: tuple[ScientificCandle, ...]) -> None:
        self._writer.persist_many(candles)

    def inspect_partitions(
        self,
        requests: tuple[HistoricalDestinationPartition, ...],
    ) -> tuple[PersistedCandleSnapshot, ...]:
        if not requests:
            return ()
        query_parameters: dict[str, str] = {
                "database": self._database,
                "date_time_input_format": "best_effort",
        }
        predicates: list[str] = []
        for index, item in enumerate(requests):
            query_parameters[f"param_instrument_id_{index}"] = item.instrument_id
            query_parameters[f"param_trading_day_{index}"] = (
                item.descriptor.key.trading_day.isoformat()
            )
            predicates.append(
                f"(instrument_id = {{instrument_id_{index}:String}} AND "
                f"trading_day = {{trading_day_{index}:Date}})"
            )
        parameters = urlencode(query_parameters)
        sql = SELECT_PARTITIONS_SQL.format(
            partition_predicate=" OR ".join(predicates)
        )
        request = Request(
            f"{self._base_url}/?{parameters}",
            data=(sql + "\n").encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8")
        except HTTPError as error:
            raise RuntimeError(
                f"ClickHouse historical candle inspection failed with status {error.code}"
            ) from error
        except URLError as error:
            raise RuntimeError(
                "ClickHouse historical candle inspection connection failed"
            ) from error
        return tuple(
            _snapshot(json.loads(line))
            for line in payload.splitlines()
            if line.strip()
        )


def _snapshot(row: object) -> PersistedCandleSnapshot:
    if not isinstance(row, dict):
        raise ValueError("ClickHouse historical candle row must be an object")
    return PersistedCandleSnapshot(
        instrument_id=str(row.get("instrument_id", "")),
        ticker=str(row.get("ticker", "")).strip().upper(),
        trading_day=date.fromisoformat(str(row["trading_day"])),
        candle_at=_timestamp(row["candle_at"]),
        open_price=Decimal(str(row["open_price"])),
        high_price=Decimal(str(row["high_price"])),
        low_price=Decimal(str(row["low_price"])),
        close_price=Decimal(str(row["close_price"])),
        volume=int(row["volume"]),
        complete=str(row["is_complete"]).strip().lower() in {"1", "true", "yes"},
        source_kind=str(row["source_kind"]),
        payload_fingerprint="sha256:"
        + str(row["payload_fingerprint"]).removeprefix("sha256:"),
    )


def _timestamp(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
