"""ClickHouse adapter for the immutable scientific one-minute candle journal."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.domain.scientific_candles import ScientificCandle


INSERT_SQL = """
INSERT INTO scientific_candles_1m
(
    instrument_id, ticker, exchange, trading_day, candle_at,
    open_price, high_price, low_price, close_price, volume, is_complete,
    source_kind, source_at, received_at, source_event_id,
    payload_fingerprint, has_gap, schema_version, record_version
)
FORMAT JSONEachRow
""".strip()


class ClickHouseScientificCandleStore:
    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def persist_many(self, candles: tuple[ScientificCandle, ...]) -> None:
        if not candles:
            return
        rows = "\n".join(
            json.dumps(_row(item), ensure_ascii=True, separators=(",", ":"))
            for item in candles
        )
        request = Request(
            f"{self._base_url}/?{urlencode({'database': self._database, 'date_time_input_format': 'best_effort'})}",
            data=(INSERT_SQL + "\n" + rows + "\n").encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                response.read()
        except HTTPError as error:
            raise RuntimeError(
                f"ClickHouse scientific candle insert failed with status {error.code}"
            ) from error
        except URLError as error:
            raise RuntimeError(
                "ClickHouse scientific candle insert connection failed"
            ) from error


def _row(candle: ScientificCandle) -> dict[str, object]:
    return {
        "instrument_id": candle.instrument_id,
        "ticker": candle.ticker,
        "exchange": candle.exchange,
        "trading_day": candle.trading_day.isoformat(),
        "candle_at": _datetime(candle.candle_at),
        "open_price": str(candle.open_price),
        "high_price": str(candle.high_price),
        "low_price": str(candle.low_price),
        "close_price": str(candle.close_price),
        "volume": candle.volume,
        "is_complete": int(candle.complete),
        "source_kind": candle.source_kind,
        "source_at": _datetime(candle.source_at),
        "received_at": _datetime(candle.received_at),
        "source_event_id": candle.source_event_id,
        "payload_fingerprint": candle.payload_fingerprint.removeprefix("sha256:"),
        "has_gap": int(candle.has_gap),
        "schema_version": candle.schema_version,
        "record_version": int(candle.received_at.timestamp() * 1_000_000),
    }


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
