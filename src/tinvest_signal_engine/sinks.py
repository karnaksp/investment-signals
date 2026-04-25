from __future__ import annotations

import json
import logging
import time
from typing import Any

import clickhouse_connect
import httpx

from .config import RuntimeSettings
from .models import TriggerSignal
from .serialization import json_dumps

logger = logging.getLogger(__name__)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


class ClickHouseSignalStore:
    def __init__(self, settings: RuntimeSettings):
        self._settings = settings
        self._client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_username,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    def ping(self) -> bool:
        return bool(self._client.ping())

    def close(self) -> None:
        self._client.close()

    def insert_signal(self, signal: TriggerSignal) -> None:
        self._client.insert(
            self._settings.clickhouse_table,
            [
                [
                    signal.signal_id,
                    signal.detected_at,
                    signal.instrument_id,
                    signal.ticker,
                    signal.class_code,
                    signal.alias,
                    signal.source_event_type,
                    signal.signal_type,
                    signal.severity,
                    signal.metric_value,
                    signal.baseline_value,
                    signal.z_score,
                    signal.window_seconds,
                    signal.summary,
                    json_dumps(signal.payload),
                ]
            ],
            column_names=[
                "signal_id",
                "detected_at",
                "instrument_id",
                "ticker",
                "class_code",
                "alias",
                "source_event_type",
                "signal_type",
                "severity",
                "metric_value",
                "baseline_value",
                "z_score",
                "window_seconds",
                "summary",
                "payload_json",
            ],
        )

    def fetch_recent(
        self, *, limit: int = 50, instrument_id: str | None = None
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 500))
        filter_clause = ""
        if instrument_id:
            filter_clause = (
                f"WHERE instrument_id = '{_escape(instrument_id.strip())}'"
            )
        query = f"""
            SELECT
                signal_id,
                detected_at,
                instrument_id,
                ticker,
                class_code,
                alias,
                source_event_type,
                signal_type,
                severity,
                metric_value,
                baseline_value,
                z_score,
                window_seconds,
                summary,
                payload_json
            FROM {self._settings.clickhouse_table}
            {filter_clause}
            ORDER BY detected_at DESC
            LIMIT {safe_limit}
        """
        result = self._client.query(query)
        rows: list[dict[str, Any]] = []
        for row in result.result_rows:
            rows.append(
                {
                    "signal_id": str(row[0]),
                    "detected_at": row[1].isoformat(),
                    "instrument_id": row[2],
                    "ticker": row[3],
                    "class_code": row[4],
                    "alias": row[5],
                    "source_event_type": row[6],
                    "signal_type": row[7],
                    "severity": row[8],
                    "metric_value": row[9],
                    "baseline_value": row[10],
                    "z_score": row[11],
                    "window_seconds": row[12],
                    "summary": row[13],
                    "payload": json.loads(row[14]),
                }
            )
        return rows

    def fetch_summary(self, *, minutes: int = 60) -> list[dict[str, Any]]:
        safe_minutes = max(1, min(minutes, 1_440))
        query = f"""
            SELECT
                signal_type,
                count() AS signal_count
            FROM {self._settings.clickhouse_table}
            WHERE detected_at >= now() - INTERVAL {safe_minutes} MINUTE
            GROUP BY signal_type
            ORDER BY signal_count DESC, signal_type ASC
        """
        result = self._client.query(query)
        return [
            {"signal_type": row[0], "signal_count": row[1]}
            for row in result.result_rows
        ]


class WebhookAlertSink:
    def __init__(self, webhook_url: str | None):
        self._webhook_url = webhook_url

    @property
    def enabled(self) -> bool:
        return bool(self._webhook_url)

    def send(self, signal: TriggerSignal) -> None:
        if not self._webhook_url:
            return
        with httpx.Client(timeout=5.0) as client:
            client.post(self._webhook_url, json=signal.to_dict())


def create_clickhouse_signal_store_with_retry(
    settings: RuntimeSettings,
    *,
    service_name: str,
) -> ClickHouseSignalStore:
    deadline = (
        time.monotonic() + settings.clickhouse_startup_timeout_seconds
    )
    attempt = 0
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        attempt += 1
        try:
            store = ClickHouseSignalStore(settings)
            store.ping()
            logger.info(
                "%s connected to ClickHouse at %s:%s on attempt %s",
                service_name,
                settings.clickhouse_host,
                settings.clickhouse_port,
                attempt,
            )
            return store
        except Exception as exc:
            last_error = exc
            logger.warning(
                "%s is waiting for ClickHouse at %s:%s (attempt %s): %s",
                service_name,
                settings.clickhouse_host,
                settings.clickhouse_port,
                attempt,
                exc,
            )
            time.sleep(settings.clickhouse_startup_check_interval_seconds)

    raise RuntimeError(
        f"{service_name} could not connect to ClickHouse within "
        f"{settings.clickhouse_startup_timeout_seconds}s"
    ) from last_error
