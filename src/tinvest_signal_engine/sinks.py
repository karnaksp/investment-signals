"""Вывод сигналов: Postgres, HTTP webhook и Telegram Bot API."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx
from psycopg import connect
from psycopg.rows import dict_row

from .config import RuntimeSettings
from .models import TriggerSignal
from .serialization import json_dumps

logger = logging.getLogger(__name__)


def _safe_identifier(value: str) -> str:
    candidate = value.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", candidate):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return candidate


class PostgresSignalStore:
    def __init__(self, settings: RuntimeSettings):
        self._table_name = _safe_identifier(settings.postgres_table)
        self._connection = connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_database,
            user=settings.postgres_username,
            password=settings.postgres_password,
            autocommit=True,
        )

    def ping(self) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None

    def close(self) -> None:
        self._connection.close()

    def insert_signal(self, signal: TriggerSignal) -> None:
        query = f"""
            INSERT INTO {self._table_name} (
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
            ) VALUES (
                %(signal_id)s,
                %(detected_at)s,
                %(instrument_id)s,
                %(ticker)s,
                %(class_code)s,
                %(alias)s,
                %(source_event_type)s,
                %(signal_type)s,
                %(severity)s,
                %(metric_value)s,
                %(baseline_value)s,
                %(z_score)s,
                %(window_seconds)s,
                %(summary)s,
                %(payload_json)s::jsonb
            )
        """
        with self._connection.cursor() as cursor:
            cursor.execute(
                query,
                {
                    "signal_id": signal.signal_id,
                    "detected_at": signal.detected_at,
                    "instrument_id": signal.instrument_id,
                    "ticker": signal.ticker,
                    "class_code": signal.class_code,
                    "alias": signal.alias,
                    "source_event_type": signal.source_event_type,
                    "signal_type": signal.signal_type,
                    "severity": signal.severity,
                    "metric_value": signal.metric_value,
                    "baseline_value": signal.baseline_value,
                    "z_score": signal.z_score,
                    "window_seconds": signal.window_seconds,
                    "summary": signal.summary,
                    "payload_json": json_dumps(signal.payload),
                },
            )

    def fetch_recent(
        self, *, limit: int = 50, instrument_id: str | None = None
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 500))
        params: dict[str, Any] = {"limit": safe_limit}
        filter_clause = ""
        if instrument_id:
            filter_clause = "WHERE instrument_id = %(instrument_id)s"
            params["instrument_id"] = instrument_id.strip()
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
            FROM {self._table_name}
            {filter_clause}
            ORDER BY detected_at DESC
            LIMIT %(limit)s
        """
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
        return [
            {
                "signal_id": str(row["signal_id"]),
                "detected_at": row["detected_at"].isoformat(),
                "instrument_id": row["instrument_id"],
                "ticker": row["ticker"],
                "class_code": row["class_code"],
                "alias": row["alias"],
                "source_event_type": row["source_event_type"],
                "signal_type": row["signal_type"],
                "severity": row["severity"],
                "metric_value": row["metric_value"],
                "baseline_value": row["baseline_value"],
                "z_score": row["z_score"],
                "window_seconds": row["window_seconds"],
                "summary": row["summary"],
                "payload": row["payload_json"],
            }
            for row in rows
        ]

    def fetch_summary(self, *, minutes: int = 60) -> list[dict[str, Any]]:
        safe_minutes = max(1, min(minutes, 1_440))
        query = f"""
            SELECT
                signal_type,
                count() AS signal_count
            FROM {self._table_name}
            WHERE detected_at >= NOW() - (%(minutes)s * INTERVAL '1 minute')
            GROUP BY signal_type
            ORDER BY signal_count DESC, signal_type ASC
        """
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, {"minutes": safe_minutes})
            rows = cursor.fetchall()
        return [
            {
                "signal_type": row["signal_type"],
                "signal_count": row["signal_count"],
            }
            for row in rows
        ]


class WebhookAlertSink:
    def __init__(self, webhook_url: str | None):
        self._webhook_url = webhook_url
        self._client = (
            httpx.Client(timeout=5.0)
            if self._webhook_url
            else None
        )

    @property
    def enabled(self) -> bool:
        return bool(self._webhook_url)

    def send(self, signal: TriggerSignal) -> None:
        if not self._webhook_url or self._client is None:
            return
        self._client.post(self._webhook_url, json=signal.to_dict())

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


class TelegramAlertSink:
    def __init__(
        self,
        *,
        bot_token: str | None,
        chat_id: str | None,
        message_thread_id: int | None = None,
    ):
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._message_thread_id = message_thread_id
        self._client = (
            httpx.Client(timeout=5.0)
            if self.enabled
            else None
        )

    @property
    def enabled(self) -> bool:
        return bool(self._bot_token and self._chat_id)

    def send(self, signal: TriggerSignal) -> None:
        if self._client is None or not self._bot_token or not self._chat_id:
            return
        payload: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": self._format_message(signal),
            "disable_web_page_preview": True,
        }
        if self._message_thread_id is not None:
            payload["message_thread_id"] = self._message_thread_id
        response = self._client.post(
            f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
            json=payload,
        )
        response.raise_for_status()
        body = response.json()
        if not body.get("ok"):
            raise RuntimeError(
                f"Telegram API returned error: {body!r}"
            )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def _format_message(self, signal: TriggerSignal) -> str:
        return (
            f"[{signal.signal_type}] {signal.ticker}\n"
            f"{signal.summary}\n"
            f"Severity={signal.severity} z={signal.z_score:.2f}"
        )


def create_postgres_signal_store_with_retry(
    settings: RuntimeSettings,
    *,
    service_name: str,
) -> PostgresSignalStore:
    timeout_seconds = settings.postgres_startup_timeout_seconds
    deadline = time.monotonic() + timeout_seconds
    check_interval = settings.postgres_startup_check_interval_seconds
    attempt = 0
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        attempt += 1
        try:
            store = PostgresSignalStore(settings)
            store.ping()
            logger.info(
                "%s connected to Postgres at %s:%s on attempt %s",
                service_name,
                settings.postgres_host,
                settings.postgres_port,
                attempt,
            )
            return store
        except Exception as last_error:
            logger.warning(
                "%s is waiting for Postgres at %s:%s (attempt %s): %s",
                service_name,
                settings.postgres_host,
                settings.postgres_port,
                attempt,
                last_error,
            )
            time.sleep(check_interval)

    raise RuntimeError(
        f"{service_name} could not connect to Postgres within "
        f"{timeout_seconds}s"
    ) from last_error
