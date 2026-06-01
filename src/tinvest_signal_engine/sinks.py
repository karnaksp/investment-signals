"""Вывод сигналов: Postgres, HTTP webhook и Telegram Bot API."""

from __future__ import annotations

import logging
import re
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import httpx
from psycopg import OperationalError, connect
from psycopg.rows import dict_row

from .config import RuntimeSettings
from .models import TriggerSignal
from .serialization import json_dumps
from .signal_enrichment import enrich_signal_for_delivery
from .signal_locale import format_plain_alert_ru
from .terminal_links import t_invest_instrument_url, t_invest_terminal_search_url

logger = logging.getLogger(__name__)


def _safe_identifier(value: str) -> str:
    candidate = value.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", candidate):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return candidate


_ADMIN_FEEDBACK_TABLE = _safe_identifier("signal_admin_feedback")


def _quality_sql(alias: str = "ms") -> str:
    return f"nullif({alias}.payload_json->>'quality_score', '')::double precision"


def _delivery_status_sql(alias: str = "ms") -> str:
    return (
        f"coalesce(nullif({alias}.payload_json->>'delivery_status', ''), "
        "'unknown')"
    )


def _delivery_reason_sql(alias: str = "ms") -> str:
    return (
        f"coalesce(nullif({alias}.payload_json->>'delivery_reason', ''), "
        "'unknown')"
    )


class PostgresSignalStore:
    def __init__(self, settings: RuntimeSettings):
        self._table_name = _safe_identifier(settings.postgres_table)
        self._feedback_table = _ADMIN_FEEDBACK_TABLE
        self._settings = settings
        self._open_connection()

    def _open_connection(self) -> None:
        self._connection = connect(
            host=self._settings.postgres_host,
            port=self._settings.postgres_port,
            dbname=self._settings.postgres_database,
            user=self._settings.postgres_username,
            password=self._settings.postgres_password,
            autocommit=True,
        )

    def _reconnect(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass
        self._open_connection()

    def _ensure_connection(self) -> None:
        if self._connection.closed:
            logger.warning(
                "Postgres connection was closed; reconnecting to %s:%s",
                self._settings.postgres_host,
                self._settings.postgres_port,
            )
            self._reconnect()

    @contextmanager
    def _cursor(self, *, row_factory=None):
        """Курсор с одним повтором при обрыве (рестарт Postgres, idle timeout)."""
        for attempt in range(2):
            self._ensure_connection()
            try:
                with self._connection.cursor(row_factory=row_factory) as cur:
                    yield cur
                return
            except OperationalError as e:
                if attempt == 0:
                    logger.warning(
                        "Postgres OperationalError; reconnecting (%s:%s): %s",
                        self._settings.postgres_host,
                        self._settings.postgres_port,
                        e,
                    )
                    self._reconnect()
                    continue
                raise

    def ping(self) -> bool:
        with self._cursor() as cursor:
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass

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
        with self._cursor() as cursor:
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

    def count_delivered_since(
        self,
        *,
        since: datetime,
        instrument_id: str | None = None,
        signal_type: str | None = None,
    ) -> int:
        conds = [
            "detected_at >= %(since)s",
            f"{_delivery_status_sql()} = 'delivered'",
        ]
        params: dict[str, Any] = {"since": since}
        if instrument_id:
            conds.append("instrument_id = %(instrument_id)s")
            params["instrument_id"] = instrument_id
        if signal_type:
            conds.append("signal_type = %(signal_type)s")
            params["signal_type"] = signal_type
        query = f"""
            SELECT count(*)::int AS cnt
            FROM {self._table_name} ms
            WHERE {" AND ".join(conds)}
        """
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        return int((row or {}).get("cnt") or 0)

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
        with self._cursor(row_factory=dict_row) as cursor:
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
                COUNT(*) AS signal_count
            FROM {self._table_name}
            WHERE detected_at >= NOW() - (%(minutes)s * INTERVAL '1 minute')
            GROUP BY signal_type
            ORDER BY signal_count DESC, signal_type ASC
        """
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, {"minutes": safe_minutes})
            rows = cursor.fetchall()
        return [
            {
                "signal_type": row["signal_type"],
                "signal_count": row["signal_count"],
            }
            for row in rows
        ]

    def _admin_time_sql(self, minutes: int) -> tuple[str, dict[str, Any], bool]:
        """Фрагмент условия по времени, параметры и признак «всё время»."""
        if int(minutes) <= 0:
            return "TRUE", {}, True
        m = min(max(int(minutes), 1), 10_080)
        return (
            "detected_at >= NOW() - (%(m)s * INTERVAL '1 minute')",
            {"m": m},
            False,
        )

    def fetch_admin_overview(self, *, minutes: int = 1440) -> dict[str, Any]:
        """Агрегаты для админ-дашборда.

        ``minutes <= 0`` — без ограничения по времени (вся таблица).
        """
        time_sql, time_params, all_time = self._admin_time_sql(minutes)
        tbl = self._table_name
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    count(*)::bigint AS total,
                    avg(
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS avg_quality,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS median_quality,
                    min(detected_at) AS first_detected_at,
                    max(detected_at) AS last_detected_at
                FROM {tbl}
                WHERE {time_sql}
                """,
                time_params,
            )
            totals = dict(cursor.fetchone() or {})
            cursor.execute(
                f"""
                SELECT
                    signal_type,
                    count(*)::bigint AS signal_count,
                    avg(
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS avg_quality
                FROM {tbl}
                WHERE {time_sql}
                GROUP BY signal_type
                ORDER BY signal_count DESC, signal_type ASC
                """,
                time_params,
            )
            by_type = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    severity::smallint AS severity,
                    count(*)::bigint AS signal_count,
                    avg(
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS avg_quality
                FROM {tbl}
                WHERE {time_sql}
                GROUP BY severity
                ORDER BY severity ASC
                """,
                time_params,
            )
            by_severity = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    ticker,
                    count(*)::bigint AS signal_count,
                    avg(
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS avg_quality
                FROM {tbl}
                WHERE {time_sql}
                GROUP BY ticker
                ORDER BY signal_count DESC
                LIMIT 30
                """,
                time_params,
            )
            by_ticker = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    bucket_label,
                    signal_count
                FROM (
                    SELECT
                        CASE
                            WHEN nullif(payload_json->>'quality_score', '') IS NULL
                            THEN 'нет оценки'
                            WHEN (
                                nullif(payload_json->>'quality_score', '')
                            )::double precision < 20 THEN '0–19'
                            WHEN (
                                nullif(payload_json->>'quality_score', '')
                            )::double precision < 40 THEN '20–39'
                            WHEN (
                                nullif(payload_json->>'quality_score', '')
                            )::double precision < 60 THEN '40–59'
                            WHEN (
                                nullif(payload_json->>'quality_score', '')
                            )::double precision < 80 THEN '60–79'
                            ELSE '80–100'
                        END AS bucket_label,
                        count(*)::bigint AS signal_count
                    FROM {tbl}
                    WHERE {time_sql}
                    GROUP BY 1
                ) AS q
                ORDER BY
                    array_position(
                        ARRAY[
                            'нет оценки',
                            '0–19',
                            '20–39',
                            '40–59',
                            '60–79',
                            '80–100'
                        ]::text[],
                        bucket_label
                    )
                """,
                time_params,
            )
            quality_buckets = [dict(r) for r in cursor.fetchall()]
            if all_time:
                timeline_sql = f"""
                    SELECT
                        date_trunc('day', detected_at) AS bucket,
                        count(*)::bigint AS signal_count,
                        avg(
                            nullif(payload_json->>'quality_score', '')
                            ::double precision
                        ) AS avg_quality
                    FROM {tbl}
                    WHERE detected_at >= COALESCE(
                        (SELECT max(detected_at) FROM {tbl}),
                        NOW()
                    ) - INTERVAL '730 days'
                    GROUP BY 1
                    ORDER BY 1 ASC
                """
                timeline_params: dict[str, Any] = {}
                timeline_granularity = "day"
            else:
                timeline_sql = f"""
                    SELECT
                        date_trunc('hour', detected_at) AS bucket,
                        count(*)::bigint AS signal_count,
                        avg(
                            nullif(payload_json->>'quality_score', '')
                            ::double precision
                        ) AS avg_quality
                    FROM {tbl}
                    WHERE {time_sql}
                    GROUP BY 1
                    ORDER BY 1 ASC
                """
                timeline_params = time_params
                timeline_granularity = "hour"
            cursor.execute(timeline_sql, timeline_params)
            hourly = [dict(r) for r in cursor.fetchall()]
        for row in hourly:
            b = row.get("bucket")
            if b is not None:
                row["bucket"] = b.isoformat()
        first_at = totals.get("first_detected_at")
        last_at = totals.get("last_detected_at")
        if first_at is not None:
            totals["first_detected_at"] = first_at.isoformat()
        if last_at is not None:
            totals["last_detected_at"] = last_at.isoformat()
        eff_minutes = 0 if all_time else int(time_params.get("m", 0))
        overview: dict[str, Any] = {
            "minutes": eff_minutes,
            "all_time": all_time,
            "timeline_granularity": timeline_granularity,
            "totals": {
                "total": int(totals.get("total") or 0),
                "avg_quality": totals.get("avg_quality"),
                "median_quality": totals.get("median_quality"),
                "first_detected_at": totals.get("first_detected_at"),
                "last_detected_at": totals.get("last_detected_at"),
            },
            "by_type": by_type,
            "by_severity": by_severity,
            "by_ticker": by_ticker,
            "quality_buckets": quality_buckets,
            "hourly": hourly,
        }
        if not all_time and eff_minutes > 0:
            cmp_totals = self.fetch_admin_compare_totals(minutes=eff_minutes)
            if cmp_totals is not None:
                overview["compare_windows"] = cmp_totals
        return overview

    def fetch_admin_signals_page(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        minutes: int = 1440,
        instrument_id: str | None = None,
        signal_type: str | None = None,
        min_quality: float | None = None,
        quality_min: float | None = None,
        quality_max: float | None = None,
        delivery_status: str | None = None,
        feedback: str | None = None,
        severity: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        lim = max(1, min(int(limit), 200))
        off = max(0, int(offset))
        conds: list[str] = []
        params: dict[str, Any] = {"lim": lim, "off": off}
        if int(minutes) > 0:
            m = min(max(int(minutes), 1), 10_080)
            conds.append("ms.detected_at >= NOW() - (%(m)s * INTERVAL '1 minute')")
            params["m"] = m
        if instrument_id:
            conds.append("ms.instrument_id = %(instrument_id)s")
            params["instrument_id"] = instrument_id.strip()
        if signal_type:
            conds.append("ms.signal_type = %(signal_type)s")
            params["signal_type"] = signal_type.strip()
        q_min = quality_min if quality_min is not None else min_quality
        if q_min is not None:
            conds.append(f"{_quality_sql()} >= %(quality_min)s")
            params["quality_min"] = float(q_min)
        if quality_max is not None:
            conds.append(f"{_quality_sql()} <= %(quality_max)s")
            params["quality_max"] = float(quality_max)
        if delivery_status:
            conds.append(f"{_delivery_status_sql()} = %(delivery_status)s")
            params["delivery_status"] = delivery_status.strip()
        if feedback:
            if feedback.strip() == "none":
                conds.append("fb.label IS NULL")
            else:
                conds.append("fb.label = %(feedback)s")
                params["feedback"] = feedback.strip()
        if severity is not None:
            conds.append("ms.severity = %(severity)s")
            params["severity"] = int(severity)
        where_sql = " AND ".join(conds) if conds else "TRUE"
        base = (
            f"FROM {self._table_name} AS ms "
            f"LEFT JOIN {self._feedback_table} AS fb ON fb.signal_id = ms.signal_id "
            f"WHERE {where_sql}"
        )
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(f"SELECT count(*)::bigint AS c {base}", params)
            total = int((cursor.fetchone() or {}).get("c") or 0)
            cursor.execute(
                f"""
                SELECT
                    ms.signal_id,
                    ms.detected_at,
                    ms.instrument_id,
                    ms.ticker,
                    ms.class_code,
                    ms.signal_type,
                    ms.severity,
                    ms.metric_value,
                    ms.baseline_value,
                    ms.z_score,
                    ms.window_seconds,
                    ms.summary,
                    ms.payload_json,
                    {_delivery_status_sql()} AS delivery_status,
                    {_delivery_reason_sql()} AS delivery_reason,
                    fb.label AS admin_feedback_label,
                    fb.note AS admin_feedback_note,
                    fb.updated_at AS admin_feedback_at
                {base}
                ORDER BY ms.detected_at DESC
                LIMIT %(lim)s OFFSET %(off)s
                """,
                params,
            )
            rows = cursor.fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            fb_at = row.get("admin_feedback_at")
            out.append(
                {
                    "signal_id": str(row["signal_id"]),
                    "detected_at": row["detected_at"].isoformat(),
                    "instrument_id": row["instrument_id"],
                    "ticker": row["ticker"],
                    "class_code": row["class_code"],
                    "signal_type": row["signal_type"],
                    "severity": row["severity"],
                    "metric_value": row["metric_value"],
                    "baseline_value": row["baseline_value"],
                    "z_score": row["z_score"],
                    "window_seconds": row["window_seconds"],
                    "summary": row["summary"],
                    "payload": row["payload_json"],
                    "delivery_status": row.get("delivery_status") or "unknown",
                    "delivery_reason": row.get("delivery_reason") or "unknown",
                    "admin_feedback_label": row.get("admin_feedback_label"),
                    "admin_feedback_note": row.get("admin_feedback_note"),
                    "admin_feedback_at": (
                        fb_at.isoformat() if fb_at is not None else None
                    ),
                }
            )
        return out, total

    def upsert_admin_feedback(
        self,
        *,
        signal_id: str,
        label: str,
        note: str = "",
    ) -> None:
        q = f"""
            INSERT INTO {self._feedback_table} (signal_id, label, note, updated_at)
            VALUES (%(signal_id)s::uuid, %(label)s, %(note)s, NOW())
            ON CONFLICT (signal_id) DO UPDATE SET
                label = EXCLUDED.label,
                note = EXCLUDED.note,
                updated_at = NOW()
        """
        with self._cursor() as cursor:
            cursor.execute(
                q,
                {
                    "signal_id": signal_id.strip(),
                    "label": label.strip(),
                    "note": (note or "").strip(),
                },
            )

    def fetch_admin_signal_by_id(self, signal_id: str) -> dict[str, Any] | None:
        q = f"""
            SELECT
                ms.signal_id,
                ms.detected_at,
                ms.instrument_id,
                ms.ticker,
                ms.class_code,
                ms.alias,
                ms.source_event_type,
                ms.signal_type,
                ms.severity,
                ms.metric_value,
                ms.baseline_value,
                ms.z_score,
                ms.window_seconds,
                ms.summary,
                ms.payload_json,
                {_delivery_status_sql()} AS delivery_status,
                {_delivery_reason_sql()} AS delivery_reason,
                fb.label AS admin_feedback_label,
                fb.note AS admin_feedback_note,
                fb.updated_at AS admin_feedback_at
            FROM {self._table_name} AS ms
            LEFT JOIN {self._feedback_table} AS fb ON fb.signal_id = ms.signal_id
            WHERE ms.signal_id = %(sid)s::uuid
        """
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(q, {"sid": signal_id.strip()})
            row = cursor.fetchone()
        if row is None:
            return None
        fb_at = row.get("admin_feedback_at")
        return {
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
            "delivery_status": row.get("delivery_status") or "unknown",
            "delivery_reason": row.get("delivery_reason") or "unknown",
            "admin_feedback_label": row.get("admin_feedback_label"),
            "admin_feedback_note": row.get("admin_feedback_note"),
            "admin_feedback_at": fb_at.isoformat() if fb_at is not None else None,
        }

    def fetch_admin_compare_totals(self, *, minutes: int) -> dict[str, Any] | None:
        """Сравнение текущего окна [now-m, now) с предыдущим [now-2m, now-m)."""
        if int(minutes) <= 0:
            return None
        m = min(max(int(minutes), 1), 10_080)
        tbl = self._table_name
        agg = f"""
            SELECT
                count(*)::bigint AS total,
                avg(
                    nullif(payload_json->>'quality_score', '')::double precision
                ) AS avg_quality
            FROM {tbl}
            WHERE detected_at >= NOW() - (%(m)s * INTERVAL '1 minute')
              AND detected_at < NOW()
        """
        prev_where = """
            detected_at >= NOW() - (%(m2)s * INTERVAL '1 minute')
            AND detected_at < NOW() - (%(m)s * INTERVAL '1 minute')
        """
        params = {"m": m, "m2": m * 2}
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(agg, {"m": m})
            cur = dict(cursor.fetchone() or {})
            cursor.execute(
                f"""
                SELECT
                    count(*)::bigint AS total,
                    avg(
                        nullif(payload_json->>'quality_score', '')::double precision
                    ) AS avg_quality
                FROM {tbl}
                WHERE {prev_where}
                """,
                params,
            )
            prev = dict(cursor.fetchone() or {})
        return {
            "window_minutes": m,
            "current": {
                "total": int(cur.get("total") or 0),
                "avg_quality": cur.get("avg_quality"),
            },
            "previous": {
                "total": int(prev.get("total") or 0),
                "avg_quality": prev.get("avg_quality"),
            },
        }

    def fetch_admin_slices(self, *, minutes: int) -> dict[str, Any]:
        """Доп. разрезы: heatmap день×час UTC, быстрые повторы по инструменту."""
        time_sql, time_params, _all_time = self._admin_time_sql(minutes)
        tbl = self._table_name
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    (EXTRACT(ISODOW FROM detected_at AT TIME ZONE 'UTC'))::int
                        AS dow,
                    (EXTRACT(HOUR FROM detected_at AT TIME ZONE 'UTC'))::int
                        AS hod,
                    count(*)::bigint AS c
                FROM {tbl}
                WHERE {time_sql}
                GROUP BY 1, 2
                ORDER BY 1, 2
                """,
                time_params,
            )
            heat_cells = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                WITH base AS (
                    SELECT
                        instrument_id,
                        detected_at,
                        lead(detected_at) OVER (
                            PARTITION BY instrument_id ORDER BY detected_at
                        ) AS nxt
                    FROM {tbl}
                    WHERE {time_sql}
                ),
                tot AS (
                    SELECT count(*)::bigint AS n FROM {tbl} WHERE {time_sql}
                )
                SELECT
                    (SELECT n FROM tot) AS total_signals,
                    count(*) FILTER (
                        WHERE nxt IS NOT NULL
                        AND nxt <= detected_at + interval '5 minutes'
                    )::bigint AS rapid_followups
                FROM base
                """,
                time_params,
            )
            rapid = dict(cursor.fetchone() or {})
        total_s = int(rapid.get("total_signals") or 0)
        rf = int(rapid.get("rapid_followups") or 0)
        rate = (rf / total_s) if total_s else 0.0
        return {
            "heatmap_utc": heat_cells,
            "rapid_followups_within_5m": rf,
            "total_signals": total_s,
            "rapid_followup_rate": round(rate, 6),
        }

    def fetch_admin_instrument_activity(self, *, minutes: int) -> dict[str, dict[str, Any]]:
        time_sql, time_params, _all_time = self._admin_time_sql(minutes)
        time_sql = time_sql.replace("detected_at", "ms.detected_at")
        status_sql = _delivery_status_sql()
        tbl = self._table_name
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    ms.instrument_id,
                    ms.ticker,
                    ms.class_code,
                    count(*)::bigint AS total,
                    count(*) FILTER (
                        WHERE {status_sql} = 'delivered'
                    )::bigint AS delivered,
                    count(*) FILTER (
                        WHERE {status_sql} = 'suppressed'
                    )::bigint AS suppressed,
                    count(*) FILTER (
                        WHERE {status_sql} = 'unknown'
                    )::bigint AS unknown,
                    avg({_quality_sql()}) AS avg_quality,
                    max(ms.detected_at) AS last_detected_at
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY ms.instrument_id, ms.ticker, ms.class_code
                """,
                time_params,
            )
            rows = [dict(r) for r in cursor.fetchall()]
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            last = row.get("last_detected_at")
            if last is not None:
                row["last_detected_at"] = last.isoformat()
            key = str(row.get("instrument_id") or "")
            if key:
                out[key] = row
        return out

    def fetch_admin_delivery_overview(self, *, minutes: int) -> dict[str, Any]:
        time_sql, time_params, all_time = self._admin_time_sql(minutes)
        time_sql = time_sql.replace("detected_at", "ms.detected_at")
        tbl = self._table_name
        status_sql = _delivery_status_sql()
        reason_sql = _delivery_reason_sql()
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    {status_sql} AS delivery_status,
                    count(*)::bigint AS signal_count,
                    avg({_quality_sql()}) AS avg_quality
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY 1
                ORDER BY signal_count DESC, delivery_status ASC
                """,
                time_params,
            )
            by_status = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    {reason_sql} AS delivery_reason,
                    {status_sql} AS delivery_status,
                    count(*)::bigint AS signal_count
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY 1, 2
                ORDER BY signal_count DESC, delivery_reason ASC
                LIMIT 20
                """,
                time_params,
            )
            reasons = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    ms.signal_type,
                    count(*)::bigint AS total,
                    count(*) FILTER (
                        WHERE {status_sql} = 'delivered'
                    )::bigint AS delivered,
                    count(*) FILTER (
                        WHERE {status_sql} = 'suppressed'
                    )::bigint AS suppressed,
                    count(*) FILTER (
                        WHERE {status_sql} = 'unknown'
                    )::bigint AS unknown,
                    avg({_quality_sql()}) AS avg_quality
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY ms.signal_type
                ORDER BY total DESC, ms.signal_type ASC
                LIMIT 40
                """,
                time_params,
            )
            by_type = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    ms.ticker,
                    count(*)::bigint AS total,
                    count(*) FILTER (
                        WHERE {status_sql} = 'delivered'
                    )::bigint AS delivered,
                    count(*) FILTER (
                        WHERE {status_sql} = 'suppressed'
                    )::bigint AS suppressed,
                    avg({_quality_sql()}) AS avg_quality,
                    max(ms.detected_at) AS last_detected_at
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY ms.ticker
                ORDER BY total DESC, ms.ticker ASC
                LIMIT 40
                """,
                time_params,
            )
            by_ticker = [dict(r) for r in cursor.fetchall()]
            cursor.execute(
                f"""
                SELECT
                    ms.signal_id,
                    ms.detected_at,
                    ms.ticker,
                    ms.instrument_id,
                    ms.signal_type,
                    ms.severity,
                    ms.z_score,
                    ms.summary,
                    ms.payload_json
                FROM {tbl} AS ms
                WHERE {time_sql}
                  AND {status_sql} = 'delivered'
                ORDER BY ms.detected_at DESC
                LIMIT 12
                """,
                time_params,
            )
            recent_delivered = [dict(r) for r in cursor.fetchall()]
        totals = {str(r["delivery_status"]): int(r["signal_count"] or 0) for r in by_status}
        total = sum(totals.values())
        delivered = totals.get("delivered", 0)
        for row in by_ticker:
            last = row.get("last_detected_at")
            if last is not None:
                row["last_detected_at"] = last.isoformat()
        for row in recent_delivered:
            row["signal_id"] = str(row["signal_id"])
            row["detected_at"] = row["detected_at"].isoformat()
            row["payload"] = row.pop("payload_json")
        return {
            "minutes": 0 if all_time else int(time_params.get("m", 0)),
            "all_time": all_time,
            "totals": {
                "total": total,
                "delivered": delivered,
                "suppressed": totals.get("suppressed", 0),
                "unknown": totals.get("unknown", 0),
                "delivery_rate": (delivered / total) if total else 0.0,
            },
            "by_status": by_status,
            "reasons": reasons,
            "by_type": by_type,
            "by_ticker": by_ticker,
            "recent_delivered": recent_delivered,
        }

    def fetch_admin_delivery_reasons(self, *, minutes: int) -> dict[str, Any]:
        time_sql, time_params, all_time = self._admin_time_sql(minutes)
        time_sql = time_sql.replace("detected_at", "ms.detected_at")
        tbl = self._table_name
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    {_delivery_reason_sql()} AS delivery_reason,
                    {_delivery_status_sql()} AS delivery_status,
                    ms.signal_type,
                    count(*)::bigint AS signal_count,
                    avg({_quality_sql()}) AS avg_quality
                FROM {tbl} AS ms
                WHERE {time_sql}
                GROUP BY 1, 2, 3
                ORDER BY signal_count DESC, delivery_reason ASC, ms.signal_type ASC
                LIMIT 200
                """,
                time_params,
            )
            rows = [dict(r) for r in cursor.fetchall()]
        return {
            "minutes": 0 if all_time else int(time_params.get("m", 0)),
            "all_time": all_time,
            "items": rows,
            "count": len(rows),
        }

    def fetch_admin_calibration(self, *, minutes: int) -> dict[str, Any]:
        time_sql, time_params, all_time = self._admin_time_sql(minutes)
        time_sql = time_sql.replace("detected_at", "ms.detected_at")
        tbl = self._table_name
        q_sql = _quality_sql()
        with self._cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                f"""
                SELECT
                    ms.signal_type,
                    CASE
                        WHEN {q_sql} IS NULL THEN 'unknown'
                        WHEN {q_sql} < 48 THEN 'low'
                        WHEN {q_sql} < 72 THEN 'medium'
                        ELSE 'high'
                    END AS quality_tier,
                    {_delivery_status_sql()} AS delivery_status,
                    coalesce(fb.label, 'none') AS feedback,
                    count(*)::bigint AS signal_count,
                    avg({q_sql}) AS avg_quality
                FROM {tbl} AS ms
                LEFT JOIN {self._feedback_table} AS fb ON fb.signal_id = ms.signal_id
                WHERE {time_sql}
                GROUP BY 1, 2, 3, 4
                ORDER BY ms.signal_type ASC, quality_tier ASC, delivery_status ASC
                """,
                time_params,
            )
            rows = [dict(r) for r in cursor.fetchall()]
        return {
            "minutes": 0 if all_time else int(time_params.get("m", 0)),
            "all_time": all_time,
            "items": rows,
            "count": len(rows),
        }


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
        self._client.post(self._webhook_url, json=_webhook_payload(signal))

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
        p = signal.payload or {}
        tg = p.get("telegram_html")
        if not (isinstance(tg, str) and tg.strip()):
            signal = enrich_signal_for_delivery(signal)
            tg = (signal.payload or {}).get("telegram_html")
        text = (
            tg
            if isinstance(tg, str) and tg.strip()
            else self._format_plain_fallback(signal)
        )
        payload: dict[str, Any] = {
            "chat_id": self._chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if self._message_thread_id is not None:
            payload["message_thread_id"] = self._message_thread_id
        response = self._client.post(
            f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
            json=payload,
        )
        if response.status_code >= 400:
            try:
                detail = response.json()
            except Exception:
                detail = response.text
            logger.error(
                "Telegram sendMessage HTTP %s: %s",
                response.status_code,
                detail,
            )
            plain = self._format_plain_fallback(signal)
            plain_payload: dict[str, Any] = {
                "chat_id": self._chat_id,
                "text": plain[:4096],
                "disable_web_page_preview": True,
            }
            if self._message_thread_id is not None:
                plain_payload["message_thread_id"] = self._message_thread_id
            response = self._client.post(
                f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
                json=plain_payload,
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

    def _format_plain_fallback(self, signal: TriggerSignal) -> str:
        sig = enrich_signal_for_delivery(signal)
        p = sig.payload or {}
        return format_plain_alert_ru(
            sig,
            ticker_terminal_url=str(
                p.get("terminal_url")
                or t_invest_terminal_search_url(
                    ticker=sig.ticker, class_code=sig.class_code
                )
            ),
            instrument_page_url=str(
                p.get("instrument_page_url")
                or t_invest_instrument_url(
                    ticker=sig.ticker, class_code=sig.class_code
                )
            ),
        )


def _webhook_payload(signal: TriggerSignal) -> dict[str, Any]:
    """Плоский JSON для вебхука: русский текст + ссылка + оценка качества."""
    d = signal.to_dict()
    p = signal.payload
    d["summary_ru"] = signal.summary
    d["terminal_url"] = p.get("terminal_url")
    d["instrument_page_url"] = p.get("instrument_page_url")
    d["quality_score"] = p.get("quality_score")
    d["quality_tier"] = p.get("quality_tier")
    d["quality_tier_ru"] = p.get("quality_tier_ru")
    d["quality_factors"] = p.get("quality_factors")
    return d


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
        except Exception as exc:
            # Не использовать ``except ... as last_error``: в конце suite Python
            # удаляет это имя — тогда ``from last_error`` после цикла даёт
            # UnboundLocalError при исчерпании таймаута.
            last_error = exc
            logger.warning(
                "%s is waiting for Postgres at %s:%s (attempt %s): %s",
                service_name,
                settings.postgres_host,
                settings.postgres_port,
                attempt,
                exc,
            )
            time.sleep(check_interval)

    raise RuntimeError(
        f"{service_name} could not connect to Postgres within "
        f"{timeout_seconds}s"
    ) from last_error
