"""Map durable delivery payloads to the application recovery guard."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Callable

from tinvest_signal_engine.application.delivery_recovery import (
    DeliveryRecoveryGuard,
)
from tinvest_signal_engine.domain.delivery_recovery import (
    DeliveryFreshnessDecision,
)
from tinvest_signal_engine.domain.reliable_processing import DeliveryTask
from tinvest_signal_engine.domain.daily_positions import (
    REPORT_SIGNAL_TYPE, REPORT_VERSION, daily_report_freshness,
)
from tinvest_signal_engine.domain.commodity_positions import DIGEST_VERSION
from tinvest_signal_engine.domain.asset_positions import ASSET_REPORT_VERSION, asset_report_freshness
from tinvest_signal_engine.serialization import parse_timestamp
from tinvest_signal_engine.domain.session_open import SESSION_SIGNAL_TYPES, SESSION_OPEN_VERSION, SESSION_VERSION, session_boundary_freshness


class QueuedDeliveryRecoveryAdapter:
    def __init__(self, guard: DeliveryRecoveryGuard, *, clock: Callable[[], datetime] | None = None) -> None:
        self._guard = guard
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def evaluate(self, task: DeliveryTask) -> DeliveryFreshnessDecision:
        if task.payload.get("signal_type") in SESSION_SIGNAL_TYPES:
            now = self._clock()
            try:
                payload = task.payload["payload"]
                if payload["session_version"] not in {SESSION_OPEN_VERSION, SESSION_VERSION}:
                    raise ValueError("unknown session version")
                if payload["session_version"] == SESSION_OPEN_VERSION and task.payload["signal_type"] != "market_session_open":
                    raise ValueError("legacy notices support openings only")
                if payload["session_version"] == SESSION_VERSION:
                    kind = payload["event_kind"]
                    if task.payload["signal_type"] != "market_session_" + kind or not payload.get("events"):
                        raise ValueError("session kind/source mismatch")
                raw_open, raw_source = payload.get("event_at") or payload.get("opens_at"), task.payload["source_event_at"]
                if not isinstance(raw_open, str) or not isinstance(raw_source, str):
                    raise ValueError("session times must be ISO timestamps")
                opens_at = datetime.fromisoformat(raw_open)
                if opens_at != datetime.fromisoformat(raw_source):
                    raise ValueError("session source time mismatch")
                return session_boundary_freshness(event_at=opens_at, evaluated_at=now)
            except (KeyError, TypeError, ValueError):
                return DeliveryFreshnessDecision(False, "session_boundary_invalid", None, 60, None, now.date().isoformat())
        if task.payload.get("signal_type") == REPORT_SIGNAL_TYPE:
            payload = task.payload.get("payload")
            now = self._clock()
            try:
                if not isinstance(payload, dict):
                    raise ValueError("unknown daily report version")
                if payload.get("daily_report_version") == ASSET_REPORT_VERSION:
                    primary = payload["sections"][0]
                    if ((payload.get("source_exchange"), payload.get("period")) not in {
                        ("SHFE", "daily"), ("CFTC", "weekly"), ("ICE", "weekly"), ("MOEX", "daily"),
                    } or any(payload[key] != primary[field] for key, field in (
                        ("source_exchange", "source"), ("period", "period"),
                        ("trading_day", "trading_day"), ("previous_day", "previous_day"),
                    ))):
                        raise ValueError("invalid asset report primary source")
                    return asset_report_freshness(
                        trading_day=date.fromisoformat(payload["trading_day"]),
                        previous_day=date.fromisoformat(payload["previous_day"]), period=payload["period"],
                        generated_at=parse_timestamp(payload["report_generated_at"]), evaluated_at=now,
                    )
                legacy = payload.get("daily_report_version") == REPORT_VERSION and payload.get("source_exchange") == "SHFE"
                digest = (payload.get("daily_report_version") == DIGEST_VERSION
                          and payload.get("source_exchange") == "MULTI" and payload.get("primary_source") == "SHFE")
                if not (legacy or digest):
                    raise ValueError("unknown daily report source")
                return daily_report_freshness(
                    trading_day=date.fromisoformat(payload["trading_day"]),
                    previous_day=date.fromisoformat(payload["previous_day"]),
                    generated_at=parse_timestamp(payload["report_generated_at"]), evaluated_at=now,
                )
            except (IndexError, KeyError, TypeError, ValueError):
                return DeliveryFreshnessDecision(False, "daily_report_invalid", None, 21600, None, now.date().isoformat())
        raw_source_at = task.payload.get("source_event_at")
        source_event_at: datetime | None = None
        if isinstance(raw_source_at, (str, datetime)):
            try:
                source_event_at = parse_timestamp(raw_source_at)
            except (TypeError, ValueError):
                source_event_at = None
        signal_type = str(task.payload.get("signal_type") or "unknown")
        return self._guard.evaluate(
            source_event_at=source_event_at,
            signal_type=signal_type,
        )
