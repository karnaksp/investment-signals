"""Persist simultaneous session boundaries as one idempotent Telegram notice."""

from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import NAMESPACE_URL, uuid5

from tinvest_signal_engine.adapters.postgres_reliability import PostgresReliableProcessingStore
from tinvest_signal_engine.adapters.session_open_message import ACTIONS, boundary_payload
from tinvest_signal_engine.application.reliable_processing import BrokerEvent
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, PreparedSignal, SignalRecord
from tinvest_signal_engine.domain.session_open import SESSION_VERSION, SessionBoundary


class PostgresSessionBoundaryPublisher:
    def __init__(self, *, store: PostgresReliableProcessingStore, target: DeliveryTarget,
                 display_timezone: str) -> None:
        self._store, self._target, self._timezone = store, target, display_timezone

    def publish(self, events: tuple[SessionBoundary, ...], *, generated_at: datetime) -> bool:
        if not events or not all(event.is_due(generated_at) for event in events):
            return False
        payload = boundary_payload(events, display_timezone=self._timezone)
        from tinvest_signal_engine.adapters.session_open_message import render_session_boundary_html
        render_session_boundary_html(payload)
        at = events[0].event_at.astimezone(timezone.utc)
        event_kind = events[0].event_kind
        signal_type = "market_session_" + event_kind
        event_id = str(uuid5(NAMESPACE_URL, f"{SESSION_VERSION}:{event_kind}:{at.isoformat()}"))
        instrument = "SESSION_" + event_kind.upper()
        body = {"event_id": event_id, "event_type": signal_type,
                "instrument_id": instrument, "source_time": at.isoformat(),
                "session_ids": sorted(e.session_id for e in events), "version": SESSION_VERSION}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        event = BrokerEvent(event_id, signal_type, SESSION_VERSION + "." + event_kind, 0,
                            int(at.timestamp()), sha256(canonical).digest(), body)
        signal = SignalRecord(
            signal_id=str(uuid5(NAMESPACE_URL, "signal:" + event_id)), detected_at=generated_at,
            instrument_id=instrument, ticker="SESSIONS", class_code="SESSION", alias="global_sessions",
            source_event_type=event.event_type, signal_type=signal_type, severity=1,
            metric_value=0, baseline_value=0, z_score=0, window_seconds=60,
            summary=ACTIONS[event_kind] + ": " + ", ".join(e.title for e in events),
            payload={**payload, "delivery_channel": "session", "delivery_status": "delivered",
                     "delivery_reason": "session_boundary_subscription"},
            source_event_id=event_id, source_event_at=at, signal_schema_version="1.0.0",
            expectation_catalog_version=SESSION_VERSION, detector_config_version=SESSION_VERSION,
            delivery_config_version=SESSION_VERSION, cost_model_version="not_applicable", provenance_status="complete",
        )
        return not self._store.persist_session_notification_once(event, PreparedSignal(signal, (self._target,))).replayed
