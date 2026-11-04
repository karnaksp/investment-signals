"""Persist scheduled openings atomically with the existing Telegram outbox."""

from datetime import datetime, timezone
from hashlib import sha256
import json
from uuid import NAMESPACE_URL, uuid5

from tinvest_signal_engine.adapters.postgres_reliability import PostgresReliableProcessingStore
from tinvest_signal_engine.adapters.session_open_message import opening_payload
from tinvest_signal_engine.application.reliable_processing import BrokerEvent
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, PreparedSignal, SignalRecord
from tinvest_signal_engine.domain.session_open import SESSION_OPEN_SIGNAL_TYPE, SESSION_OPEN_VERSION, SessionOpening


class PostgresSessionOpenPublisher:
    def __init__(self, *, store: PostgresReliableProcessingStore, target: DeliveryTarget,
                 display_timezone: str) -> None:
        self._store, self._target, self._timezone = store, target, display_timezone

    def publish(self, opening: SessionOpening, *, generated_at: datetime) -> bool:
        if not opening.is_due(generated_at):
            return False
        opens_at = opening.opens_at.astimezone(timezone.utc)
        event_id = str(uuid5(NAMESPACE_URL, f"{SESSION_OPEN_VERSION}:{opening.session_id}:{opens_at.isoformat()}"))
        instrument = "SESSION_" + opening.session_id.upper()
        body = {"event_id": event_id, "event_type": SESSION_OPEN_SIGNAL_TYPE,
                "instrument_id": instrument, "source_time": opens_at.isoformat(),
                "session_id": opening.session_id, "version": SESSION_OPEN_VERSION}
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        event = BrokerEvent(event_id, SESSION_OPEN_SIGNAL_TYPE,
                            SESSION_OPEN_VERSION + "." + opening.session_id, 0,
                            int(opens_at.timestamp()), sha256(canonical).digest(), body)
        signal = SignalRecord(
            signal_id=str(uuid5(NAMESPACE_URL, "signal:" + event_id)), detected_at=generated_at,
            instrument_id=instrument, ticker=opening.session_id.upper(), class_code="SESSION", alias=opening.session_id,
            source_event_type=event.event_type, signal_type=SESSION_OPEN_SIGNAL_TYPE, severity=1,
            metric_value=0, baseline_value=0, z_score=0, window_seconds=60,
            summary=opening.title + ": начало сессии",
            payload={**opening_payload(opening, display_timezone=self._timezone),
                     "delivery_channel": "session", "delivery_status": "delivered",
                     "delivery_reason": "session_open_subscription"},
            source_event_id=event_id, source_event_at=opens_at, signal_schema_version="1.0.0",
            expectation_catalog_version=SESSION_OPEN_VERSION, detector_config_version=SESSION_OPEN_VERSION,
            delivery_config_version=SESSION_OPEN_VERSION, cost_model_version="not_applicable", provenance_status="complete",
        )
        return not self._store.persist_session_open_once(event, PreparedSignal(signal, (self._target,))).replayed
