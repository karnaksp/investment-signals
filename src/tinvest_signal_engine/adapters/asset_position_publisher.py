"""Queue one independent, idempotent report for each updated asset."""

from datetime import datetime
from hashlib import sha256
import json
from uuid import NAMESPACE_URL, uuid5

from tinvest_signal_engine.adapters.asset_position_cards import ASSET_NAMES, asset_report_payload, render_asset_positions_html
from tinvest_signal_engine.adapters.postgres_reliability import PostgresReliableProcessingStore
from tinvest_signal_engine.application.asset_positions import build_asset_position_reports
from tinvest_signal_engine.application.reliable_processing import BrokerEvent
from tinvest_signal_engine.domain.asset_positions import ASSET_REPORT_VERSION, AssetPositionReport
from tinvest_signal_engine.domain.commodity_positions import PositionContext
from tinvest_signal_engine.domain.daily_positions import DailyPositionComparison, REPORT_SIGNAL_TYPE
from tinvest_signal_engine.domain.reliable_processing import DeliveryTarget, PreparedSignal, SignalRecord


class PostgresAssetPositionPublisher:
    def __init__(self, *, store: PostgresReliableProcessingStore, target: DeliveryTarget) -> None:
        self._store, self._target = store, target

    def publish(self, comparison: DailyPositionComparison, *, generated_at: datetime,
                context: PositionContext | None = None) -> bool:
        return self.publish_reports(build_asset_position_reports(comparison, context), generated_at=generated_at)

    def publish_reports(self, reports: tuple[AssetPositionReport, ...], *, generated_at: datetime) -> bool:
        queued = False
        for report in reports:
            payload = asset_report_payload(report)
            render_asset_positions_html(payload)
            primary = report.sections[0]
            instrument = "POSITIONS_" + report.commodity.upper()
            event_id = str(uuid5(NAMESPACE_URL, f"{ASSET_REPORT_VERSION}:{report.commodity}:{primary.source}:{primary.trading_day}"))
            identity = dict(payload["sections"][0])
            if primary.period == "weekly" or primary.source == "MOEX":
                # A yearly CSV or multi-market JSON can change because other
                # markets were updated. Only this market's observations define
                # its identity. Original document SHA stays in the stored signal.
                identity.pop("source_sha256")
                identity.pop("previous_source_sha256")
            body = {"event_id": event_id, "event_type": "daily_position_report", "instrument_id": instrument,
                    "source_time": primary.source_event_at.isoformat(), "primary_report": identity,
                    "report_version": ASSET_REPORT_VERSION}
            canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
            event = BrokerEvent(event_id, "daily_position_report", ASSET_REPORT_VERSION + "." + report.commodity, 0,
                                primary.trading_day.toordinal(), sha256(canonical).digest(), body)
            signal = SignalRecord(
                signal_id=str(uuid5(NAMESPACE_URL, "signal:" + event_id)), detected_at=generated_at,
                instrument_id=instrument, ticker=report.commodity.upper(), class_code="REPORT", alias=report.commodity,
                source_event_type=event.event_type, signal_type=REPORT_SIGNAL_TYPE, severity=1,
                metric_value=0, baseline_value=0, z_score=0, window_seconds=86400 if primary.period == "daily" else 604800,
                summary=ASSET_NAMES[report.commodity] + ": позиции игроков",
                payload={**payload, "report_generated_at": generated_at.isoformat(), "delivery_channel": "daily",
                         "delivery_status": "delivered", "delivery_reason": "position_report_subscription"},
                source_event_id=event_id, source_event_at=primary.source_event_at, signal_schema_version="1.0.0",
                expectation_catalog_version=ASSET_REPORT_VERSION, detector_config_version=ASSET_REPORT_VERSION,
                delivery_config_version=ASSET_REPORT_VERSION, cost_model_version="not_applicable", provenance_status="complete",
            )
            stored = self._store.persist_report_once(event, PreparedSignal(signal, (self._target,)))
            queued = queued or not stored.replayed
        return queued
