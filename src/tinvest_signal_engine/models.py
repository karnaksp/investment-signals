"""Доменные модели потока: нормализованное событие и сигнал детектора."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .serialization import parse_timestamp, utc_now


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    event_type: str
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    figi: str
    uid: str
    lot: int
    source_time: datetime
    received_at: datetime
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "instrument_id": self.instrument_id,
            "ticker": self.ticker,
            "class_code": self.class_code,
            "alias": self.alias,
            "figi": self.figi,
            "uid": self.uid,
            "lot": int(self.lot),
            "source_time": self.source_time.isoformat(),
            "received_at": self.received_at.isoformat(),
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NormalizedEvent":
        return cls(
            event_id=str(data["event_id"]),
            event_type=str(data["event_type"]),
            instrument_id=str(data["instrument_id"]),
            ticker=str(data["ticker"]),
            class_code=str(data["class_code"]),
            alias=str(data["alias"]),
            figi=str(data.get("figi", "")),
            uid=str(data.get("uid", "")),
            lot=int(data.get("lot", 0) or 0),
            source_time=parse_timestamp(data["source_time"]),
            received_at=parse_timestamp(data["received_at"]),
            payload=dict(data.get("payload", {})),
        )


@dataclass(frozen=True)
class TriggerSignal:
    signal_id: str
    detected_at: datetime
    instrument_id: str
    ticker: str
    class_code: str
    alias: str
    source_event_type: str
    signal_type: str
    severity: int
    metric_value: float
    baseline_value: float
    z_score: float
    window_seconds: int
    summary: str
    payload: dict[str, Any] = field(default_factory=dict)
    source_event_id: str | None = None
    source_event_at: datetime | None = None
    signal_schema_version: str = "1.0.0"
    expectation_catalog_version: str | None = None
    detector_config_version: str | None = None
    delivery_config_version: str | None = None
    cost_model_version: str | None = None
    provenance_status: str = "legacy"

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "detected_at": self.detected_at.isoformat(),
            "instrument_id": self.instrument_id,
            "ticker": self.ticker,
            "class_code": self.class_code,
            "alias": self.alias,
            "source_event_type": self.source_event_type,
            "signal_type": self.signal_type,
            "severity": self.severity,
            "metric_value": self.metric_value,
            "baseline_value": self.baseline_value,
            "z_score": self.z_score,
            "window_seconds": self.window_seconds,
            "summary": self.summary,
            "payload": self.payload,
            "source_event_id": self.source_event_id,
            "source_event_at": (
                self.source_event_at.isoformat() if self.source_event_at else None
            ),
            "signal_schema_version": self.signal_schema_version,
            "expectation_catalog_version": self.expectation_catalog_version,
            "detector_config_version": self.detector_config_version,
            "delivery_config_version": self.delivery_config_version,
            "cost_model_version": self.cost_model_version,
            "provenance_status": self.provenance_status,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TriggerSignal":
        return cls(
            signal_id=str(data["signal_id"]),
            detected_at=parse_timestamp(data["detected_at"]),
            instrument_id=str(data["instrument_id"]),
            ticker=str(data["ticker"]),
            class_code=str(data["class_code"]),
            alias=str(data["alias"]),
            source_event_type=str(data["source_event_type"]),
            signal_type=str(data["signal_type"]),
            severity=int(data["severity"]),
            metric_value=float(data["metric_value"]),
            baseline_value=float(data["baseline_value"]),
            z_score=float(data["z_score"]),
            window_seconds=int(data["window_seconds"]),
            summary=str(data["summary"]),
            payload=dict(data.get("payload", {})),
            source_event_id=(
                str(data["source_event_id"])
                if data.get("source_event_id") is not None
                else None
            ),
            source_event_at=(
                parse_timestamp(data["source_event_at"])
                if data.get("source_event_at") is not None
                else None
            ),
            signal_schema_version=str(
                data.get("signal_schema_version") or "1.0.0"
            ),
            expectation_catalog_version=(
                str(data["expectation_catalog_version"])
                if data.get("expectation_catalog_version") is not None
                else None
            ),
            detector_config_version=(
                str(data["detector_config_version"])
                if data.get("detector_config_version") is not None
                else None
            ),
            delivery_config_version=(
                str(data["delivery_config_version"])
                if data.get("delivery_config_version") is not None
                else None
            ),
            cost_model_version=(
                str(data["cost_model_version"])
                if data.get("cost_model_version") is not None
                else None
            ),
            provenance_status=str(data.get("provenance_status") or "legacy"),
        )


def make_received_at() -> datetime:
    return utc_now()
