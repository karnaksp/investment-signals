"""Lightweight inbound validation (JSON-shaped logs; no heavy GE stack)."""

from __future__ import annotations

import logging
from typing import Any

from .serialization import json_dumps, quotation_to_float

logger = logging.getLogger(__name__)

_REQUIRED_EVENT_KEYS = frozenset(
    {
        "event_id",
        "event_type",
        "instrument_id",
        "ticker",
        "class_code",
        "alias",
        "lot",
        "source_time",
        "received_at",
        "payload",
    }
)


def validate_normalized_event_dict(data: dict[str, Any]) -> list[str]:
    """Return human-readable issues; empty list means OK for downstream."""
    errors: list[str] = []
    missing = sorted(_REQUIRED_EVENT_KEYS.difference(data))
    if missing:
        errors.append(f"missing_keys:{','.join(missing)}")
        return errors

    try:
        int(data.get("lot", 0))
    except (TypeError, ValueError):
        errors.append("lot_invalid")

    event_type = str(data.get("event_type", "")).strip()
    if not event_type:
        errors.append("empty_event_type")

    payload = data.get("payload")
    if not isinstance(payload, dict):
        errors.append("payload_not_object")
        return errors

    if event_type == "trade":
        qty = payload.get("quantity")
        try:
            qf = float(qty)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            errors.append("trade_quantity_invalid")
        else:
            if qf < 0:
                errors.append("trade_quantity_negative")
        price = quotation_to_float(payload.get("price"))
        if price is None or price <= 0:
            errors.append("trade_price_invalid")

    if event_type == "orderbook":
        bids = payload.get("bids")
        asks = payload.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            errors.append("orderbook_levels_not_lists")
        elif not bids or not asks:
            errors.append("orderbook_empty_side")

    if event_type == "trading_status":
        status = str(payload.get("trading_status", "")).strip()
        if not status:
            errors.append("trading_status_empty")

    if event_type == "candle":
        for key in ("open", "high", "low", "close"):
            px = quotation_to_float(payload.get(key))
            if px is None or px <= 0:
                errors.append(f"candle_{key}_invalid")
                break

    if event_type == "open_interest":
        try:
            oi = int(payload.get("open_interest", 0))
        except (TypeError, ValueError):
            errors.append("open_interest_invalid")
        else:
            if oi < 0:
                errors.append("open_interest_negative")

    if event_type == "market_values":
        vals = payload.get("values")
        if not isinstance(vals, list):
            errors.append("market_values_values_not_list")
        src = str(payload.get("source", "")).strip()
        if not src:
            errors.append("market_values_source_empty")
        pb = str(payload.get("poll_batch_id", "")).strip()
        if not pb:
            errors.append("market_values_poll_batch_id_empty")

    if event_type == "tech_analysis":
        resp = payload.get("response")
        if resp is not None and not isinstance(resp, dict):
            errors.append("tech_analysis_response_not_object")
        src = str(payload.get("source", "")).strip()
        if not src:
            errors.append("tech_analysis_source_empty")
        pb = str(payload.get("poll_batch_id", "")).strip()
        if not pb:
            errors.append("tech_analysis_poll_batch_id_empty")

    return errors


def log_validation_failure(*, errors: list[str], sample: dict[str, Any]) -> None:
    """Structured JSON log line for observability pipelines."""
    safe_sample = {
        "event_id": sample.get("event_id"),
        "event_type": sample.get("event_type"),
        "instrument_id": sample.get("instrument_id"),
        "ticker": sample.get("ticker"),
        "lot": sample.get("lot"),
    }
    logger.warning(
        json_dumps(
            {
                "component": "data_quality",
                "severity": "warning",
                "message": "normalized_event_validation_failed",
                "errors": errors,
                "sample": safe_sample,
            }
        )
    )
