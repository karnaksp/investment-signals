"""Dry-run delivery policy for point-of-interest contracts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

POLICY_VERSION = "poi_delivery_v4"

DEFAULT_REALTIME_INTEREST_THRESHOLD = 82
DEFAULT_DIGEST_INTEREST_THRESHOLD = 62

CHANNEL_REALTIME = "realtime"
CHANNEL_DIGEST = "digest"
CHANNEL_ADMIN_ONLY = "admin_only"

STATUS_DELIVERED_CANDIDATE = "delivered_candidate"
STATUS_DIGEST_CANDIDATE = "digest_candidate"
STATUS_SUPPRESSED = "suppressed"

REASON_HIGH_CONFIDENCE = "high_confidence_poi"
REASON_MEDIUM_CONFIDENCE = "medium_confidence_poi"
REASON_LOW_CONFIDENCE_OR_SOURCE = "low_confidence_or_source"

_BAD_SOURCE_STATUSES = {"bad", "down", "error", "missing", "stale"}


def classify_poi_delivery(
    poi: Mapping[str, Any],
    *,
    realtime_interest_threshold: int = DEFAULT_REALTIME_INTEREST_THRESHOLD,
    digest_interest_threshold: int = DEFAULT_DIGEST_INTEREST_THRESHOLD,
) -> dict[str, Any]:
    """Classify one POI contract without mutating the input mapping."""
    confidence = str(poi.get("confidence") or "").lower()
    score = _number(poi.get("interest_score"))
    source_bad = _source_health_bad(poi.get("source_health"))

    if source_bad or confidence == "low":
        return _decision(
            poi,
            channel=CHANNEL_ADMIN_ONLY,
            status=STATUS_SUPPRESSED,
            reason=REASON_LOW_CONFIDENCE_OR_SOURCE,
            priority="low",
            explanation="POI is held for admin review because confidence is low or source health is stale/bad.",
        )

    if (
        confidence == "high"
        and score >= float(realtime_interest_threshold)
        and not source_bad
    ):
        return _decision(
            poi,
            channel=CHANNEL_REALTIME,
            status=STATUS_DELIVERED_CANDIDATE,
            reason=REASON_HIGH_CONFIDENCE,
            priority="high",
            explanation="High-confidence POI with sufficient interest score and acceptable source health.",
        )

    if confidence == "medium" or score >= float(digest_interest_threshold):
        return _decision(
            poi,
            channel=CHANNEL_DIGEST,
            status=STATUS_DIGEST_CANDIDATE,
            reason=REASON_MEDIUM_CONFIDENCE,
            priority="medium",
            explanation="POI is useful for digest review but does not meet realtime delivery criteria.",
        )

    return _decision(
        poi,
        channel=CHANNEL_ADMIN_ONLY,
        status=STATUS_SUPPRESSED,
        reason=REASON_LOW_CONFIDENCE_OR_SOURCE,
        priority="low",
        explanation="POI is held for admin review because confidence and interest score are low.",
    )


def classify_pois_delivery(
    pois: Iterable[Mapping[str, Any]],
    *,
    realtime_interest_threshold: int = DEFAULT_REALTIME_INTEREST_THRESHOLD,
    digest_interest_threshold: int = DEFAULT_DIGEST_INTEREST_THRESHOLD,
) -> list[dict[str, Any]]:
    """Classify many POI contracts without mutating them."""
    return [
        classify_poi_delivery(
            poi,
            realtime_interest_threshold=realtime_interest_threshold,
            digest_interest_threshold=digest_interest_threshold,
        )
        for poi in pois
    ]


def summarize_poi_delivery(
    decisions: Iterable[Mapping[str, Any]],
    *,
    sample_limit: int = 5,
) -> dict[str, Any]:
    """Aggregate dry-run POI delivery decisions."""
    items = [dict(item) for item in decisions]
    return {
        "count": len(items),
        "by_channel": dict(Counter(str(item.get("delivery_channel") or "unknown") for item in items)),
        "by_status": dict(Counter(str(item.get("delivery_status") or "unknown") for item in items)),
        "by_reason": dict(Counter(str(item.get("delivery_reason") or "unknown") for item in items)),
        "samples": items[: max(0, int(sample_limit))],
    }


def _decision(
    poi: Mapping[str, Any],
    *,
    channel: str,
    status: str,
    reason: str,
    priority: str,
    explanation: str,
) -> dict[str, Any]:
    return {
        "poi_id": poi.get("poi_id"),
        "instrument_id": poi.get("instrument_id"),
        "ticker": poi.get("ticker"),
        "contract_version": poi.get("contract_version"),
        "interest_score": poi.get("interest_score"),
        "confidence": poi.get("confidence"),
        "delivery_policy_version": POLICY_VERSION,
        "delivery_channel": channel,
        "delivery_status": status,
        "delivery_reason": reason,
        "delivery_priority": priority,
        "delivery_explanation": explanation,
    }


def _number(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _source_health_bad(value: Any) -> bool:
    if value is None:
        return False
    return any(status in _BAD_SOURCE_STATUSES for status in _source_statuses(value))


def _source_statuses(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.lower()]
    if isinstance(value, Mapping):
        statuses: list[str] = []
        own_status = value.get("status")
        if isinstance(own_status, str):
            statuses.append(own_status.lower())
        for item in value.values():
            if isinstance(item, Mapping):
                status = item.get("status")
                if isinstance(status, str):
                    statuses.append(status.lower())
            elif isinstance(item, str):
                statuses.append(item.lower())
        return statuses
    if isinstance(value, Iterable):
        statuses = []
        for item in value:
            statuses.extend(_source_statuses(item))
        return statuses
    return []
