from __future__ import annotations

from copy import deepcopy

from tinvest_signal_engine.poi_delivery import (
    classify_poi_delivery,
    classify_pois_delivery,
    summarize_poi_delivery,
)


def _poi(**overrides) -> dict:
    base = {
        "poi_id": "00000000-0000-4000-8000-000000000001",
        "contract_version": "poi_v1",
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "interest_score": 90,
        "confidence": "high",
        "drivers": [{"signal_type": "volume_spike"}],
        "nearby_signals": [{"signal_type": "volume_spike"}],
    }
    base.update(overrides)
    return base


def test_high_confidence_poi_is_realtime_candidate() -> None:
    poi = _poi(confidence="high", interest_score=90)

    decision = classify_poi_delivery(poi)

    assert decision["delivery_channel"] == "realtime"
    assert decision["delivery_status"] == "delivered_candidate"
    assert decision["delivery_reason"] == "high_confidence_poi"
    assert decision["delivery_priority"] == "high"
    assert decision["delivery_explanation"]


def test_medium_confidence_poi_is_digest_candidate() -> None:
    poi = _poi(confidence="medium", interest_score=55)

    decision = classify_poi_delivery(poi)

    assert decision["delivery_channel"] == "digest"
    assert decision["delivery_status"] == "digest_candidate"
    assert decision["delivery_reason"] == "medium_confidence_poi"
    assert decision["delivery_priority"] == "medium"


def test_low_confidence_poi_is_admin_only() -> None:
    poi = _poi(confidence="low", interest_score=20)

    decision = classify_poi_delivery(poi)

    assert decision["delivery_channel"] == "admin_only"
    assert decision["delivery_status"] == "suppressed"
    assert decision["delivery_reason"] == "low_confidence_or_source"
    assert decision["delivery_priority"] == "low"


def test_stale_source_health_suppresses_otherwise_high_confidence_poi() -> None:
    poi = _poi(
        confidence="high",
        interest_score=95,
        source_health={"trade": {"status": "stale"}},
    )

    decision = classify_poi_delivery(poi)

    assert decision["delivery_channel"] == "admin_only"
    assert decision["delivery_status"] == "suppressed"
    assert decision["delivery_reason"] == "low_confidence_or_source"


def test_classification_does_not_mutate_input_poi() -> None:
    poi = _poi(source_health={"trade": {"status": "ok"}})
    before = deepcopy(poi)

    classify_poi_delivery(poi)
    classify_pois_delivery([poi])

    assert poi == before


def test_summary_counts_and_samples_decisions() -> None:
    decisions = classify_pois_delivery(
        [
            _poi(poi_id="1", confidence="high", interest_score=90),
            _poi(poi_id="2", confidence="medium", interest_score=65),
            _poi(poi_id="3", confidence="low", interest_score=20),
        ]
    )

    summary = summarize_poi_delivery(decisions, sample_limit=2)

    assert summary["count"] == 3
    assert summary["by_channel"] == {"realtime": 1, "digest": 1, "admin_only": 1}
    assert summary["by_status"] == {
        "delivered_candidate": 1,
        "digest_candidate": 1,
        "suppressed": 1,
    }
    assert summary["by_reason"] == {
        "high_confidence_poi": 1,
        "medium_confidence_poi": 1,
        "low_confidence_or_source": 1,
    }
    assert len(summary["samples"]) == 2
