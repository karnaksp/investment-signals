from __future__ import annotations

from datetime import datetime, timezone

from tinvest_signal_engine.poi import build_pois_from_signal_rows, find_poi


def _signal(
    signal_id: str,
    signal_type: str,
    *,
    detected_at: str = "2026-06-01T10:00:00+00:00",
    quality: int = 70,
    severity: int = 2,
    z_score: float = 5.0,
    payload: dict | None = None,
) -> dict:
    merged_payload = {"quality_score": quality, "current_price": 100.0}
    merged_payload.update(payload or {})
    return {
        "signal_id": signal_id,
        "detected_at": detected_at,
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "class_code": "TQBR",
        "source_event_type": "trade",
        "signal_type": signal_type,
        "severity": severity,
        "metric_value": 100.0,
        "baseline_value": 10.0,
        "z_score": z_score,
        "window_seconds": 60,
        "summary": signal_type,
        "payload": merged_payload,
        "delivery_status": "suppressed",
        "delivery_reason": "test",
    }


def test_single_anomaly_builds_observe_poi() -> None:
    pois = build_pois_from_signal_rows(
        [
            _signal(
                "00000000-0000-4000-8000-000000000001",
                "volume_spike",
            )
        ]
    )

    assert len(pois) == 1
    poi = pois[0]
    assert poi["contract_version"] == "poi_v1"
    assert poi["setup_type"] == "admin_observe"
    assert poi["bias"] == "watch"
    assert poi["ticker"] == "SBER"
    assert poi["interest_score"] > 0
    assert poi["nearby_signals"][0]["signal_type"] == "volume_spike"
    assert poi["human_summary_ru"]


def test_confirmed_combo_cluster_raises_score_and_long_bias() -> None:
    one = build_pois_from_signal_rows(
        [
            _signal(
                "00000000-0000-4000-8000-000000000001",
                "volume_spike",
                quality=65,
            )
        ]
    )[0]
    cluster = build_pois_from_signal_rows(
        [
            _signal(
                "00000000-0000-4000-8000-000000000001",
                "volume_spike",
                quality=65,
            ),
            _signal(
                "00000000-0000-4000-8000-000000000002",
                "price_jump",
                detected_at="2026-06-01T10:01:00+00:00",
                quality=80,
                payload={"price_direction": "up", "current_price": 102.0},
            ),
            _signal(
                "00000000-0000-4000-8000-000000000003",
                "microstructure_combo_long",
                detected_at="2026-06-01T10:02:00+00:00",
                quality=86,
                payload={"direction": "buy", "current_price": 103.0},
            ),
        ]
    )[0]

    assert cluster["setup_type"] == "momentum_breakout"
    assert cluster["bias"] == "long"
    assert cluster["confidence"] in {"medium", "high"}
    assert cluster["interest_score"] > one["interest_score"]
    assert cluster["entry_zone"]["low"] < 103.0 < cluster["entry_zone"]["high"]
    assert cluster["invalidation_price"] < 103.0
    assert cluster["target_1"] > 103.0
    assert cluster["target_2"] > cluster["target_1"]


def test_short_bias_from_combo_and_sell_flow() -> None:
    poi = build_pois_from_signal_rows(
        [
            _signal(
                "00000000-0000-4000-8000-000000000004",
                "microstructure_combo_short",
                payload={"direction": "sell", "current_price": 99.0},
            ),
            _signal(
                "00000000-0000-4000-8000-000000000005",
                "aggressive_trade_burst",
                detected_at="2026-06-01T10:02:00+00:00",
                payload={"direction": "sell", "current_price": 98.5},
            ),
        ]
    )[0]

    assert poi["bias"] == "short"
    assert poi["setup_type"] == "aggressive_flow"
    assert poi["invalidation_price"] > 98.5
    assert poi["target_1"] < 98.5


def test_clusters_split_by_instrument_and_time_window() -> None:
    rows = [
        _signal("00000000-0000-4000-8000-000000000001", "volume_spike"),
        _signal(
            "00000000-0000-4000-8000-000000000002",
            "price_jump",
            detected_at="2026-06-01T10:08:00+00:00",
        ),
        {
            **_signal("00000000-0000-4000-8000-000000000003", "volume_spike"),
            "instrument_id": "GAZP_TQBR",
            "ticker": "GAZP",
        },
    ]

    pois = build_pois_from_signal_rows(rows, cluster_window_seconds=300)

    assert len(pois) == 3
    assert {poi["ticker"] for poi in pois} == {"SBER", "GAZP"}


def test_find_poi_returns_matching_contract_row() -> None:
    pois = build_pois_from_signal_rows(
        [
            _signal(
                "00000000-0000-4000-8000-000000000001",
                "volume_spike",
                detected_at=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc),
            )
        ]
    )

    assert find_poi(pois, pois[0]["poi_id"]) == pois[0]
    assert find_poi(pois, "missing") is None
