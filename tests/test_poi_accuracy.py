from __future__ import annotations

import json
from pathlib import Path

import pytest

from tinvest_signal_engine.poi_accuracy import (
    empty_poi_accuracy_summary,
    load_poi_accuracy_summary,
    summarize_poi_accuracy,
)


def test_empty_poi_accuracy_summary_is_safe_contract() -> None:
    summary = summarize_poi_accuracy([])

    assert summary == empty_poi_accuracy_summary()
    assert summary["horizons"] == []
    assert summary["by_setup_type"] == []
    assert summary["by_bias"] == []
    assert summary["by_ticker"] == []
    assert summary["by_score_tier"] == []


def test_summarizes_labelled_poi_rows_by_horizon_and_groups() -> None:
    rows = [
        {
            "horizon": "5",
            "setup_type": "momentum_breakout",
            "bias": "long",
            "ticker": "SBER",
            "interest_score": 86,
            "outcome": "hit",
            "forward_return_pct": 1.2,
            "mfe_pct": 2.0,
            "mae_pct": -0.2,
        },
        {
            "horizon": "5",
            "setup_type": "momentum_breakout",
            "bias": "long",
            "ticker": "GAZP",
            "interest_score": 64,
            "outcome": "miss",
            "forward_return_pct": -0.5,
            "mfe_pct": 0.5,
            "mae_pct": -1.0,
        },
        {
            "horizon": "15",
            "setup_type": "reversal_watch",
            "bias": "short",
            "ticker": "SBER",
            "interest_score": 45,
            "directional_hit": True,
            "forward_return_pct": -2.0,
            "max_favorable_excursion_pct": 3.0,
            "max_adverse_excursion_pct": -0.4,
        },
    ]

    summary = summarize_poi_accuracy(rows)

    assert summary["contract_version"] == "poi_accuracy_v1"
    assert summary["horizons"] == [
        {
            "horizon": "5",
            "count": 2,
            "poi_count": 2,
            "directional_hits": 1,
            "directional_misses": 1,
            "directional_decided": 2,
            "directional_hit_rate": 0.5,
            "median_forward_return_pct": pytest.approx(0.35),
            "median_mfe_pct": pytest.approx(1.25),
            "median_mae_pct": pytest.approx(-0.6),
        },
        {
            "horizon": "15",
            "count": 1,
            "poi_count": 1,
            "directional_hits": 1,
            "directional_misses": 0,
            "directional_decided": 1,
            "directional_hit_rate": 1.0,
            "median_forward_return_pct": -2.0,
            "median_mfe_pct": 3.0,
            "median_mae_pct": -0.4,
        },
    ]

    assert summary["by_setup_type"][0] == {
        "horizon": "5",
        "setup_type": "momentum_breakout",
        "count": 2,
        "poi_count": 2,
        "directional_hits": 1,
        "directional_misses": 1,
        "directional_decided": 2,
        "directional_hit_rate": 0.5,
        "median_forward_return_pct": pytest.approx(0.35),
        "median_mfe_pct": pytest.approx(1.25),
        "median_mae_pct": pytest.approx(-0.6),
    }
    assert {row["bias"] for row in summary["by_bias"]} == {"long", "short"}
    assert {row["ticker"] for row in summary["by_ticker"]} == {"SBER", "GAZP"}
    assert {row["score_tier"] for row in summary["by_score_tier"]} == {
        "high",
        "medium",
        "low",
    }


def test_summarizes_aggregate_metric_rows() -> None:
    rows = [
        {
            "horizon": 1,
            "setup_type": "lead_lag",
            "bias": "long",
            "ticker": "SBER",
            "score_tier": "high",
            "poi_count": 3,
            "directional_hits": 2,
            "directional_misses": 1,
            "median_forward_return_pct": 0.4,
            "median_mfe_pct": 1.1,
            "median_mae_pct": -0.3,
        },
        {
            "horizon": 1,
            "setup_type": "lead_lag",
            "bias": "long",
            "ticker": "GAZP",
            "score_tier": "medium",
            "poi_count": 2,
            "directional_hits": 0,
            "directional_misses": 2,
            "median_forward_return_pct": -0.2,
            "median_mfe_pct": 0.2,
            "median_mae_pct": -0.9,
        },
    ]

    summary = summarize_poi_accuracy({"metric_rows": rows})

    assert summary["horizons"][0]["horizon"] == "1"
    assert summary["horizons"][0]["count"] == 5
    assert summary["horizons"][0]["directional_hits"] == 2
    assert summary["horizons"][0]["directional_misses"] == 3
    assert summary["horizons"][0]["directional_hit_rate"] == pytest.approx(0.4)
    assert summary["horizons"][0]["median_forward_return_pct"] == 0.4
    assert summary["by_setup_type"][0]["setup_type"] == "lead_lag"
    assert summary["by_setup_type"][0]["count"] == 5
    assert [row["ticker"] for row in summary["by_ticker"]] == ["SBER", "GAZP"]


def test_missing_labels_and_values_do_not_break_summary() -> None:
    summary = summarize_poi_accuracy(
        [
            {
                "horizon": "",
                "setup_type": "",
                "ticker": None,
            }
        ]
    )

    assert summary["horizons"][0]["horizon"] == "all"
    assert summary["horizons"][0]["count"] == 1
    assert summary["horizons"][0]["directional_decided"] == 0
    assert summary["horizons"][0]["directional_hit_rate"] is None
    assert summary["horizons"][0]["median_forward_return_pct"] is None
    assert summary["horizons"][0]["median_mfe_pct"] is None
    assert summary["horizons"][0]["median_mae_pct"] is None
    assert summary["by_setup_type"][0]["setup_type"] == "unknown"
    assert summary["by_ticker"][0]["ticker"] == "unknown"
    assert summary["by_score_tier"][0]["score_tier"] == "unknown"


def test_load_poi_accuracy_summary_from_json_content(tmp_path: Path) -> None:
    path = tmp_path / "var" / "accuracy" / "poi_accuracy.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "forward_bars": 5,
                        "setup_type": "momentum_breakout",
                        "bias": "long",
                        "ticker": "SBER",
                        "score_tier": "high",
                        "outcome": "hit",
                        "forward_return_pct": 0.8,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    loaded = load_poi_accuracy_summary(path)
    missing = load_poi_accuracy_summary(tmp_path / "missing.json")

    assert loaded["status"] == "ok"
    assert loaded["summary"]["horizons"][0]["horizon"] == "5"
    assert loaded["summary"]["horizons"][0]["directional_hits"] == 1
    assert loaded["raw"]["rows"][0]["ticker"] == "SBER"
    assert missing["status"] == "missing"
    assert missing["summary"] == empty_poi_accuracy_summary()
