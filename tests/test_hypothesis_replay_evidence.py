"""Filesystem-to-transport mapping for scientific replay evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tinvest_signal_engine.adapters.hypothesis_replay_evidence import (
    LocalReplayEvidenceReader,
)


DATASET = "sha256:" + "a" * 64
ARTIFACT = "sha256:" + "b" * 64
GENERATED_AT = "2026-07-19T10:00:00+00:00"


def test_general_artifact_maps_canonical_registry_id_to_short_id(tmp_path: Path) -> None:
    _json(tmp_path / "completion.json", {"artifact_fingerprint": ARTIFACT})
    _json(tmp_path / "manifest.json", {
        "dataset_fingerprint": DATASET,
        "cost_model": {"version": "cost-v1"},
    })
    _json(tmp_path / "split.json", {
        "train_days": ["2026-01-01"],
        "validation_days": ["2026-01-02"],
        "holdout_days": ["2026-01-03"],
    })
    _json(tmp_path / "summaries.json", [{
        "hypothesis_id": "H1",
        "evaluated_observations": 10,
        "abstained_observations": 3,
    }])
    _json(tmp_path / "evidence.json", [_bundle(
        hypothesis_id="h1-morning-low-volume-reversion",
        decision="blocked_by_data",
        sample=0,
        days=0,
        lift=None,
        lower=None,
        upper=None,
        q_value=None,
        stable=0,
        ticker_share=None,
        counts=(),
    )])

    evidence = LocalReplayEvidenceReader().read_general(
        tmp_path,
        ("H1",),
        generated_at=GENERATED_AT,
    )

    assert len(evidence) == 1
    row = evidence[0]
    assert row["hypothesis_id"] == "H1"
    assert row["decision"] == "blocked_by_data"
    assert row["independent_validation"] is True
    assert row["dataset_fingerprint"] == DATASET
    assert row["artifact_fingerprint"] == ARTIFACT
    assert str(row["formula_fingerprint"]).startswith("sha256:")
    assert row["abstention_rate"] == 0.3
    assert row["catalog_hypothesis_id"] == "h1-morning-low-volume-reversion"
    assert row["expected_direction"] == "reversion_to_previous_close"
    assert row["market_phase"] == "morning_0700_0949"
    assert row["source_data_state"] == "insufficient_history"
    assert [item["horizon_seconds"] for item in row["horizons"]] == [1800, 3600]
    assert all(
        item["evidence_scope"] == "descriptive_only" for item in row["horizons"]
    )


def test_jump_horizons_aggregate_fail_closed_and_conservatively(tmp_path: Path) -> None:
    _json(tmp_path / "manifest.json", {
        "input_fingerprint": DATASET,
        "policy": {
            "version": "jump-v1",
            "horizons_seconds": [300, 900, 1800],
            "cost_model": {"version": "cost-v1", "round_trip_bps": 10.0},
        },
        "split": {
            "train_days": ["2026-01-01"],
            "validation_days": ["2026-01-02"],
            "holdout_days": ["2026-01-03"],
        },
    })
    rows: list[dict[str, Any]] = []
    h3_inputs = (
        (300, "passed", 350, 35, 4.0, 2.0, 6.0, 0.01, 5, 0.20, (10, 10, 10, 10, 10)),
        (900, "passed", 340, 34, 3.0, 1.0, 5.0, 0.02, 4, 0.25, (20, 10, 10, 10, 10)),
        (1800, "rejected", 330, 33, -0.5, -1.0, 1.0, 0.04, 1, 0.30, (20, 20, 10, 10, 10)),
    )
    h4_inputs = (
        (300, "passed"),
        (900, "blocked_by_data"),
        (1800, "inconclusive"),
    )
    for horizon, decision, sample, days, lift, lower, upper, q_value, stable, share, counts in h3_inputs:
        rows.append({
            "hypothesis": "H3",
            "horizon_seconds": horizon,
            "bundle": _bundle(
                hypothesis_id=f"H3-{horizon}s",
                decision=decision,
                sample=sample,
                days=days,
                lift=lift,
                lower=lower,
                upper=upper,
                q_value=q_value,
                stable=stable,
                ticker_share=share,
                counts=counts,
            ),
        })
    for horizon, decision in h4_inputs:
        rows.append({
            "hypothesis": "H4",
            "horizon_seconds": horizon,
            "bundle": _bundle(
                hypothesis_id=f"H4-{horizon}s",
                decision=decision,
                sample=400,
                days=40,
                lift=None if decision == "blocked_by_data" else 2.0,
                lower=None if decision == "blocked_by_data" else 1.0,
                upper=None if decision == "blocked_by_data" else 3.0,
                q_value=None if decision == "blocked_by_data" else 0.01,
                stable=0 if decision == "blocked_by_data" else 4,
                ticker_share=None if decision == "blocked_by_data" else 0.20,
                counts=() if decision == "blocked_by_data" else (10, 10, 10, 10, 10),
            ),
        })
    _json(tmp_path / "evidence.json", rows)
    (tmp_path / "complete.json").write_text("{}\n", encoding="utf-8")
    observation_rows = [
        _observation(hypothesis, second_available=available)
        for hypothesis, available in (("H3", False), ("H3", True), ("H4", True))
    ]
    (tmp_path / "observations.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in observation_rows),
        encoding="utf-8",
    )

    evidence = LocalReplayEvidenceReader().read_jump(
        tmp_path,
        ("H3", "H4"),
        generated_at=GENERATED_AT,
    )

    h3, h4 = evidence
    assert h3["decision"] == "rejected"
    assert h3["sample_count"] == 330
    assert h3["trading_days"] == 33
    assert h3["primary_metric_value"] == -0.5
    assert h3["matched_control_lift_ci95_lower"] == -1.0
    assert h3["matched_control_lift_ci95_upper"] == 1.0
    assert h3["adjusted_p_value"] == 0.04
    assert h3["stable_blocks"] == 1
    assert h3["maximum_ticker_share"] == 0.30
    assert h3["maximum_period_share"] == 2 / 6
    assert h3["abstention_rate"] == 0.5
    assert h4["decision"] == "blocked_by_data"
    assert h4["primary_metric_value"] is None
    assert h4["adjusted_p_value"] is None
    assert h4["maximum_ticker_share"] is None
    assert [item["decision"] for item in h3["horizons"]] == [
        "passed", "passed", "rejected",
    ]
    assert [item["horizon_seconds"] for item in h4["horizons"]] == [
        300, 900, 1800,
    ]
    assert all(
        item["evidence_scope"] == "independent_gate" for item in h3["horizons"]
    )


def _bundle(
    *,
    hypothesis_id: str,
    decision: str,
    sample: int,
    days: int,
    lift: float | None,
    lower: float | None,
    upper: float | None,
    q_value: float | None,
    stable: int,
    ticker_share: float | None,
    counts: tuple[int, ...],
) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis_id,
        "decision": decision,
        "dataset_fingerprint": DATASET,
        "cost_model_version": "cost-v1",
        "eligible_events": sample,
        "trading_days": days,
        "mean_lift_bps": lift,
        "lift_interval": None if lower is None else {
            "lower": lower,
            "estimate": lift,
            "upper": upper,
            "confidence_level": 0.95,
        },
        "matched_events": sample,
        "matched_controls": sample * 5,
        "adjusted_q_value": q_value,
        "stability": {
            "positive_blocks": stable,
            "blocks": [
                {"observation_count": count} for count in counts
            ],
        },
        "maximum_instrument_share": ticker_share,
    }


def _observation(hypothesis: str, *, second_available: bool) -> dict[str, Any]:
    return {
        "hypothesis": hypothesis,
        "outcomes": [
            {"horizon_seconds": 300, "available": True},
            {"horizon_seconds": 900, "available": second_available},
            {"horizon_seconds": 1800, "available": True},
        ],
    }


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
