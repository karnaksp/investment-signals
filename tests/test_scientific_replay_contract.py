"""Cross-boundary drift tests for the checked-in active replay contract."""

from __future__ import annotations

import json
from pathlib import Path

from tinvest_signal_engine.adapters.scientific_hypothesis_registry import (
    VersionedScientificRegistry,
)
from tinvest_signal_engine.domain.scientific_replay_contract import (
    ReplayDataRequirement,
    SCIENTIFIC_REPLAY_CONTRACT_V1,
)

from scripts.validate_scientific_hypothesis_replay_contract import validate_contract


ROOT = Path(__file__).resolve().parents[1]


def test_checked_in_contract_matches_registry_domain_and_transport() -> None:
    registry = VersionedScientificRegistry.from_file(
        ROOT / "config/scientific_hypotheses/registry-v1.yaml"
    )
    fixture = json.loads(
        (ROOT / "config/scientific_hypotheses/replay-contract-v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert validate_contract(registry, fixture) == ()
    assert fixture["contract_version"] == "1.2.0"
    assert tuple(item.short_id for item in SCIENTIFIC_REPLAY_CONTRACT_V1) == (
        "H1",
        "H2",
        "H3",
        "H4",
        "H5",
        "H6",
        "H7",
        "H7V2",
        "H10",
        "H11",
        "H15",
        "H8",
        "H9",
    )
    assert all(
        item.data_requirement is ReplayDataRequirement.LIVE_ORDERBOOK
        for item in SCIENTIFIC_REPLAY_CONTRACT_V1[-2:]
    )
    assert {
        item.short_id: item.catalog_version for item in SCIENTIFIC_REPLAY_CONTRACT_V1
    } == {
        "H1": "1.0.0",
        "H2": "1.0.0",
        "H3": "1.0.0",
        "H4": "1.0.0",
        "H5": "1.0.0",
        "H6": "1.0.0",
        "H7": "1.0.0",
        "H7V2": "2.0.0",
        "H10": "1.0.0",
        "H11": "1.0.0",
        "H15": "1.0.0",
        "H8": "1.0.0",
        "H9": "1.0.0",
    }


def test_contract_validator_detects_direction_drift() -> None:
    registry = VersionedScientificRegistry.from_file(
        ROOT / "config/scientific_hypotheses/registry-v1.yaml"
    )
    fixture = json.loads(
        (ROOT / "config/scientific_hypotheses/replay-contract-v1.json").read_text(
            encoding="utf-8"
        )
    )
    fixture["hypotheses"][0]["expected_direction"] = "continuation"

    errors = validate_contract(registry, fixture)

    assert "fixture definition drifted for H1" in errors
