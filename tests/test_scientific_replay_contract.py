"""Cross-boundary drift tests for the checked-in H1-H9 replay contract."""

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
    assert tuple(item.short_id for item in SCIENTIFIC_REPLAY_CONTRACT_V1) == tuple(
        f"H{number}" for number in range(1, 10)
    )
    assert all(
        item.data_requirement is ReplayDataRequirement.LIVE_ORDERBOOK
        for item in SCIENTIFIC_REPLAY_CONTRACT_V1[-2:]
    )


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
