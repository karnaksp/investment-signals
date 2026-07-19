#!/usr/bin/env python3
"""Fail when the active replay vocabulary and transport drift from the registry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from tinvest_signal_engine.adapters.scientific_hypothesis_registry import (
    VersionedScientificRegistry,
)
from tinvest_signal_engine.domain.hypothesis_formulas import HYPOTHESIS_RULES_V1
from tinvest_signal_engine.domain.scientific_replay_contract import (
    SCIENTIFIC_REPLAY_CONTRACT_V1,
)
from tinvest_signal_engine.services.hypothesis_replay_api import (
    ALL_HYPOTHESES,
    ReplayEvidenceResponse,
    ReplayHorizonEvidenceResponse,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "config/scientific_hypotheses/registry-v1.yaml"
DEFAULT_CONTRACT = ROOT / "config/scientific_hypotheses/replay-contract-v1.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    args = parser.parse_args(argv)

    registry = VersionedScientificRegistry.from_file(args.registry)
    fixture = json.loads(args.contract.read_text(encoding="utf-8"))
    errors = validate_contract(registry, fixture)
    if errors:
        for error in errors:
            print(f"Scientific replay contract error: {error}", file=sys.stderr)
        return 1
    print(
        "Scientific replay contract: OK "
        f"({len(SCIENTIFIC_REPLAY_CONTRACT_V1)} active hypotheses)"
    )
    return 0


def validate_contract(
    registry: VersionedScientificRegistry,
    fixture: object,
) -> tuple[str, ...]:
    errors: list[str] = []
    if not isinstance(fixture, dict):
        return ("fixture root must be an object",)
    fixture_rows = fixture.get("hypotheses")
    if not isinstance(fixture_rows, list):
        return ("fixture hypotheses must be an array",)
    registry_by_key = {
        (item.hypothesis_id, item.version): item for item in registry.hypotheses
    }
    fixture_by_id = {
        str(item.get("short_id")): item
        for item in fixture_rows
        if isinstance(item, dict)
    }
    contract_ids = tuple(item.short_id for item in SCIENTIFIC_REPLAY_CONTRACT_V1)
    if contract_ids != ALL_HYPOTHESES:
        errors.append("transport hypothesis selection differs from domain contract")
    if set(fixture_by_id) != set(contract_ids):
        errors.append("fixture must contain every active replay definition exactly once")
    contract_registry_keys = {
        (item.catalog_hypothesis_id, item.catalog_version)
        for item in SCIENTIFIC_REPLAY_CONTRACT_V1
    }
    missing_registry_keys = contract_registry_keys - set(registry_by_key)
    if missing_registry_keys:
        errors.append(
            "active replay definitions are absent from the scientific registry: "
            f"{sorted(missing_registry_keys)}"
        )

    for definition in SCIENTIFIC_REPLAY_CONTRACT_V1:
        row = fixture_by_id.get(definition.short_id)
        catalog = registry_by_key.get(
            (definition.catalog_hypothesis_id, definition.catalog_version)
        )
        if row is None or catalog is None:
            continue
        expected = {
            "short_id": definition.short_id,
            "catalog_hypothesis_id": definition.catalog_hypothesis_id,
            "catalog_version": definition.catalog_version,
            "expected_direction": definition.expected_direction,
            "market_phase": definition.market_phase,
            "horizons_seconds": list(definition.horizons_seconds),
            "data_requirement": definition.data_requirement.value,
            "allowed_source_data_states": [
                item.value for item in definition.allowed_source_data_states
            ],
        }
        if row != expected:
            errors.append(f"fixture definition drifted for {definition.short_id}")
        if catalog.expected_direction != definition.expected_direction:
            errors.append(f"registry direction drifted for {definition.short_id}")
        if catalog.market_phase != definition.market_phase:
            errors.append(f"registry market phase drifted for {definition.short_id}")
        if catalog.horizon_seconds != definition.horizons_seconds:
            errors.append(f"registry horizons drifted for {definition.short_id}")
        preregistration = catalog.preregistration
        if preregistration is None or not preregistration.sealed:
            errors.append(f"{definition.short_id} must have a sealed preregistration")
        elif (
            preregistration.expected_direction != definition.expected_direction
            or preregistration.market_phase != definition.market_phase
            or preregistration.horizon_seconds != definition.horizons_seconds
        ):
            errors.append(f"preregistration drifted for {definition.short_id}")
        if definition.short_id in {"H8", "H9"} and catalog.lifecycle.value != "shadow":
            errors.append(f"{definition.short_id} must remain shadow without live evidence")

    executable = {item.hypothesis_id.value for item in HYPOTHESIS_RULES_V1}
    if executable != set(contract_ids[:7]):
        errors.append("candle formula portfolio must remain exactly H1-H7")
    executable_horizons = {
        item.hypothesis_id.value: item.horizons_seconds for item in HYPOTHESIS_RULES_V1
    }
    for definition in SCIENTIFIC_REPLAY_CONTRACT_V1[:7]:
        if executable_horizons.get(definition.short_id) != definition.horizons_seconds:
            errors.append(f"executable horizons drifted for {definition.short_id}")
    strict = fixture.get("strict_evidence")
    if not isinstance(strict, dict):
        errors.append("strict_evidence must be an object")
        return tuple(errors)
    _validate_required_fields(
        strict.get("aggregate_required_fields"),
        ReplayEvidenceResponse,
        "aggregate",
        errors,
    )
    _validate_required_fields(
        strict.get("horizon_required_fields"),
        ReplayHorizonEvidenceResponse,
        "horizon",
        errors,
    )
    if strict.get("decisions") != [
        "passed", "rejected", "inconclusive", "blocked_by_data"
    ]:
        errors.append("strict evidence decision vocabulary drifted")
    response_states = set(
        ReplayEvidenceResponse.model_json_schema()["properties"][
            "source_data_state"
        ]["enum"]
    )
    declared_states = {
        state.value
        for definition in SCIENTIFIC_REPLAY_CONTRACT_V1
        for state in definition.allowed_source_data_states
    }
    if response_states != declared_states:
        errors.append("transport and domain source-data states differ")
    return tuple(errors)


def _validate_required_fields(
    declared: object,
    model: type[object],
    location: str,
    errors: list[str],
) -> None:
    if not isinstance(declared, list) or not all(
        isinstance(item, str) for item in declared
    ):
        errors.append(f"{location} required fields must be an array of strings")
        return
    model_fields = getattr(model, "model_fields")
    missing = set(declared) - set(model_fields)
    optional = {
        name for name in declared
        if name in model_fields and not model_fields[name].is_required()
    }
    if missing:
        errors.append(f"{location} response is missing fields: {sorted(missing)}")
    if optional:
        errors.append(f"{location} fields are not strict: {sorted(optional)}")


if __name__ == "__main__":
    raise SystemExit(main())
