#!/usr/bin/env python3
"""Validate scientific provenance and applied-catalog admission."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from tinvest_signal_engine.adapters.scientific_hypothesis_registry import (
    ScientificRegistryFormatError,
    VersionedScientificRegistry,
)
from tinvest_signal_engine.application.scientific_hypotheses import (
    AssessScientificHypothesisAdmission,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "config" / "scientific_hypotheses" / "registry-v1.yaml"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check the scientific source registry and applied hypotheses."
    )
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        registry = VersionedScientificRegistry.from_file(args.registry)
    except ScientificRegistryFormatError as exc:
        print(f"Scientific registry format error: {exc}", file=sys.stderr)
        return 2

    gate = AssessScientificHypothesisAdmission(
        sources=registry,
        applied_catalog=registry,
    )
    decisions = []
    unresolved = []
    for reference in registry.applied_catalog:
        hypothesis = registry.get_hypothesis(reference.hypothesis_id, reference.version)
        evidence = registry.get_evidence(reference.evidence_id)
        if hypothesis is None:
            unresolved.append(
                f"missing hypothesis {reference.hypothesis_id}@{reference.version}"
            )
            continue
        if evidence is None:
            unresolved.append(f"missing evidence {reference.evidence_id}")
            continue
        decisions.append(gate.execute(hypothesis, evidence))

    payload = {
        "schema_version": registry.schema_version,
        "sources": len(registry.sources),
        "hypotheses": len(registry.hypotheses),
        "applied": len(registry.applied_catalog),
        "unresolved": unresolved,
        "decisions": [
            {
                "hypothesis_id": decision.hypothesis_id,
                "version": decision.version,
                "admitted": decision.admitted,
                "scientific_support_allowed": decision.scientific_support_allowed,
                "issues": [
                    {"code": issue.code.value, "detail": issue.detail}
                    for issue in decision.issues
                ],
            }
            for decision in decisions
        ],
    }
    valid = not unresolved and all(decision.admitted for decision in decisions)
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    elif valid:
        print(
            "Scientific hypothesis registry: OK "
            f"({len(registry.sources)} sources, {len(decisions)} applied)"
        )
    else:
        print("Scientific hypothesis registry: REJECTED", file=sys.stderr)
        for item in unresolved:
            print(f"- {item}", file=sys.stderr)
        for decision in decisions:
            for issue in decision.issues:
                suffix = f": {issue.detail}" if issue.detail else ""
                print(
                    f"- {decision.hypothesis_id}@{decision.version}: "
                    f"{issue.code.value}{suffix}",
                    file=sys.stderr,
                )
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
