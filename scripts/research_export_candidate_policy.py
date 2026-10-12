#!/usr/bin/env python3
"""Export a research-only candidate policy from the local candidate ledger."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_extract_candidate_audit_rows import parse_candidate_rule


POLICY_SCHEMA_VERSION = 1


def _read_ledger(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"ledger must be a JSON object: {path}")
    candidates = payload.get("candidates")
    if not isinstance(candidates, dict):
        raise ValueError(f"ledger candidates must be an object: {path}")
    return payload


def _exported_rule(candidate_id: str, candidate: Mapping[str, Any]) -> dict[str, Any]:
    aggregate = candidate.get("aggregate") if isinstance(candidate.get("aggregate"), dict) else {}
    readiness = candidate.get("aggregate_readiness") if isinstance(candidate.get("aggregate_readiness"), dict) else {}
    observations = candidate.get("observations") if isinstance(candidate.get("observations"), list) else []
    source_fingerprints = sorted(
        {
            str(row.get("dataset_fingerprint"))
            for row in observations
            if isinstance(row, dict) and row.get("dataset_fingerprint") not in {None, ""}
        }
    )
    shadow_ready = bool(readiness.get("shadow_ready"))
    product_ready = bool(readiness.get("product_ready"))
    predicates = parse_candidate_rule(candidate.get("rule", ""))
    shadow_decision = predicates.get("decision") or predicates.get("frontier_decision") or ""
    decision_ready = shadow_decision in {"up", "down"}
    return {
        "candidate_id": candidate_id,
        "scope": candidate.get("scope", ""),
        "rule": candidate.get("rule", ""),
        "status": "shadow" if shadow_ready else "watch_only",
        "action": "shadow_evaluate" if shadow_ready else "skip",
        "shadow_decision": shadow_decision,
        "decision_ready": decision_ready,
        "admin_only": True,
        "product_claim_allowed": False,
        "product_ready": product_ready,
        "selected_rows": aggregate.get("selected_rows", 0),
        "sessions": aggregate.get("sessions", 0),
        "success_count": aggregate.get("success_count", 0),
        "success_rate": aggregate.get("success_rate"),
        "wilson_lower_95": aggregate.get("wilson_lower_95"),
        "mean_selected_result_bps": aggregate.get("mean_selected_result_bps"),
        "unique_observations": aggregate.get("unique_observations", 0),
        "unique_dataset_fingerprints": aggregate.get("unique_dataset_fingerprints", 0),
        "source_dataset_fingerprints": source_fingerprints,
        "blocking_reasons": readiness.get("blocking_reasons", []),
        "missing_rows_to_shadow_gate": readiness.get("missing_rows_to_shadow_gate", 0),
        "missing_sessions_to_shadow_gate": readiness.get("missing_sessions_to_shadow_gate", 0),
    }


def export_candidate_policy(ledger_path: Path, *, generated_at: str | None = None) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    ledger = _read_ledger(ledger_path)
    candidates = ledger.get("candidates", {})
    rules = [
        _exported_rule(str(candidate_id), candidate)
        for candidate_id, candidate in sorted(candidates.items())
        if isinstance(candidate, dict)
    ]
    enabled = [rule for rule in rules if rule["status"] == "shadow"]
    return {
        "schema_version": POLICY_SCHEMA_VERSION,
        "kind": "research_candidate_decision_policy",
        "generated_at": generated_at,
        "source_ledger": str(ledger_path),
        "default_action": "skip",
        "status": "shadow" if enabled else "disabled",
        "admin_only": True,
        "product_claim_allowed": False,
        "reason_code": "shadow_candidates_passed_gate" if enabled else "no_candidate_passed_aggregate_research_gate",
        "candidate_count": len(rules),
        "shadow_candidate_count": len(enabled),
        "rules": rules,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-export-candidate-policy")
    parser.add_argument("--ledger", type=Path, default=Path("var/research/candidate-watchlist-ledger.json"))
    parser.add_argument("--output", type=Path, default=Path("var/research/candidate-decision-policy.json"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    policy = export_candidate_policy(args.ledger)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(policy, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    print(
        json.dumps(
            {
                "status": "ok",
                "policy": str(args.output),
                "policy_status": policy["status"],
                "shadow_candidate_count": policy["shadow_candidate_count"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
