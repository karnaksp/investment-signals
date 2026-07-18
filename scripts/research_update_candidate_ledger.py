#!/usr/bin/env python3
"""Update the local research candidate ledger from a candidate watchlist."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


LEDGER_SCHEMA_VERSION = 1


def stable_candidate_id(scope: object, rule: object) -> str:
    payload = json.dumps(
        {
            "schema": "signal_candidate_v1",
            "scope": str(scope),
            "rule": str(rule),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _read_watchlist(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_ledger(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "kind": "signal_candidate_watchlist_ledger",
            "candidates": {},
        }
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"ledger must be a JSON object: {path}")
    data.setdefault("schema_version", LEDGER_SCHEMA_VERSION)
    data.setdefault("kind", "signal_candidate_watchlist_ledger")
    data.setdefault("candidates", {})
    if not isinstance(data["candidates"], dict):
        raise ValueError(f"ledger candidates must be an object: {path}")
    return data


def _float_or_none(value: object) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: object) -> int:
    numeric = _float_or_none(value)
    return int(numeric) if numeric is not None else 0


def _parse_day_list(value: object) -> list[str]:
    if isinstance(value, list):
        return sorted({str(item) for item in value if item not in {None, ""}})
    if value in {None, ""}:
        return []
    return sorted({item for item in str(value).split("|") if item})


def _run_metadata(run_dir: Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {}
    path = run_dir / "model-results.json"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return {
        "dataset": payload.get("dataset"),
        "dataset_fingerprint": payload.get("dataset_fingerprint"),
        "dataset_rows": payload.get("dataset_rows"),
        "validation_sessions": payload.get("validation_sessions"),
    }


def _dedupe_key(observation: Mapping[str, Any]) -> str:
    fingerprint = observation.get("dataset_fingerprint")
    if fingerprint not in {None, ""}:
        return f"dataset:{fingerprint}"
    return f"run:{observation.get('run_id', '')}"


def _same_observation_identity(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    existing_fingerprint = existing.get("dataset_fingerprint")
    incoming_fingerprint = incoming.get("dataset_fingerprint")
    if (
        existing_fingerprint not in {None, ""}
        and incoming_fingerprint not in {None, ""}
        and existing_fingerprint == incoming_fingerprint
    ):
        return True
    if existing.get("run_id") not in {None, ""} and existing.get("run_id") == incoming.get("run_id"):
        return True
    return _dedupe_key(existing) == _dedupe_key(incoming)


def _readiness_from_observation(observation: Mapping[str, Any]) -> dict[str, Any]:
    selected_rows = _int_or_zero(observation.get("selected_rows"))
    sessions = _int_or_zero(observation.get("sessions"))
    success_rate = _float_or_none(observation.get("success_rate"))
    lower = _float_or_none(observation.get("wilson_lower_95"))
    mean_result = _float_or_none(observation.get("mean_selected_result_bps"))
    shadow_ready = bool(
        selected_rows >= 300
        and sessions >= 30
        and success_rate is not None
        and success_rate >= 0.90
        and lower is not None
        and lower >= 0.75
        and mean_result is not None
        and mean_result > 0
    )
    product_ready = bool(shadow_ready and lower is not None and lower >= 0.90)
    reasons = []
    if selected_rows < 300:
        reasons.append("sample_size")
    if sessions < 30:
        reasons.append("trading_days")
    if success_rate is None or success_rate < 0.90:
        reasons.append("observed_success_rate")
    if lower is None or lower < 0.75:
        reasons.append("shadow_reliability_bound")
    if lower is None or lower < 0.90:
        reasons.append("product_reliability_bound")
    if mean_result is None or mean_result <= 0:
        reasons.append("positive_result")
    return {
        "shadow_ready": shadow_ready,
        "product_ready": product_ready,
        "blocking_reasons": reasons,
        "missing_rows_to_shadow_gate": max(0, 300 - selected_rows),
        "missing_sessions_to_shadow_gate": max(0, 30 - sessions),
    }


def _wilson_lower_bound(successes: int, total: int, z: float = 1.959963984540054) -> float | None:
    if total <= 0:
        return None
    phat = successes / total
    denominator = 1 + z * z / total
    centre = phat + z * z / (2 * total)
    margin = z * ((phat * (1 - phat) + z * z / (4 * total)) / total) ** 0.5
    return (centre - margin) / denominator


def _aggregate_observations(observations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    selected_rows = sum(_int_or_zero(row.get("selected_rows")) for row in observations)
    selected_day_sets = [_parse_day_list(row.get("selected_trading_days")) for row in observations]
    selected_days = sorted({day for days in selected_day_sets for day in days})
    sessions = len(selected_days) if selected_days else sum(_int_or_zero(row.get("sessions")) for row in observations)
    successes = sum(_int_or_zero(row.get("success_count")) for row in observations)
    weighted_result_numerator = sum(
        (_float_or_none(row.get("mean_selected_result_bps")) or 0.0) * _int_or_zero(row.get("selected_rows"))
        for row in observations
    )
    success_rate = successes / selected_rows if selected_rows else None
    lower = _wilson_lower_bound(successes, selected_rows)
    mean_result = weighted_result_numerator / selected_rows if selected_rows else None
    fingerprints = sorted(
        {
            str(row.get("dataset_fingerprint"))
            for row in observations
            if row.get("dataset_fingerprint") not in {None, ""}
        }
    )
    run_ids = sorted({str(row.get("run_id")) for row in observations if row.get("run_id") not in {None, ""}})
    return {
        "unique_observations": len(observations),
        "unique_dataset_fingerprints": len(fingerprints),
        "run_ids": run_ids,
        "selected_rows": selected_rows,
        "sessions": sessions,
        "selected_trading_days": selected_days,
        "success_count": successes,
        "success_rate": success_rate,
        "wilson_lower_95": lower,
        "mean_selected_result_bps": mean_result,
    }


def _observation_from_row(
    row: Mapping[str, Any],
    *,
    run_id: str,
    run_dir: str,
    observed_at: str,
    run_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "observed_at": observed_at,
        "run_id": run_id,
        "run_dir": run_dir,
        "dataset": run_metadata.get("dataset"),
        "dataset_fingerprint": run_metadata.get("dataset_fingerprint"),
        "dataset_rows": run_metadata.get("dataset_rows"),
        "validation_sessions": run_metadata.get("validation_sessions"),
        "selected_rows": _int_or_zero(row.get("selected_rows")),
        "sessions": _int_or_zero(row.get("sessions")),
        "selected_trading_days": _parse_day_list(row.get("selected_trading_days")),
        "success_count": _int_or_zero(row.get("success_count")),
        "success_rate": _float_or_none(row.get("success_rate")),
        "wilson_lower_95": _float_or_none(row.get("wilson_lower_95")),
        "mean_selected_result_bps": _float_or_none(row.get("mean_selected_result_bps")),
        "missing_rows_to_shadow_gate": _int_or_zero(row.get("missing_rows_to_shadow_gate")),
        "missing_sessions_to_shadow_gate": _int_or_zero(row.get("missing_sessions_to_shadow_gate")),
        "additional_successes_needed_for_90pct_at_300": _int_or_zero(
            row.get("additional_successes_needed_for_90pct_at_300")
        ),
        "missing_reasons": str(row.get("missing_reasons", "")),
        "status": str(row.get("status", "watch_only") or "watch_only"),
        "product_claim_allowed": str(row.get("product_claim_allowed", "")).lower() == "true",
    }


def update_candidate_ledger(
    *,
    watchlist_path: Path,
    ledger_path: Path,
    run_dir: Path | None = None,
    observed_at: str | None = None,
) -> dict[str, Any]:
    observed_at = observed_at or datetime.now(timezone.utc).isoformat()
    rows = _read_watchlist(watchlist_path)
    ledger = _read_ledger(ledger_path)
    candidates: dict[str, Any] = ledger["candidates"]
    effective_run_dir = run_dir or watchlist_path.parent
    run_dir_value = str(effective_run_dir)
    run_id = effective_run_dir.name
    run_metadata = _run_metadata(effective_run_dir)

    for row in rows:
        candidate_id = str(row.get("candidate_id") or stable_candidate_id(row.get("scope", ""), row.get("rule", "")))
        current = candidates.setdefault(
            candidate_id,
            {
                "candidate_id": candidate_id,
                "scope": row.get("scope", ""),
                "rule": row.get("rule", ""),
                "first_seen_at": observed_at,
                "first_run_id": run_id,
                "observations": [],
            },
        )
        observation = _observation_from_row(
            row,
            run_id=run_id,
            run_dir=run_dir_value,
            observed_at=observed_at,
            run_metadata=run_metadata,
        )
        observation["dedupe_key"] = _dedupe_key(observation)
        current["latest"] = observation
        current["readiness"] = _readiness_from_observation(observation)
        current["last_seen_at"] = observed_at
        current["last_run_id"] = run_id
        current["scope"] = row.get("scope", current.get("scope", ""))
        current["rule"] = row.get("rule", current.get("rule", ""))
        observations = list(current.get("observations") or [])
        observations = [
            item
            for item in observations
            if not _same_observation_identity(item, observation)
        ]
        observations.append(observation)
        current["observations"] = observations[-100:]
        current["aggregate"] = _aggregate_observations(current["observations"])
        current["aggregate_readiness"] = _readiness_from_observation(current["aggregate"])

    ledger["updated_at"] = observed_at
    ledger["candidate_count"] = len(candidates)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("w", encoding="utf-8") as handle:
        json.dump(ledger, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    return ledger


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-update-candidate-ledger")
    parser.add_argument("--watchlist", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=Path("var/research/candidate-watchlist-ledger.json"))
    parser.add_argument("--run-dir", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    ledger = update_candidate_ledger(
        watchlist_path=args.watchlist,
        ledger_path=args.ledger,
        run_dir=args.run_dir,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "candidate_count": ledger.get("candidate_count", 0),
                "ledger": str(args.ledger),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
