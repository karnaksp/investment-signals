"""Read strict product evidence from immutable local replay artifacts.

This outer adapter deliberately maps filesystem JSON into transport-neutral
dictionaries.  Replay statistics remain owned by the existing application use
cases; the adapter only applies the conservative portfolio aggregation needed
by the internal HTTP boundary.
"""

from __future__ import annotations

from dataclasses import fields
from enum import Enum
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.domain.hypothesis_evidence import EvidenceDecision
from tinvest_signal_engine.domain.hypothesis_formulas import HypothesisId, default_rule


_CANONICAL_TO_SHORT = {
    "h1-morning-low-volume-reversion": "H1",
    "h2-morning-high-volume-continuation": "H2",
    "h5-same-phase-return-recurrence": "H5",
    "h6-open-close-market-continuation": "H6",
    "h7-relative-volume-future-activity": "H7",
}
_CONTROLS_PER_EVENT = EvidenceGatePolicy().controls_per_event


class LocalReplayEvidenceReader:
    """Map verified replay artifacts to one conservative row per hypothesis."""

    def read_general(
        self,
        artifact_dir: str | Path,
        requested_hypotheses: Sequence[str],
        *,
        generated_at: str,
    ) -> tuple[Mapping[str, Any], ...]:
        root = Path(artifact_dir)
        completion = _read_object(root / "completion.json")
        evidence_rows = _read_array(root / "evidence.json")
        summaries = _read_array(root / "summaries.json")
        split = _read_nullable_object(root / "split.json")
        manifest = _read_object(root / "manifest.json")
        artifact_fingerprint = _sha256_value(completion.get("artifact_fingerprint"))
        summary_by_id = {
            str(row.get("hypothesis_id", "")).upper(): row for row in summaries
        }
        evidence_by_id: dict[str, list[Mapping[str, Any]]] = {}
        for row in evidence_rows:
            canonical = str(row.get("hypothesis_id", ""))
            short_id = _CANONICAL_TO_SHORT.get(canonical)
            if short_id is None:
                raise ValueError(f"unknown canonical hypothesis id in evidence: {canonical}")
            evidence_by_id.setdefault(short_id, []).append(row)

        result: list[Mapping[str, Any]] = []
        for hypothesis_id in requested_hypotheses:
            rows = evidence_by_id.get(hypothesis_id)
            if not rows:
                raise ValueError(f"immutable evidence is missing {hypothesis_id}")
            summary = summary_by_id.get(hypothesis_id)
            if summary is None:
                raise ValueError(f"immutable replay summary is missing {hypothesis_id}")
            evaluated = _integer(summary.get("evaluated_observations"))
            abstained = _integer(summary.get("abstained_observations"))
            abstention_rate = abstained / evaluated if evaluated > 0 else None
            result.append(_aggregate(
                hypothesis_id=hypothesis_id,
                rows=rows,
                independent_validation=_has_holdout(split),
                artifact_fingerprint=artifact_fingerprint,
                formula_fingerprint=_general_formula_fingerprint(hypothesis_id),
                fallback_dataset_fingerprint=_sha256_value(
                    manifest.get("dataset_fingerprint")
                ),
                fallback_cost_model_version=str(
                    _mapping(manifest.get("cost_model"), "manifest.cost_model").get(
                        "version", ""
                    )
                ),
                abstention_rates=(abstention_rate,),
                generated_at=generated_at,
            ))
        return tuple(result)

    def read_jump(
        self,
        artifact_dir: str | Path,
        requested_hypotheses: Sequence[str],
        *,
        generated_at: str,
    ) -> tuple[Mapping[str, Any], ...]:
        root = Path(artifact_dir)
        manifest = _read_object(root / "manifest.json")
        evidence_rows = _read_array(root / "evidence.json")
        observations = tuple(_read_json_lines(root / "observations.jsonl"))
        policy = _mapping(manifest.get("policy"), "manifest.policy")
        cost_model = _mapping(policy.get("cost_model"), "manifest.policy.cost_model")
        fallback_dataset = _sha256_value(manifest.get("input_fingerprint"))
        artifact_fingerprint = _directory_fingerprint(root)
        rows_by_id: dict[str, list[Mapping[str, Any]]] = {}
        abstention_by_test: dict[tuple[str, int], float | None] = {}
        for row in evidence_rows:
            hypothesis_id = str(row.get("hypothesis", "")).upper()
            if hypothesis_id not in {"H3", "H4"}:
                raise ValueError(f"unknown jump hypothesis id in evidence: {hypothesis_id}")
            bundle = dict(_mapping(row.get("bundle"), "evidence.bundle"))
            descriptive = row.get("matched_sample_summary")
            if bundle.get("mean_lift_bps") is None and isinstance(descriptive, Mapping):
                bundle["mean_lift_bps"] = descriptive.get("mean_lift_bps")
            rows_by_id.setdefault(hypothesis_id, []).append(bundle)
            horizon = _integer(row.get("horizon_seconds"))
            abstention_by_test[(hypothesis_id, horizon)] = _jump_abstention_rate(
                observations,
                hypothesis_id=hypothesis_id,
                horizon_seconds=horizon,
            )

        result: list[Mapping[str, Any]] = []
        for hypothesis_id in requested_hypotheses:
            rows = rows_by_id.get(hypothesis_id)
            if not rows:
                raise ValueError(f"immutable jump evidence is missing {hypothesis_id}")
            horizons = [
                _integer(row.get("horizon_seconds"))
                for row in evidence_rows
                if str(row.get("hypothesis", "")).upper() == hypothesis_id
            ]
            result.append(_aggregate(
                hypothesis_id=hypothesis_id,
                rows=rows,
                independent_validation=_has_holdout(
                    _mapping(manifest.get("split"), "manifest.split")
                ),
                artifact_fingerprint=artifact_fingerprint,
                formula_fingerprint=_fingerprint({
                    "hypothesis_id": hypothesis_id,
                    "policy": {
                        key: value for key, value in policy.items() if key != "cost_model"
                    },
                }),
                fallback_dataset_fingerprint=fallback_dataset,
                fallback_cost_model_version=str(cost_model.get("version", "")),
                abstention_rates=tuple(
                    abstention_by_test[(hypothesis_id, horizon)] for horizon in horizons
                ),
                generated_at=generated_at,
            ))
        return tuple(result)


def _aggregate(
    *,
    hypothesis_id: str,
    rows: Sequence[Mapping[str, Any]],
    independent_validation: bool,
    artifact_fingerprint: str,
    formula_fingerprint: str,
    fallback_dataset_fingerprint: str,
    fallback_cost_model_version: str,
    abstention_rates: Sequence[float | None],
    generated_at: str,
) -> Mapping[str, Any]:
    decisions = tuple(_decision(row.get("decision")) for row in rows)
    if EvidenceDecision.BLOCKED_BY_DATA.value in decisions:
        decision = EvidenceDecision.BLOCKED_BY_DATA.value
    elif EvidenceDecision.REJECTED.value in decisions:
        decision = EvidenceDecision.REJECTED.value
    elif all(value == EvidenceDecision.PASSED.value for value in decisions):
        decision = EvidenceDecision.PASSED.value
    else:
        decision = EvidenceDecision.INCONCLUSIVE.value

    datasets = {_sha256_value(row.get("dataset_fingerprint")) for row in rows}
    if datasets != {fallback_dataset_fingerprint}:
        raise ValueError("evidence dataset fingerprint does not match replay manifest")
    cost_versions = {
        str(row.get("cost_model_version") or fallback_cost_model_version).strip()
        for row in rows
    }
    if len(cost_versions) != 1 or not next(iter(cost_versions), ""):
        raise ValueError("evidence must have one versioned cost model")

    return {
        "hypothesis_id": hypothesis_id,
        "decision": decision,
        "independent_validation": independent_validation,
        "cost_adjusted": bool(next(iter(cost_versions))),
        "sample_count": min(_integer(row.get("eligible_events")) for row in rows),
        "trading_days": min(_integer(row.get("trading_days")) for row in rows),
        "generated_at": generated_at,
        "artifact_fingerprint": artifact_fingerprint,
        "dataset_fingerprint": fallback_dataset_fingerprint,
        "formula_fingerprint": formula_fingerprint,
        "cost_model_version": next(iter(cost_versions)),
        "primary_metric_value": _strict_extreme(
            rows, lambda row: row.get("mean_lift_bps"), min
        ),
        "matched_control_lift_ci95_lower": _strict_extreme(
            rows, lambda row: _optional_nested(row, "lift_interval", "lower"), min
        ),
        "matched_control_lift_ci95_upper": _strict_extreme(
            rows, lambda row: _optional_nested(row, "lift_interval", "upper"), min
        ),
        "matched_controls": min(_integer(row.get("matched_controls")) for row in rows),
        "controls_per_event": _CONTROLS_PER_EVENT,
        "adjusted_p_value": _strict_extreme(
            rows, lambda row: row.get("adjusted_q_value"), max
        ),
        "stable_blocks": min(
            _integer(_nested(row, "stability", "positive_blocks")) for row in rows
        ),
        "total_blocks": min(
            len(_sequence(_nested(row, "stability", "blocks"), "stability.blocks"))
            for row in rows
        ),
        "maximum_ticker_share": _strict_extreme(
            rows, lambda row: row.get("maximum_instrument_share"), max
        ),
        "maximum_period_share": _strict_period_share(rows),
        "abstention_rate": _strict_values(abstention_rates, max),
    }


def _general_formula_fingerprint(hypothesis_id: str) -> str:
    rule = default_rule(HypothesisId(hypothesis_id))
    payload: dict[str, Any] = {}
    for field in fields(rule):
        value = getattr(rule, field.name)
        if isinstance(value, Enum):
            payload[field.name] = value.value
        elif isinstance(value, tuple):
            payload[field.name] = [
                item.value if isinstance(item, Enum) else item for item in value
            ]
        else:
            payload[field.name] = value
    return _fingerprint(payload)


def _jump_abstention_rate(
    observations: Sequence[Mapping[str, Any]],
    *,
    hypothesis_id: str,
    horizon_seconds: int,
) -> float | None:
    total = 0
    unavailable = 0
    for observation in observations:
        if str(observation.get("hypothesis", "")).upper() != hypothesis_id:
            continue
        outcomes = _sequence(observation.get("outcomes"), "observation.outcomes")
        matching = [
            _mapping(outcome, "observation.outcome")
            for outcome in outcomes
            if _integer(_mapping(outcome, "observation.outcome").get("horizon_seconds"))
            == horizon_seconds
        ]
        if len(matching) != 1:
            raise ValueError("jump observation must contain exactly one requested horizon")
        total += 1
        unavailable += not bool(matching[0].get("available"))
    return unavailable / total if total else None


def _strict_period_share(rows: Sequence[Mapping[str, Any]]) -> float | None:
    shares: list[float | None] = []
    for row in rows:
        blocks = _sequence(_nested(row, "stability", "blocks"), "stability.blocks")
        if not blocks:
            shares.append(None)
            continue
        counts = [
            _integer(_mapping(block, "stability.block").get("observation_count"))
            for block in blocks
        ]
        total = sum(counts)
        shares.append(max(counts) / total if total else None)
    return _strict_values(shares, max)


def _strict_extreme(
    rows: Sequence[Mapping[str, Any]],
    accessor: Callable[[Mapping[str, Any]], object],
    operation: Callable[[Iterable[float]], float],
) -> float | None:
    return _strict_values((accessor(row) for row in rows), operation)


def _strict_values(
    values: Iterable[object],
    operation: Callable[[Iterable[float]], float],
) -> float | None:
    materialized = tuple(values)
    if not materialized or any(value is None for value in materialized):
        return None
    return operation(float(value) for value in materialized)


def _decision(value: object) -> str:
    normalized = str(value)
    allowed = {item.value for item in EvidenceDecision}
    if normalized not in allowed:
        raise ValueError(f"unknown evidence decision: {normalized}")
    return normalized


def _has_holdout(split: Mapping[str, Any] | None) -> bool:
    if split is None:
        return False
    holdout = split.get("holdout_days")
    return isinstance(holdout, list) and bool(holdout)


def _nested(row: Mapping[str, Any], parent: str, child: str) -> object:
    return _mapping(row.get(parent), parent).get(child)


def _optional_nested(row: Mapping[str, Any], parent: str, child: str) -> object:
    value = row.get(parent)
    return None if value is None else _mapping(value, parent).get(child)


def _integer(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer statistic")
    result = int(value)  # type: ignore[arg-type]
    if result < 0:
        raise ValueError("evidence counts must not be negative")
    return result


def _sha256_value(value: object) -> str:
    normalized = str(value)
    if len(normalized) != 71 or not normalized.startswith("sha256:"):
        raise ValueError("sha256 fingerprint is required")
    try:
        int(normalized.removeprefix("sha256:"), 16)
    except ValueError as exc:
        raise ValueError("sha256 fingerprint must contain lowercase hex") from exc
    if normalized != normalized.lower():
        raise ValueError("sha256 fingerprint must contain lowercase hex")
    return normalized


def _directory_fingerprint(root: Path) -> str:
    files = tuple(sorted(path for path in root.iterdir() if path.is_file()))
    if not files:
        raise ValueError("replay artifact directory is empty")
    digest = sha256()
    for path in files:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def _fingerprint(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return f"sha256:{sha256(encoded).hexdigest()}"


def _read_object(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return _mapping(value, str(path))


def _read_nullable_object(path: Path) -> Mapping[str, Any] | None:
    value = json.loads(path.read_text(encoding="utf-8"))
    return None if value is None else _mapping(value, str(path))


def _read_array(path: Path) -> tuple[Mapping[str, Any], ...]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return tuple(_mapping(item, str(path)) for item in _sequence(value, str(path)))


def _read_json_lines(path: Path) -> Iterable[Mapping[str, Any]]:
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            yield _mapping(json.loads(line), str(path))


def _mapping(value: object, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{location} must be an object")
    return value


def _sequence(value: object, location: str) -> tuple[Any, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{location} must be an array")
    return tuple(value)
