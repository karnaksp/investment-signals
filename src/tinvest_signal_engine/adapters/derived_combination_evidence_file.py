"""Strict bounded reader for completed C1-C5 evidence artifacts."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from tinvest_signal_engine.application.derived_combination_evidence import (
    CombinationConfidenceIntervalSnapshot,
    CombinationDiagnosticsSnapshot,
    CombinationEvidenceArtifactSnapshot,
    CombinationHorizonArtifactSnapshot,
    CombinationReasonCountSnapshot,
)
from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
    preregistered_combination_definition,
)


_COMBINATION_SCHEMA = "scientific-combination-stream-v1"
_MAX_COMPLETION_BYTES = 256 * 1024
_MAX_RESULTS_BYTES = 8 * 1024 * 1024
_SHA256_LENGTH = 71


class FileDerivedScientificCombinationEvidenceReader:
    """Verify the immutable result hash, then discard raw control assignments."""

    def read(
        self,
        artifact_uri: str,
        *,
        expected_artifact_fingerprint: str,
    ) -> CombinationEvidenceArtifactSnapshot:
        artifact_dir = Path(artifact_uri)
        completion = _read_json_object(
            artifact_dir / "completion.json",
            maximum_bytes=_MAX_COMPLETION_BYTES,
        )
        _exact_keys(
            completion,
            {
                "schema",
                "run_id",
                "artifact_fingerprint",
                "cost_model_version",
                "hashes",
                "partition_count",
                "observation_count",
                "result_count",
            },
            "completion",
        )
        if completion["schema"] != _COMBINATION_SCHEMA:
            raise ValueError("unsupported combination completion schema")
        artifact_fingerprint = _sha256_value(
            completion["artifact_fingerprint"],
            "artifact_fingerprint",
        )
        if artifact_fingerprint != _sha256_value(
            expected_artifact_fingerprint,
            "expected_artifact_fingerprint",
        ):
            raise ValueError("combination artifact fingerprint does not match replay")
        hashes = _string_mapping(completion["hashes"], "completion.hashes")
        if _fingerprint(hashes) != artifact_fingerprint:
            raise ValueError("combination completion fingerprint drifted")
        expected_results_hash = _sha256_value(
            hashes.get("results.json"),
            "completion.hashes.results.json",
        )
        results_path = artifact_dir / "results.json"
        results_bytes = _read_bounded(results_path, _MAX_RESULTS_BYTES)
        if _bytes_hash(results_bytes) != expected_results_hash:
            raise ValueError("combination results failed hash verification")
        try:
            payload = json.loads(results_bytes)
        except json.JSONDecodeError as exc:
            raise ValueError("combination results are not valid JSON") from exc
        if not isinstance(payload, list):
            raise ValueError("combination results must be a JSON array")
        if len(payload) != _non_negative_int(
            completion["result_count"], "completion.result_count"
        ):
            raise ValueError("combination result count drifted")
        completion_cost_model = _non_empty_string(
            completion["cost_model_version"],
            "completion.cost_model_version",
        )
        horizons = tuple(
            _parse_result(row, completion_cost_model=completion_cost_model)
            for row in payload
        )
        _verify_complete_portfolio(horizons)
        dataset_fingerprints = {row.dataset_fingerprint for row in horizons}
        cost_model_versions = {row.cost_model_version for row in horizons}
        if len(dataset_fingerprints) != 1:
            raise ValueError("combination dataset fingerprint drifted across horizons")
        if cost_model_versions != {completion_cost_model}:
            raise ValueError("combination cost model drifted across horizons")
        return CombinationEvidenceArtifactSnapshot(
            artifact_fingerprint=artifact_fingerprint,
            dataset_fingerprint=next(iter(dataset_fingerprints)),
            cost_model_version=completion_cost_model,
            horizons=tuple(
                sorted(
                    horizons,
                    key=lambda row: (
                        row.combination_id.value,
                        row.horizon_seconds,
                    ),
                )
            ),
        )


def _parse_result(
    raw: Any,
    *,
    completion_cost_model: str,
) -> CombinationHorizonArtifactSnapshot:
    row = _mapping(raw, "combination result")
    _exact_keys(
        row,
        {
            "combination_id",
            "combination_version",
            "horizon_seconds",
            "statistical_state",
            "comparison_hypotheses",
            "abstain_policy_version",
            "coverage",
            "control_matches",
            "evidence",
        },
        "combination result",
    )
    combination_id = ScientificCombinationId(
        _non_empty_string(row["combination_id"], "combination_id")
    )
    definition = preregistered_combination_definition(combination_id)
    combination_version = _non_empty_string(
        row["combination_version"], "combination_version"
    )
    if combination_version != definition.version:
        raise ValueError("combination version drifted")
    horizon_seconds = _positive_int(row["horizon_seconds"], "horizon_seconds")
    if horizon_seconds not in definition.horizons_seconds:
        raise ValueError("combination result has unsupported horizon")
    comparison_hypotheses = _string_tuple(
        row["comparison_hypotheses"], "comparison_hypotheses"
    )
    expected_comparisons = tuple(
        item.value for item in definition.comparison_hypothesis_ids
    )
    if comparison_hypotheses != expected_comparisons:
        raise ValueError("combination comparison basis drifted")
    _non_empty_string(row["abstain_policy_version"], "abstain_policy_version")
    control_matches = row["control_matches"]
    if not isinstance(control_matches, list):
        raise ValueError("control_matches must be an array")
    for raw_match in control_matches:
        match = _mapping(raw_match, "control match")
        _exact_keys(
            match,
            {
                "event_observation_id",
                "standalone_observation_ids",
                "event_net_bps",
                "standalone_mean_net_bps",
                "incremental_lift_bps",
            },
            "control match",
        )
        _sha256_value(match["event_observation_id"], "event_observation_id")
        _string_tuple(match["standalone_observation_ids"], "standalone_observation_ids")
        _number(match["event_net_bps"], "event_net_bps")
        _number(match["standalone_mean_net_bps"], "standalone_mean_net_bps")
        _number(match["incremental_lift_bps"], "incremental_lift_bps")
    coverage = _parse_coverage(row["coverage"])
    evidence = _parse_evidence(
        row["evidence"],
        completion_cost_model=completion_cost_model,
    )
    if evidence["hypothesis_id"] != f"{combination_id.value}:{horizon_seconds}":
        raise ValueError("combination evidence identity drifted")
    if evidence["hypothesis_version"] != combination_version:
        raise ValueError("combination evidence version drifted")
    expected_statistical_state = {
        "passed": "passed",
        "rejected": "rejected",
        "inconclusive": "uncertain",
        "blocked_by_data": "blocked-data",
    }[evidence["decision"]]
    if row["statistical_state"] != expected_statistical_state:
        raise ValueError("combination statistical state drifted")
    if len(control_matches) != evidence["matched_events"]:
        raise ValueError("raw control assignments do not match evidence count")
    return CombinationHorizonArtifactSnapshot(
        combination_id=combination_id,
        combination_version=combination_version,
        horizon_seconds=horizon_seconds,
        dataset_fingerprint=evidence["dataset_fingerprint"],
        decision=evidence["decision"],
        reason_codes=evidence["reason_codes"],
        total_observations=coverage["total_observations"],
        abstained_observations=coverage["abstained_observations"],
        eligible_events=coverage["eligible_events"],
        matched_events=evidence["matched_events"],
        matched_controls=evidence["matched_controls"],
        trading_days=evidence["trading_days"],
        cost_model_version=evidence["cost_model_version"],
        mean_lift_bps=evidence["mean_lift_bps"],
        lift_interval=evidence["lift_interval"],
        adjusted_q_value=evidence["adjusted_q_value"],
        positive_stability_blocks=evidence["positive_stability_blocks"],
        total_stability_blocks=evidence["total_stability_blocks"],
        maximum_instrument_share=evidence["maximum_instrument_share"],
        diagnostics=evidence["diagnostics"],
    )


def _parse_coverage(raw: Any) -> dict[str, Any]:
    coverage = _mapping(raw, "coverage")
    _exact_keys(
        coverage,
        {
            "total_observations",
            "matched_observations",
            "not_matched_observations",
            "abstained_observations",
            "available_outcomes",
            "eligible_events",
            "matched_events",
            "standalone_candidates",
            "reasons_histogram",
        },
        "coverage",
    )
    result = {
        key: _non_negative_int(coverage[key], f"coverage.{key}")
        for key in (
            "total_observations",
            "matched_observations",
            "not_matched_observations",
            "abstained_observations",
            "available_outcomes",
            "eligible_events",
            "matched_events",
            "standalone_candidates",
        )
    }
    _reason_counts(coverage["reasons_histogram"], "coverage.reasons_histogram")
    if (
        result["matched_observations"]
        + result["not_matched_observations"]
        + result["abstained_observations"]
        != result["total_observations"]
    ):
        raise ValueError("combination coverage classification drifted")
    return result


def _parse_evidence(
    raw: Any,
    *,
    completion_cost_model: str,
) -> dict[str, Any]:
    evidence = _mapping(raw, "evidence")
    _exact_keys(
        evidence,
        {
            "evidence_id",
            "hypothesis_id",
            "hypothesis_version",
            "dataset_fingerprint",
            "decision",
            "reason_codes",
            "trading_days",
            "eligible_events",
            "matched_events",
            "matched_controls",
            "cost_model_version",
            "event_mean_net_bps",
            "control_mean_net_bps",
            "mean_lift_bps",
            "lift_interval",
            "positive_rate_interval",
            "raw_p_value",
            "adjusted_q_value",
            "fdr_significant",
            "stability",
            "instrument_concentration",
            "maximum_instrument_share",
            "diagnostics_v2",
        },
        "evidence",
    )
    decision = _non_empty_string(evidence["decision"], "evidence.decision")
    if decision not in {"passed", "rejected", "inconclusive", "blocked_by_data"}:
        raise ValueError("unsupported combination evidence decision")
    raw_cost_model_version = evidence["cost_model_version"]
    cost_model_version = (
        completion_cost_model
        if raw_cost_model_version is None
        else _non_empty_string(
            raw_cost_model_version,
            "evidence.cost_model_version",
        )
    )
    if cost_model_version != completion_cost_model:
        raise ValueError("evidence cost model differs from completion")
    stability = _mapping(evidence["stability"], "evidence.stability")
    _exact_keys(
        stability,
        {
            "blocks",
            "required_positive_blocks",
            "positive_blocks",
            "assessed",
            "stable",
        },
        "evidence.stability",
    )
    blocks = stability["blocks"]
    if not isinstance(blocks, list):
        raise ValueError("evidence stability blocks must be an array")
    for block in blocks:
        value = _mapping(block, "evidence stability block")
        _exact_keys(
            value,
            {
                "block_number",
                "trading_days",
                "observation_count",
                "mean_lift_bps",
                "positive",
            },
            "evidence stability block",
        )
        _positive_int(value["block_number"], "stability.block_number")
        _string_tuple(value["trading_days"], "stability.trading_days")
        _non_negative_int(value["observation_count"], "stability.observation_count")
        _number(value["mean_lift_bps"], "stability.mean_lift_bps")
        _boolean(value["positive"], "stability.positive")
    _positive_int(
        stability["required_positive_blocks"],
        "stability.required_positive_blocks",
    )
    _non_negative_int(stability["positive_blocks"], "stability.positive_blocks")
    _boolean(stability["assessed"], "stability.assessed")
    _boolean(stability["stable"], "stability.stable")
    concentration = evidence["instrument_concentration"]
    if not isinstance(concentration, list):
        raise ValueError("instrument_concentration must be an array")
    for item in concentration:
        value = _mapping(item, "instrument concentration")
        _exact_keys(
            value,
            {"instrument_id", "event_count", "share"},
            "instrument concentration",
        )
        _non_empty_string(value["instrument_id"], "concentration.instrument_id")
        _non_negative_int(value["event_count"], "concentration.event_count")
        _bounded_ratio(value["share"], "concentration.share")
    eligible_events = _non_negative_int(
        evidence["eligible_events"], "evidence.eligible_events"
    )
    matched_events = _non_negative_int(
        evidence["matched_events"], "evidence.matched_events"
    )
    if matched_events > eligible_events:
        raise ValueError("matched evidence events exceed eligible events")
    _sha256_value(evidence["evidence_id"], "evidence.evidence_id")
    _optional_number(evidence["event_mean_net_bps"], "evidence.event_mean_net_bps")
    _optional_number(evidence["control_mean_net_bps"], "evidence.control_mean_net_bps")
    _optional_interval(
        evidence["positive_rate_interval"], "evidence.positive_rate_interval"
    )
    _optional_probability(evidence["raw_p_value"], "evidence.raw_p_value")
    adjusted_q_value = _optional_probability(
        evidence["adjusted_q_value"], "evidence.adjusted_q_value"
    )
    _boolean(evidence["fdr_significant"], "evidence.fdr_significant")
    maximum_instrument_share = _optional_ratio(
        evidence["maximum_instrument_share"],
        "evidence.maximum_instrument_share",
    )
    return {
        "hypothesis_id": _non_empty_string(
            evidence["hypothesis_id"], "evidence.hypothesis_id"
        ),
        "hypothesis_version": _non_empty_string(
            evidence["hypothesis_version"], "evidence.hypothesis_version"
        ),
        "dataset_fingerprint": _sha256_value(
            evidence["dataset_fingerprint"], "evidence.dataset_fingerprint"
        ),
        "decision": decision,
        "reason_codes": _string_tuple(
            evidence["reason_codes"], "evidence.reason_codes"
        ),
        "trading_days": _non_negative_int(
            evidence["trading_days"], "evidence.trading_days"
        ),
        "matched_events": matched_events,
        "matched_controls": _non_negative_int(
            evidence["matched_controls"], "evidence.matched_controls"
        ),
        "cost_model_version": cost_model_version,
        "mean_lift_bps": _optional_number(
            evidence["mean_lift_bps"], "evidence.mean_lift_bps"
        ),
        "lift_interval": _optional_interval(
            evidence["lift_interval"], "evidence.lift_interval"
        ),
        "adjusted_q_value": adjusted_q_value,
        "positive_stability_blocks": _non_negative_int(
            stability["positive_blocks"], "evidence.stability.positive_blocks"
        ),
        "total_stability_blocks": len(blocks),
        "maximum_instrument_share": maximum_instrument_share,
        "diagnostics": _optional_diagnostics(evidence["diagnostics_v2"]),
    }


def _optional_diagnostics(raw: Any) -> CombinationDiagnosticsSnapshot | None:
    if raw is None:
        return None
    diagnostics = _mapping(raw, "evidence.diagnostics_v2")
    _exact_keys(
        diagnostics,
        {
            "version",
            "event_prevalence",
            "eligible_event_count",
            "matched_event_count",
            "match_coverage",
            "data_coverage",
            "reasons_histogram",
            "primary_effect_estimate",
            "primary_effect_interval",
            "primary_p_value",
            "descriptive_only",
        },
        "evidence.diagnostics_v2",
    )
    return CombinationDiagnosticsSnapshot(
        version=_diagnostics_version(diagnostics["version"]),
        event_prevalence=_optional_ratio(
            diagnostics["event_prevalence"], "diagnostics.event_prevalence"
        ),
        eligible_event_count=_non_negative_int(
            diagnostics["eligible_event_count"], "diagnostics.eligible_event_count"
        ),
        matched_event_count=_non_negative_int(
            diagnostics["matched_event_count"], "diagnostics.matched_event_count"
        ),
        match_coverage=_optional_ratio(
            diagnostics["match_coverage"], "diagnostics.match_coverage"
        ),
        data_coverage=_optional_ratio(
            diagnostics["data_coverage"], "diagnostics.data_coverage"
        ),
        reasons_histogram=_reason_counts(
            diagnostics["reasons_histogram"], "diagnostics.reasons_histogram"
        ),
        primary_effect_estimate=_optional_number(
            diagnostics["primary_effect_estimate"],
            "diagnostics.primary_effect_estimate",
        ),
        primary_effect_interval=_optional_interval(
            diagnostics["primary_effect_interval"],
            "diagnostics.primary_effect_interval",
        ),
        primary_p_value=_optional_probability(
            diagnostics["primary_p_value"], "diagnostics.primary_p_value"
        ),
        descriptive_only=_boolean(
            diagnostics["descriptive_only"], "diagnostics.descriptive_only"
        ),
    )


def _verify_complete_portfolio(
    rows: tuple[CombinationHorizonArtifactSnapshot, ...],
) -> None:
    actual = tuple(
        sorted((row.combination_id.value, row.horizon_seconds) for row in rows)
    )
    expected = tuple(
        sorted(
            (combination_id.value, horizon)
            for combination_id in ScientificCombinationId
            for horizon in preregistered_combination_definition(
                combination_id
            ).horizons_seconds
        )
    )
    if actual != expected:
        raise ValueError("combination artifact must contain each C1-C5 horizon once")


def _optional_interval(
    raw: Any,
    name: str,
) -> CombinationConfidenceIntervalSnapshot | None:
    if raw is None:
        return None
    interval = _mapping(raw, name)
    _exact_keys(
        interval,
        {"lower", "estimate", "upper", "confidence_level"},
        name,
    )
    result = CombinationConfidenceIntervalSnapshot(
        lower=_number(interval["lower"], f"{name}.lower"),
        estimate=_number(interval["estimate"], f"{name}.estimate"),
        upper=_number(interval["upper"], f"{name}.upper"),
        confidence_level=_number(
            interval["confidence_level"], f"{name}.confidence_level"
        ),
    )
    if not result.lower <= result.estimate <= result.upper:
        raise ValueError(f"{name} does not contain its estimate")
    if not 0.0 < result.confidence_level < 1.0:
        raise ValueError(f"{name}.confidence_level must be between zero and one")
    return result


def _reason_counts(raw: Any, name: str) -> tuple[CombinationReasonCountSnapshot, ...]:
    if not isinstance(raw, list):
        raise ValueError(f"{name} must be an array")
    result: list[CombinationReasonCountSnapshot] = []
    for item in raw:
        value = _mapping(item, name)
        _exact_keys(value, {"reason_code", "count"}, name)
        result.append(
            CombinationReasonCountSnapshot(
                reason_code=_non_empty_string(value["reason_code"], f"{name}.code"),
                count=_positive_int(value["count"], f"{name}.count"),
            )
        )
    return tuple(result)


def _read_json_object(path: Path, *, maximum_bytes: int) -> Mapping[str, Any]:
    raw = _read_bounded(path, maximum_bytes)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path.name} is not valid JSON") from exc
    return _mapping(payload, path.name)


def _read_bounded(path: Path, maximum_bytes: int) -> bytes:
    size = path.stat().st_size
    if size <= 0 or size > maximum_bytes:
        raise ValueError(f"{path.name} exceeds the bounded artifact contract")
    data = path.read_bytes()
    if len(data) != size:
        raise ValueError(f"{path.name} changed while it was read")
    return data


def _mapping(raw: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(raw, dict) or not all(isinstance(key, str) for key in raw):
        raise ValueError(f"{name} must be an object")
    return raw


def _string_mapping(raw: Any, name: str) -> dict[str, str]:
    value = _mapping(raw, name)
    if not value or not all(isinstance(item, str) for item in value.values()):
        raise ValueError(f"{name} must contain strings")
    return dict(value)


def _exact_keys(raw: Mapping[str, Any], expected: set[str], name: str) -> None:
    if set(raw) != expected:
        raise ValueError(f"{name} has an unsupported schema")


def _sha256_value(raw: Any, name: str) -> str:
    value = _non_empty_string(raw, name)
    if (
        len(value) != _SHA256_LENGTH
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{name} must be a sha256 fingerprint")
    return value


def _non_empty_string(raw: Any, name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return raw


def _string_tuple(raw: Any, name: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not all(
        isinstance(item, str) and item for item in raw
    ):
        raise ValueError(f"{name} must be an array of strings")
    return tuple(raw)


def _positive_int(raw: Any, name: str) -> int:
    value = _non_negative_int(raw, name)
    if value == 0:
        raise ValueError(f"{name} must be positive")
    return value


def _non_negative_int(raw: Any, name: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return raw


def _number(raw: Any, name: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{name} must be numeric")
    return float(raw)


def _optional_number(raw: Any, name: str) -> float | None:
    return None if raw is None else _number(raw, name)


def _bounded_ratio(raw: Any, name: str) -> float:
    value = _number(raw, name)
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be between zero and one")
    return value


def _optional_ratio(raw: Any, name: str) -> float | None:
    return None if raw is None else _bounded_ratio(raw, name)


def _optional_probability(raw: Any, name: str) -> float | None:
    return _optional_ratio(raw, name)


def _boolean(raw: Any, name: str) -> bool:
    if not isinstance(raw, bool):
        raise ValueError(f"{name} must be boolean")
    return raw


def _diagnostics_version(raw: Any) -> str:
    value = _non_empty_string(raw, "diagnostics.version")
    if value != "evidence-diagnostics-v2":
        raise ValueError("unsupported evidence diagnostics version")
    return value


def _bytes_hash(data: bytes) -> str:
    return "sha256:" + sha256(data).hexdigest()


def _fingerprint(value: Mapping[str, str]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return _bytes_hash(encoded)
