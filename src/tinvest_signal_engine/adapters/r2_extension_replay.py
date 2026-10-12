"""Immutable fail-closed replay artifacts for the causal H10/H11 R2 engine."""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any

from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    R2ExtensionReport,
)
from tinvest_signal_engine.application.r2_extension_evidence import (
    R2_EVIDENCE_POLICY,
    AssessR2ExtensionEvidence,
    R2EvidenceCoverage,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    EvidenceBundle,
    EvidenceDecision,
    EvidenceDiagnosticsV2,
)
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2Feature,
    R2Outcome,
)
from tinvest_signal_engine.domain.scientific_replay_contract import (
    scientific_replay_definition,
)

R2_EXTENSION_EVIDENCE_SCHEMA_VERSION = 3


@dataclass(frozen=True, slots=True)
class R2ExtensionReplayArtifact:
    artifact_uri: str
    artifact_fingerprint: str
    evidence: tuple[Mapping[str, Any], ...]


class R2ExtensionReplayArtifactAdapter:
    """Persist causal observations and independently assess H10's holdout."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def save(
        self,
        report: R2ExtensionReport,
        requested_hypotheses: Sequence[R2ExtensionHypothesis],
        *,
        cost_model_version: str,
        blocking_reason_codes: Sequence[str] = (
            "independent_evidence_gate_unavailable",
        ),
    ) -> R2ExtensionReplayArtifact:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        reasons = tuple(sorted(set(blocking_reason_codes)))
        if not selected:
            raise ValueError("at least one R2 hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost model version must not be empty")
        if any(not item.strip() for item in reasons):
            raise ValueError("R2 replay must name its fail-closed reason")
        h10_assessment = None
        h10_days = {
            item.trading_day
            for item in report.features
            if item.hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION
        }
        if (
            R2ExtensionHypothesis.OPENING_GAP_REVERSION in selected
            and len(h10_days) >= 5
        ):
            h10_assessment = AssessR2ExtensionEvidence().execute(
                report,
                (R2ExtensionHypothesis.OPENING_GAP_REVERSION,),
                cost_model_version=cost_model_version,
            )
        artifact_fingerprint = _fingerprint(
            {
                "schema_version": R2_EXTENSION_EVIDENCE_SCHEMA_VERSION,
                "blocking_reason_codes": reasons,
                "cost_model_version": cost_model_version,
                "evidence_policy": asdict(R2_EVIDENCE_POLICY),
                "report_fingerprint": report.report_fingerprint,
                "selected_hypotheses": tuple(item.value for item in selected),
            }
        )
        evidence = tuple(
            _assessed_evidence(
                report,
                hypothesis,
                bundle=h10_assessment.for_hypothesis(hypothesis),
                coverage=h10_assessment.coverage_for(hypothesis),
                artifact_fingerprint=artifact_fingerprint,
                cost_model_version=cost_model_version,
            )
            if (
                hypothesis is R2ExtensionHypothesis.OPENING_GAP_REVERSION
                and h10_assessment is not None
            )
            else _blocked_evidence(
                report,
                hypothesis,
                artifact_fingerprint=artifact_fingerprint,
                cost_model_version=cost_model_version,
                blocking_reason_codes=(
                    reasons
                    if reasons
                    else ("minimum_history_for_holdout_split_not_met",)
                ),
            )
            for hypothesis in selected
        )
        run_dir = self._root / artifact_fingerprint.removeprefix("sha256:")
        manifest = {
            "schema_version": R2_EXTENSION_EVIDENCE_SCHEMA_VERSION,
            "kind": "causal_h10_h11_r2_replay",
            "portfolio_version": report.portfolio_version,
            "dataset_fingerprint": report.dataset_fingerprint,
            "request_fingerprint": report.request_fingerprint,
            "report_fingerprint": report.report_fingerprint,
            "artifact_fingerprint": artifact_fingerprint,
            "cost_model_version": cost_model_version,
            "evidence_policy": asdict(R2_EVIDENCE_POLICY),
            "blocking_reason_codes": reasons,
            "selected_hypotheses": tuple(item.value for item in selected),
            "feature_count": len(report.features),
            "outcome_count": len(report.outcomes),
            "feature_set_fingerprint": _sequence_fingerprint(
                item.fingerprint for item in report.features
            ),
            "outcome_set_fingerprint": _sequence_fingerprint(
                item.fingerprint for item in report.outcomes
            ),
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        _write_once_or_verify(run_dir / "evidence.json", _json_bytes(evidence))
        return R2ExtensionReplayArtifact(
            artifact_uri=str(run_dir.resolve()),
            artifact_fingerprint=artifact_fingerprint,
            evidence=evidence,
        )


def _assessed_evidence(
    report: R2ExtensionReport,
    hypothesis: R2ExtensionHypothesis,
    *,
    bundle: EvidenceBundle,
    coverage: R2EvidenceCoverage,
    artifact_fingerprint: str,
    cost_model_version: str,
) -> Mapping[str, Any]:
    definition = scientific_replay_definition(hypothesis.value)
    features = tuple(item for item in report.features if item.hypothesis is hypothesis)
    outcomes = tuple(
        outcome
        for feature, outcome in zip(report.features, report.outcomes, strict=True)
        if feature.hypothesis is hypothesis
    )
    expected_horizons = _report_horizons(report, hypothesis)
    diagnostics = bundle.diagnostics_v2
    descriptive_interval = (
        diagnostics.primary_effect_interval if diagnostics is not None else None
    )
    interval = bundle.lift_interval or descriptive_interval
    primary = (
        bundle.mean_lift_bps
        if bundle.mean_lift_bps is not None
        else (diagnostics.primary_effect_estimate if diagnostics is not None else None)
    )
    total_block_observations = sum(
        item.observation_count for item in bundle.stability.blocks
    )
    maximum_period_share = (
        max(
            item.observation_count / total_block_observations
            for item in bundle.stability.blocks
        )
        if total_block_observations
        else None
    )
    horizons = tuple(
        _horizon_summary(
            features,
            outcomes,
            horizon=horizon,
            primary_horizon=coverage.primary_horizon_seconds,
            decision=bundle.decision.value,
            source_data_state=(
                "insufficient_history"
                if bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
                else "ready"
            ),
            primary_metric_value=primary,
            primary_sample_count=bundle.matched_events,
        )
        for horizon in expected_horizons
    )
    return {
        "hypothesis_id": hypothesis.value,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": definition.market_phase,
        "source_data_state": (
            "insufficient_history"
            if bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
            else "ready"
        ),
        "decision": bundle.decision.value,
        "reason_codes": bundle.reason_codes,
        "independent_validation": True,
        "cost_adjusted": True,
        "sample_count": bundle.matched_events,
        "trading_days": bundle.trading_days,
        "generated_at": _generated_at(features, outcomes),
        "artifact_fingerprint": artifact_fingerprint,
        "dataset_fingerprint": report.dataset_fingerprint,
        "formula_fingerprint": _fingerprint(
            {
                "hypothesis_id": hypothesis.value,
                "hypothesis_version": hypothesis.version,
                "portfolio_version": report.portfolio_version,
                "request_fingerprint": report.request_fingerprint,
            }
        ),
        "cost_model_version": cost_model_version,
        "primary_metric_value": primary,
        "matched_control_lift_ci95_lower": interval.lower if interval else None,
        "matched_control_lift_ci95_upper": interval.upper if interval else None,
        "matched_controls": bundle.matched_controls,
        "controls_per_event": R2_EVIDENCE_POLICY.controls_per_event,
        "adjusted_p_value": bundle.adjusted_q_value,
        "stable_blocks": bundle.stability.positive_blocks,
        "total_blocks": len(bundle.stability.blocks),
        "maximum_ticker_share": bundle.maximum_instrument_share,
        "maximum_period_share": maximum_period_share,
        "abstention_rate": (
            1.0 - coverage.triggered_events / coverage.holdout_observations
            if coverage.holdout_observations
            else None
        ),
        "diagnostics_v2": _diagnostics_payload(diagnostics),
        "horizons": horizons,
    }


def _horizon_summary(
    features: Sequence[R2Feature],
    outcomes: Sequence[R2Outcome],
    *,
    horizon: int,
    primary_horizon: int,
    decision: str,
    source_data_state: str,
    primary_metric_value: float | None,
    primary_sample_count: int,
) -> Mapping[str, Any]:
    pairs = tuple(
        (feature, outcome)
        for feature, outcome in zip(features, outcomes, strict=True)
        if feature.horizon_seconds == horizon
    )
    matched = tuple(
        outcome.cost_adjusted_signed_return_bps
        for feature, outcome in pairs
        if feature.decision is R2Decision.MATCHED
        and outcome.available
        and outcome.cost_adjusted_signed_return_bps is not None
    )
    is_primary = horizon == primary_horizon
    return {
        "horizon_seconds": horizon,
        "evidence_scope": (
            "independent_gate" if is_primary else "secondary_descriptive"
        ),
        "source_data_state": source_data_state,
        "decision": decision if is_primary else "inconclusive",
        "sample_count": primary_sample_count if is_primary else len(matched),
        "primary_metric_value": (
            primary_metric_value
            if is_primary
            else (sum(matched) / len(matched) if matched else None)
        ),
    }


def _diagnostics_payload(
    diagnostics: EvidenceDiagnosticsV2 | None,
) -> Mapping[str, Any] | None:
    if diagnostics is None:
        return None
    interval = diagnostics.primary_effect_interval
    return {
        "version": diagnostics.version,
        "event_prevalence": diagnostics.event_prevalence,
        "eligible_event_count": diagnostics.eligible_event_count,
        "matched_event_count": diagnostics.matched_event_count,
        "match_coverage": diagnostics.match_coverage,
        "data_coverage": diagnostics.data_coverage,
        "reasons_histogram": tuple(
            {"reason_code": item.reason_code, "count": item.count}
            for item in diagnostics.reasons_histogram
        ),
        "primary_effect_estimate": diagnostics.primary_effect_estimate,
        "primary_effect_interval": (
            {
                "lower": interval.lower,
                "estimate": interval.estimate,
                "upper": interval.upper,
                "confidence_level": interval.confidence_level,
            }
            if interval is not None
            else None
        ),
        "primary_p_value": diagnostics.primary_p_value,
        "descriptive_only": diagnostics.descriptive_only,
    }


def _blocked_evidence(
    report: R2ExtensionReport,
    hypothesis: R2ExtensionHypothesis,
    *,
    artifact_fingerprint: str,
    cost_model_version: str,
    blocking_reason_codes: tuple[str, ...],
) -> Mapping[str, Any]:
    definition = scientific_replay_definition(hypothesis.value)
    features = tuple(item for item in report.features if item.hypothesis is hypothesis)
    outcomes = tuple(
        outcome
        for feature, outcome in zip(report.features, report.outcomes, strict=True)
        if feature.hypothesis is hypothesis
    )
    reason_counts = Counter(item.reason.value for item in features)
    reason_counts.update({item: 1 for item in blocking_reason_codes})
    matched = sum(item.decision is R2Decision.MATCHED for item in features)
    available_outcomes = sum(item.available for item in outcomes)
    horizons = tuple(sorted({item.horizon_seconds for item in features}))
    expected_horizons = _report_horizons(report, hypothesis)
    if horizons != expected_horizons:
        raise ValueError("R2 report does not contain every registered horizon")
    total = len(features)
    generated_at = _generated_at(features, outcomes)
    diagnostics = {
        "version": "evidence-diagnostics-v2",
        "event_prevalence": matched / total if total else None,
        "eligible_event_count": matched,
        "matched_event_count": 0,
        "match_coverage": 0.0 if matched else None,
        "data_coverage": available_outcomes / matched if matched else None,
        "reasons_histogram": tuple(
            {"reason_code": reason, "count": count}
            for reason, count in sorted(reason_counts.items())
            if count > 0
        ),
        "primary_effect_estimate": None,
        "primary_effect_interval": None,
        "primary_p_value": None,
        "descriptive_only": True,
    }
    return {
        "hypothesis_id": hypothesis.value,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": definition.market_phase,
        "source_data_state": "unavailable",
        "decision": "blocked_by_data",
        "reason_codes": blocking_reason_codes,
        "independent_validation": False,
        "cost_adjusted": True,
        "sample_count": 0,
        "trading_days": 0,
        "generated_at": generated_at,
        "artifact_fingerprint": artifact_fingerprint,
        "dataset_fingerprint": report.dataset_fingerprint,
        "formula_fingerprint": _fingerprint(
            {
                "hypothesis_id": hypothesis.value,
                "hypothesis_version": hypothesis.version,
                "portfolio_version": report.portfolio_version,
                "request_fingerprint": report.request_fingerprint,
            }
        ),
        "cost_model_version": cost_model_version,
        "primary_metric_value": None,
        "matched_control_lift_ci95_lower": None,
        "matched_control_lift_ci95_upper": None,
        "matched_controls": 0,
        "controls_per_event": 5,
        "adjusted_p_value": None,
        "stable_blocks": 0,
        "total_blocks": 0,
        "maximum_ticker_share": None,
        "maximum_period_share": None,
        "abstention_rate": (1.0 - matched / total if total else None),
        "diagnostics_v2": diagnostics,
        "horizons": tuple(
            {
                "horizon_seconds": horizon,
                "evidence_scope": "not_evaluated",
                "source_data_state": "unavailable",
                "decision": "blocked_by_data",
                "sample_count": 0,
                "primary_metric_value": None,
            }
            for horizon in expected_horizons
        ),
    }


def _report_horizons(
    report: R2ExtensionReport,
    hypothesis: R2ExtensionHypothesis,
) -> tuple[int, ...]:
    policy_horizons = scientific_replay_definition(hypothesis.value).horizons_seconds
    actual = tuple(
        sorted(
            {
                item.horizon_seconds
                for item in report.features
                if item.hypothesis is hypothesis
            }
        )
    )
    if actual and actual != policy_horizons:
        raise ValueError("R2 policy horizons differ from replay contract")
    return policy_horizons


def _sequence_fingerprint(values: Iterable[str]) -> str:
    digest = sha256()
    for value in values:
        digest.update(str(value).encode("utf-8"))
        digest.update(b"\n")
    return "sha256:" + digest.hexdigest()


def _generated_at(
    features: Sequence[R2Feature],
    outcomes: Sequence[R2Outcome],
) -> str:
    values = [item.available_at for item in outcomes]
    values.extend(item.available_at for item in features)
    return (
        max(values).astimezone(UTC).isoformat()
        if values
        else datetime(1970, 1, 1, tzinfo=UTC).isoformat()
    )


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(
        _json_value(payload), sort_keys=True, separators=(",", ":")
    ).encode()
    return "sha256:" + sha256(encoded).hexdigest()


def _json_bytes(payload: object) -> bytes:
    return json.dumps(
        _json_value(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_value(value: object) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    try:
        return _json_value(asdict(value))
    except TypeError as exc:
        raise TypeError(f"cannot serialize R2 replay value {type(value)!r}") from exc


def _write_once_or_verify(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(
                f"refusing to overwrite immutable R2 artifact: {path.name}"
            )
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
