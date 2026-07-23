"""Persist conservative internal evidence for the next candle hypotheses.

The application layer owns causal feature and outcome calculation.  This
adapter only selects the sealed holdout partition, maps it to the internal
replay evidence vocabulary, and writes a deterministic local artifact.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, time, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.scientific_candle_evidence import (
    AssessScientificCandleHoldoutEvidence,
    ScientificCandleEvidenceCoverage,
)
from tinvest_signal_engine.application.scientific_candle_models import (
    ScientificCandleResearchReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import EvidenceBundle, EvidenceDecision
from tinvest_signal_engine.domain.scientific_candle_models import (
    ScientificCandleHypothesis,
)


@dataclass(frozen=True, slots=True)
class ScientificCandleReplayDefinition:
    hypothesis: ScientificCandleHypothesis
    catalog_hypothesis_id: str
    hypothesis_version: str
    expected_direction: str
    market_phase: str


@dataclass(frozen=True, slots=True)
class ScientificCandleReplayArtifact:
    artifact_uri: str
    artifact_fingerprint: str
    evidence: tuple[Mapping[str, Any], ...]


_DEFINITIONS = {
    ScientificCandleHypothesis.OPENING_GAP_REVERSION: (
        ScientificCandleReplayDefinition(
            ScientificCandleHypothesis.OPENING_GAP_REVERSION,
            "h10-positive-main-open-gap-reversion",
            "1.0.0",
            "reversion_to_previous_close",
            "main_session_open",
        )
    ),
    ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION: (
        ScientificCandleReplayDefinition(
            ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION,
            "h11-residual-move-reversion",
            "1.0.0",
            "reversal",
            "main_session",
        )
    ),
    ScientificCandleHypothesis.HAR_VOLATILITY: ScientificCandleReplayDefinition(
        ScientificCandleHypothesis.HAR_VOLATILITY,
        "h15-multi-window-volatility-forecast",
        "1.0.0",
        "volatility_increase",
        "any_liquid_session_phase",
    ),
    ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2: (
        ScientificCandleReplayDefinition(
            ScientificCandleHypothesis.RELATIVE_VOLUME_ACTIVITY_V2,
            "h7-relative-volume-future-activity",
            "2.0.0",
            "activity_increase",
            "any_liquid_session_phase",
        )
    ),
}


INTERMEDIATE_SCIENTIFIC_CANDLE_EVIDENCE_POLICY = EvidenceGatePolicy(
    minimum_trading_days=20,
    minimum_eligible_events=200,
    controls_per_event=5,
    false_discovery_rate=0.05,
    required_positive_stability_blocks=3,
    maximum_instrument_share=0.40,
    minimum_common_support_coverage=0.10,
)


class ScientificCandleReplayArtifactAdapter:
    """Map one report to immutable, reproducible evidence JSON."""

    def __init__(
        self,
        root: str | Path,
        *,
        evidence_policy: EvidenceGatePolicy = (
            INTERMEDIATE_SCIENTIFIC_CANDLE_EVIDENCE_POLICY
        ),
    ) -> None:
        self._root = Path(root)
        self._evidence_policy = evidence_policy
        self._evidence_gate = AssessScientificCandleHoldoutEvidence(evidence_policy)

    def save(
        self,
        report: ScientificCandleResearchReport,
        requested_hypotheses: Sequence[ScientificCandleHypothesis],
        *,
        cost_model_version: str,
    ) -> ScientificCandleReplayArtifact:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        if not selected:
            raise ValueError("at least one scientific candle hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost model version must not be empty")
        unavailable = set(selected) - set(report.selected_hypotheses)
        if unavailable:
            raise ValueError("requested hypothesis is absent from research report")

        artifact_fingerprint = _fingerprint(
            {
                "cost_model_version": cost_model_version,
                "evidence_policy": asdict(self._evidence_policy),
                "report_fingerprint": report.report_fingerprint,
                "selected_hypotheses": [item.value for item in selected],
            }
        )
        generated_at = _deterministic_generated_at(report)
        assessment = self._evidence_gate.execute(
            report,
            selected,
            cost_model_version=cost_model_version,
        )
        evidence = tuple(
            _evidence_row(
                report,
                hypothesis,
                bundle=assessment.for_hypothesis(hypothesis),
                coverage=assessment.coverage_for(hypothesis),
                controls_per_event=self._evidence_policy.controls_per_event,
                artifact_fingerprint=artifact_fingerprint,
                generated_at=generated_at,
                cost_model_version=cost_model_version,
            )
            for hypothesis in selected
        )
        run_dir = self._root / artifact_fingerprint.removeprefix("sha256:")
        manifest = {
            "dataset_fingerprint": report.dataset_fingerprint,
            "evidence_policy": asdict(self._evidence_policy),
            "report_fingerprint": report.report_fingerprint,
            "policy": asdict(report.policy),
            "selected_hypotheses": [item.value for item in selected],
            "cost_model_version": cost_model_version,
            "evidence_coverage": {
                item.hypothesis_id: _coverage_manifest(item)
                for item in assessment.coverage
            },
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        _write_once_or_verify(run_dir / "evidence.json", _json_bytes(evidence))
        return ScientificCandleReplayArtifact(
            artifact_uri=str(run_dir.resolve()),
            artifact_fingerprint=artifact_fingerprint,
            evidence=evidence,
        )


def _evidence_row(
    report: ScientificCandleResearchReport,
    hypothesis: ScientificCandleHypothesis,
    *,
    bundle: EvidenceBundle,
    coverage: ScientificCandleEvidenceCoverage,
    controls_per_event: int,
    artifact_fingerprint: str,
    generated_at: str,
    cost_model_version: str,
) -> Mapping[str, Any]:
    definition = _DEFINITIONS[hypothesis]
    source_state = (
        "insufficient_history"
        if bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
        else "ready"
    )
    decision = bundle.decision.value
    primary = bundle.mean_lift_bps
    sample_count = bundle.matched_events
    interval = bundle.lift_interval
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
    return {
        "hypothesis_id": hypothesis.value,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": definition.market_phase,
        "source_data_state": source_state,
        "decision": decision,
        "independent_validation": bool(report.split.holdout_days),
        "cost_adjusted": hypothesis
        in {
            ScientificCandleHypothesis.OPENING_GAP_REVERSION,
            ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION,
        },
        "sample_count": sample_count,
        "trading_days": bundle.trading_days,
        "generated_at": generated_at,
        "artifact_fingerprint": artifact_fingerprint,
        "dataset_fingerprint": report.dataset_fingerprint,
        "formula_fingerprint": _formula_fingerprint(report, hypothesis),
        "cost_model_version": cost_model_version,
        "primary_metric_value": primary,
        "matched_control_lift_ci95_lower": interval.lower if interval else None,
        "matched_control_lift_ci95_upper": interval.upper if interval else None,
        "matched_controls": bundle.matched_controls,
        "controls_per_event": controls_per_event,
        "adjusted_p_value": bundle.adjusted_q_value,
        "stable_blocks": bundle.stability.positive_blocks,
        "total_blocks": len(bundle.stability.blocks),
        "maximum_ticker_share": bundle.maximum_instrument_share,
        "maximum_period_share": maximum_period_share,
        "abstention_rate": (
            1.0
            - coverage.eligible_common_support_events
            / coverage.available_holdout_observations
            if coverage.available_holdout_observations
            else None
        ),
        "horizons": (
            {
                "horizon_seconds": _horizon_seconds(report, hypothesis),
                "evidence_scope": "independent_gate",
                "source_data_state": source_state,
                "decision": decision,
                "sample_count": sample_count,
                "primary_metric_value": primary,
            },
        ),
    }


def _coverage_manifest(
    coverage: ScientificCandleEvidenceCoverage,
) -> Mapping[str, Any]:
    return {
        "available_holdout_observations": coverage.available_holdout_observations,
        "triggered_events": coverage.triggered_events,
        "eligible_common_support_events": coverage.eligible_common_support_events,
        "unmatched_events": coverage.unmatched_events,
        "control_candidates": coverage.control_candidates,
        "common_support_rate": coverage.common_support_rate,
        "selection_policy": "pre_outcome_deterministic_common_support_v1",
    }


def _horizon_seconds(
    report: ScientificCandleResearchReport,
    hypothesis: ScientificCandleHypothesis,
) -> int:
    policy = report.policy
    if hypothesis is ScientificCandleHypothesis.OPENING_GAP_REVERSION:
        return policy.opening_gap_horizon_seconds
    if hypothesis is ScientificCandleHypothesis.MARKET_RESIDUAL_REVERSION:
        return policy.residual_horizon_seconds
    if hypothesis is ScientificCandleHypothesis.HAR_VOLATILITY:
        return policy.har_horizon_seconds
    return policy.activity_horizon_seconds


def _formula_fingerprint(
    report: ScientificCandleResearchReport,
    hypothesis: ScientificCandleHypothesis,
) -> str:
    payload = {
        "hypothesis": hypothesis.value,
        "hypothesis_version": _DEFINITIONS[hypothesis].hypothesis_version,
        "policy": asdict(report.policy),
    }
    return _fingerprint(payload)


def _deterministic_generated_at(report: ScientificCandleResearchReport) -> str:
    if report.outcomes:
        return max(item.target_at for item in report.outcomes).isoformat()
    last_day = max(report.split.holdout_days)
    return datetime.combine(last_day, time.min, tzinfo=timezone.utc).isoformat()


def _fingerprint(payload: object) -> str:
    return (
        "sha256:"
        + sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


def _json_bytes(payload: object) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _write_once_or_verify(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != content:
            raise ValueError(
                f"immutable scientific replay artifact differs: {path.name}"
            )
        return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(content)
    os.replace(temporary, path)
