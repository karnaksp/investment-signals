"""Immutable local evidence artifacts for prospective scientific models."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.prospective_scientific_evidence import (
    BOUNDED_CONTROL_REUSE_HYPOTHESES,
    BOUNDED_CONTROL_REUSE_LIMIT,
    BOUNDED_CONTROL_SELECTION_POLICY_VERSION,
    PROSPECTIVE_EVIDENCE_DEFINITIONS,
    AssessProspectiveScientificEvidence,
    PreparedProspectiveEvidence,
    ProspectiveEvidenceCoverage,
)
from tinvest_signal_engine.application.prospective_scientific_models import (
    ProspectiveScientificReport,
)
from tinvest_signal_engine.domain.hypothesis_evidence import (
    ChronologicalSplit,
    EvidenceBundle,
    EvidenceDecision,
    EvidenceDiagnosticsV2,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveScientificPolicy,
)


PROSPECTIVE_SCIENTIFIC_EVIDENCE_POLICY = EvidenceGatePolicy(
    minimum_trading_days=30,
    minimum_eligible_events=300,
    controls_per_event=5,
    false_discovery_rate=0.05,
    required_positive_stability_blocks=4,
    maximum_instrument_share=0.50,
    minimum_common_support_coverage=0.10,
)

# H12 was the last formula added to this portfolio and was sealed on
# 2026-07-22.  A combined portfolio claim therefore starts no earlier than the
# next trading day; older observations remain useful only for research.
PROSPECTIVE_PRIMARY_HOLDOUT_START = date(2026, 7, 23)
PROSPECTIVE_SCIENTIFIC_EVIDENCE_SCHEMA = "prospective-scientific-evidence-v1.2.0"


@dataclass(frozen=True, slots=True)
class ProspectiveScientificReplayArtifact:
    artifact_uri: str
    artifact_fingerprint: str
    evidence: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _ReportSummary:
    dataset_fingerprint: str
    report_fingerprint: str
    split: ChronologicalSplit
    policy: ProspectiveScientificPolicy
    generated_at: str


@dataclass(frozen=True, slots=True)
class PreparedProspectiveScientificReplay:
    hypothesis: ProspectiveHypothesis
    evidence: PreparedProspectiveEvidence
    dataset_fingerprint: str
    report_fingerprint: str
    split: ChronologicalSplit
    policy: ProspectiveScientificPolicy
    generated_at: str


class ProspectiveScientificReplayArtifactAdapter:
    """Write deterministic evidence once, or verify byte-for-byte identity."""

    def __init__(
        self,
        root: str | Path,
        *,
        evidence_policy: EvidenceGatePolicy = PROSPECTIVE_SCIENTIFIC_EVIDENCE_POLICY,
        primary_holdout_start: date = PROSPECTIVE_PRIMARY_HOLDOUT_START,
    ) -> None:
        self._root = Path(root)
        self._evidence_policy = evidence_policy
        self._evidence_gate = AssessProspectiveScientificEvidence(evidence_policy)
        self._primary_holdout_start = primary_holdout_start

    def save(
        self,
        report: ProspectiveScientificReport,
        requested_hypotheses: Sequence[ProspectiveHypothesis],
        *,
        cost_model_version: str,
    ) -> ProspectiveScientificReplayArtifact:
        return self.save_portfolio(
            (report,),
            requested_hypotheses,
            cost_model_version=cost_model_version,
        )

    def save_portfolio(
        self,
        reports: Iterable[ProspectiveScientificReport],
        requested_hypotheses: Sequence[ProspectiveHypothesis],
        *,
        cost_model_version: str,
    ) -> ProspectiveScientificReplayArtifact:
        """Persist one globally assessed portfolio from sequential reports.

        Callers may yield one report per hypothesis.  Only compact matched
        control requests and report summaries are retained, so the six-model
        portfolio does not keep six full feature/outcome graphs in memory.
        """

        entries: list[PreparedProspectiveScientificReplay] = []
        for report in reports:
            included = tuple(
                hypothesis
                for hypothesis in requested_hypotheses
                if hypothesis in report.selected_hypotheses
                and all(item.hypothesis is not hypothesis for item in entries)
            )
            if not included:
                raise ValueError("research report adds no requested hypothesis")
            for hypothesis in included:
                entries.append(
                    PreparedProspectiveScientificReplay(
                        hypothesis=hypothesis,
                        evidence=self._evidence_gate.prepare(
                            report,
                            hypothesis,
                            cost_model_version,
                        ),
                        dataset_fingerprint=report.dataset_fingerprint,
                        report_fingerprint=report.report_fingerprint,
                        split=report.split,
                        policy=report.policy,
                        generated_at=_deterministic_generated_at(report),
                    )
                )
        return self.save_prepared_portfolio(
            entries,
            requested_hypotheses,
            cost_model_version=cost_model_version,
        )

    def prepare_report(
        self,
        report: ProspectiveScientificReport,
        hypothesis: ProspectiveHypothesis,
        *,
        cost_model_version: str,
    ) -> PreparedProspectiveScientificReplay:
        return PreparedProspectiveScientificReplay(
            hypothesis=hypothesis,
            evidence=self._evidence_gate.prepare(
                report,
                hypothesis,
                cost_model_version,
            ),
            dataset_fingerprint=report.dataset_fingerprint,
            report_fingerprint=report.report_fingerprint,
            split=report.split,
            policy=report.policy,
            generated_at=_deterministic_generated_at(report),
        )

    def prepare_rows(
        self,
        rows: Iterable[tuple[ProspectiveFeature, ProspectiveOutcome]],
        *,
        hypothesis: ProspectiveHypothesis,
        dataset_fingerprint: str,
        report_fingerprint: str,
        split: ChronologicalSplit,
        policy: ProspectiveScientificPolicy,
        generated_at: str,
        cost_model_version: str,
    ) -> PreparedProspectiveScientificReplay:
        return PreparedProspectiveScientificReplay(
            hypothesis=hypothesis,
            evidence=self._evidence_gate.prepare_rows(
                rows,
                hypothesis=hypothesis,
                split=split,
                policy=policy,
                dataset_fingerprint=dataset_fingerprint,
                cost_model_version=cost_model_version,
            ),
            dataset_fingerprint=dataset_fingerprint,
            report_fingerprint=report_fingerprint,
            split=split,
            policy=policy,
            generated_at=generated_at,
        )

    def save_prepared_portfolio(
        self,
        entries: Sequence[PreparedProspectiveScientificReplay],
        requested_hypotheses: Sequence[ProspectiveHypothesis],
        *,
        cost_model_version: str,
    ) -> ProspectiveScientificReplayArtifact:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        if not selected:
            raise ValueError("at least one prospective hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost_model_version must not be empty")
        by_hypothesis = {item.hypothesis: item for item in entries}
        if len(by_hypothesis) != len(entries):
            raise ValueError("prepared prospective hypotheses must be unique")
        unavailable = set(selected) - set(by_hypothesis)
        if unavailable:
            raise ValueError("requested hypothesis is absent from research reports")
        dataset_fingerprints = {item.dataset_fingerprint for item in entries}
        policies = {item.policy for item in entries}
        if len(dataset_fingerprints) != 1 or len(policies) != 1:
            raise ValueError("portfolio reports must share dataset and policy")
        portfolio_dataset_fingerprint = next(iter(dataset_fingerprints))
        portfolio_policy = next(iter(policies))
        prepared = [by_hypothesis[item].evidence for item in selected]
        summaries = {
            hypothesis: _ReportSummary(
                dataset_fingerprint=by_hypothesis[hypothesis].dataset_fingerprint,
                report_fingerprint=by_hypothesis[hypothesis].report_fingerprint,
                split=by_hypothesis[hypothesis].split,
                policy=by_hypothesis[hypothesis].policy,
                generated_at=by_hypothesis[hypothesis].generated_at,
            )
            for hypothesis in selected
        }

        report_fingerprints = tuple(
            summaries[hypothesis].report_fingerprint for hypothesis in selected
        )
        unique_report_fingerprints = tuple(dict.fromkeys(report_fingerprints))
        portfolio_report_fingerprint = (
            unique_report_fingerprints[0]
            if len(unique_report_fingerprints) == 1
            else _fingerprint(
                {
                    "schema": "prospective-scientific-report-portfolio-v1",
                    "reports": [
                        {
                            "hypothesis": hypothesis.value,
                            "report_fingerprint": summaries[
                                hypothesis
                            ].report_fingerprint,
                        }
                        for hypothesis in selected
                    ],
                }
            )
        )

        artifact_fingerprint = _fingerprint(
            {
                "artifact_schema": PROSPECTIVE_SCIENTIFIC_EVIDENCE_SCHEMA,
                "cost_model_version": cost_model_version,
                "evidence_policy": asdict(self._evidence_policy),
                "bounded_control_reuse": {
                    "hypotheses": sorted(
                        item.value for item in BOUNDED_CONTROL_REUSE_HYPOTHESES
                    ),
                    "maximum_control_reuse": BOUNDED_CONTROL_REUSE_LIMIT,
                    "selection_policy_version": (
                        BOUNDED_CONTROL_SELECTION_POLICY_VERSION
                    ),
                    "minimum_independent_clusters": (
                        self._evidence_policy.minimum_trading_days
                    ),
                },
                "report_fingerprint": portfolio_report_fingerprint,
                "selected_hypotheses": [item.value for item in selected],
                "claim_definitions": [
                    {
                        "hypothesis": item.value,
                        "version": item.version,
                        "claim_family": PROSPECTIVE_EVIDENCE_DEFINITIONS[
                            item
                        ].claim_family.value,
                        "effect_unit": PROSPECTIVE_EVIDENCE_DEFINITIONS[
                            item
                        ].effect_unit.value,
                        "claim_scope": PROSPECTIVE_EVIDENCE_DEFINITIONS[
                            item
                        ].claim_scope,
                        "target_metric": PROSPECTIVE_EVIDENCE_DEFINITIONS[
                            item
                        ].target_metric.value,
                    }
                    for item in selected
                ],
            }
        )
        generated_at = max(summaries[item].generated_at for item in selected)
        assessment = self._evidence_gate.assess_prepared(prepared)
        evidence = tuple(
            _evidence_row(
                summaries[hypothesis],
                hypothesis,
                bundle=assessment.for_hypothesis(hypothesis),
                coverage=assessment.coverage_for(hypothesis),
                controls_per_event=5,
                artifact_fingerprint=artifact_fingerprint,
                generated_at=generated_at,
                cost_model_version=cost_model_version,
                independent_validation=_has_primary_holdout(
                    summaries[hypothesis],
                    starts_on=self._primary_holdout_start,
                ),
            )
            for hypothesis in selected
        )
        run_dir = self._root / artifact_fingerprint.removeprefix("sha256:")
        manifest = {
            "artifact_schema": PROSPECTIVE_SCIENTIFIC_EVIDENCE_SCHEMA,
            "dataset_fingerprint": portfolio_dataset_fingerprint,
            "evidence_policy": asdict(self._evidence_policy),
            "report_fingerprint": portfolio_report_fingerprint,
            "report_fingerprints": {
                hypothesis.value: summaries[hypothesis].report_fingerprint
                for hypothesis in selected
            },
            "policy": asdict(portfolio_policy),
            "selected_hypotheses": [item.value for item in selected],
            "cost_model_version": cost_model_version,
            "bounded_control_reuse": {
                "hypotheses": sorted(
                    item.value for item in BOUNDED_CONTROL_REUSE_HYPOTHESES
                ),
                "maximum_control_reuse": BOUNDED_CONTROL_REUSE_LIMIT,
                "selection_policy_version": (BOUNDED_CONTROL_SELECTION_POLICY_VERSION),
                "minimum_independent_clusters": (
                    self._evidence_policy.minimum_trading_days
                ),
            },
            "evidence_coverage": {
                item.hypothesis_id: _coverage_manifest(item)
                for item in assessment.coverage
            },
        }
        _write_once_or_verify(run_dir / "manifest.json", _json_bytes(manifest))
        _write_once_or_verify(run_dir / "evidence.json", _json_bytes(evidence))
        return ProspectiveScientificReplayArtifact(
            artifact_uri=str(run_dir.resolve()),
            artifact_fingerprint=artifact_fingerprint,
            evidence=evidence,
        )


def _evidence_row(
    report: _ReportSummary,
    hypothesis: ProspectiveHypothesis,
    *,
    bundle: EvidenceBundle,
    coverage: ProspectiveEvidenceCoverage,
    controls_per_event: int,
    artifact_fingerprint: str,
    generated_at: str,
    cost_model_version: str,
    independent_validation: bool,
) -> Mapping[str, Any]:
    definition = PROSPECTIVE_EVIDENCE_DEFINITIONS[hypothesis]
    source_state = (
        "insufficient_history"
        if bundle.decision is EvidenceDecision.BLOCKED_BY_DATA
        else "ready"
    )
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
    horizons = _horizons(report, hypothesis)
    return {
        "hypothesis_id": hypothesis.value,
        "catalog_hypothesis_id": definition.catalog_hypothesis_id,
        "expected_direction": definition.expected_direction,
        "market_phase": "eligible_moex_equity_session",
        "source_data_state": source_state,
        "decision": bundle.decision.value,
        "reason_codes": bundle.reason_codes,
        "independent_validation": independent_validation,
        "cost_adjusted": hypothesis
        in {
            ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION,
            ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION,
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
            ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
            ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
            ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE,
            ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION,
            ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION,
        },
        "sample_count": bundle.matched_events,
        "trading_days": bundle.trading_days,
        "generated_at": generated_at,
        "artifact_fingerprint": artifact_fingerprint,
        "dataset_fingerprint": report.dataset_fingerprint,
        "formula_fingerprint": _formula_fingerprint(report, hypothesis),
        "cost_model_version": cost_model_version,
        "primary_metric_value": bundle.mean_lift_bps,
        "matched_control_lift_ci95_lower": interval.lower if interval else None,
        "matched_control_lift_ci95_upper": interval.upper if interval else None,
        "matched_controls": bundle.matched_controls,
        "controls_per_event": controls_per_event,
        "control_matching": {
            "selection_policy_version": (coverage.control_selection_policy_version),
            "maximum_control_reuse": coverage.maximum_control_reuse,
            "distinct_controls": coverage.distinct_controls,
            "maximum_observed_control_reuse": (coverage.maximum_observed_control_reuse),
            "mean_control_reuse": coverage.mean_control_reuse,
            "independent_control_clusters": (coverage.independent_control_clusters),
            "minimum_independent_control_clusters": (
                coverage.minimum_independent_control_clusters
            ),
            "inference_unit": (
                "control_dependency_cluster"
                if coverage.maximum_observed_control_reuse > 1
                else "trading_day"
            ),
        },
        "adjusted_p_value": bundle.adjusted_q_value,
        "stable_blocks": bundle.stability.positive_blocks,
        "total_blocks": len(bundle.stability.blocks),
        "maximum_ticker_share": bundle.maximum_instrument_share,
        "maximum_period_share": maximum_period_share,
        "diagnostics_v2": _diagnostics_v2_payload(bundle.diagnostics_v2),
        "abstention_rate": (
            1.0
            - coverage.eligible_common_support_events
            / coverage.available_holdout_observations
            if coverage.available_holdout_observations
            else None
        ),
        "horizons": tuple(
            {
                "horizon_seconds": horizon,
                "evidence_scope": "independent_gate",
                "source_data_state": source_state,
                "decision": bundle.decision.value,
                "sample_count": bundle.matched_events,
                "primary_metric_value": bundle.mean_lift_bps,
            }
            for horizon in horizons
        ),
        "claim_family": definition.claim_family.value,
        "effect_unit": definition.effect_unit.value,
        "claim_scope": definition.claim_scope,
        "target_metric": definition.target_metric.value,
    }


def _diagnostics_v2_payload(
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
            {
                "reason_code": item.reason_code,
                "count": item.count,
            }
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


def _has_primary_holdout(
    report: _ReportSummary,
    *,
    starts_on: date,
) -> bool:
    return (
        bool(report.split.holdout_days) and min(report.split.holdout_days) >= starts_on
    )


def _coverage_manifest(
    coverage: ProspectiveEvidenceCoverage,
) -> Mapping[str, Any]:
    return {
        "available_holdout_observations": coverage.available_holdout_observations,
        "triggered_events": coverage.triggered_events,
        "eligible_common_support_events": coverage.eligible_common_support_events,
        "unmatched_events": coverage.unmatched_events,
        "control_candidates": coverage.control_candidates,
        "common_support_rate": coverage.common_support_rate,
        "selection_policy": coverage.control_selection_policy_version,
        "maximum_control_reuse": coverage.maximum_control_reuse,
        "distinct_controls": coverage.distinct_controls,
        "maximum_observed_control_reuse": (coverage.maximum_observed_control_reuse),
        "mean_control_reuse": coverage.mean_control_reuse,
        "independent_control_clusters": (coverage.independent_control_clusters),
        "minimum_independent_control_clusters": (
            coverage.minimum_independent_control_clusters
        ),
    }


def _horizons(
    report: _ReportSummary,
    hypothesis: ProspectiveHypothesis,
) -> tuple[int, ...]:
    if hypothesis is ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION:
        return report.policy.morning_reversion_horizons_seconds
    if hypothesis is ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION:
        return report.policy.morning_continuation_horizons_seconds
    if hypothesis in {
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V3,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V3,
    }:
        return report.policy.jump_horizons_seconds
    if hypothesis is ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3:
        return (report.policy.volume_horizon_seconds,)
    if hypothesis is ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE:
        return (report.policy.phase_recurrence_horizon_seconds,)
    if hypothesis is ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION:
        return (report.policy.open_close_horizon_seconds,)
    if hypothesis is ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION:
        return report.policy.pair_horizons_seconds
    if hypothesis is ProspectiveHypothesis.HAR_VOLATILITY_V2:
        return (report.policy.har_horizon_seconds,)
    if hypothesis is ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK:
        return (report.policy.semivariance_horizon_seconds,)
    return (report.policy.jump_variance_horizon_seconds,)


def _formula_fingerprint(
    report: _ReportSummary,
    hypothesis: ProspectiveHypothesis,
) -> str:
    definition = PROSPECTIVE_EVIDENCE_DEFINITIONS[hypothesis]
    return _fingerprint(
        {
            "hypothesis": hypothesis.value,
            "hypothesis_version": hypothesis.version,
            "claim_family": definition.claim_family.value,
            "effect_unit": definition.effect_unit.value,
            "target_metric": definition.target_metric.value,
            "policy": asdict(report.policy),
        }
    )


def _deterministic_generated_at(report: ProspectiveScientificReport) -> str:
    if report.outcomes:
        return max(item.target_at for item in report.outcomes).isoformat()
    return datetime.combine(
        max(report.split.holdout_days), time.min, tzinfo=timezone.utc
    ).isoformat()


def _fingerprint(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(encoded).hexdigest()


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
                f"immutable prospective replay artifact differs: {path.name}"
            )
        return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(content)
    os.replace(temporary, path)
