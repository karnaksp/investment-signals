"""Product-safe C1-C4 aggregates derived from immutable replay artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tinvest_signal_engine.domain.scientific_hypothesis_combinations import (
    ScientificCombinationId,
    combination_formula_fingerprint,
    preregistered_combination_definition,
)


@dataclass(frozen=True, slots=True)
class CombinationConfidenceIntervalSnapshot:
    lower: float
    estimate: float
    upper: float
    confidence_level: float


@dataclass(frozen=True, slots=True)
class CombinationReasonCountSnapshot:
    reason_code: str
    count: int


@dataclass(frozen=True, slots=True)
class CombinationDiagnosticsSnapshot:
    version: str
    event_prevalence: float | None
    eligible_event_count: int
    matched_event_count: int
    match_coverage: float | None
    data_coverage: float | None
    reasons_histogram: tuple[CombinationReasonCountSnapshot, ...]
    primary_effect_estimate: float | None
    primary_effect_interval: CombinationConfidenceIntervalSnapshot | None
    primary_p_value: float | None
    descriptive_only: bool


@dataclass(frozen=True, slots=True)
class CombinationHorizonArtifactSnapshot:
    combination_id: ScientificCombinationId
    combination_version: str
    horizon_seconds: int
    dataset_fingerprint: str
    decision: str
    reason_codes: tuple[str, ...]
    total_observations: int
    abstained_observations: int
    eligible_events: int
    matched_events: int
    matched_controls: int
    trading_days: int
    cost_model_version: str
    mean_lift_bps: float | None
    lift_interval: CombinationConfidenceIntervalSnapshot | None
    adjusted_q_value: float | None
    positive_stability_blocks: int
    total_stability_blocks: int
    maximum_instrument_share: float | None
    diagnostics: CombinationDiagnosticsSnapshot | None

    def __post_init__(self) -> None:
        definition = preregistered_combination_definition(self.combination_id)
        if self.combination_version != definition.version:
            raise ValueError("derived combination version drifted")
        if self.horizon_seconds not in definition.horizons_seconds:
            raise ValueError("derived combination horizon is not registered")
        if not _is_sha256(self.dataset_fingerprint):
            raise ValueError("derived combination dataset fingerprint is invalid")
        if not self.cost_model_version.strip():
            raise ValueError("derived combination cost model is required")
        counts = (
            self.total_observations,
            self.abstained_observations,
            self.eligible_events,
            self.matched_events,
            self.matched_controls,
            self.trading_days,
            self.positive_stability_blocks,
            self.total_stability_blocks,
        )
        if any(value < 0 for value in counts):
            raise ValueError("derived combination counts must be non-negative")


@dataclass(frozen=True, slots=True)
class CombinationEvidenceArtifactSnapshot:
    artifact_fingerprint: str
    dataset_fingerprint: str
    cost_model_version: str
    horizons: tuple[CombinationHorizonArtifactSnapshot, ...]

    def __post_init__(self) -> None:
        if not _is_sha256(self.artifact_fingerprint):
            raise ValueError("derived combination artifact fingerprint is invalid")
        if not _is_sha256(self.dataset_fingerprint):
            raise ValueError("derived combination dataset fingerprint is invalid")
        if not self.cost_model_version.strip():
            raise ValueError("derived combination cost model is required")
        actual = tuple(
            sorted((row.combination_id.value, row.horizon_seconds) for row in self.horizons)
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
            raise ValueError("derived artifact must contain every C1-C4 horizon once")
        if any(
            row.dataset_fingerprint != self.dataset_fingerprint
            or row.cost_model_version != self.cost_model_version
            for row in self.horizons
        ):
            raise ValueError("derived artifact metadata drifted across horizons")


class ScientificCombinationEvidenceArtifactPort(Protocol):
    def read(
        self,
        artifact_uri: str,
        *,
        expected_artifact_fingerprint: str,
    ) -> CombinationEvidenceArtifactSnapshot: ...


@dataclass(frozen=True, slots=True)
class DerivedCombinationHorizonEvidence:
    horizon_seconds: int
    evidence_scope: str
    source_data_state: str
    decision: str
    sample_count: int
    primary_metric_value: float | None


@dataclass(frozen=True, slots=True)
class DerivedScientificCombinationEvidence:
    hypothesis_id: str
    catalog_hypothesis_id: str
    expected_direction: str
    market_phase: str
    source_data_state: str
    decision: str
    reason_codes: tuple[str, ...]
    independent_validation: bool
    cost_adjusted: bool
    sample_count: int
    trading_days: int
    generated_at: datetime
    artifact_fingerprint: str
    dataset_fingerprint: str
    formula_fingerprint: str
    cost_model_version: str
    primary_metric_value: float | None
    matched_control_lift_ci95_lower: float | None
    matched_control_lift_ci95_upper: float | None
    matched_controls: int
    controls_per_event: int
    adjusted_p_value: float | None
    stable_blocks: int
    total_blocks: int
    maximum_ticker_share: float | None
    maximum_period_share: float | None
    abstention_rate: float | None
    diagnostics_v2: CombinationDiagnosticsSnapshot | None
    horizons: tuple[DerivedCombinationHorizonEvidence, ...]
    claim_family: str
    effect_unit: str
    claim_scope: str
    target_metric: str


_CATALOG_IDS = {
    ScientificCombinationId.C1: "c1-volume-risk-confirmed-continuation",
    ScientificCombinationId.C2: "c2-unconfirmed-jump-reversal",
    ScientificCombinationId.C3: "c3-morning-regime-selection",
    ScientificCombinationId.C4: "c4-calm-market-pair-reversion",
}
_MARKET_PHASES = {
    ScientificCombinationId.C1: "intraday",
    ScientificCombinationId.C2: "intraday",
    ScientificCombinationId.C3: "morning",
    ScientificCombinationId.C4: "intraday",
}


class BuildDerivedScientificCombinationEvidence:
    """Read one sealed artifact and expose only bounded aggregate evidence."""

    def __init__(self, artifacts: ScientificCombinationEvidenceArtifactPort) -> None:
        self._artifacts = artifacts

    def execute(
        self,
        artifact_uri: str,
        *,
        expected_artifact_fingerprint: str,
        generated_at: datetime,
    ) -> tuple[DerivedScientificCombinationEvidence, ...]:
        if generated_at.tzinfo is None or generated_at.utcoffset() is None:
            raise ValueError("derived evidence timestamp must be timezone-aware")
        snapshot = self._artifacts.read(
            artifact_uri,
            expected_artifact_fingerprint=expected_artifact_fingerprint,
        )
        grouped = {
            combination_id: tuple(
                row for row in snapshot.horizons if row.combination_id is combination_id
            )
            for combination_id in ScientificCombinationId
        }
        return tuple(
            self._aggregate(
                combination_id,
                grouped[combination_id],
                snapshot=snapshot,
                generated_at=generated_at,
            )
            for combination_id in ScientificCombinationId
        )

    @staticmethod
    def _aggregate(
        combination_id: ScientificCombinationId,
        rows: tuple[CombinationHorizonArtifactSnapshot, ...],
        *,
        snapshot: CombinationEvidenceArtifactSnapshot,
        generated_at: datetime,
    ) -> DerivedScientificCombinationEvidence:
        definition = preregistered_combination_definition(combination_id)
        primary = next(
            row
            for row in rows
            if row.horizon_seconds == definition.primary_horizon_seconds
        )
        reason_codes = tuple(
            sorted({reason for row in rows for reason in row.reason_codes})
        )
        source_data_state = _source_data_state(primary.decision)
        abstention_rate = (
            primary.abstained_observations / primary.total_observations
            if primary.total_observations
            else None
        )
        return DerivedScientificCombinationEvidence(
            hypothesis_id=combination_id.value,
            catalog_hypothesis_id=_CATALOG_IDS[combination_id],
            expected_direction=definition.expected_effect.value,
            market_phase=_MARKET_PHASES[combination_id],
            source_data_state=source_data_state,
            decision=primary.decision,
            reason_codes=reason_codes,
            # Current C1-C4 artifacts use one chronological holdout from the
            # same source dataset.  They are never independent validation.
            independent_validation=False,
            cost_adjusted=True,
            sample_count=primary.eligible_events,
            trading_days=primary.trading_days,
            generated_at=generated_at,
            artifact_fingerprint=snapshot.artifact_fingerprint,
            dataset_fingerprint=snapshot.dataset_fingerprint,
            formula_fingerprint=combination_formula_fingerprint(combination_id),
            cost_model_version=snapshot.cost_model_version,
            primary_metric_value=primary.mean_lift_bps,
            matched_control_lift_ci95_lower=(
                primary.lift_interval.lower if primary.lift_interval else None
            ),
            matched_control_lift_ci95_upper=(
                primary.lift_interval.upper if primary.lift_interval else None
            ),
            matched_controls=primary.matched_controls,
            controls_per_event=5,
            adjusted_p_value=primary.adjusted_q_value,
            stable_blocks=primary.positive_stability_blocks,
            total_blocks=primary.total_stability_blocks,
            maximum_ticker_share=primary.maximum_instrument_share,
            maximum_period_share=None,
            abstention_rate=abstention_rate,
            diagnostics_v2=primary.diagnostics,
            horizons=tuple(
                DerivedCombinationHorizonEvidence(
                    horizon_seconds=row.horizon_seconds,
                    evidence_scope="descriptive_only",
                    source_data_state=_source_data_state(row.decision),
                    decision=row.decision,
                    sample_count=row.eligible_events,
                    primary_metric_value=row.mean_lift_bps,
                )
                for row in rows
            ),
            claim_family="directional",
            effect_unit="incremental_cost_adjusted_lift_bps",
            claim_scope="combination_incremental_effect",
            target_metric=definition.target.value,
        )


def _source_data_state(decision: str) -> str:
    return "insufficient_history" if decision == "blocked_by_data" else "ready"


def _is_sha256(value: str) -> bool:
    return (
        len(value) == 71
        and value.startswith("sha256:")
        and all(character in "0123456789abcdef" for character in value[7:])
    )
