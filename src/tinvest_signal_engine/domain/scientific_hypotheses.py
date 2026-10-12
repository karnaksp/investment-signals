"""Domain model for traceable, preregistered market hypotheses."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
import json
from re import fullmatch
from typing import Mapping


class HypothesisOrigin(str, Enum):
    """How a hypothesis entered the research process."""

    RESEARCH_DERIVED = "research_derived"
    RESEARCH_EXTENSION = "research_extension"
    AUTHOR_PROPOSED = "author_proposed"
    DATA_DISCOVERED = "data_discovered"

    @property
    def supports_scientific_claim(self) -> bool:
        return self in {
            HypothesisOrigin.RESEARCH_DERIVED,
            HypothesisOrigin.RESEARCH_EXTENSION,
        }


class EvidenceLevel(str, Enum):
    NOT_TESTED = "not_tested"
    RESEARCHING = "researching"
    PROMISING = "promising"
    VALIDATED = "validated"
    STRICT_90 = "strict_90"
    REJECTED = "rejected"


class HypothesisLifecycle(str, Enum):
    DRAFT = "draft"
    PRE_REGISTERED = "pre_registered"
    EVALUATED = "evaluated"
    SHADOW = "shadow"
    APPROVED = "approved"
    APPLIED = "applied"
    SUSPENDED = "suspended"
    ROLLED_BACK = "rolled_back"
    RETIRED = "retired"
    REJECTED = "rejected"


class ReplicationResult(str, Enum):
    PASSED = "passed"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"
    BLOCKED_BY_DATA = "blocked_by_data"


@dataclass(frozen=True)
class ScientificSource:
    """A primary or corroborating publication used by a hypothesis."""

    source_id: str
    title: str
    authors: tuple[str, ...]
    publication_year: int
    identifier: str
    url: str
    primary_publication: bool
    market: str
    sample_period: str
    sample_description: str
    data_frequency: str
    economic_mechanism: str
    limitations: tuple[str, ...]
    original_result: str


@dataclass(frozen=True)
class PreregisteredTest:
    """Parameters sealed before an independent validation set is observed."""

    registration_id: str
    hypothesis_id: str
    hypothesis_version: str
    sealed_at: datetime | None
    expected_direction: str
    feature_definitions: tuple[str, ...]
    thresholds: tuple[tuple[str, str], ...]
    market_phase: str
    horizon_seconds: tuple[int, ...]
    success_criterion: str
    abstention_conditions: tuple[str, ...]
    cost_model_version: str
    data_split_policy: str
    multiple_testing_policy: str

    @classmethod
    def with_thresholds(
        cls,
        *,
        thresholds: Mapping[str, object],
        **fields: object,
    ) -> "PreregisteredTest":
        normalized = tuple(
            sorted((str(key), str(value)) for key, value in thresholds.items())
        )
        return cls(thresholds=normalized, **fields)  # type: ignore[arg-type]

    @property
    def sealed(self) -> bool:
        return self.sealed_at is not None

    def sealed_parameters_fingerprint(self) -> str:
        """Stable fingerprint of every parameter that cannot be tuned in-place."""

        payload = {
            "expected_direction": self.expected_direction,
            "feature_definitions": self.feature_definitions,
            "thresholds": self.thresholds,
            "market_phase": self.market_phase,
            "horizon_seconds": self.horizon_seconds,
            "success_criterion": self.success_criterion,
            "abstention_conditions": self.abstention_conditions,
            "cost_model_version": self.cost_model_version,
            "data_split_policy": self.data_split_policy,
            "multiple_testing_policy": self.multiple_testing_policy,
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return f"sha256:{sha256(encoded).hexdigest()}"


@dataclass(frozen=True)
class ScientificHypothesis:
    """A falsifiable hypothesis and the provenance of its product claim."""

    hypothesis_id: str
    version: str
    title: str
    origin: HypothesisOrigin
    source_ids: tuple[str, ...]
    testable_statement: str
    economic_mechanism: str
    market_phase: str
    trigger_conditions: tuple[str, ...]
    expected_direction: str
    horizon_seconds: tuple[int, ...]
    abstention_conditions: tuple[str, ...]
    falsification_criterion: str
    original_market_result: str
    evidence_level: EvidenceLevel
    lifecycle: HypothesisLifecycle
    scientific_claim: bool
    preregistration: PreregisteredTest | None

    def sealed_parameters_fingerprint(self) -> str:
        """Fingerprint analytical inputs that require a new hypothesis version."""

        payload = {
            "origin": self.origin.value,
            "source_ids": self.source_ids,
            "testable_statement": self.testable_statement,
            "economic_mechanism": self.economic_mechanism,
            "market_phase": self.market_phase,
            "trigger_conditions": self.trigger_conditions,
            "expected_direction": self.expected_direction,
            "horizon_seconds": self.horizon_seconds,
            "abstention_conditions": self.abstention_conditions,
            "falsification_criterion": self.falsification_criterion,
            "preregistration": (
                self.preregistration.sealed_parameters_fingerprint()
                if self.preregistration is not None
                else None
            ),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return f"sha256:{sha256(encoded).hexdigest()}"


@dataclass(frozen=True)
class ReplicationEvidence:
    """Immutable result of reproducing a hypothesis on a sealed market sample."""

    evidence_id: str
    hypothesis_id: str
    hypothesis_version: str
    market: str
    observed_at: datetime
    result: ReplicationResult
    independent_validation: bool
    trading_days: int
    eligible_events: int
    cost_adjusted: bool
    matched_controls_applied: bool
    multiple_testing_applied: bool
    stability_checked: bool
    mean_net_bps: float | None
    result_summary: str
    artifact_uri: str
    primary_metric: str = ""
    controls_per_event: int = 0
    lift_ci_lower: float | None = None
    lift_ci_upper: float | None = None
    adjusted_p_value: float | None = None
    stable_blocks: int = 0
    total_blocks: int = 0
    max_ticker_share: float | None = None
    max_period_share: float | None = None
    dataset_fingerprint: str = ""
    formula_fingerprint: str = ""
    cost_model_version: str = ""
    abstention_rate: float | None = None
    success_rate: float | None = None
    success_wilson_lower: float | None = None


def semantic_version_key(version: str) -> tuple[int, int, int] | None:
    """Parse the catalog's deliberately small semantic-version subset."""

    if fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", version) is None:
        return None
    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)
