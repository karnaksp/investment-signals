"""YAML/JSON adapter for the versioned scientific hypothesis registry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from tinvest_signal_engine.domain.scientific_hypotheses import (
    EvidenceLevel,
    HypothesisLifecycle,
    HypothesisOrigin,
    PreregisteredTest,
    ReplicationEvidence,
    ReplicationResult,
    ScientificHypothesis,
    ScientificSource,
    semantic_version_key,
)


class ScientificRegistryFormatError(ValueError):
    """The external registry cannot be mapped to the domain contract."""


@dataclass(frozen=True)
class AppliedHypothesisReference:
    hypothesis_id: str
    version: str
    evidence_id: str


@dataclass(frozen=True)
class VersionedScientificRegistry:
    schema_version: str
    sources: tuple[ScientificSource, ...]
    hypotheses: tuple[ScientificHypothesis, ...]
    replication_evidence: tuple[ReplicationEvidence, ...]
    applied_catalog: tuple[AppliedHypothesisReference, ...]

    @classmethod
    def from_file(cls, path: str | Path) -> "VersionedScientificRegistry":
        registry_path = Path(path)
        try:
            raw_text = registry_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ScientificRegistryFormatError(str(exc)) from exc
        try:
            if registry_path.suffix.lower() == ".json":
                payload = json.loads(raw_text)
            elif registry_path.suffix.lower() in {".yaml", ".yml"}:
                payload = yaml.safe_load(raw_text)
            else:
                raise ScientificRegistryFormatError(
                    "Registry file must use .json, .yaml, or .yml"
                )
        except (json.JSONDecodeError, yaml.YAMLError) as exc:
            raise ScientificRegistryFormatError(str(exc)) from exc
        if not isinstance(payload, Mapping):
            raise ScientificRegistryFormatError("Registry root must be an object")
        return cls.from_mapping(payload)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "VersionedScientificRegistry":
        schema_version = _required_text(payload, "schema_version", "registry")
        if semantic_version_key(schema_version) is None:
            raise ScientificRegistryFormatError(
                f"registry.schema_version is not semantic: {schema_version!r}"
            )
        sources = tuple(
            _source(item, index)
            for index, item in enumerate(_records(payload, "sources"))
        )
        hypotheses = tuple(
            _hypothesis(item, index)
            for index, item in enumerate(_records(payload, "hypotheses"))
        )
        evidence = tuple(
            _replication(item, index)
            for index, item in enumerate(_records(payload, "replication_evidence"))
        )
        applied = tuple(
            _applied_reference(item, index)
            for index, item in enumerate(_records(payload, "applied_catalog"))
        )
        _unique((item.source_id for item in sources), "source_id")
        _unique(
            (f"{item.hypothesis_id}@{item.version}" for item in hypotheses),
            "hypothesis id/version",
        )
        _unique((item.evidence_id for item in evidence), "evidence_id")
        _unique((item.hypothesis_id for item in applied), "applied hypothesis_id")
        return cls(
            schema_version=schema_version,
            sources=sources,
            hypotheses=hypotheses,
            replication_evidence=evidence,
            applied_catalog=applied,
        )

    def get_source(self, source_id: str) -> ScientificSource | None:
        return next(
            (source for source in self.sources if source.source_id == source_id),
            None,
        )

    def get_hypothesis(
        self,
        hypothesis_id: str,
        version: str,
    ) -> ScientificHypothesis | None:
        return next(
            (
                hypothesis
                for hypothesis in self.hypotheses
                if hypothesis.hypothesis_id == hypothesis_id
                and hypothesis.version == version
            ),
            None,
        )

    def get_evidence(self, evidence_id: str) -> ReplicationEvidence | None:
        return next(
            (
                evidence
                for evidence in self.replication_evidence
                if evidence.evidence_id == evidence_id
            ),
            None,
        )

    def latest_version(self, hypothesis_id: str) -> ScientificHypothesis | None:
        """Return the version currently referenced by the applied catalog."""

        reference = next(
            (
                item
                for item in self.applied_catalog
                if item.hypothesis_id == hypothesis_id
            ),
            None,
        )
        if reference is None:
            return None
        return self.get_hypothesis(reference.hypothesis_id, reference.version)


def _records(payload: Mapping[str, Any], field: str) -> Sequence[Mapping[str, Any]]:
    value = payload.get(field, [])
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise ScientificRegistryFormatError(f"registry.{field} must be a list of objects")
    return value


def _required_text(record: Mapping[str, Any], field: str, location: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ScientificRegistryFormatError(f"{location}.{field} must be non-empty text")
    return value.strip()


def _optional_text(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field, "")
    if not isinstance(value, str):
        raise ScientificRegistryFormatError(f"{field} must be text")
    return value.strip()


def _text_tuple(record: Mapping[str, Any], field: str, location: str) -> tuple[str, ...]:
    value = record.get(field, [])
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise ScientificRegistryFormatError(
            f"{location}.{field} must be a list of non-empty strings"
        )
    return tuple(item.strip() for item in value)


def _integer_tuple(record: Mapping[str, Any], field: str, location: str) -> tuple[int, ...]:
    value = record.get(field, [])
    if not isinstance(value, list) or any(
        not isinstance(item, int) or isinstance(item, bool) or item <= 0
        for item in value
    ):
        raise ScientificRegistryFormatError(
            f"{location}.{field} must be a list of positive integers"
        )
    return tuple(value)


def _required_bool(record: Mapping[str, Any], field: str, location: str) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        raise ScientificRegistryFormatError(f"{location}.{field} must be boolean")
    return value


def _required_int(record: Mapping[str, Any], field: str, location: str) -> int:
    value = record.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ScientificRegistryFormatError(
            f"{location}.{field} must be a non-negative integer"
        )
    return value


def _timestamp(value: object, location: str, *, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ScientificRegistryFormatError(f"{location} must be an RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScientificRegistryFormatError(f"{location} must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise ScientificRegistryFormatError(f"{location} must include a timezone")
    return parsed


def _enum(enum_type: type[Any], value: object, location: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        choices = ", ".join(item.value for item in enum_type)
        raise ScientificRegistryFormatError(
            f"{location} must be one of: {choices}"
        ) from exc


def _source(record: Mapping[str, Any], index: int) -> ScientificSource:
    location = f"sources[{index}]"
    year = record.get("publication_year")
    if not isinstance(year, int) or isinstance(year, bool) or year < 1900:
        raise ScientificRegistryFormatError(
            f"{location}.publication_year must be an integer from 1900"
        )
    authors = _text_tuple(record, "authors", location)
    if not authors:
        raise ScientificRegistryFormatError(f"{location}.authors must not be empty")
    limitations = _text_tuple(record, "limitations", location)
    if not limitations:
        raise ScientificRegistryFormatError(f"{location}.limitations must not be empty")
    return ScientificSource(
        source_id=_required_text(record, "source_id", location),
        title=_required_text(record, "title", location),
        authors=authors,
        publication_year=year,
        identifier=_required_text(record, "identifier", location),
        url=_required_text(record, "url", location),
        primary_publication=_required_bool(record, "primary_publication", location),
        market=_required_text(record, "market", location),
        sample_period=_required_text(record, "sample_period", location),
        sample_description=_required_text(record, "sample_description", location),
        data_frequency=_required_text(record, "data_frequency", location),
        economic_mechanism=_required_text(record, "economic_mechanism", location),
        limitations=limitations,
        original_result=_required_text(record, "original_result", location),
    )


def _preregistration(
    record: Mapping[str, Any] | None,
    location: str,
) -> PreregisteredTest | None:
    if record is None:
        return None
    thresholds = record.get("thresholds", {})
    if not isinstance(thresholds, Mapping):
        raise ScientificRegistryFormatError(f"{location}.thresholds must be an object")
    return PreregisteredTest.with_thresholds(
        thresholds=thresholds,
        registration_id=_required_text(record, "registration_id", location),
        hypothesis_id=_required_text(record, "hypothesis_id", location),
        hypothesis_version=_required_text(record, "hypothesis_version", location),
        sealed_at=_timestamp(record.get("sealed_at"), f"{location}.sealed_at", optional=True),
        expected_direction=_required_text(record, "expected_direction", location),
        feature_definitions=_text_tuple(record, "feature_definitions", location),
        market_phase=_required_text(record, "market_phase", location),
        horizon_seconds=_integer_tuple(record, "horizon_seconds", location),
        success_criterion=_required_text(record, "success_criterion", location),
        abstention_conditions=_text_tuple(record, "abstention_conditions", location),
        cost_model_version=_required_text(record, "cost_model_version", location),
        data_split_policy=_required_text(record, "data_split_policy", location),
        multiple_testing_policy=_required_text(
            record, "multiple_testing_policy", location
        ),
    )


def _hypothesis(record: Mapping[str, Any], index: int) -> ScientificHypothesis:
    location = f"hypotheses[{index}]"
    preregistration = record.get("preregistration")
    if preregistration is not None and not isinstance(preregistration, Mapping):
        raise ScientificRegistryFormatError(
            f"{location}.preregistration must be an object or null"
        )
    return ScientificHypothesis(
        hypothesis_id=_required_text(record, "hypothesis_id", location),
        version=_required_text(record, "version", location),
        title=_required_text(record, "title", location),
        origin=_enum(HypothesisOrigin, record.get("origin"), f"{location}.origin"),
        source_ids=_text_tuple(record, "source_ids", location),
        testable_statement=_optional_text(record, "testable_statement"),
        economic_mechanism=_required_text(record, "economic_mechanism", location),
        market_phase=_required_text(record, "market_phase", location),
        trigger_conditions=_text_tuple(record, "trigger_conditions", location),
        expected_direction=_required_text(record, "expected_direction", location),
        horizon_seconds=_integer_tuple(record, "horizon_seconds", location),
        abstention_conditions=_text_tuple(record, "abstention_conditions", location),
        falsification_criterion=_optional_text(record, "falsification_criterion"),
        original_market_result=_required_text(record, "original_market_result", location),
        evidence_level=_enum(
            EvidenceLevel,
            record.get("evidence_level"),
            f"{location}.evidence_level",
        ),
        lifecycle=_enum(
            HypothesisLifecycle,
            record.get("lifecycle"),
            f"{location}.lifecycle",
        ),
        scientific_claim=_required_bool(record, "scientific_claim", location),
        preregistration=_preregistration(
            preregistration,
            f"{location}.preregistration",
        ),
    )


def _replication(record: Mapping[str, Any], index: int) -> ReplicationEvidence:
    location = f"replication_evidence[{index}]"
    mean_net_bps = record.get("mean_net_bps")
    if mean_net_bps is not None and (
        not isinstance(mean_net_bps, (int, float)) or isinstance(mean_net_bps, bool)
    ):
        raise ScientificRegistryFormatError(f"{location}.mean_net_bps must be numeric or null")
    return ReplicationEvidence(
        evidence_id=_required_text(record, "evidence_id", location),
        hypothesis_id=_required_text(record, "hypothesis_id", location),
        hypothesis_version=_required_text(record, "hypothesis_version", location),
        market=_required_text(record, "market", location),
        observed_at=_timestamp(record.get("observed_at"), f"{location}.observed_at"),  # type: ignore[arg-type]
        result=_enum(ReplicationResult, record.get("result"), f"{location}.result"),
        independent_validation=_required_bool(
            record, "independent_validation", location
        ),
        trading_days=_required_int(record, "trading_days", location),
        eligible_events=_required_int(record, "eligible_events", location),
        cost_adjusted=_required_bool(record, "cost_adjusted", location),
        matched_controls_applied=_required_bool(
            record, "matched_controls_applied", location
        ),
        multiple_testing_applied=_required_bool(
            record, "multiple_testing_applied", location
        ),
        stability_checked=_required_bool(record, "stability_checked", location),
        mean_net_bps=float(mean_net_bps) if mean_net_bps is not None else None,
        result_summary=_required_text(record, "result_summary", location),
        artifact_uri=_optional_text(record, "artifact_uri"),
    )


def _applied_reference(
    record: Mapping[str, Any],
    index: int,
) -> AppliedHypothesisReference:
    location = f"applied_catalog[{index}]"
    return AppliedHypothesisReference(
        hypothesis_id=_required_text(record, "hypothesis_id", location),
        version=_required_text(record, "version", location),
        evidence_id=_required_text(record, "evidence_id", location),
    )


def _unique(values: Sequence[str] | Any, label: str) -> None:
    collected = tuple(values)
    if len(collected) != len(set(collected)):
        raise ScientificRegistryFormatError(f"Duplicate {label} in registry")
