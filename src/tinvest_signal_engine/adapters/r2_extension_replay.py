"""Immutable fail-closed replay artifacts for the causal H10/H11 R2 engine."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from enum import Enum
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    R2ExtensionReport,
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


@dataclass(frozen=True, slots=True)
class R2ExtensionReplayArtifact:
    artifact_uri: str
    artifact_fingerprint: str
    evidence: tuple[Mapping[str, Any], ...]


class R2ExtensionReplayArtifactAdapter:
    """Persist causal observations while refusing to manufacture an evidence gate.

    R2 currently has causal features and sealed outcomes, but no independently
    assessed matched-control evidence bundle.  The adapter therefore exposes
    exact registered horizons and diagnostics, while the claim remains
    ``blocked_by_data`` until that gate is implemented.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def save(
        self,
        report: R2ExtensionReport,
        requested_hypotheses: Sequence[R2ExtensionHypothesis],
        *,
        cost_model_version: str,
        blocking_reason_codes: Sequence[str],
    ) -> R2ExtensionReplayArtifact:
        selected = tuple(sorted(set(requested_hypotheses), key=lambda item: item.value))
        reasons = tuple(sorted(set(blocking_reason_codes)))
        if not selected:
            raise ValueError("at least one R2 hypothesis is required")
        if not cost_model_version.strip():
            raise ValueError("cost model version must not be empty")
        if not reasons or any(not item.strip() for item in reasons):
            raise ValueError("R2 replay must name its fail-closed reason")
        artifact_fingerprint = _fingerprint(
            {
                "blocking_reason_codes": reasons,
                "cost_model_version": cost_model_version,
                "report_fingerprint": report.report_fingerprint,
                "selected_hypotheses": tuple(item.value for item in selected),
            }
        )
        evidence = tuple(
            _blocked_evidence(
                report,
                hypothesis,
                artifact_fingerprint=artifact_fingerprint,
                cost_model_version=cost_model_version,
                blocking_reason_codes=reasons,
            )
            for hypothesis in selected
        )
        run_dir = self._root / artifact_fingerprint.removeprefix("sha256:")
        manifest = {
            "schema_version": 1,
            "kind": "causal_h10_h11_r2_replay",
            "portfolio_version": report.portfolio_version,
            "dataset_fingerprint": report.dataset_fingerprint,
            "request_fingerprint": report.request_fingerprint,
            "report_fingerprint": report.report_fingerprint,
            "artifact_fingerprint": artifact_fingerprint,
            "cost_model_version": cost_model_version,
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
        max(values).astimezone(timezone.utc).isoformat()
        if values
        else datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()
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
