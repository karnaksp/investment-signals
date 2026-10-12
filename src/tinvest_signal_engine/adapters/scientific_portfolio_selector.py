"""Filesystem adapters for the selective scientific portfolio research run."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timedelta
from enum import Enum
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from tinvest_signal_engine.adapters.scientific_model_shadow import (
    ImmutableJsonShadowDatasetSource,
)
from tinvest_signal_engine.domain.scientific_model_shadow import (
    SealedShadowDataset,
    ShadowModelExample,
)
from tinvest_signal_engine.domain.scientific_portfolio_selector import (
    PortfolioAction,
    PortfolioSelectorExample,
    ScientificPortfolioSelectorResult,
)


PORTFOLIO_REPORT_SCHEMA = "scientific-portfolio-selector-report-v1"
PORTFOLIO_REPORT_VERSION = "1.0.0"


class SealedScientificPortfolioSelectorExampleSource:
    """Map checksummed scientific observation/outcome exports to selector rows."""

    def __init__(
        self,
        root: str | Path,
        *,
        minimum_absolute_effect: float = 0.0,
    ) -> None:
        if minimum_absolute_effect < 0.0:
            raise ValueError("minimum absolute effect must not be negative")
        self._source = ImmutableJsonShadowDatasetSource(root)
        self._minimum_absolute_effect = minimum_absolute_effect

    def load(self) -> tuple[PortfolioSelectorExample, ...]:
        return selector_examples_from_shadow_dataset(
            self._source.load(),
            minimum_absolute_effect=self._minimum_absolute_effect,
        )


def selector_examples_from_shadow_dataset(
    dataset: SealedShadowDataset,
    *,
    minimum_absolute_effect: float = 0.0,
) -> tuple[PortfolioSelectorExample, ...]:
    """Create leak-free multiclass rows from already sealed result artifacts.

    For directional rules, a positive sealed effect confirms the expected
    direction and a negative effect identifies the inverse direction.  For
    non-directional studies, a positive effect becomes ``risk``.  Effects
    inside the configured materiality band become ``abstain``.
    """

    if minimum_absolute_effect < 0.0:
        raise ValueError("minimum absolute effect must not be negative")
    cost_models = {item.scope.cost_model_version for item in dataset.examples}
    if len(cost_models) != 1:
        raise ValueError("selector source requires one cost model version")
    raw_feature_names = tuple(
        sorted(
            {
                name
                for item in dataset.examples
                for name, _ in item.feature_values
            }
        )
    )
    study_feature_names = tuple(
        f"selector.study={scope.study_id}@{scope.study_version}"
        for scope in dataset.scopes
    )
    feature_names = tuple(
        sorted(
            (
                *raw_feature_names,
                *(f"selector.missing={name}" for name in raw_feature_names),
                *study_feature_names,
                "selector.horizon_seconds",
                "selector.study_kind=combination",
                "selector.study_kind=hypothesis",
            )
        )
    )
    source_fingerprints = tuple(sorted(dataset.source_artifact_fingerprints))
    return tuple(
        _selector_example(
            item,
            feature_names=feature_names,
            raw_feature_names=raw_feature_names,
            source_fingerprints=source_fingerprints,
            minimum_absolute_effect=minimum_absolute_effect,
        )
        for item in sorted(
            dataset.examples,
            key=lambda value: (
                value.trading_day,
                value.observed_at,
                value.observation_id,
            ),
        )
    )


def _selector_example(
    item: ShadowModelExample,
    *,
    feature_names: tuple[str, ...],
    raw_feature_names: tuple[str, ...],
    source_fingerprints: tuple[str, ...],
    minimum_absolute_effect: float,
) -> PortfolioSelectorExample:
    raw = dict(item.feature_values)
    direction = int(round(raw.get("sealed_expected_direction", 0.0)))
    if direction not in {-1, 0, 1}:
        raise ValueError("sealed expected direction must be -1, 0 or 1")
    if direction and not item.scope.costs_applied:
        raise ValueError("directional selector examples require sealed costs")
    sealed_action = _directional_action(direction)
    target_action = _target_action(
        direction=direction,
        effect=item.effect_value,
        minimum_absolute_effect=minimum_absolute_effect,
    )
    study_feature = (
        f"selector.study={item.scope.study_id}@{item.scope.study_version}"
    )
    kind_feature = f"selector.study_kind={item.scope.study_kind.value}"
    values: dict[str, float] = {}
    for name in feature_names:
        if name in raw:
            values[name] = raw[name]
        elif name.startswith("selector.missing="):
            raw_name = name.removeprefix("selector.missing=")
            values[name] = float(raw_name not in raw)
        elif name == "selector.horizon_seconds":
            values[name] = float(item.scope.horizon_seconds)
        elif name == study_feature or name == kind_feature:
            values[name] = 1.0
        else:
            values[name] = 0.0
    missing_features = tuple(
        name for name in raw_feature_names if name not in raw
    )
    if any(values[f"selector.missing={name}"] != 1.0 for name in missing_features):
        raise AssertionError("selector feature missingness mapping drifted")
    session_bucket = item.observed_at.hour
    return PortfolioSelectorExample(
        event_id=item.observation_id,
        instrument_id=item.instrument_id,
        source_study_ids=(
            f"{item.scope.study_id}@{item.scope.study_version}",
        ),
        source_artifact_fingerprints=source_fingerprints,
        trading_day=item.trading_day,
        observed_at=item.observed_at,
        feature_max_observed_at=item.feature_max_observed_at,
        label_observed_at=item.observed_at
        + timedelta(seconds=item.scope.horizon_seconds),
        horizon_seconds=item.scope.horizon_seconds,
        sealed_action=sealed_action,
        target_action=target_action,
        probability_stratum=(
            f"{item.scope.study_id}@{item.scope.study_version}"
            f":h{item.scope.horizon_seconds}:hour{session_bucket:02d}"
        ),
        feature_values=tuple(sorted(values.items())),
        cost_model_version=item.scope.cost_model_version,
    )


def _directional_action(direction: int) -> PortfolioAction:
    if direction > 0:
        return PortfolioAction.UP
    if direction < 0:
        return PortfolioAction.DOWN
    return PortfolioAction.RISK


def _target_action(
    *,
    direction: int,
    effect: float,
    minimum_absolute_effect: float,
) -> PortfolioAction:
    if abs(effect) <= minimum_absolute_effect:
        return PortfolioAction.ABSTAIN
    if direction == 0:
        return (
            PortfolioAction.RISK
            if effect > minimum_absolute_effect
            else PortfolioAction.ABSTAIN
        )
    expected = _directional_action(direction)
    if effect > minimum_absolute_effect:
        return expected
    return (
        PortfolioAction.DOWN
        if expected is PortfolioAction.UP
        else PortfolioAction.UP
    )


class ImmutableJsonScientificPortfolioReportAdapter:
    """Persist a checksummed, versioned report without mutable partial state."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None:
        run_dir = self._run_dir(run_id)
        completion_path = run_dir / "completion.json"
        if not completion_path.is_file():
            return None
        try:
            completion = _object(completion_path)
        except (OSError, ValueError, TypeError):
            return None
        if (
            completion.get("schema") != PORTFOLIO_REPORT_SCHEMA
            or completion.get("report_version") != PORTFOLIO_REPORT_VERSION
            or completion.get("run_id") != run_id
            or completion.get("input_fingerprint") != input_fingerprint
        ):
            return None
        hashes = completion.get("hashes")
        if not isinstance(hashes, Mapping) or set(hashes) != {
            "manifest.json",
            "report.json",
        }:
            return None
        for name, expected in hashes.items():
            path = run_dir / str(name)
            if not path.is_file() or _file_hash(path) != expected:
                return None
        return str(run_dir.resolve())

    def persist(self, result: ScientificPortfolioSelectorResult) -> str:
        existing = self.completed_uri(result.run_id, result.input_fingerprint)
        if existing is not None:
            return existing
        run_dir = self._run_dir(result.run_id)
        if run_dir.exists() and any(run_dir.iterdir()):
            raise RuntimeError("refusing to overwrite incomplete portfolio report")
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema": PORTFOLIO_REPORT_SCHEMA,
            "report_version": PORTFOLIO_REPORT_VERSION,
            "run_id": result.run_id,
            "input_fingerprint": result.input_fingerprint,
            "policy_fingerprint": result.policy_fingerprint,
            "state": result.state.value,
            "selected_model": result.selected_model.value,
            "causal_evidence_gate_unchanged": (
                result.causal_evidence_gate_unchanged
            ),
            "claim_allowed": result.claim_allowed,
            "source": "sealed_scientific_observation_result_artifacts",
            "artifacts": {"report": "report.json"},
        }
        report = _report_payload(result)
        payloads = {
            "manifest.json": _json_bytes(manifest),
            "report.json": _json_bytes(report),
        }
        for name, payload in payloads.items():
            _write_once(run_dir / name, payload)
        hashes = {name: _file_hash(run_dir / name) for name in payloads}
        _write_once(
            run_dir / "completion.json",
            _json_bytes(
                {
                    "schema": PORTFOLIO_REPORT_SCHEMA,
                    "report_version": PORTFOLIO_REPORT_VERSION,
                    "run_id": result.run_id,
                    "input_fingerprint": result.input_fingerprint,
                    "hashes": hashes,
                }
            ),
        )
        return str(run_dir.resolve())

    def _run_dir(self, run_id: str) -> Path:
        if not run_id.startswith("sha256:") or len(run_id) != 71:
            raise ValueError("portfolio selector run id must be a sha256 fingerprint")
        return self._root / run_id.removeprefix("sha256:")


def _report_payload(result: ScientificPortfolioSelectorResult) -> dict[str, Any]:
    models = []
    for evaluation in result.evaluations:
        models.append(
            {
                "model": evaluation.model_kind.value,
                "eligible_on_validation": evaluation.eligible,
                "reason_codes": evaluation.reason_codes,
                "selected_confidence_threshold": (
                    evaluation.selected_confidence_threshold
                ),
                "validation_accuracy_lift": evaluation.validation_accuracy_lift,
                "holdout_accuracy_lift": evaluation.holdout_accuracy_lift,
                "positive_walk_forward_folds": (
                    evaluation.positive_walk_forward_folds
                ),
                "total_walk_forward_folds": evaluation.total_walk_forward_folds,
                "validation": asdict(evaluation.validation_metrics),
                "holdout": asdict(evaluation.holdout_metrics),
                "walk_forward": tuple(asdict(item) for item in evaluation.walk_forward),
                "explanation": asdict(evaluation.explanation),
            }
        )
    selected = next(
        item
        for item in result.evaluations
        if item.model_kind is result.selected_model
    )
    return {
        "schema": PORTFOLIO_REPORT_SCHEMA,
        "report_version": PORTFOLIO_REPORT_VERSION,
        "run_id": result.run_id,
        "input_fingerprint": result.input_fingerprint,
        "policy_fingerprint": result.policy_fingerprint,
        "state": result.state.value,
        "reason_codes": result.reason_codes,
        "selected_model": result.selected_model.value,
        "summary": {
            "examples": result.examples,
            "trading_days": result.trading_days,
            "holdout_accuracy_when_acted": (
                selected.holdout_metrics.accuracy_when_acted
            ),
            "holdout_coverage": selected.holdout_metrics.coverage,
            "holdout_abstention_rate": selected.holdout_metrics.abstention_rate,
            "holdout_expected_calibration_error": (
                selected.holdout_metrics.expected_calibration_error
            ),
        },
        "split": asdict(result.split),
        "models": models,
        "decisions": tuple(asdict(item) for item in result.holdout_decisions),
        "causal_evidence_gate_unchanged": result.causal_evidence_gate_unchanged,
        "claim_allowed": result.claim_allowed,
    }


def _object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return payload


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
        + "\n"
    ).encode("utf-8")


def _json_default(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _write_once(path: Path, payload: bytes) -> None:
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise RuntimeError(f"refusing to overwrite immutable artifact {path}")
