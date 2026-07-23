"""Filesystem and optional sklearn adapters for scientific model shadowing."""

from __future__ import annotations

import csv
from dataclasses import asdict
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from tinvest_signal_engine.application.scientific_model_shadow import (
    FeatureRow,
    FittedShadowEstimator,
)
from tinvest_signal_engine.domain.scientific_model_shadow import (
    SealedShadowDataset,
    ShadowModelExample,
    ShadowModelKind,
    ShadowPortfolioResult,
    ShadowStudyKind,
    ShadowStudyScope,
)


SHADOW_INPUT_SCHEMA = "scientific-model-shadow-input-v1"
SHADOW_OUTPUT_SCHEMA = "scientific-model-shadow-result-v1"
_PROSPECTIVE_PARTITION_SCHEMA = "prospective-scientific-partitions-v1"
_COMBINATION_STREAM_SCHEMA = "scientific-combination-stream-v1"


class SklearnShadowEstimatorFactory:
    """Fit deterministic models without making sklearn an inner dependency."""

    def fit(
        self,
        *,
        model_kind: ShadowModelKind,
        feature_names: tuple[str, ...],
        rows: Sequence[FeatureRow],
        labels: Sequence[int],
        seed: int,
    ) -> FittedShadowEstimator | None:
        del feature_names
        if model_kind is ShadowModelKind.BASE_RATE:
            return None
        if importlib.util.find_spec("sklearn") is None or len(set(labels)) < 2:
            return None
        import numpy as np

        if model_kind is ShadowModelKind.LOGISTIC_REGRESSION:
            from sklearn.linear_model import LogisticRegression
            from sklearn.pipeline import make_pipeline
            from sklearn.preprocessing import StandardScaler

            estimator = make_pipeline(
                StandardScaler(),
                LogisticRegression(
                    C=1.0,
                    max_iter=2_000,
                    random_state=seed,
                    solver="lbfgs",
                ),
            )
        elif model_kind is ShadowModelKind.GRADIENT_BOOSTING:
            from sklearn.ensemble import HistGradientBoostingClassifier

            estimator = HistGradientBoostingClassifier(
                learning_rate=0.05,
                max_iter=96,
                max_leaf_nodes=7,
                max_depth=3,
                min_samples_leaf=30,
                l2_regularization=1.0,
                random_state=seed,
            )
        else:
            return None
        estimator.fit(np.asarray(rows, dtype=float), np.asarray(labels, dtype=int))
        return _SklearnProbabilityEstimator(estimator)


class _SklearnProbabilityEstimator:
    def __init__(self, estimator: object) -> None:
        self._estimator = estimator

    def predict_probabilities(self, rows: Sequence[FeatureRow]) -> tuple[float, ...]:
        import numpy as np

        probabilities = self._estimator.predict_proba(  # type: ignore[attr-defined]
            np.asarray(rows, dtype=float)
        )
        return tuple(float(item[1]) for item in probabilities)


class ImmutableJsonShadowDatasetSource:
    """Read a checksummed export of sealed portfolio observations and outcomes."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def load(self) -> SealedShadowDataset:
        completion = _object(self._root / "completion.json")
        if completion.get("schema") != SHADOW_INPUT_SCHEMA:
            raise ValueError("unsupported scientific shadow input schema")
        hashes = _mapping(completion.get("hashes"), "completion.hashes")
        expected_files = ("manifest.json", "examples.jsonl")
        if set(hashes) != set(expected_files):
            raise ValueError("scientific shadow input file set is incomplete")
        for name in expected_files:
            path = self._root / name
            if not path.is_file() or _file_hash(path) != hashes[name]:
                raise ValueError(f"scientific shadow input checksum failed: {name}")
        manifest = _object(self._root / "manifest.json")
        if manifest.get("schema") != SHADOW_INPUT_SCHEMA:
            raise ValueError("scientific shadow input manifest is incompatible")
        scopes = tuple(
            _scope(_mapping(item, "manifest.scopes[]"))
            for item in _sequence(manifest.get("scopes"), "manifest.scopes")
        )
        scope_index = {scope.key: scope for scope in scopes}
        examples = tuple(
            _example(_line_object(line), scope_index)
            for line in (self._root / "examples.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        )
        dataset = SealedShadowDataset(
            dataset_fingerprint=_text(manifest, "dataset_fingerprint"),
            source_artifact_fingerprints=tuple(
                str(item)
                for item in _sequence(
                    manifest.get("source_artifact_fingerprints"),
                    "manifest.source_artifact_fingerprints",
                )
            ),
            scopes=scopes,
            examples=examples,
        )
        expected = completion.get("dataset_content_fingerprint")
        if _dataset_content_fingerprint(dataset) != expected:
            raise ValueError("scientific shadow dataset content fingerprint drifted")
        return dataset


def seal_shadow_dataset(root: str | Path, dataset: SealedShadowDataset) -> str:
    """Create or byte-verify the immutable adapter-boundary input artifact."""

    directory = Path(root)
    directory.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema": SHADOW_INPUT_SCHEMA,
        "dataset_fingerprint": dataset.dataset_fingerprint,
        "source_artifact_fingerprints": dataset.source_artifact_fingerprints,
        "scopes": dataset.scopes,
        "privacy": {
            "broker_tokens_persisted": False,
            "account_identifiers_persisted": False,
        },
    }
    examples = b"".join(
        _json_bytes(_example_payload(item)) for item in dataset.examples
    )
    payloads = {
        "manifest.json": _json_bytes(manifest),
        "examples.jsonl": examples,
    }
    for name, payload in payloads.items():
        _write_once_or_verify(directory / name, payload)
    hashes = {name: _file_hash(directory / name) for name in payloads}
    completion = {
        "schema": SHADOW_INPUT_SCHEMA,
        "hashes": hashes,
        "dataset_content_fingerprint": _dataset_content_fingerprint(dataset),
    }
    _write_once_or_verify(directory / "completion.json", _json_bytes(completion))
    return str(directory.resolve())


def build_shadow_dataset_from_sealed_portfolio(
    *,
    prospective_artifact_root: str | Path,
    combination_artifact_dir: str | Path,
) -> SealedShadowDataset:
    """Map existing H1-H17 and C1-C4 artifacts at the outer boundary.

    Only observations whose outcome is already sealed and available become
    examples. Non-events and unavailable results stay accounted for by the
    upstream causal evidence artifacts and are never synthesized here.
    """

    prospective_dirs = _prospective_run_dirs(Path(prospective_artifact_root))
    if not prospective_dirs:
        raise ValueError("no sealed prospective scientific artifacts found")
    scopes: dict[tuple[str, str, int], ShadowStudyScope] = {}
    examples: list[ShadowModelExample] = []
    dataset_fingerprints: set[str] = set()
    source_fingerprints: set[str] = set()
    for run_dir in prospective_dirs:
        manifest, partition_paths = _verified_prospective_artifact(run_dir)
        dataset_fingerprints.add(_text(manifest, "dataset_fingerprint"))
        source_fingerprints.add(_text(manifest, "report_fingerprint"))
        hypothesis_id = _text(manifest, "hypothesis")
        hypothesis_version = _text(manifest, "hypothesis_version")
        cost_model_version = _text(manifest, "cost_model_version")
        for path in partition_paths:
            payload = _sequence(json.loads(path.read_text(encoding="utf-8")), path.name)
            for raw_pair in payload:
                pair = _mapping(raw_pair, "prospective partition row")
                feature = _mapping(pair.get("feature"), "prospective feature")
                outcome = _mapping(pair.get("outcome"), "prospective outcome")
                if feature.get("observation_id") != outcome.get("observation_id"):
                    raise ValueError(
                        "prospective feature and outcome identities differ"
                    )
                horizon = int(feature["horizon_seconds"])
                effect = _prospective_effect(feature, outcome)
                unit, costs_applied = _prospective_effect_metadata(feature)
                scope = ShadowStudyScope(
                    study_id=hypothesis_id,
                    study_version=hypothesis_version,
                    study_kind=ShadowStudyKind.HYPOTHESIS,
                    horizon_seconds=horizon,
                    effect_unit=unit,
                    cost_model_version=cost_model_version,
                    costs_applied=costs_applied,
                )
                scopes[scope.key] = scope
                if effect is None:
                    continue
                examples.append(_prospective_example(scope, feature, effect))

    combination_dir = Path(combination_artifact_dir)
    combination_manifest, combination_paths, combination_fingerprint, cost_model = (
        _verified_combination_artifact(combination_dir)
    )
    dataset_fingerprints.add(_text(combination_manifest, "dataset_fingerprint"))
    source_fingerprints.add(combination_fingerprint)
    for path in combination_paths:
        payload = _object(path)
        observations = _sequence(
            payload.get("observations"), "combination.observations"
        )
        outcomes = _sequence(payload.get("outcomes"), "combination.outcomes")
        if len(observations) != len(outcomes):
            raise ValueError("combination observations and outcomes do not align")
        for raw_observation, raw_outcome in zip(observations, outcomes, strict=True):
            observation = _mapping(raw_observation, "combination observation")
            outcome = _mapping(raw_outcome, "combination outcome")
            if observation.get("observation_id") != outcome.get("observation_id"):
                raise ValueError(
                    "combination observation and outcome identities differ"
                )
            scope = ShadowStudyScope(
                study_id=_text(observation, "combination_id"),
                study_version=_text(observation, "combination_version"),
                study_kind=ShadowStudyKind.COMBINATION,
                horizon_seconds=int(observation["horizon_seconds"]),
                effect_unit="basis_points",
                cost_model_version=cost_model,
                costs_applied=True,
            )
            scopes[scope.key] = scope
            effect = outcome.get("net_directional_return_bps")
            if observation.get("decision") != "matched" or effect is None:
                continue
            examples.append(_combination_example(scope, observation, float(effect)))

    if len(dataset_fingerprints) != 1:
        raise ValueError("sealed shadow sources use different datasets")
    ordered_scopes = tuple(sorted(scopes.values(), key=lambda item: item.key))
    ordered_examples = tuple(
        sorted(
            examples,
            key=lambda item: (
                item.scope.key,
                item.trading_day,
                item.observed_at,
                item.observation_id,
            ),
        )
    )
    return SealedShadowDataset(
        dataset_fingerprint=next(iter(dataset_fingerprints)),
        source_artifact_fingerprints=tuple(sorted(source_fingerprints)),
        scopes=ordered_scopes,
        examples=ordered_examples,
    )


class ImmutableJsonShadowArtifactAdapter:
    """Persist a comparison once, with checksums and explicit no-claim metadata."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None:
        run_dir = self._run_dir(run_id)
        path = run_dir / "completion.json"
        if not path.is_file():
            return None
        payload = _object(path)
        if (
            payload.get("schema") != SHADOW_OUTPUT_SCHEMA
            or payload.get("run_id") != run_id
            or payload.get("input_fingerprint") != input_fingerprint
        ):
            return None
        hashes = _mapping(payload.get("hashes"), "completion.hashes")
        for name, expected in hashes.items():
            artifact = run_dir / str(name)
            if not artifact.is_file() or _file_hash(artifact) != expected:
                return None
        return str(run_dir.resolve())

    def persist(self, result: ShadowPortfolioResult) -> str:
        run_dir = self._run_dir(result.run_id)
        completion = run_dir / "completion.json"
        existing = self.completed_uri(result.run_id, result.input_fingerprint)
        if existing is not None:
            return existing
        if completion.exists() or (run_dir.exists() and any(run_dir.iterdir())):
            raise RuntimeError("refusing to overwrite immutable shadow comparison")
        run_dir.mkdir(parents=True, exist_ok=True)
        payloads = {
            "manifest.json": _json_bytes(
                {
                    "schema": SHADOW_OUTPUT_SCHEMA,
                    "run_id": result.run_id,
                    "input_fingerprint": result.input_fingerprint,
                    "policy_fingerprint": result.policy_fingerprint,
                    "state": result.state,
                    "causal_evidence_gate_unchanged": True,
                    "claim_allowed": False,
                    "missing_study_ids": result.missing_study_ids,
                }
            ),
            "model-results.json": _json_bytes(result),
            "leaderboard.csv": _csv_bytes(_leaderboard(result)),
            "selection.csv": _csv_bytes(_selection_rows(result)),
            "calibration.csv": _csv_bytes(_calibration_rows(result)),
            "report.md": _report(result).encode("utf-8"),
        }
        for name, payload in payloads.items():
            _write_once_or_verify(run_dir / name, payload)
        hashes = {name: _file_hash(run_dir / name) for name in payloads}
        _write_once_or_verify(
            completion,
            _json_bytes(
                {
                    "schema": SHADOW_OUTPUT_SCHEMA,
                    "run_id": result.run_id,
                    "input_fingerprint": result.input_fingerprint,
                    "hashes": hashes,
                }
            ),
        )
        return str(run_dir.resolve())

    def _run_dir(self, run_id: str) -> Path:
        if not run_id.startswith("sha256:") or len(run_id) != 71:
            raise ValueError("shadow run id must be a sha256 fingerprint")
        return self._root / run_id.removeprefix("sha256:")


def _leaderboard(result: ShadowPortfolioResult) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for study in result.results:
        for model in study.models:
            metrics = model.metrics
            validation = model.validation_metrics
            rows.append(
                {
                    "study_id": study.scope.study_id,
                    "study_version": study.scope.study_version,
                    "study_kind": study.scope.study_kind.value,
                    "horizon_seconds": study.scope.horizon_seconds,
                    "model": model.model_kind.value,
                    "state": model.state.value,
                    "selected": model.model_kind is study.selected_model_kind,
                    "action_probability_threshold": (
                        model.action_probability_threshold
                    ),
                    "holdout_examples": study.holdout_examples,
                    "holdout_positive_stability_blocks": (
                        model.holdout_positive_stability_blocks
                    ),
                    "holdout_total_stability_blocks": (
                        model.holdout_total_stability_blocks
                    ),
                    "validation_coverage": (
                        validation.coverage if validation else None
                    ),
                    "validation_useful_rate_when_acted": (
                        validation.useful_rate_when_acted if validation else None
                    ),
                    "validation_mean_cost_adjusted_effect": (
                        validation.mean_effect_when_acted if validation else None
                    ),
                    "accuracy": metrics.accuracy if metrics else None,
                    "coverage": metrics.coverage if metrics else None,
                    "abstention_rate": metrics.abstention_rate if metrics else None,
                    "useful_rate_when_acted": (
                        metrics.useful_rate_when_acted if metrics else None
                    ),
                    "mean_cost_adjusted_effect": (
                        metrics.mean_effect_when_acted if metrics else None
                    ),
                    "effect_unit": study.scope.effect_unit,
                    "cost_model_version": study.scope.cost_model_version,
                    "costs_applied": study.scope.costs_applied,
                    "brier_score": metrics.brier_score if metrics else None,
                    "expected_calibration_error": (
                        metrics.expected_calibration_error if metrics else None
                    ),
                    "reason_codes": "|".join(model.reason_codes),
                }
            )
    return rows


def _selection_rows(result: ShadowPortfolioResult) -> list[dict[str, object]]:
    return [
        {
            "study_id": study.scope.study_id,
            "study_version": study.scope.study_version,
            "study_kind": study.scope.study_kind.value,
            "horizon_seconds": study.scope.horizon_seconds,
            "selection_state": study.selection_state.value,
            "selected_model": (
                study.selected_model_kind.value
                if study.selected_model_kind is not None
                else None
            ),
            "selection_reason_codes": "|".join(study.selection_reason_codes),
            "causal_evidence_gate_unchanged": True,
            "claim_allowed": False,
        }
        for study in result.results
    ]


def _calibration_rows(result: ShadowPortfolioResult) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for study in result.results:
        for model in study.models:
            if model.metrics is None:
                continue
            for item in model.metrics.calibration:
                rows.append(
                    {
                        "study_id": study.scope.study_id,
                        "horizon_seconds": study.scope.horizon_seconds,
                        "model": model.model_kind.value,
                        "lower_probability": item.lower_probability,
                        "upper_probability": item.upper_probability,
                        "observations": item.observations,
                        "mean_probability": item.mean_probability,
                        "observed_useful_rate": item.observed_useful_rate,
                    }
                )
    return rows


def _report(result: ShadowPortfolioResult) -> str:
    lines = [
        "# Теневое сравнение моделей научного портфеля",
        "",
        "Сравнение использует только запечатанные причинные признаки и более поздние результаты. Оно не меняет научный шлюз, не включает гипотезы и не создаёт продуктовых обещаний.",
        "",
        f"- Состояние: `{result.state.value}`",
        f"- Запуск: `{result.run_id}`",
        f"- Отпечаток входа: `{result.input_fingerprint}`",
        f"- Недостающие исследования: `{', '.join(result.missing_study_ids) or 'нет'}`",
        "",
        "Выбор выполняется отдельно для каждого исследования. Сложная модель "
        "заменяет более простую только при устойчивом улучшении на проверочной "
        "и окончательной частях. Если ни один вариант не проходит требования, "
        "решение — воздержаться.",
        "",
        "| Исследование | Горизонт | Решение | Выбранная модель | Причина |",
        "|---|---:|---|---|---|",
    ]
    for row in _selection_rows(result):
        lines.append(
            "| "
            + " | ".join(
                (
                    str(row["study_id"]),
                    str(row["horizon_seconds"]),
                    _selection_label(str(row["selection_state"])),
                    _model_label(row["selected_model"]),
                    _reason_labels(str(row["selection_reason_codes"])),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "| Исследование | Горизонт | Модель | Выбрана | Порог действия | "
            "Охват на проверке | Полезны на проверке | Охват на окончательной "
            "части | Устойчивые временные блоки | Воздержание | Полезны при "
            "действии | Средний результат после издержек |",
            "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for row in _leaderboard(result):
        lines.append(
            "| "
            + " | ".join(
                (
                    str(row["study_id"]),
                    str(row["horizon_seconds"]),
                    _model_label(row["model"]),
                    "да" if row["selected"] else "нет",
                    _number(row["action_probability_threshold"]),
                    _number(row["validation_coverage"], percent=True),
                    _number(row["validation_useful_rate_when_acted"], percent=True),
                    _number(row["coverage"], percent=True),
                    (
                        f"{row['holdout_positive_stability_blocks']}/"
                        f"{row['holdout_total_stability_blocks']}"
                    ),
                    _number(row["abstention_rate"], percent=True),
                    _number(row["useful_rate_when_acted"], percent=True),
                    _number(row["mean_cost_adjusted_effect"]),
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "`blocked_by_data` означает, что данных, торговых дней, классов или доступной библиотеки недостаточно. Такой результат не заменяется предположением.",
            "",
        )
    )
    return "\n".join(lines)


def _model_label(value: object) -> str:
    labels = {
        "scientific_rule": "научное правило",
        "base_rate": "сглаженная вероятностная оценка",
        "logistic_regression": "регуляризованная логистическая модель",
        "gradient_boosting": "неглубокое усиление деревьев",
        None: "нет",
    }
    return labels.get(value, str(value))


def _selection_label(value: str) -> str:
    return "выбрана модель" if value == "selected" else "воздержаться"


def _reason_labels(value: str) -> str:
    labels = {
        "simplest_stable_candidate_selected": "выбран простейший устойчивый вариант",
        "complexity_selected_after_stable_improvement": (
            "сложность оправдана устойчивым улучшением"
        ),
        "no_model_stable_on_validation_and_holdout": (
            "нет устойчивого варианта на проверочной и окончательной частях"
        ),
        "model_comparison_blocked": "сравнение заблокировано данными",
    }
    return ", ".join(labels.get(item, item) for item in value.split("|") if item)


def _prospective_run_dirs(root: Path) -> tuple[Path, ...]:
    candidates = (
        (root,)
        if (root / "manifest.json").is_file()
        else tuple(path.parent for path in sorted(root.glob("*/manifest.json")))
    )
    result = []
    for path in candidates:
        try:
            manifest = _object(path / "manifest.json")
        except ValueError:
            continue
        if manifest.get("schema") == _PROSPECTIVE_PARTITION_SCHEMA:
            result.append(path)
    return tuple(result)


def _verified_prospective_artifact(
    run_dir: Path,
) -> tuple[Mapping[str, Any], tuple[Path, ...]]:
    manifest_path = run_dir / "manifest.json"
    manifest = _object(manifest_path)
    completion = _object(run_dir / "completion.json")
    if (
        manifest.get("schema") != _PROSPECTIVE_PARTITION_SCHEMA
        or completion.get("schema") != _PROSPECTIVE_PARTITION_SCHEMA
    ):
        raise ValueError("unsupported prospective scientific artifact")
    if _file_hash(manifest_path) != completion.get("manifest_hash"):
        raise ValueError("prospective scientific manifest checksum failed")
    hashes = _mapping(completion.get("partition_hashes"), "partition_hashes")
    paths = tuple(run_dir / str(relative) for relative in sorted(hashes))
    for path, expected in zip(
        paths, (hashes[key] for key in sorted(hashes)), strict=True
    ):
        if not path.is_file() or _file_hash(path) != expected:
            raise ValueError("prospective scientific partition checksum failed")
    return manifest, paths


def _verified_combination_artifact(
    run_dir: Path,
) -> tuple[Mapping[str, Any], tuple[Path, ...], str, str]:
    manifest = _object(run_dir / "manifest.json")
    completion = _object(run_dir / "completion.json")
    if (
        manifest.get("schema") != _COMBINATION_STREAM_SCHEMA
        or completion.get("schema") != _COMBINATION_STREAM_SCHEMA
    ):
        raise ValueError("unsupported scientific combination artifact")
    hashes = _mapping(completion.get("hashes"), "completion.hashes")
    for relative, expected in hashes.items():
        path = run_dir / str(relative)
        if not path.is_file() or _file_hash(path) != expected:
            raise ValueError("scientific combination checksum failed")
    fingerprint = _text(completion, "artifact_fingerprint")
    expected_fingerprint = (
        "sha256:"
        + sha256(
            json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
    )
    if fingerprint != expected_fingerprint:
        raise ValueError("scientific combination artifact fingerprint drifted")
    cost_model = _text(completion, "cost_model_version")
    partitions = tuple(
        run_dir / str(relative)
        for relative in sorted(hashes)
        if str(relative).startswith("partitions/")
    )
    return manifest, partitions, fingerprint, cost_model


def _prospective_effect(
    feature: Mapping[str, Any], outcome: Mapping[str, Any]
) -> float | None:
    if feature.get("decision") != "matched" or outcome.get("available") is not True:
        return None
    measurements = {
        _text(_mapping(item, "outcome measurement"), "name"): float(
            _mapping(item, "outcome measurement")["value"]
        )
        for item in _sequence(outcome.get("measurements"), "outcome.measurements")
    }
    target = _text(feature, "target")
    if target == "forward_return":
        return measurements.get("cost_adjusted_directional_return")
    if target == "future_variance_uplift":
        value = measurements.get("future_variance_uplift")
        return value * 10_000.0 if value is not None else None
    if target == "future_realized_variance":
        model = measurements.get("har_qlike")
        ewma = measurements.get("ewma_qlike")
        phase = measurements.get("phase_qlike")
        if model is None or ewma is None or phase is None:
            return None
        return (min(ewma, phase) - model) * 10_000.0
    return None


def _prospective_effect_metadata(feature: Mapping[str, Any]) -> tuple[str, bool]:
    target = _text(feature, "target")
    if target == "forward_return":
        return "basis_points", True
    if target == "future_variance_uplift":
        return "variance_uplift_ratio_x_10000", False
    return "qlike_improvement_x_10000", False


def _prospective_example(
    scope: ShadowStudyScope,
    feature: Mapping[str, Any],
    effect: float,
) -> ShadowModelExample:
    observed_at = datetime.fromisoformat(_text(feature, "observed_at"))
    raw_values = _sequence(feature.get("feature_values"), "feature.feature_values")
    values = {
        _text(_mapping(item, "feature metric"), "name"): float(
            _mapping(item, "feature metric")["value"]
        )
        for item in raw_values
    }
    values["sealed_expected_direction"] = float(feature.get("expected_direction", 0))
    values["sealed_observed_minute"] = float(observed_at.hour * 60 + observed_at.minute)
    forecast = feature.get("forecast")
    if isinstance(forecast, Mapping):
        values["sealed_forecast"] = float(forecast["value"])
    return ShadowModelExample(
        scope=scope,
        observation_id=_text(feature, "observation_id"),
        instrument_id=_text(feature, "ticker"),
        trading_day=date.fromisoformat(_text(feature, "trading_day")),
        observed_at=observed_at,
        feature_max_observed_at=datetime.fromisoformat(
            _text(feature, "feature_max_observed_at")
        ),
        feature_values=tuple(sorted(values.items())),
        effect_value=effect,
    )


def _combination_example(
    scope: ShadowStudyScope,
    observation: Mapping[str, Any],
    effect: float,
) -> ShadowModelExample:
    observed_at = datetime.fromisoformat(_text(observation, "observed_at"))
    values = {
        "sealed_expected_direction": float(observation.get("expected_direction", 0)),
        "sealed_observed_minute": float(observed_at.hour * 60 + observed_at.minute),
    }
    components = _sequence(observation.get("components"), "combination.components")
    for raw in components:
        component = _mapping(raw, "combination component")
        key = _text(component, "requirement_key").replace("@", "_")
        component_at = datetime.fromisoformat(_text(component, "observed_at"))
        values[f"{key}.age_seconds"] = max(
            0.0, (observed_at - component_at).total_seconds()
        )
        values[f"{key}.expected_direction"] = float(
            component.get("expected_direction", 0)
        )
        values[f"{key}.matched"] = float(component.get("decision") == "matched")
    max_used = observation.get("max_used_observed_at")
    feature_max = (
        datetime.fromisoformat(str(max_used)) if max_used is not None else observed_at
    )
    return ShadowModelExample(
        scope=scope,
        observation_id=_text(observation, "observation_id"),
        instrument_id=_text(observation, "primary_scope"),
        trading_day=date.fromisoformat(_text(observation, "trading_day")),
        observed_at=observed_at,
        feature_max_observed_at=feature_max,
        feature_values=tuple(sorted(values.items())),
        effect_value=effect,
    )


def _scope(value: Mapping[str, Any]) -> ShadowStudyScope:
    return ShadowStudyScope(
        study_id=_text(value, "study_id"),
        study_version=_text(value, "study_version"),
        study_kind=ShadowStudyKind(_text(value, "study_kind")),
        horizon_seconds=int(value["horizon_seconds"]),
        effect_unit=_text(value, "effect_unit"),
        cost_model_version=_text(value, "cost_model_version"),
        costs_applied=bool(value["costs_applied"]),
    )


def _example(
    value: Mapping[str, Any],
    scopes: Mapping[tuple[str, str, int], ShadowStudyScope],
) -> ShadowModelExample:
    scope_key = (
        _text(value, "study_id"),
        _text(value, "study_version"),
        int(value["horizon_seconds"]),
    )
    try:
        scope = scopes[scope_key]
    except KeyError as exc:
        raise ValueError("shadow example references an unknown scope") from exc
    features = _sequence(value.get("feature_values"), "example.feature_values")
    parsed_features: list[tuple[str, float]] = []
    for item in features:
        if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
            raise ValueError("shadow feature value must be a name/value pair")
        parsed_features.append((item[0], float(item[1])))
    return ShadowModelExample(
        scope=scope,
        observation_id=_text(value, "observation_id"),
        instrument_id=_text(value, "instrument_id"),
        trading_day=date.fromisoformat(_text(value, "trading_day")),
        observed_at=datetime.fromisoformat(_text(value, "observed_at")),
        feature_max_observed_at=datetime.fromisoformat(
            _text(value, "feature_max_observed_at")
        ),
        feature_values=tuple(parsed_features),
        effect_value=float(value["effect_value"]),
    )


def _example_payload(item: ShadowModelExample) -> Mapping[str, Any]:
    return {
        "study_id": item.scope.study_id,
        "study_version": item.scope.study_version,
        "horizon_seconds": item.scope.horizon_seconds,
        "observation_id": item.observation_id,
        "instrument_id": item.instrument_id,
        "trading_day": item.trading_day,
        "observed_at": item.observed_at,
        "feature_max_observed_at": item.feature_max_observed_at,
        "feature_values": item.feature_values,
        "effect_value": item.effect_value,
    }


def _dataset_content_fingerprint(dataset: SealedShadowDataset) -> str:
    payload = {
        "dataset_fingerprint": dataset.dataset_fingerprint,
        "source_artifact_fingerprints": dataset.source_artifact_fingerprints,
        "scopes": dataset.scopes,
        "examples": [_example_payload(item) for item in dataset.examples],
    }
    return "sha256:" + sha256(_json_bytes(payload)).hexdigest()


def _csv_bytes(rows: list[dict[str, object]]) -> bytes:
    if not rows:
        return b""
    from io import StringIO

    handle = StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=tuple(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return handle.getvalue().encode("utf-8")


def _number(value: object, *, percent: bool = False) -> str:
    if value is None:
        return "—"
    number = float(value)
    return f"{number * 100:.1f}%" if percent else f"{number:.3f}"


def _object(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        raise ValueError(f"required shadow artifact is missing: {path.name}")
    return _line_object(path.read_text(encoding="utf-8"))


def _line_object(value: str) -> Mapping[str, Any]:
    payload = json.loads(value)
    if not isinstance(payload, Mapping):
        raise ValueError("shadow JSON value must be an object")
    return payload


def _mapping(value: object, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{location} must be an object")
    return value


def _sequence(value: object, location: str) -> tuple[Any, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{location} must be a list")
    return tuple(value)


def _text(value: Mapping[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"{key} must be non-empty text")
    return result


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            default=_json_default,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _json_default(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    raise TypeError(f"cannot encode shadow artifact value {type(value)!r}")


def _write_once_or_verify(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError(f"immutable shadow artifact differs: {path.name}")
        return
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _file_hash(path: Path) -> str:
    return "sha256:" + sha256(path.read_bytes()).hexdigest()
