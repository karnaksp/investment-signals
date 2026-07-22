"""Outer machine-learning and filesystem adapters for selective research."""

from __future__ import annotations

import csv
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import warnings
from typing import Any, Iterable, Mapping, Sequence

from tinvest_signal_engine.application.selective_hypothesis_policy import (
    FeatureRow,
    FittedProbabilityEstimator,
    SelectivePortfolioResult,
)
from tinvest_signal_engine.domain.selective_hypothesis_policy import SelectiveModelKind


class SklearnLightgbmEstimatorFactory:
    """Create deterministic tabular estimators when optional packages exist."""

    def available_model_kinds(self) -> tuple[SelectiveModelKind, ...]:
        return (
            SelectiveModelKind.LOGISTIC_REGRESSION,
            SelectiveModelKind.GRADIENT_BOOSTED_TREES,
        )

    def fit(
        self,
        *,
        model_kind: SelectiveModelKind,
        feature_names: tuple[str, ...],
        rows: Sequence[FeatureRow],
        labels: Sequence[int],
        seed: int,
    ) -> FittedProbabilityEstimator | None:
        del feature_names
        dependency = (
            "sklearn"
            if model_kind is SelectiveModelKind.LOGISTIC_REGRESSION
            else "lightgbm"
        )
        if importlib.util.find_spec(dependency) is None:
            return None
        import numpy as np

        if len(set(labels)) < 2:
            return None
        if model_kind is SelectiveModelKind.LOGISTIC_REGRESSION:
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
        elif model_kind is SelectiveModelKind.GRADIENT_BOOSTED_TREES:
            from lightgbm import LGBMClassifier

            estimator = LGBMClassifier(
                objective="binary",
                n_estimators=96,
                learning_rate=0.04,
                num_leaves=4,
                max_depth=2,
                min_child_samples=50,
                subsample=1.0,
                colsample_bytree=1.0,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=seed,
                deterministic=True,
                force_col_wise=True,
                n_jobs=1,
                verbosity=-1,
            )
        else:
            return None
        estimator.fit(np.asarray(rows, dtype=float), np.asarray(labels, dtype=int))
        return _ProbabilityEstimator(estimator)


class _ProbabilityEstimator:
    def __init__(self, estimator: object) -> None:
        self._estimator = estimator

    def predict_probabilities(self, rows: Sequence[FeatureRow]) -> tuple[float, ...]:
        import numpy as np

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="X does not have valid feature names",
                category=UserWarning,
            )
            probabilities = self._estimator.predict_proba(  # type: ignore[attr-defined]
                np.asarray(rows, dtype=float)
            )
        return tuple(float(row[1]) for row in probabilities)


class JsonSelectiveResearchArtifactAdapter:
    """Write immutable, resumable and human-readable research artifacts."""

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def completed_uri(self, run_id: str, input_fingerprint: str) -> str | None:
        marker = self._output_dir / run_id / "complete.json"
        if not marker.is_file():
            return None
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if (
            payload.get("status") != "completed"
            or payload.get("run_id") != run_id
            or payload.get("input_fingerprint") != input_fingerprint
        ):
            return None
        checksums = payload.get("artifact_checksums")
        if not isinstance(checksums, dict) or not checksums:
            return None
        for name, expected in checksums.items():
            artifact = marker.parent / str(name)
            if not artifact.is_file() or _file_sha256(artifact) != expected:
                return None
        return str(marker.parent.resolve())

    def persist(self, result: SelectivePortfolioResult) -> str:
        run_dir = self._output_dir / result.run_id
        marker = run_dir / "complete.json"
        if marker.is_file():
            existing = json.loads(marker.read_text(encoding="utf-8"))
            if (
                existing.get("run_id") == result.run_id
                and existing.get("input_fingerprint") == result.input_fingerprint
                and existing.get("policy_fingerprint") == result.policy_fingerprint
                and self.completed_uri(result.run_id, result.input_fingerprint)
                is not None
            ):
                return str(run_dir.resolve())
            raise RuntimeError(f"refusing to overwrite immutable research run {result.run_id}")
        if run_dir.exists() and any(run_dir.iterdir()):
            raise RuntimeError(f"refusing to overwrite incomplete research run {result.run_id}")
        run_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "schema_version": 1,
            "kind": "selective_h3_h4_policy_research",
            "source": "existing_local_parquet_cache_no_download",
            "run_id": result.run_id,
            "input_fingerprint": result.input_fingerprint,
            "policy_fingerprint": result.policy_fingerprint,
            "examples": result.examples,
            "studies": len(result.results),
            "artifacts": {
                "results": "model-results.json",
                "leaderboard": "leaderboard.csv",
                "report": "report.md",
            },
        }
        _atomic_json(run_dir / "manifest.json", manifest)
        _atomic_json(run_dir / "model-results.json", result.results)
        _atomic_csv(run_dir / "leaderboard.csv", _leaderboard_rows(result))
        _atomic_text(run_dir / "report.md", _report(result))
        artifact_checksums = {
            name: _file_sha256(run_dir / name)
            for name in (
                "manifest.json",
                "model-results.json",
                "leaderboard.csv",
                "report.md",
            )
        }
        _atomic_json(
            marker,
            {
                "status": "completed",
                "run_id": result.run_id,
                "input_fingerprint": result.input_fingerprint,
                "policy_fingerprint": result.policy_fingerprint,
                "artifact_checksums": artifact_checksums,
            },
        )
        return str(run_dir.resolve())


def _leaderboard_rows(result: SelectivePortfolioResult) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for study in result.results:
        rows.append(
            {
                "hypothesis_id": study.hypothesis_id,
                "horizon_seconds": study.horizon_seconds,
                "train_examples": study.train_examples,
                "tune_examples": study.tune_examples,
                "holdout_examples": study.holdout_examples,
                "tune_selected_model": study.tune_selected_model.value,
                "probability_threshold": study.tune_selected_threshold,
                "holdout_rule_useful_rate": study.holdout_rule_metrics.useful_rate_when_acted,
                "holdout_selected_useful_rate": study.holdout_selected_metrics.useful_rate_when_acted,
                "holdout_selected_coverage": study.holdout_selected_metrics.coverage,
                "holdout_selected_abstention_rate": study.holdout_selected_metrics.abstention_rate,
                "holdout_selected_coverage_day_lower": _interval_bound(
                    study.holdout_selected_metrics.coverage_day_interval, "lower"
                ),
                "holdout_selected_coverage_day_upper": _interval_bound(
                    study.holdout_selected_metrics.coverage_day_interval, "upper"
                ),
                "holdout_selected_useful_rate_day_lower": _interval_bound(
                    study.holdout_selected_metrics.useful_rate_day_interval, "lower"
                ),
                "holdout_selected_useful_rate_day_upper": _interval_bound(
                    study.holdout_selected_metrics.useful_rate_day_interval, "upper"
                ),
                "holdout_rule_mean_net_bps": study.holdout_rule_metrics.mean_cost_adjusted_result_bps,
                "holdout_selected_mean_net_bps": study.holdout_selected_metrics.mean_cost_adjusted_result_bps,
                "holdout_selected_mean_net_day_lower_bps": _interval_bound(
                    study.holdout_selected_metrics.mean_cost_adjusted_result_day_interval,
                    "lower",
                ),
                "holdout_selected_mean_net_day_upper_bps": _interval_bound(
                    study.holdout_selected_metrics.mean_cost_adjusted_result_day_interval,
                    "upper",
                ),
                "holdout_lift_bps": study.holdout_lift_over_rule_bps,
                "holdout_lift_lower_bps": (
                    study.holdout_lift_interval.lower if study.holdout_lift_interval else None
                ),
                "holdout_rule_brier": study.holdout_rule_metrics.brier_score,
                "holdout_selected_brier": study.holdout_selected_metrics.brier_score,
                "decision": study.decision.value,
                "deployment_model": study.deployment_model.value,
                "claim_allowed": study.claim_allowed,
                "total_examples": study.total_examples,
                "total_trading_days": study.total_trading_days,
                "complex_model_gate_passed": study.complex_model_gate_passed,
                "reason_codes": "|".join(study.reason_codes),
            }
        )
    return rows


def _report(result: SelectivePortfolioResult) -> str:
    lines = [
        "# Выборочная политика H3/H4",
        "",
        "Модели выбирались на настроечной части. Окончательная часть открывалась только после выбора модели и порога. Модель не меняет направление гипотезы: она только разрешает показать исходное направление либо воздержаться.",
        "",
        f"- Запуск: `{result.run_id}`",
        f"- Примеров: {result.examples}",
        f"- Отпечаток данных: `{result.input_fingerprint}`",
        "",
        "| Гипотеза | Горизонт | Выбранная модель | Охват и 95% ДИ | Полезных и 95% ДИ | Результат после издержек и 95% ДИ | Прирост к правилу | Решение |",
        "|---|---:|---|---:|---:|---:|---:|---|",
    ]
    for item in result.results:
        metrics = item.holdout_selected_metrics
        lines.append(
            "| "
            + " | ".join(
                (
                    item.hypothesis_id,
                    f"{item.horizon_seconds // 60} мин",
                    _model_name(item.tune_selected_model),
                    _interval_text(metrics.coverage_day_interval, percent=True),
                    _interval_text(metrics.useful_rate_day_interval, percent=True),
                    _interval_text(
                        metrics.mean_cost_adjusted_result_day_interval,
                        suffix=" б. п.",
                    ),
                    _number(item.holdout_lift_over_rule_bps, " б. п."),
                    item.decision.value,
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "Продуктовое правило: если окончательная проверка не подтверждает улучшение с положительной нижней границей доверительного интервала, в рабочем продукте сохраняется исходное правило, а новый вывод не заявляется.",
            "",
        )
    )
    return "\n".join(lines)


def _model_name(value: SelectiveModelKind) -> str:
    return {
        SelectiveModelKind.SEALED_RULE: "научное правило",
        SelectiveModelKind.SMOOTHED_PROBABILITY: "сглаженная вероятность",
        SelectiveModelKind.LOGISTIC_REGRESSION: "регуляризованная логистическая модель",
        SelectiveModelKind.GRADIENT_BOOSTED_TREES: "неглубокое усиление деревьев",
    }[value]


def _interval_bound(value: object, name: str) -> float | None:
    return float(getattr(value, name)) if value is not None else None


def _interval_text(
    value: object | None, *, percent: bool = False, suffix: str = ""
) -> str:
    if value is None:
        return "—"
    multiplier = 100.0 if percent else 1.0
    unit = "%" if percent else suffix
    estimate = float(getattr(value, "estimate")) * multiplier
    lower = float(getattr(value, "lower")) * multiplier
    upper = float(getattr(value, "upper")) * multiplier
    return f"{estimate:.1f}{unit} [{lower:.1f}; {upper:.1f}]"


def _percent(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _number(value: float | None, suffix: str) -> str:
    return "—" if value is None else f"{value:.2f}{suffix}"


def _json_value(value: object) -> Any:
    if is_dataclass(value):
        return _json_value(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    return value


def _atomic_json(path: Path, value: object) -> None:
    _atomic_text(
        path,
        json.dumps(_json_value(value), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
    )


def _atomic_csv(path: Path, rows: Iterable[dict[str, object]]) -> None:
    materialized = list(rows)
    if not materialized:
        _atomic_text(path, "")
        return
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(materialized[0]))
        writer.writeheader()
        writer.writerows(materialized)
    os.replace(temporary, path)


def _atomic_text(path: Path, value: str) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"
