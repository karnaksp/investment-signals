#!/usr/bin/env python3
"""Train and evaluate offline signal-triggered price prediction baselines."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    bayesian_score_summary,
    chronological_split,
    dataset_feature_columns,
    event_study_summary,
    fingerprint_records,
    float_or_none,
    read_table,
    render_markdown_report,
    write_csv_records,
    write_json,
)


def _target_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if str(row.get("meta_label")) in {"0", "1"}]


def _validation_sessions(rows: Sequence[Mapping[str, Any]]) -> int:
    return len({str(row["trading_day"]) for row in rows})


def _naive_positive_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    selected = _target_rows(rows)
    if not selected:
        return None
    return sum(1 for row in selected if str(row["meta_label"]) == "1") / len(selected)


def _feature_dicts(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[int], list[float], list[str]]:
    numeric, categorical = dataset_feature_columns(rows)
    features: list[dict[str, Any]] = []
    labels: list[int] = []
    returns: list[float] = []
    ids: list[str] = []
    for row in _target_rows(rows):
        item: dict[str, Any] = {}
        for column in numeric:
            item[column] = float_or_none(row.get(column)) or 0.0
        for column in categorical:
            item[column] = str(row.get(column, ""))
        features.append(item)
        labels.append(int(str(row["meta_label"])))
        returns.append(float_or_none(row.get("cost_adjusted_directional_bps")) or 0.0)
        ids.append(str(row["row_id"]))
    return features, labels, returns, ids


def _classification_metrics(y_true: Sequence[int], probabilities: Sequence[float]) -> dict[str, Any]:
    if not y_true:
        return {"status": "no_validation_rows"}
    predictions = [1 if score >= 0.5 else 0 for score in probabilities]
    tp = sum(1 for y, p in zip(y_true, predictions) if y == 1 and p == 1)
    tn = sum(1 for y, p in zip(y_true, predictions) if y == 0 and p == 0)
    fp = sum(1 for y, p in zip(y_true, predictions) if y == 0 and p == 1)
    fn = sum(1 for y, p in zip(y_true, predictions) if y == 1 and p == 0)
    brier = statistics.fmean((y - score) ** 2 for y, score in zip(y_true, probabilities))
    return {
        "accuracy": (tp + tn) / len(y_true),
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
        "brier_score": brier,
        "tp": tp,
        "tn": tn,
        "fp": fp,
        "fn": fn,
    }


def run_logistic_regression(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    valid_x, valid_y, _, _ = _feature_dicts(validation_rows)
    if len(set(train_y)) < 2 or not valid_x:
        return {"model": "logistic_regression", "status": "insufficient_classes_or_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from sklearn.linear_model import LogisticRegression  # type: ignore
    except ImportError:
        return {"model": "logistic_regression", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LogisticRegression(max_iter=1000, class_weight="balanced", random_state=20260716)
    model.fit(x_train, train_y)
    probabilities = [float(item) for item in model.predict_proba(x_valid)[:, 1]]
    metrics = _classification_metrics(valid_y, probabilities)
    coefficients = model.coef_[0]
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "logistic_regression",
                "feature": str(name),
                "importance": float(abs(value)),
                "signed_value": float(value),
            }
            for name, value in zip(names, coefficients)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "logistic_regression", "status": "ok", "n": len(valid_y), **metrics}, importance


def run_lightgbm_classifier(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    valid_x, valid_y, _, _ = _feature_dicts(validation_rows)
    if len(set(train_y)) < 2 or not valid_x:
        return {"model": "lightgbm_classifier", "status": "insufficient_classes_or_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError:
        return {"model": "lightgbm_classifier", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LGBMClassifier(
        n_estimators=250,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260716,
        class_weight="balanced",
        verbose=-1,
    )
    model.fit(x_train, train_y)
    probabilities = [float(item) for item in model.predict_proba(x_valid)[:, 1]]
    metrics = _classification_metrics(valid_y, probabilities)
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "lightgbm_classifier",
                "feature": str(name),
                "importance": float(value),
                "signed_value": float(value),
            }
            for name, value in zip(names, model.feature_importances_)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "lightgbm_classifier", "status": "ok", "n": len(valid_y), **metrics}, importance


def run_lightgbm_regressor(
    train_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train_x, _, train_y, _ = _feature_dicts(train_rows)
    valid_x, _, valid_y, _ = _feature_dicts(validation_rows)
    if len(train_y) < 50 or not valid_x:
        return {"model": "lightgbm_regressor", "status": "insufficient_rows"}, []
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMRegressor  # type: ignore
    except ImportError:
        return {"model": "lightgbm_regressor", "status": "not_available_install_research_extra"}, []
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
    x_valid = vectorizer.transform(valid_x)
    model = LGBMRegressor(
        n_estimators=250,
        learning_rate=0.03,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260716,
        verbose=-1,
    )
    model.fit(x_train, train_y)
    predictions = [float(item) for item in model.predict(x_valid)]
    mae = statistics.fmean(abs(y - p) for y, p in zip(valid_y, predictions))
    rmse = math.sqrt(statistics.fmean((y - p) ** 2 for y, p in zip(valid_y, predictions)))
    names = vectorizer.get_feature_names_out()
    importance = sorted(
        (
            {
                "model": "lightgbm_regressor",
                "feature": str(name),
                "importance": float(value),
                "signed_value": float(value),
            }
            for name, value in zip(names, model.feature_importances_)
        ),
        key=lambda item: item["importance"],
        reverse=True,
    )[:100]
    return {"model": "lightgbm_regressor", "status": "ok", "n": len(valid_y), "mae_bps": mae, "rmse_bps": rmse}, importance


def univariate_feature_importance(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    numeric, _ = dataset_feature_columns(rows)
    selected = _target_rows(rows)
    result: list[dict[str, Any]] = []
    for column in numeric:
        positive = [float_or_none(row.get(column)) for row in selected if str(row["meta_label"]) == "1"]
        negative = [float_or_none(row.get(column)) for row in selected if str(row["meta_label"]) == "0"]
        pos_values = [item for item in positive if item is not None]
        neg_values = [item for item in negative if item is not None]
        if not pos_values or not neg_values:
            continue
        diff = statistics.fmean(pos_values) - statistics.fmean(neg_values)
        result.append(
            {
                "model": "univariate_screen",
                "feature": column,
                "importance": abs(diff),
                "signed_value": diff,
            }
        )
    return sorted(result, key=lambda item: item["importance"], reverse=True)


def build_leaderboard(
    model_results: Sequence[Mapping[str, Any]],
    event_study: Sequence[Mapping[str, Any]],
    *,
    validation_sessions: int,
    naive_positive_rate: float | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in model_results:
        if item.get("status") != "ok":
            rows.append(
                {
                    "model": item["model"],
                    "status": item.get("status"),
                    "n": item.get("n", 0),
                    "score": "",
                    "accepted": False,
                }
            )
            continue
        if item["model"].endswith("_regressor"):
            score = -item.get("mae_bps", 0)
            accepted = False
        else:
            score = item.get("precision") or item.get("accuracy")
            accepted = bool(
                validation_sessions >= 30
                and int(item.get("n", 0)) >= 300
                and score is not None
                and naive_positive_rate is not None
                and float(score) > naive_positive_rate
            )
        rows.append(
            {
                "model": item["model"],
                "status": "ok",
                "n": item.get("n", 0),
                "score": score,
                "accepted": accepted,
            }
        )
    for item in event_study:
        score = item.get("mean_cost_adjusted_directional_bps")
        rows.append(
            {
                "model": "event_study_baseline",
                "status": "ok",
                "signal_type": item.get("signal_type"),
                "horizon_seconds": item.get("horizon_seconds"),
                "n": item.get("n", 0),
                "score": score,
                "accepted": bool(
                    validation_sessions >= 30
                    and int(item.get("n", 0)) >= 300
                    and score is not None
                    and float(score) > 0
                ),
            }
        )
    return sorted(rows, key=lambda item: (bool(item["accepted"]), float(item["score"] or -999999)), reverse=True)


def run_research(dataset: Path, output_dir: Path) -> dict[str, Any]:
    rows = read_table(dataset)
    train_rows, validation_rows = chronological_split(rows)
    validation_sessions = _validation_sessions(validation_rows)
    naive_positive_rate = _naive_positive_rate(validation_rows)
    event_study = event_study_summary(validation_rows, split="validation")
    bayesian = bayesian_score_summary(train_rows, split="train") + bayesian_score_summary(validation_rows, split="validation")
    model_results: list[dict[str, Any]] = []
    feature_importance = univariate_feature_importance(train_rows)
    for runner in (run_logistic_regression, run_lightgbm_classifier, run_lightgbm_regressor):
        result, importance = runner(train_rows, validation_rows)
        model_results.append(result)
        feature_importance.extend(importance)
    leaderboard = build_leaderboard(
        model_results,
        event_study,
        validation_sessions=validation_sessions,
        naive_positive_rate=naive_positive_rate,
    )
    payload = {
        "schema_version": 1,
        "kind": "signal_price_prediction_research_run",
        "dataset": str(dataset),
        "dataset_fingerprint": fingerprint_records(rows),
        "dataset_rows": len(rows),
        "train_rows": len(train_rows),
        "validation_rows": len(validation_rows),
        "validation_sessions": validation_sessions,
        "naive_positive_rate": naive_positive_rate,
        "event_study": event_study,
        "bayesian_score": bayesian,
        "models": model_results,
        "leaderboard": leaderboard,
    }
    run_id = hashlib.sha256(
        json.dumps(
            {
                "dataset": str(dataset),
                "fingerprint": payload["dataset_fingerprint"],
                "models": [item["model"] for item in model_results],
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_json(run_dir / "model-results.json", payload)
    write_json(run_dir / "dataset-manifest.json", {"dataset": str(dataset), "fingerprint": payload["dataset_fingerprint"]})
    write_csv_records(run_dir / "leaderboard.csv", leaderboard or [{"model": "", "status": "", "n": "", "score": "", "accepted": ""}])
    write_csv_records(
        run_dir / "feature-importance.csv",
        feature_importance or [{"model": "", "feature": "", "importance": "", "signed_value": ""}],
    )
    write_csv_records(
        run_dir / "slice-report.csv",
        event_study or [{"model": "event_study_baseline", "split": "validation", "n": 0}],
    )
    (run_dir / "report.md").write_text(render_markdown_report(payload), encoding="utf-8")
    payload["run_id"] = run_id
    payload["run_dir"] = str(run_dir)
    return payload


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-train-price-models")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/runs"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_research(args.dataset, args.output_dir)
    print(
        json.dumps(
            {
                "status": "ok",
                "run_id": result["run_id"],
                "run_dir": result["run_dir"],
                "dataset_rows": result["dataset_rows"],
                "validation_sessions": result["validation_sessions"],
                "accepted_candidates": sum(1 for item in result["leaderboard"] if item.get("accepted")),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
