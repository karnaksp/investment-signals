#!/usr/bin/env python3
"""Mine interpretable validation patterns from signal price prediction data."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    chronological_split,
    float_or_none,
    read_table,
    wilson_lower_bound,
    write_csv_records,
)
from research_train_price_models import _feature_dicts, _target_rows  # noqa: E402


PATTERN_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("signal_horizon", ("signal_type", "horizon_seconds")),
    ("signal_horizon_session_volatility", ("signal_type", "horizon_seconds", "session_bucket", "_volatility_bucket")),
    ("combo_horizon", ("combo_key_300s", "horizon_seconds")),
    ("ticker_signal_horizon", ("ticker", "signal_type", "horizon_seconds")),
    ("signal_horizon_combo", ("signal_type", "horizon_seconds", "combo_key_300s")),
)


def scored_validation_rows(dataset_rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], float]:
    train_rows, validation_rows = chronological_split(dataset_rows)
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    valid_x, valid_y, _, _ = _feature_dicts(validation_rows)
    if len(set(train_y)) < 2 or not valid_x:
        raise RuntimeError("not enough classes or validation rows for pattern mining")
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install research dependencies: pip install -e '.[research]'") from exc
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
    rows: list[dict[str, Any]] = []
    for row, y_true, probability in zip(_target_rows(validation_rows), valid_y, probabilities):
        enriched = dict(row)
        enriched["_target"] = int(y_true)
        enriched["_predicted_probability"] = probability
        enriched["_cost_adjusted_directional_bps"] = float_or_none(
            row.get("cost_adjusted_directional_bps")
        ) or 0.0
        enriched["_reverse_directional_bps"] = float_or_none(row.get("reverse_directional_bps")) or 0.0
        enriched["_volatility_bucket"] = volatility_bucket(
            float_or_none(row.get("day_volatility_quantile"))
        )
        rows.append(enriched)
    naive_rate = sum(valid_y) / len(valid_y) if valid_y else 0.0
    return rows, naive_rate


def train_pattern_model(
    train_rows: Sequence[Mapping[str, Any]],
) -> tuple[Any, Any]:
    train_x, train_y, _, _ = _feature_dicts(train_rows)
    if len(set(train_y)) < 2:
        raise RuntimeError("not enough classes for pattern model training")
    try:
        from sklearn.feature_extraction import DictVectorizer  # type: ignore
        from lightgbm import LGBMClassifier  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Install research dependencies: pip install -e '.[research]'") from exc
    vectorizer = DictVectorizer(sparse=True)
    x_train = vectorizer.fit_transform(train_x)
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
    return vectorizer, model


def score_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    vectorizer: Any,
    model: Any,
) -> list[dict[str, Any]]:
    features, labels, _, _ = _feature_dicts(rows)
    target_rows = _target_rows(rows)
    if not features:
        return []
    probabilities = [float(item) for item in model.predict_proba(vectorizer.transform(features))[:, 1]]
    result: list[dict[str, Any]] = []
    for row, y_true, probability in zip(target_rows, labels, probabilities):
        enriched = dict(row)
        enriched["_target"] = int(y_true)
        enriched["_predicted_probability"] = probability
        enriched["_cost_adjusted_directional_bps"] = float_or_none(
            row.get("cost_adjusted_directional_bps")
        ) or 0.0
        enriched["_reverse_directional_bps"] = float_or_none(row.get("reverse_directional_bps")) or 0.0
        enriched["_volatility_bucket"] = volatility_bucket(
            float_or_none(row.get("day_volatility_quantile"))
        )
        result.append(enriched)
    return result


def scored_discovery_and_validation_rows(
    dataset_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float]:
    train_rows, validation_rows = chronological_split(dataset_rows)
    fit_rows, discovery_rows = chronological_split(train_rows, train_fraction=0.80)
    if not discovery_rows:
        fit_rows, discovery_rows = train_rows, train_rows
    vectorizer, model = train_pattern_model(fit_rows)
    discovery_scored = score_rows(discovery_rows, vectorizer=vectorizer, model=model)
    validation_scored = score_rows(validation_rows, vectorizer=vectorizer, model=model)
    naive_rate = (
        sum(int(row["_target"]) for row in validation_scored) / len(validation_scored)
        if validation_scored
        else 0.0
    )
    return discovery_scored, validation_scored, naive_rate


def probability_deciles(scored_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(scored_rows, key=lambda row: float(row["_predicted_probability"]), reverse=True)
    if not ordered:
        return []
    result: list[dict[str, Any]] = []
    for index in range(10):
        start = index * len(ordered) // 10
        end = (index + 1) * len(ordered) // 10
        group = ordered[start:end]
        if not group:
            continue
        result.append(metric_row("probability_decile", str(index + 1), group))
    return result


def mine_pattern_candidates(
    scored_rows: Sequence[Mapping[str, Any]],
    *,
    naive_positive_rate: float,
    top_fraction: float = 0.10,
    min_n: int = 100,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    accepted_min_lower_bound: float = 0.75,
) -> list[dict[str, Any]]:
    ordered = sorted(scored_rows, key=lambda row: float(row["_predicted_probability"]), reverse=True)
    top_count = max(1, int(len(ordered) * top_fraction))
    selected = ordered[:top_count]
    threshold = max(0.20, naive_positive_rate * 3.0)
    result: list[dict[str, Any]] = []
    for group_name, columns in PATTERN_GROUPS:
        groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in selected:
            groups[tuple(str(row.get(column, "")) for column in columns)].append(row)
        for key, rows in groups.items():
            if len(rows) < min_n:
                continue
            item = metric_row(group_name, " | ".join(f"{column}={value}" for column, value in zip(columns, key)), rows)
            item["top_fraction"] = top_fraction
            item["naive_positive_rate"] = naive_positive_rate
            item["positive_rate_lift"] = (
                item["positive_rate"] / naive_positive_rate if naive_positive_rate else None
            )
            item["accepted_exploratory"] = bool(
                item["n"] >= accepted_min_n
                and item["sessions"] >= accepted_min_sessions
                and item["wilson_lower_95"] >= accepted_min_lower_bound
                and item["positive_rate"] >= threshold
                and item["mean_cost_adjusted_directional_bps"] > 0
            )
            result.append(item)
    return sorted(
        result,
        key=lambda item: (
            bool(item["accepted_exploratory"]),
            item["mean_cost_adjusted_directional_bps"],
            item["positive_rate"],
            item["n"],
        ),
        reverse=True,
    )


def _rule_text(columns: Sequence[str], key: Sequence[str]) -> str:
    return " | ".join(f"{column}={value}" for column, value in zip(columns, key))


def _matching_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    columns: Sequence[str],
    key: Sequence[str],
    min_probability: float,
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if float(row["_predicted_probability"]) >= min_probability
        and tuple(str(row.get(column, "")) for column in columns) == tuple(key)
    ]


def _empty_metric_row(group_set: str, rule: str) -> dict[str, Any]:
    return {
        "group_set": group_set,
        "rule": rule,
        "n": 0,
        "sessions": 0,
        "positive_count": 0,
        "positive_rate": 0.0,
        "wilson_lower_95": 0.0,
        "mean_cost_adjusted_directional_bps": 0.0,
        "mean_reverse_directional_bps": 0.0,
        "avg_predicted_probability": 0.0,
        "min_predicted_probability": 0.0,
        "max_predicted_probability": 0.0,
    }


def mine_out_of_sample_pattern_candidates(
    discovery_rows: Sequence[Mapping[str, Any]],
    validation_rows: Sequence[Mapping[str, Any]],
    *,
    naive_positive_rate: float,
    top_fraction: float = 0.10,
    min_n: int = 100,
    accepted_min_n: int = 300,
    accepted_min_sessions: int = 30,
    accepted_min_lower_bound: float = 0.75,
) -> list[dict[str, Any]]:
    ordered = sorted(discovery_rows, key=lambda row: float(row["_predicted_probability"]), reverse=True)
    top_count = max(1, int(len(ordered) * top_fraction))
    selected = ordered[:top_count]
    if not selected:
        return []
    probability_threshold = min(float(row["_predicted_probability"]) for row in selected)
    positive_rate_threshold = max(0.20, naive_positive_rate * 3.0)
    result: list[dict[str, Any]] = []
    for group_name, columns in PATTERN_GROUPS:
        groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in selected:
            groups[tuple(str(row.get(column, "")) for column in columns)].append(row)
        for key, discovery_group in groups.items():
            if len(discovery_group) < min_n:
                continue
            rule = _rule_text(columns, key)
            discovery_metric = metric_row(group_name, rule, discovery_group)
            validation_group = _matching_rows(
                validation_rows,
                columns=columns,
                key=key,
                min_probability=probability_threshold,
            )
            validation_metric = (
                metric_row(group_name, rule, validation_group)
                if validation_group
                else _empty_metric_row(group_name, rule)
            )
            accepted = bool(
                validation_metric["n"] >= accepted_min_n
                and validation_metric["sessions"] >= accepted_min_sessions
                and validation_metric["wilson_lower_95"] >= accepted_min_lower_bound
                and validation_metric["positive_rate"] >= positive_rate_threshold
                and validation_metric["mean_cost_adjusted_directional_bps"] > 0
            )
            result.append(
                {
                    "group_set": group_name,
                    "rule": rule,
                    "top_fraction": top_fraction,
                    "probability_threshold_from_discovery": probability_threshold,
                    "naive_positive_rate": naive_positive_rate,
                    "discovery_n": discovery_metric["n"],
                    "discovery_sessions": discovery_metric["sessions"],
                    "discovery_positive_rate": discovery_metric["positive_rate"],
                    "discovery_wilson_lower_95": discovery_metric["wilson_lower_95"],
                    "discovery_mean_cost_adjusted_directional_bps": discovery_metric[
                        "mean_cost_adjusted_directional_bps"
                    ],
                    "validation_n": validation_metric["n"],
                    "validation_sessions": validation_metric["sessions"],
                    "validation_positive_count": validation_metric["positive_count"],
                    "validation_positive_rate": validation_metric["positive_rate"],
                    "validation_wilson_lower_95": validation_metric["wilson_lower_95"],
                    "validation_mean_cost_adjusted_directional_bps": validation_metric[
                        "mean_cost_adjusted_directional_bps"
                    ],
                    "validation_mean_reverse_directional_bps": validation_metric[
                        "mean_reverse_directional_bps"
                    ],
                    "validation_positive_rate_lift": (
                        validation_metric["positive_rate"] / naive_positive_rate
                        if naive_positive_rate
                        else None
                    ),
                    "accepted_out_of_sample": accepted,
                }
            )
    return sorted(
        result,
        key=lambda item: (
            bool(item["accepted_out_of_sample"]),
            item["validation_wilson_lower_95"],
            item["validation_positive_rate"],
            item["validation_mean_cost_adjusted_directional_bps"],
            item["validation_n"],
        ),
        reverse=True,
    )


def metric_row(group_set: str, rule: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    positives = sum(int(row["_target"]) for row in rows)
    probabilities = [float(row["_predicted_probability"]) for row in rows]
    directional = [float(row["_cost_adjusted_directional_bps"]) for row in rows]
    reverse = [float(row["_reverse_directional_bps"]) for row in rows]
    return {
        "group_set": group_set,
        "rule": rule,
        "n": len(rows),
        "sessions": len({str(row["trading_day"]) for row in rows}),
        "positive_count": positives,
        "positive_rate": positives / len(rows) if rows else 0.0,
        "wilson_lower_95": wilson_lower_bound(positives, len(rows)) or 0.0,
        "mean_cost_adjusted_directional_bps": statistics.fmean(directional),
        "mean_reverse_directional_bps": statistics.fmean(reverse),
        "avg_predicted_probability": statistics.fmean(probabilities),
        "min_predicted_probability": min(probabilities),
        "max_predicted_probability": max(probabilities),
    }


def volatility_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.33:
        return "low"
    if value < 0.66:
        return "mid"
    return "high"


def write_report(
    output: Path,
    *,
    deciles: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    naive_positive_rate: float,
) -> None:
    accepted = [
        row
        for row in candidates
        if row.get("accepted_out_of_sample") or row.get("accepted_exploratory")
    ]
    lines = [
        "# Signal price pattern mining",
        "",
        f"- Naive validation positive rate: {naive_positive_rate:.4f}",
        f"- Accepted out-of-sample patterns: {len(accepted)}",
        "",
        "## Probability deciles",
        "",
        "| Decile | n | positive rate | mean directional bps | min probability |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in deciles:
        lines.append(
            "| {decile} | {n} | {rate:.4f} | {mean:.3f} | {prob:.4f} |".format(
                decile=row["rule"],
                n=row["n"],
                rate=row["positive_rate"],
                mean=row["mean_cost_adjusted_directional_bps"],
                prob=row["min_predicted_probability"],
            )
        )
    lines.extend(["", "## Accepted out-of-sample patterns", ""])
    if not accepted:
        lines.append("No pattern passed the out-of-sample acceptance rule.")
    else:
        lines.append(
            "| Rule | validation n | sessions | positive rate | Wilson lower 95% | lift | mean directional bps |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for row in accepted[:30]:
            lines.append(
                "| {rule} | {n} | {sessions} | {rate:.4f} | {lower:.4f} | {lift:.2f}x | {mean:.3f} |".format(
                    rule=row["rule"],
                    n=row.get("validation_n", row.get("n", 0)),
                    sessions=row.get("validation_sessions", row.get("sessions", 0)),
                    rate=row.get("validation_positive_rate", row.get("positive_rate", 0.0)),
                    lower=row.get("validation_wilson_lower_95", row.get("wilson_lower_95", 0.0)),
                    lift=row.get("validation_positive_rate_lift", row.get("positive_rate_lift")) or 0.0,
                    mean=row.get(
                        "validation_mean_cost_adjusted_directional_bps",
                        row.get("mean_cost_adjusted_directional_bps", 0.0),
                    ),
                )
            )
    lines.extend(["", "## Top rejected out-of-sample rules", ""])
    rejected = [row for row in candidates if not row.get("accepted_out_of_sample")]
    if not rejected:
        lines.append("No rejected rule rows were generated.")
    else:
        lines.append(
            "| Rule | discovery rate | validation n | validation rate | Wilson lower 95% | mean directional bps |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in rejected[:20]:
            lines.append(
                "| {rule} | {discovery:.4f} | {n} | {rate:.4f} | {lower:.4f} | {mean:.3f} |".format(
                    rule=row["rule"],
                    discovery=row.get("discovery_positive_rate", row.get("positive_rate", 0.0)),
                    n=row.get("validation_n", row.get("n", 0)),
                    rate=row.get("validation_positive_rate", row.get("positive_rate", 0.0)),
                    lower=row.get("validation_wilson_lower_95", row.get("wilson_lower_95", 0.0)),
                    mean=row.get(
                        "validation_mean_cost_adjusted_directional_bps",
                        row.get("mean_cost_adjusted_directional_bps", 0.0),
                    ),
                )
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Rules are selected on an earlier discovery slice and evaluated on the later validation slice "
            "with the same probability threshold. They are still not product claims until an independent "
            "future holdout or production shadow period confirms the same effect.",
            "",
        ]
    )
    output.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-mine-price-patterns")
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--top-fraction", type=float, default=0.10)
    parser.add_argument("--min-n", type=int, default=100)
    parser.add_argument("--accepted-min-n", type=int, default=300)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = read_table(args.dataset)
    discovery_rows, validation_rows, naive_rate = scored_discovery_and_validation_rows(rows)
    deciles = probability_deciles(validation_rows)
    candidates = mine_out_of_sample_pattern_candidates(
        discovery_rows,
        validation_rows,
        naive_positive_rate=naive_rate,
        top_fraction=args.top_fraction,
        min_n=args.min_n,
        accepted_min_n=args.accepted_min_n,
    )
    args.run_dir.mkdir(parents=True, exist_ok=True)
    write_csv_records(args.run_dir / "probability-deciles.csv", deciles)
    write_csv_records(args.run_dir / "pattern-candidates.csv", candidates)
    write_report(
        args.run_dir / "pattern-report.md",
        deciles=deciles,
        candidates=candidates,
        naive_positive_rate=naive_rate,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "run_dir": str(args.run_dir),
                "scored_discovery_rows": len(discovery_rows),
                "scored_validation_rows": len(validation_rows),
                "naive_positive_rate": naive_rate,
                "accepted_patterns": sum(1 for row in candidates if row["accepted_out_of_sample"]),
                "pattern_report": str(args.run_dir / "pattern-report.md"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
