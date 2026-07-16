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
    accepted_min_sessions: int = 20,
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
    accepted = [row for row in candidates if row.get("accepted_exploratory")]
    lines = [
        "# Signal price pattern mining",
        "",
        f"- Naive validation positive rate: {naive_positive_rate:.4f}",
        f"- Accepted exploratory patterns: {len(accepted)}",
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
    lines.extend(["", "## Accepted exploratory patterns", ""])
    if not accepted:
        lines.append("No pattern passed the exploratory acceptance rule.")
    else:
        lines.append("| Rule | n | sessions | positive rate | lift | mean directional bps |")
        lines.append("|---|---:|---:|---:|---:|---:|")
        for row in accepted[:30]:
            lines.append(
                "| {rule} | {n} | {sessions} | {rate:.4f} | {lift:.2f}x | {mean:.3f} |".format(
                    rule=row["rule"],
                    n=row["n"],
                    sessions=row["sessions"],
                    rate=row["positive_rate"],
                    lift=row["positive_rate_lift"] or 0.0,
                    mean=row["mean_cost_adjusted_directional_bps"],
                )
            )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "These are validation-only exploratory patterns selected from the top LightGBM probability decile. "
            "They are not product claims until a later independent holdout confirms the same rules.",
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
    scored_rows, naive_rate = scored_validation_rows(rows)
    deciles = probability_deciles(scored_rows)
    candidates = mine_pattern_candidates(
        scored_rows,
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
                "scored_validation_rows": len(scored_rows),
                "naive_positive_rate": naive_rate,
                "accepted_patterns": sum(1 for row in candidates if row["accepted_exploratory"]),
                "pattern_report": str(args.run_dir / "pattern-report.md"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
