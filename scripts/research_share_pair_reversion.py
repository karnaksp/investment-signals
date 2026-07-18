#!/usr/bin/env python3
"""Research common/preferred share spread reversion on cached minute candles."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict, deque
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    ResearchCandle,
    chronological_split,
    read_cache,
    trading_day,
    wilson_lower_bound,
    write_csv_records,
    write_json,
)


DEFAULT_PAIRS = (("SBER", "SBERP"), ("TATN", "TATNP"), ("SNGS", "SNGSP"), ("RTKM", "RTKMP"))
HORIZONS_MINUTES = (5, 15, 30, 60)
LOOKBACK_MINUTES = 120
ENTRY_Z = 2.0
EXIT_Z = 1.0
PAIR_ROUND_TRIP_COST_BPS = 20.0
MATERIALITY_BPS = 5.0
TARGET_RATE = 0.90
MIN_ROWS = 300
MIN_SESSIONS = 30


def _return_bps(start: float, end: float) -> float:
    return 10_000.0 * (end / start - 1.0) if start > 0 and end > 0 else 0.0


def _aligned_days(
    candles: Sequence[ResearchCandle],
    pairs: Sequence[tuple[str, str]],
) -> dict[tuple[str, str, date], list[tuple[ResearchCandle, ResearchCandle]]]:
    by_ticker_time = {(row.ticker, row.at): row for row in candles if row.complete and row.close > 0}
    result: dict[tuple[str, str, date], list[tuple[ResearchCandle, ResearchCandle]]] = defaultdict(list)
    for common, preferred in pairs:
        common_rows = sorted(
            (row for row in candles if row.ticker == common and row.complete and row.close > 0),
            key=lambda row: row.at,
        )
        for common_row in common_rows:
            preferred_row = by_ticker_time.get((preferred, common_row.at))
            if preferred_row is None:
                continue
            local_day = trading_day(common_row.at)
            result[(common, preferred, local_day)].append((common_row, preferred_row))
    return result


def build_pair_rows(
    candles: Sequence[ResearchCandle],
    *,
    pairs: Sequence[tuple[str, str]] = DEFAULT_PAIRS,
    lookback_minutes: int = LOOKBACK_MINUTES,
    entry_z: float = ENTRY_Z,
    exit_z: float = EXIT_Z,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (common, preferred, trading_day), aligned in sorted(_aligned_days(candles, pairs).items()):
        if len(aligned) < lookback_minutes + max(HORIZONS_MINUTES) + 1:
            continue
        ratios: deque[float] = deque(maxlen=lookback_minutes)
        armed = True
        for index, (common_row, preferred_row) in enumerate(aligned):
            ratio = math.log(common_row.close / preferred_row.close)
            if len(ratios) < lookback_minutes:
                ratios.append(ratio)
                continue
            mean = statistics.fmean(ratios)
            std = statistics.pstdev(ratios)
            z_score = (ratio - mean) / std if std > 1e-12 else 0.0
            if abs(z_score) <= exit_z:
                armed = True
            if not armed or abs(z_score) < entry_z:
                ratios.append(ratio)
                continue
            armed = False
            past = aligned[max(0, index - 60):index]
            past_common = [item[0].close for item in past]
            past_preferred = [item[1].close for item in past]
            pair_name = f"{common}/{preferred}"
            direction = -1 if z_score > 0 else 1
            for horizon in HORIZONS_MINUTES:
                future_index = index + horizon
                if future_index >= len(aligned):
                    continue
                future_common, future_preferred = aligned[future_index]
                if future_common.at - common_row.at != timedelta(minutes=horizon):
                    continue
                future_ratio = math.log(future_common.close / future_preferred.close)
                raw_reversion_bps = direction * (future_ratio - ratio) * 10_000.0
                net_reversion_bps = raw_reversion_bps - PAIR_ROUND_TRIP_COST_BPS
                rows.append(
                    {
                        "row_id": f"{pair_name}:{common_row.at.isoformat()}:{horizon}",
                        "pair": pair_name,
                        "common": common,
                        "preferred": preferred,
                        "source_event_at": common_row.at.isoformat(),
                        "feature_max_observed_at": aligned[index - 1][0].at.isoformat(),
                        "feature_leakage_flag": aligned[index - 1][0].at >= common_row.at,
                        "trading_day": trading_day.isoformat(),
                        "session_hour": common_row.at.hour,
                        "horizon_minutes": horizon,
                        "direction": direction,
                        "z_score": z_score,
                        "abs_z_score": abs(z_score),
                        "distance_from_mean_bps": (ratio - mean) * 10_000.0,
                        "ratio_volatility_bps": std * 10_000.0,
                        "common_return_5m": _return_bps(past_common[-5], past_common[-1]) if len(past_common) >= 5 else 0.0,
                        "preferred_return_5m": _return_bps(past_preferred[-5], past_preferred[-1]) if len(past_preferred) >= 5 else 0.0,
                        "common_return_15m": _return_bps(past_common[-15], past_common[-1]) if len(past_common) >= 15 else 0.0,
                        "preferred_return_15m": _return_bps(past_preferred[-15], past_preferred[-1]) if len(past_preferred) >= 15 else 0.0,
                        "common_volume": common_row.volume,
                        "preferred_volume": preferred_row.volume,
                        "common_aggressor_imbalance": (
                            (common_row.volume_buy - common_row.volume_sell)
                            / (common_row.volume_buy + common_row.volume_sell)
                            if common_row.volume_buy + common_row.volume_sell > 0 else 0.0
                        ),
                        "preferred_aggressor_imbalance": (
                            (preferred_row.volume_buy - preferred_row.volume_sell)
                            / (preferred_row.volume_buy + preferred_row.volume_sell)
                            if preferred_row.volume_buy + preferred_row.volume_sell > 0 else 0.0
                        ),
                        "raw_reversion_bps": raw_reversion_bps,
                        "net_reversion_bps": net_reversion_bps,
                        "success": int(net_reversion_bps >= MATERIALITY_BPS),
                    }
                )
            ratios.append(ratio)
    return rows


def _feature_rows(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[int]]:
    excluded = {
        "row_id", "source_event_at", "feature_max_observed_at", "feature_leakage_flag",
        "trading_day", "raw_reversion_bps", "net_reversion_bps", "success",
    }
    return (
        [{key: value for key, value in row.items() if key not in excluded} for row in rows],
        [int(row["success"]) for row in rows],
    )


def _fit_probabilities(
    train: Sequence[Mapping[str, Any]], validation: Sequence[Mapping[str, Any]]
) -> list[float]:
    from lightgbm import LGBMClassifier  # type: ignore
    from sklearn.feature_extraction import DictVectorizer  # type: ignore

    train_x, train_y = _feature_rows(train)
    validation_x, _ = _feature_rows(validation)
    if not validation_x or len(set(train_y)) < 2:
        return []
    vectorizer = DictVectorizer(sparse=True)
    encoded_train = vectorizer.fit_transform(train_x)
    encoded_validation = vectorizer.transform(validation_x)
    model = LGBMClassifier(
        n_estimators=200,
        learning_rate=0.03,
        num_leaves=15,
        min_child_samples=50,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=20260718,
        verbose=-1,
    )
    model.fit(encoded_train, train_y)
    return [float(item) for item in model.predict_proba(encoded_validation)[:, 1]]


def evaluate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    train, validation = chronological_split(rows)
    probabilities = _fit_probabilities(train, validation)
    scored = [dict(row) | {"probability": probability} for row, probability in zip(validation, probabilities)]
    candidates: list[dict[str, Any]] = []
    for threshold in (0.50, 0.60, 0.70, 0.80, 0.90, 0.95):
        selected = [row for row in scored if row["probability"] >= threshold]
        successes = sum(int(row["success"]) for row in selected)
        sessions = len({str(row["trading_day"]) for row in selected})
        rate = successes / len(selected) if selected else 0.0
        lower = wilson_lower_bound(successes, len(selected)) or 0.0
        candidates.append(
            {
                "selection": f"probability>={threshold:.2f}",
                "rows": len(selected),
                "sessions": sessions,
                "successes": successes,
                "success_rate": rate,
                "wilson_lower_95": lower,
                "accepted": len(selected) >= MIN_ROWS and sessions >= MIN_SESSIONS and rate >= TARGET_RATE and lower >= TARGET_RATE,
            }
        )
    ordered = sorted(scored, key=lambda row: row["probability"], reverse=True)
    for count in (20, 50, 100, 300, 1000):
        selected = ordered[:count]
        if len(selected) < count:
            continue
        successes = sum(int(row["success"]) for row in selected)
        sessions = len({str(row["trading_day"]) for row in selected})
        rate = successes / len(selected)
        lower = wilson_lower_bound(successes, len(selected)) or 0.0
        candidates.append(
            {
                "selection": f"top_{count}",
                "rows": len(selected),
                "sessions": sessions,
                "successes": successes,
                "success_rate": rate,
                "wilson_lower_95": lower,
                "accepted": len(selected) >= MIN_ROWS and sessions >= MIN_SESSIONS and rate >= TARGET_RATE and lower >= TARGET_RATE,
            }
        )
    best = max(candidates, key=lambda row: (row["wilson_lower_95"], row["rows"])) if candidates else None
    return {
        "schema_version": 1,
        "method": "common_preferred_spread_reversion_v1",
        "cost_bps": PAIR_ROUND_TRIP_COST_BPS,
        "materiality_bps": MATERIALITY_BPS,
        "rows": len(rows),
        "train_rows": len(train),
        "validation_rows": len(validation),
        "validation_sessions": len({str(row["trading_day"]) for row in validation}),
        "feature_leakage_rows": sum(bool(row["feature_leakage_flag"]) for row in rows),
        "accepted_candidates": sum(bool(row["accepted"]) for row in candidates),
        "best_candidate": best,
        "candidates": candidates,
        "scored_rows": scored,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-share-pair-reversion")
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    rows = build_pair_rows(read_cache(args.cache_dir))
    result = evaluate(rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv_records(args.output_dir / "pair-events.csv", rows, fields=tuple(rows[0]) if rows else ())
    scored = result.pop("scored_rows")
    write_csv_records(args.output_dir / "validation-predictions.csv", scored, fields=tuple(scored[0]) if scored else ())
    write_csv_records(args.output_dir / "candidate-thresholds.csv", result["candidates"])
    write_json(args.output_dir / "result.json", result)
    best = result["best_candidate"] or {}
    report = "\n".join(
        [
            "# Возврат соотношения обыкновенных и привилегированных акций",
            "",
            f"- Строк исследования: {result['rows']}",
            f"- Строк поздней проверки: {result['validation_rows']}",
            f"- Торговых дней поздней проверки: {result['validation_sessions']}",
            f"- Утечек будущих данных: {result['feature_leakage_rows']}",
            f"- Лучший отбор: {best.get('selection', 'нет')}",
            f"- Доля верных случаев: {float(best.get('success_rate', 0)):.1%}",
            f"- Нижняя граница надёжности: {float(best.get('wilson_lower_95', 0)):.1%}",
            f"- Прошедших строгий порог 90%: {result['accepted_candidates']}",
        ]
    )
    (args.output_dir / "report.md").write_text(report + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "candidates"}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
