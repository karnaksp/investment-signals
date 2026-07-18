#!/usr/bin/env python3
"""Find exclusion guards that remove false positives from directional candidates."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from research_mine_selective_rules import row_atoms, wilson_lower_bound


DEFAULT_THRESHOLDS = (0.40,)
TARGET_SUCCESS_RATE = 0.90
SHADOW_MIN_LOWER_BOUND = 0.75
MIN_ROWS = 300
MIN_SESSIONS = 30


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float_or_zero(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _int_or_zero(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _eligible_rows(rows: Sequence[Mapping[str, Any]], *, threshold: float) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("frontier_decision")) not in {"up", "down"}:
            continue
        if row.get("frontier_success") in {None, ""}:
            continue
        if _float_or_zero(row.get("frontier_confidence")) < threshold:
            continue
        result.append(dict(row))
    return result


def _max_share(rows: Sequence[Mapping[str, Any]], field: str) -> float:
    if not rows:
        return 0.0
    counts = Counter(str(row.get(field, "")) for row in rows)
    return max(counts.values()) / len(rows)


def _max_index_share(values: Sequence[str], indices: frozenset[int]) -> float:
    if not indices:
        return 0.0
    counts = Counter(values[index] for index in indices)
    return max(counts.values()) / len(indices)


def metric_row(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    successes = sum(_int_or_zero(row.get("frontier_success")) for row in rows)
    result_values = [_float_or_zero(row.get("frontier_result_bps")) for row in rows]
    return {
        "rows": len(rows),
        "sessions": len({str(row.get("trading_day", "")) for row in rows if row.get("trading_day")}),
        "tickers": len({str(row.get("ticker", "")) for row in rows if row.get("ticker")}),
        "success_count": successes,
        "failure_count": len(rows) - successes,
        "success_rate": successes / len(rows) if rows else 0.0,
        "wilson_lower_95": wilson_lower_bound(successes, len(rows)),
        "mean_result_bps": statistics.fmean(result_values) if result_values else 0.0,
        "max_day_share": _max_share(rows, "trading_day"),
        "max_ticker_share": _max_share(rows, "ticker"),
    }


def _prepared_columns(rows: Sequence[Mapping[str, Any]]) -> dict[str, tuple[Any, ...]]:
    return {
        "success": tuple(_int_or_zero(row.get("frontier_success")) for row in rows),
        "result_bps": tuple(_float_or_zero(row.get("frontier_result_bps")) for row in rows),
        "trading_day": tuple(str(row.get("trading_day", "")) for row in rows),
        "ticker": tuple(str(row.get("ticker", "")) for row in rows),
    }


def metric_indices(
    prepared: Mapping[str, Sequence[Any]],
    indices: frozenset[int],
    *,
    include_concentration: bool = False,
) -> dict[str, Any]:
    if not indices:
        return {
            "rows": 0,
            "sessions": 0,
            "tickers": 0,
            "success_count": 0,
            "failure_count": 0,
            "success_rate": 0.0,
            "wilson_lower_95": 0.0,
            "mean_result_bps": 0.0,
            "max_day_share": 0.0,
            "max_ticker_share": 0.0,
        }
    successes = sum(int(prepared["success"][index]) for index in indices)
    result_values = [float(prepared["result_bps"][index]) for index in indices]
    trading_days = prepared["trading_day"]
    tickers = prepared["ticker"]
    metric = {
        "rows": len(indices),
        "sessions": len({str(trading_days[index]) for index in indices if trading_days[index]}),
        "tickers": len({str(tickers[index]) for index in indices if tickers[index]}),
        "success_count": successes,
        "failure_count": len(indices) - successes,
        "success_rate": successes / len(indices),
        "wilson_lower_95": wilson_lower_bound(successes, len(indices)),
        "mean_result_bps": statistics.fmean(result_values) if result_values else 0.0,
        "max_day_share": 1.0,
        "max_ticker_share": 1.0,
    }
    if include_concentration:
        metric["max_day_share"] = _max_index_share(trading_days, indices)  # type: ignore[arg-type]
        metric["max_ticker_share"] = _max_index_share(tickers, indices)  # type: ignore[arg-type]
    return metric


def _passes_non_concentration_gates(metric: Mapping[str, Any]) -> bool:
    return bool(
        int(metric["rows"]) >= MIN_ROWS
        and int(metric["sessions"]) >= MIN_SESSIONS
        and float(metric["success_rate"]) >= TARGET_SUCCESS_RATE
        and float(metric["wilson_lower_95"]) >= SHADOW_MIN_LOWER_BOUND
        and float(metric["mean_result_bps"]) > 0
    )


def _accepted(metric: Mapping[str, Any]) -> bool:
    return bool(
        int(metric["rows"]) >= MIN_ROWS
        and int(metric["sessions"]) >= MIN_SESSIONS
        and float(metric["success_rate"]) >= TARGET_SUCCESS_RATE
        and float(metric["wilson_lower_95"]) >= SHADOW_MIN_LOWER_BOUND
        and float(metric["mean_result_bps"]) > 0
        and float(metric["max_day_share"]) <= 0.20
        and float(metric["max_ticker_share"]) <= 0.25
    )


def _status(*, accepted: bool, kept: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    if accepted:
        return "shadow_guard_candidate"
    if int(kept["rows"]) < MIN_ROWS or int(kept["sessions"]) < MIN_SESSIONS:
        return "too_small_after_exclusion"
    if float(kept["success_rate"]) <= float(baseline["success_rate"]):
        return "no_precision_gain"
    if float(kept["success_rate"]) < TARGET_SUCCESS_RATE:
        return "improves_but_below_90pct"
    if float(kept["wilson_lower_95"]) < SHADOW_MIN_LOWER_BOUND:
        return "observed_90_but_weak_bound"
    if float(kept["mean_result_bps"]) <= 0:
        return "observed_90_but_negative_after_costs"
    return "blocked_by_concentration"


def _missing_successes_to_90(successes: int, rows: int) -> int:
    if rows <= 0:
        return 0
    required = int(rows * TARGET_SUCCESS_RATE)
    if rows * TARGET_SUCCESS_RATE > required:
        required += 1
    return max(0, required - successes)


def _atom_field(atom: str) -> str:
    for separator in (">=", "<=", "=", ">", "<"):
        if separator in atom:
            return atom.split(separator, 1)[0]
    return atom


def _redundant_guard(guard: Sequence[str]) -> bool:
    fields = [_atom_field(atom) for atom in guard]
    return len(fields) != len(set(fields))


def _build_atom_index(atoms_by_row: Sequence[frozenset[str]]) -> dict[str, frozenset[int]]:
    result: dict[str, set[int]] = {}
    for index, atoms in enumerate(atoms_by_row):
        for atom in atoms:
            result.setdefault(atom, set()).add(index)
    return {atom: frozenset(indices) for atom, indices in result.items()}


def _union_support(atom_index: Mapping[str, frozenset[int]], guard: frozenset[str]) -> frozenset[int]:
    indices: set[int] = set()
    for atom in guard:
        indices.update(atom_index.get(atom, frozenset()))
    return frozenset(indices)


def _guard_sort_key(row: Mapping[str, Any]) -> tuple[object, ...]:
    return (
        bool(row["accepted_shadow"]),
        float(row["precision_gain"]),
        float(row["kept_success_rate"]),
        float(row["kept_wilson_lower_95"]),
        float(row["removed_failure_share"]),
        int(row["kept_rows"]),
        -int(row.get("guard_terms", 1)),
    )


def mine_false_positive_guards(
    rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Sequence[float] = DEFAULT_THRESHOLDS,
    min_removed_rows: int = 20,
    max_guard_terms: int = 2,
    beam_width: int = 50,
    top_n: int = 200,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for threshold in thresholds:
        selected = _eligible_rows(rows, threshold=threshold)
        baseline = metric_row(selected)
        if not selected:
            continue
        atoms_by_row = [row_atoms(row) for row in selected]
        atom_index = _build_atom_index(atoms_by_row)
        prepared = _prepared_columns(selected)
        baseline_failures = int(baseline["failure_count"])
        all_indices = frozenset(range(len(selected)))

        def evaluate_guard(guard: frozenset[str]) -> dict[str, Any] | None:
            removed_indices = _union_support(atom_index, guard)
            if len(removed_indices) < min_removed_rows or len(removed_indices) >= len(selected):
                return None
            kept_indices = all_indices - removed_indices
            kept_metric = metric_indices(prepared, kept_indices)
            if _passes_non_concentration_gates(kept_metric):
                kept_metric = metric_indices(prepared, kept_indices, include_concentration=True)
            removed_metric = metric_indices(prepared, removed_indices)
            precision_gain = float(kept_metric["success_rate"]) - float(baseline["success_rate"])
            accepted = _accepted(kept_metric)
            removed_failures = int(removed_metric["failure_count"])
            ordered_guard = tuple(sorted(guard))
            return {
                "threshold": threshold,
                "guard_terms": len(ordered_guard),
                "guard": " & ".join(f"exclude({atom})" for atom in ordered_guard),
                "baseline_rows": baseline["rows"],
                "baseline_success_rate": baseline["success_rate"],
                "baseline_wilson_lower_95": baseline["wilson_lower_95"],
                "baseline_failure_count": baseline["failure_count"],
                "kept_rows": kept_metric["rows"],
                "kept_sessions": kept_metric["sessions"],
                "kept_success_count": kept_metric["success_count"],
                "kept_success_rate": kept_metric["success_rate"],
                "kept_wilson_lower_95": kept_metric["wilson_lower_95"],
                "kept_mean_result_bps": kept_metric["mean_result_bps"],
                "removed_rows": removed_metric["rows"],
                "removed_success_rate": removed_metric["success_rate"],
                "removed_failure_count": removed_failures,
                "removed_failure_share": (
                    removed_failures / baseline_failures
                    if baseline_failures
                    else 0.0
                ),
                "precision_gain": precision_gain,
                "missing_successes_to_90_current_rows": _missing_successes_to_90(
                    int(kept_metric["success_count"]),
                    int(kept_metric["rows"]),
                ),
                "accepted_shadow": accepted,
                "product_claim_allowed": False,
                "status": _status(accepted=accepted, kept=kept_metric, baseline=baseline),
            }

        single_guards: list[dict[str, Any]] = []
        for atom, indices in atom_index.items():
            if len(indices) < min_removed_rows or len(indices) >= len(selected):
                continue
            candidate = evaluate_guard(frozenset((atom,)))
            if candidate is not None:
                single_guards.append(candidate)
                result.append(candidate)

        current_level = [
            frozenset((str(row["guard"]).removeprefix("exclude(").removesuffix(")"),))
            for row in sorted(single_guards, key=_guard_sort_key, reverse=True)[:beam_width]
            if " & " not in str(row["guard"])
        ]
        frequent_atoms = sorted({next(iter(guard)) for guard in current_level})
        seen = set(current_level)
        for _size in range(2, max(1, max_guard_terms) + 1):
            generated: list[dict[str, Any]] = []
            next_level: list[frozenset[str]] = []
            for guard in current_level:
                last_atom = max(guard)
                for atom in frequent_atoms:
                    if atom <= last_atom or atom in guard:
                        continue
                    merged = frozenset(set(guard) | {atom})
                    if merged in seen or _redundant_guard(tuple(merged)):
                        continue
                    seen.add(merged)
                    candidate = evaluate_guard(merged)
                    if candidate is None:
                        continue
                    generated.append(candidate)
                    next_level.append(merged)
            result.extend(generated)
            current_level = [
                guard
                for _, guard in sorted(
                    zip(generated, next_level),
                    key=lambda item: _guard_sort_key(item[0]),
                    reverse=True,
                )[:beam_width]
            ]
            if not current_level:
                break
    return sorted(
        result,
        key=_guard_sort_key,
        reverse=True,
    )[:top_n]


def summarize_guards(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    status_counts = Counter(str(row.get("status", "")) for row in rows)
    return {
        "candidate_rows": len(rows),
        "accepted_shadow": sum(1 for row in rows if str(row.get("accepted_shadow")).lower() == "true"),
        "status_counts": dict(sorted(status_counts.items())),
        "guard_term_counts": dict(sorted(Counter(str(row.get("guard_terms", "")) for row in rows).items())),
        "best_precision_gain": max((float(row.get("precision_gain") or 0.0) for row in rows), default=0.0),
        "best_success_rate_after_guard": max((float(row.get("kept_success_rate") or 0.0) for row in rows), default=0.0),
        "top_guards": [dict(row) for row in rows[:10]],
    }


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "threshold",
        "guard_terms",
        "guard",
        "baseline_rows",
        "baseline_success_rate",
        "baseline_wilson_lower_95",
        "baseline_failure_count",
        "kept_rows",
        "kept_sessions",
        "kept_success_count",
        "kept_success_rate",
        "kept_wilson_lower_95",
        "kept_mean_result_bps",
        "removed_rows",
        "removed_success_rate",
        "removed_failure_count",
        "removed_failure_share",
        "precision_gain",
        "missing_successes_to_90_current_rows",
        "accepted_shadow",
        "product_claim_allowed",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _pct(value: object) -> str:
    return f"{_float_or_zero(value) * 100:.2f}%"


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    lines = [
        "# Исключающие правила для ложных срабатываний",
        "",
        f"- Проверено правил: {summary.get('candidate_rows')}",
        f"- Кандидатов для теневого режима: {summary.get('accepted_shadow')}",
        f"- Лучший прирост доли успеха: {_pct(summary.get('best_precision_gain'))}",
        f"- Лучшая доля успеха после исключения: {_pct(summary.get('best_success_rate_after_guard'))}",
        f"- Статусы: `{json.dumps(summary.get('status_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        f"- Число условий в исключении: `{json.dumps(summary.get('guard_term_counts', {}), ensure_ascii=False, sort_keys=True)}`",
        "",
        "## Лучшие исключения",
        "",
        "| Порог | Что исключить | Осталось | Доля успеха | Нижняя граница | Прирост | Статус |",
        "|---:|---|---:|---:|---:|---:|---|",
    ]
    for row in summary.get("top_guards", []):
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {threshold:.2f} | `{guard}` | {kept_rows} | {success_rate} | {lower} | {gain} | `{status}` |".format(
                threshold=_float_or_zero(row.get("threshold")),
                guard=row.get("guard"),
                kept_rows=row.get("kept_rows"),
                success_rate=_pct(row.get("kept_success_rate")),
                lower=_pct(row.get("kept_wilson_lower_95")),
                gain=_pct(row.get("precision_gain")),
                status=row.get("status"),
            )
        )
    lines.extend(
        [
            "",
            "## Вывод",
            "",
            "Этот отчёт не разрешает продуктовый вывод сам по себе. Он показывает, какие состояния стоит исключать или вынести в отдельную будущую проверку. Правило можно использовать только после независимой проверки на следующих торговых днях.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-mine-false-positive-guards")
    parser.add_argument("--decision-audit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--min-removed-rows", type=int, default=20)
    parser.add_argument("--max-guard-terms", type=int, default=2)
    parser.add_argument("--beam-width", type=int, default=50)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument(
        "--thresholds",
        default=",".join(str(item) for item in DEFAULT_THRESHOLDS),
        help="Comma-separated confidence thresholds.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    thresholds = tuple(float(item.strip()) for item in str(args.thresholds).split(",") if item.strip())
    rows = _read_csv(args.decision_audit)
    guard_rows = mine_false_positive_guards(
        rows,
        thresholds=thresholds,
        min_removed_rows=args.min_removed_rows,
        max_guard_terms=args.max_guard_terms,
        beam_width=args.beam_width,
        top_n=args.top_n,
    )
    summary = summarize_guards(guard_rows)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "false-positive-guards.csv", guard_rows)
    (args.output_dir / "false-positive-guards.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "false_positive_guard_report",
                "summary": summary,
                "rows": guard_rows,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    write_report(args.output_dir / "false-positive-guards.md", summary)
    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "candidate_rows": summary["candidate_rows"],
                "accepted_shadow": summary["accepted_shadow"],
                "status": "ok",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
