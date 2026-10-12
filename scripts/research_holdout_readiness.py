#!/usr/bin/env python3
"""Evaluate whether an order-book holdout is ready for high-confidence research."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import (  # noqa: E402
    holdout_readiness_summary,
    write_csv_records,
    write_json,
)


def _load_coverage_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, Mapping) else None
    if not isinstance(rows, list):
        raise RuntimeError(f"Coverage file {path} must contain a top-level rows list")
    return [dict(row) for row in rows]


def _csv_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["reason_codes"] = json.dumps(item.get("reason_codes", []), ensure_ascii=False)
        result.append(item)
    return result


def _write_report(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    ready_rows = [row for row in rows if row.get("ready")]
    lines = [
        "# Holdout readiness for liquidity-aware signal research",
        "",
        f"- Ready windows: {len(ready_rows)}",
        "",
        "| Max age, seconds | Covered signals | Missing signals | Covered sessions | Missing sessions | Coverage | Ready | Reasons |",
        "|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        lines.append(
            "| {age} | {signals} | {missing_signals} | {sessions} | {missing_sessions} | {coverage:.4f} | {ready} | {reasons} |".format(
                age=row["max_age_seconds"],
                signals=row["covered_signals"],
                missing_signals=row.get("missing_covered_signals", ""),
                sessions=row["covered_sessions"],
                missing_sessions=row.get("missing_covered_sessions", ""),
                coverage=float(row["coverage"]),
                ready=row["ready"],
                reasons=", ".join(row.get("reason_codes", [])),
            )
        )
    lines.extend(
        [
            "",
            "A ready holdout only means there is enough order-book coverage to run the "
            "liquidity-aware research. It does not mean a 90% signal has been proven.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-holdout-readiness")
    parser.add_argument(
        "--coverage-json",
        type=Path,
        default=Path("var/research/orderbook_coverage/current/coverage.json"),
    )
    parser.add_argument("--min-covered-signals", type=int, default=300)
    parser.add_argument("--min-covered-sessions", type=int, default=30)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--preferred-max-age-seconds", type=int, default=30)
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/holdout_readiness/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    coverage_rows = _load_coverage_rows(args.coverage_json)
    rows = holdout_readiness_summary(
        coverage_rows,
        min_covered_signals=args.min_covered_signals,
        min_covered_sessions=args.min_covered_sessions,
        min_coverage=args.min_coverage,
        preferred_max_age_seconds=args.preferred_max_age_seconds,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "kind": "orderbook_holdout_readiness",
        "coverage_json": str(args.coverage_json),
        "ready": any(row["ready"] for row in rows),
        "rows": rows,
    }
    write_json(args.output_dir / "readiness.json", payload)
    write_csv_records(args.output_dir / "readiness.csv", _csv_rows(rows))
    _write_report(args.output_dir / "readiness-report.md", rows)
    print(
        json.dumps(
            {
                "status": "ok",
                "ready": payload["ready"],
                "output_dir": str(args.output_dir),
                "ready_windows": sum(1 for row in rows if row["ready"]),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
