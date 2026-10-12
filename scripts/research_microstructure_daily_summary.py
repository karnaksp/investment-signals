#!/usr/bin/env python3
"""Write a concise daily summary for liquidity-aware 90% research readiness."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _int_value(row: Mapping[str, Any], key: str) -> int:
    try:
        return int(row.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


def _float_value(row: Mapping[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _preferred_coverage_row(rows: Sequence[Mapping[str, Any]], preferred_max_age_seconds: int) -> dict[str, Any]:
    preferred = [dict(row) for row in rows if _int_value(row, "max_age_seconds") <= preferred_max_age_seconds]
    candidates = preferred or [dict(row) for row in rows]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            _float_value(row, "coverage"),
            _int_value(row, "covered_signals"),
            _int_value(row, "covered_sessions"),
        ),
    )


def _preferred_readiness_row(rows: Sequence[Mapping[str, Any]], preferred_max_age_seconds: int) -> dict[str, Any]:
    preferred = [dict(row) for row in rows if _int_value(row, "max_age_seconds") <= preferred_max_age_seconds]
    candidates = preferred or [dict(row) for row in rows]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            bool(row.get("ready")),
            _float_value(row, "coverage"),
            _int_value(row, "covered_signals"),
            _int_value(row, "covered_sessions"),
        ),
    )


def _worst_ticker_days(rows: Sequence[Mapping[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in sorted(
            rows,
            key=lambda row: (
                _float_value(row, "coverage"),
                -_int_value(row, "signals"),
                str(row.get("ticker", "")),
                str(row.get("trading_day", "")),
            ),
        )[:limit]
    ]


def _next_action(*, ready: bool, coverage: Mapping[str, Any], status: Mapping[str, Any]) -> str:
    if ready:
        return "run_liquidity_aware_research"
    if _int_value(coverage, "covered_signals") <= 0 and _int_value(coverage, "orderbook_snapshots") > 0:
        return "fix_collection_window_before_collecting_more"
    missing_reasons = status.get("missing_reasons")
    if isinstance(missing_reasons, list) and "liquidity_holdout_not_ready" in missing_reasons:
        return "continue_dense_orderbook_collection"
    return "refresh_status_after_collection"


def build_daily_summary(
    *,
    coverage_path: Path,
    readiness_path: Path,
    signal_status_path: Path,
    collection_plan_path: Path,
) -> dict[str, Any]:
    coverage_payload = _load_json(coverage_path)
    readiness_payload = _load_json(readiness_path)
    signal_status = _load_json(signal_status_path)
    collection_plan = _load_json(collection_plan_path)
    preferred_max_age_seconds = int(collection_plan.get("preferred_max_age_seconds") or 30)
    coverage_rows = coverage_payload.get("rows") if isinstance(coverage_payload.get("rows"), list) else []
    by_day_rows = (
        coverage_payload.get("by_ticker_day")
        if isinstance(coverage_payload.get("by_ticker_day"), list)
        else []
    )
    readiness_rows = readiness_payload.get("rows") if isinstance(readiness_payload.get("rows"), list) else []
    coverage = _preferred_coverage_row(coverage_rows, preferred_max_age_seconds)
    readiness = _preferred_readiness_row(readiness_rows, preferred_max_age_seconds)
    ready = bool(readiness_payload.get("ready")) or bool(readiness.get("ready"))
    missing_covered_signals = _int_value(readiness, "missing_covered_signals")
    missing_covered_sessions = _int_value(readiness, "missing_covered_sessions")
    return {
        "schema_version": 1,
        "kind": "microstructure_daily_summary",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "ready_for_liquidity_research" if ready else "collect_more_data",
        "ready": ready,
        "preferred_max_age_seconds": preferred_max_age_seconds,
        "coverage_json": str(coverage_path),
        "readiness_json": str(readiness_path),
        "signal_status_json": str(signal_status_path),
        "collection_plan_json": str(collection_plan_path),
        "covered_signals": _int_value(coverage, "covered_signals"),
        "total_signals": _int_value(coverage, "signals"),
        "coverage": _float_value(coverage, "coverage"),
        "covered_sessions": _int_value(coverage, "covered_sessions"),
        "orderbook_snapshots": _int_value(coverage, "orderbook_snapshots"),
        "nearest_prior_orderbook_age_seconds": coverage.get("nearest_prior_orderbook_age_seconds", ""),
        "nearest_signal_orderbook_gap_seconds": coverage.get("nearest_signal_orderbook_gap_seconds", ""),
        "missing_covered_signals": missing_covered_signals,
        "missing_covered_sessions": missing_covered_sessions,
        "required_covered_signals": _int_value(readiness, "required_covered_signals"),
        "required_covered_sessions": _int_value(readiness, "min_covered_sessions"),
        "readiness_reasons": readiness.get("reason_codes", []),
        "product_claim_allowed": bool(signal_status.get("product_claim_allowed")),
        "signal_status": signal_status.get("status", "missing"),
        "signal_missing_reasons": signal_status.get("missing_reasons", []),
        "next_recommended_start_moscow": (
            collection_plan.get("collection_window_preflight", {})
            if isinstance(collection_plan.get("collection_window_preflight"), Mapping)
            else {}
        ).get("recommended_start_moscow"),
        "next_action": _next_action(ready=ready, coverage=coverage, status=signal_status),
        "worst_ticker_days": _worst_ticker_days(by_day_rows),
    }


def write_report(path: Path, summary: Mapping[str, Any]) -> None:
    lines = [
        "# Дневной отчёт по стакану",
        "",
        f"- Статус: `{summary.get('status')}`",
        f"- Можно запускать исследование со стаканом: {'да' if summary.get('ready') else 'нет'}",
        f"- Покрыто сигналов: {summary.get('covered_signals')} из {summary.get('total_signals')}",
        f"- Покрытие: {float(summary.get('coverage', 0.0) or 0.0):.4f}",
        f"- Покрыто торговых дней: {summary.get('covered_sessions')}",
        f"- Не хватает сигналов: {summary.get('missing_covered_signals')}",
        f"- Не хватает торговых дней: {summary.get('missing_covered_sessions')}",
        f"- Снимков стакана: {summary.get('orderbook_snapshots')}",
        f"- Ближайший предшествующий снимок, секунд: {summary.get('nearest_prior_orderbook_age_seconds')}",
        f"- Следующее действие: `{summary.get('next_action')}`",
        f"- Следующий рекомендуемый старт: {summary.get('next_recommended_start_moscow')}",
        "",
        "## Причины неготовности",
        "",
    ]
    reasons = summary.get("readiness_reasons")
    if isinstance(reasons, list) and reasons:
        lines.extend(f"- `{reason}`" for reason in reasons)
    else:
        lines.append("- нет")
    lines.extend(
        [
            "",
            "## Худшие тикер-дни",
            "",
            "| Тикер | День | Сигналы | Покрыто | Покрытие | Первый сигнал | Первый снимок стакана |",
            "|---|---|---:|---:|---:|---|---|",
        ]
    )
    for row in summary.get("worst_ticker_days", []):
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {ticker} | {day} | {signals} | {covered} | {coverage:.4f} | {first_signal} | {first_snapshot} |".format(
                ticker=row.get("ticker", ""),
                day=row.get("trading_day", ""),
                signals=row.get("signals", 0),
                covered=row.get("covered_signals", 0),
                coverage=float(row.get("coverage", 0.0) or 0.0),
                first_signal=row.get("first_signal_at", ""),
                first_snapshot=row.get("first_orderbook_at", ""),
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-microstructure-daily-summary")
    parser.add_argument("--coverage-json", type=Path, default=Path("var/research/liquidity_holdout/current/coverage/coverage.json"))
    parser.add_argument("--readiness-json", type=Path, default=Path("var/research/liquidity_holdout/current/readiness/readiness.json"))
    parser.add_argument("--signal-status", type=Path, default=Path("var/research/signal_90_status/current/signal-90-status.json"))
    parser.add_argument("--collection-plan", type=Path, default=Path("var/research/liquidity_holdout/current/collection_plan/collection-plan.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current/daily_summary"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    summary = build_daily_summary(
        coverage_path=args.coverage_json,
        readiness_path=args.readiness_json,
        signal_status_path=args.signal_status,
        collection_plan_path=args.collection_plan,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "daily-summary.json"
    report_path = args.output_dir / "daily-summary.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_report(report_path, summary)
    print(
        json.dumps(
            {
                "status": summary["status"],
                "ready": summary["ready"],
                "covered_signals": summary["covered_signals"],
                "covered_sessions": summary["covered_sessions"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
