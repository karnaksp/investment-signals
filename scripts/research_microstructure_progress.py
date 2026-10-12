#!/usr/bin/env python3
"""Summarize progress toward the microstructure evidence gate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _int(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _best_readiness_row(readiness: Mapping[str, Any]) -> dict[str, Any]:
    rows = readiness.get("rows")
    if not isinstance(rows, list):
        return {}
    candidates = [dict(row) for row in rows if isinstance(row, Mapping)]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda row: (
            bool(row.get("ready")),
            _int(row.get("covered_signals")),
            _int(row.get("covered_sessions")),
            _float(row.get("coverage")),
            -_int(row.get("max_age_seconds")),
        ),
    )


def build_progress_report(
    *,
    coverage: Mapping[str, Any],
    readiness: Mapping[str, Any],
    live_status: Mapping[str, Any],
    watchdog: Mapping[str, Any],
) -> dict[str, Any]:
    best = _best_readiness_row(readiness)
    covered_signals = _int(best.get("covered_signals"))
    required_signals = _int(best.get("min_covered_signals")) or 300
    covered_sessions = _int(best.get("covered_sessions"))
    required_sessions = _int(best.get("min_covered_sessions")) or 30
    orderbook_snapshots = _int(best.get("orderbook_snapshots"))
    missing_signals = max(0, required_signals - covered_signals)
    missing_sessions = max(0, required_sessions - covered_sessions)
    signal_progress = covered_signals / required_signals if required_signals else 0.0
    session_progress = covered_sessions / required_sessions if required_sessions else 0.0
    ready = bool(best.get("ready")) and missing_signals == 0 and missing_sessions == 0
    watchdog_status = str(watchdog.get("status", "unknown"))
    live_state = str(live_status.get("status", "unknown"))
    if ready:
        status = "ready_for_liquidity_retrain"
        next_action = "retrain_with_microstructure"
    elif watchdog_status in {
        "scheduled_start_missed",
        "collection_window_missed",
        "launchd_not_loaded",
        "scheduler_not_loaded",
    }:
        status = "collection_attention_required"
        next_action = str(watchdog.get("next_action", "inspect_watchdog"))
    elif live_state == "waiting_for_start":
        status = "waiting_for_collection_start"
        next_action = "wait_for_scheduled_start"
    elif orderbook_snapshots > 0 and covered_signals == 0:
        status = "collecting_but_no_prior_signal_coverage_yet"
        next_action = "continue_continuous_collection"
    else:
        status = "collect_more_microstructure"
        next_action = "continue_collection"
    return {
        "schema_version": 1,
        "kind": "microstructure_gate_progress",
        "status": status,
        "next_action": next_action,
        "ready": ready,
        "coverage_max_age_seconds": best.get("max_age_seconds"),
        "coverage": _float(best.get("coverage")),
        "covered_signals": covered_signals,
        "required_signals": required_signals,
        "missing_signals": missing_signals,
        "signal_progress": signal_progress,
        "covered_sessions": covered_sessions,
        "required_sessions": required_sessions,
        "missing_sessions": missing_sessions,
        "session_progress": session_progress,
        "orderbook_snapshots": orderbook_snapshots,
        "reason_codes": best.get("reason_codes", []),
        "live_status": live_state,
        "watchdog_status": watchdog_status,
        "watchdog_next_action": watchdog.get("next_action", ""),
        "by_ticker_day_rows": len(coverage.get("by_ticker_day", [])) if isinstance(coverage.get("by_ticker_day"), list) else 0,
    }


def _pct(value: object) -> str:
    return f"{_float(value) * 100:.2f}%"


def write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    lines = [
        "# Прогресс покрытия стаканом",
        "",
        f"- Статус: `{report.get('status')}`",
        f"- Следующее действие: `{report.get('next_action')}`",
        f"- Готово к retrain со стаканом: {'да' if report.get('ready') else 'нет'}",
        f"- Покрытые сигналы: {report.get('covered_signals')} из {report.get('required_signals')} ({_pct(report.get('signal_progress'))})",
        f"- Не хватает сигналов: {report.get('missing_signals')}",
        f"- Покрытые сессии: {report.get('covered_sessions')} из {report.get('required_sessions')} ({_pct(report.get('session_progress'))})",
        f"- Не хватает сессий: {report.get('missing_sessions')}",
        f"- Снимков стакана: {report.get('orderbook_snapshots')}",
        f"- Максимальный возраст prior-стакана: {report.get('coverage_max_age_seconds')} секунд",
        f"- Причины неготовности: `{json.dumps(report.get('reason_codes', []), ensure_ascii=False)}`",
        f"- Live-статус: `{report.get('live_status')}`",
        f"- Watchdog-статус: `{report.get('watchdog_status')}`",
        "",
        "## Вывод",
        "",
        "Для проверки 90% нужны не просто снимки стакана, а prior-снимки до сигналов. Прогресс считается по покрытым сигналам и сессиям, а не по числу файлов.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def write_progress(
    *,
    coverage_path: Path,
    readiness_path: Path,
    live_status_path: Path,
    watchdog_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    report = build_progress_report(
        coverage=_read_json(coverage_path),
        readiness=_read_json(readiness_path),
        live_status=_read_json(live_status_path),
        watchdog=_read_json(watchdog_path),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "microstructure-progress.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "microstructure-progress.md", report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-microstructure-progress")
    parser.add_argument("--coverage-json", type=Path, default=Path("var/research/liquidity_holdout/current/coverage/coverage.json"))
    parser.add_argument("--readiness-json", type=Path, default=Path("var/research/liquidity_holdout/current/readiness/readiness.json"))
    parser.add_argument("--live-status", type=Path, default=Path("var/research/liquidity_holdout/current/live_status/live-status.json"))
    parser.add_argument("--watchdog", type=Path, default=Path("var/research/liquidity_holdout/current/watchdog/collection-watchdog.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current/progress"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = write_progress(
        coverage_path=args.coverage_json,
        readiness_path=args.readiness_json,
        live_status_path=args.live_status,
        watchdog_path=args.watchdog,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "covered_signals": report["covered_signals"],
                "missing_signals": report["missing_signals"],
                "covered_sessions": report["covered_sessions"],
                "missing_sessions": report["missing_sessions"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
