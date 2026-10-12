#!/usr/bin/env python3
"""Continuously run short passive order-book collection while the product lives.

This is offline research tooling. It does not change detector, delivery, or
trading behavior. Each loop iteration delegates to
``research_passive_orderbook_collector.py``. That collector checks whether the
local product is alive before calling T-Invest, so this loop can safely run in
the background on the same host as the product.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import DEFAULT_RESEARCH_TICKERS, UTC, write_json  # noqa: E402


CommandRunner = Callable[[Sequence[str]], Mapping[str, Any]]
Sleeper = Callable[[float], None]


DEFAULT_SERVICE_HEALTH_URLS = (
    "http://127.0.0.1:38000/health",
    "http://127.0.0.1:18080/health",
    "http://127.0.0.1:18443/health",
)
DEFAULT_SERVICE_PROCESS_MARKERS = (
    "tinvest-api",
    "investment-signals-pro",
)


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _run_json_command(command: Sequence[str]) -> Mapping[str, Any]:
    completed = subprocess.run(  # noqa: S603 - command is built from a fixed local script plus user CLI values
        list(command),
        check=False,
        capture_output=True,
        text=True,
    )
    last_json_line = ""
    for line in completed.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            last_json_line = stripped
    if completed.returncode != 0:
        return {
            "status": "failed",
            "reason_code": "passive_collector_process_failed",
            "returncode": completed.returncode,
            "stderr_tail": completed.stderr[-4000:],
            "stdout_tail": completed.stdout[-4000:],
        }
    if not last_json_line:
        return {
            "status": "failed",
            "reason_code": "passive_collector_no_json_result",
            "stdout_tail": completed.stdout[-4000:],
        }
    return json.loads(last_json_line)


def build_collector_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_passive_orderbook_collector.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.cache_dir),
        "--output-dir",
        str(args.passive_output_dir),
        "--tickers",
        ",".join(args.tickers),
        "--depth",
        str(args.depth),
        "--samples",
        str(args.samples),
        "--interval-seconds",
        str(args.sample_interval_seconds),
        "--flush-every-samples",
        str(args.flush_every_samples),
        "--request-timeout",
        str(args.request_timeout),
        "--request-attempts",
        str(args.request_attempts),
        "--health-timeout-seconds",
        str(args.health_timeout_seconds),
    ]
    for url in args.service_health_url:
        command.extend(["--service-health-url", url])
    for marker in args.service_process_marker:
        command.extend(["--service-process-marker", marker])
    if args.ca_cert is not None:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    if args.allow_without_service_check:
        command.append("--allow-without-service-check")
    return command


def _summary_from_iterations(iterations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ok_count = sum(1 for item in iterations if item.get("status") == "ok")
    skipped_count = sum(1 for item in iterations if item.get("status") == "skipped")
    failed_count = sum(1 for item in iterations if item.get("status") == "failed")
    rows = 0
    for item in iterations:
        collector = item.get("collector_result") if isinstance(item.get("collector_result"), Mapping) else {}
        try:
            rows += int(collector.get("rows_collected") or 0)
        except (TypeError, ValueError):
            continue
    return {
        "ok_iterations": ok_count,
        "skipped_iterations": skipped_count,
        "failed_iterations": failed_count,
        "rows_collected": rows,
    }


def _write_report(path: Path, payload: Mapping[str, Any]) -> None:
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    lines = [
        "# Пассивный фоновый сбор стакана",
        "",
        f"- Статус: `{payload.get('status')}`",
        f"- Следующее действие: `{payload.get('next_action')}`",
        f"- Итераций выполнено: {payload.get('iterations_completed')}",
        f"- Успешных итераций: {summary.get('ok_iterations')}",
        f"- Пропущенных итераций: {summary.get('skipped_iterations')}",
        f"- Ошибочных итераций: {summary.get('failed_iterations')}",
        f"- Строк стакана собрано: {summary.get('rows_collected')}",
        f"- Кэш: `{payload.get('cache_dir')}`",
        "",
        "Процесс можно держать рядом с работающим продуктом. Если продукт не отвечает, "
        "итерация пропускается без обращения к T-Invest.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_loop(
    args: argparse.Namespace,
    *,
    runner: CommandRunner = _run_json_command,
    sleeper: Sleeper = time.sleep,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    iterations: list[dict[str, Any]] = []
    target_iterations = max(0, int(args.iterations))
    command = build_collector_command(args)

    status = "running"
    next_action = "continue_background_collection"
    iteration_index = 0
    try:
        while target_iterations == 0 or iteration_index < target_iterations:
            iteration_index += 1
            started_iteration_at = datetime.now(UTC)
            result = dict(runner(command))
            iterations.append(
                {
                    "iteration": iteration_index,
                    "started_at": started_iteration_at.isoformat(),
                    "finished_at": datetime.now(UTC).isoformat(),
                    "status": result.get("status", "unknown"),
                    "reason_code": result.get("reason_code", ""),
                    "collector_result": result.get("collector_result", {}),
                }
            )
            summary = _summary_from_iterations(iterations)
            payload = {
                "schema_version": 1,
                "kind": "tinvest_research_passive_orderbook_loop",
                "status": status,
                "next_action": next_action,
                "started_at": started_at.isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
                "cache_dir": str(args.cache_dir),
                "collector_command": command,
                "iterations_completed": len(iterations),
                "target_iterations": target_iterations,
                "sleep_seconds": float(args.sleep_seconds),
                "summary": summary,
                "recent_iterations": iterations[-20:],
                "privacy": {
                    "tokens_persisted": False,
                    "account_identifiers_persisted": False,
                    "instrument_uids_persisted": False,
                },
            }
            write_json(output_dir / "passive-orderbook-loop-status.json", payload)
            _write_report(output_dir / "passive-orderbook-loop-report.md", payload)
            if target_iterations and iteration_index >= target_iterations:
                break
            sleeper(max(1.0, float(args.sleep_seconds)))
    except KeyboardInterrupt:
        status = "stopped"
        next_action = "inspect_collected_cache"

    summary = _summary_from_iterations(iterations)
    final = {
        "schema_version": 1,
        "kind": "tinvest_research_passive_orderbook_loop",
        "status": "completed" if target_iterations and len(iterations) >= target_iterations else status,
        "next_action": "inspect_collected_cache" if iterations else next_action,
        "started_at": started_at.isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
        "cache_dir": str(args.cache_dir),
        "collector_command": command,
        "iterations_completed": len(iterations),
        "target_iterations": target_iterations,
        "sleep_seconds": float(args.sleep_seconds),
        "summary": summary,
        "recent_iterations": iterations[-20:],
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
    }
    write_json(output_dir / "passive-orderbook-loop-status.json", final)
    _write_report(output_dir / "passive-orderbook-loop-report.md", final)
    return final


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-passive-orderbook-loop")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--passive-output-dir", type=Path, default=Path("var/research/passive_orderbook/current"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/passive_orderbook/loop"))
    parser.add_argument("--tickers", type=_parse_csv, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--samples", type=int, default=4)
    parser.add_argument("--sample-interval-seconds", type=float, default=15.0)
    parser.add_argument("--flush-every-samples", type=int, default=1)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=3)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--service-health-url", action="append", default=list(DEFAULT_SERVICE_HEALTH_URLS))
    parser.add_argument("--service-process-marker", action="append", default=list(DEFAULT_SERVICE_PROCESS_MARKERS))
    parser.add_argument("--health-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--allow-without-service-check", action="store_true")
    parser.add_argument(
        "--iterations",
        type=int,
        default=0,
        help="0 means run until interrupted.",
    )
    parser.add_argument("--sleep-seconds", type=float, default=300.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_loop(args)
    print(
        json.dumps(
            {
                "status": result["status"],
                "iterations_completed": result["iterations_completed"],
                "summary": result["summary"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if result["status"] in {"completed", "stopped", "running"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
