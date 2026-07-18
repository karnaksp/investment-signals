#!/usr/bin/env python3
"""Run a short order-book collection only while the local product is alive.

This is offline research tooling. It intentionally stays outside production
runtime code and writes only local research artifacts. The goal is to accumulate
microstructure data opportunistically while a self-hosted installation is
already running, without blocking for a full trading session.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import DEFAULT_RESEARCH_TICKERS, UTC, write_json  # noqa: E402


HealthChecker = Callable[[str, float], bool]
CommandRunner = Callable[[Sequence[str]], Mapping[str, Any]]


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _http_health_ok(url: str, timeout_seconds: float) -> bool:
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - user-provided local health URL
            return 200 <= int(response.status) < 400
    except (OSError, urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return False


def _process_marker_ok(marker: str, process_output: str | None = None) -> bool:
    output = process_output
    if output is None:
        completed = subprocess.run(  # noqa: S603 - fixed executable and arguments
            ["ps", "-axo", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
        output = completed.stdout
    ignored_helpers = (
        "research_passive_orderbook_collector.py",
        "research_passive_orderbook_loop.py",
        "research_plan_passive_orderbook_collection.py",
    )
    return any(
        marker in line and not any(helper in line for helper in ignored_helpers)
        for line in output.splitlines()
    )


def service_available(
    *,
    health_urls: Sequence[str],
    process_markers: Sequence[str],
    allow_without_service_check: bool,
    health_timeout_seconds: float,
    process_output: str | None = None,
    health_checker: HealthChecker = _http_health_ok,
) -> tuple[bool, str, dict[str, Any]]:
    """Return whether a short passive collection is allowed now."""

    checks: dict[str, Any] = {
        "health_urls": [],
        "process_markers": [],
        "allow_without_service_check": bool(allow_without_service_check),
    }
    if not health_urls and not process_markers:
        if allow_without_service_check:
            return True, "service_check_bypassed", checks
        return False, "service_check_required", checks

    for url in health_urls:
        ok = health_checker(url, health_timeout_seconds)
        checks["health_urls"].append({"url": url, "available": ok})
        if ok:
            return True, "health_url_available", checks

    for marker in process_markers:
        ok = _process_marker_ok(marker, process_output=process_output)
        checks["process_markers"].append({"marker": marker, "available": ok})
        if ok:
            return True, "process_marker_available", checks

    return False, "service_not_running", checks


def build_orderbook_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(SCRIPT_DIR / "research_collect_tinvest_orderbook_snapshots.py"),
        "--env-file",
        str(args.env_file),
        "--cache-dir",
        str(args.cache_dir),
        "--tickers",
        ",".join(args.tickers),
        "--depth",
        str(args.depth),
        "--samples",
        str(args.samples),
        "--interval-seconds",
        str(args.interval_seconds),
        "--request-timeout",
        str(args.request_timeout),
        "--request-attempts",
        str(args.request_attempts),
        "--flush-every-samples",
        str(args.flush_every_samples),
    ]
    if args.ca_cert is not None:
        command.extend(["--ca-cert", str(args.ca_cert)])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    return command


def _run_json_command(command: Sequence[str]) -> Mapping[str, Any]:
    completed = subprocess.run(  # noqa: S603 - command is built from fixed local script plus user CLI values
        list(command),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return {
            "status": "failed",
            "reason_code": "orderbook_collector_failed",
            "returncode": completed.returncode,
            "stderr_tail": completed.stderr[-4000:],
        }
    last_json_line = ""
    for line in completed.stdout.splitlines():
        if line.strip().startswith("{") and line.strip().endswith("}"):
            last_json_line = line.strip()
    if not last_json_line:
        return {
            "status": "failed",
            "reason_code": "orderbook_collector_no_json_result",
            "stdout_tail": completed.stdout[-4000:],
        }
    return json.loads(last_json_line)


def _write_report(path: Path, result: Mapping[str, Any]) -> None:
    status = result.get("status")
    reason = result.get("reason_code", result.get("reason", ""))
    rows = result.get("collector_result", {}).get("rows_collected", 0)
    cache_dir = result.get("cache_dir", "")
    lines = [
        "# Пассивный сбор стакана",
        "",
        f"- Статус: `{status}`",
        f"- Причина: `{reason}`",
        f"- Собрано строк: `{rows}`",
        f"- Кэш: `{cache_dir}`",
        "",
        "Сбор запускается короткими проходами только когда локальный продукт уже работает. "
        "Если сервис недоступен, утилита не обращается к T-Invest и сразу завершает работу.",
        "",
        "В артефактах не сохраняются токены, счета, FIGI и внутренние идентификаторы инструментов.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def run_passive_collection(
    args: argparse.Namespace,
    *,
    runner: CommandRunner = _run_json_command,
    process_output: str | None = None,
    health_checker: HealthChecker = _http_health_ok,
) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    available, reason, checks = service_available(
        health_urls=args.service_health_url,
        process_markers=args.service_process_marker,
        allow_without_service_check=args.allow_without_service_check,
        health_timeout_seconds=args.health_timeout_seconds,
        process_output=process_output,
        health_checker=health_checker,
    )
    base_result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "tinvest_research_passive_orderbook_collection",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "cache_dir": str(args.cache_dir),
        "checks": checks,
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
    }
    if not available:
        result = {
            **base_result,
            "status": "skipped",
            "reason_code": reason,
            "next_action": "start_product_then_retry_passive_collection",
        }
        write_json(args.output_dir / "passive-orderbook-result.json", result)
        _write_report(args.output_dir / "passive-orderbook-report.md", result)
        return result

    command = build_orderbook_command(args)
    collector_result = dict(runner(command))
    status = "ok" if collector_result.get("status") == "ok" else "failed"
    result = {
        **base_result,
        "finished_at": datetime.now(UTC).isoformat(),
        "status": status,
        "reason_code": reason if status == "ok" else collector_result.get("reason_code", "collector_failed"),
        "command": command,
        "collector_result": collector_result,
    }
    write_json(args.output_dir / "passive-orderbook-result.json", result)
    _write_report(args.output_dir / "passive-orderbook-report.md", result)
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-passive-orderbook-collector")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--tickers", type=_parse_csv, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--samples", type=int, default=4)
    parser.add_argument("--interval-seconds", type=float, default=15.0)
    parser.add_argument("--flush-every-samples", type=int, default=1)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=3)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--service-health-url", action="append", default=[])
    parser.add_argument("--service-process-marker", action="append", default=[])
    parser.add_argument("--health-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--allow-without-service-check", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/passive_orderbook/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = run_passive_collection(args)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] in {"ok", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
