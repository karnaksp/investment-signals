#!/usr/bin/env python3
"""Create a short-lived passive order-book collection schedule.

The generated schedule runs the passive collector every few minutes. Each run
first checks whether the product is alive; if it is not, the run exits without
calling T-Invest. This avoids full-day blocking collection while still allowing
the host to accumulate order-book features whenever the product is used.
"""

from __future__ import annotations

import argparse
import json
import plistlib
import shlex
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import DEFAULT_RESEARCH_TICKERS, MOSCOW, write_json  # noqa: E402


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _shell_command(command: Sequence[object]) -> str:
    return " ".join(shlex.quote(str(item)) for item in command)


def build_passive_collector_command(args: argparse.Namespace) -> list[object]:
    command: list[object] = [
        "uv",
        "run",
        "--extra",
        "research",
        "python",
        "scripts/research_passive_orderbook_collector.py",
        "--env-file",
        args.env_file,
        "--cache-dir",
        args.cache_dir,
        "--output-dir",
        args.passive_output_dir,
        "--tickers",
        ",".join(args.tickers),
        "--depth",
        args.depth,
        "--samples",
        args.samples,
        "--interval-seconds",
        args.sample_interval_seconds,
        "--flush-every-samples",
        args.flush_every_samples,
        "--request-timeout",
        args.request_timeout,
        "--request-attempts",
        args.request_attempts,
    ]
    for url in args.service_health_url:
        command.extend(["--service-health-url", url])
    for marker in args.service_process_marker:
        command.extend(["--service-process-marker", marker])
    if args.ca_cert is not None:
        command.extend(["--ca-cert", args.ca_cert])
    if args.insecure_skip_tls_verify:
        command.append("--insecure-skip-tls-verify")
    return command


def build_plan(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = args.output_dir.resolve()
    working_directory = args.working_directory.resolve()
    log_path = output_dir / "passive-orderbook-collector.log"
    shell_script = output_dir / "run-passive-orderbook-collector.sh"
    launchd_plist = output_dir / "com.investment-signals.research-passive-orderbook.plist"
    systemd_service = output_dir / "investment-signals-research-passive-orderbook.service"
    systemd_timer = output_dir / "investment-signals-research-passive-orderbook.timer"
    command = build_passive_collector_command(args)
    run_line = "cd {cwd} && {command} >> {log} 2>&1".format(
        cwd=shlex.quote(str(working_directory)),
        command=_shell_command(command),
        log=shlex.quote(str(log_path)),
    )
    interval_seconds = max(60, int(args.schedule_interval_seconds))
    return {
        "schema_version": 1,
        "kind": "passive_orderbook_collection_plan",
        "created_at": datetime.now(MOSCOW).isoformat(),
        "status": "ready",
        "mode": "short_passive_when_product_is_alive",
        "working_directory": str(working_directory),
        "schedule_interval_seconds": interval_seconds,
        "collection": {
            "tickers": list(args.tickers),
            "depth": int(args.depth),
            "samples_per_run": int(args.samples),
            "sample_interval_seconds": float(args.sample_interval_seconds),
            "flush_every_samples": int(args.flush_every_samples),
            "cache_dir": str(args.cache_dir),
            "output_dir": str(args.passive_output_dir),
        },
        "service_gate": {
            "health_urls": list(args.service_health_url),
            "process_markers": list(args.service_process_marker),
            "behavior_when_unavailable": "skip_without_tinvest_api_call",
        },
        "artifacts": {
            "output_dir": str(output_dir),
            "shell_script": str(shell_script),
            "launchd_plist": str(launchd_plist),
            "systemd_service": str(systemd_service),
            "systemd_timer": str(systemd_timer),
            "log_path": str(log_path),
        },
        "commands": {
            "run_once": run_line,
            "load_launchd": f"launchctl load {shlex.quote(str(launchd_plist))}",
            "unload_launchd": f"launchctl unload {shlex.quote(str(launchd_plist))}",
            "install_systemd_user": (
                "mkdir -p ~/.config/systemd/user && "
                f"cp {shlex.quote(str(systemd_service))} {shlex.quote(str(systemd_timer))} ~/.config/systemd/user/ && "
                "systemctl --user daemon-reload && "
                f"systemctl --user enable --now {shlex.quote(systemd_timer.name)}"
            ),
            "disable_systemd_user": (
                f"systemctl --user disable --now {shlex.quote(systemd_timer.name)}"
            ),
        },
        "privacy": {
            "tokens_persisted": False,
            "account_identifiers_persisted": False,
            "instrument_uids_persisted": False,
        },
    }


def _write_shell_script(path: Path, run_line: str) -> None:
    path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + run_line + "\n", encoding="utf-8")
    path.chmod(0o755)


def _write_launchd_plist(path: Path, *, label: str, shell_script: Path, interval_seconds: int, log_path: Path) -> None:
    payload = {
        "Label": label,
        "ProgramArguments": [str(shell_script)],
        "StartInterval": int(interval_seconds),
        "RunAtLoad": False,
        "StandardOutPath": str(log_path),
        "StandardErrorPath": str(log_path),
    }
    path.write_bytes(plistlib.dumps(payload, sort_keys=True))


def _write_systemd_units(
    *,
    service_path: Path,
    timer_path: Path,
    shell_script: Path,
    interval_seconds: int,
) -> None:
    service_path.write_text(
        "\n".join(
            [
                "[Unit]",
                "Description=Investment Signals passive research order-book collector",
                "Documentation=man:systemd.service(5)",
                "",
                "[Service]",
                "Type=oneshot",
                f"ExecStart={shell_script}",
                "Nice=10",
                "IOSchedulingClass=best-effort",
                "",
            ]
        ),
        encoding="utf-8",
    )
    timer_path.write_text(
        "\n".join(
            [
                "[Unit]",
                "Description=Run Investment Signals passive research order-book collector",
                "",
                "[Timer]",
                "OnBootSec=2min",
                f"OnUnitActiveSec={int(interval_seconds)}s",
                "AccuracySec=30s",
                "Persistent=false",
                "",
                "[Install]",
                "WantedBy=timers.target",
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_plan(plan: Mapping[str, Any]) -> None:
    artifacts = plan["artifacts"]
    output_dir = Path(str(artifacts["output_dir"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    shell_script = Path(str(artifacts["shell_script"]))
    launchd_plist = Path(str(artifacts["launchd_plist"]))
    systemd_service = Path(str(artifacts["systemd_service"]))
    systemd_timer = Path(str(artifacts["systemd_timer"]))
    log_path = Path(str(artifacts["log_path"]))
    _write_shell_script(shell_script, str(plan["commands"]["run_once"]))
    _write_launchd_plist(
        launchd_plist,
        label="com.investment-signals.research-passive-orderbook",
        shell_script=shell_script,
        interval_seconds=int(plan["schedule_interval_seconds"]),
        log_path=log_path,
    )
    _write_systemd_units(
        service_path=systemd_service,
        timer_path=systemd_timer,
        shell_script=shell_script,
        interval_seconds=int(plan["schedule_interval_seconds"]),
    )
    write_json(output_dir / "passive-collection-plan.json", plan)
    write_report(output_dir / "passive-collection-plan.md", plan)


def write_report(path: Path, plan: Mapping[str, Any]) -> None:
    collection = plan.get("collection") if isinstance(plan.get("collection"), Mapping) else {}
    service_gate = plan.get("service_gate") if isinstance(plan.get("service_gate"), Mapping) else {}
    artifacts = plan.get("artifacts") if isinstance(plan.get("artifacts"), Mapping) else {}
    commands = plan.get("commands") if isinstance(plan.get("commands"), Mapping) else {}
    lines = [
        "# План пассивного сбора стакана",
        "",
        f"- Статус: `{plan.get('status')}`",
        f"- Режим: `{plan.get('mode')}`",
        f"- Интервал запуска: {plan.get('schedule_interval_seconds')} секунд",
        f"- Тикеры: `{', '.join(collection.get('tickers', []))}`",
        f"- Снимков за запуск: {collection.get('samples_per_run')}",
        f"- Интервал между снимками: {collection.get('sample_interval_seconds')} секунд",
        f"- Кэш стакана: `{collection.get('cache_dir')}`",
        "",
        "## Проверка живого сервиса",
        "",
        f"- Health URL: `{', '.join(service_gate.get('health_urls', []))}`",
        f"- Маркеры процесса: `{', '.join(service_gate.get('process_markers', []))}`",
        "- Если сервис не работает, запуск завершается без обращения к T-Invest.",
        "",
        "## Артефакты",
        "",
        f"- Shell-скрипт: `{artifacts.get('shell_script')}`",
        f"- launchd plist: `{artifacts.get('launchd_plist')}`",
        f"- systemd service: `{artifacts.get('systemd_service')}`",
        f"- systemd timer: `{artifacts.get('systemd_timer')}`",
        f"- Лог: `{artifacts.get('log_path')}`",
        "",
        "## Команды",
        "",
        f"- Разовый запуск: `{commands.get('run_once')}`",
        f"- Включить расписание: `{commands.get('load_launchd')}`",
        f"- Выключить расписание: `{commands.get('unload_launchd')}`",
        f"- Включить systemd user timer: `{commands.get('install_systemd_user')}`",
        f"- Выключить systemd user timer: `{commands.get('disable_systemd_user')}`",
        "",
        "Этот режим не заменяет историческое исследование на свечах и не ждёт полный торговый день. "
        "Он только постепенно добавляет признаки стакана, пока продукт реально используется.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-plan-passive-orderbook-collection")
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/passive_orderbook/plan"))
    parser.add_argument("--working-directory", type=Path, default=Path.cwd())
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--passive-output-dir", type=Path, default=Path("var/research/passive_orderbook/current"))
    parser.add_argument("--tickers", type=_parse_csv, default=DEFAULT_RESEARCH_TICKERS)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--samples", type=int, default=4)
    parser.add_argument("--sample-interval-seconds", type=float, default=15.0)
    parser.add_argument("--flush-every-samples", type=int, default=1)
    parser.add_argument("--request-timeout", type=float, default=30.0)
    parser.add_argument("--request-attempts", type=int, default=3)
    parser.add_argument("--schedule-interval-seconds", type=int, default=300)
    parser.add_argument("--service-health-url", action="append", default=[])
    parser.add_argument("--service-process-marker", action="append", default=None)
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    args = parser.parse_args(argv)
    if args.service_process_marker is None:
        args.service_process_marker = ["investment-signals-pro"]
    return args


def main(argv: Sequence[str] | None = None) -> int:
    plan = build_plan(parse_args(argv))
    write_plan(plan)
    print(
        json.dumps(
            {
                "status": plan["status"],
                "mode": plan["mode"],
                "output_dir": plan["artifacts"]["output_dir"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
