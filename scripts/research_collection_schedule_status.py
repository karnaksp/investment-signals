#!/usr/bin/env python3
"""Check whether the liquidity collection schedule is ready to run."""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from zoneinfo import ZoneInfo


MOSCOW = ZoneInfo("Europe/Moscow")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _file_status(path: Path, *, executable: bool = False) -> dict[str, Any]:
    exists = path.exists()
    return {
        "path": str(path),
        "exists": exists,
        "is_file": path.is_file() if exists else False,
        "executable": os.access(path, os.X_OK) if exists else False,
        "ok": bool(exists and path.is_file() and (not executable or os.access(path, os.X_OK))),
    }


def _plist_status(path: Path) -> dict[str, Any]:
    base = _file_status(path)
    if not base["ok"]:
        return {**base, "valid": False, "label": ""}
    try:
        payload = plistlib.loads(path.read_bytes())
    except Exception as exc:  # pragma: no cover - exact plist parser errors vary
        return {**base, "valid": False, "label": "", "error": str(exc)}
    label = str(payload.get("Label", "")) if isinstance(payload, Mapping) else ""
    return {**base, "valid": bool(label), "label": label, "ok": bool(base["ok"] and label)}


def _launchctl_output() -> str:
    completed = subprocess.run(  # noqa: S603 - read-only system status command
        ["launchctl", "list"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if completed.returncode != 0:
        return ""
    return completed.stdout


def _launchd_loaded(label: str, launchctl_output: str | None) -> bool | None:
    if not label:
        return False
    output = _launchctl_output() if launchctl_output is None else launchctl_output
    if output == "":
        return None
    return label in output


def _systemctl_is_active_output(timer_name: str) -> str:
    try:
        completed = subprocess.run(  # noqa: S603 - read-only system status command
            ["systemctl", "--user", "is-active", timer_name],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        return ""
    return completed.stdout.strip()


def _systemd_timer_active(timer_path: Path, systemctl_output: str | None) -> bool | None:
    if not timer_path.name:
        return False
    output = _systemctl_is_active_output(timer_path.name) if systemctl_output is None else systemctl_output
    if output == "":
        return None
    return output.strip() == "active"


def _next_action(*, schedule_files_ok: bool, scheduler_loaded: bool | None, start_has_passed: bool) -> str:
    if not schedule_files_ok:
        return "fix_schedule_files"
    if scheduler_loaded is True:
        return "wait_for_scheduled_collection" if not start_has_passed else "inspect_collection_log"
    if scheduler_loaded is False:
        return "load_scheduler_before_recommended_start" if not start_has_passed else "load_scheduler_or_run_shell_script_now"
    return "verify_scheduler_manually"


def build_schedule_status(
    *,
    collection_plan_path: Path,
    now: datetime | None = None,
    launchctl_output: str | None = None,
    systemctl_output: str | None = None,
) -> dict[str, Any]:
    plan = _load_json(collection_plan_path)
    schedule = plan.get("schedule") if isinstance(plan.get("schedule"), Mapping) else {}
    preflight = (
        plan.get("collection_window_preflight")
        if isinstance(plan.get("collection_window_preflight"), Mapping)
        else {}
    )
    shell = _file_status(Path(str(schedule.get("shell_script", ""))), executable=True)
    cron = _file_status(Path(str(schedule.get("cron_file", ""))))
    plist = _plist_status(Path(str(schedule.get("launchd_plist", ""))))
    systemd_service = _file_status(Path(str(schedule.get("systemd_service", "")))) if schedule.get("systemd_service") else {}
    systemd_timer = _file_status(Path(str(schedule.get("systemd_timer", "")))) if schedule.get("systemd_timer") else {}
    log_path = Path(str(schedule.get("log_path", "")))
    label = str(plist.get("label", ""))
    loaded = _launchd_loaded(label, launchctl_output)
    systemd_loaded = (
        _systemd_timer_active(Path(str(schedule.get("systemd_timer", ""))), systemctl_output)
        if schedule.get("systemd_timer")
        else None
    )
    scheduler_loaded = (
        True
        if loaded is True or systemd_loaded is True
        else False
        if loaded is False or systemd_loaded is False
        else None
    )
    local_now = (now or datetime.now(MOSCOW)).astimezone(MOSCOW)
    recommended_start_raw = str(
        schedule.get("recommended_start_moscow")
        or preflight.get("recommended_start_moscow")
        or ""
    )
    scheduled_start_raw = str(
        schedule.get("scheduled_start_moscow")
        or schedule.get("recommended_start_moscow")
        or preflight.get("session_start_moscow")
        or recommended_start_raw
    )
    try:
        recommended_start = datetime.fromisoformat(recommended_start_raw).astimezone(MOSCOW)
    except ValueError:
        recommended_start = None
    start_has_passed = bool(recommended_start and local_now >= recommended_start)
    optional_systemd_ok = bool(
        (not systemd_service and not systemd_timer)
        or (systemd_service.get("ok") and systemd_timer.get("ok"))
    )
    schedule_files_ok = bool(shell["ok"] and cron["ok"] and plist["ok"] and optional_systemd_ok)
    status = (
        "ready_loaded"
        if schedule_files_ok and scheduler_loaded is True
        else "ready_not_loaded"
        if schedule_files_ok and scheduler_loaded is False
        else "ready_scheduler_unknown"
        if schedule_files_ok and scheduler_loaded is None
        else "invalid"
    )
    return {
        "schema_version": 1,
        "kind": "liquidity_collection_schedule_status",
        "created_at": datetime.now(MOSCOW).isoformat(),
        "status": status,
        "collection_plan": str(collection_plan_path),
        "recommended_start_moscow": recommended_start_raw,
        "scheduled_start_moscow": scheduled_start_raw,
        "weekday_start_local": schedule.get("weekday_start_local", ""),
        "now_moscow": local_now.isoformat(),
        "recommended_start_has_passed": start_has_passed,
        "schedule_files_ok": schedule_files_ok,
        "launchd_label": label,
        "launchd_loaded": loaded,
        "systemd_loaded": systemd_loaded,
        "scheduler_loaded": scheduler_loaded,
        "shell_script": shell,
        "cron_file": cron,
        "launchd_plist": plist,
        "systemd_service": systemd_service,
        "systemd_timer": systemd_timer,
        "log_path": str(log_path),
        "log_exists": log_path.exists(),
        "launchctl_load_command": schedule.get("launchctl_load_command", ""),
        "launchctl_unload_command": schedule.get("launchctl_unload_command", ""),
        "systemd_install_user_command": schedule.get("systemd_install_user_command", ""),
        "systemd_disable_user_command": schedule.get("systemd_disable_user_command", ""),
        "next_action": _next_action(
            schedule_files_ok=schedule_files_ok,
            scheduler_loaded=scheduler_loaded,
            start_has_passed=start_has_passed,
        ),
    }


def write_report(path: Path, status: Mapping[str, Any]) -> None:
    lines = [
        "# Статус расписания сбора стакана",
        "",
        f"- Статус: `{status.get('status')}`",
        f"- Следующее действие: `{status.get('next_action')}`",
        f"- Рекомендуемый старт: {status.get('recommended_start_moscow')}",
        f"- Регулярный старт расписания: {status.get('scheduled_start_moscow')} ({status.get('weekday_start_local')})",
        f"- Сейчас: {status.get('now_moscow')}",
        f"- Рекомендуемый старт уже прошёл: {'да' if status.get('recommended_start_has_passed') else 'нет'}",
        f"- Файлы расписания готовы: {'да' if status.get('schedule_files_ok') else 'нет'}",
        f"- launchd label: `{status.get('launchd_label')}`",
        f"- launchd загружен: {status.get('launchd_loaded')}",
        f"- systemd загружен: {status.get('systemd_loaded')}",
        f"- Любой планировщик загружен: {status.get('scheduler_loaded')}",
        f"- systemd service: `{dict(status.get('systemd_service') or {}).get('path', '')}`",
        f"- systemd timer: `{dict(status.get('systemd_timer') or {}).get('path', '')}`",
        f"- Лог: `{status.get('log_path')}`",
        f"- Лог существует: {'да' if status.get('log_exists') else 'нет'}",
        "",
        "## Команды",
        "",
        f"- Загрузить launchd: `{status.get('launchctl_load_command')}`",
        f"- Выгрузить launchd: `{status.get('launchctl_unload_command')}`",
        f"- Включить systemd user timer: `{status.get('systemd_install_user_command')}`",
        f"- Выключить systemd user timer: `{status.get('systemd_disable_user_command')}`",
        "",
        "## Файлы",
        "",
        "| Файл | Есть | Готов | Исполняемый |",
        "|---|---|---|---|",
    ]
    for key in ("shell_script", "cron_file", "launchd_plist", "systemd_service", "systemd_timer"):
        item = status.get(key) if isinstance(status.get(key), Mapping) else {}
        lines.append(
            "| `{path}` | {exists} | {ok} | {executable} |".format(
                path=item.get("path", ""),
                exists="да" if item.get("exists") else "нет",
                ok="да" if item.get("ok") else "нет",
                executable="да" if item.get("executable") else "нет",
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-collection-schedule-status")
    parser.add_argument(
        "--collection-plan",
        type=Path,
        default=Path("var/research/liquidity_holdout/current/collection_plan/collection-plan.json"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/liquidity_holdout/current/collection_plan"),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    status = build_schedule_status(collection_plan_path=args.collection_plan)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "schedule-status.json"
    report_path = args.output_dir / "schedule-status.md"
    json_path.write_text(json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_report(report_path, status)
    print(json.dumps({"status": status["status"], "next_action": status["next_action"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
