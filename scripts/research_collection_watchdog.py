#!/usr/bin/env python3
"""Watchdog for scheduled liquidity collection runs."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_dt(value: object) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _int(value: object) -> int:
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _recovery_command(schedule_status: Mapping[str, Any]) -> str:
    shell_script = schedule_status.get("shell_script")
    if isinstance(shell_script, Mapping) and shell_script.get("path"):
        return str(shell_script["path"])
    return ""


def _scheduler_loaded(payload: Mapping[str, Any]) -> bool:
    return bool(
        payload.get("scheduler_loaded")
        or payload.get("launchd_loaded")
        or payload.get("systemd_loaded")
    )


def build_watchdog_report(
    *,
    live_status: Mapping[str, Any],
    schedule_status: Mapping[str, Any],
    grace_minutes: int = 5,
) -> dict[str, Any]:
    now = _parse_dt(live_status.get("now_moscow")) or datetime.now().astimezone()
    start = _parse_dt(live_status.get("recommended_start_moscow") or schedule_status.get("recommended_start_moscow"))
    end = _parse_dt(live_status.get("recommended_end_moscow"))
    start_grace_deadline = start + timedelta(minutes=grace_minutes) if start else None
    running = bool(live_status.get("running_collectors"))
    log_exists = bool(live_status.get("log_exists"))
    cache_files = live_status.get("cache_files") if isinstance(live_status.get("cache_files"), Mapping) else {}
    updated_after_start = _int(cache_files.get("files_updated_after_recommended_start"))
    scheduler_loaded = _scheduler_loaded(live_status) or _scheduler_loaded(schedule_status)
    recovery = _recovery_command(schedule_status)

    if not scheduler_loaded:
        status = "scheduler_not_loaded"
        next_action = "load_scheduler"
        severity = "error"
        reason = "Планировщик не загружен; запуск по расписанию не гарантирован."
    elif start and now < start:
        status = "waiting_for_start"
        next_action = "wait"
        severity = "info"
        reason = "Рекомендуемое время старта ещё не наступило."
    elif running:
        status = "collector_running"
        next_action = "monitor_progress"
        severity = "ok"
        reason = "Процесс сбора активен."
    elif updated_after_start > 0:
        status = "data_updated_after_start"
        next_action = "monitor_or_refresh_reports"
        severity = "ok"
        reason = "После стартового времени появились обновлённые parquet-файлы."
    elif start_grace_deadline and now <= start_grace_deadline and (scheduler_loaded or log_exists):
        status = "within_start_grace"
        next_action = "wait_for_launchd"
        severity = "info"
        reason = "Стартовое время прошло, но grace-период ещё не истёк."
    elif start and now > start and not log_exists and updated_after_start == 0:
        status = "scheduled_start_missed"
        next_action = "run_recovery_command"
        severity = "error"
        reason = "Стартовое время прошло, но нет лога и новых данных."
    elif end and now > end and updated_after_start == 0:
        status = "collection_window_missed"
        next_action = "reschedule_next_session"
        severity = "error"
        reason = "Окно сбора завершилось без новых данных."
    else:
        status = "needs_operator_review"
        next_action = "inspect_log_and_processes"
        severity = "warning"
        reason = "Состояние не доказывает активный сбор и не классифицируется как нормальное ожидание."

    return {
        "schema_version": 1,
        "kind": "liquidity_collection_watchdog",
        "status": status,
        "severity": severity,
        "next_action": next_action,
        "reason": reason,
        "now_moscow": now.isoformat(),
        "recommended_start_moscow": start.isoformat() if start else "",
        "recommended_end_moscow": end.isoformat() if end else "",
        "start_grace_deadline_moscow": start_grace_deadline.isoformat() if start_grace_deadline else "",
        "launchd_loaded": live_status.get("launchd_loaded") or schedule_status.get("launchd_loaded"),
        "systemd_loaded": live_status.get("systemd_loaded") or schedule_status.get("systemd_loaded"),
        "scheduler_loaded": scheduler_loaded,
        "running_collectors": live_status.get("running_collectors", []),
        "log_exists": log_exists,
        "files_updated_after_recommended_start": updated_after_start,
        "recovery_command": recovery,
        "launchctl_load_command": schedule_status.get("launchctl_load_command", ""),
        "launchctl_unload_command": schedule_status.get("launchctl_unload_command", ""),
        "systemd_install_user_command": schedule_status.get("systemd_install_user_command", ""),
        "systemd_disable_user_command": schedule_status.get("systemd_disable_user_command", ""),
    }


def write_markdown(path: Path, report: Mapping[str, Any]) -> None:
    lines = [
        "# Watchdog сбора стакана",
        "",
        f"- Статус: `{report.get('status')}`",
        f"- Важность: `{report.get('severity')}`",
        f"- Следующее действие: `{report.get('next_action')}`",
        f"- Причина: {report.get('reason')}",
        f"- Сейчас: {report.get('now_moscow')}",
        f"- Старт: {report.get('recommended_start_moscow')}",
        f"- Grace до: {report.get('start_grace_deadline_moscow')}",
        f"- `launchd` загружен: {'да' if report.get('launchd_loaded') else 'нет'}",
        f"- `systemd` загружен: {'да' if report.get('systemd_loaded') else 'нет'}",
        f"- Планировщик загружен: {'да' if report.get('scheduler_loaded') else 'нет'}",
        f"- Лог существует: {'да' if report.get('log_exists') else 'нет'}",
        f"- Файлов обновлено после старта: {report.get('files_updated_after_recommended_start')}",
        "",
        "## Команды восстановления",
        "",
    ]
    if report.get("recovery_command"):
        lines.append(f"- Запустить сбор вручную: `{report.get('recovery_command')}`")
    if report.get("launchctl_load_command"):
        lines.append(f"- Загрузить launchd: `{report.get('launchctl_load_command')}`")
    if report.get("systemd_install_user_command"):
        lines.append(f"- Включить systemd user timer: `{report.get('systemd_install_user_command')}`")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_watchdog(
    *,
    live_status_path: Path,
    schedule_status_path: Path,
    output_dir: Path,
    grace_minutes: int,
) -> dict[str, Any]:
    report = build_watchdog_report(
        live_status=_read_json(live_status_path),
        schedule_status=_read_json(schedule_status_path),
        grace_minutes=grace_minutes,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "collection-watchdog.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "collection-watchdog.md", report)
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-collection-watchdog")
    parser.add_argument("--live-status", type=Path, default=Path("var/research/liquidity_holdout/current/live_status/live-status.json"))
    parser.add_argument("--schedule-status", type=Path, default=Path("var/research/liquidity_holdout/current/collection_plan/schedule-status.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current/watchdog"))
    parser.add_argument("--grace-minutes", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    report = write_watchdog(
        live_status_path=args.live_status,
        schedule_status_path=args.schedule_status,
        output_dir=args.output_dir,
        grace_minutes=args.grace_minutes,
    )
    print(
        json.dumps(
            {
                "status": report["status"],
                "severity": report["severity"],
                "next_action": report["next_action"],
                "output_dir": str(args.output_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
