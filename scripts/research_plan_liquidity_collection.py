#!/usr/bin/env python3
"""Create an operator collection plan for liquidity-aware signal research."""

from __future__ import annotations

import argparse
import json
import math
import shlex
import sys
from datetime import datetime, time as datetime_time, timedelta
from html import escape
from pathlib import Path
from typing import Any, Mapping, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from research_price_prediction_lib import MOSCOW, REGULAR_SESSION_END, REGULAR_SESSION_START  # noqa: E402

LAUNCHD_MARKET_WEEKDAYS: tuple[int, ...] = (1, 2, 3, 4, 5)
"""launchd weekday numbers for Monday-Friday; 0 and 7 are Sunday."""


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _best_row(rows: Sequence[Mapping[str, Any]], preferred_max_age_seconds: int) -> Mapping[str, Any] | None:
    if not rows:
        return None
    preferred = [row for row in rows if int(row.get("max_age_seconds", 0) or 0) <= preferred_max_age_seconds]
    candidates = preferred or list(rows)
    return max(
        candidates,
        key=lambda row: (
            float(row.get("coverage", 0.0) or 0.0),
            int(row.get("covered_signals", 0) or 0),
            int(row.get("covered_sessions", 0) or 0),
        ),
    )


def _estimated_sessions_for_signal_target(missing_signals: int, covered_signals: int, covered_sessions: int) -> int | None:
    if missing_signals <= 0:
        return 0
    if covered_signals <= 0 or covered_sessions <= 0:
        return None
    signals_per_session = covered_signals / covered_sessions
    if signals_per_session <= 0:
        return None
    return math.ceil(missing_signals / signals_per_session)


def continuous_samples_for_hours(*, target_hours: float, interval_seconds: float) -> int:
    if target_hours <= 0:
        raise ValueError("target_hours must be positive")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    return math.ceil(target_hours * 3600 / interval_seconds)


def _next_weekday_start_after(local_now: datetime, session_start: datetime_time) -> datetime:
    candidate_date = local_now.date() + timedelta(days=1)
    while candidate_date.weekday() >= 5:
        candidate_date += timedelta(days=1)
    return datetime.combine(candidate_date, session_start, tzinfo=local_now.tzinfo)


def _recommended_collection_start(
    *,
    local_now: datetime,
    status: str,
    session_start_at: datetime,
    session_start: datetime_time,
) -> datetime:
    if status == "ready_before_session":
        return session_start_at
    if status == "ready_now":
        return local_now
    return _next_weekday_start_after(local_now, session_start)


def _shell_command(command: Sequence[object]) -> str:
    return " ".join(shlex.quote(str(item)) for item in command)


def _build_schedule(
    *,
    commands: Sequence[Sequence[object]],
    preflight: Mapping[str, Any],
    schedule_dir: Path,
    working_directory: Path,
) -> dict[str, Any]:
    resolved_schedule_dir = schedule_dir.resolve()
    resolved_working_directory = working_directory.resolve()
    # The operator recommendation can be "start now" when a plan is generated
    # during an active session. Recurring schedule files must still start at the
    # regular session open, otherwise the next days miss early prior snapshots.
    start = datetime.fromisoformat(
        str(preflight.get("session_start_moscow") or preflight["recommended_start_moscow"])
    )
    shell_commands = [_shell_command(command) for command in commands]
    log_path = resolved_schedule_dir / "liquidity-collector.log"
    shell_script = resolved_schedule_dir / "run-liquidity-collector.sh"
    cron_file = resolved_schedule_dir / "liquidity-collector.cron"
    launchd_plist = resolved_schedule_dir / "com.investment-signals.research-liquidity-collector.plist"
    systemd_service = resolved_schedule_dir / "investment-signals-research-liquidity-collector.service"
    systemd_timer = resolved_schedule_dir / "investment-signals-research-liquidity-collector.timer"
    run_line = "cd {cwd} && {commands}".format(
        cwd=shlex.quote(str(resolved_working_directory)),
        commands=" && ".join(shell_commands),
    )
    cron_line = "{minute} {hour} * * 1-5 {run_line} >> {log} 2>&1".format(
        minute=start.minute,
        hour=start.hour,
        run_line=run_line,
        log=shlex.quote(str(log_path)),
    )
    return {
        "timezone": "Europe/Moscow",
        "weekday_start_local": f"{start.hour:02d}:{start.minute:02d}",
        "scheduled_start_moscow": start.isoformat(),
        "recommended_start_moscow": preflight.get("recommended_start_moscow"),
        "recommended_end_moscow": preflight.get("recommended_end_moscow"),
        "working_directory": str(resolved_working_directory),
        "shell_script": str(shell_script),
        "cron_file": str(cron_file),
        "cron_line": cron_line,
        "launchd_plist": str(launchd_plist),
        "launchctl_load_command": f"launchctl load {shlex.quote(str(launchd_plist))}",
        "launchctl_unload_command": f"launchctl unload {shlex.quote(str(launchd_plist))}",
        "systemd_service": str(systemd_service),
        "systemd_timer": str(systemd_timer),
        "systemd_install_user_command": (
            "mkdir -p ~/.config/systemd/user && "
            f"cp {shlex.quote(str(systemd_service))} {shlex.quote(str(systemd_timer))} ~/.config/systemd/user/ && "
            "systemctl --user daemon-reload && "
            f"systemctl --user enable --now {shlex.quote(systemd_timer.name)}"
        ),
        "systemd_disable_user_command": (
            f"systemctl --user disable --now {shlex.quote(systemd_timer.name)}"
        ),
        "log_path": str(log_path),
        "run_line": run_line,
        "commands": shell_commands,
    }


def collection_window_preflight(
    *,
    now: datetime,
    target_hours: float,
    interval_seconds: float,
    session_start: datetime_time = REGULAR_SESSION_START,
    session_end: datetime_time = REGULAR_SESSION_END,
) -> dict[str, Any]:
    samples = continuous_samples_for_hours(target_hours=target_hours, interval_seconds=interval_seconds)
    local_now = now.astimezone(MOSCOW)
    session_start_at = local_now.replace(
        hour=session_start.hour,
        minute=session_start.minute,
        second=session_start.second,
        microsecond=0,
    )
    session_end_at = local_now.replace(
        hour=session_end.hour,
        minute=session_end.minute,
        second=session_end.second,
        microsecond=0,
    )
    required_seconds = target_hours * 3600
    latest_full_start_at = session_end_at - timedelta(seconds=required_seconds)
    if latest_full_start_at < session_start_at:
        status = "target_longer_than_research_session"
    elif local_now < session_start_at:
        status = "ready_before_session"
    elif local_now <= latest_full_start_at:
        status = "ready_now"
    elif local_now <= session_end_at:
        status = "insufficient_remaining_session"
    else:
        status = "outside_research_session"
    remaining_seconds = max(0.0, (session_end_at - max(local_now, session_start_at)).total_seconds())
    recommended_start_at = _recommended_collection_start(
        local_now=local_now,
        status=status,
        session_start_at=session_start_at,
        session_start=session_start,
    )
    recommended_end_at = recommended_start_at + timedelta(seconds=required_seconds)
    return {
        "status": status,
        "now_moscow": local_now.isoformat(),
        "session_start_moscow": session_start_at.isoformat(),
        "session_end_moscow": session_end_at.isoformat(),
        "latest_full_start_moscow": latest_full_start_at.isoformat(),
        "recommended_start_moscow": recommended_start_at.isoformat(),
        "recommended_end_moscow": recommended_end_at.isoformat(),
        "seconds_until_recommended_start": max(0.0, (recommended_start_at - local_now).total_seconds()),
        "target_continuous_hours": target_hours,
        "interval_seconds": interval_seconds,
        "required_samples": samples,
        "required_seconds": required_seconds,
        "remaining_session_seconds": remaining_seconds,
        "can_complete_full_window_today": status in {"ready_before_session", "ready_now"},
        "reason_code": (
            "start_continuous_sampler"
            if status in {"ready_before_session", "ready_now"}
            else "schedule_next_full_prior_window"
        ),
    }


def build_collection_plan(
    *,
    readiness_path: Path,
    tickers: Sequence[str],
    preferred_max_age_seconds: int = 30,
    target_calendar_days: int = 45,
    target_continuous_hours: float = 8.0,
    orderbook_interval_seconds: int = 15,
    orderbook_flush_every_samples: int = 20,
    signal_triggered_interval_seconds: int = 15,
    output_dir: Path = Path("var/research/liquidity_holdout/current"),
    schedule_dir: Path | None = None,
    working_directory: Path | None = None,
    status_run_dir: Path = Path("var/research/runs/fe7da78bab3fd474"),
    status_output_dir: Path = Path("var/research/signal_90_status/current"),
    audit_output_dir: Path = Path("var/research/goal_90_audit/current"),
    ca_cert: Path | None = None,
    insecure_skip_tls_verify: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = _load_json(readiness_path)
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise RuntimeError(f"Readiness file {readiness_path} must contain rows")
    best = _best_row(rows, preferred_max_age_seconds)
    if best is None:
        raise RuntimeError(f"Readiness file {readiness_path} contains no rows")
    covered_sessions = int(best.get("covered_sessions", 0) or 0)
    covered_signals = int(best.get("covered_signals", 0) or 0)
    missing_sessions = int(best.get("missing_covered_sessions", 0) or 0)
    missing_signals = int(best.get("missing_covered_signals", 0) or 0)
    estimated_sessions_for_signals = _estimated_sessions_for_signal_target(
        missing_signals,
        covered_signals,
        covered_sessions,
    )
    sessions_to_collect = max(
        missing_sessions,
        target_calendar_days if covered_sessions == 0 else 0,
        estimated_sessions_for_signals or 0,
    )
    if missing_sessions > 0:
        sessions_to_collect = max(sessions_to_collect, math.ceil(missing_sessions * 1.5))
    continuous_orderbook_samples = continuous_samples_for_hours(
        target_hours=target_continuous_hours,
        interval_seconds=orderbook_interval_seconds,
    )
    signal_triggered_polls = continuous_samples_for_hours(
        target_hours=target_continuous_hours,
        interval_seconds=signal_triggered_interval_seconds,
    )
    preflight = collection_window_preflight(
        now=now or datetime.now(MOSCOW),
        target_hours=target_continuous_hours,
        interval_seconds=orderbook_interval_seconds,
    )
    recommended_orderbook_command = [
        "uv",
        "run",
        "--extra",
        "research",
        "python",
        "scripts/research_update_liquidity_holdout.py",
        "--env-file",
        ".env",
        "--collect-orderbook",
        "--collect-signal-triggered-orderbook",
        "--tickers",
        ",".join(tickers),
        "--orderbook-depth",
        "10",
        "--orderbook-samples",
        str(continuous_orderbook_samples),
        "--orderbook-interval-seconds",
        str(orderbook_interval_seconds),
        "--orderbook-flush-every-samples",
        str(orderbook_flush_every_samples),
        "--signal-triggered-polls",
        str(signal_triggered_polls),
        "--signal-triggered-interval-seconds",
        str(signal_triggered_interval_seconds),
        "--signal-triggered-max-signal-age-seconds",
        "180",
        "--preferred-max-age-seconds",
        str(preferred_max_age_seconds),
        "--require-full-prior-window",
    ]
    if ca_cert is not None:
        recommended_orderbook_command.extend(["--ca-cert", str(ca_cert)])
    if insecure_skip_tls_verify:
        recommended_orderbook_command.append("--insecure-skip-tls-verify")
    recommended_orderbook_command.extend(["--output-dir", str(output_dir)])
    resolved_schedule_dir = schedule_dir or output_dir / "collection_plan"
    resolved_working_directory = working_directory or Path.cwd()
    collection_plan_path = resolved_schedule_dir / "collection-plan.json"
    refresh_reports_command = [
        "python",
        "scripts/research_refresh_90_reports.py",
        "--holdout-dir",
        str(output_dir),
        "--fallback-run-dir",
        str(status_run_dir),
        "--fallback-dataset",
        "var/research/datasets/signal_price_prediction.parquet",
        "--output-dir",
        "var/research/refresh_90/current",
    ]
    schedule = _build_schedule(
        commands=(
            recommended_orderbook_command,
            refresh_reports_command,
        ),
        preflight=preflight,
        schedule_dir=resolved_schedule_dir,
        working_directory=resolved_working_directory,
    )
    return {
        "schema_version": 1,
        "kind": "liquidity_collection_plan",
        "readiness": str(readiness_path),
        "status": "ready" if payload.get("ready") else "collect_more_data",
        "preferred_max_age_seconds": preferred_max_age_seconds,
        "best_window": dict(best),
        "missing_covered_signals": missing_signals,
        "missing_covered_sessions": missing_sessions,
        "observed_covered_signals_per_session": (
            covered_signals / covered_sessions if covered_signals > 0 and covered_sessions > 0 else None
        ),
        "estimated_sessions_for_missing_signals": estimated_sessions_for_signals,
        "recommended_additional_market_sessions": sessions_to_collect,
        "target_calendar_days": target_calendar_days,
        "target_continuous_hours_per_session": target_continuous_hours,
        "continuous_orderbook_samples": continuous_orderbook_samples,
        "orderbook_interval_seconds": orderbook_interval_seconds,
        "orderbook_flush_every_samples": orderbook_flush_every_samples,
        "signal_triggered_polls": signal_triggered_polls,
        "signal_triggered_interval_seconds": signal_triggered_interval_seconds,
        "collection_window_preflight": preflight,
        "prior_feature_collection_mode": "continuous_orderbook_sampling",
        "tickers": list(tickers),
        "ca_cert": str(ca_cert) if ca_cert is not None else None,
        "insecure_skip_tls_verify": bool(insecure_skip_tls_verify),
        "recommended_command": recommended_orderbook_command,
        "post_collection_commands": [refresh_reports_command],
        "schedule": schedule,
        "notes": [
            "Run the continuous order-book sampler during active market hours before signals occur.",
            "recommended_start_moscow is the one-off operator start; recurring schedule artifacts use the regular session start to preserve early prior snapshots.",
            "If preflight is outside the allowed full window, wait for the next regular session start instead of forcing a late run.",
            "Signal-triggered snapshots are post-signal context; they do not by themselves prove prior order-book feature coverage.",
            "A forced smoke run is not evidence for a 90% product claim.",
            "A product claim still requires a later independent holdout after shadow validation.",
        ],
    }


def write_schedule_files(schedule: Mapping[str, Any]) -> dict[str, str]:
    shell_script = Path(str(schedule["shell_script"]))
    cron_file = Path(str(schedule["cron_file"]))
    launchd_plist = Path(str(schedule["launchd_plist"]))
    systemd_service = Path(str(schedule["systemd_service"]))
    systemd_timer = Path(str(schedule["systemd_timer"]))
    log_path = Path(str(schedule["log_path"]))
    for path in (shell_script, cron_file, launchd_plist, systemd_service, systemd_timer, log_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    shell_script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"cd {shlex.quote(str(schedule['working_directory']))}",
                *(str(command) for command in schedule.get("commands", [])),
                "",
            ]
        ),
        encoding="utf-8",
    )
    shell_script.chmod(0o755)
    cron_file.write_text(str(schedule["cron_line"]) + "\n", encoding="utf-8")
    program = "/bin/zsh"
    argument = str(schedule["run_line"]) + f" >> {shlex.quote(str(log_path))} 2>&1"
    start_hour, start_minute = str(schedule["weekday_start_local"]).split(":", 1)
    launchd_intervals = "\n".join(
        "    <dict><key>Weekday</key><integer>{weekday}</integer><key>Hour</key><integer>{hour}</integer><key>Minute</key><integer>{minute}</integer></dict>".format(
            weekday=weekday,
            hour=int(start_hour),
            minute=int(start_minute),
        )
        for weekday in LAUNCHD_MARKET_WEEKDAYS
    )
    launchd_plist.write_text(
        f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.investment-signals.research-liquidity-collector</string>
  <key>ProgramArguments</key>
  <array>
    <string>{escape(program)}</string>
    <string>-lc</string>
    <string>{escape(argument)}</string>
  </array>
  <key>StartCalendarInterval</key>
  <array>
{launchd_intervals}
  </array>
  <key>StandardOutPath</key>
  <string>{escape(str(log_path))}</string>
  <key>StandardErrorPath</key>
  <string>{escape(str(log_path))}</string>
  <key>WorkingDirectory</key>
  <string>{escape(str(schedule["working_directory"]))}</string>
</dict>
</plist>
""",
        encoding="utf-8",
    )
    systemd_service.write_text(
        "\n".join(
            [
                "[Unit]",
                "Description=Investment Signals research liquidity collector",
                "Documentation=man:systemd.service(5)",
                "",
                "[Service]",
                "Type=oneshot",
                f"WorkingDirectory={schedule['working_directory']}",
                f"ExecStart={shell_script}",
                f"StandardOutput=append:{log_path}",
                f"StandardError=append:{log_path}",
                "Nice=10",
                "IOSchedulingClass=best-effort",
                "",
            ]
        ),
        encoding="utf-8",
    )
    systemd_timer.write_text(
        "\n".join(
            [
                "[Unit]",
                "Description=Run Investment Signals research liquidity collector on market weekdays",
                "",
                "[Timer]",
                f"OnCalendar=Mon..Fri *-*-* {int(start_hour):02d}:{int(start_minute):02d}:00 Europe/Moscow",
                "Persistent=false",
                "AccuracySec=30s",
                "",
                "[Install]",
                "WantedBy=timers.target",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return {
        "shell_script": str(shell_script),
        "cron_file": str(cron_file),
        "launchd_plist": str(launchd_plist),
        "systemd_service": str(systemd_service),
        "systemd_timer": str(systemd_timer),
    }


def write_report(path: Path, plan: Mapping[str, Any]) -> None:
    line_joiner = " " + "\\" + "\n  "
    command = line_joiner.join(shlex.quote(str(item)) for item in plan["recommended_command"])
    post_commands = [
        line_joiner.join(shlex.quote(str(item)) for item in command_items)
        for command_items in plan.get("post_collection_commands", [])
        if isinstance(command_items, Sequence) and not isinstance(command_items, (str, bytes))
    ]
    schedule = plan.get("schedule") if isinstance(plan.get("schedule"), Mapping) else {}
    lines = [
        "# Liquidity collection plan",
        "",
        f"- Status: `{plan['status']}`",
        f"- Preferred max order-book age: {plan['preferred_max_age_seconds']} seconds",
        f"- Missing covered signals: {plan['missing_covered_signals']}",
        f"- Missing covered sessions: {plan['missing_covered_sessions']}",
        f"- Observed covered signals per session: {plan.get('observed_covered_signals_per_session')}",
        f"- Estimated sessions for missing signals: {plan.get('estimated_sessions_for_missing_signals')}",
        f"- Recommended additional market sessions: {plan['recommended_additional_market_sessions']}",
        f"- Prior feature collection mode: `{plan.get('prior_feature_collection_mode')}`",
        f"- Continuous hours per session: {plan.get('target_continuous_hours_per_session')}",
        f"- Continuous order-book samples per run: {plan.get('continuous_orderbook_samples')}",
        f"- Order-book interval: {plan.get('orderbook_interval_seconds')} seconds",
        f"- Order-book flush cadence: every {plan.get('orderbook_flush_every_samples')} samples",
        f"- Collection preflight: `{dict(plan.get('collection_window_preflight', {})).get('status')}`",
        f"- Latest full-window start: {dict(plan.get('collection_window_preflight', {})).get('latest_full_start_moscow')}",
        f"- Recommended start: {dict(plan.get('collection_window_preflight', {})).get('recommended_start_moscow')}",
        f"- Recommended end: {dict(plan.get('collection_window_preflight', {})).get('recommended_end_moscow')}",
        f"- Recurring schedule start: {schedule.get('weekday_start_local')} {schedule.get('timezone')}",
        f"- Schedule shell script: {schedule.get('shell_script')}",
        f"- Schedule cron file: {schedule.get('cron_file')}",
        f"- Schedule launchd plist: {schedule.get('launchd_plist')}",
        f"- Schedule systemd service: {schedule.get('systemd_service')}",
        f"- Schedule systemd timer: {schedule.get('systemd_timer')}",
        f"- Post-collection commands: {len(post_commands)}",
        "",
        "## Recommended command",
        "",
        "```bash",
        command,
        "```",
        "",
        "## Post-collection commands",
        "",
    ]
    for item in post_commands:
        lines.extend(["```bash", item, "```", ""])
    lines.extend(
        [
        "## Schedule artifacts",
        "",
        f"- Weekday start: {schedule.get('weekday_start_local')} {schedule.get('timezone')}",
        f"- Shell script: `{schedule.get('shell_script')}`",
        f"- Cron file: `{schedule.get('cron_file')}`",
        f"- launchd plist: `{schedule.get('launchd_plist')}`",
        f"- launchd load: `{schedule.get('launchctl_load_command')}`",
        f"- systemd service: `{schedule.get('systemd_service')}`",
        f"- systemd timer: `{schedule.get('systemd_timer')}`",
        f"- systemd user install: `{schedule.get('systemd_install_user_command')}`",
        f"- systemd user disable: `{schedule.get('systemd_disable_user_command')}`",
        f"- Log file: `{schedule.get('log_path')}`",
        "",
        "## Notes",
        "",
        ]
    )
    lines.extend(f"- {note}" for note in plan.get("notes", []))
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-plan-liquidity-collection")
    parser.add_argument("--readiness-json", type=Path, required=True)
    parser.add_argument("--tickers", default="SBER,GAZP,LKOH,YDEX,T")
    parser.add_argument("--preferred-max-age-seconds", type=int, default=30)
    parser.add_argument("--target-calendar-days", type=int, default=45)
    parser.add_argument("--target-continuous-hours", type=float, default=8.0)
    parser.add_argument("--orderbook-interval-seconds", type=int, default=15)
    parser.add_argument("--orderbook-flush-every-samples", type=int, default=20)
    parser.add_argument("--signal-triggered-interval-seconds", type=int, default=15)
    parser.add_argument("--status-run-dir", type=Path, default=Path("var/research/runs/fe7da78bab3fd474"))
    parser.add_argument("--status-output-dir", type=Path, default=Path("var/research/signal_90_status/current"))
    parser.add_argument("--audit-output-dir", type=Path, default=Path("var/research/goal_90_audit/current"))
    parser.add_argument("--ca-cert", type=Path)
    parser.add_argument("--insecure-skip-tls-verify", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_collection_plan/current"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = tuple(item.strip().upper() for item in args.tickers.split(",") if item.strip())
    plan = build_collection_plan(
        readiness_path=args.readiness_json,
        tickers=tickers,
        preferred_max_age_seconds=args.preferred_max_age_seconds,
        target_calendar_days=args.target_calendar_days,
        target_continuous_hours=args.target_continuous_hours,
        orderbook_interval_seconds=args.orderbook_interval_seconds,
        orderbook_flush_every_samples=args.orderbook_flush_every_samples,
        signal_triggered_interval_seconds=args.signal_triggered_interval_seconds,
        schedule_dir=args.output_dir,
        status_run_dir=args.status_run_dir,
        status_output_dir=args.status_output_dir,
        audit_output_dir=args.audit_output_dir,
        ca_cert=args.ca_cert,
        insecure_skip_tls_verify=args.insecure_skip_tls_verify,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    plan_path = args.output_dir / "collection-plan.json"
    report_path = args.output_dir / "collection-plan.md"
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_schedule_files(plan["schedule"])
    write_report(report_path, plan)
    print(json.dumps({"status": plan["status"], "output_dir": str(args.output_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
