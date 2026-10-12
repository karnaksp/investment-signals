#!/usr/bin/env python3
"""Report live status for the liquidity collection job."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from zoneinfo import ZoneInfo


MOSCOW = ZoneInfo("Europe/Moscow")
RUNNING_MARKERS = (
    "research_update_liquidity_holdout.py",
    "research_collect_tinvest_orderbook_snapshots.py",
    "research_collect_signal_triggered_orderbooks.py",
)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_dt(value: object) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        return datetime.fromisoformat(str(value)).astimezone(MOSCOW)
    except ValueError:
        return None


def _ps_output() -> str:
    completed = subprocess.run(  # noqa: S603 - read-only process status command
        ["ps", "-axo", "pid=,command="],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return completed.stdout if completed.returncode == 0 else ""


def _running_collectors(process_output: str | None = None) -> list[dict[str, str]]:
    output = _ps_output() if process_output is None else process_output
    rows: list[dict[str, str]] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if not any(marker in stripped for marker in RUNNING_MARKERS):
            continue
        pid, _, command = stripped.partition(" ")
        rows.append({"pid": pid, "command": command.strip()})
    return rows


def _manifest_snapshot(manifest_path: Path) -> dict[str, Any]:
    manifest = _read_json(manifest_path)
    quality = manifest.get("quality") if isinstance(manifest.get("quality"), Mapping) else {}
    rows_by_partition = (
        quality.get("rows_by_partition")
        if isinstance(quality.get("rows_by_partition"), Mapping)
        else {}
    )
    row_count = 0
    for value in rows_by_partition.values():
        try:
            row_count += int(value)
        except (TypeError, ValueError):
            continue
    return {
        "manifest_path": str(manifest_path),
        "exists": manifest_path.exists(),
        "created_at": manifest.get("created_at"),
        "partition_count": quality.get("partition_count"),
        "rows_by_partition": dict(rows_by_partition),
        "row_count": row_count,
    }


def _progress_snapshot(progress_path: Path) -> dict[str, Any]:
    progress = _read_json(progress_path)
    progress_section = (
        progress.get("progress")
        if isinstance(progress.get("progress"), Mapping)
        else {}
    )
    return {
        "progress_path": str(progress_path),
        "exists": progress_path.exists(),
        "status": progress.get("status", ""),
        "updated_at": progress.get("updated_at", ""),
        "completed_samples": progress_section.get("completed_samples"),
        "target_samples": progress_section.get("target_samples"),
        "completed_share": progress_section.get("completed_share"),
        "rows_collected": progress_section.get("rows_collected"),
        "rows_flushed": progress_section.get("rows_flushed"),
        "unflushed_rows": progress_section.get("unflushed_rows"),
        "failures": progress_section.get("failures"),
    }


def _file_mtime(path: Path) -> datetime | None:
    if not path.exists():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=MOSCOW)


def _cache_files_snapshot(cache_dir: Path, start_at: datetime | None) -> dict[str, Any]:
    files = sorted(cache_dir.glob("ticker=*/date=*.parquet"))
    mtimes = [item.stat().st_mtime for item in files if item.exists()]
    newest = datetime.fromtimestamp(max(mtimes), tz=MOSCOW) if mtimes else None
    updated_after_start = [
        str(item)
        for item in files
        if start_at and datetime.fromtimestamp(item.stat().st_mtime, tz=MOSCOW) >= start_at
    ]
    return {
        "cache_dir": str(cache_dir),
        "parquet_files": len(files),
        "newest_parquet_mtime": newest.isoformat() if newest else "",
        "files_updated_after_recommended_start": len(updated_after_start),
        "updated_after_start_examples": updated_after_start[:10],
    }


def _next_action(
    *,
    start_has_passed: bool,
    loaded: bool,
    running: bool,
    log_exists: bool,
    files_updated_after_start: int,
) -> str:
    if not loaded:
        return "load_scheduler_before_start"
    if not start_has_passed:
        return "wait_for_scheduled_start"
    if running:
        return "watch_log_and_cache_growth"
    if log_exists or files_updated_after_start > 0:
        return "inspect_finished_run_outputs"
    return "inspect_launchd_and_log_missing"


def build_live_status(
    *,
    collection_plan_path: Path,
    schedule_status_path: Path,
    orderbook_cache_dir: Path,
    process_output: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    plan = _read_json(collection_plan_path)
    schedule_status = _read_json(schedule_status_path)
    schedule = plan.get("schedule") if isinstance(plan.get("schedule"), Mapping) else {}
    start_raw = schedule.get("recommended_start_moscow") or schedule_status.get("recommended_start_moscow")
    end_raw = schedule.get("recommended_end_moscow")
    start_at = _parse_dt(start_raw)
    end_at = _parse_dt(end_raw)
    local_now = (now or datetime.now(MOSCOW)).astimezone(MOSCOW)
    start_has_passed = bool(start_at and local_now >= start_at)
    end_has_passed = bool(end_at and local_now >= end_at)
    collectors = _running_collectors(process_output)
    log_path = Path(str(schedule.get("log_path") or schedule_status.get("log_path") or ""))
    manifest = _manifest_snapshot(orderbook_cache_dir / "manifest.json")
    progress = _progress_snapshot(orderbook_cache_dir / "collection-progress.json")
    cache_files = _cache_files_snapshot(orderbook_cache_dir, start_at)
    scheduler_loaded = bool(schedule_status.get("scheduler_loaded") or schedule_status.get("launchd_loaded"))
    running = bool(collectors)
    log_mtime = _file_mtime(log_path)
    return {
        "schema_version": 1,
        "kind": "liquidity_collection_live_status",
        "created_at": datetime.now(MOSCOW).isoformat(),
        "status": (
            "waiting_for_start"
            if scheduler_loaded and not start_has_passed
            else "running"
            if running
            else "post_start_activity_seen"
            if start_has_passed and (log_path.exists() or cache_files["files_updated_after_recommended_start"] > 0)
            else "post_start_no_activity"
            if start_has_passed
            else "not_loaded"
        ),
        "now_moscow": local_now.isoformat(),
        "recommended_start_moscow": start_at.isoformat() if start_at else "",
        "recommended_end_moscow": end_at.isoformat() if end_at else "",
        "recommended_start_has_passed": start_has_passed,
        "recommended_end_has_passed": end_has_passed,
        "launchd_loaded": schedule_status.get("launchd_loaded"),
        "scheduler_loaded": scheduler_loaded,
        "systemd_loaded": schedule_status.get("systemd_loaded"),
        "running_collectors": collectors,
        "log_path": str(log_path),
        "log_exists": log_path.exists(),
        "log_mtime": log_mtime.isoformat() if log_mtime else "",
        "manifest": manifest,
        "progress": progress,
        "cache_files": cache_files,
        "next_action": _next_action(
            start_has_passed=start_has_passed,
            loaded=scheduler_loaded,
            running=running,
            log_exists=log_path.exists(),
            files_updated_after_start=int(cache_files["files_updated_after_recommended_start"]),
        ),
    }


def write_markdown(path: Path, status: Mapping[str, Any]) -> None:
    lines = [
        "# Live-статус сбора стакана",
        "",
        f"- Статус: `{status.get('status')}`",
        f"- Следующее действие: `{status.get('next_action')}`",
        f"- Сейчас: {status.get('now_moscow')}",
        f"- Рекомендуемый старт: {status.get('recommended_start_moscow')}",
        f"- Рекомендуемый конец: {status.get('recommended_end_moscow')}",
        f"- Старт уже прошёл: {'да' if status.get('recommended_start_has_passed') else 'нет'}",
        f"- `launchd` загружен: {'да' if status.get('launchd_loaded') else 'нет'}",
        f"- Планировщик загружен: {'да' if status.get('scheduler_loaded') else 'нет'}",
        f"- `systemd` загружен: {status.get('systemd_loaded')}",
        f"- Активных процессов сбора: {len(status.get('running_collectors', []))}",
        f"- Лог существует: {'да' if status.get('log_exists') else 'нет'}",
        f"- Лог обновлён: {status.get('log_mtime')}",
        "",
        "## Кэш стакана",
        "",
    ]
    manifest = status.get("manifest") if isinstance(status.get("manifest"), Mapping) else {}
    progress = status.get("progress") if isinstance(status.get("progress"), Mapping) else {}
    cache_files = status.get("cache_files") if isinstance(status.get("cache_files"), Mapping) else {}
    lines.extend(
        [
            f"- Manifest существует: {'да' if manifest.get('exists') else 'нет'}",
            f"- Строк в manifest: {manifest.get('row_count')}",
            f"- Партиций в manifest: {manifest.get('partition_count')}",
            f"- Parquet-файлов: {cache_files.get('parquet_files')}",
            f"- Новейший parquet: {cache_files.get('newest_parquet_mtime')}",
            f"- Файлов обновлено после старта: {cache_files.get('files_updated_after_recommended_start')}",
            "",
            "## Прогресс текущего сбора",
            "",
            f"- Progress существует: {'да' if progress.get('exists') else 'нет'}",
            f"- Статус progress: `{progress.get('status')}`",
            f"- Обновлён: {progress.get('updated_at')}",
            f"- Сэмплов: {progress.get('completed_samples')} из {progress.get('target_samples')}",
            f"- Доля выполнения: {float(progress.get('completed_share') or 0.0):.2%}",
            f"- Строк собрано: {progress.get('rows_collected')}",
            f"- Строк сброшено на диск: {progress.get('rows_flushed')}",
            f"- Строк ещё в памяти: {progress.get('unflushed_rows')}",
            f"- Ошибок: {progress.get('failures')}",
            "",
            "## Активные процессы",
            "",
        ]
    )
    collectors = status.get("running_collectors")
    if isinstance(collectors, list) and collectors:
        for item in collectors:
            if isinstance(item, Mapping):
                lines.append(f"- `{item.get('pid')}` — `{item.get('command')}`")
    else:
        lines.append("- нет")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_live_status(
    *,
    collection_plan_path: Path,
    schedule_status_path: Path,
    orderbook_cache_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    status = build_live_status(
        collection_plan_path=collection_plan_path,
        schedule_status_path=schedule_status_path,
        orderbook_cache_dir=orderbook_cache_dir,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "live-status.json").write_text(
        json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(output_dir / "live-status.md", status)
    return status


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="research-liquidity-collection-live-status")
    parser.add_argument("--collection-plan", type=Path, default=Path("var/research/liquidity_holdout/current/collection_plan/collection-plan.json"))
    parser.add_argument("--schedule-status", type=Path, default=Path("var/research/liquidity_holdout/current/collection_plan/schedule-status.json"))
    parser.add_argument("--orderbook-cache-dir", type=Path, default=Path("var/research/tinvest_orderbooks/v1"))
    parser.add_argument("--output-dir", type=Path, default=Path("var/research/liquidity_holdout/current/live_status"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    status = write_live_status(
        collection_plan_path=args.collection_plan,
        schedule_status_path=args.schedule_status,
        orderbook_cache_dir=args.orderbook_cache_dir,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {"status": status["status"], "next_action": status["next_action"], "output_dir": str(args.output_dir)},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
