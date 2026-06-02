"""Runtime build/version fingerprint exposed by API and admin UI."""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any


def runtime_fingerprint() -> dict[str, Any]:
    """Return a small immutable-ish fingerprint for debugging live deploys."""
    return {
        "app_version": _app_version(),
        "commit_sha": _first_env(
            "APP_COMMIT_SHA",
            "BUILD_COMMIT_SHA",
            "GIT_COMMIT_SHA",
            "GITHUB_SHA",
        )
        or _git_sha(),
        "build_time": _first_env(
            "APP_BUILD_TIME",
            "BUILD_TIME",
            "GITHUB_RUN_STARTED_AT",
        )
        or _source_date_epoch()
        or "unknown",
    }


def _app_version() -> str:
    env_version = _first_env("APP_VERSION", "BUILD_VERSION")
    if env_version:
        return env_version
    try:
        return metadata.version("tinvest-signal-engine")
    except metadata.PackageNotFoundError:
        return "0.1.0"


def _first_env(*names: str) -> str | None:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return None


def _source_date_epoch() -> str | None:
    raw = (os.getenv("SOURCE_DATE_EPOCH") or "").strip()
    if not raw:
        return None
    try:
        ts = datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    return ts.isoformat()


def _git_sha() -> str:
    root = Path(__file__).resolve().parents[2]
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=root,
            capture_output=True,
            check=False,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    if proc.returncode != 0:
        return "unknown"
    return proc.stdout.strip() or "unknown"
