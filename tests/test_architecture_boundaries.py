"""Blocking Clean Architecture dependency-rule test."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_clean_architecture_import_boundaries() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "check_architecture.py")],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_application_cannot_import_adapter(tmp_path: Path) -> None:
    package = tmp_path / "tinvest_signal_engine"
    application = package / "application"
    application.mkdir(parents=True)
    (application / "bad.py").write_text(
        "from tinvest_signal_engine.adapters import migrations\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "check_architecture.py"),
            str(tmp_path),
        ],
        cwd=ROOT,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 1
    assert "application layer imports 'tinvest_signal_engine.adapters'" in (
        result.stdout
    )
