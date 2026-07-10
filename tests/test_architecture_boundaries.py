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

