#!/usr/bin/env python3
"""Скачать актуальный каталог `tinkoff/` из RussianInvestments/invest-python (для локальных pytest)."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ref",
        default="0.2.0-beta117",
        help="Git tag or branch (default: 0.2.0-beta117)",
    )
    parser.add_argument(
        "--repo",
        default="https://github.com/RussianInvestments/invest-python.git",
        help="Git repository URL",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    dest = root / "src" / "tinkoff"
    dest_parent = dest.parent
    dest_parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        shutil.rmtree(dest)

    with tempfile.TemporaryDirectory() as tmp:
        clone = Path(tmp) / "invest-python"
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                args.ref,
                args.repo,
                str(clone),
            ],
            check=True,
        )
        src = clone / "tinkoff"
        if not src.is_dir():
            print("Clone OK but tinkoff/ not found in repo", file=sys.stderr)
            return 1
        shutil.copytree(src, dest)

    print(f"Wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
