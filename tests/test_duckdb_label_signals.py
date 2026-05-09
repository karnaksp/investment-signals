from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


def test_duckdb_label_signals_smoke(tmp_path: Path) -> None:
    if importlib.util.find_spec("duckdb") is None:
        pytest.skip("install optional: pip install duckdb")

    sig = tmp_path / "signals.csv"
    bars = tmp_path / "bars.csv"
    sig.write_text(
        "instrument_id,signal_type,detected_at\n"
        "X_TQBR,microstructure_combo_long,2026-01-15 10:00:05\n",
        encoding="utf-8",
    )
    bars.write_text(
        "instrument_id,bucket,vwap\n"
        "X_TQBR,2026-01-15 10:00:00,100.0\n"
        "X_TQBR,2026-01-15 10:01:00,101.0\n",
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "duckdb_label_signals.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--signals",
            str(sig),
            "--bars",
            str(bars),
            "--forward-bars",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    out = json.loads(proc.stdout)
    assert out["directional_hits"] >= 1
    assert out["signal_time_column"] == "detected_at"


def test_duckdb_label_signals_multi_horizon(tmp_path: Path) -> None:
    if importlib.util.find_spec("duckdb") is None:
        pytest.skip("install optional: pip install duckdb")

    sig = tmp_path / "signals.csv"
    bars = tmp_path / "bars.csv"
    sig.write_text(
        "instrument_id,signal_type,detected_at\n"
        "X_TQBR,microstructure_combo_long,2026-01-15 10:00:05\n",
        encoding="utf-8",
    )
    bars.write_text(
        "instrument_id,bucket,vwap\n"
        "X_TQBR,2026-01-15 10:00:00,100.0\n"
        "X_TQBR,2026-01-15 10:01:00,101.0\n"
        "X_TQBR,2026-01-15 10:05:00,102.0\n",
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "duckdb_label_signals.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--signals",
            str(sig),
            "--bars",
            str(bars),
            "--forward-bars",
            "1,5",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr + proc.stdout
    out = json.loads(proc.stdout)
    assert out["forward_bars"] == [1, 5]
    assert out["by_horizon"]["1"]["directional_hits"] >= 1
    assert out["by_horizon"]["5"]["directional_hits"] >= 1
