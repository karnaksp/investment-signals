#!/usr/bin/env python3
"""Seal model-comparison input from existing H1-H17 and C1-C4 artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src"
if str(SOURCE) not in sys.path:
    sys.path.insert(0, str(SOURCE))

from tinvest_signal_engine.adapters.scientific_model_shadow import (  # noqa: E402
    build_shadow_dataset_from_sealed_portfolio,
    seal_shadow_dataset,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="research-build-scientific-shadow-dataset",
        description=(
            "Map already sealed scientific portfolio and C1-C4 artifacts to "
            "a checksummed local model-comparison dataset. No network access."
        ),
    )
    parser.add_argument("--prospective-artifact-root", type=Path, required=True)
    parser.add_argument("--combination-artifact-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    dataset = build_shadow_dataset_from_sealed_portfolio(
        prospective_artifact_root=args.prospective_artifact_root,
        combination_artifact_dir=args.combination_artifact_dir,
    )
    output = seal_shadow_dataset(args.output_dir, dataset)
    print(
        json.dumps(
            {
                "status": "completed",
                "artifact_uri": output,
                "scopes": len(dataset.scopes),
                "examples": len(dataset.examples),
                "dataset_fingerprint": dataset.dataset_fingerprint,
                "network_used": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
