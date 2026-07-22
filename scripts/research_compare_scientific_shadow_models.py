#!/usr/bin/env python3
"""Compare models over a sealed H1-H17/C1-C4 shadow dataset."""

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
    ImmutableJsonShadowArtifactAdapter,
    ImmutableJsonShadowDatasetSource,
    SklearnShadowEstimatorFactory,
)
from tinvest_signal_engine.application.scientific_model_shadow import (  # noqa: E402
    RunScientificModelShadowComparison,
    ShadowComparisonPolicy,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="research-compare-scientific-shadow-models",
        description=(
            "Reproducibly compare a base rate, logistic regression and "
            "gradient boosting on sealed portfolio results. No network access."
        ),
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/runs/scientific-model-shadow"),
    )
    parser.add_argument("--minimum-train-examples", type=int, default=100)
    parser.add_argument("--minimum-validation-examples", type=int, default=50)
    parser.add_argument("--minimum-holdout-examples", type=int, default=50)
    parser.add_argument("--minimum-trading-days", type=int, default=30)
    parser.add_argument("--action-probability", type=float, default=0.60)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    execution = RunScientificModelShadowComparison(
        source=ImmutableJsonShadowDatasetSource(args.input_dir),
        estimators=SklearnShadowEstimatorFactory(),
        artifacts=ImmutableJsonShadowArtifactAdapter(args.output_dir),
        policy=ShadowComparisonPolicy(
            minimum_train_examples=args.minimum_train_examples,
            minimum_validation_examples=args.minimum_validation_examples,
            minimum_holdout_examples=args.minimum_holdout_examples,
            minimum_total_trading_days=args.minimum_trading_days,
            action_probability_threshold=args.action_probability,
        ),
    ).execute()
    payload: dict[str, object] = {
        "status": "reused" if execution.reused else "completed",
        "run_id": execution.run_id,
        "artifact_uri": execution.artifact_uri,
        "network_used": False,
        "causal_evidence_gate_unchanged": True,
        "claim_allowed": False,
    }
    if execution.result is not None:
        payload["state"] = execution.result.state.value
        payload["missing_study_ids"] = execution.result.missing_study_ids
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
