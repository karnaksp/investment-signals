#!/usr/bin/env python3
"""Select an explainable portfolio from sealed scientific research artifacts."""

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

from tinvest_signal_engine.adapters.scientific_portfolio_selector import (  # noqa: E402
    ImmutableJsonScientificPortfolioReportAdapter,
    SealedScientificPortfolioSelectorExampleSource,
)
from tinvest_signal_engine.application.scientific_portfolio_selector import (  # noqa: E402
    RunScientificPortfolioSelector,
    ScientificPortfolioSelectorPolicy,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="research-select-scientific-portfolio",
        description=(
            "Read a checksummed scientific observation/result export, compare "
            "fixed, Bayesian and regularized logistic selectors, and write an "
            "immutable JSON report. No network access."
        ),
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/runs/scientific-portfolio-selector"),
    )
    parser.add_argument("--minimum-absolute-effect", type=float, default=0.0)
    parser.add_argument("--minimum-train-examples", type=int, default=120)
    parser.add_argument("--minimum-validation-examples", type=int, default=40)
    parser.add_argument("--minimum-holdout-examples", type=int, default=40)
    parser.add_argument("--minimum-trading-days", type=int, default=30)
    parser.add_argument("--minimum-acted-examples", type=int, default=20)
    parser.add_argument("--minimum-coverage", type=float, default=0.20)
    parser.add_argument("--minimum-accuracy-lift", type=float, default=0.02)
    parser.add_argument("--gap-trading-days", type=int, default=1)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    execution = RunScientificPortfolioSelector(
        source=SealedScientificPortfolioSelectorExampleSource(
            args.input_dir,
            minimum_absolute_effect=args.minimum_absolute_effect,
        ),
        artifacts=ImmutableJsonScientificPortfolioReportAdapter(args.output_dir),
        policy=ScientificPortfolioSelectorPolicy(
            minimum_train_examples=args.minimum_train_examples,
            minimum_validation_examples=args.minimum_validation_examples,
            minimum_holdout_examples=args.minimum_holdout_examples,
            minimum_total_trading_days=args.minimum_trading_days,
            minimum_acted_examples=args.minimum_acted_examples,
            minimum_coverage=args.minimum_coverage,
            minimum_accuracy_lift=args.minimum_accuracy_lift,
            gap_trading_days=args.gap_trading_days,
        ),
    ).execute()
    payload: dict[str, object] = {
        "status": "reused" if execution.reused else "completed",
        "run_id": execution.run_id,
        "artifact_uri": execution.artifact_uri,
        "network_used": False,
        "source": "sealed_scientific_observation_result_artifacts",
        "causal_evidence_gate_unchanged": True,
        "claim_allowed": False,
    }
    if execution.result is not None:
        selected = next(
            item
            for item in execution.result.evaluations
            if item.model_kind is execution.result.selected_model
        )
        payload.update(
            {
                "state": execution.result.state.value,
                "selected_model": execution.result.selected_model.value,
                "holdout_coverage": selected.holdout_metrics.coverage,
                "holdout_abstention_rate": (
                    selected.holdout_metrics.abstention_rate
                ),
                "holdout_accuracy_when_acted": (
                    selected.holdout_metrics.accuracy_when_acted
                ),
            }
        )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
