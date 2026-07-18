#!/usr/bin/env python3
"""Compare sealed H3/H4 rules with selective models from a local cache."""

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

from tinvest_signal_engine.adapters.jump_activity_replay import (  # noqa: E402
    ParquetCandleCacheAdapter,
)
from tinvest_signal_engine.adapters.selective_hypothesis_policy import (  # noqa: E402
    JsonSelectiveResearchArtifactAdapter,
    SklearnLightgbmEstimatorFactory,
)
from tinvest_signal_engine.application.selective_jump_policy_research import (  # noqa: E402
    RunSelectiveJumpPolicyResearch,
)
from tinvest_signal_engine.domain.jump_activity_replay import (  # noqa: E402
    CostModel,
    JumpReplayPolicy,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="research-selective-hypothesis-policy",
        description=(
            "Compare H3/H4 sealed rules, logistic regression and boosted trees "
            "with chronological tune-before-holdout validation. No network access."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("var/research/tinvest_candles/v1"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("var/research/runs/selective-h3-h4"),
    )
    parser.add_argument("--tickers", default="")
    parser.add_argument("--cost-model-version", default="research-cost-v1.0.0")
    parser.add_argument("--round-trip-cost-bps", type=float, default=10.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = tuple(
        item.strip().upper() for item in args.tickers.split(",") if item.strip()
    )
    replay_policy = JumpReplayPolicy(
        cost_model=CostModel(
            version=args.cost_model_version,
            round_trip_bps=args.round_trip_cost_bps,
        )
    )
    execution = RunSelectiveJumpPolicyResearch(
        candle_cache=ParquetCandleCacheAdapter(args.cache_dir),
        artifacts=JsonSelectiveResearchArtifactAdapter(args.output_dir),
        estimator_factory=SklearnLightgbmEstimatorFactory(),
    ).execute(replay_policy=replay_policy, tickers=tickers or None)
    payload: dict[str, object] = {
        "status": "reused" if execution.reused else "completed",
        "run_id": execution.run_id,
        "artifact_uri": execution.artifact_uri,
        "cache_reused_without_download": True,
    }
    if execution.result is not None:
        payload["studies"] = [
            {
                "hypothesis": item.hypothesis_id,
                "horizon_seconds": item.horizon_seconds,
                "selected_model": item.tune_selected_model.value,
                "threshold": item.tune_selected_threshold,
                "holdout_coverage": item.holdout_selected_metrics.coverage,
                "holdout_useful_rate": item.holdout_selected_metrics.useful_rate_when_acted,
                "holdout_mean_net_bps": item.holdout_selected_metrics.mean_cost_adjusted_result_bps,
                "holdout_lift_bps": item.holdout_lift_over_rule_bps,
                "decision": item.decision.value,
                "claim_allowed": item.claim_allowed,
                "reason_codes": item.reason_codes,
            }
            for item in execution.result.results
        ]
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
