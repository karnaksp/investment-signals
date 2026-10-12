#!/usr/bin/env python3
"""Run preregistered H3/H4 replay from an existing local candle cache."""

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
    JsonJumpReplayArtifactAdapter,
    ParquetCandleCacheAdapter,
)
from tinvest_signal_engine.application.jump_activity_replay import (  # noqa: E402
    RunJumpActivityReplay,
)
from tinvest_signal_engine.domain.jump_activity_replay import (  # noqa: E402
    CostModel,
    JumpReplayPolicy,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="research-replay-jump-activity-hypotheses",
        description=(
            "Reproduce preregistered H3/H4 from immutable local 1m candles. "
            "The command never contacts T-Invest."
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
        default=Path("var/research/runs/h3-h4"),
    )
    parser.add_argument(
        "--tickers",
        default="",
        help="Optional comma-separated ticker subset; empty means every cached ticker.",
    )
    parser.add_argument("--cost-model-version", default="research-cost-v1.0.0")
    parser.add_argument("--round-trip-cost-bps", type=float, default=10.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    tickers = tuple(
        item.strip().upper() for item in args.tickers.split(",") if item.strip()
    )
    policy = JumpReplayPolicy(
        cost_model=CostModel(
            version=args.cost_model_version,
            round_trip_bps=args.round_trip_cost_bps,
        )
    )
    execution = RunJumpActivityReplay(
        candle_cache=ParquetCandleCacheAdapter(args.cache_dir),
        artifacts=JsonJumpReplayArtifactAdapter(args.output_dir),
    ).execute(policy=policy, tickers=tickers or None)
    summary: dict[str, object] = {
        "status": "reused" if execution.reused else "completed",
        "run_id": execution.run_id,
        "artifact_uri": execution.artifact_uri,
        "cache_reused_without_download": True,
    }
    if execution.result is not None:
        summary.update(
            {
                "candles": execution.result.candle_count,
                "raw_features": execution.result.raw_feature_count,
                "observations": len(execution.result.observations),
                "evidence": [
                    {
                        "hypothesis": item.hypothesis.value,
                        "horizon_seconds": item.horizon_seconds,
                        "decision": item.bundle.decision.value,
                        "reason_codes": item.bundle.reason_codes,
                        "eligible_events": item.bundle.eligible_events,
                        "matched_events": item.bundle.matched_events,
                        "mean_lift_bps": item.bundle.mean_lift_bps,
                        "matched_sample_mean_lift_bps": item.matched_mean_lift_bps,
                        "matched_sample_positive_lift_rate": item.matched_positive_lift_rate,
                    }
                    for item in execution.result.evidence
                ],
            }
        )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
