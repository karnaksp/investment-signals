#!/usr/bin/env python3
"""Replay preregistered candle hypotheses from an existing local cache."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from tinvest_signal_engine.adapters.local_hypothesis_replay import (
    ImmutableReplayArtifactStore,
    LocalCandleCache,
)
from tinvest_signal_engine.application.historical_hypothesis_replay import (
    DEFAULT_LIQUID_UNIVERSE,
    SUPPORTED_HYPOTHESES,
    HistoricalReplayRequest,
    RunHistoricalHypothesisReplay,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import ReplayCostModel
from tinvest_signal_engine.domain.hypothesis_formulas import HypothesisId


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Локально проверить научные гипотезы без повторной загрузки T-Invest",
    )
    result.add_argument("--cache-dir", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, default=Path("var/research/hypothesis-runs"))
    selection = result.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--hypothesis",
        action="append",
        choices=[item.value for item in SUPPORTED_HYPOTHESES],
    )
    selection.add_argument("--all", action="store_true")
    result.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    result.add_argument("--cost-model-version", default="1.0.0")
    result.add_argument("--commission-bps", type=float, default=3.0)
    result.add_argument("--slippage-bps", type=float, default=3.0)
    result.add_argument("--entry-half-spread-bps", type=float, default=2.0)
    result.add_argument("--exit-half-spread-bps", type=float, default=2.0)
    result.add_argument(
        "--liquid-universe",
        default=",".join(DEFAULT_LIQUID_UNIVERSE),
        help="Фиксированный список тикеров через запятую для H6",
    )
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    selected = (
        SUPPORTED_HYPOTHESES
        if args.all
        else tuple(HypothesisId(value) for value in args.hypothesis)
    )
    request = HistoricalReplayRequest(
        selected_hypotheses=selected,
        cost_model=ReplayCostModel(
            version=args.cost_model_version,
            commission_bps=args.commission_bps,
            slippage_bps=args.slippage_bps,
            half_spread_entry_bps=args.entry_half_spread_bps,
            half_spread_exit_bps=args.exit_half_spread_bps,
        ),
        liquid_universe=tuple(
            item.strip().upper()
            for item in args.liquid_universe.split(",")
            if item.strip()
        ),
        resume=args.resume,
    )
    execution = RunHistoricalHypothesisReplay(
        cache=LocalCandleCache(args.cache_dir),
        artifacts=ImmutableReplayArtifactStore(args.output_dir),
    ).execute(request)
    payload = {
        "run_id": execution.completion.run_id,
        "artifact_fingerprint": execution.completion.artifact_fingerprint,
        "dataset_fingerprint": execution.completion.dataset_fingerprint,
        "selected_hypotheses": [
            item.value for item in execution.completion.selected_hypotheses
        ],
        "cache_reused": True,
        "run_resumed": execution.completion.resumed,
        "network_download_performed": False,
    }
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
