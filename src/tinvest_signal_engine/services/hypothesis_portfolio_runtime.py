"""Composition root for durable portfolio orchestration beside replay API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tinvest_signal_engine.adapters.hypothesis_portfolio_files import (
    ImmutableFileHypothesisPortfolioStore,
    SafeFileHypothesisPortfolioProgress,
)
from tinvest_signal_engine.application.hypothesis_portfolio_runner import (
    HypothesisReplayPort,
    PortfolioEvidenceGatePort,
    RunHypothesisPortfolio,
)
from tinvest_signal_engine.application.scientific_portfolio_versions import (
    ScientificPortfolioVersion,
)


@dataclass(frozen=True, slots=True)
class FileHypothesisPortfolioRuntime:
    """Composed application service and its durable local adapters."""

    runner: RunHypothesisPortfolio
    store: ImmutableFileHypothesisPortfolioStore
    progress: SafeFileHypothesisPortfolioProgress
    repaired_progress_runs: int
    portfolio_version: ScientificPortfolioVersion


def build_file_hypothesis_portfolio_runtime(
    *,
    state_dir: str | Path,
    replay: HypothesisReplayPort,
    evidence_gates: PortfolioEvidenceGatePort,
    portfolio_version: ScientificPortfolioVersion = (
        ScientificPortfolioVersion.SEALED_ELEVEN_V1
    ),
) -> FileHypothesisPortfolioRuntime:
    """Build and repair the portfolio runtime without transport dependencies."""

    root = Path(state_dir) / "hypothesis-portfolios"
    store = ImmutableFileHypothesisPortfolioStore(root / "state")
    progress = SafeFileHypothesisPortfolioProgress(root / "progress")
    repaired = progress.repair_from_store(store)
    return FileHypothesisPortfolioRuntime(
        runner=RunHypothesisPortfolio(
            replay=replay,
            evidence_gates=evidence_gates,
            store=store,
            progress=progress,
        ),
        store=store,
        progress=progress,
        repaired_progress_runs=repaired,
        portfolio_version=portfolio_version,
    )
