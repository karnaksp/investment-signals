from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from tinvest_signal_engine.adapters.r2_extension_replay import (
    R2ExtensionReplayArtifactAdapter,
)
from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    BuildR2ExtensionReplay,
    R2ExtensionRequest,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2ExtensionHypothesis,
)
from tinvest_signal_engine.services.hypothesis_replay_api import (
    LocalHypothesisPortfolioRunner,
    ReplayEvidenceResponse,
    StartReplayRequest,
)


MOSCOW = ZoneInfo("Europe/Moscow")
DATASET = "sha256:" + "d" * 64


def _candles() -> tuple[HistoricalCandle, ...]:
    rows: list[HistoricalCandle] = []
    start = date(2026, 6, 1)
    closes = {"SBER": 100.0, "GAZP": 200.0}
    for day_offset in range(3):
        trading_day = start + timedelta(days=day_offset)
        for ticker in ("SBER", "GAZP"):
            price = closes[ticker] * (1.0 + (day_offset + 1) / 10_000.0)
            for minute in range(35):
                at = datetime(
                    trading_day.year,
                    trading_day.month,
                    trading_day.day,
                    10,
                    minute,
                    tzinfo=MOSCOW,
                )
                next_price = price * (1.0 + (0.0002 if ticker == "SBER" else 0.0001))
                rows.append(
                    HistoricalCandle(
                        ticker=ticker,
                        at=at,
                        open=price,
                        high=max(price, next_price),
                        low=min(price, next_price),
                        close=next_price,
                        volume=1_000.0,
                    )
                )
                price = next_price
            closes[ticker] = price
    return tuple(rows)


class _Cache:
    def __init__(self, candles: tuple[HistoricalCandle, ...]) -> None:
        self.candles = candles
        self.describe_calls = 0
        self.load_calls = 0

    def describe(self):
        self.describe_calls += 1
        return SimpleNamespace(dataset_fingerprint=DATASET)

    def load(self):
        self.load_calls += 1
        return self.candles


def test_application_use_case_reads_injected_cache_once() -> None:
    candles = _candles()
    cache = _Cache(candles)
    days = tuple(sorted({item.at.date() for item in candles}))

    report = BuildR2ExtensionReplay(cache).execute(
        R2ExtensionRequest(
            market_universe=("GAZP",),
            exchange_schedule_known_days=days,
        )
    )

    assert cache.describe_calls == 1
    assert cache.load_calls == 1
    assert report.dataset_fingerprint == DATASET
    assert {item.hypothesis for item in report.features} == set(R2ExtensionHypothesis)


def test_artifact_is_immutable_and_exposes_exact_fail_closed_horizons(
    tmp_path: Path,
) -> None:
    candles = _candles()
    days = tuple(sorted({item.at.date() for item in candles}))
    report = BuildR2ExtensionReplay(_Cache(candles)).execute(
        R2ExtensionRequest(
            market_universe=("GAZP",),
            exchange_schedule_known_days=days,
        )
    )
    adapter = R2ExtensionReplayArtifactAdapter(tmp_path)
    reasons = (
        "independent_evidence_gate_unavailable",
        "r2_reference_data_unavailable",
    )

    artifact = adapter.save(
        report,
        tuple(R2ExtensionHypothesis),
        cost_model_version="1.0.0",
        blocking_reason_codes=reasons,
    )
    repeated = adapter.save(
        report,
        tuple(R2ExtensionHypothesis),
        cost_model_version="1.0.0",
        blocking_reason_codes=reasons,
    )

    assert repeated == artifact
    assert [item["hypothesis_id"] for item in artifact.evidence] == ["H10", "H11"]
    assert [
        tuple(row["horizon_seconds"] for row in item["horizons"])
        for item in artifact.evidence
    ] == [(1800, 3600), (900, 1800)]
    assert all(item["decision"] == "blocked_by_data" for item in artifact.evidence)
    assert all(item["source_data_state"] == "unavailable" for item in artifact.evidence)
    assert all(item["independent_validation"] is False for item in artifact.evidence)
    assert all(
        row["evidence_scope"] == "not_evaluated"
        and row["decision"] == "blocked_by_data"
        for item in artifact.evidence
        for row in item["horizons"]
    )
    assert all(
        ReplayEvidenceResponse.model_validate(item).decision == "blocked_by_data"
        for item in artifact.evidence
    )
    manifest = json.loads((Path(artifact.artifact_uri) / "manifest.json").read_text())
    assert manifest["kind"] == "causal_h10_h11_r2_replay"
    assert manifest["schema_version"] == 3
    assert manifest["blocking_reason_codes"] == list(reasons)
    assert manifest["feature_set_fingerprint"].startswith("sha256:")

    evidence_path = Path(artifact.artifact_uri) / "evidence.json"
    evidence_path.write_text("corrupted", encoding="utf-8")
    with pytest.raises(ValueError, match="immutable"):
        adapter.save(
            report,
            tuple(R2ExtensionHypothesis),
            cost_model_version="1.0.0",
            blocking_reason_codes=reasons,
        )


def test_runner_composes_real_r2_use_case_and_immutable_adapter(
    tmp_path: Path,
) -> None:
    runner = LocalHypothesisPortfolioRunner(
        cache_dir=tmp_path / "cache",
        artifact_root=tmp_path / "artifacts",
    )
    cache = _Cache(_candles())
    runner._descriptor_cache = cache  # type: ignore[assignment]

    result = runner.execute(
        StartReplayRequest(
            hypothesis_ids=("H10", "H11"),
            liquid_universe=("GAZP",),
        ),
        run_fingerprint="sha256:" + "r" * 64,
    )

    assert cache.describe_calls == 1
    assert cache.load_calls == 1
    assert tuple(item["hypothesis_id"] for item in result["evidence"]) == (
        "H10",
        "H11",
    )
    validated = tuple(
        ReplayEvidenceResponse.model_validate(item) for item in result["evidence"]
    )
    assert tuple(item.decision for item in validated) == (
        "blocked_by_data",
        "blocked_by_data",
    )
    assert tuple(
        tuple(row.horizon_seconds for row in item.horizons) for item in validated
    ) == ((1800, 3600), (900, 1800))
    artifact_uri = Path(result["engines"][0]["artifact_uri"])
    assert (artifact_uri / "manifest.json").is_file()
    assert (artifact_uri / "evidence.json").is_file()
