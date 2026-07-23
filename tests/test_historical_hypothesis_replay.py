from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import tracemalloc

import pytest

import tinvest_signal_engine.adapters.local_hypothesis_replay as replay_artifacts
from tinvest_signal_engine.adapters.local_hypothesis_replay import (
    ImmutableReplayArtifactStore,
    LocalCandleCache,
)
from tinvest_signal_engine.application.historical_hypothesis_replay import (
    HistoricalReplayRequest,
    RunHistoricalHypothesisReplay,
    SUPPORTED_HYPOTHESES,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.domain.historical_hypothesis_replay import (
    CandleCacheDescriptor,
    HistoricalCandle,
    ReplayCostModel,
)
from tinvest_signal_engine.domain.hypothesis_formulas import (
    HypothesisId,
    ObservationVerdict,
)
from tinvest_signal_engine.domain.trading_phases import TradingPhase


UTC = timezone.utc
START_DAY = date(2026, 1, 5)


class _Cache:
    def __init__(self, candles: tuple[HistoricalCandle, ...]) -> None:
        self.candles = candles
        self.load_calls = 0
        days = {item.at.date() for item in candles}
        tickers = {item.ticker for item in candles}
        self.descriptor = CandleCacheDescriptor(
            dataset_fingerprint="sha256:" + "7" * 64,
            partition_count=len(days) * len(tickers),
            tickers=tuple(sorted(tickers)),
            start_day=min(days),
            end_day=max(days),
        )

    def describe(self) -> CandleCacheDescriptor:
        return self.descriptor

    def load(self) -> tuple[HistoricalCandle, ...]:
        self.load_calls += 1
        return self.candles


class _PartitionedCache(_Cache):
    def __init__(self, candles: tuple[HistoricalCandle, ...]) -> None:
        super().__init__(candles)
        self.partition_passes = 0
        self.maximum_partition_size = 0

    def load(self) -> tuple[HistoricalCandle, ...]:
        self.load_calls += 1
        raise AssertionError("partitioned replay must not load the full candle cache")

    def iter_ticker_partitions(self):
        self.partition_passes += 1
        for ticker in self.descriptor.tickers:
            partition = tuple(item for item in self.candles if item.ticker == ticker)
            self.maximum_partition_size = max(
                self.maximum_partition_size,
                len(partition),
            )
            yield partition


def _costs() -> ReplayCostModel:
    return ReplayCostModel(
        version="cost-v1",
        commission_bps=3.0,
        slippage_bps=3.0,
        half_spread_entry_bps=2.0,
        half_spread_exit_bps=2.0,
    )


def _gate() -> EvidenceGatePolicy:
    return EvidenceGatePolicy(
        minimum_trading_days=1,
        minimum_eligible_events=1,
        controls_per_event=1,
        bootstrap_samples=25,
        required_positive_stability_blocks=1,
        maximum_instrument_share=0.99,
    )


def _minute_ranges() -> tuple[range, ...]:
    return (
        range(7 * 60, 7 * 60 + 15),
        range(10 * 60, 11 * 60 + 1),
        range(18 * 60 + 10, 18 * 60 + 40),
    )


def _fixture_candles(*, future_multiplier: float = 1.0) -> tuple[HistoricalCandle, ...]:
    rows: list[HistoricalCandle] = []
    for day_index in range(40):
        trading_day = START_DAY + timedelta(days=day_index)
        holdout_pattern = day_index >= 24
        high_volume = day_index % 2 == 1
        for ticker_index, ticker in enumerate(("SBER", "GAZP")):
            ticker_bias = ticker_index * 0.01
            morning_deviation = 2.0 if holdout_pattern else ((day_index % 5) - 2) * 0.05
            morning_volume = (
                200.0
                if holdout_pattern and high_volume
                else (20.0 if holdout_pattern else 100.0)
            )
            for minute_range in _minute_ranges():
                for local_minute in minute_range:
                    hour, minute = divmod(local_minute, 60)
                    at = datetime.combine(
                        trading_day,
                        time(hour=hour - 3, minute=minute),
                        tzinfo=UTC,
                    )
                    if local_minute < 10 * 60:
                        close = 100.0 + ticker_bias + morning_deviation
                        volume = morning_volume
                        span = 0.40 if holdout_pattern and high_volume else 0.02
                    elif local_minute <= 11 * 60:
                        progress = (local_minute - 10 * 60) / 60
                        close = 100.0 + ticker_bias + progress * future_multiplier
                        volume = 100.0 + (
                            500.0
                            if high_volume and local_minute >= 10 * 60 + 15
                            else 0.0
                        )
                        span = 0.04 + (0.20 if high_volume else 0.0)
                    else:
                        progress = (local_minute - (18 * 60 + 10)) / 29
                        close = 101.0 + ticker_bias + progress * 0.5 * future_multiplier
                        volume = 100.0
                        span = 0.04
                    rows.append(
                        HistoricalCandle(
                            ticker=ticker,
                            at=at,
                            open=close,
                            high=close + span,
                            low=close - span,
                            close=close,
                            volume=volume,
                        )
                    )
    return tuple(rows)


def _request(*hypotheses: HypothesisId) -> HistoricalReplayRequest:
    return HistoricalReplayRequest(
        selected_hypotheses=hypotheses or SUPPORTED_HYPOTHESES,
        cost_model=_costs(),
        liquid_universe=("SBER", "GAZP"),
    )


def test_full_portfolio_replay_is_causal_partitioned_and_resumable(
    tmp_path: Path,
) -> None:
    cache = _Cache(_fixture_candles())
    artifacts = ImmutableReplayArtifactStore(tmp_path / "runs")
    use_case = RunHistoricalHypothesisReplay(
        cache=cache,
        artifacts=artifacts,
        gate_policy=_gate(),
    )

    first = use_case.execute(_request())

    assert first.report is not None
    assert cache.load_calls == 1
    assert first.report.split is not None
    assert len(first.report.split.train_days) == 24
    assert len(first.report.split.validation_days) == 8
    assert len(first.report.split.holdout_days) == 8
    assert {item.hypothesis_id for item in first.report.summaries} == set(
        SUPPORTED_HYPOTHESES
    )
    assert all(item.evaluated_observations > 0 for item in first.report.summaries)
    assert all(
        item.feature_cutoff_at <= item.event_at for item in first.report.outcomes
    )
    assert any(
        item.hypothesis_id is HypothesisId.H1
        and item.phase is TradingPhase.MORNING_LOW_LIQUIDITY
        and item.verdict is ObservationVerdict.MATCHED
        for item in first.report.outcomes
    )
    assert any(item.hypothesis_id is HypothesisId.H6 for item in first.report.outcomes)
    assert len(first.report.evidence) == len(SUPPORTED_HYPOTHESES)
    assert first.completion.resumed is False

    second = use_case.execute(_request())

    assert second.report is None
    assert second.completion.resumed is True
    assert (
        second.completion.artifact_fingerprint == first.completion.artifact_fingerprint
    )
    assert cache.load_calls == 1


def test_partitioned_full_portfolio_is_bitwise_equivalent_and_never_loads_all(
    tmp_path: Path,
) -> None:
    candles = _fixture_candles()
    monolithic = RunHistoricalHypothesisReplay(
        cache=_Cache(candles),
        artifacts=ImmutableReplayArtifactStore(tmp_path / "monolithic"),
        gate_policy=_gate(),
    ).execute(_request())
    partitioned_cache = _PartitionedCache(candles)
    partitioned = RunHistoricalHypothesisReplay(
        cache=partitioned_cache,
        artifacts=ImmutableReplayArtifactStore(tmp_path / "partitioned"),
        gate_policy=_gate(),
    ).execute(_request())

    assert partitioned.report == monolithic.report
    assert (
        partitioned.completion.artifact_fingerprint
        == monolithic.completion.artifact_fingerprint
    )
    assert partitioned_cache.load_calls == 0
    assert partitioned_cache.partition_passes == 2
    assert partitioned_cache.maximum_partition_size < len(candles)


def test_outcome_artifact_writer_has_bounded_incremental_memory(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outcomes.jsonl"

    def rows():
        for index in range(20_000):
            yield {"index": index, "payload": "x" * 1_024}

    tracemalloc.start()
    try:
        artifact_hash = replay_artifacts._write_jsonl_once_or_verify(
            path,
            rows(),
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert path.stat().st_size > 20_000_000
    assert artifact_hash.startswith("sha256:")
    assert peak < 4_000_000
    assert (
        replay_artifacts._write_jsonl_once_or_verify(
            path,
            rows(),
        )
        == artifact_hash
    )


def test_future_price_changes_labels_but_not_h1_trigger_decisions(
    tmp_path: Path,
) -> None:
    def run(candles: tuple[HistoricalCandle, ...], suffix: str) -> object:
        return (
            RunHistoricalHypothesisReplay(
                cache=_Cache(candles),
                artifacts=ImmutableReplayArtifactStore(tmp_path / suffix),
                gate_policy=_gate(),
            )
            .execute(_request(HypothesisId.H1))
            .report
        )

    rising = run(_fixture_candles(future_multiplier=1.0), "rising")
    falling = run(_fixture_candles(future_multiplier=-1.0), "falling")
    assert rising is not None and falling is not None
    rising_decisions = {
        (item.ticker, item.event_at, item.verdict, item.reason)
        for item in rising.outcomes
    }
    falling_decisions = {
        (item.ticker, item.event_at, item.verdict, item.reason)
        for item in falling.outcomes
    }

    assert rising_decisions == falling_decisions
    assert {
        item.net_effect_bps for item in rising.outcomes if item.label_available
    } != {item.net_effect_bps for item in falling.outcomes if item.label_available}


def test_local_cache_adapter_reads_fixture_without_modifying_cache(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"
    partition = cache_dir / "ticker=SBER" / "date=2026-01-05.jsonl"
    partition.parent.mkdir(parents=True)
    candle = HistoricalCandle(
        ticker="SBER",
        at=datetime(2026, 1, 5, 4, 0, tzinfo=UTC),
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1000.0,
    )
    partition.write_text(
        json.dumps(
            {
                "ticker": candle.ticker,
                "at": candle.at.isoformat(),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "complete": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (cache_dir / "manifest.json").write_text(
        json.dumps(
            {
                "kind": "tinvest_research_candle_cache",
                "scope": {
                    "tickers": ["SBER"],
                    "from": "2026-01-05",
                    "to": "2026-01-05",
                },
                "quality": {"partition_count": 1},
                "privacy": {
                    "tokens_persisted": False,
                    "account_identifiers_persisted": False,
                    "instrument_uids_persisted": False,
                },
                "content_fingerprint": "a" * 64,
            }
        ),
        encoding="utf-8",
    )
    before = {
        path.relative_to(cache_dir): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in cache_dir.rglob("*")
        if path.is_file()
    }

    adapter = LocalCandleCache(cache_dir)
    descriptor = adapter.describe()
    loaded = adapter.load()
    after = {
        path.relative_to(cache_dir): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in cache_dir.rglob("*")
        if path.is_file()
    }

    assert descriptor.dataset_fingerprint == "sha256:" + "a" * 64
    assert loaded == (candle,)
    assert after == before


def test_stale_incremental_manifest_falls_back_to_all_cached_partitions(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "cache"
    first = cache_dir / "ticker=SBER" / "date=2026-01-05.jsonl"
    second = cache_dir / "ticker=SBER" / "date=2026-01-06.jsonl"
    first.parent.mkdir(parents=True)
    record = {
        "ticker": "SBER",
        "at": "2026-01-05T04:00:00+00:00",
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100,
        "volume": 100,
        "complete": True,
    }
    first.write_text(json.dumps(record) + "\n", encoding="utf-8")
    second.write_text(
        json.dumps(
            {
                **record,
                "at": "2026-01-06T04:00:00+00:00",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (cache_dir / "manifest.json").write_text(
        json.dumps(
            {
                "kind": "tinvest_research_candle_cache",
                "scope": {
                    "tickers": ["SBER"],
                    "from": "2026-01-06",
                    "to": "2026-01-06",
                },
                "quality": {"partition_count": 1},
                "privacy": {
                    "tokens_persisted": False,
                    "account_identifiers_persisted": False,
                    "instrument_uids_persisted": False,
                },
                "content_fingerprint": "b" * 64,
            }
        ),
        encoding="utf-8",
    )

    descriptor = LocalCandleCache(cache_dir).describe()

    assert descriptor.partition_count == 2
    assert descriptor.start_day == date(2026, 1, 5)
    assert descriptor.end_day == date(2026, 1, 6)
    assert descriptor.dataset_fingerprint != "sha256:" + "b" * 64


def test_immutable_artifact_detects_tampering(tmp_path: Path) -> None:
    cache = _Cache(_fixture_candles())
    artifacts = ImmutableReplayArtifactStore(tmp_path)
    first = RunHistoricalHypothesisReplay(
        cache=cache,
        artifacts=artifacts,
        gate_policy=_gate(),
    ).execute(_request(HypothesisId.H1))
    run_dir = tmp_path / first.completion.run_id.removeprefix("sha256:")
    (run_dir / "evidence.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="failed verification"):
        artifacts.load_completed(first.completion.run_id)


def test_h1_threshold_matches_canonical_half_volume_rule(tmp_path: Path) -> None:
    candles = _fixture_candles()
    cache = _Cache(candles)
    report = (
        RunHistoricalHypothesisReplay(
            cache=cache,
            artifacts=ImmutableReplayArtifactStore(tmp_path),
            gate_policy=_gate(),
        )
        .execute(_request(HypothesisId.H1))
        .report
    )
    assert report is not None
    assert any(item.verdict is ObservationVerdict.MATCHED for item in report.outcomes)
