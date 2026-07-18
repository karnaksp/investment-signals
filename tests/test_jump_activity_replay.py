from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from tinvest_signal_engine.adapters.jump_activity_replay import (
    JsonJumpReplayArtifactAdapter,
    ParquetCandleCacheAdapter,
)
from tinvest_signal_engine.application.hypothesis_evidence import EvidenceGatePolicy
from tinvest_signal_engine.application.jump_activity_replay import (
    RunJumpActivityReplay,
    TrainingProfile,
    build_jump_observations,
    build_raw_jump_features,
    build_training_profiles,
    classify_jump_feature,
    replay_jump_activity_hypotheses,
)
from tinvest_signal_engine.domain.hypothesis_evidence import EvidenceDecision
from tinvest_signal_engine.domain.jump_activity_replay import (
    CandleBar,
    ClassifiedJumpFeature,
    CostModel,
    FeatureThresholds,
    JumpHypothesis,
    JumpReplayPolicy,
    RawJumpFeature,
)


UTC = timezone.utc
START = date(2026, 1, 5)


def _policy() -> JumpReplayPolicy:
    return JumpReplayPolicy(
        version="fixture-v1",
        history_window_minutes=5,
        volatility_window_minutes=6,
        minimum_training_observations=20,
        horizons_seconds=(300, 900, 1800),
        cost_model=CostModel(version="fixture-cost-v1", round_trip_bps=1.0),
    )


def _gate_policy() -> EvidenceGatePolicy:
    return EvidenceGatePolicy(
        minimum_trading_days=1,
        minimum_eligible_events=1,
        controls_per_event=5,
        bootstrap_samples=50,
        bootstrap_seed=7,
        maximum_instrument_share=0.80,
    )


def _fixture_candles(
    *,
    days: int = 10,
    tickers: tuple[str, ...] = ("SBER", "GAZP", "LKOH"),
    holdout_multiplier: float = 1.0,
) -> tuple[CandleBar, ...]:
    result: list[CandleBar] = []
    for ticker_index, ticker in enumerate(tickers):
        for day_offset in range(days):
            trading_day = START + timedelta(days=day_offset)
            previous = 100.0 + ticker_index * 10
            is_holdout = day_offset >= int(days * 0.8)
            for minute in range(90):
                opened_at = datetime.combine(
                    trading_day,
                    datetime.min.time(),
                    tzinfo=UTC,
                ) + timedelta(hours=7, minutes=minute)
                baseline = 100.0 + ticker_index * 10 + minute * 0.001
                close = baseline
                volume = 100.0 + (minute % 11) * 3.0
                if is_holdout and 7 <= minute <= 35:
                    volume = 1.0
                if is_holdout and minute == 40:
                    close = baseline + 1.2 * holdout_multiplier
                    volume = 0.5
                if is_holdout and minute == 50:
                    close = baseline + 1.2 * holdout_multiplier
                    volume = 2_000_000.0 * holdout_multiplier
                if is_holdout and minute > 50:
                    close = baseline + 2.0 * holdout_multiplier
                high = max(previous, close) + (0.01 if minute != 50 else 0.5)
                low = min(previous, close) - (0.01 if minute != 50 else 0.5)
                result.append(
                    CandleBar(
                        ticker=ticker,
                        opened_at=opened_at,
                        open_price=previous,
                        high_price=high,
                        low_price=low,
                        close_price=close,
                        volume=volume,
                    )
                )
                previous = close
    return tuple(result)


def _raw(
    *,
    observed_at: datetime,
    volume: float,
    range_bps: float,
    illiquidity: float,
    movement: float = 200.0,
) -> RawJumpFeature:
    return RawJumpFeature(
        feature_id=f"feature-{observed_at.isoformat()}-{volume}",
        ticker="SBER",
        observed_at=observed_at,
        trading_day=observed_at.date(),
        session_bucket="10:00-10:59",
        anchor_price=100.0,
        five_minute_return_bps=movement,
        absolute_return_bps=abs(movement),
        five_minute_volume=volume,
        five_minute_range_bps=range_bps,
        illiquidity_proxy=illiquidity,
        prior_volatility_bps=5.0,
        feature_max_observed_at=observed_at,
    )


def _profile() -> TrainingProfile:
    samples = tuple(float(index) for index in range(1, 101))
    return TrainingProfile(
        thresholds=FeatureThresholds(
            ticker="SBER",
            session_bucket="10:00-10:59",
            training_observations=100,
            jump_absolute_return_bps=100.0,
            median_volume=50.0,
            high_volume=90.0,
            high_range_bps=90.0,
            high_illiquidity=90.0,
            volatility_low_bps=3.0,
            volatility_high_bps=7.0,
        ),
        volumes=samples,
        ranges_bps=samples,
        illiquidity=samples,
    )


def test_raw_feature_rejects_any_future_feature_timestamp() -> None:
    observed_at = datetime(2026, 1, 5, 10, tzinfo=UTC)
    with pytest.raises(ValueError, match="future market data"):
        replace(
            _raw(
                observed_at=observed_at,
                volume=1.0,
                range_bps=20.0,
                illiquidity=99.0,
            ),
            feature_max_observed_at=observed_at + timedelta(seconds=1),
        )


def test_h3_and_h4_are_mutually_exclusive_preregistered_regimes() -> None:
    at = datetime(2026, 1, 5, 10, tzinfo=UTC)
    profiles = {("SBER", "10:00-10:59"): _profile()}
    policy = _policy()

    h3 = classify_jump_feature(
        _raw(observed_at=at, volume=10.0, range_bps=50.0, illiquidity=95.0),
        profiles,
        policy,
    )
    h4 = classify_jump_feature(
        _raw(
            observed_at=at + timedelta(minutes=1),
            volume=95.0,
            range_bps=95.0,
            illiquidity=20.0,
        ),
        profiles,
        policy,
    )
    middle = classify_jump_feature(
        _raw(
            observed_at=at + timedelta(minutes=2),
            volume=70.0,
            range_bps=95.0,
            illiquidity=95.0,
        ),
        profiles,
        policy,
    )

    assert h3 is not None and h3.hypothesis is JumpHypothesis.LOW_ACTIVITY_REVERSAL
    assert h4 is not None and h4.hypothesis is JumpHypothesis.HIGH_ACTIVITY_CONTINUATION
    assert middle is not None and middle.hypothesis is None


def test_training_thresholds_do_not_change_when_holdout_is_mutated() -> None:
    policy = _policy()
    baseline = _fixture_candles(holdout_multiplier=1.0)
    mutated = _fixture_candles(holdout_multiplier=100.0)
    baseline_features = build_raw_jump_features(baseline, policy)
    mutated_features = build_raw_jump_features(mutated, policy)
    days = sorted({item.trading_day for item in baseline_features})
    from tinvest_signal_engine.application.hypothesis_evidence import BuildChronologicalSplit

    split = BuildChronologicalSplit().execute(days)

    baseline_profiles = build_training_profiles(baseline_features, split, policy)
    mutated_profiles = build_training_profiles(mutated_features, split, policy)

    assert baseline_profiles == mutated_profiles


def test_raw_features_are_causal_and_use_only_contiguous_minutes() -> None:
    candles = list(_fixture_candles(days=5, tickers=("SBER",)))
    removed = candles.pop(20)
    features = build_raw_jump_features(candles, _policy())

    assert all(item.feature_max_observed_at <= item.observed_at for item in features)
    affected = {
        removed.observed_at + timedelta(minutes=offset) for offset in range(0, 6)
    }
    assert not affected & {item.observed_at for item in features}


def test_outcome_refuses_to_bridge_missing_forward_candle() -> None:
    at = datetime(2026, 1, 5, 10, tzinfo=UTC)
    classified = classify_jump_feature(
        _raw(observed_at=at, volume=10.0, range_bps=50.0, illiquidity=95.0),
        {("SBER", "10:00-10:59"): _profile()},
        _policy(),
    )
    assert classified is not None
    bars = {
        (
            "SBER",
            at + timedelta(minutes=minute),
        ): CandleBar(
            ticker="SBER",
            opened_at=at + timedelta(minutes=minute - 1),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.0,
            volume=10.0,
        )
        for minute in (1, 2, 4, 5)
    }

    (observation,) = build_jump_observations((classified,), bars, _policy())

    five_minute = next(
        outcome for outcome in observation.outcomes if outcome.horizon_seconds == 300
    )
    assert five_minute.available is False
    assert five_minute.reason_code == "trading_gap_in_horizon"


def test_end_to_end_small_fixture_builds_both_hypotheses_and_six_evidence_tests() -> None:
    result = replay_jump_activity_hypotheses(
        _fixture_candles(),
        input_fingerprint="sha256:fixture",
        policy_fingerprint="sha256:policy",
        run_id="fixture-run",
        policy=_policy(),
        evidence_policy=_gate_policy(),
    )

    assert result.candle_count == 2_700
    assert result.raw_feature_count > 2_000
    assert {item.hypothesis for item in result.observations} == set(JumpHypothesis)
    assert len(result.evidence) == 6
    assert {
        (item.hypothesis, item.horizon_seconds) for item in result.evidence
    } == {
        (hypothesis, horizon)
        for hypothesis in JumpHypothesis
        for horizon in (300, 900, 1800)
    }
    assert all(
        outcome.cost_model_version == "fixture-cost-v1"
        for observation in result.observations
        for outcome in observation.outcomes
    )
    assert any(item.bundle.cost_model_version == "fixture-cost-v1" for item in result.evidence)
    assert any(item.matched_mean_lift_bps is not None for item in result.evidence)
    assert any(item.matched_positive_lift_rate is not None for item in result.evidence)
    assert all(
        item.bundle.decision
        in {
            EvidenceDecision.PASSED,
            EvidenceDecision.REJECTED,
            EvidenceDecision.INCONCLUSIVE,
            EvidenceDecision.BLOCKED_BY_DATA,
        }
        for item in result.evidence
    )
    assert all(
        observation.feature.raw.feature_max_observed_at
        <= observation.feature.raw.observed_at
        for observation in result.observations
    )
    assert not (
        {
            item.feature.raw.feature_id
            for item in result.observations
            if item.hypothesis is JumpHypothesis.LOW_ACTIVITY_REVERSAL
        }
        & {
            item.feature.raw.feature_id
            for item in result.observations
            if item.hypothesis is JumpHypothesis.HIGH_ACTIVITY_CONTINUATION
        }
    )


def test_parquet_adapter_reads_partition_and_fingerprint_tracks_content(
    tmp_path: Path,
) -> None:
    duckdb = pytest.importorskip("duckdb")
    partition = tmp_path / "ticker=SBER" / "date=2026-01-05.parquet"
    partition.parent.mkdir(parents=True)
    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            f"""
            COPY (
                SELECT
                    'SBER'::VARCHAR AS ticker,
                    TIMESTAMPTZ '2026-01-05 07:00:00+00' AS at,
                    100.0::DOUBLE AS open,
                    101.0::DOUBLE AS high,
                    99.0::DOUBLE AS low,
                    100.5::DOUBLE AS close,
                    1000.0::DOUBLE AS volume,
                    true::BOOLEAN AS complete
            ) TO '{str(partition).replace("'", "''")}' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()
    adapter = ParquetCandleCacheAdapter(tmp_path)

    first_fingerprint = adapter.fingerprint()
    (candle,) = adapter.load()
    partition.write_bytes(partition.read_bytes() + b"fingerprint-change")
    second_fingerprint = adapter.fingerprint()

    assert candle.ticker == "SBER"
    assert candle.opened_at == datetime(2026, 1, 5, 7, tzinfo=UTC)
    assert candle.observed_at == datetime(2026, 1, 5, 7, 1, tzinfo=UTC)
    assert first_fingerprint.startswith("sha256:")
    assert second_fingerprint != first_fingerprint


class _CountingCache:
    def __init__(self, candles: tuple[CandleBar, ...]) -> None:
        self._candles = candles
        self.load_count = 0

    def fingerprint(self, tickers: object = None) -> str:
        return "sha256:counting-cache"

    def load(self, tickers: object = None) -> tuple[CandleBar, ...]:
        self.load_count += 1
        return self._candles


def test_artifacts_are_immutable_and_second_run_resumes_without_cache_load(
    tmp_path: Path,
) -> None:
    cache = _CountingCache(_fixture_candles())
    runner = RunJumpActivityReplay(
        candle_cache=cache,
        artifacts=JsonJumpReplayArtifactAdapter(tmp_path),
        evidence_policy=_gate_policy(),
    )

    first = runner.execute(policy=_policy())
    second = runner.execute(policy=_policy())

    assert first.reused is False
    assert second.reused is True
    assert first.run_id == second.run_id
    assert cache.load_count == 1
    artifact_dir = Path(first.artifact_uri)
    assert (artifact_dir / "manifest.json").is_file()
    assert (artifact_dir / "evidence.json").is_file()
    assert (artifact_dir / "observations.jsonl").is_file()
    assert (artifact_dir / "matched-controls.jsonl").is_file()
    assert (artifact_dir / "complete.json").is_file()
    assert first.result is not None
    with pytest.raises(RuntimeError, match="refusing to overwrite immutable replay"):
        JsonJumpReplayArtifactAdapter(tmp_path).persist(
            replace(first.result, input_fingerprint="sha256:different-input")
        )
