from __future__ import annotations

import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIB_SPEC = importlib.util.spec_from_file_location(
    "research_price_prediction_lib",
    ROOT / "scripts" / "research_price_prediction_lib.py",
)
assert LIB_SPEC and LIB_SPEC.loader
lib = importlib.util.module_from_spec(LIB_SPEC)
sys.modules["research_price_prediction_lib"] = lib
LIB_SPEC.loader.exec_module(lib)

TRAIN_SPEC = importlib.util.spec_from_file_location(
    "research_train_price_models",
    ROOT / "scripts" / "research_train_price_models.py",
)
assert TRAIN_SPEC and TRAIN_SPEC.loader
trainer = importlib.util.module_from_spec(TRAIN_SPEC)
sys.modules["research_train_price_models"] = trainer
TRAIN_SPEC.loader.exec_module(trainer)

PATTERN_SPEC = importlib.util.spec_from_file_location(
    "research_mine_price_patterns",
    ROOT / "scripts" / "research_mine_price_patterns.py",
)
assert PATTERN_SPEC and PATTERN_SPEC.loader
patterns = importlib.util.module_from_spec(PATTERN_SPEC)
sys.modules["research_mine_price_patterns"] = patterns
PATTERN_SPEC.loader.exec_module(patterns)


def _candle(index: int, close: float, *, volume: float = 100.0) -> object:
    at = datetime(2026, 7, 15, 7, 5, tzinfo=timezone.utc) + timedelta(minutes=index)
    return lib.ResearchCandle(
        ticker="SBER",
        at=at,
        open=close,
        high=close * 1.0005,
        low=close * 0.9995,
        close=close,
        volume=volume,
        complete=True,
    )


def _event(at: datetime) -> object:
    return lib.SignalEvent(
        ticker="SBER",
        signal_type="price_jump",
        family="directional",
        direction=1,
        source_event_at=at,
        trading_day=date(2026, 7, 15),
        session_bucket=0,
        event_move_bps=100.0,
        baseline_move_bps=1.0,
        z_score=5.0,
        volume_z_score=4.0,
        range_z_score=4.0,
        candle_range_bps=10.0,
        baseline_volatility_bps=2.0,
        anchor_price=100.0,
    )


def test_cache_manifest_redacts_secrets() -> None:
    payload = lib.build_cache_manifest(
        tickers=("SBER",),
        start_day=date(2026, 7, 1),
        end_day=date(2026, 7, 2),
        row_counts={"SBER/2026-07-01": 10},
        content_fingerprint="abc",
    )

    encoded = json.dumps(payload, ensure_ascii=False)
    assert payload["privacy"] == {
        "account_identifiers_persisted": False,
        "instrument_uids_persisted": False,
        "tokens_persisted": False,
    }
    assert "token-value" not in encoded
    assert "uid" not in encoded.lower().replace("instrument_uids_persisted", "")


def test_feature_windows_never_include_future_candles() -> None:
    signal = _event(datetime(2026, 7, 15, 7, 10, tzinfo=timezone.utc))
    rows = [
        _candle(0, 100.0),
        _candle(1, 100.1),
        _candle(2, 100.2),
        _candle(3, 100.3),
        _candle(4, 100.4),
        _candle(5, 100.5),
        _candle(6, 1000.0, volume=1_000_000.0),
    ]

    features = lib._pre_signal_features(rows, signal, (5,))

    assert features["feature_leakage_flag"] is False
    assert features["feature_max_observed_at"] == rows[4].at.isoformat()
    assert float(features["pre_return_bps_5m"]) < 60.0


def test_gap_after_signal_makes_label_unavailable() -> None:
    signal = _event(datetime(2026, 7, 15, 7, 10, tzinfo=timezone.utc))
    rows = [
        lib.ResearchCandle("SBER", signal.source_event_at, 100, 100, 100, 100, 100),
        lib.ResearchCandle(
            "SBER",
            signal.source_event_at + timedelta(minutes=2),
            101,
            101,
            101,
            101,
            100,
        ),
    ]

    outcome = lib._outcome_fields(signal, 60, rows, lib.ReplayPolicy())

    assert outcome["forward_available"] is False
    assert outcome["forward_reason_code"] == "forward_price_unavailable_or_session_gap"
    assert outcome["triple_barrier_label"] == "unavailable"


def test_triple_barrier_label_is_deterministic() -> None:
    signal = _event(datetime(2026, 7, 15, 7, 10, tzinfo=timezone.utc))
    path = [
        lib.ResearchCandle("SBER", signal.source_event_at + timedelta(minutes=1), 100, 100, 100, 100.05, 100),
        lib.ResearchCandle("SBER", signal.source_event_at + timedelta(minutes=2), 100, 100, 100, 100.20, 100),
    ]

    first = lib._triple_barrier_label(signal, path, lib.ReplayPolicy(triple_barrier_bps=10.0))
    second = lib._triple_barrier_label(signal, path, lib.ReplayPolicy(triple_barrier_bps=10.0))

    assert first == second == "take_profit"


def test_dataset_builder_produces_no_leakage_rows_on_synthetic_signal() -> None:
    candles = []
    price = 100.0
    for index in range(70):
        if index == 45:
            price = 102.0
            volume = 10_000.0
        elif index > 45:
            price += 0.05
            volume = 200.0
        else:
            price += 0.01
            volume = 100.0
        candles.append(_candle(index, price, volume=volume))

    rows, manifest = lib.build_signal_price_dataset(
        candles,
        horizons_seconds=(60, 300),
        lookback_windows=(5, 15, 30, 60),
        max_signals_per_instrument=100,
    )

    assert manifest["quality"]["signals"] > 0
    assert manifest["quality"]["rows"] == len(rows)
    assert manifest["quality"]["feature_leakage_rows"] == 0
    assert {"price_jump", "volume_spike", "candle_range_spike"} & set(manifest["quality"]["signals_by_type"])


def test_research_runner_writes_required_artifacts(tmp_path: Path) -> None:
    rows = []
    for day_index in range(4):
        day = date(2026, 7, 1 + day_index)
        for index in range(12):
            rows.append(
                {
                    field: ""
                    for field in lib.DATASET_FIELDS
                }
                | {
                    "row_id": f"{day}-{index}",
                    "ticker": "SBER",
                    "signal_type": "price_jump",
                    "family": "directional",
                    "direction": "1",
                    "source_event_at": f"{day.isoformat()}T07:{index:02d}:00+00:00",
                    "trading_day": day.isoformat(),
                    "session_bucket": "0",
                    "horizon_seconds": "300",
                    "z_score": str(4 + index % 3),
                    "volume_z_score": str(2 + index % 4),
                    "range_z_score": str(2 + index % 5),
                    "day_volatility_quantile": "0.5",
                    "forward_available": "True",
                    "cost_adjusted_directional_bps": str(10 if index % 2 else -5),
                    "reverse_directional_bps": str(-20 if index % 2 else 2),
                    "meta_label": "1" if index % 2 else "0",
                }
            )
    dataset = tmp_path / "dataset.csv"
    lib.write_table(dataset, rows)

    result = trainer.run_research(dataset, tmp_path / "runs")
    run_dir = Path(result["run_dir"])

    assert (run_dir / "dataset-manifest.json").exists()
    assert (run_dir / "model-results.json").exists()
    assert (run_dir / "leaderboard.csv").exists()
    assert (run_dir / "feature-importance.csv").exists()
    assert (run_dir / "slice-report.csv").exists()
    assert (run_dir / "report.md").exists()


def test_pattern_mining_promotes_positive_top_decile_rule() -> None:
    rows = []
    for index in range(320):
        rows.append(
            {
                "ticker": "SBER",
                "signal_type": "price_jump",
                "horizon_seconds": "900",
                "session_bucket": "0",
                "_volatility_bucket": "high",
                "combo_key_300s": "price_jump+volume_spike",
                "trading_day": f"2026-07-{1 + index % 25:02d}",
                "_target": 1 if index % 2 == 0 else 0,
                "_predicted_probability": 0.9 - index / 10_000,
                "_cost_adjusted_directional_bps": 8.0 if index % 2 == 0 else 1.0,
                "_reverse_directional_bps": -18.0,
            }
        )
    for index in range(80):
        rows.append(
            {
                "ticker": "SBER",
                "signal_type": "volume_spike",
                "horizon_seconds": "60",
                "session_bucket": "3",
                "_volatility_bucket": "low",
                "combo_key_300s": "volume_spike",
                "trading_day": "2026-07-01",
                "_target": 0,
                "_predicted_probability": 0.1,
                "_cost_adjusted_directional_bps": -10.0,
                "_reverse_directional_bps": 0.0,
            }
        )

    candidates = patterns.mine_pattern_candidates(
        rows,
        naive_positive_rate=0.05,
        top_fraction=0.80,
        min_n=100,
        accepted_min_n=300,
    )

    accepted = [row for row in candidates if row["accepted_exploratory"]]
    assert accepted
    detailed = [
        row
        for row in accepted
        if row["group_set"] == "signal_horizon_session_volatility"
    ]
    assert detailed
    assert "signal_type=price_jump" in detailed[0]["rule"]
    assert detailed[0]["positive_rate"] == 0.5
    assert detailed[0]["mean_cost_adjusted_directional_bps"] > 0
