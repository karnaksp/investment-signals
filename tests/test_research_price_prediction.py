from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import plistlib
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest


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


def test_replay_includes_events_from_the_morning_phase() -> None:
    start = datetime(2026, 7, 15, 4, 0, tzinfo=timezone.utc)  # 07:00 Moscow
    candles = [
        lib.ResearchCandle(
            ticker="SBER",
            at=start + timedelta(minutes=index),
            open=100.0,
            high=100.01,
            low=99.99,
            close=100.0,
            volume=100.0,
        )
        for index in range(8)
    ]
    candles.append(
        lib.ResearchCandle(
            ticker="SBER",
            at=start + timedelta(minutes=8),
            open=100.0,
            high=102.0,
            low=99.9,
            close=101.0,
            volume=100.0,
        )
    )

    signals = lib.replay_signals(
        candles,
        lib.ReplayPolicy(
            detector_window_minutes=1,
            detector_baseline_points=10,
            detector_min_baseline_points=3,
            detector_z_score=1.0,
            min_relative_metric_excursion=0.1,
            volatility_lookback_points=5,
            volatility_min_points=3,
            volatility_floor_bps=0.1,
        ),
    )

    assert any(signal.source_event_at == start + timedelta(minutes=8) for signal in signals)
    assert all(signal.session_bucket == 0 for signal in signals)


def test_replay_signals_adds_event_shape_reversal_features() -> None:
    start = datetime(2026, 7, 15, 7, 5, tzinfo=timezone.utc)
    candles = [
        lib.ResearchCandle(
            ticker="SBER",
            at=start + timedelta(minutes=index),
            open=100.0,
            high=100.05,
            low=99.95,
            close=100.0,
            volume=100.0,
        )
        for index in range(25)
    ]
    candles.append(
        lib.ResearchCandle(
            ticker="SBER",
            at=start + timedelta(minutes=25),
            open=100.0,
            high=110.0,
            low=99.0,
            close=101.0,
            volume=100.0,
            volume_buy=80.0,
            volume_sell=20.0,
        )
    )

    signals = lib.replay_signals(candles)
    price_jump = next(signal for signal in signals if signal.signal_type == "price_jump")

    assert price_jump.direction == 1
    assert price_jump.event_body_to_range == pytest.approx(1 / 11)
    assert price_jump.event_upper_wick_to_range == pytest.approx(9 / 11)
    assert price_jump.candle_close_position == pytest.approx(2 / 11)
    assert price_jump.event_close_to_direction == pytest.approx(2 / 11)
    assert price_jump.event_reversal_pressure == pytest.approx(1.0)
    assert price_jump.event_aggressor_imbalance == pytest.approx(0.6)
    assert price_jump.event_classified_volume_share == pytest.approx(1.0)


def test_pre_signal_aggressor_features_never_include_event_candle() -> None:
    event_at = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    candles = (
        lib.ResearchCandle("SBER", event_at - timedelta(minutes=2), 100, 100, 100, 100, 100, True, 60, 40),
        lib.ResearchCandle("SBER", event_at - timedelta(minutes=1), 100, 100, 100, 100, 100, True, 80, 20),
        lib.ResearchCandle("SBER", event_at, 100, 100, 100, 100, 100, True, 0, 100),
    )

    features = lib._pre_signal_features(candles, _event(event_at), (5,))

    assert float(features["pre_aggressor_imbalance_5m"]) == pytest.approx(0.4)
    assert float(features["pre_classified_volume_share_5m"]) == pytest.approx(1.0)
    assert features["feature_leakage_flag"] is False


def test_pre_signal_technical_features_never_include_event_candle() -> None:
    event_at = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    candles = tuple(
        lib.ResearchCandle(
            "SBER",
            event_at - timedelta(minutes=30 - index),
            100 + index * 0.1,
            100.2 + index * 0.1,
            99.8 + index * 0.1,
            100 + index * 0.1,
            100 + index,
        )
        for index in range(30)
    ) + (
        lib.ResearchCandle("SBER", event_at, 1, 1, 1, 1, 1_000_000),
    )

    features = lib._pre_signal_features(candles, _event(event_at), (30,))

    assert float(features["pre_rsi_30m"]) == pytest.approx(100.0)
    assert float(features["pre_macd_bps_30m"]) > 0
    assert float(features["pre_bollinger_z_30m"]) > 0
    assert float(features["pre_atr_bps_30m"]) > 0
    assert 0.0 <= float(features["pre_price_position_30m"]) <= 1.0
    assert features["feature_max_observed_at"] == candles[-2].at.isoformat()
    assert features["feature_leakage_flag"] is False


def test_share_pair_reversion_uses_only_prior_features() -> None:
    pair_reversion = _load_script(
        "research_share_pair_reversion_test",
        "research_share_pair_reversion.py",
    )
    start = datetime(2026, 7, 15, 7, 0, tzinfo=timezone.utc)
    candles = []
    for index in range(220):
        common_price = 110.0 if 120 <= index < 125 else 100.0
        at = start + timedelta(minutes=index)
        candles.extend(
            [
                lib.ResearchCandle("SBER", at, common_price, common_price, common_price, common_price, 100),
                lib.ResearchCandle("SBERP", at, 100, 100, 100, 100, 100),
            ]
        )

    rows = pair_reversion.build_pair_rows(
        candles,
        pairs=(("SBER", "SBERP"),),
        lookback_minutes=120,
    )

    five_minutes = next(row for row in rows if row["horizon_minutes"] == 5)
    assert five_minutes["feature_leakage_flag"] is False
    assert five_minutes["feature_max_observed_at"] < five_minutes["source_event_at"]
    assert five_minutes["success"] == 1


def test_external_market_context_never_uses_event_or_future_candle() -> None:
    event_at = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)
    context_rows = (
            lib.ResearchCandle("IMOEX", event_at - timedelta(minutes=5), 100, 100, 100, 100, 1),
            lib.ResearchCandle("IMOEX", event_at - timedelta(minutes=1), 101, 101, 101, 101, 1),
            lib.ResearchCandle("IMOEX", event_at, 500, 500, 500, 500, 1),
    )
    context = {
        ("IMOEX", date(2026, 7, 15)): (context_rows, tuple(row.at for row in context_rows))
    }

    features = lib._external_market_context_features(context, _event(event_at), (5,))

    assert float(features["context_imoex_return_bps_5m"]) == pytest.approx(100.0)
    assert features["context_rvi_return_bps_5m"] == ""


def _load_orderbook_script() -> object:
    pytest.importorskip("httpx")
    spec = importlib.util.spec_from_file_location(
        "research_collect_tinvest_orderbook_snapshots",
        ROOT / "scripts" / "research_collect_tinvest_orderbook_snapshots.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["research_collect_tinvest_orderbook_snapshots"] = module
    spec.loader.exec_module(module)
    return module


def _load_script(module_name: str, path: str) -> object:
    spec = importlib.util.spec_from_file_location(module_name, ROOT / "scripts" / path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


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


def test_native_signal_cache_normalization_redacts_api_identifiers() -> None:
    native = _load_script(
        "research_cache_tinvest_native_signals",
        "research_cache_tinvest_native_signals.py",
    )
    row = native.normalize_signal(
        {
            "signalId": "raw-signal-id",
            "strategyId": "raw-strategy-id",
            "strategyName": "Тестовая стратегия",
            "instrumentUid": "raw-instrument-uid",
            "createDt": "2026-07-18T07:00:00Z",
            "endDt": "2026-07-18T08:00:00Z",
            "closeDt": "2026-07-18T08:00:00Z",
            "direction": "SIGNAL_DIRECTION_BUY",
            "probability": 91,
            "initialPrice": {"units": "100", "nano": 0},
            "targetPrice": {"units": "101", "nano": 0},
            "stoploss": {"units": "99", "nano": 0},
            "closePrice": {"units": "101", "nano": 0},
        },
        instruments={
            "raw-instrument-uid": {
                "ticker": "SBER",
                "class_code": "TQBR",
                "instrument_type": "share",
                "instrument_name": "Сбер Банк",
            }
        },
        strategies={
            "raw-strategy-id": {
                "strategy_key": native.stable_key("raw-strategy-id"),
                "strategy_name": "Тестовая стратегия",
                "strategy_type": "STRATEGY_TYPE_TECHNICAL",
            }
        },
    )

    encoded = json.dumps(row, ensure_ascii=False)
    assert row["ticker"] == "SBER"
    assert row["probability"] == 91
    assert row["broker_signed_return_bps"] == pytest.approx(100.0)
    assert "raw-signal-id" not in encoded
    assert "raw-strategy-id" not in encoded
    assert "raw-instrument-uid" not in encoded
    assert "instrument_uid" not in row


def test_native_signal_features_are_as_of_event_time_only() -> None:
    event_at = datetime(2026, 7, 18, 7, 30, tzinfo=timezone.utc)
    signal = _event(event_at)
    indexed = lib._native_signals_by_ticker(
        [
            {
                "ticker": "SBER",
                "strategy_key": "active-buy",
                "strategy_type": "STRATEGY_TYPE_TECHNICAL",
                "create_at": "2026-07-18T07:00:00+00:00",
                "close_at": "2026-07-18T08:00:00+00:00",
                "direction": 1,
                "probability": 90,
            },
            {
                "ticker": "SBER",
                "strategy_key": "already-closed",
                "strategy_type": "STRATEGY_TYPE_FUNDAMENTAL",
                "create_at": "2026-07-18T06:00:00+00:00",
                "close_at": "2026-07-18T07:10:00+00:00",
                "direction": -1,
                "probability": 99,
            },
            {
                "ticker": "SBER",
                "strategy_key": "future-sell",
                "strategy_type": "STRATEGY_TYPE_TECHNICAL",
                "create_at": "2026-07-18T07:31:00+00:00",
                "close_at": "2026-07-18T09:00:00+00:00",
                "direction": -1,
                "probability": 100,
            },
        ]
    )

    features = lib._native_signal_features(signal, indexed)

    assert features["native_signal_available"] is True
    assert features["native_signal_active_count"] == 1
    assert features["native_signal_buy_count"] == 1
    assert features["native_signal_sell_count"] == 0
    assert features["native_signal_probability_max"] == "90.00000000"
    assert features["native_signal_consensus_direction"] == "buy"
    assert features["native_signal_detector_alignment"] == 1


def test_orderbook_manifest_redacts_secrets() -> None:
    payload = lib.build_orderbook_cache_manifest(
        tickers=("SBER",),
        start_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        end_at=datetime(2026, 7, 1, 1, tzinfo=timezone.utc),
        depth=10,
        row_counts={"SBER/2026-07-01": 3},
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


def test_orderbook_collection_progress_redacts_secrets(tmp_path: Path) -> None:
    orderbook = _load_orderbook_script()
    payload = orderbook._collection_progress_payload(
        status="running",
        cache_dir=tmp_path,
        tickers=("SBER", "GAZP"),
        depth=10,
        started_at=datetime(2026, 7, 17, 7, 5, tzinfo=timezone.utc),
        sample_index=10,
        samples=20,
        rows_collected=20,
        rows_flushed=10,
        unflushed_rows=10,
        failures=[],
        row_counts={"SBER/2026-07-17": 10},
    )

    assert payload["kind"] == "tinvest_research_orderbook_collection_progress"
    assert payload["progress"]["completed_share"] == 0.5
    assert payload["progress"]["rows_flushed"] == 10
    assert payload["progress"]["unflushed_rows"] == 10
    assert payload["privacy"] == {
        "account_identifiers_persisted": False,
        "instrument_uids_persisted": False,
        "tokens_persisted": False,
    }


def test_orderbook_snapshot_features_are_deterministic() -> None:
    snapshot = lib.orderbook_snapshot_from_levels(
        ticker="SBER",
        at=datetime(2026, 7, 15, 7, 5, tzinfo=timezone.utc),
        depth=2,
        bids=[
            {"price": {"units": "99", "nano": 900_000_000}, "quantity": "10"},
            {"price": {"units": "99", "nano": 800_000_000}, "quantity": "30"},
        ],
        asks=[
            {"price": {"units": "100", "nano": 100_000_000}, "quantity": "20"},
            {"price": {"units": "100", "nano": 200_000_000}, "quantity": "40"},
        ],
    )

    assert snapshot is not None
    assert snapshot.best_bid == 99.9
    assert snapshot.best_ask == 100.1
    assert snapshot.mid == 100.0
    assert round(snapshot.spread_bps, 6) == 20.0
    assert snapshot.bid_qty == 40.0
    assert snapshot.ask_qty == 60.0
    assert snapshot.imbalance_ratio == 0.4
    assert round(snapshot.imbalance_abs, 6) == 0.2


def test_false_positive_guard_finds_bad_context_without_product_claim() -> None:
    guards = _load_script(
        "research_mine_false_positive_guards",
        "research_mine_false_positive_guards.py",
    )
    rows: list[dict[str, object]] = []
    for index in range(6):
        rows.append(
            {
                "frontier_decision": "up",
                "frontier_success": 1,
                "frontier_confidence": 0.5,
                "frontier_result_bps": 12.0,
                "trading_day": f"2026-07-{index + 1:02d}",
                "ticker": "SBER",
                "liquidity_bucket": "normal",
                "horizon_seconds": 300,
            }
        )
    for index in range(4):
        rows.append(
            {
                "frontier_decision": "up",
                "frontier_success": 0,
                "frontier_confidence": 0.5,
                "frontier_result_bps": -8.0,
                "trading_day": f"2026-07-{index + 1:02d}",
                "ticker": "GAZP",
                "liquidity_bucket": "noisy",
                "horizon_seconds": 300,
            }
        )

    result = guards.mine_false_positive_guards(
        rows,
        thresholds=(0.4,),
        min_removed_rows=1,
        top_n=5,
    )

    noisy_guard = next(row for row in result if row["guard"] == "exclude(liquidity_bucket=noisy)")
    assert noisy_guard["kept_success_rate"] == 1.0
    assert noisy_guard["precision_gain"] > 0
    assert noisy_guard["accepted_shadow"] is False
    assert noisy_guard["product_claim_allowed"] is False
    assert noisy_guard["status"] == "too_small_after_exclusion"


def test_false_positive_guard_can_combine_exclusions() -> None:
    guards = _load_script(
        "research_mine_false_positive_guards_combo",
        "research_mine_false_positive_guards.py",
    )
    rows: list[dict[str, object]] = []
    for index in range(6):
        rows.append(
            {
                "frontier_decision": "up",
                "frontier_success": 1,
                "frontier_confidence": 0.5,
                "frontier_result_bps": 10.0,
                "trading_day": f"2026-07-{index + 1:02d}",
                "ticker": "SBER",
                "liquidity_bucket": "normal",
                "horizon_seconds": 300,
            }
        )
    for index in range(2):
        rows.append(
            {
                "frontier_decision": "up",
                "frontier_success": 0,
                "frontier_confidence": 0.5,
                "frontier_result_bps": -10.0,
                "trading_day": f"2026-07-{index + 1:02d}",
                "ticker": "GAZP",
                "liquidity_bucket": "noisy",
                "horizon_seconds": 300,
            }
        )
    for index in range(2):
        rows.append(
            {
                "frontier_decision": "up",
                "frontier_success": 0,
                "frontier_confidence": 0.5,
                "frontier_result_bps": -10.0,
                "trading_day": f"2026-07-{index + 3:02d}",
                "ticker": "LKOH",
                "liquidity_bucket": "normal",
                "horizon_seconds": 900,
            }
        )

    result = guards.mine_false_positive_guards(
        rows,
        thresholds=(0.4,),
        min_removed_rows=1,
        max_guard_terms=2,
        beam_width=10,
        top_n=10,
    )

    combo = next(row for row in result if " & " in row["guard"])
    assert combo["guard_terms"] == 2
    assert combo["kept_success_rate"] == 1.0
    assert "exclude(horizon_seconds=900)" in combo["guard"]
    assert "exclude(liquidity_bucket=noisy)" in combo["guard"]


def test_gap_audit_reports_missing_successes_and_blockers(tmp_path: Path) -> None:
    gap = _load_script("research_audit_90_gap", "research_audit_90_gap.py")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    with (run_dir / "confidence-threshold-report.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "threshold",
                "selected_rows",
                "sessions",
                "success_count",
                "success_rate",
                "wilson_lower_95",
                "mean_selected_result_bps",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "threshold": 0.4,
                "selected_rows": 10,
                "sessions": 3,
                "success_count": 7,
                "success_rate": 0.7,
                "wilson_lower_95": 0.4,
                "mean_selected_result_bps": 5.0,
            }
        )

    report = gap.build_gap_report(run_dir)

    assert report["status"] == "not_ready"
    assert report["summary"]["candidate_rows"] == 1
    row = report["rows"][0]
    assert row["missing_successes_to_90_current_rows"] == 2
    assert row["missing_rows_to_minimum"] == 290
    assert row["additional_successes_needed_at_min_rows"] == 263
    assert row["accepted_shadow"] is False
    assert "sample_size" in row["blockers"]
    assert "lower_bound" in row["blockers"]


def test_next_action_plan_prioritizes_microstructure_collection() -> None:
    planner = _load_script("research_plan_90_next_actions", "research_plan_90_next_actions.py")

    plan = planner.build_next_action_plan(
        gap_audit={
            "rows": [
                {
                    "source": "precision_scout",
                    "rule": "candidate-a",
                    "rows": 50,
                    "success_rate": 0.64,
                    "wilson_lower_95": 0.50,
                    "success_rate_gap_to_90": 0.26,
                    "blockers": "success_rate,lower_bound",
                    "next_action": "needs_new_features_not_more_thresholding",
                },
                {
                    "source": "precision_scout",
                    "rule": "candidate-b",
                    "rows": 300,
                    "success_rate": 0.4,
                    "wilson_lower_95": 0.35,
                    "success_rate_gap_to_90": 0.5,
                    "blockers": "cannot_reach_90_at_min_rows",
                    "next_action": "retire_or_redefine_rule",
                },
            ]
        },
        feature_coverage={
            "microstructure_value_coverage": {"ready": False},
        },
        live_status={"status": "waiting_for_start"},
    )

    assert plan["status"] == "waiting_for_microstructure"
    assert plan["microstructure_needed"] is True
    assert plan["next_actions"][0]["action"] == "collect_microstructure_holdout"
    assert plan["top_new_feature_candidates"][0]["rule"] == "candidate-a"
    assert plan["summary"]["action_counts"]["needs_new_features_not_more_thresholding"] == 1


def test_collection_watchdog_waits_before_start() -> None:
    watchdog = _load_script("research_collection_watchdog_wait", "research_collection_watchdog.py")

    report = watchdog.build_watchdog_report(
        live_status={
            "now_moscow": "2026-07-17T09:55:00+03:00",
            "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
            "launchd_loaded": True,
            "log_exists": False,
            "cache_files": {"files_updated_after_recommended_start": 0},
            "running_collectors": [],
        },
        schedule_status={"launchd_loaded": True},
    )

    assert report["status"] == "waiting_for_start"
    assert report["next_action"] == "wait"
    assert report["severity"] == "info"


def test_collection_watchdog_accepts_systemd_loaded_scheduler() -> None:
    watchdog = _load_script("research_collection_watchdog_systemd", "research_collection_watchdog.py")

    report = watchdog.build_watchdog_report(
        live_status={
            "now_moscow": "2026-07-17T09:55:00+03:00",
            "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
            "systemd_loaded": True,
            "scheduler_loaded": True,
            "log_exists": False,
            "cache_files": {"files_updated_after_recommended_start": 0},
            "running_collectors": [],
        },
        schedule_status={"systemd_loaded": True, "scheduler_loaded": True},
    )

    assert report["status"] == "waiting_for_start"
    assert report["scheduler_loaded"] is True
    assert report["systemd_loaded"] is True
    assert report["next_action"] == "wait"


def test_collection_watchdog_requests_scheduler_load_before_start() -> None:
    watchdog = _load_script("research_collection_watchdog_load_scheduler", "research_collection_watchdog.py")

    report = watchdog.build_watchdog_report(
        live_status={
            "now_moscow": "2026-07-17T09:55:00+03:00",
            "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
            "scheduler_loaded": False,
            "log_exists": False,
            "cache_files": {"files_updated_after_recommended_start": 0},
            "running_collectors": [],
        },
        schedule_status={"scheduler_loaded": False},
    )

    assert report["status"] == "scheduler_not_loaded"
    assert report["next_action"] == "load_scheduler"
    assert report["severity"] == "error"


def test_collection_watchdog_detects_missed_start() -> None:
    watchdog = _load_script("research_collection_watchdog_missed", "research_collection_watchdog.py")

    report = watchdog.build_watchdog_report(
        live_status={
            "now_moscow": "2026-07-17T10:20:00+03:00",
            "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
            "recommended_end_moscow": "2026-07-17T18:05:00+03:00",
            "launchd_loaded": True,
            "log_exists": False,
            "cache_files": {"files_updated_after_recommended_start": 0},
            "running_collectors": [],
        },
        schedule_status={
            "launchd_loaded": True,
            "shell_script": {"path": "/tmp/run-liquidity-collector.sh"},
        },
        grace_minutes=5,
    )

    assert report["status"] == "scheduled_start_missed"
    assert report["next_action"] == "run_recovery_command"
    assert report["severity"] == "error"
    assert report["recovery_command"] == "/tmp/run-liquidity-collector.sh"


def test_microstructure_progress_tracks_signal_and_session_gates() -> None:
    progress = _load_script("research_microstructure_progress", "research_microstructure_progress.py")

    report = progress.build_progress_report(
        coverage={"by_ticker_day": [{"ticker": "SBER"}]},
        readiness={
            "rows": [
                {
                    "ready": False,
                    "max_age_seconds": 30,
                    "coverage": 0.25,
                    "covered_signals": 75,
                    "min_covered_signals": 300,
                    "covered_sessions": 6,
                    "min_covered_sessions": 30,
                    "orderbook_snapshots": 1000,
                    "reason_codes": ["not_enough_orderbook_covered_signals"],
                }
            ]
        },
        live_status={"status": "collecting"},
        watchdog={"status": "collector_running", "next_action": "monitor_progress"},
    )

    assert report["status"] == "collect_more_microstructure"
    assert report["covered_signals"] == 75
    assert report["missing_signals"] == 225
    assert report["covered_sessions"] == 6
    assert report["missing_sessions"] == 24
    assert report["signal_progress"] == pytest.approx(0.25)


def test_orderbook_partition_write_deduplicates_existing_rows(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    orderbooks = _load_orderbook_script()
    first = {
        field: ""
        for field in lib.ORDERBOOK_CACHE_FIELDS
    } | {
        "ticker": "SBER",
        "at": "2026-07-15T07:05:00+00:00",
        "depth": 10,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "mid": 100.0,
        "spread_bps": 20.0,
        "bid_qty": 40.0,
        "ask_qty": 60.0,
        "total_qty": 100.0,
        "imbalance_ratio": 0.4,
        "imbalance_abs": 0.2,
        "is_consistent": True,
    }
    second = first | {"at": "2026-07-15T07:06:00+00:00", "bid_qty": 50.0}

    first_counts = orderbooks._write_partitions(tmp_path, [first])
    second_counts = orderbooks._write_partitions(tmp_path, [first, second])

    assert first_counts == {"SBER/2026-07-15": 1}
    assert second_counts == {"SBER/2026-07-15": 2}


def test_build_dataset_cli_accepts_ticker_filter() -> None:
    builder = _load_script("research_build_signal_price_dataset", "research_build_signal_price_dataset.py")

    args = builder.parse_args(["--tickers", "sber, gazp"])

    assert args.tickers == ("SBER", "GAZP")


def test_build_dataset_cli_accepts_only_orderbook_dates() -> None:
    builder = _load_script(
        "research_build_signal_price_dataset_only_orderbook_dates",
        "research_build_signal_price_dataset.py",
    )

    args = builder.parse_args(["--only-orderbook-dates"])

    assert args.only_orderbook_dates is True


def test_build_dataset_cli_accepts_required_orderbook_features() -> None:
    builder = _load_script(
        "research_build_signal_price_dataset_require_orderbook_features",
        "research_build_signal_price_dataset.py",
    )

    args = builder.parse_args(["--require-orderbook-features"])

    assert args.require_orderbook_features is True


def test_build_dataset_rejects_empty_orderbook_features() -> None:
    builder = _load_script(
        "research_build_signal_price_dataset_reject_empty_orderbook_features",
        "research_build_signal_price_dataset.py",
    )

    with pytest.raises(SystemExit) as exc:
        builder.validate_orderbook_feature_requirement(
            require_orderbook_features=True,
            orderbook_cache_dir=Path("orderbooks"),
            manifest={"quality": {"orderbook_feature_rows": 0}},
        )

    assert "No prior order-book feature rows" in str(exc.value)


def test_build_dataset_accepts_present_orderbook_features() -> None:
    builder = _load_script(
        "research_build_signal_price_dataset_accept_orderbook_features",
        "research_build_signal_price_dataset.py",
    )

    builder.validate_orderbook_feature_requirement(
        require_orderbook_features=True,
        orderbook_cache_dir=Path("orderbooks"),
        manifest={"quality": {"orderbook_feature_rows": 4}},
    )


def test_build_dataset_filters_candles_to_orderbook_dates() -> None:
    builder = _load_script(
        "research_build_signal_price_dataset_filter_dates",
        "research_build_signal_price_dataset.py",
    )
    kept = _candle(0, 100.0)
    dropped = lib.ResearchCandle(
        "SBER",
        kept.at + timedelta(days=1),
        open=101.0,
        high=101.0,
        low=101.0,
        close=101.0,
        volume=100.0,
    )
    snapshot = lib.ResearchOrderBookSnapshot(
        ticker="SBER",
        at=kept.at,
        depth=10,
        best_bid=99.0,
        best_ask=101.0,
        mid=100.0,
        spread_bps=200.0,
        bid_qty=10.0,
        ask_qty=10.0,
        total_qty=20.0,
        imbalance_ratio=0.5,
        imbalance_abs=0.0,
    )

    rows = builder.filter_candles_to_orderbook_dates((kept, dropped), (snapshot,))

    assert rows == (kept,)


def test_read_cache_filters_partitions_before_returning_rows(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    sber = _candle(0, 100.0)
    gazp = lib.ResearchCandle(
        "GAZP",
        sber.at,
        open=200.0,
        high=200.0,
        low=200.0,
        close=200.0,
        volume=100.0,
    )
    lib.write_table(
        tmp_path / "ticker=SBER" / "date=2026-07-15.parquet",
        lib.candle_rows_for_storage([sber]),
    )
    lib.write_table(
        tmp_path / "ticker=GAZP" / "date=2026-07-15.parquet",
        lib.candle_rows_for_storage([gazp]),
    )

    rows = lib.read_cache(tmp_path, tickers=("SBER",))

    assert {row.ticker for row in rows} == {"SBER"}


def test_candle_cache_cli_requires_start_and_end_together() -> None:
    pytest.importorskip("httpx")
    cache = _load_script("research_cache_tinvest_candles", "research_cache_tinvest_candles.py")

    args = cache.parse_args(["--env-file", ".env", "--start-day", "2026-07-15", "--end-day", "2026-07-16"])

    assert args.start_day == date(2026, 7, 15)
    assert args.end_day == date(2026, 7, 16)


def test_candle_cache_cli_accepts_refresh_days() -> None:
    pytest.importorskip("httpx")
    cache = _load_script("research_cache_tinvest_candles", "research_cache_tinvest_candles.py")

    args = cache.parse_args(
        [
            "--env-file",
            ".env",
            "--start-day",
            "2026-07-15",
            "--end-day",
            "2026-07-16",
            "--refresh-days",
            "2026-07-16",
            "--insecure-skip-tls-verify",
        ]
    )

    assert args.refresh_days == "2026-07-16"
    assert args.insecure_skip_tls_verify is True


def test_liquidity_holdout_update_reads_orderbook_dates(tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    update = _load_script("research_update_liquidity_holdout", "research_update_liquidity_holdout.py")
    rows = lib.orderbook_rows_for_storage(
        [
            lib.ResearchOrderBookSnapshot(
                ticker="SBER",
                at=datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc),
                depth=10,
                best_bid=100.0,
                best_ask=100.1,
                mid=100.05,
                spread_bps=10.0,
                bid_qty=100.0,
                ask_qty=120.0,
                total_qty=220.0,
                imbalance_ratio=-0.09,
                imbalance_abs=0.09,
            ),
            lib.ResearchOrderBookSnapshot(
                ticker="SBER",
                at=datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc),
                depth=10,
                best_bid=101.0,
                best_ask=101.1,
                mid=101.05,
                spread_bps=9.9,
                bid_qty=130.0,
                ask_qty=110.0,
                total_qty=240.0,
                imbalance_ratio=0.08,
                imbalance_abs=0.08,
            ),
        ]
    )
    lib.write_table(
        tmp_path / "ticker=SBER" / "date=2026-07-15.parquet",
        [rows[0]],
        fields=lib.ORDERBOOK_CACHE_FIELDS,
    )
    lib.write_table(
        tmp_path / "ticker=SBER" / "date=2026-07-16.parquet",
        [rows[1]],
        fields=lib.ORDERBOOK_CACHE_FIELDS,
    )

    dates = update.orderbook_ticker_dates(tmp_path, ("SBER",))

    assert dates == {"SBER": (date(2026, 7, 15), date(2026, 7, 16))}


def test_liquidity_holdout_update_refreshes_latest_orderbook_day(monkeypatch: pytest.MonkeyPatch) -> None:
    update = _load_script("research_update_liquidity_holdout", "research_update_liquidity_holdout.py")
    commands = []

    def fake_run(command: list[str]) -> dict[str, object]:
        commands.append(command)
        return {"status": "ok"}

    monkeypatch.setattr(update, "_run_json_command", fake_run)
    args = argparse.Namespace(
        env_file=Path(".env"),
        cache_dir=Path("candles"),
        tickers=("SBER", "GAZP"),
        request_timeout=30.0,
        request_attempts=3,
        request_interval=0.05,
        max_workers=2,
        ca_cert=None,
        insecure_skip_tls_verify=True,
        refresh_candle_days=frozenset({date(2026, 7, 15)}),
        refresh_latest_orderbook_day=True,
    )

    result = update._sync_candles(
        args,
        ticker_dates={"SBER": (date(2026, 7, 15), date(2026, 7, 16))},
    )

    command = commands[0]
    assert result["status"] == "ok"
    assert command[command.index("--start-day") + 1] == "2026-07-15"
    assert command[command.index("--end-day") + 1] == "2026-07-16"
    assert command[command.index("--refresh-days") + 1] == "2026-07-15,2026-07-16"
    assert "--insecure-skip-tls-verify" in command


def test_liquidity_holdout_update_builds_signal_triggered_command(monkeypatch: pytest.MonkeyPatch) -> None:
    update = _load_script("research_update_liquidity_holdout", "research_update_liquidity_holdout.py")
    commands = []

    def fake_run(command: list[str]) -> dict[str, object]:
        commands.append(command)
        return {"status": "ok", "rows_collected": 1}

    monkeypatch.setattr(update, "_run_json_command", fake_run)
    args = argparse.Namespace(
        env_file=Path(".env"),
        orderbook_cache_dir=Path("orderbooks"),
        signal_triggered_state_file=Path("state.json"),
        tickers=("SBER", "GAZP"),
        orderbook_depth=10,
        signal_triggered_polls=3,
        signal_triggered_interval_seconds=15.0,
        signal_triggered_max_signal_age_seconds=180,
        signal_triggered_target_day=date(2026, 7, 16),
        request_timeout=30.0,
        request_attempts=3,
        ca_cert=None,
        insecure_skip_tls_verify=True,
    )

    result = update._collect_signal_triggered_orderbook(args)

    command = commands[0]
    assert result["rows_collected"] == 1
    assert "research_collect_signal_triggered_orderbooks.py" in command[1]
    assert command[command.index("--tickers") + 1] == "SBER,GAZP"
    assert command[command.index("--target-day") + 1] == "2026-07-16"
    assert command[command.index("--polls") + 1] == "3"
    assert command[command.index("--max-signal-age-seconds") + 1] == "180"
    assert "--insecure-skip-tls-verify" in command


def test_signal_triggered_orderbook_selection_deduplicates_seen_signals() -> None:
    pytest.importorskip("httpx")
    triggered = _load_script(
        "research_collect_signal_triggered_orderbooks",
        "research_collect_signal_triggered_orderbooks.py",
    )
    now = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc)
    fresh = _event(now - timedelta(seconds=60))
    old = _event(now - timedelta(seconds=600))
    seen = {triggered.signal_key(fresh)}

    tickers, keys = triggered.select_fresh_signal_tickers(
        [fresh, old],
        seen_signal_keys=seen,
        now=now,
        max_signal_age_seconds=180,
    )

    assert tickers == set()
    assert keys == []


def test_signal_triggered_orderbook_selection_returns_fresh_ticker() -> None:
    pytest.importorskip("httpx")
    triggered = _load_script(
        "research_collect_signal_triggered_orderbooks",
        "research_collect_signal_triggered_orderbooks.py",
    )
    now = datetime(2026, 7, 16, 15, 0, tzinfo=timezone.utc)
    fresh = _event(now - timedelta(seconds=60))

    tickers, keys = triggered.select_fresh_signal_tickers(
        [fresh],
        seen_signal_keys=set(),
        now=now,
        max_signal_age_seconds=180,
    )

    assert tickers == {"SBER"}
    assert keys == [triggered.signal_key(fresh)]


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
    assert float(features["pre_directional_return_bps_5m"]) == float(features["pre_return_bps_5m"])
    assert "event_to_pre_volatility_5m" in features
    assert "pre_consolidation_score_5m" in features


def test_market_context_features_never_include_future_candles() -> None:
    signal = _event(datetime(2026, 7, 15, 7, 10, tzinfo=timezone.utc))
    sber_rows = [
        lib.ResearchCandle("SBER", signal.source_event_at - timedelta(minutes=5), 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", signal.source_event_at - timedelta(minutes=1), 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", signal.source_event_at, 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", signal.source_event_at + timedelta(minutes=1), 200, 200, 200, 200, 100),
    ]
    gazp_rows = [
        lib.ResearchCandle("GAZP", signal.source_event_at - timedelta(minutes=5), 100, 100, 100, 100, 100),
        lib.ResearchCandle("GAZP", signal.source_event_at - timedelta(minutes=1), 101, 101, 101, 101, 100),
        lib.ResearchCandle("GAZP", signal.source_event_at, 101, 101, 101, 101, 100),
        lib.ResearchCandle("GAZP", signal.source_event_at + timedelta(minutes=1), 500, 500, 500, 500, 100),
    ]
    by_ticker_day = {
        ("SBER", signal.trading_day): sber_rows,
        ("GAZP", signal.trading_day): gazp_rows,
    }

    features = lib._market_context_features(by_ticker_day, signal, (5,))

    assert round(float(features["market_return_bps_5m"]), 6) == 100.0
    assert round(float(features["signal_vs_market_bps_5m"]), 6) == -100.0
    assert round(float(features["signal_directional_vs_market_bps_5m"]), 6) == -100.0
    assert round(float(features["signal_market_alignment_bps_5m"]), 6) == 100.0


def test_market_context_index_can_be_limited_to_signal_times() -> None:
    signal = _event(datetime(2026, 7, 15, 7, 10, tzinfo=timezone.utc))
    extra_at = signal.source_event_at + timedelta(minutes=1)
    sber_rows = [
        lib.ResearchCandle("SBER", signal.source_event_at - timedelta(minutes=5), 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", signal.source_event_at - timedelta(minutes=1), 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", signal.source_event_at, 100, 100, 100, 100, 100),
        lib.ResearchCandle("SBER", extra_at, 120, 120, 120, 120, 100),
    ]
    gazp_rows = [
        lib.ResearchCandle("GAZP", signal.source_event_at - timedelta(minutes=5), 100, 100, 100, 100, 100),
        lib.ResearchCandle("GAZP", signal.source_event_at - timedelta(minutes=1), 101, 101, 101, 101, 100),
        lib.ResearchCandle("GAZP", signal.source_event_at, 101, 101, 101, 101, 100),
        lib.ResearchCandle("GAZP", extra_at, 500, 500, 500, 500, 100),
    ]
    by_ticker_day = {
        ("SBER", signal.trading_day): sber_rows,
        ("GAZP", signal.trading_day): gazp_rows,
    }

    index = lib._market_context_index(
        by_ticker_day,
        (5,),
        target_keys=((signal.ticker, signal.trading_day, signal.source_event_at),),
    )

    assert set(index) == {(signal.ticker, signal.trading_day, signal.source_event_at)}
    features = lib._apply_signal_direction_to_market_context(
        lib._market_context_from_index(index, signal, (5,)),
        signal,
        (5,),
    )
    assert round(float(features["market_return_bps_5m"]), 6) == 100.0


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
    assert "ticker_volume_quantile" in rows[0]
    assert "market_return_bps_5m" in rows[0]
    assert "signal_vs_market_bps_5m" in rows[0]


def test_dataset_builder_uses_only_prior_orderbook_snapshot() -> None:
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
    signals = lib.replay_signals(candles, max_signals_per_instrument=100)
    assert signals
    signal = signals[0]
    prior = lib.ResearchOrderBookSnapshot(
        ticker=signal.ticker,
        at=signal.source_event_at - timedelta(seconds=10),
        depth=10,
        best_bid=99.95,
        best_ask=100.05,
        mid=100.0,
        spread_bps=10.0,
        bid_qty=100.0,
        ask_qty=80.0,
        total_qty=180.0,
        imbalance_ratio=100 / 180,
        imbalance_abs=abs(100 / 180 - 0.5) * 2,
    )
    future = lib.ResearchOrderBookSnapshot(
        ticker=signal.ticker,
        at=signal.source_event_at + timedelta(seconds=1),
        depth=10,
        best_bid=99.0,
        best_ask=101.0,
        mid=100.0,
        spread_bps=200.0,
        bid_qty=1.0,
        ask_qty=999.0,
        total_qty=1000.0,
        imbalance_ratio=0.001,
        imbalance_abs=0.998,
    )

    rows, manifest = lib.build_signal_price_dataset(
        candles,
        horizons_seconds=(60,),
        lookback_windows=(5, 15, 30, 60),
        max_signals_per_instrument=100,
        orderbook_snapshots=(future, prior),
        orderbook_max_age_seconds=30,
    )
    matched = [
        row
        for row in rows
        if row["ticker"] == signal.ticker
        and row["source_event_at"] == signal.source_event_at.isoformat()
        and row["signal_type"] == signal.signal_type
    ]

    assert matched
    assert manifest["quality"]["orderbook_feature_rows"] > 0
    assert matched[0]["orderbook_available"] is True
    assert float(matched[0]["orderbook_age_seconds"]) == 10.0
    assert float(matched[0]["orderbook_spread_bps"]) == 10.0
    assert float(matched[0]["orderbook_spread_bps"]) != 200.0


def test_orderbook_signal_coverage_uses_age_and_no_future_snapshot() -> None:
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
    signal = lib.replay_signals(candles, max_signals_per_instrument=100)[0]
    prior = lib.ResearchOrderBookSnapshot(
        ticker=signal.ticker,
        at=signal.source_event_at - timedelta(seconds=20),
        depth=10,
        best_bid=99.95,
        best_ask=100.05,
        mid=100.0,
        spread_bps=10.0,
        bid_qty=100.0,
        ask_qty=80.0,
        total_qty=180.0,
        imbalance_ratio=100 / 180,
        imbalance_abs=abs(100 / 180 - 0.5) * 2,
    )
    future = prior.__class__(
        ticker=signal.ticker,
        at=signal.source_event_at + timedelta(seconds=1),
        depth=10,
        best_bid=99.0,
        best_ask=101.0,
        mid=100.0,
        spread_bps=200.0,
        bid_qty=1.0,
        ask_qty=999.0,
        total_qty=1000.0,
        imbalance_ratio=0.001,
        imbalance_abs=0.998,
    )

    rows = lib.orderbook_signal_coverage_summary(
        candles,
        (future, prior),
        max_age_seconds_options=(5, 30),
        max_signals_per_instrument=100,
    )

    by_age = {row["max_age_seconds"]: row for row in rows}
    assert by_age[5]["covered_signals"] == 0
    assert by_age[30]["covered_signals"] >= 1
    assert by_age[30]["covered_by_type"][signal.signal_type] >= 1
    assert by_age[30]["first_signal_at"]
    assert by_age[30]["last_signal_at"]
    assert by_age[30]["first_orderbook_at"]
    assert by_age[30]["last_orderbook_at"]
    assert float(by_age[30]["nearest_prior_orderbook_age_seconds"]) <= 30
    assert float(by_age[30]["nearest_signal_orderbook_gap_seconds"]) <= 30


def test_orderbook_coverage_by_ticker_day_marks_missing_days() -> None:
    candles = []
    for day_offset in range(2):
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
            base_at = datetime(2026, 7, 15 + day_offset, 7, 5, tzinfo=timezone.utc)
            candles.append(
                lib.ResearchCandle(
                    ticker="SBER",
                    at=base_at + timedelta(minutes=index),
                    open=price,
                    high=price * 1.0005,
                    low=price * 0.9995,
                    close=price,
                    volume=volume,
                    complete=True,
                )
            )
    signals = lib.replay_signals(candles, max_signals_per_instrument=100)
    assert signals
    covered_signal = next(signal for signal in signals if signal.trading_day == date(2026, 7, 15))
    prior = lib.ResearchOrderBookSnapshot(
        ticker="SBER",
        at=covered_signal.source_event_at - timedelta(seconds=10),
        depth=10,
        best_bid=99.95,
        best_ask=100.05,
        mid=100.0,
        spread_bps=10.0,
        bid_qty=100.0,
        ask_qty=80.0,
        total_qty=180.0,
        imbalance_ratio=100 / 180,
        imbalance_abs=abs(100 / 180 - 0.5) * 2,
    )
    future = prior.__class__(
        ticker="SBER",
        at=covered_signal.source_event_at + timedelta(seconds=1),
        depth=10,
        best_bid=99.0,
        best_ask=101.0,
        mid=100.0,
        spread_bps=200.0,
        bid_qty=1.0,
        ask_qty=999.0,
        total_qty=1000.0,
        imbalance_ratio=0.001,
        imbalance_abs=0.998,
    )

    rows = lib.orderbook_signal_coverage_by_ticker_day(
        candles,
        (future, prior),
        max_age_seconds=30,
        max_signals_per_instrument=100,
    )

    by_day = {row["trading_day"]: row for row in rows}
    assert by_day["2026-07-15"]["covered_signals"] >= 1
    assert float(by_day["2026-07-15"]["min_prior_age_seconds"]) == 10.0
    assert by_day["2026-07-16"]["covered_signals"] == 0
    assert by_day["2026-07-16"]["status"] == "missing"


def test_holdout_readiness_rejects_sparse_orderbook_coverage() -> None:
    rows = lib.holdout_readiness_summary(
        [
            {
                "max_age_seconds": 30,
                "signals": 1000,
                "covered_signals": 20,
                "coverage": 0.02,
                "sessions": 40,
                "covered_sessions": 3,
                "orderbook_snapshots": 25,
            }
        ],
        min_covered_signals=300,
        min_covered_sessions=30,
        min_coverage=0.8,
        preferred_max_age_seconds=30,
    )

    assert rows[0]["ready"] is False
    assert rows[0]["coverage_target_signals"] == 800
    assert rows[0]["required_covered_signals"] == 800
    assert rows[0]["missing_covered_signals"] == 780
    assert rows[0]["missing_covered_sessions"] == 27
    assert set(rows[0]["reason_codes"]) == {
        "not_enough_orderbook_covered_signals",
        "not_enough_orderbook_covered_sessions",
        "orderbook_coverage_too_sparse",
    }


def test_holdout_readiness_accepts_dense_orderbook_coverage() -> None:
    rows = lib.holdout_readiness_summary(
        [
            {
                "max_age_seconds": 30,
                "signals": 1000,
                "covered_signals": 850,
                "coverage": 0.85,
                "sessions": 40,
                "covered_sessions": 35,
                "orderbook_snapshots": 10_000,
            }
        ],
        min_covered_signals=300,
        min_covered_sessions=30,
        min_coverage=0.8,
        preferred_max_age_seconds=30,
    )

    assert rows[0]["ready"] is True
    assert rows[0]["missing_covered_signals"] == 0
    assert rows[0]["missing_covered_sessions"] == 0
    assert rows[0]["reason_codes"] == []


def test_liquidity_pipeline_selects_smallest_ready_window() -> None:
    pipeline = _load_script("research_run_liquidity_holdout", "research_run_liquidity_holdout.py")

    selected = pipeline.choose_ready_window(
        [
            {"max_age_seconds": 60, "ready": True},
            {"max_age_seconds": 15, "ready": False},
            {"max_age_seconds": 30, "ready": True},
        ]
    )

    assert selected == 30


def test_liquidity_pipeline_returns_no_window_when_holdout_not_ready() -> None:
    pipeline = _load_script("research_run_liquidity_holdout", "research_run_liquidity_holdout.py")

    selected = pipeline.choose_ready_window(
        [
            {"max_age_seconds": 30, "ready": False},
            {"max_age_seconds": 60, "ready": False},
        ]
    )

    assert selected is None


def test_liquidity_collection_plan_reports_missing_evidence(tmp_path: Path) -> None:
    planner = _load_script(
        "research_plan_liquidity_collection",
        "research_plan_liquidity_collection.py",
    )
    readiness = tmp_path / "readiness.json"
    readiness.write_text(
        json.dumps(
            {
                "ready": False,
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "coverage": 0.10,
                        "covered_signals": 20,
                        "covered_sessions": 3,
                        "missing_covered_signals": 280,
                        "missing_covered_sessions": 27,
                    },
                    {
                        "max_age_seconds": 3600,
                        "coverage": 0.50,
                        "covered_signals": 100,
                        "covered_sessions": 4,
                        "missing_covered_signals": 200,
                        "missing_covered_sessions": 26,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    plan = planner.build_collection_plan(
        readiness_path=readiness,
        tickers=("SBER", "GAZP"),
        preferred_max_age_seconds=30,
        target_calendar_days=45,
        target_continuous_hours=8,
        orderbook_interval_seconds=15,
        signal_triggered_interval_seconds=15,
        output_dir=tmp_path / "holdout",
        schedule_dir=tmp_path / "collection_plan",
        ca_cert=tmp_path / "russiantrustedca2024.pem",
        now=datetime(2026, 7, 16, 9, 30, tzinfo=planner.MOSCOW),
    )

    assert plan["status"] == "collect_more_data"
    assert plan["best_window"]["max_age_seconds"] == 30
    assert plan["missing_covered_signals"] == 280
    assert plan["missing_covered_sessions"] == 27
    assert plan["observed_covered_signals_per_session"] == 20 / 3
    assert plan["estimated_sessions_for_missing_signals"] == 42
    assert plan["recommended_additional_market_sessions"] >= 40
    assert plan["prior_feature_collection_mode"] == "continuous_orderbook_sampling"
    assert plan["continuous_orderbook_samples"] == 1920
    assert plan["collection_window_preflight"]["required_samples"] == 1920
    assert plan["schedule"]["weekday_start_local"] == "10:05"
    assert plan["schedule"]["cron_line"].startswith("5 10 * * 1-5")
    assert plan["schedule"]["systemd_timer"].endswith(".timer")
    assert "systemctl --user enable --now" in plan["schedule"]["systemd_install_user_command"]
    assert "research_update_liquidity_holdout.py" in plan["schedule"]["run_line"]
    assert "research_refresh_90_reports.py" in plan["schedule"]["run_line"]
    assert len(plan["post_collection_commands"]) == 1
    assert "--orderbook-samples" in plan["recommended_command"]
    samples_index = plan["recommended_command"].index("--orderbook-samples") + 1
    assert plan["recommended_command"][samples_index] == "1920"
    assert "--orderbook-flush-every-samples" in plan["recommended_command"]
    flush_index = plan["recommended_command"].index("--orderbook-flush-every-samples") + 1
    assert plan["recommended_command"][flush_index] == "20"
    assert "--require-full-prior-window" in plan["recommended_command"]
    assert "--collect-signal-triggered-orderbook" in plan["recommended_command"]
    assert "--ca-cert" in plan["recommended_command"]
    ca_cert_index = plan["recommended_command"].index("--ca-cert") + 1
    assert plan["recommended_command"][ca_cert_index].endswith("russiantrustedca2024.pem")
    report = tmp_path / "collection-plan.md"
    planner.write_report(report, plan)
    report_text = report.read_text(encoding="utf-8")
    assert "+  run" not in report_text
    assert "uv \\" in report_text
    assert "Estimated sessions for missing signals: 42" in report_text
    assert "Continuous order-book samples per run: 1920" in report_text
    assert "Collection preflight:" in report_text
    assert "Schedule artifacts" in report_text
    written = planner.write_schedule_files(plan["schedule"])
    assert Path(written["shell_script"]).exists()
    assert Path(written["systemd_service"]).exists()
    assert Path(written["systemd_timer"]).exists()
    shell_text = Path(written["shell_script"]).read_text(encoding="utf-8")
    assert "research_refresh_90_reports.py" in shell_text
    assert "--fallback-run-dir" in shell_text
    assert "--orderbook-flush-every-samples" in shell_text
    assert "--ca-cert" in shell_text
    assert "russiantrustedca2024.pem" in Path(written["launchd_plist"]).read_text(encoding="utf-8")
    assert "ExecStart=" in Path(written["systemd_service"]).read_text(encoding="utf-8")
    assert "OnCalendar=Mon..Fri" in Path(written["systemd_timer"]).read_text(encoding="utf-8")
    assert "Europe/Moscow" in Path(written["systemd_timer"]).read_text(encoding="utf-8")
    assert Path(written["cron_file"]).read_text(encoding="utf-8").startswith("5 10 * * 1-5")
    launchd_payload = plistlib.loads(Path(written["launchd_plist"]).read_bytes())
    assert [item["Weekday"] for item in launchd_payload["StartCalendarInterval"]] == [1, 2, 3, 4, 5]
    assert all(item["Hour"] == 10 and item["Minute"] == 5 for item in launchd_payload["StartCalendarInterval"])


def test_continuous_samples_for_hours_rounds_up() -> None:
    planner = _load_script(
        "research_plan_liquidity_collection_samples",
        "research_plan_liquidity_collection.py",
    )

    assert planner.continuous_samples_for_hours(target_hours=1, interval_seconds=15) == 240
    assert planner.continuous_samples_for_hours(target_hours=8, interval_seconds=15) == 1920
    assert planner.continuous_samples_for_hours(target_hours=0.01, interval_seconds=60) == 1


def test_liquidity_collection_plan_recurring_schedule_starts_at_session_open(tmp_path: Path) -> None:
    planner = _load_script(
        "research_plan_liquidity_collection_ready_now",
        "research_plan_liquidity_collection.py",
    )
    readiness = tmp_path / "readiness.json"
    readiness.write_text(
        json.dumps(
            {
                "ready": False,
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "coverage": 0.0,
                        "covered_signals": 0,
                        "covered_sessions": 0,
                        "missing_covered_signals": 300,
                        "missing_covered_sessions": 30,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    plan = planner.build_collection_plan(
        readiness_path=readiness,
        tickers=("SBER",),
        target_continuous_hours=8,
        orderbook_interval_seconds=15,
        signal_triggered_interval_seconds=15,
        output_dir=tmp_path / "holdout",
        schedule_dir=tmp_path / "collection_plan",
        now=datetime(2026, 7, 16, 10, 16, tzinfo=planner.MOSCOW),
    )

    assert plan["collection_window_preflight"]["status"] == "ready_now"
    assert plan["collection_window_preflight"]["recommended_start_moscow"].startswith("2026-07-16T10:16:00")
    assert plan["schedule"]["weekday_start_local"] == "10:05"
    assert plan["schedule"]["scheduled_start_moscow"].startswith("2026-07-16T10:05:00")


def test_collection_window_preflight_rejects_late_session_start() -> None:
    planner = _load_script(
        "research_plan_liquidity_collection_preflight",
        "research_plan_liquidity_collection.py",
    )

    early = planner.collection_window_preflight(
        now=datetime(2026, 7, 16, 10, 10, tzinfo=planner.MOSCOW),
        target_hours=8,
        interval_seconds=15,
    )
    late = planner.collection_window_preflight(
        now=datetime(2026, 7, 16, 14, 0, tzinfo=planner.MOSCOW),
        target_hours=8,
        interval_seconds=15,
    )
    closed = planner.collection_window_preflight(
        now=datetime(2026, 7, 16, 20, 0, tzinfo=planner.MOSCOW),
        target_hours=8,
        interval_seconds=15,
    )

    assert early["status"] == "ready_now"
    assert early["can_complete_full_window_today"] is True
    assert late["status"] == "insufficient_remaining_session"
    assert late["can_complete_full_window_today"] is False
    assert closed["status"] == "outside_research_session"
    assert closed["reason_code"] == "schedule_next_full_prior_window"
    assert closed["recommended_start_moscow"].startswith("2026-07-17T10:05:00")

    friday_closed = planner.collection_window_preflight(
        now=datetime(2026, 7, 17, 20, 0, tzinfo=planner.MOSCOW),
        target_hours=8,
        interval_seconds=15,
    )

    assert friday_closed["recommended_start_moscow"].startswith("2026-07-20T10:05:00")


def test_liquidity_update_writes_collection_plan_from_holdout_readiness(tmp_path: Path) -> None:
    updater = _load_script(
        "research_update_liquidity_holdout",
        "research_update_liquidity_holdout.py",
    )
    readiness_dir = tmp_path / "readiness"
    readiness_dir.mkdir()
    (readiness_dir / "readiness.json").write_text(
        json.dumps(
            {
                "ready": False,
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "coverage": 0.0,
                        "covered_signals": 0,
                        "covered_sessions": 0,
                        "missing_covered_signals": 300,
                        "missing_covered_sessions": 30,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        output_dir=tmp_path,
        tickers=("SBER",),
        preferred_max_age_seconds=30,
        collection_plan_target_calendar_days=45,
        collection_plan_target_continuous_hours=8,
        orderbook_interval_seconds=15.0,
        signal_triggered_interval_seconds=15.0,
    )

    result = updater._write_collection_plan(args, {"readiness": {"output_dir": str(readiness_dir)}})

    assert result["status"] == "collect_more_data"
    assert result["missing_covered_signals"] == 300
    assert (tmp_path / "collection_plan" / "collection-plan.json").exists()
    assert (tmp_path / "collection_plan" / "collection-plan.md").exists()
    assert (tmp_path / "collection_plan" / "run-liquidity-collector.sh").exists()
    assert (tmp_path / "collection_plan" / "liquidity-collector.cron").exists()
    assert (tmp_path / "collection_plan" / "com.investment-signals.research-liquidity-collector.plist").exists()
    assert (tmp_path / "collection_plan" / "investment-signals-research-liquidity-collector.service").exists()
    assert (tmp_path / "collection_plan" / "investment-signals-research-liquidity-collector.timer").exists()


def test_liquidity_update_preflight_blocks_late_full_window(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    updater = _load_script(
        "research_update_liquidity_holdout_preflight",
        "research_update_liquidity_holdout.py",
    )
    monkeypatch.setattr(
        updater,
        "_collection_preflight",
        lambda args: {
            "status": "outside_research_session",
            "can_complete_full_window_today": False,
            "reason_code": "not_enough_time_for_full_prior_window",
        },
    )
    monkeypatch.setattr(
        updater,
        "_collect_orderbook",
        lambda args: (_ for _ in ()).throw(AssertionError("collector must not run")),
    )
    args = argparse.Namespace(
        output_dir=tmp_path,
        collection_plan_target_continuous_hours=8,
        orderbook_interval_seconds=15,
        preflight_only=False,
        require_full_prior_window=True,
        force=False,
    )

    result = updater.run_update(args)

    assert result["status"] == "preflight_blocked"
    assert result["reason_code"] == "not_enough_time_for_full_prior_window"
    assert (tmp_path / "liquidity-update-result.json").exists()


def test_signal_90_status_stays_not_ready_without_policy_and_liquidity(tmp_path: Path) -> None:
    status_module = _load_script(
        "research_signal_90_status",
        "research_signal_90_status.py",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "run-1", "dataset_rows": 1000, "validation_sessions": 30}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "disabled", "default_action": "skip", "product_claim_allowed": False}),
        encoding="utf-8",
    )
    (run_dir / "confidence-threshold-report.csv").write_text(
        "\n".join(
            [
                "threshold,selected_rows,success_count,success_rate,wilson_lower_95,mean_selected_result_bps,accepted_research",
                "0.90,20,18,0.90,0.70,10.0,False",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "confidence-reliability-report.csv").write_text(
        "\n".join(
            [
                "scope,rule,nominal_action,selected_rows,sessions,success_count,observed_success_rate,wilson_lower_95,mean_model_confidence,mean_result_bps,shadow_allowed,product_90_allowed,safe_runtime_action",
                "confidence_band,strong_signal,candidate,20,5,18,0.90,0.70,0.94,10.0,False,False,skip",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "candidate-watchlist.csv").write_text(
        "candidate_id,scope,rule,selected_rows,status\n",
        encoding="utf-8",
    )
    (run_dir / "directional-state-candidates.csv").write_text(
        "\n".join(
            [
                "rule,evaluation_rows,evaluation_success_rate,evaluation_wilson_lower_95,evaluation_mean_result_bps,accepted_shadow,blocking_reasons",
                "decision=down,20,0.90,0.70,10.0,False,sample_size",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "selective-rule-candidates.csv").write_text(
        "rule,evaluation_rows,evaluation_success_rate,evaluation_wilson_lower_95,evaluation_mean_result_bps,accepted_shadow,blocking_reasons\n",
        encoding="utf-8",
    )
    (run_dir / "precision-scout-candidates.csv").write_text(
        "\n".join(
            [
                "rule,status,discovery_gate_passed,dominant_decision,dominant_relation,evaluation_rows,evaluation_sessions,evaluation_success_rate,evaluation_wilson_lower_95,evaluation_mean_result_bps,accepted_shadow,blocking_reasons",
                "high hit-rate but negative,discovery_weak,False,down,inverse,20,10,0.95,0.75,-5.0,False,positive_result",
                "lower hit-rate but positive,watch_only,True,down,neutral,30,12,0.70,0.55,8.0,False,sample_size",
            ]
        ),
        encoding="utf-8",
    )
    plan_path = tmp_path / "collection-plan.json"
    plan_path.write_text(
        json.dumps(
                {
                    "status": "collect_more_data",
                    "missing_covered_signals": 300,
                    "missing_covered_sessions": 30,
                    "recommended_additional_market_sessions": 45,
                    "collection_window_preflight": {
                        "status": "outside_research_session",
                        "reason_code": "schedule_next_full_prior_window",
                        "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                        "recommended_end_moscow": "2026-07-17T18:05:00+03:00",
                        "latest_full_start_moscow": "2026-07-16T10:39:00+03:00",
                        "can_complete_full_window_today": False,
                    },
                    "recommended_command": ["uv", "run", "--extra", "research", "python", "collector.py"],
                }
            ),
        encoding="utf-8",
    )

    status = status_module.build_signal_90_status(run_dir=run_dir, collection_plan_path=plan_path)

    assert status["status"] == "not_ready"
    assert status["product_claim_allowed"] is False
    assert "liquidity_holdout_not_ready" in status["missing_reasons"]
    assert "no_calibrated_90_confidence_band" in status["missing_reasons"]
    assert "no_microstructure_validation_rows" in status["missing_reasons"]
    assert "microstructure_validation_not_ready" in status["missing_reasons"]
    assert status["microstructure"]["usable_rows"] == 0
    assert status["microstructure"]["required_usable_rows"] == 300
    assert status["microstructure"]["missing_usable_rows"] == 300
    assert status["microstructure"]["missing_usable_sessions"] == 30
    assert status["liquidity"]["recommended_command"][-1] == "collector.py"
    assert status["liquidity"]["collection_window_preflight"]["recommended_start_moscow"] == "2026-07-17T10:05:00+03:00"
    assert status["best_threshold"]["selected_rows"] == "20"
    assert status["best_reliability_band"]["safe_runtime_action"] == "skip"
    assert status["best_precision_scout"]["rule"] == "lower hit-rate but positive"
    report = tmp_path / "signal-90-status.md"
    status_module.write_report(report, status)
    report_text = report.read_text(encoding="utf-8")
    assert "not_ready" in report_text
    assert "Не хватает пригодных строк: 300" in report_text
    assert "Следующий рекомендуемый старт: 2026-07-17T10:05:00+03:00" in report_text
    assert "uv run --extra research python collector.py" in report_text


def test_microstructure_daily_summary_flags_late_collection_window(tmp_path: Path) -> None:
    summary_module = _load_script(
        "research_microstructure_daily_summary",
        "research_microstructure_daily_summary.py",
    )
    coverage = tmp_path / "coverage.json"
    readiness = tmp_path / "readiness.json"
    signal_status = tmp_path / "signal-90-status.json"
    collection_plan = tmp_path / "collection-plan.json"
    coverage.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "signals": 141,
                        "covered_signals": 0,
                        "coverage": 0.0,
                        "covered_sessions": 0,
                        "orderbook_snapshots": 102,
                        "nearest_prior_orderbook_age_seconds": "551.2",
                    }
                ],
                "by_ticker_day": [
                    {
                        "ticker": "SBER",
                        "trading_day": "2026-07-16",
                        "signals": 26,
                        "covered_signals": 0,
                        "coverage": 0.0,
                        "first_signal_at": "2026-07-16T07:37:00+00:00",
                        "first_orderbook_at": "2026-07-16T14:24:36+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    readiness.write_text(
        json.dumps(
            {
                "ready": False,
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "covered_signals": 0,
                        "covered_sessions": 0,
                        "missing_covered_signals": 300,
                        "missing_covered_sessions": 30,
                        "required_covered_signals": 300,
                        "min_covered_sessions": 30,
                        "coverage": 0.0,
                        "reason_codes": ["not_enough_orderbook_covered_signals"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    signal_status.write_text(
        json.dumps(
            {
                "status": "not_ready",
                "product_claim_allowed": False,
                "missing_reasons": ["liquidity_holdout_not_ready"],
            }
        ),
        encoding="utf-8",
    )
    collection_plan.write_text(
        json.dumps(
            {
                "preferred_max_age_seconds": 30,
                "collection_window_preflight": {
                    "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                },
            }
        ),
        encoding="utf-8",
    )

    summary = summary_module.build_daily_summary(
        coverage_path=coverage,
        readiness_path=readiness,
        signal_status_path=signal_status,
        collection_plan_path=collection_plan,
    )

    assert summary["status"] == "collect_more_data"
    assert summary["next_action"] == "fix_collection_window_before_collecting_more"
    assert summary["missing_covered_signals"] == 300
    assert summary["worst_ticker_days"][0]["ticker"] == "SBER"
    report = tmp_path / "daily-summary.md"
    summary_module.write_report(report, summary)
    report_text = report.read_text(encoding="utf-8")
    assert "Дневной отчёт по стакану" in report_text
    assert "fix_collection_window_before_collecting_more" in report_text


def test_microstructure_daily_summary_marks_ready_holdout(tmp_path: Path) -> None:
    summary_module = _load_script(
        "research_microstructure_daily_summary_ready",
        "research_microstructure_daily_summary.py",
    )
    coverage = tmp_path / "coverage.json"
    readiness = tmp_path / "readiness.json"
    signal_status = tmp_path / "signal-90-status.json"
    collection_plan = tmp_path / "collection-plan.json"
    coverage.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "signals": 400,
                        "covered_signals": 340,
                        "coverage": 0.85,
                        "covered_sessions": 31,
                        "orderbook_snapshots": 10000,
                    }
                ],
                "by_ticker_day": [],
            }
        ),
        encoding="utf-8",
    )
    readiness.write_text(
        json.dumps(
            {
                "ready": True,
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "covered_signals": 340,
                        "covered_sessions": 31,
                        "missing_covered_signals": 0,
                        "missing_covered_sessions": 0,
                        "required_covered_signals": 300,
                        "min_covered_sessions": 30,
                        "coverage": 0.85,
                        "ready": True,
                        "reason_codes": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    signal_status.write_text(
        json.dumps({"status": "not_ready", "product_claim_allowed": False, "missing_reasons": []}),
        encoding="utf-8",
    )
    collection_plan.write_text(json.dumps({"preferred_max_age_seconds": 30}), encoding="utf-8")

    summary = summary_module.build_daily_summary(
        coverage_path=coverage,
        readiness_path=readiness,
        signal_status_path=signal_status,
        collection_plan_path=collection_plan,
    )

    assert summary["ready"] is True
    assert summary["status"] == "ready_for_liquidity_research"
    assert summary["next_action"] == "run_liquidity_aware_research"


def test_microstructure_collection_loop_stops_when_status_is_ready(tmp_path: Path) -> None:
    loop = _load_script(
        "research_collect_until_microstructure_ready",
        "research_collect_until_microstructure_ready.py",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "run-1", "dataset_rows": 1000, "validation_sessions": 30}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "disabled", "default_action": "skip", "product_claim_allowed": False}),
        encoding="utf-8",
    )
    (run_dir / "confidence-threshold-report.csv").write_text(
        "threshold,selected_rows,success_count,success_rate,wilson_lower_95,mean_selected_result_bps,accepted_research\n",
        encoding="utf-8",
    )
    (run_dir / "confidence-reliability-report.csv").write_text(
        "scope,rule,selected_rows,product_90_allowed,shadow_allowed\n",
        encoding="utf-8",
    )
    (run_dir / "candidate-watchlist.csv").write_text("candidate_id,scope,rule,selected_rows,status\n", encoding="utf-8")
    (run_dir / "directional-state-candidates.csv").write_text(
        "rule,evaluation_rows,accepted_shadow\n",
        encoding="utf-8",
    )
    (run_dir / "decision-audit.csv").write_text(
        "\n".join(
            [
                "row_id,ticker,trading_day,spread_bucket,depth_bucket,imbalance_bucket",
                *[
                    f"row-{index},SBER,2026-07-{1 + index % 30:02d},tight,deep,bid_heavy"
                    for index in range(300)
                ],
            ]
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "holdout"
    plan_dir = output_dir / "collection_plan"
    plan_dir.mkdir(parents=True)
    coverage_dir = output_dir / "coverage"
    coverage_dir.mkdir(parents=True)
    (coverage_dir / "coverage.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "max_age_seconds": 30,
                        "signals": 300,
                        "covered_signals": 0,
                        "coverage": 0.0,
                        "covered_sessions": 0,
                        "nearest_prior_orderbook_age_seconds": "",
                    }
                ],
                "by_ticker_day": [
                    {
                        "ticker": "SBER",
                        "trading_day": "2026-07-01",
                        "signals": 10,
                        "covered_signals": 0,
                        "coverage": 0.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (plan_dir / "collection-plan.json").write_text(
        json.dumps({"status": "ready", "recommended_command": ["collector"]}),
        encoding="utf-8",
    )
    args = loop.parse_args(
        [
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(output_dir),
            "--status-output-dir",
            str(tmp_path / "status"),
            "--max-iterations",
            "3",
            "--tickers",
            "SBER",
        ]
    )
    calls: list[list[str]] = []

    def fake_runner(command: object) -> dict[str, object]:
        calls.append(list(command))  # type: ignore[arg-type]
        (coverage_dir / "coverage.json").write_text(
            json.dumps(
                {
                    "rows": [
                        {
                            "max_age_seconds": 30,
                            "signals": 300,
                            "covered_signals": 300,
                            "coverage": 1.0,
                            "covered_sessions": 30,
                            "nearest_prior_orderbook_age_seconds": "10",
                        }
                    ],
                    "by_ticker_day": [
                        {
                            "ticker": "SBER",
                            "trading_day": "2026-07-01",
                            "signals": 10,
                            "covered_signals": 10,
                            "coverage": 1.0,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return {"status": "ok"}

    result = loop.run_collection_loop(args, runner=fake_runner)

    assert result["status"] == "ready"
    assert len(result["iterations"]) == 1
    assert len(calls) == 1
    assert result["final_signal_90_status"]["microstructure"]["ready"] is True
    assert result["iterations"][0]["coverage_progress"]["covered_signals"] == 300
    assert result["iterations"][0]["coverage_delta"]["covered_signals_delta"] == 300
    assert result["iterations"][0]["coverage_delta"]["improved_prior_coverage"] is True
    assert result["final_coverage_progress"]["missing_ticker_days"] == 0
    assert (output_dir / "microstructure-collection-loop.json").exists()
    assert (output_dir / "microstructure-collection-loop.md").exists()


def test_safe_triage_export_skips_when_policy_disabled() -> None:
    exporter = _load_script(
        "research_export_safe_triage_decisions",
        "research_export_safe_triage_decisions.py",
    )

    rows = exporter.export_safe_triage_rows(
        audit_rows=[
            {
                "row_id": "row-1",
                "ticker": "SBER",
                "signal_type": "price_jump",
                "horizon_seconds": "300",
                "max_confidence": "0.94",
                "confidence_band": "strong_signal",
                "frontier_decision": "up",
                "frontier_decision_relation": "direct",
            }
        ],
        policy={"status": "disabled", "default_action": "skip", "product_claim_allowed": False},
        reliability_rows=[
            {
                "scope": "confidence_band",
                "rule": "strong_signal",
                "safe_runtime_action": "candidate",
            }
        ],
    )

    assert rows[0]["product_decision"] == "skip"
    assert rows[0]["product_label_ru"] == "пропустить, недостаточно уверенности"
    assert rows[0]["reason_code"] == "policy_not_enabled"


def test_safe_triage_export_allows_validated_shadow_direction() -> None:
    exporter = _load_script(
        "research_export_safe_triage_decisions_shadow",
        "research_export_safe_triage_decisions.py",
    )

    rows = exporter.export_safe_triage_rows(
        audit_rows=[
            {
                "row_id": "row-1",
                "ticker": "SBER",
                "signal_type": "price_jump",
                "horizon_seconds": "900",
                "max_confidence": "0.82",
                "confidence_band": "working_hypothesis",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
            }
        ],
        policy={"status": "shadow", "selected_threshold": 0.75, "product_claim_allowed": False},
        reliability_rows=[
            {
                "scope": "confidence_band",
                "rule": "working_hypothesis",
                "safe_runtime_action": "shadow",
            }
        ],
    )

    assert rows[0]["product_decision"] == "down"
    assert rows[0]["product_label_ru"] == "ожидается снижение"
    assert rows[0]["display_tier"] == "working_hypothesis"
    assert rows[0]["reason_code"] == "validated_shadow_policy"
    assert rows[0]["product_claim_allowed"] is False


def test_orderbook_coverage_filters_candles_to_snapshot_dates() -> None:
    coverage = _load_script("research_orderbook_signal_coverage", "research_orderbook_signal_coverage.py")
    target_day = date(2026, 7, 16)
    candles = (
        lib.ResearchCandle(
            ticker="SBER",
            at=datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=100.0,
        ),
        lib.ResearchCandle(
            ticker="SBER",
            at=datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=100.0,
        ),
    )
    snapshots = (
        lib.ResearchOrderBookSnapshot(
            ticker="SBER",
            at=datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc),
            depth=10,
            best_bid=100.0,
            best_ask=100.1,
            mid=100.05,
            spread_bps=10.0,
            bid_qty=100.0,
            ask_qty=100.0,
            total_qty=200.0,
            imbalance_ratio=0.0,
            imbalance_abs=0.0,
        ),
    )

    filtered = coverage.filter_candles_to_orderbook_dates(candles, snapshots)

    assert len(filtered) == 1
    assert filtered[0].at.date() == target_day


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
    assert (run_dir / "confidence-threshold-report.csv").exists()
    assert (run_dir / "decision-audit.csv").exists()
    assert (run_dir / "confidence-reliability-report.csv").exists()
    assert (run_dir / "selective-frontier.csv").exists()
    assert (run_dir / "candidate-watchlist.csv").exists()
    assert (run_dir / "high-confidence-slices.csv").exists()
    assert (run_dir / "temporal-stability-report.csv").exists()
    assert (run_dir / "temporal-stability-summary.csv").exists()
    assert (run_dir / "bayesian-state-threshold-report.csv").exists()
    assert (run_dir / "bayesian-state-temporal-summary.csv").exists()
    assert (run_dir / "bayesian-state-candidates.csv").exists()
    assert (run_dir / "confidence-band-audit.csv").exists()
    assert (run_dir / "confidence-band-audit.md").exists()
    assert (run_dir / "directional-state-candidates.csv").exists()
    assert (run_dir / "directional-state-report.md").exists()
    assert (run_dir / "selective-rule-candidates.csv").exists()
    assert (run_dir / "selective-rule-report.md").exists()
    assert (run_dir / "precision-scout-candidates.csv").exists()
    assert (run_dir / "precision-scout-report.md").exists()
    assert (run_dir / "honest-market-states" / "honest-market-state-candidates.csv").exists()
    assert (run_dir / "honest-market-states" / "honest-market-state-report.md").exists()
    assert (run_dir / "safe-triage" / "safe-triage-decisions.csv").exists()
    assert (run_dir / "safe-triage" / "safe-triage-summary.json").exists()
    assert (run_dir / "safe-triage" / "safe-triage-report.md").exists()
    assert (run_dir / "selection-90-report.json").exists()
    assert (run_dir / "selection-90-report.md").exists()
    assert (run_dir / "report.md").exists()
    selection_report = json.loads((run_dir / "selection-90-report.json").read_text(encoding="utf-8"))
    assert selection_report["target"]["success_rate"] == 0.90
    assert selection_report["target"]["minimum_rows"] == 300
    assert selection_report["conclusion"] in {"not_ready_keep_default_skip", "ready_for_shadow"}
    model_results = json.loads((run_dir / "model-results.json").read_text(encoding="utf-8"))
    assert model_results["safe_triage_summary"]["kind"] == "safe_triage_decision_export"
    assert "decision_counts" in model_results["safe_triage_summary"]
    assert "selective_rule_summary" in model_results
    assert "confidence_band_audit" in model_results
    assert "directional_state_candidates" in model_results
    assert "directional_state_summary" in model_results
    assert "honest_market_state_candidates" in model_results
    assert "honest_market_state_summary" in model_results
    assert "selective_rule_candidates" in model_results
    assert "precision_scout_summary" in model_results
    assert "precision_scout_candidates" in model_results
    assert "proof_viability_counts" in model_results["precision_scout_summary"]
    assert "next_action_counts" in model_results["precision_scout_summary"]
    assert "Selective conjunction rules" in (run_dir / "report.md").read_text(encoding="utf-8")
    assert "Precision scout rules" in (run_dir / "report.md").read_text(encoding="utf-8")


def test_wilson_lower_bound_prevents_small_sample_overclaim() -> None:
    lower = lib.wilson_lower_bound(18, 20)

    assert lower is not None
    assert lower < 0.80


def test_confidence_band_audit_blocks_small_strong_sample() -> None:
    audit = _load_script("research_confidence_band_audit_small", "research_confidence_band_audit.py")
    rows = []
    for index in range(20):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 4:02d}",
                "max_confidence": "0.93",
                "frontier_confidence": "0.93",
                "frontier_decision": "up",
                "frontier_decision_relation": "direct",
                "frontier_success": "1" if index < 18 else "0",
                "frontier_result_bps": "12.0" if index < 18 else "-8.0",
            }
        )

    report = audit.build_confidence_band_audit(rows)
    strong = next(row for row in report if row["scope"] == "confidence_band" and row["band"] == "strong_signal")

    assert strong["selected_rows"] == 20
    assert strong["success_rate"] == 0.9
    assert strong["accepted_shadow"] is False
    assert strong["safe_runtime_decision_ru"] == "пропустить, недостаточно уверенности"
    assert "мало случаев" in strong["blocking_reasons_ru"]


def test_confidence_band_audit_allows_large_validated_shadow_candidate() -> None:
    audit = _load_script("research_confidence_band_audit_large", "research_confidence_band_audit.py")
    rows = []
    for index in range(320):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 32:02d}",
                "max_confidence": "0.94",
                "frontier_confidence": "0.94",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
                "frontier_success": "1" if index < 290 else "0",
                "frontier_result_bps": "14.0" if index < 290 else "-6.0",
            }
        )

    report = audit.build_confidence_band_audit(rows)
    strong_down = next(
        row
        for row in report
        if row["scope"] == "confidence_band_direction"
        and row["band"] == "strong_signal"
        and row["candidate_decision"] == "down"
    )

    assert strong_down["selected_rows"] == 320
    assert strong_down["sessions"] == 32
    assert strong_down["success_rate"] >= 0.90
    assert strong_down["wilson_lower_95"] >= 0.75
    assert strong_down["accepted_shadow"] is True
    assert strong_down["safe_runtime_decision_ru"] == "ожидается снижение"
    assert strong_down["product_claim_allowed"] is False


def test_probability_calibration_uses_observed_rates() -> None:
    bins = trainer._calibration_bins(
        probabilities=[0.1, 0.2, 0.8, 0.9],
        labels=[0, 0, 1, 1],
        bins=2,
    )

    assert len(bins) == 2
    assert trainer._apply_calibration(0.15, bins) < trainer._apply_calibration(0.85, bins)


def test_confidence_threshold_rows_use_three_way_decisions() -> None:
    rows = []
    for index in range(10):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 4:02d}",
                "_forward_return_bps": 30.0,
                "_up_target": 1,
                "_down_target": 0,
                "_up_probability": 0.93,
                "_down_probability": 0.10,
            }
        )
    for index in range(5):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 4:02d}",
                "_forward_return_bps": -30.0,
                "_up_target": 0,
                "_down_target": 1,
                "_up_probability": 0.15,
                "_down_probability": 0.91,
            }
        )
    for index in range(7):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 4:02d}",
                "_forward_return_bps": 1.0,
                "_up_target": 0,
                "_down_target": 0,
                "_up_probability": 0.40,
                "_down_probability": 0.35,
            }
        )

    report = trainer.confidence_threshold_rows(rows, thresholds=(0.90,))

    assert report == [
        {
            "threshold": 0.9,
            "eligible_rows": 22,
            "selected_rows": 15,
            "skipped_rows": 7,
            "up_decisions": 10,
            "down_decisions": 5,
            "direct_decisions": 0,
            "inverse_decisions": 0,
            "neutral_decisions": 15,
            "success_count": 15,
            "success_rate": 1.0,
            "wilson_lower_95": report[0]["wilson_lower_95"],
            "sessions": 4,
            "coverage": 15 / 22,
            "mean_selected_result_bps": 20.0,
            "target_success_rate": 0.9,
            "observed_90_success": True,
            "reliable_90_success": False,
            "accepted_research": False,
        }
    ]
    assert report[0]["wilson_lower_95"] < 1.0
    assert report[0]["wilson_lower_95"] < 0.9


def test_confidence_reliability_blocks_uncalibrated_strong_band() -> None:
    rows = []
    for index in range(20):
        rows.append(
            {
                "trading_day": f"2026-07-{1 + index % 5:02d}",
                "_forward_return_bps": 30.0 if index < 8 else -30.0,
                "_up_target": 1 if index < 8 else 0,
                "_down_target": 0 if index < 8 else 1,
                "_up_probability": 0.94,
                "_down_probability": 0.05,
            }
        )

    report = trainer.confidence_reliability_rows(rows)
    strong = next(
        row
        for row in report
        if row["scope"] == "confidence_band" and row["rule"] == "strong_signal"
    )

    assert strong["selected_rows"] == 20
    assert strong["observed_success_rate"] == 0.4
    assert strong["mean_model_confidence"] == pytest.approx(0.94)
    assert strong["confidence_minus_observed"] > 0.5
    assert strong["product_90_allowed"] is False
    assert strong["safe_runtime_action"] == "skip"


def test_selective_frontier_reports_top_confidence_sample_without_overclaim() -> None:
    rows = []
    for index in range(40):
        success = index < 18
        rows.append(
            {
                "row_id": f"row-{index}",
                "signal_type": "price_jump",
                "horizon_seconds": "300",
                "session_bucket": "1",
                "day_volatility_quantile": "0.8",
                "recent_signal_count_300s": "1",
                "trading_day": f"2026-07-{1 + index % 10:02d}",
                "direction": "1",
                "_forward_return_bps": 30.0 if success else -30.0,
                "_up_target": 1 if success else 0,
                "_down_target": 0 if success else 1,
                "_up_probability": 0.99 - index * 0.01,
                "_down_probability": 0.01,
            }
        )

    report = trainer.selective_frontier_rows(rows, counts=(20,), min_report_n=20)
    overall = next(row for row in report if row["scope"] == "all" and row["selected_rows"] == 20)

    assert overall["success_count"] == 18
    assert overall["success_rate"] == 0.9
    assert overall["observed_90_success"] is True
    assert overall["reliable_90_success"] is False
    assert overall["accepted_research"] is False


def test_selective_frontier_does_not_use_event_time_to_break_confidence_ties() -> None:
    rows = [
        {
            "row_id": "z-older-success",
            "signal_type": "price_jump",
            "horizon_seconds": "300",
            "source_event_at": "2026-07-01T07:00:00+00:00",
            "trading_day": "2026-07-01",
            "direction": "1",
            "_forward_return_bps": 30.0,
            "_up_target": 1,
            "_down_target": 0,
            "_up_probability": 0.70,
            "_down_probability": 0.10,
        },
        {
            "row_id": "a-newer-failure",
            "signal_type": "price_jump",
            "horizon_seconds": "300",
            "source_event_at": "2026-07-15T07:00:00+00:00",
            "trading_day": "2026-07-15",
            "direction": "1",
            "_forward_return_bps": -30.0,
            "_up_target": 0,
            "_down_target": 1,
            "_up_probability": 0.70,
            "_down_probability": 0.10,
        },
    ]

    report = trainer.selective_frontier_rows(rows, counts=(1,), min_report_n=1)
    overall = next(row for row in report if row["scope"] == "all" and row["selected_rows"] == 1)

    assert overall["success_count"] == 1


def test_candidate_watchlist_tracks_underpowered_90pct_frontier_candidate() -> None:
    frontier_rows = [
        {
            "scope": "decision_signal_horizon",
            "rule": "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
            "group_population": 179,
            "selected_rows": 20,
            "sessions": 8,
            "selected_trading_days": "2026-07-01|2026-07-02|2026-07-03|2026-07-04|2026-07-05|2026-07-06|2026-07-07|2026-07-08",
            "success_count": 19,
            "success_rate": 0.95,
            "wilson_lower_95": lib.wilson_lower_bound(19, 20),
            "mean_selected_result_bps": 65.0,
            "min_confidence": 0.38,
            "max_confidence": 0.38,
            "up_decisions": 0,
            "down_decisions": 20,
            "direct_decisions": 0,
            "inverse_decisions": 20,
            "neutral_decisions": 0,
            "accepted_research": False,
        }
    ]

    watchlist = trainer.candidate_watchlist_rows(frontier_rows)

    assert len(watchlist) == 1
    assert watchlist[0]["status"] == "watch_only"
    assert watchlist[0]["candidate_id"] == trainer.stable_candidate_id(
        "decision_signal_horizon",
        "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
    )
    assert watchlist[0]["missing_rows_to_shadow_gate"] == 280
    assert watchlist[0]["missing_sessions_to_shadow_gate"] == 22
    assert watchlist[0]["selected_trading_days"] == "2026-07-01|2026-07-02|2026-07-03|2026-07-04|2026-07-05|2026-07-06|2026-07-07|2026-07-08"
    assert watchlist[0]["additional_successes_needed_for_90pct_at_300"] == 251
    assert watchlist[0]["product_claim_allowed"] is False


def test_candidate_ledger_accumulates_observations_by_stable_id(tmp_path: Path) -> None:
    ledger_module = _load_script("research_update_candidate_ledger", "research_update_candidate_ledger.py")
    candidate_id = ledger_module.stable_candidate_id(
        "decision_signal_horizon",
        "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
    )
    watchlist = tmp_path / "run-1" / "candidate-watchlist.csv"
    watchlist.parent.mkdir(parents=True)
    (watchlist.parent / "model-results.json").write_text(
        json.dumps(
            {
                "dataset": "dataset.parquet",
                "dataset_fingerprint": "fingerprint-1",
                "dataset_rows": 1000,
                "validation_sessions": 52,
            }
        ),
        encoding="utf-8",
    )
    watchlist.write_text(
        "\n".join(
            [
                "candidate_id,scope,rule,selected_rows,sessions,selected_trading_days,success_count,success_rate,wilson_lower_95,mean_selected_result_bps,missing_rows_to_shadow_gate,missing_sessions_to_shadow_gate,additional_successes_needed_for_90pct_at_300,missing_reasons,status,product_claim_allowed",
                f"{candidate_id},decision_signal_horizon,decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800,20,8,2026-07-01|2026-07-02|2026-07-03|2026-07-04|2026-07-05|2026-07-06|2026-07-07|2026-07-08,19,0.95,0.7638,65.0,280,22,251,sample_size,watch_only,False",
                "",
            ]
        ),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "candidate-ledger.json"

    ledger = ledger_module.update_candidate_ledger(
        watchlist_path=watchlist,
        ledger_path=ledger_path,
        run_dir=watchlist.parent,
        observed_at="2026-07-16T10:00:00+00:00",
    )

    assert ledger["candidate_count"] == 1
    assert candidate_id in ledger["candidates"]
    assert ledger["candidates"][candidate_id]["latest"]["selected_rows"] == 20
    assert ledger["candidates"][candidate_id]["latest"]["product_claim_allowed"] is False
    assert ledger["candidates"][candidate_id]["latest"]["dataset_fingerprint"] == "fingerprint-1"
    assert ledger["candidates"][candidate_id]["latest"]["selected_trading_days"] == [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
        "2026-07-05",
        "2026-07-06",
        "2026-07-07",
        "2026-07-08",
    ]
    assert ledger["candidates"][candidate_id]["readiness"]["shadow_ready"] is False
    assert len(ledger["candidates"][candidate_id]["observations"]) == 1
    second = ledger_module.update_candidate_ledger(
        watchlist_path=watchlist,
        ledger_path=ledger_path,
        run_dir=watchlist.parent,
        observed_at="2026-07-16T11:00:00+00:00",
    )
    assert len(second["candidates"][candidate_id]["observations"]) == 1
    assert second["candidates"][candidate_id]["latest"]["observed_at"] == "2026-07-16T11:00:00+00:00"
    second_run = tmp_path / "run-2"
    second_run.mkdir()
    (second_run / "model-results.json").write_text(
        json.dumps(
            {
                "dataset": "dataset-2.parquet",
                "dataset_fingerprint": "fingerprint-2",
                "dataset_rows": 1200,
                "validation_sessions": 40,
            }
        ),
        encoding="utf-8",
    )
    second_watchlist = second_run / "candidate-watchlist.csv"
    second_watchlist.write_text(
        "\n".join(
            [
                "candidate_id,scope,rule,selected_rows,sessions,selected_trading_days,success_count,success_rate,wilson_lower_95,mean_selected_result_bps,missing_rows_to_shadow_gate,missing_sessions_to_shadow_gate,additional_successes_needed_for_90pct_at_300,missing_reasons,status,product_claim_allowed",
                f"{candidate_id},decision_signal_horizon,decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800,20,8,2026-07-05|2026-07-06|2026-07-07|2026-07-08|2026-07-09|2026-07-10|2026-07-11|2026-07-12,19,0.95,0.7638,55.0,280,22,251,sample_size,watch_only,False",
                "",
            ]
        ),
        encoding="utf-8",
    )
    third = ledger_module.update_candidate_ledger(
        watchlist_path=second_watchlist,
        ledger_path=ledger_path,
        run_dir=second_run,
        observed_at="2026-07-17T11:00:00+00:00",
    )
    candidate = third["candidates"][candidate_id]
    assert len(candidate["observations"]) == 2
    assert candidate["aggregate"]["selected_rows"] == 40
    assert candidate["aggregate"]["sessions"] == 12
    assert candidate["aggregate"]["success_count"] == 38
    assert candidate["aggregate"]["success_rate"] == 0.95
    assert candidate["aggregate"]["unique_dataset_fingerprints"] == 2
    assert candidate["aggregate_readiness"]["missing_rows_to_shadow_gate"] == 260
    assert candidate["aggregate_readiness"]["missing_sessions_to_shadow_gate"] == 18


def test_candidate_policy_export_keeps_underpowered_candidate_disabled(tmp_path: Path) -> None:
    policy_module = _load_script("research_export_candidate_policy", "research_export_candidate_policy.py")
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "signal_candidate_watchlist_ledger",
                "candidates": {
                    "candidate-1": {
                        "candidate_id": "candidate-1",
                        "scope": "decision_signal_horizon",
                        "rule": "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
                        "aggregate": {
                            "selected_rows": 20,
                            "sessions": 8,
                            "success_count": 19,
                            "success_rate": 0.95,
                            "wilson_lower_95": 0.7638,
                            "mean_selected_result_bps": 65.0,
                            "unique_observations": 1,
                            "unique_dataset_fingerprints": 1,
                        },
                        "aggregate_readiness": {
                            "shadow_ready": False,
                            "product_ready": False,
                            "blocking_reasons": ["sample_size", "trading_days"],
                            "missing_rows_to_shadow_gate": 280,
                            "missing_sessions_to_shadow_gate": 22,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    policy = policy_module.export_candidate_policy(ledger_path, generated_at="2026-07-16T12:00:00+00:00")

    assert policy["status"] == "disabled"
    assert policy["default_action"] == "skip"
    assert policy["shadow_candidate_count"] == 0
    assert policy["rules"][0]["status"] == "watch_only"
    assert policy["rules"][0]["action"] == "skip"
    assert policy["rules"][0]["product_claim_allowed"] is False


def test_candidate_policy_export_promotes_shadow_only_after_gate(tmp_path: Path) -> None:
    policy_module = _load_script("research_export_candidate_policy", "research_export_candidate_policy.py")
    ledger_path = tmp_path / "ledger.json"
    ledger_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "signal_candidate_watchlist_ledger",
                "candidates": {
                    "candidate-1": {
                        "candidate_id": "candidate-1",
                        "scope": "decision_signal_horizon",
                        "rule": "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
                        "aggregate": {
                            "selected_rows": 320,
                            "sessions": 34,
                            "success_count": 292,
                            "success_rate": 0.9125,
                            "wilson_lower_95": 0.875,
                            "mean_selected_result_bps": 12.0,
                            "unique_observations": 4,
                            "unique_dataset_fingerprints": 4,
                        },
                        "aggregate_readiness": {
                            "shadow_ready": True,
                            "product_ready": False,
                            "blocking_reasons": ["product_reliability_bound"],
                            "missing_rows_to_shadow_gate": 0,
                            "missing_sessions_to_shadow_gate": 0,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    policy = policy_module.export_candidate_policy(ledger_path, generated_at="2026-07-16T12:00:00+00:00")

    assert policy["status"] == "shadow"
    assert policy["default_action"] == "skip"
    assert policy["shadow_candidate_count"] == 1
    assert policy["product_claim_allowed"] is False
    assert policy["rules"][0]["status"] == "shadow"
    assert policy["rules"][0]["action"] == "shadow_evaluate"
    assert policy["rules"][0]["shadow_decision"] == "down"
    assert policy["rules"][0]["decision_ready"] is True
    assert policy["rules"][0]["admin_only"] is True
    assert policy["rules"][0]["product_claim_allowed"] is False


def test_candidate_policy_evaluation_ignores_disabled_policy(tmp_path: Path) -> None:
    evaluator = _load_script("research_evaluate_candidate_policy", "research_evaluate_candidate_policy.py")
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "research_candidate_decision_policy",
                "status": "disabled",
                "rules": [
                    {
                        "candidate_id": "candidate-1",
                        "status": "watch_only",
                        "action": "skip",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    frontier_path = tmp_path / "selective-frontier.csv"
    frontier_path.write_text("scope,rule,selected_rows,sessions,success_count\n", encoding="utf-8")

    result = evaluator.evaluate_candidate_policy(policy_path=policy_path, frontier_path=frontier_path)

    assert result["evaluated_rules"] == 0
    assert result["status"] == "no_shadow_candidate_passed"
    assert result["product_claim_allowed"] is False


def test_candidate_policy_evaluation_requires_independent_dataset(tmp_path: Path) -> None:
    evaluator = _load_script("research_evaluate_candidate_policy", "research_evaluate_candidate_policy.py")
    scope = "decision_signal_horizon"
    rule = "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800"
    candidate_id = evaluator.stable_candidate_id(scope, rule)
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "research_candidate_decision_policy",
                "status": "shadow",
                "rules": [
                    {
                        "candidate_id": candidate_id,
                        "scope": scope,
                        "rule": rule,
                        "status": "shadow",
                        "source_dataset_fingerprints": ["training-fingerprint"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"dataset_fingerprint": "training-fingerprint"}),
        encoding="utf-8",
    )
    frontier_path = run_dir / "selective-frontier.csv"
    frontier_path.write_text(
        "\n".join(
            [
                "scope,rule,selected_rows,sessions,success_count,mean_selected_result_bps",
                f"{scope},{rule},320,34,292,12.0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = evaluator.evaluate_candidate_policy(policy_path=policy_path, frontier_path=frontier_path, run_dir=run_dir)

    assert result["evaluated_rules"] == 1
    assert result["evaluations"][0]["independent_dataset"] is False
    assert result["evaluations"][0]["passed_shadow_gate"] is False

    (run_dir / "model-results.json").write_text(
        json.dumps({"dataset_fingerprint": "new-forward-fingerprint"}),
        encoding="utf-8",
    )
    independent = evaluator.evaluate_candidate_policy(policy_path=policy_path, frontier_path=frontier_path, run_dir=run_dir)

    assert independent["evaluations"][0]["independent_dataset"] is True
    assert independent["evaluations"][0]["passed_shadow_gate"] is True
    assert independent["status"] == "passed_shadow"
    assert independent["product_claim_allowed"] is False


def test_candidate_policy_application_skips_without_shadow_policy() -> None:
    applier = _load_script("research_apply_candidate_policy_disabled", "research_apply_candidate_policy.py")
    rows = applier.apply_candidate_policy_rows(
        audit_rows=[
            {
                "row_id": "row-1",
                "ticker": "SBER",
                "frontier_decision": "down",
                "frontier_confidence": "0.9",
            }
        ],
        policy={"status": "disabled", "rules": []},
    )

    assert rows[0]["product_decision"] == "skip"
    assert rows[0]["product_label_ru"] == "пропустить, недостаточно уверенности"
    assert rows[0]["reason_code"] == "no_shadow_candidate_policy"
    assert rows[0]["product_claim_allowed"] is False


def test_candidate_policy_application_emits_admin_only_shadow_direction() -> None:
    applier = _load_script("research_apply_candidate_policy_shadow", "research_apply_candidate_policy.py")
    rows = applier.apply_candidate_policy_rows(
        audit_rows=[
            {
                "row_id": "match",
                "ticker": "SBER",
                "signal_type": "price_volume_range_combo_long",
                "horizon_seconds": "1800",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
                "frontier_confidence": "0.91",
                "frontier_success": "1",
                "frontier_result_bps": "18.0",
            },
            {
                "row_id": "skip",
                "ticker": "SBER",
                "signal_type": "volume_spike",
                "horizon_seconds": "300",
                "frontier_decision": "up",
                "frontier_decision_relation": "direct",
                "frontier_confidence": "0.88",
            },
        ],
        policy={
            "status": "shadow",
            "rules": [
                {
                    "candidate_id": "candidate-1",
                    "status": "shadow",
                    "rule": (
                        "decision=down | decision_relation=inverse | "
                        "signal_type=price_volume_range_combo_long | horizon_seconds=1800"
                    ),
                    "shadow_decision": "down",
                }
            ],
        },
    )

    assert rows[0]["product_decision"] == "down"
    assert rows[0]["product_label_ru"] == "ожидается снижение"
    assert rows[0]["shadow_candidate_id"] == "candidate-1"
    assert rows[0]["shadow_admin_only"] is True
    assert rows[0]["product_claim_allowed"] is False
    assert rows[1]["product_decision"] == "skip"
    assert rows[1]["reason_code"] == "no_shadow_rule_match"


def test_candidate_policy_application_blocks_same_dataset_shadow_rule() -> None:
    applier = _load_script("research_apply_candidate_policy_same_dataset", "research_apply_candidate_policy.py")
    rows = applier.apply_candidate_policy_rows(
        audit_rows=[
            {
                "row_id": "same-dataset",
                "ticker": "SBER",
                "signal_type": "price_volume_range_combo_long",
                "horizon_seconds": "1800",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
                "frontier_confidence": "0.91",
            }
        ],
        policy={
            "status": "shadow",
            "rules": [
                {
                    "candidate_id": "candidate-1",
                    "status": "shadow",
                    "rule": "decision=down | signal_type=price_volume_range_combo_long | horizon_seconds=1800",
                    "shadow_decision": "down",
                    "source_dataset_fingerprints": ["same-fingerprint"],
                }
            ],
        },
        current_dataset_fingerprint="same-fingerprint",
    )

    assert rows[0]["product_decision"] == "skip"
    assert rows[0]["reason_code"] == "shadow_policy_not_independent"
    assert rows[0]["product_claim_allowed"] is False


def test_decision_audit_rows_include_skip_and_directional_decisions() -> None:
    rows = [
        {
            "row_id": "up",
            "ticker": "SBER",
            "signal_type": "price_jump",
            "source_event_at": "2026-07-15T07:05:00+00:00",
            "trading_day": "2026-07-15",
            "horizon_seconds": "300",
            "direction": "1",
            "session_bucket": "3",
            "combo_key_300s": "price_jump+volume_spike",
            "recent_signal_count_300s": "2",
            "day_volatility_quantile": "0.80",
            "ticker_volatility_quantile": "0.70",
            "ticker_volume_quantile": "0.90",
            "pre_consolidation_score_60m": "0.10",
            "pre_return_bps_60m": "42.0",
            "pre_return_to_volatility_60m": "1.8",
            "market_return_bps_60m": "18.0",
            "signal_vs_market_bps_60m": "24.0",
            "orderbook_available": True,
            "orderbook_spread_bps": "8.0",
            "orderbook_total_qty": "12000",
            "orderbook_imbalance_ratio": "0.72",
            "event_to_pre_volatility_60m": "4.2",
            "event_to_pre_range_60m": "3.1",
            "event_body_to_range": "0.10",
            "event_upper_wick_to_range": "0.82",
            "event_lower_wick_to_range": "0.08",
            "event_close_to_direction": "0.18",
            "event_reversal_pressure": "1.0",
            "_forward_return_bps": 30.0,
            "_up_target": 1,
            "_down_target": 0,
            "_up_probability": 0.8,
            "_down_probability": 0.1,
        },
        {
            "row_id": "skip",
            "ticker": "SBER",
            "signal_type": "price_jump",
            "source_event_at": "2026-07-15T07:06:00+00:00",
            "trading_day": "2026-07-15",
            "horizon_seconds": "300",
            "direction": "1",
            "_forward_return_bps": 1.0,
            "_up_target": 0,
            "_down_target": 0,
            "_up_probability": 0.2,
            "_down_probability": 0.3,
        },
    ]

    audit = trainer.decision_audit_rows(rows)

    assert audit[0]["decision"] == "up"
    assert audit[0]["confidence_band"] == "working_hypothesis"
    assert audit[0]["success"] == 1
    assert audit[0]["session_bucket"] == "3"
    assert audit[0]["volatility_bucket"] == "high"
    assert audit[0]["consolidation_bucket"] == "compressed"
    assert audit[0]["liquidity_bucket"] == "liquid"
    assert audit[0]["pre_trend_bucket"] == "up"
    assert audit[0]["pre_trend_strength_bucket"] == "strong"
    assert audit[0]["event_trend_relation"] == "with_pretrend"
    assert audit[0]["decision_trend_relation"] == "with_pretrend"
    assert audit[0]["event_close_quality_bucket"] == "weak_close"
    assert audit[0]["event_reversal_pressure_bucket"] == "high_reversal_pressure"
    assert audit[0]["event_upper_wick_to_range"] == "0.82"
    assert audit[0]["event_reversal_pressure"] == "1.0"
    assert audit[0]["market_alignment_bucket"] == "with_market"
    assert audit[0]["relative_market_bucket"] == "up"
    assert audit[0]["spread_bucket"] == "tight"
    assert audit[0]["depth_bucket"] == "deep"
    assert audit[0]["imbalance_bucket"] == "bid_heavy"
    assert audit[0]["signal_count_bucket"] == "cluster_2_3"
    assert audit[0]["combo_key_300s"] == "price_jump+volume_spike"
    assert audit[0]["event_to_pre_volatility_60m"] == "4.2"
    assert audit[0]["frontier_decision"] == "up"
    assert audit[0]["frontier_success"] == 1
    assert audit[1]["decision"] == "skip"
    assert audit[1]["confidence_band"] == "skip"
    assert audit[1]["frontier_decision"] == "down"
    assert audit[1]["frontier_confidence"] == 0.3
    assert audit[1]["frontier_success"] == 0


def test_candidate_audit_extractor_uses_frontier_decision_for_watchlist_rule(tmp_path: Path) -> None:
    extractor = _load_script(
        "research_extract_candidate_audit_rows",
        "research_extract_candidate_audit_rows.py",
    )
    scope = "decision_signal_session_volatility_horizon"
    rule = (
        "decision=down | signal_type=price_volume_range_combo_long | "
        "session_bucket=3 | volatility_bucket=high | horizon_seconds=1800"
    )
    candidate_id = extractor.stable_candidate_id(scope, rule)
    watchlist = tmp_path / "candidate-watchlist.csv"
    watchlist.write_text(
        "\n".join(
            [
                "candidate_id,scope,rule,selected_rows,status",
                f"{candidate_id},{scope},{rule},2,watch_only",
            ]
        ),
        encoding="utf-8",
    )
    audit = tmp_path / "decision-audit.csv"
    audit.write_text(
        "\n".join(
            [
                (
                    "row_id,ticker,signal_type,source_event_at,trading_day,horizon_seconds,"
                    "original_direction,session_bucket,volatility_bucket,signal_count_bucket,"
                    "combo_key_300s,recent_signal_count_60s,recent_signal_count_300s,"
                    "recent_signal_count_900s,up_confidence,down_confidence,decision,success,"
                    "decision_result_bps,confidence_band,frontier_decision,"
                    "frontier_decision_relation,frontier_success,frontier_confidence,"
                    "frontier_result_bps,forward_return_bps"
                ),
                (
                    "match-low,SBER,price_volume_range_combo_long,2026-07-15T07:05:00+00:00,"
                    "2026-07-15,1800,1,3,high,cluster_2_3,price_jump+volume_spike,1,2,2,"
                    "0.10,0.61,skip,0,0.0,skip,down,inverse,1,0.61,25.0,-27.0"
                ),
                (
                    "match-high,GAZP,price_volume_range_combo_long,2026-07-15T07:06:00+00:00,"
                    "2026-07-15,1800,1,3,high,cluster_2_3,price_jump+volume_spike,1,2,2,"
                    "0.10,0.72,skip,0,0.0,skip,down,inverse,0,0.72,-12.0,10.0"
                ),
                (
                    "no-match,LKOH,price_volume_range_combo_long,2026-07-15T07:07:00+00:00,"
                    "2026-07-15,1800,1,3,high,cluster_2_3,price_jump+volume_spike,1,2,2,"
                    "0.80,0.10,up,1,20.0,working_hypothesis,up,direct,1,0.80,20.0,22.0"
                ),
            ]
        ),
        encoding="utf-8",
    )

    rows = extractor.extract_candidate_audit_rows(watchlist_path=watchlist, audit_path=audit)

    assert [row["row_id"] for row in rows] == ["match-high", "match-low"]
    assert rows[0]["candidate_id"] == candidate_id
    assert rows[0]["rank"] == 1
    assert rows[0]["policy_decision"] == "skip"
    assert rows[0]["frontier_decision"] == "down"
    assert rows[0]["frontier_decision_relation"] == "inverse"


def test_candidate_audit_extractor_ignores_empty_watchlist_placeholder(tmp_path: Path) -> None:
    extractor = _load_script(
        "research_extract_candidate_audit_rows_empty",
        "research_extract_candidate_audit_rows.py",
    )
    watchlist = tmp_path / "candidate-watchlist.csv"
    watchlist.write_text(
        "candidate_id,scope,rule,selected_rows,status\n,,,,\n",
        encoding="utf-8",
    )
    audit = tmp_path / "decision-audit.csv"
    audit.write_text(
        "row_id,ticker,frontier_decision,frontier_confidence\nrow-1,SBER,down,0.9\n",
        encoding="utf-8",
    )

    rows = extractor.extract_candidate_audit_rows(watchlist_path=watchlist, audit_path=audit)

    assert rows == []


def test_selective_rule_atoms_include_trend_and_market_context() -> None:
    miner = _load_script(
        "research_mine_selective_rules_trend_atoms",
        "research_mine_selective_rules.py",
    )
    atoms = miner.row_atoms(
        {
            "frontier_decision": "down",
            "frontier_decision_relation": "inverse",
            "signal_type": "price_jump",
            "horizon_seconds": "1800",
            "pre_trend_bucket": "up",
            "pre_trend_strength_bucket": "strong",
            "event_trend_relation": "with_pretrend",
            "decision_trend_relation": "against_pretrend",
            "market_alignment_bucket": "against_market",
            "relative_market_bucket": "down",
            "pre_abs_return_bps_60m": "55.0",
            "pre_directional_return_bps_60m": "40.0",
            "pre_return_to_volatility_60m": "2.2",
            "signal_vs_market_bps_60m": "32.0",
        }
    )

    assert "pre_trend_bucket=up" in atoms
    assert "pre_trend_strength_bucket=strong" in atoms
    assert "decision_trend_relation=against_pretrend" in atoms
    assert "market_alignment_bucket=against_market" in atoms
    assert "relative_market_bucket=down" in atoms
    assert "pre_abs_return_bps_60m>=50" in atoms
    assert "pre_return_to_volatility_60m>=1.5" in atoms


def test_directional_state_miner_accepts_inverse_state_only_out_of_sample() -> None:
    miner = _load_script(
        "research_mine_directional_states",
        "research_mine_directional_states.py",
    )
    rows = []
    for index in range(60):
        rows.append(
            {
                "row_id": f"row-{index}",
                "ticker": f"T{index}",
                "trading_day": f"2026-07-{1 + index:02d}",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
                "signal_type": "price_volume_range_combo_long",
                "horizon_seconds": "1800",
                "session_bucket": "3",
                "volatility_bucket": "high",
                "consolidation_bucket": "compressed",
                "liquidity_bucket": "liquid",
                "spread_bucket": "tight",
                "depth_bucket": "deep",
                "imbalance_bucket": "bid_heavy",
                "signal_count_bucket": "cluster_2_3",
                "combo_key_300s": "price_jump+volume_spike",
                "frontier_confidence": "0.80",
                "frontier_success": "1",
                "frontier_result_bps": "25.0",
            }
        )

    candidates = miner.mine_directional_state_candidates(
        rows,
        confidence_thresholds=(0.75,),
        min_discovery_rows=30,
        accepted_min_rows=30,
        accepted_min_sessions=30,
    )

    accepted = [row for row in candidates if row["accepted_shadow"]]
    assert accepted
    assert accepted[0]["evaluation_inverse_rows"] == 30
    assert accepted[0]["temporal_supported"] is True
    assert accepted[0]["product_claim_allowed"] is False
    assert any(
        row["group_set"] == "decision_relation_consolidation_liquidity_horizon"
        and "consolidation_bucket=compressed" in row["rule"]
        and "liquidity_bucket=liquid" in row["rule"]
        for row in accepted
    )
    assert any(
        row["group_set"] == "decision_relation_microstructure_horizon"
        and "spread_bucket=tight" in row["rule"]
        and "depth_bucket=deep" in row["rule"]
        and "imbalance_bucket=bid_heavy" in row["rule"]
        for row in accepted
    )


def test_directional_state_miner_rejects_discovery_rule_that_fails_later() -> None:
    miner = _load_script(
        "research_mine_directional_states_fail_later",
        "research_mine_directional_states.py",
    )
    rows = []
    for index in range(10):
        rows.append(
            {
                "row_id": f"row-{index}",
                "ticker": f"T{index}",
                "trading_day": f"2026-07-{1 + index:02d}",
                "frontier_decision": "up",
                "frontier_decision_relation": "direct",
                "signal_type": "price_jump",
                "horizon_seconds": "300",
                "session_bucket": "1",
                "volatility_bucket": "high",
                "signal_count_bucket": "single",
                "combo_key_300s": "price_jump",
                "frontier_confidence": "0.80",
                "frontier_success": "1" if index < 5 else "0",
                "frontier_result_bps": "20.0" if index < 5 else "-20.0",
            }
        )

    candidates = miner.mine_directional_state_candidates(
        rows,
        confidence_thresholds=(0.75,),
        min_discovery_rows=5,
        accepted_min_rows=5,
        accepted_min_sessions=5,
    )

    assert candidates
    assert all(not row["accepted_shadow"] for row in candidates)
    assert "success_rate" in candidates[0]["blocking_reasons"]


def test_directional_state_miner_rejects_inverse_state_with_weak_late_block() -> None:
    miner = _load_script(
        "research_mine_directional_states_inverse_temporal",
        "research_mine_directional_states.py",
    )
    rows = []
    for index in range(80):
        later_index = index - 40
        in_weak_late_block = 40 <= index < 48
        success = index < 40 or not in_weak_late_block or later_index < 4
        rows.append(
            {
                "row_id": f"inverse-temporal-{index}",
                "ticker": f"T{index % 8}",
                "trading_day": f"2026-07-{1 + index:02d}",
                "frontier_decision": "down",
                "frontier_decision_relation": "inverse",
                "signal_type": "price_jump",
                "horizon_seconds": "1800",
                "session_bucket": "2",
                "volatility_bucket": "high",
                "signal_count_bucket": "cluster_2_3",
                "combo_key_300s": "price_jump+volume_spike",
                "frontier_confidence": "0.82",
                "frontier_success": "1" if success else "0",
                "frontier_result_bps": "20.0" if success else "-15.0",
            }
        )

    candidates = miner.mine_directional_state_candidates(
        rows,
        confidence_thresholds=(0.75,),
        min_discovery_rows=40,
        accepted_min_rows=40,
        accepted_min_sessions=30,
    )
    target = next(
        row
        for row in candidates
        if row["group_set"] == "decision_relation_signal_horizon"
        and "frontier_decision_relation=inverse" in row["rule"]
    )

    assert target["evaluation_success_rate"] >= 0.9
    assert target["temporal_supported"] is False
    assert target["temporal_weak_blocks"] >= 1
    assert target["accepted_shadow"] is False
    assert "temporal_instability" in target["blocking_reasons"]


def test_temporal_stability_rows_split_thresholds_by_trading_days() -> None:
    rows = []
    for day_index in range(4):
        for index in range(3):
            rows.append(
                {
                    "trading_day": f"2026-07-{1 + day_index:02d}",
                    "_forward_return_bps": 30.0 if day_index < 2 else -30.0,
                    "_up_target": 1 if day_index < 2 else 0,
                    "_down_target": 0 if day_index < 2 else 1,
                    "_up_probability": 0.91,
                    "_down_probability": 0.10,
                }
            )

    report = trainer.temporal_stability_rows(rows, thresholds=(0.90,), blocks=2)

    assert len(report) == 2
    assert report[0]["first_day"] == "2026-07-01"
    assert report[0]["last_day"] == "2026-07-02"
    assert report[0]["selected_rows"] == 6
    assert report[0]["success_rate"] == 1.0
    assert report[0]["observed_90_success"] is True
    assert report[1]["first_day"] == "2026-07-03"
    assert report[1]["last_day"] == "2026-07-04"
    assert report[1]["selected_rows"] == 6
    assert report[1]["success_rate"] == 0.0
    assert report[1]["observed_90_success"] is False


def test_bayesian_state_candidates_score_matching_validation_rows() -> None:
    train_rows = []
    for index in range(6):
        train_rows.append(
            {
                "row_id": f"train-{index}",
                "signal_type": "price_jump",
                "horizon_seconds": "300",
                "session_bucket": "1",
                "combo_key_300s": "price_jump+volume_spike",
                "day_volatility_quantile": "0.8",
                "recent_signal_count_300s": "2",
                "trading_day": f"2026-07-{1 + index:02d}",
                "forward_return_bps": "30" if index < 5 else "-30",
            }
        )
    validation_rows = [
        {
            "row_id": "valid-1",
            "signal_type": "price_jump",
            "horizon_seconds": "300",
            "session_bucket": "1",
            "combo_key_300s": "price_jump+volume_spike",
            "day_volatility_quantile": "0.8",
            "recent_signal_count_300s": "2",
            "trading_day": "2026-07-20",
            "forward_return_bps": "30",
        }
    ]

    candidates = trainer.train_bayesian_state_candidates(train_rows, min_train_rows=2)
    scored = trainer.score_bayesian_state_rows(validation_rows, candidates)

    assert candidates
    assert scored[0]["_up_probability"] > scored[0]["_down_probability"]
    assert scored[0]["_up_probability"] > 0.5
    assert scored[0]["_bayesian_train_rows"] >= 2


def test_high_confidence_slice_rows_require_enough_observations() -> None:
    rows = []
    for index in range(320):
        success = index < 290
        rows.append(
            {
                "row_id": f"large-{index}",
                "ticker": "SBER",
                "signal_type": "price_jump",
                "trading_day": f"2026-07-{1 + index % 31:02d}",
                "horizon_seconds": "300",
                "session_bucket": "0",
                "combo_key_300s": "price_jump+volume_spike",
                "recent_signal_count_300s": "2",
                "day_volatility_quantile": "0.8",
                "direction": "1",
                "_forward_return_bps": 30.0 if success else -30.0,
                "_up_target": 1 if success else 0,
                "_down_target": 0 if success else 1,
                "_up_probability": 0.92,
                "_down_probability": 0.10,
            }
        )
    for index in range(20):
        success = index < 18
        rows.append(
            {
                "row_id": f"small-{index}",
                "ticker": "GAZP",
                "signal_type": "price_jump",
                "trading_day": f"2026-07-{1 + index % 4:02d}",
                "horizon_seconds": "900",
                "session_bucket": "1",
                "combo_key_300s": "price_jump",
                "recent_signal_count_300s": "1",
                "day_volatility_quantile": "0.9",
                "direction": "1",
                "_forward_return_bps": 30.0 if success else -30.0,
                "_up_target": 1 if success else 0,
                "_down_target": 0 if success else 1,
                "_up_probability": 0.93,
                "_down_probability": 0.10,
            }
        )

    report = trainer.high_confidence_slice_rows(rows, thresholds=(0.90,), min_n=1)
    accepted = [row for row in report if row["accepted_shadow"]]
    small = [
        row
        for row in report
        if "ticker=GAZP" in row["rule"] or "horizon_seconds=900" in row["rule"]
    ]

    assert accepted
    assert accepted[0]["selected_rows"] >= 300
    assert accepted[0]["sessions"] >= 30
    assert accepted[0]["success_rate"] >= 0.90
    assert accepted[0]["wilson_lower_95"] >= 0.75
    assert accepted[0]["product_claim_allowed"] is False
    assert small
    assert all(not row["accepted_shadow"] for row in small)


def test_decision_policy_skips_by_default_without_accepted_threshold() -> None:
    policy = trainer.build_decision_policy(
        [
            {
                "threshold": 0.9,
                "selected_rows": 20,
                "success_rate": 0.9,
                "wilson_lower_95": 0.7,
                "accepted_research": False,
            }
        ]
    )

    assert policy["status"] == "disabled"
    assert policy["default_action"] == "skip"
    assert policy["selected_threshold"] is None
    assert policy["product_claim_allowed"] is False


def test_decision_policy_selects_best_accepted_threshold_for_shadow() -> None:
    policy = trainer.build_decision_policy(
        [
            {
                "threshold": 0.75,
                "selected_rows": 400,
                "success_rate": 0.8,
                "wilson_lower_95": 0.76,
                "accepted_research": True,
            },
            {
                "threshold": 0.9,
                "selected_rows": 320,
                "success_rate": 0.88,
                "wilson_lower_95": 0.81,
                "accepted_research": True,
            },
        ]
    )

    assert policy["status"] == "shadow"
    assert policy["default_action"] == "skip"
    assert policy["selected_threshold"] == 0.9
    assert policy["product_claim_allowed"] is False


def test_decision_policy_rejects_threshold_without_temporal_support() -> None:
    policy = trainer.build_decision_policy(
        [
            {
                "threshold": 0.9,
                "selected_rows": 320,
                "success_rate": 0.91,
                "wilson_lower_95": 0.81,
                "accepted_research": True,
            }
        ],
        [
            {
                "threshold": 0.9,
                "blocks": 5,
                "blocks_with_selected": 1,
                "min_success_rate": 0.91,
                "min_wilson_lower_95": 0.81,
                "weak_blocks": 0,
                "temporal_supported": False,
            }
        ],
    )

    assert policy["status"] == "disabled"
    assert policy["reason_code"] == "no_temporally_stable_confidence_threshold_passed_research_gate"
    assert policy["selected_threshold"] is None


def test_decision_policy_accepts_temporally_supported_threshold_for_shadow() -> None:
    policy = trainer.build_decision_policy(
        [
            {
                "threshold": 0.9,
                "selected_rows": 320,
                "success_rate": 0.91,
                "wilson_lower_95": 0.81,
                "accepted_research": True,
            }
        ],
        [
            {
                "threshold": 0.9,
                "blocks": 5,
                "blocks_with_selected": 4,
                "min_success_rate": 0.9,
                "min_wilson_lower_95": 0.76,
                "weak_blocks": 0,
                "temporal_supported": True,
            }
        ],
    )

    assert policy["status"] == "shadow"
    assert policy["selected_threshold"] == 0.9
    assert policy["selected_threshold_temporal_evidence"]["blocks_with_selected"] == 4


def test_pattern_mining_promotes_positive_top_decile_rule() -> None:
    rows = []
    for index in range(320):
        day = date(2026, 7, 1) + timedelta(days=index % 35)
        rows.append(
            {
                "ticker": "SBER",
                "signal_type": "price_jump",
                "horizon_seconds": "900",
                "session_bucket": "0",
                "_volatility_bucket": "high",
                "combo_key_300s": "price_jump+volume_spike",
                "trading_day": day.isoformat(),
                "_target": 1 if index < 290 else 0,
                "_predicted_probability": 0.9 - index / 10_000,
                "_cost_adjusted_directional_bps": 8.0 if index < 290 else 1.0,
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
    assert detailed[0]["positive_rate"] == 290 / 320
    assert detailed[0]["wilson_lower_95"] >= 0.75
    assert detailed[0]["mean_cost_adjusted_directional_bps"] > 0


def test_out_of_sample_pattern_mining_rejects_discovery_only_rule() -> None:
    discovery_rows = []
    validation_rows = []
    for index in range(320):
        base = {
            "ticker": "SBER",
            "signal_type": "price_jump",
            "horizon_seconds": "900",
            "session_bucket": "0",
            "_volatility_bucket": "high",
            "combo_key_300s": "price_jump+volume_spike",
            "_predicted_probability": 0.95,
            "_reverse_directional_bps": -18.0,
        }
        discovery_rows.append(
            base
            | {
                "trading_day": (date(2026, 7, 1) + timedelta(days=index % 35)).isoformat(),
                "_target": 1,
                "_cost_adjusted_directional_bps": 12.0,
            }
        )
        validation_rows.append(
            base
            | {
                "trading_day": (date(2026, 9, 1) + timedelta(days=index % 35)).isoformat(),
                "_target": 0,
                "_cost_adjusted_directional_bps": -12.0,
            }
        )

    candidates = patterns.mine_out_of_sample_pattern_candidates(
        discovery_rows,
        validation_rows,
        naive_positive_rate=0.05,
        top_fraction=1.0,
        min_n=100,
        accepted_min_n=300,
    )

    assert candidates
    assert candidates[0]["discovery_positive_rate"] == 1.0
    assert candidates[0]["validation_positive_rate"] == 0.0
    assert candidates[0]["accepted_out_of_sample"] is False


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_goal_90_audit_blocks_unsafe_product_claim(tmp_path: Path) -> None:
    audit_script = _load_script("research_audit_90_goal_readiness", "research_audit_90_goal_readiness.py")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "test-run", "validation_sessions": 52}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "disabled", "product_claim_allowed": False}),
        encoding="utf-8",
    )
    (run_dir / "safe-triage").mkdir()
    (run_dir / "safe-triage" / "safe-triage-summary.json").write_text(
        json.dumps({"rows": 10, "decision_counts": {"skip": 10}}),
        encoding="utf-8",
    )
    _write_csv(
        run_dir / "confidence-threshold-report.csv",
        [
            {
                "threshold": 0.9,
                "selected_rows": 10,
                "success_rate": 0.9,
                "wilson_lower_95": 0.6,
                "accepted_research": False,
            }
        ],
    )
    _write_csv(
        run_dir / "confidence-reliability-report.csv",
        [
            {
                "scope": "confidence_band",
                "rule": "strong_signal",
                "selected_rows": 10,
                "observed_success_rate": 0.9,
                "wilson_lower_95": 0.6,
                "product_90_allowed": False,
            }
        ],
    )
    _write_csv(
        run_dir / "directional-state-candidates.csv",
        [
            {
                "rule": "relation=inverse",
                "evaluation_rows": 10,
                "accepted_shadow": False,
            }
        ],
    )
    signal_status = tmp_path / "signal-90-status.json"
    signal_status.write_text(
        json.dumps(
            {
                "product_claim_allowed": False,
                "status": "not_ready",
                "microstructure": {"ready": False, "usable_rows": 0},
            }
        ),
        encoding="utf-8",
    )
    collection_plan = tmp_path / "collection-plan.json"
    collection_plan.write_text(
        json.dumps({"status": "collect_more_data", "missing_covered_signals": 300}),
        encoding="utf-8",
    )

    audit = audit_script.build_goal_90_audit(
        run_dir=run_dir,
        signal_status_path=signal_status,
        collection_plan_path=collection_plan,
    )

    assert audit["status"] == "not_ready"
    assert "safe_default_skip" not in audit["blocking_failures"]
    assert "accepted_confidence_threshold" in audit["blocking_failures"]
    assert "product_reliability_lower_bound" in audit["blocking_failures"]
    assert "microstructure_gate" in audit["blocking_failures"]
    assert "liquidity_holdout" in audit["blocking_failures"]


def test_goal_90_audit_counts_honest_market_states(tmp_path: Path) -> None:
    audit_script = _load_script(
        "research_audit_90_goal_readiness_honest_states",
        "research_audit_90_goal_readiness.py",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "test-run", "validation_sessions": 52}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "disabled", "product_claim_allowed": False}),
        encoding="utf-8",
    )
    (run_dir / "safe-triage").mkdir()
    (run_dir / "safe-triage" / "safe-triage-summary.json").write_text(
        json.dumps({"rows": 10, "decision_counts": {"skip": 10}}),
        encoding="utf-8",
    )
    _write_csv(run_dir / "confidence-threshold-report.csv", [{"selected_rows": 0}])
    _write_csv(run_dir / "confidence-reliability-report.csv", [{"selected_rows": 0}])
    _write_csv(
        run_dir / "honest-market-states" / "honest-market-state-candidates.csv",
        [
            {
                "rule": "candidate_action=inverse | signal_type=price_jump",
                "evaluation_rows": 320,
                "evaluation_sessions": 35,
                "accepted_shadow": True,
            }
        ],
    )

    audit = audit_script.build_goal_90_audit(run_dir=run_dir)
    by_id = {check["id"]: check for check in audit["checks"]}

    assert by_id["market_state_search"]["status"] == "passed"
    assert by_id["accepted_market_state"]["status"] == "passed"
    assert by_id["inverse_hypothesis_search"]["status"] == "passed"
    assert by_id["market_state_search"]["observed"]["honest_market_state_rows"] == 1
    assert by_id["accepted_market_state"]["observed"]["accepted_honest_market_states"] == 1


def test_goal_90_audit_allows_only_fully_evidenced_policy(tmp_path: Path) -> None:
    audit_script = _load_script(
        "research_audit_90_goal_readiness_ready",
        "research_audit_90_goal_readiness.py",
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "ready-run", "validation_sessions": 35}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "shadow", "product_claim_allowed": True}),
        encoding="utf-8",
    )
    (run_dir / "safe-triage").mkdir()
    (run_dir / "safe-triage" / "safe-triage-summary.json").write_text(
        json.dumps({"rows": 320, "decision_counts": {"up": 300, "down": 20}}),
        encoding="utf-8",
    )
    _write_csv(
        run_dir / "confidence-threshold-report.csv",
        [
            {
                "threshold": 0.95,
                "selected_rows": 320,
                "success_rate": 0.94,
                "wilson_lower_95": 0.91,
                "accepted_research": True,
            }
        ],
    )
    _write_csv(
        run_dir / "confidence-reliability-report.csv",
        [
            {
                "scope": "confidence_band",
                "rule": "strong_signal",
                "selected_rows": 320,
                "observed_success_rate": 0.94,
                "wilson_lower_95": 0.91,
                "product_90_allowed": True,
            }
        ],
    )
    _write_csv(
        run_dir / "directional-state-candidates.csv",
        [
            {
                "rule": "relation=inverse",
                "evaluation_rows": 320,
                "accepted_shadow": True,
            }
        ],
    )
    signal_status = tmp_path / "signal-90-status.json"
    signal_status.write_text(
        json.dumps(
            {
                "product_claim_allowed": True,
                "status": "ready_for_product_claim",
                "microstructure": {"ready": True, "usable_rows": 320, "usable_sessions": 35},
            }
        ),
        encoding="utf-8",
    )
    collection_plan = tmp_path / "collection-plan.json"
    collection_plan.write_text(json.dumps({"status": "ready"}), encoding="utf-8")

    audit = audit_script.build_goal_90_audit(
        run_dir=run_dir,
        signal_status_path=signal_status,
        collection_plan_path=collection_plan,
    )

    assert audit["status"] == "ready_for_shadow_candidate"
    assert audit["blocking_failures"] == []


def test_selective_rule_miner_accepts_stable_conjunctive_rule() -> None:
    miner = _load_script("research_mine_selective_rules", "research_mine_selective_rules.py")
    rows: list[dict[str, object]] = []
    tickers = ["SBER", "GAZP", "LKOH", "YDEX", "T"]
    for day_index in range(40):
        day = (date(2026, 7, 1) + timedelta(days=day_index)).isoformat()
        for row_index in range(10):
            success = row_index < 9
            rows.append(
                {
                    "row_id": f"rule-{day_index}-{row_index}",
                    "ticker": tickers[row_index % len(tickers)],
                    "trading_day": day,
                    "frontier_decision": "up",
                    "frontier_decision_relation": "direct",
                    "signal_type": "price_jump",
                    "horizon_seconds": "300",
                    "session_bucket": "1",
                    "volatility_bucket": "high",
                    "consolidation_bucket": "compressed",
                    "liquidity_bucket": "liquid",
                    "signal_count_bucket": "single",
                    "combo_key_300s": "price_jump",
                    "frontier_confidence": "0.92",
                    "max_confidence": "0.92",
                    "frontier_success": "1" if success else "0",
                    "frontier_result_bps": "15.0" if success else "-5.0",
                }
            )
        for row_index in range(5):
            rows.append(
                {
                    "row_id": f"noise-{day_index}-{row_index}",
                    "ticker": tickers[row_index % len(tickers)],
                    "trading_day": day,
                    "frontier_decision": "down",
                    "frontier_decision_relation": "inverse",
                    "signal_type": "volume_spike",
                    "horizon_seconds": "60",
                    "session_bucket": "3",
                    "volatility_bucket": "medium",
                    "consolidation_bucket": "mixed",
                    "liquidity_bucket": "medium",
                    "signal_count_bucket": "cluster_2_3",
                    "combo_key_300s": "volume_spike",
                    "frontier_confidence": "0.20",
                    "max_confidence": "0.20",
                    "frontier_success": "0",
                    "frontier_result_bps": "-10.0",
                }
            )

    candidates = miner.mine_selective_rules(
        rows,
        min_discovery_rows=50,
        min_discovery_success_rate=0.80,
        max_terms=2,
        accepted_min_rows=100,
        accepted_min_sessions=10,
    )
    accepted = [row for row in candidates if row["accepted_shadow"]]

    assert accepted
    assert any("signal_type=price_jump" in row["rule"] for row in accepted)
    assert accepted[0]["evaluation_success_rate"] >= 0.90
    assert accepted[0]["evaluation_wilson_lower_95"] >= 0.75
    assert accepted[0]["temporal_supported"] is True


def test_selective_rule_miner_rejects_aggregate_pass_with_weak_late_block() -> None:
    miner = _load_script("research_mine_selective_rules_temporal", "research_mine_selective_rules.py")
    rows: list[dict[str, object]] = []
    tickers = ["SBER", "GAZP", "LKOH", "YDEX", "T"]
    for day_index in range(40):
        day = (date(2026, 7, 1) + timedelta(days=day_index)).isoformat()
        for row_index in range(10):
            # First half is discovery and looks perfect. In the later evaluation
            # half, one full temporal block is weak even though the aggregate
            # evaluation rate remains above 90%.
            if day_index < 20:
                success = True
            elif 20 <= day_index < 24:
                success = row_index < 7
            else:
                success = True
            rows.append(
                {
                    "row_id": f"temporal-{day_index}-{row_index}",
                    "ticker": tickers[row_index % len(tickers)],
                    "trading_day": day,
                    "frontier_decision": "up",
                    "frontier_decision_relation": "direct",
                    "signal_type": "price_jump",
                    "horizon_seconds": "300",
                    "session_bucket": "1",
                    "volatility_bucket": "high",
                    "consolidation_bucket": "compressed",
                    "liquidity_bucket": "liquid",
                    "signal_count_bucket": "single",
                    "combo_key_300s": "price_jump",
                    "frontier_confidence": "0.92",
                    "max_confidence": "0.92",
                    "frontier_success": "1" if success else "0",
                    "frontier_result_bps": "15.0" if success else "-5.0",
                }
            )

    candidates = miner.mine_selective_rules(
        rows,
        min_discovery_rows=50,
        min_discovery_success_rate=0.80,
        max_terms=2,
        accepted_min_rows=150,
        accepted_min_sessions=10,
    )
    target = next(row for row in candidates if "signal_type=price_jump" in row["rule"])

    assert target["evaluation_success_rate"] >= 0.90
    assert target["evaluation_wilson_lower_95"] >= 0.75
    assert target["temporal_supported"] is False
    assert target["temporal_weak_blocks"] >= 1
    assert target["accepted_shadow"] is False
    assert "temporal_instability" in target["blocking_reasons"]


def test_precision_scout_miner_accepts_deeper_stable_state() -> None:
    miner = _load_script("research_mine_precision_scout", "research_mine_selective_rules.py")
    rows: list[dict[str, object]] = []
    tickers = ["SBER", "GAZP", "LKOH", "YDEX", "T"]
    for day_index in range(40):
        day = (date(2026, 7, 1) + timedelta(days=day_index)).isoformat()
        for row_index in range(10):
            success = row_index < 9
            rows.append(
                {
                    "row_id": f"deep-{day_index}-{row_index}",
                    "ticker": tickers[row_index % len(tickers)],
                    "trading_day": day,
                    "frontier_decision": "down",
                    "frontier_decision_relation": "inverse",
                    "signal_type": "price_jump",
                    "horizon_seconds": "1800",
                    "session_bucket": "3",
                    "volatility_bucket": "high",
                    "consolidation_bucket": "compressed",
                    "liquidity_bucket": "liquid",
                    "signal_count_bucket": "cluster_2_3",
                    "combo_key_300s": "price_jump+volume_spike",
                    "frontier_confidence": "0.94",
                    "max_confidence": "0.94",
                    "frontier_success": "1" if success else "0",
                    "frontier_result_bps": "18.0" if success else "-4.0",
                }
            )
        for row_index in range(6):
            rows.append(
                {
                    "row_id": f"scout-noise-{day_index}-{row_index}",
                    "ticker": tickers[row_index % len(tickers)],
                    "trading_day": day,
                    "frontier_decision": "up",
                    "frontier_decision_relation": "direct",
                    "signal_type": "volume_spike",
                    "horizon_seconds": "300",
                    "session_bucket": "1",
                    "volatility_bucket": "medium",
                    "consolidation_bucket": "mixed",
                    "liquidity_bucket": "medium",
                    "signal_count_bucket": "single",
                    "combo_key_300s": "volume_spike",
                    "frontier_confidence": "0.30",
                    "max_confidence": "0.30",
                    "frontier_success": "0",
                    "frontier_result_bps": "-9.0",
                }
            )

    candidates = miner.mine_precision_scout_rules(
        rows,
        min_discovery_rows=20,
        min_discovery_success_rate=0.80,
        max_terms=4,
        beam_width=50,
        accepted_min_rows=100,
        accepted_min_sessions=10,
    )
    accepted = [row for row in candidates if row["accepted_shadow"]]

    assert accepted
    assert all("max_confidence" not in row["rule"] for row in candidates)
    assert miner._canonical_atom("max_confidence>=0.9") == "model_confidence>=0.9"
    assert accepted[0]["evaluation_success_rate"] >= 0.90
    assert accepted[0]["dominant_decision"] == "down"
    assert accepted[0]["dominant_relation"] == "inverse"
    assert accepted[0]["additional_successes_needed_for_90pct_at_min_rows"] == 0


def test_precision_scout_deduplicates_same_support_by_product_preference() -> None:
    miner = _load_script("research_mine_precision_scout_dedup", "research_mine_selective_rules.py")

    rows = miner._deduplicate_precision_scout_rows(
        [
            {
                "_evaluation_support_id": "same",
                "dominant_decision": "down",
                "dominant_relation": "inverse",
                "accepted_shadow": False,
                "discovery_gate_passed": False,
                "status": "discovery_weak",
                "evaluation_success_rate": 0.90,
                "evaluation_wilson_lower_95": 0.60,
                "evaluation_sessions": 10,
                "evaluation_rows": 20,
                "terms": 4,
                "evaluation_mean_result_bps": -5.0,
                "rule": "apparently accurate but negative after costs",
            },
            {
                "_evaluation_support_id": "same",
                "dominant_decision": "down",
                "dominant_relation": "inverse",
                "accepted_shadow": False,
                "discovery_gate_passed": True,
                "status": "watch_only",
                "evaluation_success_rate": 0.70,
                "evaluation_wilson_lower_95": 0.55,
                "evaluation_sessions": 10,
                "evaluation_rows": 20,
                "terms": 3,
                "evaluation_mean_result_bps": 8.0,
                "rule": "lower hit rate but positive after costs",
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0]["rule"] == "lower hit rate but positive after costs"


def test_precision_scout_future_success_requirement_is_explicit() -> None:
    miner = _load_script("research_mine_precision_scout_future_requirement", "research_mine_selective_rules.py")

    requirement = miner._future_success_requirement(35, 52, min_rows=300)

    assert requirement["current_successes_needed_for_90pct"] == 12
    assert requirement["additional_successes_needed_for_90pct_at_min_rows"] == 235
    assert requirement["allowed_future_failures_for_90pct_at_min_rows"] == 13
    assert requirement["required_future_success_rate_for_90pct_at_min_rows"] == pytest.approx(235 / 248)
    assert requirement["can_reach_90pct_at_min_rows"] is True
    assert miner._proof_viability(requirement)[0] == "severe_forward_validation_required"

    impossible = miner._future_success_requirement(53, 91, min_rows=300)

    assert impossible["required_future_success_rate_for_90pct_at_min_rows"] > 1
    assert impossible["can_reach_90pct_at_min_rows"] is False
    assert miner._proof_viability(impossible)[0] == "impossible_at_min_rows"
    assert (
        miner._candidate_status(
            accepted_shadow=False,
            discovery_gate_passed=True,
            evaluation_rows=91,
            proof_viability="impossible_at_min_rows",
        )
        == "retired_90_impossible"
    )


def test_precision_scout_summary_counts_viability_and_next_action() -> None:
    miner = _load_script("research_mine_precision_scout_summary", "research_mine_selective_rules.py")

    rows = [
        {
            "proof_viability": "severe_forward_validation_required",
            "proof_next_action": "forward_holdout_candidate",
            "status": "watch_only",
            "accepted_shadow": False,
            "evaluation_mean_result_bps": 5.0,
            "can_reach_90pct_at_min_rows": True,
            "evaluation_success_rate": 0.7,
            "evaluation_wilson_lower_95": 0.55,
        },
        {
            "proof_viability": "impossible_at_min_rows",
            "proof_next_action": "retire_for_90pct_min_row_gate",
            "status": "discovery_weak",
            "accepted_shadow": False,
            "evaluation_mean_result_bps": -1.0,
            "can_reach_90pct_at_min_rows": False,
            "evaluation_success_rate": 0.6,
            "evaluation_wilson_lower_95": 0.4,
        },
    ]

    summary = miner.summarize_precision_scout_rows(rows)

    assert summary["candidate_rows"] == 2
    assert summary["watch_only"] == 1
    assert summary["positive_result_rows"] == 1
    assert summary["can_reach_90pct_at_min_rows"] == 1
    assert summary["proof_viability_counts"]["impossible_at_min_rows"] == 1
    assert summary["next_action_counts"]["forward_holdout_candidate"] == 1


def test_collection_schedule_status_ready_but_not_loaded(tmp_path: Path) -> None:
    schedule_status = _load_script(
        "research_collection_schedule_status_ready",
        "research_collection_schedule_status.py",
    )
    shell = tmp_path / "run-liquidity-collector.sh"
    shell.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    shell.chmod(0o755)
    cron = tmp_path / "liquidity-collector.cron"
    cron.write_text("5 10 * * 1-5 /tmp/run-liquidity-collector.sh\n", encoding="utf-8")
    plist = tmp_path / "com.investment-signals.research-liquidity-collector.plist"
    plist.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.investment-signals.research-liquidity-collector</string>
</dict>
</plist>
""",
        encoding="utf-8",
    )
    systemd_service = tmp_path / "investment-signals-research-liquidity-collector.service"
    systemd_service.write_text("[Service]\nType=oneshot\nExecStart=/tmp/run-liquidity-collector.sh\n", encoding="utf-8")
    systemd_timer = tmp_path / "investment-signals-research-liquidity-collector.timer"
    systemd_timer.write_text("[Timer]\nOnCalendar=Mon..Fri *-*-* 10:05:00 Europe/Moscow\n", encoding="utf-8")
    plan_path = tmp_path / "collection-plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "shell_script": str(shell),
                    "cron_file": str(cron),
                    "launchd_plist": str(plist),
                    "systemd_service": str(systemd_service),
                    "systemd_timer": str(systemd_timer),
                    "log_path": str(tmp_path / "collector.log"),
                    "recommended_start_moscow": "2026-07-17T10:22:00+03:00",
                    "scheduled_start_moscow": "2026-07-17T10:05:00+03:00",
                    "weekday_start_local": "10:05",
                    "launchctl_load_command": "launchctl load -w plist",
                    "launchctl_unload_command": "launchctl unload -w plist",
                    "systemd_install_user_command": "systemctl --user enable --now investment-signals-research-liquidity-collector.timer",
                    "systemd_disable_user_command": "systemctl --user disable --now investment-signals-research-liquidity-collector.timer",
                }
            }
        ),
        encoding="utf-8",
    )

    status = schedule_status.build_schedule_status(
        collection_plan_path=plan_path,
        now=datetime(2026, 7, 17, 9, 0, tzinfo=schedule_status.MOSCOW),
        launchctl_output="123\t0\tcom.example.other-job\n",
    )

    assert status["status"] == "ready_not_loaded"
    assert status["next_action"] == "load_scheduler_before_recommended_start"
    assert status["schedule_files_ok"] is True
    assert status["launchd_loaded"] is False
    assert status["launchd_label"] == "com.investment-signals.research-liquidity-collector"
    assert status["systemd_service"]["ok"] is True
    assert status["systemd_timer"]["ok"] is True
    assert "systemctl --user enable" in status["systemd_install_user_command"]
    assert status["recommended_start_moscow"] == "2026-07-17T10:22:00+03:00"
    assert status["scheduled_start_moscow"] == "2026-07-17T10:05:00+03:00"
    assert status["weekday_start_local"] == "10:05"


def test_collection_schedule_status_uses_active_systemd_timer(tmp_path: Path) -> None:
    schedule_status = _load_script(
        "research_collection_schedule_status_systemd",
        "research_collection_schedule_status.py",
    )
    shell = tmp_path / "run-liquidity-collector.sh"
    shell.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    shell.chmod(0o755)
    cron = tmp_path / "liquidity-collector.cron"
    cron.write_text("5 10 * * 1-5 /tmp/run-liquidity-collector.sh\n", encoding="utf-8")
    plist = tmp_path / "com.investment-signals.research-liquidity-collector.plist"
    plist.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.investment-signals.research-liquidity-collector</string>
</dict>
</plist>
""",
        encoding="utf-8",
    )
    systemd_service = tmp_path / "investment-signals-research-liquidity-collector.service"
    systemd_service.write_text("[Service]\nType=oneshot\nExecStart=/tmp/run-liquidity-collector.sh\n", encoding="utf-8")
    systemd_timer = tmp_path / "investment-signals-research-liquidity-collector.timer"
    systemd_timer.write_text("[Timer]\nOnCalendar=Mon..Fri *-*-* 10:05:00 Europe/Moscow\n", encoding="utf-8")
    plan_path = tmp_path / "collection-plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "shell_script": str(shell),
                    "cron_file": str(cron),
                    "launchd_plist": str(plist),
                    "systemd_service": str(systemd_service),
                    "systemd_timer": str(systemd_timer),
                    "log_path": str(tmp_path / "collector.log"),
                    "recommended_start_moscow": "2026-07-17T10:22:00+03:00",
                    "scheduled_start_moscow": "2026-07-17T10:05:00+03:00",
                    "weekday_start_local": "10:05",
                }
            }
        ),
        encoding="utf-8",
    )

    status = schedule_status.build_schedule_status(
        collection_plan_path=plan_path,
        now=datetime(2026, 7, 17, 9, 0, tzinfo=schedule_status.MOSCOW),
        launchctl_output="",
        systemctl_output="active",
    )

    assert status["status"] == "ready_loaded"
    assert status["scheduler_loaded"] is True
    assert status["systemd_loaded"] is True
    assert status["launchd_loaded"] is None
    assert status["next_action"] == "wait_for_scheduled_collection"


def test_collection_schedule_status_invalid_when_files_missing(tmp_path: Path) -> None:
    schedule_status = _load_script(
        "research_collection_schedule_status_invalid",
        "research_collection_schedule_status.py",
    )
    plan_path = tmp_path / "collection-plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "shell_script": str(tmp_path / "missing.sh"),
                    "cron_file": str(tmp_path / "missing.cron"),
                    "launchd_plist": str(tmp_path / "missing.plist"),
                    "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                }
            }
        ),
        encoding="utf-8",
    )

    status = schedule_status.build_schedule_status(
        collection_plan_path=plan_path,
        now=datetime(2026, 7, 17, 9, 0, tzinfo=schedule_status.MOSCOW),
        launchctl_output="",
    )

    assert status["status"] == "invalid"
    assert status["next_action"] == "fix_schedule_files"
    assert status["schedule_files_ok"] is False


def test_selection_90_report_counts_skips_and_missing_successes(tmp_path: Path) -> None:
    reporter = _load_script("research_report_90_selection_test", "research_report_90_selection.py")
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text(
        json.dumps({"run_id": "run-1", "dataset_rows": 1000, "validation_sessions": 31}),
        encoding="utf-8",
    )
    (run_dir / "decision-policy.json").write_text(
        json.dumps({"status": "disabled", "reason_code": "no_confidence_threshold", "product_claim_allowed": False}),
        encoding="utf-8",
    )
    (run_dir / "confidence-threshold-report.csv").write_text(
        "\n".join(
            [
                "threshold,eligible_rows,selected_rows,skipped_rows,up_decisions,down_decisions,inverse_decisions,success_count,success_rate,wilson_lower_95,sessions,mean_selected_result_bps,accepted_research",
                "0.5,1000,100,900,70,30,12,80,0.8,0.71,31,4.0,False",
                "0.9,1000,20,980,4,16,16,18,0.9,0.69,10,8.0,False",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "confidence-reliability-report.csv").write_text(
        "\n".join(
            [
                "scope,rule,nominal_action,min_confidence,max_confidence,selected_rows,sessions,success_count,observed_success_rate,wilson_lower_95,mean_model_confidence,mean_result_bps,shadow_allowed,product_90_allowed,safe_runtime_action",
                "confidence_band,strong_signal,candidate,0.9,1.01,20,10,18,0.9,0.69,0.93,8.0,False,False,skip",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "precision-scout-candidates.csv").write_text(
        "\n".join(
            [
                "rule,evaluation_rows,evaluation_success_rate,evaluation_wilson_lower_95,evaluation_mean_result_bps,can_reach_90pct_at_min_rows,proof_viability,proof_next_action,status,additional_successes_needed_for_90pct_at_min_rows",
                "a,20,0.9,0.69,8.0,True,severe_forward_validation_required,collect_or_refine_features,discovery_weak,252",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "selective-frontier.csv").write_text(
        "\n".join(
            [
                "rule,selected_rows,inverse_decisions,success_rate,wilson_lower_95,mean_selected_result_bps",
                "decision_relation=inverse | signal_type=price_jump,20,20,0.65,0.43,5.0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    report = reporter.build_selection_report(run_dir)

    assert report["conclusion"] == "not_ready_keep_default_skip"
    assert report["threshold_rows"][0]["skipped_share"] == pytest.approx(0.9)
    assert report["threshold_rows"][0]["up_decisions"] == 70
    assert report["threshold_rows"][0]["down_decisions"] == 30
    assert report["threshold_rows"][1]["inverse_decisions"] == 16
    assert report["threshold_rows"][0]["missing_successes_to_90_current_rows"] == 10
    assert report["threshold_rows"][1]["passes_observed_90"] is True
    assert report["threshold_rows"][1]["passes_sample_gate"] is False
    assert report["confidence_band_rows"][0]["label_ru"] == "сильный сигнал"
    assert report["confidence_band_rows"][0]["product_90_allowed"] is False
    assert report["confidence_band_rows"][0]["safe_runtime_action"] == "skip"
    assert report["inverse_hypotheses"]["rows"] == 1


def test_selection_90_report_writes_markdown(tmp_path: Path) -> None:
    reporter = _load_script("research_report_90_selection_write", "research_report_90_selection.py")
    run_dir = tmp_path / "run"
    output_dir = tmp_path / "out"
    run_dir.mkdir()
    (run_dir / "model-results.json").write_text("{}", encoding="utf-8")
    (run_dir / "decision-policy.json").write_text("{}", encoding="utf-8")
    (run_dir / "confidence-threshold-report.csv").write_text(
        "\n".join(
            [
                "threshold,eligible_rows,selected_rows,skipped_rows,up_decisions,down_decisions,inverse_decisions,success_count,success_rate,wilson_lower_95,sessions,mean_selected_result_bps,accepted_research",
                "0.9,1000,20,980,4,16,16,18,0.9,0.69,10,8.0,False",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (run_dir / "confidence-reliability-report.csv").write_text(
        "scope,rule,nominal_action,min_confidence,max_confidence,selected_rows,sessions,success_count,observed_success_rate,wilson_lower_95,mean_model_confidence,mean_result_bps,shadow_allowed,product_90_allowed,safe_runtime_action\n",
        encoding="utf-8",
    )
    (run_dir / "precision-scout-candidates.csv").write_text("", encoding="utf-8")
    (run_dir / "selective-frontier.csv").write_text("", encoding="utf-8")

    reporter.write_selection_report(run_dir, output_dir)

    assert (output_dir / "selection-90-report.json").exists()
    text = (output_dir / "selection-90-report.md").read_text(encoding="utf-8")
    assert "Точность против количества сигналов" in text
    assert "| Порог | Осталось | Рост | Снижение | Обратных | Пропущено |" in text
    assert "| 0.90 | 20 | 4 | 16 | 16 | 980 (98.00%) | 18 | 90.00% | 69.00% | 10 | 0 |" in text
    assert "пропустить, недостаточно уверенности" in text


def test_objective_contract_audit_accepts_mechanism_without_evidence() -> None:
    auditor = _load_script(
        "research_audit_90_objective_contract_ready",
        "research_audit_90_objective_contract.py",
    )
    goal_audit = {
        "checks": [
            {"id": "safe_default_skip", "status": "passed"},
            {"id": "three_way_decision_export", "status": "passed"},
            {"id": "market_state_search", "status": "passed"},
            {"id": "inverse_hypothesis_search", "status": "passed"},
        ]
    }
    selection_report = {
        "product_claim_allowed": False,
        "target": {"minimum_rows": 300, "minimum_sessions": 30, "minimum_lower_bound": 0.75},
        "threshold_rows": [{"threshold": 0.4}],
        "confidence_band_rows": [
            {"band": "skip", "label_ru": "пропустить, недостаточно уверенности", "safe_runtime_action": "skip"},
            {"band": "weak_observation", "label_ru": "слабое наблюдение", "safe_runtime_action": "skip"},
            {"band": "working_hypothesis", "label_ru": "рабочая гипотеза", "safe_runtime_action": "skip"},
            {"band": "strong_signal", "label_ru": "сильный сигнал", "safe_runtime_action": "skip"},
        ],
        "inverse_hypotheses": {"rows": 3},
    }

    audit = auditor.build_objective_contract_audit(
        selection_report=selection_report,
        signal_status={"product_claim_allowed": False},
        goal_audit=goal_audit,
        schedule_status={"status": "ready_loaded"},
        feature_coverage={
            "ready": True,
            "value_status": "waiting_for_microstructure_values",
            "microstructure_value_coverage": {"ready": False, "orderbook_available_rows": 0},
        },
        gap_audit={
            "status": "not_ready",
            "target": {"minimum_rows": 300, "minimum_sessions": 30, "minimum_lower_bound": 0.75},
            "summary": {"candidate_rows": 10, "accepted_shadow": 0, "best_success_rate": 0.64},
            "rows": [{"rule": "x"}],
        },
    )

    assert audit["status"] == "mechanism_ready_waiting_for_evidence"
    assert audit["mechanism_ready"] is True
    assert audit["evidence_ready"] is False
    assert audit["product_claim_allowed"] is False
    gap_check = next(check for check in audit["checks"] if check["id"] == "gap_to_90_audit")
    assert gap_check["status"] == "passed"


def test_objective_contract_audit_requires_gap_audit() -> None:
    auditor = _load_script(
        "research_audit_90_objective_contract_gap_required",
        "research_audit_90_objective_contract.py",
    )

    audit = auditor.build_objective_contract_audit(
        selection_report={
            "product_claim_allowed": False,
            "target": {"minimum_rows": 300, "minimum_sessions": 30, "minimum_lower_bound": 0.75},
            "threshold_rows": [{"threshold": 0.4}],
            "confidence_band_rows": [
                {"band": "skip", "label_ru": "пропустить, недостаточно уверенности", "safe_runtime_action": "skip"},
                {"band": "weak_observation", "label_ru": "слабое наблюдение", "safe_runtime_action": "skip"},
                {"band": "working_hypothesis", "label_ru": "рабочая гипотеза", "safe_runtime_action": "skip"},
                {"band": "strong_signal", "label_ru": "сильный сигнал", "safe_runtime_action": "skip"},
            ],
            "inverse_hypotheses": {"rows": 3},
        },
        signal_status={"product_claim_allowed": False},
        goal_audit={
            "checks": [
                {"id": "safe_default_skip", "status": "passed"},
                {"id": "three_way_decision_export", "status": "passed"},
                {"id": "market_state_search", "status": "passed"},
                {"id": "inverse_hypothesis_search", "status": "passed"},
            ]
        },
        schedule_status={"status": "ready_loaded"},
        feature_coverage={
            "ready": True,
            "value_status": "waiting_for_microstructure_values",
            "microstructure_value_coverage": {"ready": False},
        },
    )

    gap_check = next(check for check in audit["checks"] if check["id"] == "gap_to_90_audit")
    assert audit["status"] == "mechanism_incomplete"
    assert gap_check["status"] == "failed"


def test_objective_contract_audit_fails_missing_confidence_band() -> None:
    auditor = _load_script(
        "research_audit_90_objective_contract_missing_band",
        "research_audit_90_objective_contract.py",
    )

    audit = auditor.build_objective_contract_audit(
        selection_report={
            "target": {"minimum_rows": 300, "minimum_sessions": 30, "minimum_lower_bound": 0.75},
            "threshold_rows": [{"threshold": 0.4}],
            "confidence_band_rows": [
                {"band": "skip", "label_ru": "пропустить, недостаточно уверенности", "safe_runtime_action": "skip"}
            ],
            "inverse_hypotheses": {"rows": 1},
        },
        signal_status={"product_claim_allowed": False},
        goal_audit={"checks": [{"id": "safe_default_skip", "status": "passed"}]},
        schedule_status={"status": "ready_loaded"},
        feature_coverage={
            "ready": False,
            "value_status": "waiting_for_microstructure_values",
            "microstructure_value_coverage": {"ready": False},
        },
        gap_audit={
            "status": "not_ready",
            "target": {"minimum_rows": 300, "minimum_sessions": 30, "minimum_lower_bound": 0.75},
            "summary": {"candidate_rows": 1, "accepted_shadow": 0, "best_success_rate": 0.4},
            "rows": [{"rule": "x"}],
        },
    )

    product_band_check = next(check for check in audit["checks"] if check["id"] == "product_confidence_bands")
    assert audit["status"] == "mechanism_incomplete"
    assert product_band_check["status"] == "failed"
    assert "strong_signal" in product_band_check["observed"]["missing_bands"]


def test_feature_coverage_audit_passes_required_objective_features() -> None:
    auditor = _load_script("research_audit_90_feature_coverage_pass", "research_audit_90_feature_coverage.py")
    dataset_columns = {
        "session_bucket",
        "volume_z_score",
        "range_z_score",
        "event_volume_ratio",
        "event_range_ratio",
        "candle_range_bps",
        "day_volatility_bps",
        "day_volatility_quantile",
        "ticker_volatility_quantile",
        "day_volume_quantile",
        "ticker_volume_quantile",
        "ticker_mean_daily_volume",
        "pre_trend_bucket",
        "pre_trend_strength_bucket",
        "event_trend_relation",
        "decision_trend_relation",
        "consolidation_bucket",
        "z_score",
        "event_strength_to_volatility",
        "baseline_volatility_bps",
        "event_to_pre_volatility_60m",
        "event_to_pre_range_60m",
        "decision_relation",
        "frontier_decision_relation",
        "reverse_directional_bps",
        "feature_max_observed_at",
        "feature_leakage_flag",
        "recent_signal_count_60s",
        "recent_signal_count_300s",
        "recent_signal_count_900s",
    }
    for window in (5, 15, 30, 60):
        dataset_columns.update(
            {
                f"pre_return_bps_{window}m",
                f"pre_volatility_bps_{window}m",
                f"pre_range_bps_{window}m",
                f"pre_consolidation_score_{window}m",
            }
        )

    audit = auditor.build_feature_coverage_audit(
        dataset_columns=dataset_columns,
        decision_audit_columns=set(),
        threshold_columns={"inverse_decisions"},
        precision_scout_columns=set(),
        value_profile={"columns": {"orderbook_available": {"true_rows": 300}}},
    )

    assert audit["status"] == "ready"
    assert audit["ready"] is True
    assert audit["value_status"] == "microstructure_values_ready"
    assert audit["summary"]["failed"] == 0


def test_feature_coverage_audit_fails_missing_pre_signal_windows() -> None:
    auditor = _load_script("research_audit_90_feature_coverage_fail", "research_audit_90_feature_coverage.py")

    audit = auditor.build_feature_coverage_audit(
        dataset_columns={"pre_return_bps_5m", "pre_volatility_bps_5m", "pre_range_bps_5m"},
        decision_audit_columns=set(),
        threshold_columns=set(),
        precision_scout_columns=set(),
    )

    pre_signal = next(check for check in audit["checks"] if check["id"] == "pre_signal_windows")
    assert audit["status"] == "missing_features"
    assert pre_signal["status"] == "failed"
    assert audit["value_status"] == "waiting_for_microstructure_values"


def test_liquidity_live_status_waits_before_start(tmp_path: Path) -> None:
    live_status = _load_script(
        "research_liquidity_collection_live_status_waiting",
        "research_liquidity_collection_live_status.py",
    )
    plan_path = tmp_path / "collection-plan.json"
    schedule_path = tmp_path / "schedule-status.json"
    cache_dir = tmp_path / "orderbooks"
    cache_dir.mkdir()
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                    "recommended_end_moscow": "2026-07-17T18:05:00+03:00",
                    "log_path": str(tmp_path / "collector.log"),
                }
            }
        ),
        encoding="utf-8",
    )
    schedule_path.write_text(json.dumps({"launchd_loaded": True}), encoding="utf-8")

    status = live_status.build_live_status(
        collection_plan_path=plan_path,
        schedule_status_path=schedule_path,
        orderbook_cache_dir=cache_dir,
        process_output="",
        now=datetime(2026, 7, 17, 9, 30, tzinfo=live_status.MOSCOW),
    )

    assert status["status"] == "waiting_for_start"
    assert status["next_action"] == "wait_for_scheduled_start"
    assert status["recommended_start_has_passed"] is False


def test_liquidity_live_status_accepts_systemd_scheduler(tmp_path: Path) -> None:
    live_status = _load_script(
        "research_liquidity_collection_live_status_systemd",
        "research_liquidity_collection_live_status.py",
    )
    plan_path = tmp_path / "collection-plan.json"
    schedule_path = tmp_path / "schedule-status.json"
    cache_dir = tmp_path / "orderbooks"
    cache_dir.mkdir()
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                    "recommended_end_moscow": "2026-07-17T18:05:00+03:00",
                    "log_path": str(tmp_path / "collector.log"),
                }
            }
        ),
        encoding="utf-8",
    )
    schedule_path.write_text(
        json.dumps({"launchd_loaded": None, "systemd_loaded": True, "scheduler_loaded": True}),
        encoding="utf-8",
    )

    status = live_status.build_live_status(
        collection_plan_path=plan_path,
        schedule_status_path=schedule_path,
        orderbook_cache_dir=cache_dir,
        process_output="",
        now=datetime(2026, 7, 17, 9, 30, tzinfo=live_status.MOSCOW),
    )

    assert status["status"] == "waiting_for_start"
    assert status["scheduler_loaded"] is True
    assert status["systemd_loaded"] is True
    assert status["launchd_loaded"] is None


def test_liquidity_live_status_detects_running_collector(tmp_path: Path) -> None:
    live_status = _load_script(
        "research_liquidity_collection_live_status_running",
        "research_liquidity_collection_live_status.py",
    )
    plan_path = tmp_path / "collection-plan.json"
    schedule_path = tmp_path / "schedule-status.json"
    cache_dir = tmp_path / "orderbooks"
    cache_dir.mkdir()
    (cache_dir / "manifest.json").write_text(
        json.dumps({"quality": {"partition_count": 1, "rows_by_partition": {"SBER/2026-07-17": 12}}}),
        encoding="utf-8",
    )
    (cache_dir / "collection-progress.json").write_text(
        json.dumps(
            {
                "status": "running",
                "updated_at": "2026-07-17T07:06:00+00:00",
                "progress": {
                    "completed_samples": 4,
                    "target_samples": 20,
                    "completed_share": 0.2,
                    "rows_collected": 20,
                    "rows_flushed": 10,
                    "unflushed_rows": 10,
                    "failures": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    plan_path.write_text(
        json.dumps(
            {
                "schedule": {
                    "recommended_start_moscow": "2026-07-17T10:05:00+03:00",
                    "recommended_end_moscow": "2026-07-17T18:05:00+03:00",
                    "log_path": str(tmp_path / "collector.log"),
                }
            }
        ),
        encoding="utf-8",
    )
    schedule_path.write_text(json.dumps({"launchd_loaded": True}), encoding="utf-8")

    status = live_status.build_live_status(
        collection_plan_path=plan_path,
        schedule_status_path=schedule_path,
        orderbook_cache_dir=cache_dir,
        process_output="123 uv run --extra research python scripts/research_update_liquidity_holdout.py\n",
        now=datetime(2026, 7, 17, 10, 6, tzinfo=live_status.MOSCOW),
    )

    assert status["status"] == "running"
    assert status["next_action"] == "watch_log_and_cache_growth"
    assert status["manifest"]["row_count"] == 12
    assert status["progress"]["status"] == "running"
    assert status["progress"]["completed_samples"] == 4
    assert status["progress"]["rows_flushed"] == 10
    assert status["running_collectors"][0]["pid"] == "123"


def test_refresh_90_reports_resolves_baseline_when_liquidity_not_ready(tmp_path: Path) -> None:
    refresher = _load_script("research_refresh_90_reports_baseline", "research_refresh_90_reports.py")
    holdout = tmp_path / "holdout"
    holdout.mkdir()
    (holdout / "pipeline-result.json").write_text(
        json.dumps({"status": "waiting_for_data"}),
        encoding="utf-8",
    )

    current = refresher.resolve_current_run(
        holdout_dir=holdout,
        fallback_run_dir=Path("baseline-run"),
        fallback_dataset=Path("baseline.parquet"),
    )

    assert current["source"] == "baseline"
    assert current["run_dir"] == "baseline-run"
    assert current["dataset"] == "baseline.parquet"
    assert current["fallback_reason"] == "liquidity_run_not_ready"


def test_refresh_90_reports_resolves_liquidity_run_when_ready(tmp_path: Path) -> None:
    refresher = _load_script("research_refresh_90_reports_liquidity", "research_refresh_90_reports.py")
    holdout = tmp_path / "holdout"
    run_dir = holdout / "runs" / "abc123"
    run_dir.mkdir(parents=True)
    dataset = holdout / "signal_price_prediction_liquidity.parquet"
    dataset.write_text("placeholder", encoding="utf-8")
    (holdout / "pipeline-result.json").write_text(
        json.dumps({"status": "ok", "training": {"run_id": "abc123"}}),
        encoding="utf-8",
    )

    current = refresher.resolve_current_run(
        holdout_dir=holdout,
        fallback_run_dir=Path("baseline-run"),
        fallback_dataset=Path("baseline.parquet"),
    )

    assert current["source"] == "liquidity_holdout"
    assert current["run_dir"] == str(run_dir)
    assert current["dataset"] == str(dataset)
    assert current["run_id"] == "abc123"


def test_passive_orderbook_collector_requires_service_check(tmp_path: Path) -> None:
    passive = _load_script(
        "research_passive_orderbook_collector_requires_check",
        "research_passive_orderbook_collector.py",
    )
    args = passive.parse_args(
        [
            "--output-dir",
            str(tmp_path / "passive"),
            "--cache-dir",
            str(tmp_path / "orderbooks"),
            "--tickers",
            "SBER",
        ]
    )
    calls: list[list[str]] = []

    result = passive.run_passive_collection(
        args,
        runner=lambda command: calls.append(list(command)) or {"status": "ok"},
    )

    assert result["status"] == "skipped"
    assert result["reason_code"] == "service_check_required"
    assert calls == []
    assert (tmp_path / "passive" / "passive-orderbook-result.json").exists()
    assert "токены" in (tmp_path / "passive" / "passive-orderbook-report.md").read_text(encoding="utf-8")


def test_passive_orderbook_collector_skips_when_service_is_unavailable(tmp_path: Path) -> None:
    passive = _load_script(
        "research_passive_orderbook_collector_service_unavailable",
        "research_passive_orderbook_collector.py",
    )
    args = passive.parse_args(
        [
            "--service-health-url",
            "http://127.0.0.1:18080/api/v1/system/status",
            "--service-process-marker",
            "investment-signals-pro",
            "--output-dir",
            str(tmp_path / "passive"),
        ]
    )
    calls: list[list[str]] = []

    result = passive.run_passive_collection(
        args,
        runner=lambda command: calls.append(list(command)) or {"status": "ok"},
        process_output="",
        health_checker=lambda _url, _timeout: False,
    )

    assert result["status"] == "skipped"
    assert result["reason_code"] == "service_not_running"
    assert result["checks"]["health_urls"][0]["available"] is False
    assert result["checks"]["process_markers"][0]["available"] is False
    assert calls == []


def test_passive_orderbook_collector_ignores_own_process_marker(tmp_path: Path) -> None:
    passive = _load_script(
        "research_passive_orderbook_collector_own_marker",
        "research_passive_orderbook_collector.py",
    )
    args = passive.parse_args(
        [
            "--service-process-marker",
            "tinvest-api",
            "--output-dir",
            str(tmp_path / "passive"),
        ]
    )
    process_output = (
        "123 python scripts/research_passive_orderbook_collector.py "
        "--service-process-marker tinvest-api\n"
    )
    calls: list[list[str]] = []

    result = passive.run_passive_collection(
        args,
        runner=lambda command: calls.append(list(command)) or {"status": "ok"},
        process_output=process_output,
    )

    assert result["status"] == "skipped"
    assert result["reason_code"] == "service_not_running"
    assert result["checks"]["process_markers"][0]["available"] is False
    assert calls == []


def test_passive_orderbook_collector_runs_short_batch_when_process_is_present(tmp_path: Path) -> None:
    passive = _load_script(
        "research_passive_orderbook_collector_runs",
        "research_passive_orderbook_collector.py",
    )
    args = passive.parse_args(
        [
            "--service-process-marker",
            "investment-signals-pro",
            "--env-file",
            str(tmp_path / ".env"),
            "--cache-dir",
            str(tmp_path / "orderbooks"),
            "--output-dir",
            str(tmp_path / "passive"),
            "--tickers",
            "SBER,GAZP",
            "--samples",
            "3",
            "--interval-seconds",
            "2",
            "--flush-every-samples",
            "1",
            "--ca-cert",
            str(tmp_path / "russiantrustedca2024.pem"),
        ]
    )
    calls: list[list[str]] = []

    def runner(command: list[str]) -> dict[str, object]:
        calls.append(list(command))
        return {"status": "ok", "rows_collected": 6, "rows_flushed": 6}

    result = passive.run_passive_collection(
        args,
        runner=runner,
        process_output="123 investment-signals-pro product-api\n",
        health_checker=lambda _url, _timeout: False,
    )

    assert result["status"] == "ok"
    assert result["reason_code"] == "process_marker_available"
    assert result["collector_result"]["rows_collected"] == 6
    assert len(calls) == 1
    command = calls[0]
    assert "--samples" in command
    assert command[command.index("--samples") + 1] == "3"
    assert "--flush-every-samples" in command
    assert command[command.index("--flush-every-samples") + 1] == "1"
    assert "--ca-cert" in command
    assert "--tickers" in command
    assert command[command.index("--tickers") + 1] == "SBER,GAZP"


def test_passive_orderbook_collection_plan_writes_short_launchd_schedule(tmp_path: Path) -> None:
    planner = _load_script(
        "research_plan_passive_orderbook_collection_test",
        "research_plan_passive_orderbook_collection.py",
    )
    args = planner.parse_args(
        [
            "--output-dir",
            str(tmp_path / "plan"),
            "--working-directory",
            str(ROOT),
            "--service-health-url",
            "http://127.0.0.1:18080/api/v1/system/status",
            "--tickers",
            "SBER,GAZP",
            "--samples",
            "2",
            "--sample-interval-seconds",
            "5",
            "--schedule-interval-seconds",
            "120",
            "--ca-cert",
            str(tmp_path / "russiantrustedca2024.pem"),
        ]
    )

    plan = planner.build_plan(args)
    planner.write_plan(plan)

    assert plan["status"] == "ready"
    assert plan["mode"] == "short_passive_when_product_is_alive"
    assert plan["schedule_interval_seconds"] == 120
    assert plan["collection"]["samples_per_run"] == 2
    assert plan["service_gate"]["behavior_when_unavailable"] == "skip_without_tinvest_api_call"
    assert plan["privacy"]["tokens_persisted"] is False
    shell_script = Path(plan["artifacts"]["shell_script"])
    launchd_plist = Path(plan["artifacts"]["launchd_plist"])
    systemd_service = Path(plan["artifacts"]["systemd_service"])
    systemd_timer = Path(plan["artifacts"]["systemd_timer"])
    report = Path(plan["artifacts"]["output_dir"]) / "passive-collection-plan.md"
    assert shell_script.exists()
    assert launchd_plist.exists()
    assert systemd_service.exists()
    assert systemd_timer.exists()
    assert report.exists()
    shell_text = shell_script.read_text(encoding="utf-8")
    assert "research_passive_orderbook_collector.py" in shell_text
    assert "--samples 2" in shell_text
    assert "--ca-cert" in shell_text
    assert "OnUnitActiveSec=120s" in systemd_timer.read_text(encoding="utf-8")
    report_text = report.read_text(encoding="utf-8")
    assert "Если сервис не работает" in report_text
    assert "systemd user timer" in report_text


def test_passive_orderbook_loop_runs_repeated_short_batches(tmp_path: Path) -> None:
    loop = _load_script(
        "research_passive_orderbook_loop_repeated",
        "research_passive_orderbook_loop.py",
    )
    args = loop.parse_args(
        [
            "--iterations",
            "2",
            "--sleep-seconds",
            "1",
            "--output-dir",
            str(tmp_path / "loop"),
            "--passive-output-dir",
            str(tmp_path / "passive"),
            "--cache-dir",
            str(tmp_path / "orderbooks"),
            "--tickers",
            "SBER,GAZP",
            "--samples",
            "2",
            "--sample-interval-seconds",
            "5",
        ]
    )
    calls: list[list[str]] = []
    sleeps: list[float] = []

    def runner(command: list[str]) -> dict[str, object]:
        calls.append(list(command))
        return {
            "status": "ok",
            "reason_code": "health_url_available",
            "collector_result": {"rows_collected": 4, "rows_flushed": 4},
        }

    result = loop.run_loop(args, runner=runner, sleeper=lambda seconds: sleeps.append(seconds))

    assert result["status"] == "completed"
    assert result["iterations_completed"] == 2
    assert result["summary"]["ok_iterations"] == 2
    assert result["summary"]["rows_collected"] == 8
    assert len(calls) == 2
    assert sleeps == [1.0]
    command = calls[0]
    assert "research_passive_orderbook_collector.py" in " ".join(command)
    assert "--service-health-url" in command
    assert "--service-process-marker" in command
    assert command[command.index("--tickers") + 1] == "SBER,GAZP"
    status_path = tmp_path / "loop" / "passive-orderbook-loop-status.json"
    report_path = tmp_path / "loop" / "passive-orderbook-loop-report.md"
    assert status_path.exists()
    assert report_path.exists()
    assert "Если продукт не отвечает" in report_path.read_text(encoding="utf-8")


def test_passive_orderbook_loop_records_skipped_iterations_without_tinvest_calls(tmp_path: Path) -> None:
    loop = _load_script(
        "research_passive_orderbook_loop_skipped",
        "research_passive_orderbook_loop.py",
    )
    args = loop.parse_args(
        [
            "--iterations",
            "1",
            "--output-dir",
            str(tmp_path / "loop"),
            "--cache-dir",
            str(tmp_path / "orderbooks"),
        ]
    )

    result = loop.run_loop(
        args,
        runner=lambda _command: {
            "status": "skipped",
            "reason_code": "service_not_running",
        },
        sleeper=lambda _seconds: None,
    )

    assert result["status"] == "completed"
    assert result["summary"]["skipped_iterations"] == 1
    assert result["summary"]["rows_collected"] == 0
    encoded = json.dumps(result, ensure_ascii=False)
    assert "tokens_persisted" in encoded
    assert "secret-token" not in encoded


def _honest_state_dataset(*, weak_late_block: bool = False) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day_index in range(40):
        day = date(2026, 1, 1) + timedelta(days=day_index)
        is_late = day_index >= 28
        for item_index in range(12):
            if weak_late_block and is_late and item_index >= 3:
                direct = -4.0
                reverse = 3.0
            else:
                direct = 6.0
                reverse = -3.0
            rows.append(
                {
                    "row_id": f"{day.isoformat()}-{item_index}",
                    "ticker": f"T{item_index % 4}",
                    "signal_type": "price_jump",
                    "trading_day": day.isoformat(),
                    "horizon_seconds": "300",
                    "session_bucket": "1",
                    "combo_key_300s": "price_jump+volume_spike",
                    "day_volatility_quantile": "0.8",
                    "ticker_volume_quantile": "0.8",
                    "pre_consolidation_score_60m": "0.1",
                    "pre_return_bps_60m": "20.0",
                    "event_close_to_direction": "0.9",
                    "event_reversal_pressure": "0.1",
                    "recent_signal_count_300s": "2",
                    "forward_available": "True",
                    "cost_adjusted_directional_bps": str(direct),
                    "reverse_directional_bps": str(reverse),
                }
            )
    return rows


def test_honest_market_state_miner_accepts_only_late_verified_rule() -> None:
    miner = _load_script("research_mine_honest_market_states_accept", "research_mine_honest_market_states.py")

    candidates = miner.mine_states(
        _honest_state_dataset(),
        min_discovery_rows=20,
        accepted_min_rows=100,
        accepted_min_sessions=10,
        accepted_min_success_rate=0.90,
        accepted_min_lower_bound=0.75,
    )

    accepted = [row for row in candidates if row["accepted_shadow"]]
    assert accepted
    assert accepted[0]["evaluation_success_rate"] == 1.0
    assert "candidate_action=direct" in accepted[0]["rule"]


def test_honest_market_state_miner_rejects_weak_late_block() -> None:
    miner = _load_script("research_mine_honest_market_states_reject", "research_mine_honest_market_states.py")

    candidates = miner.mine_states(
        _honest_state_dataset(weak_late_block=True),
        min_discovery_rows=20,
        accepted_min_rows=100,
        accepted_min_sessions=10,
        accepted_min_success_rate=0.90,
        accepted_min_lower_bound=0.75,
    )

    assert not [row for row in candidates if row["accepted_shadow"]]
    assert candidates[0]["evaluation_success_rate"] < 0.90
