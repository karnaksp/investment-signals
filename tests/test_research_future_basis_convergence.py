from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "research_future_basis_convergence.py"
SPEC = importlib.util.spec_from_file_location("research_future_basis_convergence", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _rows(expiration: str = "2026-06-19") -> list[dict[str, object]]:
    return [
        {
            "future_ticker": "SRM6",
            "basic_asset": "SBER",
            "expiration": expiration,
            "day": f"2026-06-{day:02d}",
            "future_close": future,
            "share_close": 100.0,
            "future_volume": 1000,
            "share_volume": 10_000,
        }
        for day, future in zip(range(8, 19), (101.0, 100.9, 100.8, 100.7, 100.6, 100.5, 100.4, 100.3, 100.2, 100.1, 100.0))
    ]


def test_basis_observation_uses_only_entry_and_later_exit() -> None:
    observations = MODULE.build_basis_observations(_rows(), days_to_exit=(5,))

    assert len(observations) == 1
    row = observations[0]
    assert row["entry_day"] == "2026-06-13"
    assert row["exit_day"] == "2026-06-18"
    assert round(float(row["entry_basis_bps"]), 6) == 50.0
    assert round(float(row["raw_convergence_bps"]), 6) == 50.0
    assert round(float(row["prior_basis_change_3d_bps"]), 6) == -30.0
    assert row["entry_day"] < row["exit_day"]


def test_chronological_split_keeps_expirations_disjoint() -> None:
    rows = [
        {"expiration": expiration, "marker": expiration}
        for expiration in ("2025-03-21", "2025-06-20", "2025-09-19", "2025-12-19")
    ]

    discovery, holdout = MODULE.chronological_expiration_split(rows, discovery_fraction=0.5)

    assert {row["expiration"] for row in discovery} == {"2025-03-21", "2025-06-20"}
    assert {row["expiration"] for row in holdout} == {"2025-09-19", "2025-12-19"}


def test_rule_summary_applies_costs_before_counting_success() -> None:
    observations = MODULE.build_basis_observations(_rows(), days_to_exit=(5,))

    passing = MODULE.summarize_rule(
        observations,
        trading_days_to_exit=5,
        minimum_basis_bps=40.0,
        round_trip_cost_bps=20.0,
    )
    failing = MODULE.summarize_rule(
        observations,
        trading_days_to_exit=5,
        minimum_basis_bps=40.0,
        round_trip_cost_bps=60.0,
    )

    assert passing["successes"] == 1
    assert failing["successes"] == 0


def test_only_dividends_declared_before_entry_adjust_result() -> None:
    events = [
        {
            "basic_asset": "SBER",
            "declared_date": "2026-06-01",
            "last_buy_date": "2026-06-15",
            "dividend_amount": 1.0,
        },
        {
            "basic_asset": "SBER",
            "declared_date": "2026-06-14",
            "last_buy_date": "2026-06-15",
            "dividend_amount": 10.0,
        },
    ]

    observation = MODULE.build_basis_observations(
        _rows(),
        days_to_exit=(5,),
        dividend_events=events,
    )[0]

    assert observation["known_dividend_events"] == 1
    assert round(float(observation["known_dividend_bps"]), 6) == 100.0
    assert round(float(observation["dividend_adjusted_convergence_bps"]), 6) == 150.0


def test_key_rate_is_taken_only_from_day_not_after_entry() -> None:
    observation = MODULE.build_basis_observations(
        _rows(),
        days_to_exit=(5,),
        key_rate_rows=(
            {"day": "2026-06-12", "key_rate_percent": 10.0},
            {"day": "2026-06-14", "key_rate_percent": 99.0},
        ),
    )[0]

    assert observation["key_rate_percent"] == 10.0
    assert round(float(observation["financing_bps"]), 6) == round(10 / 100 * 5 / 365 * 10_000, 6)
    assert round(float(observation["carry_adjusted_convergence_bps"]), 6) == round(
        50 - 10 / 100 * 5 / 365 * 10_000,
        6,
    )
