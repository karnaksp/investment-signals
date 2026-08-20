from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from tinvest_signal_engine.application.r2_live_shadow import (
    ProcessR2OpeningGapLiveShadow,
)
from tinvest_signal_engine.adapters.clickhouse_r2_live_shadow import (
    ClickHouseR2OpeningGapSource,
    ClickHouseR2LiveShadowStore,
)
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2ExtensionPolicy,
    R2Feature,
    R2Metric,
    R2Outcome,
    R2Reason,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
)
from tinvest_signal_engine.domain.r2_live_shadow import R2LiveShadowInput
from tinvest_signal_engine.services.prospective_live_shadow_worker import (
    R2OpeningGapSchedule,
)

UTC = UTC


def test_h10_source_bounds_history_with_policy_trading_days() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, str]]] = []

        def _request(self, sql: str, *, parameters: dict[str, str]) -> bytes:
            self.calls.append((sql, parameters))
            return b""

    client = Client()
    source = ClickHouseR2OpeningGapSource(
        client,
        instrument_ids=("SBER_TQBR",),
        policy=R2ExtensionPolicy(opening_gap_history_days=37),
    )

    assert source.load(as_of=datetime(2026, 7, 30, 8, 0, tzinfo=UTC)) == ()
    assert client.calls[0][1]["history_trading_days"] == "37"


def _item(*, target_at: datetime) -> R2LiveShadowInput:
    observed_at = target_at - timedelta(minutes=30)
    feature = R2Feature(
        observation_id="sha256:" + "a" * 64,
        hypothesis=R2ExtensionHypothesis.OPENING_GAP_REVERSION,
        ticker="SBER",
        trading_day=date(2026, 7, 30),
        event_at=observed_at - timedelta(minutes=1),
        available_at=observed_at,
        feature_source_available_at=observed_at,
        history_available_at=observed_at - timedelta(days=1),
        model_available_at=observed_at - timedelta(days=1),
        horizon_seconds=1800,
        decision=R2Decision.MATCHED,
        reason=R2Reason.CONDITIONS_MATCHED,
        expected_direction=-1,
        values=(
            R2Metric("opening_gap_bps", 120.0),
            R2Metric("opening_gap_z", 2.5),
            R2Metric("market_gap_z", 0.1),
            R2Metric("history_days", 20.0),
        ),
    )
    return R2LiveShadowInput(
        instrument_id="uid-sber",
        feature=feature,
        outcome=R2Outcome(
            observation_id=feature.observation_id,
            target_at=target_at,
            available_at=target_at,
            available=True,
            reason=R2Reason.CONDITIONS_MATCHED,
            forward_return_bps=-30.0,
            cost_adjusted_signed_return_bps=20.0,
        ),
        dataset_fingerprint="sha256:" + "b" * 64,
        source_event_ids=("opening-event", "previous-close-event"),
    )


def test_live_shadow_records_observation_before_maturity_and_outcome_after() -> None:
    target_at = datetime(2026, 7, 30, 7, 31, tzinfo=UTC)
    item = _item(target_at=target_at)

    class Source:
        def load(self, *, as_of):
            return (item,)

    class Store:
        def __init__(self):
            self.observations = 0
            self.outcomes = 0

        def persist_observation(self, item, *, recorded_at):
            self.observations += 1
            return PersistenceDisposition.INSERTED

        def persist_outcome(self, item, *, evaluated_at):
            self.outcomes += 1
            return PersistenceDisposition.INSERTED

    store = Store()
    worker = ProcessR2OpeningGapLiveShadow(source=Source(), store=store)

    early = worker.run_once(now=target_at - timedelta(minutes=10))
    mature = worker.run_once(now=target_at + timedelta(minutes=15))

    assert early.observations_stored == 1
    assert early.outcomes_stored == 0
    assert mature.observations_stored == 1
    assert mature.outcomes_stored == 1


def test_h10_schedule_allows_five_minutes_for_open_and_fifteen_for_outcomes() -> None:
    schedule = R2OpeningGapSchedule()
    before = datetime(2026, 7, 30, 7, 4, tzinfo=UTC)
    opening_ready = datetime(2026, 7, 30, 7, 5, tzinfo=UTC)
    first_outcome_ready = datetime(2026, 7, 30, 7, 45, tzinfo=UTC)

    assert schedule.due(now=before) is False
    assert schedule.due(now=opening_ready) is True
    schedule.complete_due(now=opening_ready)
    assert schedule.due(now=opening_ready) is False
    assert schedule.due(now=first_outcome_ready) is True


def test_h10_store_preserves_first_real_time_decision_after_restart() -> None:
    class Store(ClickHouseR2LiveShadowStore):
        def __init__(self) -> None:
            self.insert_attempts = 0

        def _fingerprints(self, **_kwargs):
            return frozenset({"sha256:" + "f" * 64})

        def _persist_immutable(self, **_kwargs):
            self.insert_attempts += 1
            return PersistenceDisposition.INSERTED

    store = Store()
    item = _item(target_at=datetime(2026, 7, 30, 7, 31, tzinfo=UTC))

    observation = store.persist_observation(
        item,
        recorded_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
    )
    outcome = store.persist_outcome(
        item,
        evaluated_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
    )

    assert observation is PersistenceDisposition.REPLAYED
    assert outcome is PersistenceDisposition.REPLAYED
    assert store.insert_attempts == 0
