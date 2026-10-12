from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from tinvest_signal_engine.adapters.postgres_signal_outcomes import (
    PostgresDirectionalSignalOutcomeCandidateSource,
    PostgresSignalOutcomeStore,
    deterministic_signal_outcome_id,
)
from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    DirectionalSignalOutcome,
    SignalOutcomeConflict,
)


SIGNAL_ID = "fd56ea27-aeb3-47f1-b038-182f747f5aa2"
SOURCE_AT = datetime(2026, 7, 10, 7, 5, tzinfo=timezone.utc)


@dataclass
class _Cursor:
    responses: list[object | None]
    fetchall_rows: list[object] = field(default_factory=list)
    executions: list[tuple[str, object]] = field(default_factory=list)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self.executions.append((sql, params))

    def fetchone(self):
        return self.responses.pop(0)

    def fetchall(self):
        return self.fetchall_rows


@dataclass
class _Connection:
    cursor_obj: _Cursor
    commits: int = 0
    rollbacks: int = 0

    def cursor(self, **_kwargs):
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _outcome(**overrides: object) -> DirectionalSignalOutcome:
    values = {
        "signal_id": SIGNAL_ID,
        "instrument_id": "SBER",
        "signal_type": "price_jump",
        "source_event_at": SOURCE_AT,
        "horizon_seconds": 300,
        "verdict": "confirmed",
        "reason_code": "confirmed",
        "expected_direction": 1,
        "anchor_price": Decimal("100"),
        "forward_price": Decimal("102"),
        "raw_return_bps": Decimal("200"),
        "net_expected_bps": Decimal("199"),
        "net_reverse_bps": Decimal("-201"),
        "materiality_bps": Decimal("5"),
        "cost_model_version": "cost-v1",
        "policy_version": "policy-v1",
        "inverse_hypothesis_candidate": False,
    }
    values.update(overrides)
    return DirectionalSignalOutcome(**values)


def _policy(**overrides: object) -> DirectionalOutcomePolicy:
    values = {
        "policy_version": "policy-v1",
        "cost_model_version": "cost-v1",
        "horizon_seconds": 300,
        "anchor_max_age_seconds": 5,
        "forward_grace_seconds": 30,
        "min_move_bps": Decimal("5"),
        "volatility_multiplier": Decimal("0"),
        "round_trip_cost_bps": Decimal("1"),
    }
    values.update(overrides)
    return DirectionalOutcomePolicy(**values)


def _row(outcome: DirectionalSignalOutcome) -> dict[str, object]:
    return {
        "outcome_id": deterministic_signal_outcome_id(outcome),
        "signal_id": UUID(outcome.signal_id),
        "instrument_id": outcome.instrument_id,
        "signal_type": outcome.signal_type,
        "source_event_at": outcome.source_event_at,
        "horizon_seconds": outcome.horizon_seconds,
        "verdict": outcome.verdict,
        "reason_code": outcome.reason_code,
        "expected_direction": outcome.expected_direction,
        "anchor_price": outcome.anchor_price,
        "forward_price": outcome.forward_price,
        "raw_return_bps": outcome.raw_return_bps,
        "net_expected_bps": outcome.net_expected_bps,
        "net_reverse_bps": outcome.net_reverse_bps,
        "materiality_bps": outcome.materiality_bps,
        "cost_model_version": outcome.cost_model_version,
        "policy_version": outcome.policy_version,
        "inverse_hypothesis_candidate": outcome.inverse_hypothesis_candidate,
    }


def test_postgres_signal_outcome_store_inserts_parameterized_row() -> None:
    outcome = _outcome()
    cursor = _Cursor([{"outcome_id": deterministic_signal_outcome_id(outcome)}])
    connection = _Connection(cursor)

    outcome_id = PostgresSignalOutcomeStore(connection).persist(outcome)

    sql, params = cursor.executions[0]
    assert outcome_id == str(deterministic_signal_outcome_id(outcome))
    assert "INSERT INTO core_directional_signal_outcomes" in sql
    assert "ON CONFLICT" in sql
    assert SIGNAL_ID not in sql
    assert params["signal_id"] == UUID(SIGNAL_ID)
    assert params["inverse_hypothesis_candidate"] is False
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_postgres_signal_outcome_store_accepts_identical_replay() -> None:
    outcome = _outcome()
    cursor = _Cursor([None, _row(outcome)])
    connection = _Connection(cursor)

    outcome_id = PostgresSignalOutcomeStore(connection).persist(outcome)

    assert outcome_id == str(deterministic_signal_outcome_id(outcome))
    assert len(cursor.executions) == 2
    assert "SELECT outcome_id" in cursor.executions[1][0]
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_postgres_signal_outcome_store_rejects_drifted_replay() -> None:
    outcome = _outcome()
    existing = _row(_outcome(verdict="contradicted", reason_code="contradicted"))
    cursor = _Cursor([None, existing])
    connection = _Connection(cursor)

    with pytest.raises(SignalOutcomeConflict):
        PostgresSignalOutcomeStore(connection).persist(outcome)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_postgres_candidate_source_emits_price_jump_directional_requests() -> None:
    cursor = _Cursor(
        [],
        fetchall_rows=[
            {
                "signal_id": UUID(SIGNAL_ID),
                "instrument_id": "SBER",
                "signal_type": "price_jump",
                "source_event_at": SOURCE_AT,
                "payload_json": {
                    "price_direction": "up",
                    "baseline_volatility_bps": "3.5",
                },
            },
            {
                "signal_id": UUID("7a9ad851-df7c-4b64-9e37-9eae5c5c78f8"),
                "instrument_id": "GAZP",
                "signal_type": "price_jump",
                "source_event_at": SOURCE_AT,
                "payload_json": {
                    "price_direction": "down",
                    "baseline_volatility_bps": 4.25,
                },
            },
        ],
    )
    connection = _Connection(cursor)
    now = datetime(2026, 7, 10, 7, 11, tzinfo=timezone.utc)

    requests = PostgresDirectionalSignalOutcomeCandidateSource(
        connection,
        policy=_policy(),
    ).due(now=now, limit=50)

    sql, params = cursor.executions[0]
    assert "FROM market_signals ms" in sql
    assert "NOT EXISTS" in sql
    assert "core_directional_signal_outcomes so" in sql
    assert SIGNAL_ID not in sql
    assert params == {
        "matured_before": now - timedelta(seconds=330),
        "horizon_seconds": 300,
        "policy_version": "policy-v1",
        "cost_model_version": "cost-v1",
        "limit": 50,
    }
    assert len(requests) == 2
    assert requests[0].signal_id == SIGNAL_ID
    assert requests[0].expected_direction == 1
    assert requests[0].realized_volatility_bps == Decimal("3.5")
    assert requests[1].expected_direction == -1
    assert requests[1].realized_volatility_bps == Decimal("4.25")


def test_postgres_candidate_source_skips_unmappable_payload_rows() -> None:
    cursor = _Cursor(
        [],
        fetchall_rows=[
            {
                "signal_id": UUID(SIGNAL_ID),
                "instrument_id": "SBER",
                "signal_type": "price_jump",
                "source_event_at": SOURCE_AT,
                "payload_json": {
                    "price_direction": "sideways",
                    "baseline_volatility_bps": "3.5",
                },
            },
            {
                "signal_id": UUID("7a9ad851-df7c-4b64-9e37-9eae5c5c78f8"),
                "instrument_id": "GAZP",
                "signal_type": "price_jump",
                "source_event_at": SOURCE_AT,
                "payload_json": {
                    "price_direction": "up",
                    "baseline_volatility_bps": "-1",
                },
            },
        ],
    )

    requests = PostgresDirectionalSignalOutcomeCandidateSource(
        _Connection(cursor),
        policy=_policy(),
    ).due(now=datetime(2026, 7, 10, 7, 11, tzinfo=timezone.utc), limit=50)

    assert requests == ()


def test_postgres_candidate_source_rejects_invalid_clock_and_limit() -> None:
    source = PostgresDirectionalSignalOutcomeCandidateSource(
        _Connection(_Cursor([])),
        policy=_policy(),
    )

    with pytest.raises(ValueError, match="now must be timezone-aware"):
        source.due(now=SOURCE_AT.replace(tzinfo=None), limit=1)
    with pytest.raises(ValueError, match="limit must be positive"):
        source.due(now=SOURCE_AT, limit=0)
