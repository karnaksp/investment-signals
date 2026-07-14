"""PostgreSQL persistence for automatic signal outcomes."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from psycopg.rows import dict_row

from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalSignalOutcome,
    SignalOutcomeConflict,
)


_OUTCOME_NAMESPACE = UUID("8a54f9b1-60d7-5a20-83e7-45af5a8e3f35")


class PostgresSignalOutcomeStore:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def persist(self, outcome: DirectionalSignalOutcome) -> str:
        row = _outcome_row(outcome)
        try:
            with self._connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    INSERT INTO signal_outcomes (
                        outcome_id, signal_id, instrument_id, signal_type,
                        source_event_at, horizon_seconds, verdict, reason_code,
                        expected_direction, anchor_price, forward_price,
                        raw_return_bps, net_expected_bps, net_reverse_bps,
                        materiality_bps, cost_model_version, policy_version,
                        inverse_hypothesis_candidate
                    ) VALUES (
                        %(outcome_id)s, %(signal_id)s, %(instrument_id)s,
                        %(signal_type)s, %(source_event_at)s,
                        %(horizon_seconds)s, %(verdict)s, %(reason_code)s,
                        %(expected_direction)s, %(anchor_price)s,
                        %(forward_price)s, %(raw_return_bps)s,
                        %(net_expected_bps)s, %(net_reverse_bps)s,
                        %(materiality_bps)s, %(cost_model_version)s,
                        %(policy_version)s, %(inverse_hypothesis_candidate)s
                    )
                    ON CONFLICT (
                        signal_id, horizon_seconds, policy_version,
                        cost_model_version
                    ) DO NOTHING
                    RETURNING outcome_id
                    """,
                    row,
                )
                if cursor.fetchone() is not None:
                    self._connection.commit()
                    return str(row["outcome_id"])

                cursor.execute(
                    """
                    SELECT outcome_id, signal_id, instrument_id, signal_type,
                           source_event_at, horizon_seconds, verdict,
                           reason_code, expected_direction, anchor_price,
                           forward_price, raw_return_bps, net_expected_bps,
                           net_reverse_bps, materiality_bps,
                           cost_model_version, policy_version,
                           inverse_hypothesis_candidate
                    FROM signal_outcomes
                    WHERE signal_id = %(signal_id)s
                      AND horizon_seconds = %(horizon_seconds)s
                      AND policy_version = %(policy_version)s
                      AND cost_model_version = %(cost_model_version)s
                    """,
                    row,
                )
                existing = cursor.fetchone()
                if existing is None or not _matches(existing, row):
                    raise SignalOutcomeConflict(
                        "signal outcome key was reused with different content"
                    )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise
        return str(row["outcome_id"])


def deterministic_signal_outcome_id(outcome: DirectionalSignalOutcome) -> UUID:
    signal_id = UUID(outcome.signal_id)
    name = (
        f"{signal_id}\x1f{outcome.horizon_seconds}\x1f"
        f"{outcome.policy_version}\x1f{outcome.cost_model_version}"
    )
    return uuid5(_OUTCOME_NAMESPACE, name)


def _outcome_row(outcome: DirectionalSignalOutcome) -> dict[str, object]:
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


def _matches(existing: dict[str, Any], expected: dict[str, object]) -> bool:
    comparable = (
        "outcome_id",
        "signal_id",
        "instrument_id",
        "signal_type",
        "source_event_at",
        "horizon_seconds",
        "verdict",
        "reason_code",
        "expected_direction",
        "anchor_price",
        "forward_price",
        "raw_return_bps",
        "net_expected_bps",
        "net_reverse_bps",
        "materiality_bps",
        "cost_model_version",
        "policy_version",
        "inverse_hypothesis_candidate",
    )
    return all(_normalize(existing.get(key)) == _normalize(expected[key]) for key in comparable)


def _normalize(value: object) -> object:
    if isinstance(value, Decimal):
        return value.normalize()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value
