"""PostgreSQL persistence for automatic signal outcomes."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid5

from psycopg.rows import dict_row

from tinvest_signal_engine.application.signal_outcomes import (
    DirectionalSignalOutcomeRequest,
)
from tinvest_signal_engine.domain.signal_outcomes import (
    DirectionalOutcomePolicy,
    DirectionalSignalOutcome,
    SignalOutcomeConflict,
)


_OUTCOME_NAMESPACE = UUID("8a54f9b1-60d7-5a20-83e7-45af5a8e3f35")


class PostgresDirectionalSignalOutcomeCandidateSource:
    """Load mature directional signal candidates from Postgres.

    The first production-safe family is deliberately narrow: complete
    ``price_jump`` rows with explicit payload direction and realized volatility.
    Other signal families need explicit typed expectations before this adapter
    may expose them to the automatic outcome worker.
    """

    def __init__(self, connection: Any, *, policy: DirectionalOutcomePolicy) -> None:
        self._connection = connection
        self._policy = policy

    def due(
        self,
        *,
        now: datetime,
        limit: int,
    ) -> tuple[DirectionalSignalOutcomeRequest, ...]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if limit <= 0:
            raise ValueError("limit must be positive")
        matured_before = now - timedelta(
            seconds=self._policy.horizon_seconds + self._policy.forward_grace_seconds,
        )
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    ms.signal_id,
                    ms.instrument_id,
                    ms.signal_type,
                    ms.source_event_at,
                    ms.payload_json
                FROM market_signals ms
                WHERE ms.signal_type = 'price_jump'
                  AND ms.provenance_status = 'complete'
                  AND ms.source_event_at IS NOT NULL
                  AND ms.source_event_at <= %(matured_before)s
                  AND ms.cost_model_version = %(cost_model_version)s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM core_directional_signal_outcomes so
                      WHERE so.signal_id = ms.signal_id
                        AND so.horizon_seconds = %(horizon_seconds)s
                        AND so.policy_version = %(policy_version)s
                        AND so.cost_model_version = %(cost_model_version)s
                  )
                ORDER BY ms.source_event_at ASC, ms.signal_id ASC
                LIMIT %(limit)s
                """,
                {
                    "matured_before": matured_before,
                    "horizon_seconds": self._policy.horizon_seconds,
                    "policy_version": self._policy.policy_version,
                    "cost_model_version": self._policy.cost_model_version,
                    "limit": limit,
                },
            )
            rows = cursor.fetchall()
        return tuple(
            request
            for row in rows
            if (request := self._request_from_row(dict(row))) is not None
        )

    def _request_from_row(
        self,
        row: dict[str, object],
    ) -> DirectionalSignalOutcomeRequest | None:
        payload = row["payload_json"]
        if not isinstance(payload, dict):
            return None
        expected_direction = _price_jump_direction(payload)
        realized_volatility_bps = _payload_decimal(payload, "baseline_volatility_bps")
        source_event_at = row["source_event_at"]
        if (
            expected_direction is None
            or realized_volatility_bps is None
            or not isinstance(source_event_at, datetime)
            or source_event_at.tzinfo is None
            or source_event_at.utcoffset() is None
        ):
            return None
        return DirectionalSignalOutcomeRequest(
            signal_id=str(row["signal_id"]),
            instrument_id=str(row["instrument_id"]),
            signal_type=str(row["signal_type"]),
            source_event_at=source_event_at,
            expected_direction=expected_direction,
            realized_volatility_bps=realized_volatility_bps,
            policy=self._policy,
        )


class PostgresSignalOutcomeStore:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    def persist(self, outcome: DirectionalSignalOutcome) -> str:
        row = _outcome_row(outcome)
        try:
            with self._connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(
                    """
                    INSERT INTO core_directional_signal_outcomes (
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
                    FROM core_directional_signal_outcomes
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


def _price_jump_direction(payload: dict[str, object]) -> int | None:
    direction = payload.get("price_direction")
    if direction == "up":
        return 1
    if direction == "down":
        return -1
    return None


def _payload_decimal(payload: dict[str, object], key: str) -> Decimal | None:
    value = payload.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        decimal = Decimal(str(value))
    except Exception:
        return None
    if not decimal.is_finite() or decimal < 0:
        return None
    return decimal
