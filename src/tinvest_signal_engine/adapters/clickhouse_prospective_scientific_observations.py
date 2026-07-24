"""ClickHouse HTTP adapter for prospective scientific evidence.

The target tables use ``ReplacingMergeTree``.  Transport retries may therefore
leave duplicate physical rows, but every logical identity must have exactly one
payload fingerprint.  This adapter checks that invariant before and after an
insert and treats same-fingerprint retries as successful replays.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    transient_clickhouse_error,
)
from zoneinfo import ZoneInfo

from tinvest_signal_engine.domain.prospective_live_shadow import (
    LIVE_SHADOW_RECORD_VERSION,
    ProspectiveLiveObservation,
    ProspectiveLiveOutcome,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    MetricUnit,
    MetricValue,
    ProspectiveDecision,
    ProspectiveFeature,
    ProspectiveHypothesis,
    ProspectiveOutcome,
    ProspectiveReason,
    TargetMetric,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
    ProspectiveObservationProvenance,
    ProspectiveScientificObservation,
    ProspectiveScientificOutcome,
    ProspectiveSourceKind,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    AbstentionReason,
    CausalFeatureVector,
    FeatureDecision,
    ScientificCandleHypothesis,
    ScientificTarget,
)
from tinvest_signal_engine.domain.trading_phases import (
    MOEX_EQUITY_PHASE_SCHEDULE_V1,
)


_OBSERVATIONS_TABLE = "scientific_hypothesis_observations"
_OUTCOMES_TABLE = "scientific_hypothesis_outcomes"
_MOSCOW = ZoneInfo("Europe/Moscow")

_FINGERPRINT_SQL = """
SELECT DISTINCT payload_fingerprint
FROM {table:Identifier}
PREWHERE hypothesis_id = {hypothesis_id:String}
  AND hypothesis_version = {hypothesis_version:String}
  AND instrument_id = {instrument_id:String}
  AND trading_day = {trading_day:Date}
WHERE {identity_column:Identifier} = {identity:String}
LIMIT 2
SETTINGS max_execution_time = 10,
         max_rows_to_read = 250000,
         max_bytes_to_read = 250000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()

_LIVE_HYPOTHESES = """(
    ('H3V2', '2.0.0'),
    ('H4V2', '2.0.0'),
    ('H3V3', '3.0.0'),
    ('H4V3', '3.0.0'),
    ('H7V3', '3.0.0'),
    ('H15V2', '2.0.0'),
    ('H16', '1.0.0'),
    ('H16V2', '2.0.0'),
    ('H17', '1.0.0'),
    ('H17V2', '2.0.0')
)"""

_PENDING_SQL = """
SELECT
    observation.observation_id,
    observation.hypothesis_id,
    observation.hypothesis_version,
    observation.policy_version,
    observation.formula_version,
    observation.scientific_source_ids,
    observation.instrument_id,
    observation.ticker,
    observation.trading_day,
    observation.observed_at,
    observation.feature_max_observed_at,
    observation.model_trained_until,
    observation.decision,
    observation.reason_code,
    observation.expected_direction,
    observation.forecast_value,
    observation.target_metric,
    observation.horizon_seconds,
    observation.feature_values_json,
    observation.input_window_start,
    observation.input_window_end,
    observation.source_kind,
    observation.source_max_observed_at,
    observation.source_event_ids,
    observation.input_fingerprint,
    observation.dataset_fingerprint,
    observation.payload_fingerprint,
    observation.recorded_at
FROM scientific_hypothesis_observations AS observation
LEFT ANTI JOIN
(
    SELECT DISTINCT observation_id
    FROM scientific_hypothesis_outcomes
    WHERE outcome_policy_version = {outcome_policy_version:String}
) AS completed USING observation_id
ORDER BY observation.target_at ASC, observation.observation_id ASC
LIMIT {limit:UInt32}
FORMAT JSONEachRow
""".strip()

_LIVE_OBSERVATION_COLUMNS = """
    observation.observation_id,
    observation.record_schema_version,
    observation.instrument_id,
    observation.policy_version,
    observation.hypothesis_id,
    observation.hypothesis_version,
    observation.ticker,
    observation.trading_day,
    observation.observed_at,
    observation.feature_max_observed_at,
    observation.model_trained_until,
    observation.decision,
    observation.reason_code,
    observation.expected_direction,
    observation.target_metric,
    observation.horizon_seconds,
    observation.feature_values_json,
    observation.source_event_ids,
    observation.dataset_fingerprint,
    observation.input_fingerprint,
    observation.recorded_at,
    observation.payload_fingerprint
""".strip()

_LIVE_PENDING_SQL = f"""
SELECT
{_LIVE_OBSERVATION_COLUMNS}
FROM scientific_hypothesis_observations AS observation
LEFT ANTI JOIN
(
    SELECT DISTINCT observation_id
    FROM scientific_hypothesis_outcomes
    WHERE record_schema_version = {{record_schema_version:String}}
      AND outcome_policy_version = {{outcome_policy_version:String}}
      AND (hypothesis_id, hypothesis_version) IN {_LIVE_HYPOTHESES}
      AND instrument_id IN {{instrument_ids:Array(String)}}
      AND trading_day >= today() - 365
) AS completed USING observation_id
WHERE observation.record_schema_version = {{record_schema_version:String}}
  AND (observation.hypothesis_id, observation.hypothesis_version)
      IN {_LIVE_HYPOTHESES}
  AND observation.instrument_id IN {{instrument_ids:Array(String)}}
  AND observation.trading_day >= today() - 365
ORDER BY observation.target_at ASC, observation.observation_id ASC
LIMIT {{limit:UInt32}}
SETTINGS max_execution_time = 30,
         max_rows_to_read = 5000000,
         max_bytes_to_read = 2000000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()

_LIVE_OBSERVATIONS_SQL = f"""
SELECT
{_LIVE_OBSERVATION_COLUMNS}
FROM scientific_hypothesis_observations AS observation
WHERE observation.record_schema_version = {{record_schema_version:String}}
  AND (observation.hypothesis_id, observation.hypothesis_version)
      IN {_LIVE_HYPOTHESES}
  AND observation.instrument_id IN {{instrument_ids:Array(String)}}
  AND observation.trading_day >= today() - 365
ORDER BY observation.observed_at ASC, observation.observation_id ASC
LIMIT 5000000
SETTINGS max_execution_time = 30,
         max_rows_to_read = 5000000,
         max_bytes_to_read = 2000000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()

_LIVE_OBSERVATION_BY_ID_SQL = f"""
SELECT
{_LIVE_OBSERVATION_COLUMNS}
FROM scientific_hypothesis_observations AS observation
WHERE observation.record_schema_version = {{record_schema_version:String}}
  AND observation.observation_id = {{observation_id:String}}
  AND (observation.hypothesis_id, observation.hypothesis_version)
      IN {_LIVE_HYPOTHESES}
  AND observation.instrument_id IN {{instrument_ids:Array(String)}}
  AND observation.trading_day >= today() - 365
LIMIT 2
SETTINGS max_execution_time = 10,
         max_rows_to_read = 500000,
         max_bytes_to_read = 500000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()

_LIVE_EXISTING_OBSERVATION_IDS_SQL = f"""
SELECT
    observation_id,
    uniqExact(payload_fingerprint) AS payload_fingerprint_count
FROM scientific_hypothesis_observations
WHERE record_schema_version = {{record_schema_version:String}}
  AND observation_id IN {{observation_ids:Array(String)}}
  AND (hypothesis_id, hypothesis_version) IN {_LIVE_HYPOTHESES}
  AND instrument_id IN {{instrument_ids:Array(String)}}
  AND trading_day >= today() - 365
GROUP BY observation_id
LIMIT 64
SETTINGS max_execution_time = 10,
         max_rows_to_read = 500000,
         max_bytes_to_read = 500000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()

_LIVE_OUTCOMES_SQL = f"""
SELECT
    outcome_id,
    record_schema_version,
    observation_id,
    outcome_policy_version,
    target_metric,
    target_at,
    available,
    reason_code,
    measurements_json,
    evidence_fingerprint,
    evaluated_at,
    payload_fingerprint
FROM scientific_hypothesis_outcomes
WHERE record_schema_version = {{record_schema_version:String}}
  AND outcome_policy_version = {{outcome_policy_version:String}}
  AND (hypothesis_id, hypothesis_version) IN {_LIVE_HYPOTHESES}
  AND instrument_id IN {{instrument_ids:Array(String)}}
  AND trading_day >= today() - 365
ORDER BY target_at ASC, outcome_id ASC
LIMIT 5000000
SETTINGS max_execution_time = 30,
         max_rows_to_read = 5000000,
         max_bytes_to_read = 2000000000,
         timeout_before_checking_execution_speed = 0
FORMAT JSONEachRow
""".strip()


class ClickHouseProspectiveScientificStore:
    """Implement the application store port through ClickHouse HTTP."""

    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        timeout_seconds: float = 15.0,
    ) -> None:
        if not base_url.startswith(("http://", "https://")):
            raise ValueError("ClickHouse URL must use HTTP or HTTPS")
        if not database.strip() or not username.strip():
            raise ValueError("ClickHouse database and username are required")
        if timeout_seconds <= 0:
            raise ValueError("ClickHouse timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._timeout_seconds = timeout_seconds

    def persist_observation(
        self, observation: ProspectiveScientificObservation
    ) -> PersistenceDisposition:
        row = _observation_row(observation)
        return self._persist_immutable(
            table=_OBSERVATIONS_TABLE,
            identity_column="observation_id",
            identity=observation.observation_id,
            payload_fingerprint=observation.payload_fingerprint,
            row=row,
        )

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveScientificObservation, ...]:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        payload = self._request(
            _PENDING_SQL,
            parameters={
                "outcome_policy_version": outcome_policy_version,
                "limit": str(limit),
            },
        )
        return tuple(
            _observation_from_row(row)
            for row in _canonical_observation_rows(_json_each_row(payload))
        )

    def persist_outcome(
        self, outcome: ProspectiveScientificOutcome
    ) -> PersistenceDisposition:
        row = _outcome_row(outcome)
        return self._persist_immutable(
            table=_OUTCOMES_TABLE,
            identity_column="outcome_id",
            identity=outcome.outcome_id,
            payload_fingerprint=outcome.payload_fingerprint,
            row=row,
        )

    def _persist_immutable(
        self,
        *,
        table: str,
        identity_column: str,
        identity: str,
        payload_fingerprint: str,
        row: Mapping[str, object],
    ) -> PersistenceDisposition:
        before = self._fingerprints(
            table=table,
            identity_column=identity_column,
            identity=identity,
            row_context=row,
        )
        if before:
            _require_single_fingerprint(
                identity=identity,
                expected=payload_fingerprint,
                observed=before,
            )
            return PersistenceDisposition.REPLAYED

        body = (
            f"INSERT INTO {table} FORMAT JSONEachRow\n"
            + json.dumps(
                row,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        self._request(
            body,
            parameters={
                "async_insert": "1",
                "date_time_input_format": "best_effort",
                "wait_for_async_insert": "1",
            },
        )
        after = self._fingerprints(
            table=table,
            identity_column=identity_column,
            identity=identity,
            row_context=row,
        )
        if not after:
            raise RuntimeError(
                "ClickHouse insert was not visible after acknowledgement"
            )
        _require_single_fingerprint(
            identity=identity,
            expected=payload_fingerprint,
            observed=after,
        )
        return PersistenceDisposition.INSERTED

    def _fingerprints(
        self,
        *,
        table: str,
        identity_column: str,
        identity: str,
        row_context: Mapping[str, object],
    ) -> frozenset[str]:
        if table not in {_OBSERVATIONS_TABLE, _OUTCOMES_TABLE}:
            raise ValueError("unsupported scientific evidence table")
        allowed_identity = {
            _OBSERVATIONS_TABLE: "observation_id",
            _OUTCOMES_TABLE: "outcome_id",
        }[table]
        if identity_column != allowed_identity:
            raise ValueError("unsupported scientific evidence identity column")
        sql = _FINGERPRINT_SQL.replace("{table:Identifier}", table).replace(
            "{identity_column:Identifier}", identity_column
        )
        payload = self._request(
            sql,
            parameters={
                "hypothesis_id": str(row_context["hypothesis_id"]),
                "hypothesis_version": str(row_context["hypothesis_version"]),
                "instrument_id": str(row_context["instrument_id"]),
                "trading_day": str(row_context["trading_day"]),
                "identity": identity,
            },
        )
        return frozenset(
            str(row["payload_fingerprint"]) for row in _json_each_row(payload)
        )

    def _request(self, sql: str, *, parameters: Mapping[str, str]) -> bytes:
        query = {"database": self._database}
        query.update({f"param_{key}": value for key, value in parameters.items()})
        request = Request(
            f"{self._base_url}/?{urlencode(query)}",
            data=sql.encode("utf-8"),
            headers={
                "Content-Type": "text/plain; charset=utf-8",
                "X-ClickHouse-User": self._username,
                "X-ClickHouse-Key": self._password,
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError, ConnectionResetError) as error:
            transient = transient_clickhouse_error(
                error,
                operation="scientific_evidence_request",
            )
            if transient is not None:
                raise transient from error
            raise RuntimeError(
                "ClickHouse scientific evidence request failed "
                f"with status {error.code}"
            ) from error


class ClickHouseProspectiveLiveShadowStore(ClickHouseProspectiveScientificStore):
    """Durable implementation of the prospective live-shadow store port."""

    def __init__(
        self,
        *,
        base_url: str,
        database: str,
        username: str,
        password: str,
        instrument_ids: tuple[str, ...],
        timeout_seconds: float = 15.0,
    ) -> None:
        normalized = tuple(dict.fromkeys(item.strip() for item in instrument_ids))
        if not normalized or any(not item for item in normalized):
            raise ValueError("live store instrument_ids must be non-empty and unique")
        super().__init__(
            base_url=base_url,
            database=database,
            username=username,
            password=password,
            timeout_seconds=timeout_seconds,
        )
        self._live_instrument_ids = normalized

    def _live_parameters(self) -> dict[str, str]:
        return {
            "instrument_ids": _clickhouse_string_array_parameter(
                self._live_instrument_ids
            )
        }

    def existing_observation_ids(
        self, observation_ids: tuple[str, ...]
    ) -> frozenset[str]:
        normalized = tuple(dict.fromkeys(item.strip() for item in observation_ids))
        if not normalized:
            return frozenset()
        if any(not item for item in normalized) or len(normalized) > 64:
            raise ValueError(
                "live observation identity lookup must contain at most 64 ids"
            )
        payload = self._request(
            _LIVE_EXISTING_OBSERVATION_IDS_SQL,
            parameters={
                "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
                "observation_ids": _clickhouse_string_array_parameter(normalized),
                **self._live_parameters(),
            },
        )
        rows = _json_each_row(payload)
        conflicting_ids = tuple(
            str(row["observation_id"])
            for row in rows
            if int(row["payload_fingerprint_count"]) != 1
        )
        if conflicting_ids:
            raise ProspectiveEvidenceConflict(
                "live scientific observation identity has conflicting physical rows: "
                + ", ".join(conflicting_ids)
            )
        return frozenset(str(row["observation_id"]) for row in rows)

    def persist_observation(
        self, observation: ProspectiveLiveObservation
    ) -> PersistenceDisposition:
        return self._persist_immutable(
            table=_OBSERVATIONS_TABLE,
            identity_column="observation_id",
            identity=observation.observation_id,
            payload_fingerprint=observation.payload_fingerprint,
            row=_live_observation_row(observation),
        )

    def pending_observations(
        self,
        *,
        outcome_policy_version: str,
        limit: int,
    ) -> tuple[ProspectiveLiveObservation, ...]:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        if limit <= 0:
            raise ValueError("limit must be positive")
        return tuple(
            _live_observation_from_row(row)
            for row in _canonical_live_rows(
                _json_each_row(
                    self._request(
                        _LIVE_PENDING_SQL,
                        parameters={
                            "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
                            "outcome_policy_version": outcome_policy_version,
                            "limit": str(limit),
                            **self._live_parameters(),
                        },
                    )
                ),
                identity_column="observation_id",
            )
        )

    def persist_outcome(
        self, outcome: ProspectiveLiveOutcome
    ) -> PersistenceDisposition:
        observation = self._load_live_observation(outcome.observation_id)
        return self._persist_immutable(
            table=_OUTCOMES_TABLE,
            identity_column="outcome_id",
            identity=outcome.outcome_id,
            payload_fingerprint=outcome.payload_fingerprint,
            row=_live_outcome_row(outcome, observation),
        )

    def observations(self) -> tuple[ProspectiveLiveObservation, ...]:
        payload = self._request(
            _LIVE_OBSERVATIONS_SQL,
            parameters={
                "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
                **self._live_parameters(),
            },
        )
        return tuple(
            _live_observation_from_row(row)
            for row in _canonical_live_rows(
                _json_each_row(payload), identity_column="observation_id"
            )
        )

    def outcomes(
        self, *, outcome_policy_version: str
    ) -> tuple[ProspectiveLiveOutcome, ...]:
        if not outcome_policy_version.strip():
            raise ValueError("outcome_policy_version must not be empty")
        payload = self._request(
            _LIVE_OUTCOMES_SQL,
            parameters={
                "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
                "outcome_policy_version": outcome_policy_version,
                **self._live_parameters(),
            },
        )
        return tuple(
            _live_outcome_from_row(row)
            for row in _canonical_live_rows(
                _json_each_row(payload), identity_column="outcome_id"
            )
        )

    def _load_live_observation(self, observation_id: str) -> ProspectiveLiveObservation:
        payload = self._request(
            _LIVE_OBSERVATION_BY_ID_SQL,
            parameters={
                "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
                "observation_id": observation_id,
                **self._live_parameters(),
            },
        )
        rows = _canonical_live_rows(
            _json_each_row(payload), identity_column="observation_id"
        )
        if len(rows) != 1:
            raise ValueError("live outcome observation is missing from ClickHouse")
        return _live_observation_from_row(rows[0])


def _require_single_fingerprint(
    *, identity: str, expected: str, observed: frozenset[str]
) -> None:
    if observed != frozenset({expected}):
        raise ProspectiveEvidenceConflict(
            "scientific evidence identity has conflicting payload fingerprints: "
            f"{identity}"
        )


def _observation_row(
    observation: ProspectiveScientificObservation,
) -> Mapping[str, object]:
    feature = observation.feature
    provenance = observation.provenance
    local = feature.observed_at.astimezone(_MOSCOW)
    return {
        "observation_id": observation.observation_id,
        "hypothesis_id": feature.hypothesis.value,
        "hypothesis_version": feature.hypothesis_version,
        "policy_version": observation.policy_version,
        "formula_version": observation.formula_version,
        "formula_fingerprint": _version_fingerprint(
            "formula", observation.formula_version
        ),
        "scientific_source_ids": list(provenance.scientific_source_ids),
        "instrument_id": observation.instrument_id,
        "ticker": feature.ticker,
        "trading_day": feature.trading_day.isoformat(),
        "observed_at": _clickhouse_datetime(feature.observed_at),
        "feature_max_observed_at": _clickhouse_datetime(
            feature.feature_max_observed_at
        ),
        "model_trained_until": (
            _clickhouse_datetime(feature.model_trained_until)
            if feature.model_trained_until is not None
            else None
        ),
        "market_phase": MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(
            feature.observed_at
        ).value,
        "phase_bucket": f"{local.hour:02d}:{(local.minute // 15) * 15:02d}",
        "decision": feature.decision.value,
        "reason_code": feature.reason.value,
        "expected_direction": feature.expected_direction,
        "forecast_value": feature.forecast_value,
        "target_metric": feature.target.value,
        "effect_unit": _effect_unit(feature.target),
        "claim_scope": "shadow",
        "horizon_seconds": feature.horizon_seconds,
        "target_at": _clickhouse_datetime(observation.target_at),
        "feature_values_json": _json(tuple(feature.feature_values)),
        "thresholds_json": "{}",
        "input_window_start": _clickhouse_datetime(provenance.source_window_start),
        "input_window_end": _clickhouse_datetime(provenance.source_window_end),
        "source_kind": provenance.source_kind.value,
        "source_max_observed_at": _clickhouse_datetime(
            provenance.source_max_observed_at
        ),
        "has_gap": int(feature.reason is AbstentionReason.NON_CONTIGUOUS_WINDOW),
        "source_event_ids": list(provenance.source_event_ids),
        "input_fingerprint": provenance.input_fingerprint,
        "dataset_fingerprint": provenance.dataset_fingerprint,
        "config_fingerprint": _version_fingerprint(
            "policy", observation.policy_version
        ),
        "payload_fingerprint": observation.payload_fingerprint,
        "recorded_at": _clickhouse_datetime(observation.recorded_at),
        "record_version": _record_version(observation.recorded_at),
    }


def _outcome_row(outcome: ProspectiveScientificOutcome) -> Mapping[str, object]:
    result = outcome.result
    return {
        "outcome_id": outcome.outcome_id,
        "observation_id": outcome.observation_id,
        "hypothesis_id": outcome.hypothesis.value,
        "hypothesis_version": outcome.hypothesis_version,
        "instrument_id": outcome.instrument_id,
        "trading_day": outcome.trading_day.isoformat(),
        "target_at": _clickhouse_datetime(outcome.target_at),
        "observed_range_start": _clickhouse_datetime(outcome.source_window_start),
        "observed_range_end": _clickhouse_datetime(outcome.source_window_end),
        "available": int(result.available),
        "reason_code": result.reason.value,
        "actual_value": result.actual_value,
        "cost_adjusted_value": result.cost_adjusted_value,
        "model_loss": result.model_loss,
        "benchmark_loss": result.benchmark_loss,
        "supported": int(result.supported) if result.supported is not None else None,
        "target_metric": outcome.target.value,
        "effect_unit": _effect_unit(outcome.target),
        "outcome_policy_version": outcome.outcome_policy_version,
        "source_event_ids": list(outcome.source_event_ids),
        "source_window_start": _clickhouse_datetime(outcome.source_window_start),
        "source_window_end": _clickhouse_datetime(outcome.source_window_end),
        "source_max_observed_at": _clickhouse_datetime(outcome.source_max_observed_at),
        "input_fingerprint": outcome.input_fingerprint,
        "evaluated_at": _clickhouse_datetime(outcome.evaluated_at),
        "payload_fingerprint": outcome.payload_fingerprint,
        "record_version": _record_version(outcome.evaluated_at),
    }


def _live_observation_row(
    observation: ProspectiveLiveObservation,
) -> Mapping[str, object]:
    feature = observation.feature
    local = feature.observed_at.astimezone(_MOSCOW)
    history_boundaries = tuple(
        item
        for item in (feature.history_observed_until, feature.model_trained_until)
        if item is not None
    )
    input_start = min(history_boundaries, default=feature.observed_at)
    feature_payload = {
        "feature_observation_id": feature.observation_id,
        "history_observed_until": (
            feature.history_observed_until.isoformat()
            if feature.history_observed_until is not None
            else None
        ),
        "forecast": (
            {
                "name": feature.forecast.name,
                "unit": feature.forecast.unit.value,
                "value": feature.forecast.value,
            }
            if feature.forecast is not None
            else None
        ),
        "values": [
            {"name": item.name, "unit": item.unit.value, "value": item.value}
            for item in feature.feature_values
        ],
    }
    return {
        "observation_id": observation.observation_id,
        "record_schema_version": observation.record_version,
        "hypothesis_id": feature.hypothesis.value,
        "hypothesis_version": feature.hypothesis_version,
        "policy_version": observation.policy_version,
        "formula_version": feature.hypothesis_version,
        "formula_fingerprint": _version_fingerprint(
            feature.hypothesis.value, feature.hypothesis_version
        ),
        "scientific_source_ids": [],
        "instrument_id": observation.instrument_id,
        "ticker": feature.ticker,
        "trading_day": feature.trading_day.isoformat(),
        "observed_at": _clickhouse_datetime(feature.observed_at),
        "feature_max_observed_at": _clickhouse_datetime(
            feature.feature_max_observed_at
        ),
        "model_trained_until": (
            _clickhouse_datetime(feature.model_trained_until)
            if feature.model_trained_until is not None
            else None
        ),
        "market_phase": MOEX_EQUITY_PHASE_SCHEDULE_V1.phase_at(
            feature.observed_at
        ).value,
        "phase_bucket": f"{local.hour:02d}:{(local.minute // 15) * 15:02d}",
        "decision": feature.decision.value,
        "reason_code": feature.reason.value,
        "expected_direction": feature.expected_direction,
        "forecast_value": (
            feature.forecast.value if feature.forecast is not None else None
        ),
        "target_metric": feature.target.value,
        "effect_unit": feature.target.unit.value,
        "claim_scope": "shadow",
        "horizon_seconds": feature.horizon_seconds,
        "target_at": _clickhouse_datetime(observation.target_at),
        "feature_values_json": _json(feature_payload),
        "thresholds_json": "{}",
        "input_window_start": _clickhouse_datetime(input_start),
        "input_window_end": _clickhouse_datetime(feature.observed_at),
        "source_kind": "stream",
        "source_max_observed_at": _clickhouse_datetime(feature.observed_at),
        "has_gap": int(feature.reason is ProspectiveReason.NON_CONTIGUOUS_WINDOW),
        "source_event_ids": list(observation.source_event_ids),
        "input_fingerprint": observation.input_fingerprint,
        "dataset_fingerprint": observation.dataset_fingerprint,
        "config_fingerprint": _version_fingerprint(
            "policy", observation.policy_version
        ),
        "payload_fingerprint": observation.payload_fingerprint,
        "recorded_at": _clickhouse_datetime(observation.recorded_at),
        "record_version": _record_version(observation.recorded_at),
    }


def _live_outcome_row(
    outcome: ProspectiveLiveOutcome,
    observation: ProspectiveLiveObservation,
) -> Mapping[str, object]:
    result = outcome.outcome
    if result.observation_id != observation.feature.observation_id:
        raise ValueError("live outcome feature differs from stored observation")
    measurements = {item.name: item.value for item in result.measurements}
    descriptive_effect = _live_effect(result.target, measurements)
    return {
        "outcome_id": outcome.outcome_id,
        "record_schema_version": outcome.record_version,
        "observation_id": outcome.observation_id,
        "hypothesis_id": observation.feature.hypothesis.value,
        "hypothesis_version": observation.feature.hypothesis_version,
        "instrument_id": observation.instrument_id,
        "trading_day": observation.feature.trading_day.isoformat(),
        "target_at": _clickhouse_datetime(result.target_at),
        "observed_range_start": _clickhouse_datetime(observation.feature.observed_at),
        "observed_range_end": _clickhouse_datetime(result.target_at),
        "available": int(result.available),
        "reason_code": result.reason.value,
        "actual_value": _first_optional(
            measurements.get("forward_return"),
            measurements.get("future_realized_variance"),
        ),
        "cost_adjusted_value": measurements.get("cost_adjusted_directional_return"),
        "model_loss": measurements.get("har_qlike"),
        "benchmark_loss": _minimum_optional(
            measurements.get("ewma_qlike"), measurements.get("phase_qlike")
        ),
        "supported": (
            int(descriptive_effect > 0.0) if descriptive_effect is not None else None
        ),
        "target_metric": result.target.value,
        "effect_unit": result.target.unit.value,
        "outcome_policy_version": outcome.outcome_policy_version,
        "source_event_ids": list(observation.source_event_ids),
        "source_window_start": _clickhouse_datetime(observation.feature.observed_at),
        "source_window_end": _clickhouse_datetime(result.target_at),
        "source_max_observed_at": _clickhouse_datetime(result.target_at),
        "input_fingerprint": outcome.evidence_fingerprint,
        "evidence_fingerprint": outcome.evidence_fingerprint,
        "measurements_json": _json(
            {
                "feature_observation_id": result.observation_id,
                "values": [
                    {"name": item.name, "unit": item.unit.value, "value": item.value}
                    for item in result.measurements
                ],
            }
        ),
        "evaluated_at": _clickhouse_datetime(outcome.evaluated_at),
        "payload_fingerprint": outcome.payload_fingerprint,
        "record_version": _record_version(outcome.evaluated_at),
    }


def _live_observation_from_row(
    row: Mapping[str, object],
) -> ProspectiveLiveObservation:
    payload = json.loads(str(row["feature_values_json"]))
    if not isinstance(payload, dict):
        raise ValueError("live feature_values_json must contain an object")
    raw_values = payload.get("values")
    if not isinstance(raw_values, list):
        raise ValueError("live feature values must contain an array")
    raw_forecast = payload.get("forecast")
    if raw_forecast is not None and not isinstance(raw_forecast, dict):
        raise ValueError("live forecast must contain an object")
    observed_at = _datetime(row["observed_at"])
    feature = ProspectiveFeature(
        observation_id=str(payload["feature_observation_id"]),
        hypothesis=ProspectiveHypothesis(str(row["hypothesis_id"])),
        ticker=str(row["ticker"]),
        trading_day=date.fromisoformat(str(row["trading_day"])),
        observed_at=observed_at,
        feature_max_observed_at=_datetime(row["feature_max_observed_at"]),
        history_observed_until=(
            _datetime(payload["history_observed_until"])
            if payload.get("history_observed_until") is not None
            else None
        ),
        model_trained_until=(
            _datetime(row["model_trained_until"])
            if row.get("model_trained_until") is not None
            else None
        ),
        horizon_seconds=int(row["horizon_seconds"]),
        target=TargetMetric(str(row["target_metric"])),
        decision=ProspectiveDecision(str(row["decision"])),
        reason=ProspectiveReason(str(row["reason_code"])),
        expected_direction=int(row["expected_direction"]),
        forecast=(
            MetricValue(
                name=str(raw_forecast["name"]),
                unit=MetricUnit(str(raw_forecast["unit"])),
                value=_metric_number(raw_forecast["value"]),
            )
            if raw_forecast is not None
            else None
        ),
        feature_values=tuple(
            MetricValue(
                name=str(item["name"]),
                unit=MetricUnit(str(item["unit"])),
                value=_metric_number(item["value"]),
            )
            for item in raw_values
            if isinstance(item, dict)
        ),
    )
    return ProspectiveLiveObservation(
        observation_id=str(row["observation_id"]),
        record_version=str(row["record_schema_version"]),
        instrument_id=str(row["instrument_id"]),
        policy_version=str(row["policy_version"]),
        feature=feature,
        source_event_ids=tuple(str(item) for item in _array(row["source_event_ids"])),
        dataset_fingerprint=str(row["dataset_fingerprint"]),
        input_fingerprint=str(row["input_fingerprint"]),
        recorded_at=_datetime(row["recorded_at"]),
        payload_fingerprint=str(row["payload_fingerprint"]),
    )


def _live_outcome_from_row(row: Mapping[str, object]) -> ProspectiveLiveOutcome:
    payload = json.loads(str(row["measurements_json"]))
    if not isinstance(payload, dict) or not isinstance(payload.get("values"), list):
        raise ValueError("measurements_json must contain a live outcome object")
    raw_measurements = payload["values"]
    result = ProspectiveOutcome(
        observation_id=str(payload["feature_observation_id"]),
        target_at=_datetime(row["target_at"]),
        available=_boolean(row["available"]),
        reason=ProspectiveReason(str(row["reason_code"])),
        target=TargetMetric(str(row["target_metric"])),
        measurements=tuple(
            MetricValue(
                name=str(item["name"]),
                unit=MetricUnit(str(item["unit"])),
                value=_metric_number(item["value"]),
            )
            for item in raw_measurements
            if isinstance(item, dict)
        ),
    )
    return ProspectiveLiveOutcome(
        outcome_id=str(row["outcome_id"]),
        record_version=str(row["record_schema_version"]),
        observation_id=str(row["observation_id"]),
        outcome_policy_version=str(row["outcome_policy_version"]),
        outcome=result,
        evidence_fingerprint=str(row["evidence_fingerprint"]),
        evaluated_at=_datetime(row["evaluated_at"]),
        payload_fingerprint=str(row["payload_fingerprint"]),
    )


def _observation_from_row(
    row: Mapping[str, object],
) -> ProspectiveScientificObservation:
    feature_values_raw = json.loads(str(row["feature_values_json"]))
    if not isinstance(feature_values_raw, list):
        raise ValueError("feature_values_json must contain an array")
    observed_at = _datetime(row["observed_at"])
    hypothesis = ScientificCandleHypothesis(str(row["hypothesis_id"]))
    hypothesis_version = str(row["hypothesis_version"])
    ticker = str(row["ticker"])
    feature = CausalFeatureVector(
        observation_id=_scientific_feature_id(
            hypothesis=hypothesis,
            hypothesis_version=hypothesis_version,
            ticker=ticker,
            observed_at=observed_at,
        ),
        hypothesis=hypothesis,
        hypothesis_version=hypothesis_version,
        ticker=ticker,
        trading_day=date.fromisoformat(str(row["trading_day"])),
        observed_at=observed_at,
        feature_max_observed_at=_datetime(row["feature_max_observed_at"]),
        model_trained_until=(
            _datetime(row["model_trained_until"])
            if row.get("model_trained_until") is not None
            else None
        ),
        horizon_seconds=int(row["horizon_seconds"]),
        target=ScientificTarget(str(row["target_metric"])),
        decision=FeatureDecision(str(row["decision"])),
        reason=AbstentionReason(str(row["reason_code"])),
        expected_direction=int(row["expected_direction"]),
        forecast_value=(
            float(row["forecast_value"])
            if row.get("forecast_value") is not None
            else None
        ),
        feature_values=tuple(
            (str(item[0]), float(item[1])) for item in feature_values_raw
        ),
    )
    provenance = ProspectiveObservationProvenance(
        source_kind=ProspectiveSourceKind(str(row["source_kind"])),
        source_event_ids=tuple(str(item) for item in _array(row["source_event_ids"])),
        source_window_start=_datetime(row["input_window_start"]),
        source_window_end=_datetime(row["input_window_end"]),
        source_max_observed_at=_datetime(row["source_max_observed_at"]),
        input_fingerprint=str(row["input_fingerprint"]),
        dataset_fingerprint=str(row["dataset_fingerprint"]),
        scientific_source_ids=tuple(
            str(item) for item in _array(row["scientific_source_ids"])
        ),
    )
    return ProspectiveScientificObservation(
        observation_id=str(row["observation_id"]),
        instrument_id=str(row["instrument_id"]),
        feature=feature,
        policy_version=str(row["policy_version"]),
        formula_version=str(row["formula_version"]),
        provenance=provenance,
        recorded_at=_datetime(row["recorded_at"]),
        payload_fingerprint=str(row["payload_fingerprint"]),
    )


def _canonical_observation_rows(
    rows: tuple[Mapping[str, object], ...],
) -> tuple[Mapping[str, object], ...]:
    canonical: dict[str, Mapping[str, object]] = {}
    for row in rows:
        identity = str(row["observation_id"])
        existing = canonical.get(identity)
        if existing is None:
            canonical[identity] = row
            continue
        if str(existing["payload_fingerprint"]) != str(row["payload_fingerprint"]):
            raise ProspectiveEvidenceConflict(
                f"scientific observation has conflicting physical versions: {identity}"
            )
    return tuple(
        canonical[identity]
        for identity in sorted(
            canonical,
            key=lambda item: (
                _datetime(canonical[item]["observed_at"]),
                item,
            ),
        )
    )


def _canonical_live_rows(
    rows: tuple[Mapping[str, object], ...],
    *,
    identity_column: str,
) -> tuple[Mapping[str, object], ...]:
    if identity_column not in {"observation_id", "outcome_id"}:
        raise ValueError("unsupported live evidence identity column")
    canonical: dict[str, Mapping[str, object]] = {}
    for row in rows:
        identity = str(row[identity_column])
        existing = canonical.get(identity)
        if existing is not None and str(existing["payload_fingerprint"]) != str(
            row["payload_fingerprint"]
        ):
            raise ProspectiveEvidenceConflict(
                f"live evidence has conflicting physical versions: {identity}"
            )
        canonical[identity] = row
    return tuple(canonical[identity] for identity in sorted(canonical))


def _live_effect(
    target: TargetMetric, measurements: Mapping[str, float]
) -> float | None:
    if target is TargetMetric.FORWARD_RETURN:
        return measurements.get("cost_adjusted_directional_return")
    if target is TargetMetric.FUTURE_VARIANCE_UPLIFT:
        return measurements.get("future_variance_uplift")
    model = measurements.get("har_qlike")
    benchmark = _minimum_optional(
        measurements.get("ewma_qlike"), measurements.get("phase_qlike")
    )
    if model is None or benchmark is None:
        return None
    return benchmark - model


def _minimum_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return min(left, right)


def _first_optional(left: float | None, right: float | None) -> float | None:
    return left if left is not None else right


def _effect_unit(target: ScientificTarget) -> str:
    if target is ScientificTarget.DIRECTIONAL_RETURN_BPS:
        return "basis_points"
    if target is ScientificTarget.FUTURE_ACTIVITY_UPLIFT:
        return "uplift_ratio"
    return "realized_variance"


def _scientific_feature_id(
    *,
    hypothesis: ScientificCandleHypothesis,
    hypothesis_version: str,
    ticker: str,
    observed_at: datetime,
) -> str:
    identity = "|".join(
        (hypothesis.value, hypothesis_version, ticker, observed_at.isoformat())
    )
    return "sha256:" + sha256(identity.encode("utf-8")).hexdigest()


def _version_fingerprint(kind: str, version: str) -> str:
    return "sha256:" + sha256(f"{kind}\x1f{version}".encode("utf-8")).hexdigest()


def _record_version(value: datetime) -> int:
    utc = value.astimezone(timezone.utc)
    return int(utc.timestamp()) * 1_000_000 + utc.microsecond


def _clickhouse_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _datetime(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("ClickHouse timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("ClickHouse array column must decode as an array")
    return value


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    raise ValueError(f"invalid ClickHouse boolean: {value!r}")


def _metric_number(value: object) -> float | int:
    """Preserve the JSON numeric representation used by sealed fingerprints."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid ClickHouse metric number: {value!r}")
    return value


def _json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _json_each_row(payload: bytes) -> tuple[Mapping[str, object], ...]:
    rows: list[Mapping[str, object]] = []
    for line in payload.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError("ClickHouse JSONEachRow response must contain objects")
        rows.append(row)
    return tuple(rows)


def _clickhouse_string_array_parameter(values: tuple[str, ...]) -> str:
    """Encode a typed ``Array(String)`` HTTP query parameter.

    ClickHouse 25.3 expects SQL literal syntax here rather than JSON syntax.
    Values remain typed request parameters and are never interpolated into SQL.
    """
    if not values:
        raise ValueError("ClickHouse string array parameter must not be empty")
    return (
        "["
        + ",".join(
            "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
            for value in values
        )
        + "]"
    )
