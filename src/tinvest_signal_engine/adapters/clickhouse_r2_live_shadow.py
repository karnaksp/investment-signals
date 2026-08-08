"""Bounded ClickHouse source and immutable store for H10 live-shadow rows."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from zoneinfo import ZoneInfo

from tinvest_signal_engine.adapters.clickhouse_prospective_live_shadow import (
    _load_candles,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveScientificStore,
)
from tinvest_signal_engine.application.prospective_portfolio_extensions import (
    R2ExtensionRequest,
    build_r2_extension_research,
)
from tinvest_signal_engine.domain.historical_hypothesis_replay import HistoricalCandle
from tinvest_signal_engine.domain.prospective_live_shadow import (
    LIVE_SHADOW_RECORD_VERSION,
)
from tinvest_signal_engine.domain.prospective_portfolio_extensions import (
    R2Decision,
    R2ExtensionHypothesis,
    R2ExtensionPolicy,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
)
from tinvest_signal_engine.domain.r2_live_shadow import R2LiveShadowInput

MOSCOW = ZoneInfo("Europe/Moscow")
H10_OUTCOME_POLICY_VERSION = "prospective-live-outcomes-v2"
H10_POLICY = R2ExtensionPolicy()
_LOOKBACK_DAYS = 50
_CORPORATE_ACTION_GAP_BPS = 1_500.0


class ClickHouseR2OpeningGapSource:
    """Read one instrument at a time, aligned with the candle primary key."""

    def __init__(
        self,
        client,
        *,
        instrument_ids: tuple[str, ...],
        policy: R2ExtensionPolicy = H10_POLICY,
    ) -> None:
        normalized = tuple(dict.fromkeys(item.strip() for item in instrument_ids))
        if not normalized or any(not item for item in normalized):
            raise ValueError("H10 live-shadow instrument ids are required")
        self._client = client
        self._instrument_ids = normalized
        self._policy = policy

    def load(self, *, as_of: datetime) -> tuple[R2LiveShadowInput, ...]:
        cutoff = _aware_utc(as_of)
        local_day = cutoff.astimezone(MOSCOW).date()
        scientific_rows = []
        for instrument_id in self._instrument_ids:
            loaded = tuple(
                candle
                for candle in _load_candles(
                    self._client,
                    as_of=cutoff,
                    lookback_start=cutoff - timedelta(days=_LOOKBACK_DAYS),
                    candle_until=cutoff,
                    instrument_id=instrument_id,
                    query_kind="history",
                )
                if candle.complete
            )
            scientific_rows.extend(_h10_relevant_candles(loaded))
        scientific = tuple(scientific_rows)
        if not scientific:
            return ()
        by_ticker = defaultdict(list)
        instrument_by_ticker: dict[str, str] = {}
        for candle in scientific:
            existing = instrument_by_ticker.setdefault(
                candle.ticker, candle.instrument_id
            )
            if existing != candle.instrument_id:
                raise ValueError("H10 ticker maps to multiple configured instruments")
            by_ticker[candle.ticker].append(candle)
        historical = tuple(
            HistoricalCandle(
                ticker=candle.ticker,
                at=candle.candle_at,
                open=float(candle.open_price),
                high=float(candle.high_price),
                low=float(candle.low_price),
                close=float(candle.close_price),
                volume=float(candle.volume),
                complete=candle.complete,
            )
            for candle in scientific
        )
        report_dataset_fingerprint = _fingerprint(
            tuple(sorted(candle.payload_fingerprint for candle in scientific))
        )
        corporate_actions = _corporate_action_ticker_days(by_ticker)
        report = build_r2_extension_research(
            historical,
            dataset_fingerprint=report_dataset_fingerprint,
            request=R2ExtensionRequest(
                policy=self._policy,
                selected_hypotheses=(R2ExtensionHypothesis.OPENING_GAP_REVERSION,),
                market_universe=tuple(sorted(by_ticker)),
                target_universe=tuple(sorted(by_ticker)),
                corporate_action_ticker_days=corporate_actions,
                observed_exchange_open_is_schedule_evidence=True,
            ),
        )
        result: list[R2LiveShadowInput] = []
        for feature, outcome in zip(report.features, report.outcomes, strict=True):
            if feature.trading_day != local_day:
                continue
            ticker_rows = tuple(by_ticker[feature.ticker])
            feature_dataset_fingerprint = _fingerprint(
                tuple(
                    sorted(
                        candle.payload_fingerprint
                        for candle in scientific
                        if candle.candle_at < feature.available_at
                    )
                )
            )
            result.append(
                R2LiveShadowInput(
                    instrument_id=instrument_by_ticker[feature.ticker],
                    feature=feature,
                    outcome=outcome,
                    dataset_fingerprint=feature_dataset_fingerprint,
                    source_event_ids=_source_event_ids(
                        ticker_rows,
                        feature.trading_day,
                    ),
                )
            )
        return tuple(
            sorted(
                result,
                key=lambda item: (
                    item.feature.available_at,
                    item.instrument_id,
                    item.feature.horizon_seconds,
                ),
            )
        )


class ClickHouseR2LiveShadowStore(ClickHouseProspectiveScientificStore):
    def persist_observation(
        self,
        item: R2LiveShadowInput,
        *,
        recorded_at: datetime,
    ) -> PersistenceDisposition:
        identity = _live_observation_id(item)
        payload_fingerprint = _live_observation_payload_fingerprint(item)
        return self._persist_sealed_decision(
            table="scientific_hypothesis_observations",
            identity_column="observation_id",
            identity=identity,
            payload_fingerprint=payload_fingerprint,
            row=_observation_row(
                item,
                observation_id=identity,
                payload_fingerprint=payload_fingerprint,
                recorded_at=recorded_at,
            ),
        )

    def persist_outcome(
        self,
        item: R2LiveShadowInput,
        *,
        evaluated_at: datetime,
    ) -> PersistenceDisposition:
        observation_id = _live_observation_id(item)
        identity = _live_outcome_id(observation_id)
        payload_fingerprint = _live_outcome_payload_fingerprint(
            item, observation_id=observation_id
        )
        return self._persist_sealed_decision(
            table="scientific_hypothesis_outcomes",
            identity_column="outcome_id",
            identity=identity,
            payload_fingerprint=payload_fingerprint,
            row=_outcome_row(
                item,
                observation_id=observation_id,
                outcome_id=identity,
                payload_fingerprint=payload_fingerprint,
                evaluated_at=evaluated_at,
            ),
        )

    def _persist_sealed_decision(
        self,
        *,
        table: str,
        identity_column: str,
        identity: str,
        payload_fingerprint: str,
        row: Mapping[str, object],
    ) -> PersistenceDisposition:
        """Keep the first real-time H10 decision authoritative after restarts."""

        existing = self._fingerprints(
            table=table,
            identity_column=identity_column,
            identity=identity,
            row_context=row,
        )
        if len(existing) > 1:
            raise ProspectiveEvidenceConflict(
                "sealed H10 evidence identity has multiple payloads"
            )
        if existing:
            return PersistenceDisposition.REPLAYED
        return self._persist_immutable(
            table=table,
            identity_column=identity_column,
            identity=identity,
            payload_fingerprint=payload_fingerprint,
            row=row,
        )


def _observation_row(
    item: R2LiveShadowInput,
    *,
    observation_id: str,
    payload_fingerprint: str,
    recorded_at: datetime,
) -> Mapping[str, object]:
    feature = item.feature
    local = feature.available_at.astimezone(MOSCOW)
    input_start = feature.history_available_at or feature.available_at
    return {
        "observation_id": observation_id,
        "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
        "hypothesis_id": feature.hypothesis.value,
        "hypothesis_version": feature.hypothesis.version,
        "policy_version": H10_POLICY.version,
        "formula_version": feature.hypothesis.version,
        "formula_fingerprint": _fingerprint(
            (
                feature.hypothesis.value,
                feature.hypothesis.version,
                H10_POLICY.fingerprint,
            )
        ),
        "scientific_source_ids": [
            "doi:10.1017/S0022109012000270",
            "doi:10.1006/jfin.1995.1006",
        ],
        "instrument_id": item.instrument_id,
        "ticker": feature.ticker,
        "trading_day": feature.trading_day.isoformat(),
        "observed_at": _timestamp(feature.available_at),
        "feature_max_observed_at": _timestamp(feature.feature_source_available_at),
        "model_trained_until": (
            _timestamp(feature.model_available_at)
            if feature.model_available_at is not None
            else None
        ),
        "market_phase": "main_session",
        "phase_bucket": f"{local.hour:02d}:{(local.minute // 15) * 15:02d}",
        "decision": feature.decision.value,
        "reason_code": feature.reason.value,
        "expected_direction": feature.expected_direction,
        "forecast_value": (
            -feature.value("opening_gap_bps")
            if feature.decision is R2Decision.MATCHED
            else None
        ),
        "target_metric": "forward_return",
        "effect_unit": "basis_points",
        "claim_scope": "shadow",
        "horizon_seconds": feature.horizon_seconds,
        "target_at": _timestamp(item.outcome.target_at),
        "feature_values_json": _json(
            {
                "feature_observation_id": feature.observation_id,
                "values": [
                    {"name": metric.name, "unit": "ratio", "value": metric.value}
                    for metric in feature.values
                ],
            }
        ),
        "thresholds_json": _json(
            {
                "opening_gap_z_min": H10_POLICY.opening_gap_z_min,
                "market_gap_z_abstention_min": (H10_POLICY.market_gap_z_abstention_min),
            }
        ),
        "input_window_start": _timestamp(input_start),
        "input_window_end": _timestamp(feature.available_at),
        "source_kind": "stream",
        "source_max_observed_at": _timestamp(feature.available_at),
        "has_gap": 0,
        "source_event_ids": list(item.source_event_ids),
        "input_fingerprint": feature.fingerprint,
        "dataset_fingerprint": item.dataset_fingerprint,
        "config_fingerprint": H10_POLICY.fingerprint,
        "payload_fingerprint": payload_fingerprint,
        "recorded_at": _timestamp(recorded_at),
        "record_version": _record_version(recorded_at),
    }


def _outcome_row(
    item: R2LiveShadowInput,
    *,
    observation_id: str,
    outcome_id: str,
    payload_fingerprint: str,
    evaluated_at: datetime,
) -> Mapping[str, object]:
    outcome = item.outcome
    measurements = []
    if outcome.forward_return_bps is not None:
        measurements.append(
            {
                "name": "forward_return",
                "unit": "basis_points",
                "value": outcome.forward_return_bps,
            }
        )
    if outcome.cost_adjusted_signed_return_bps is not None:
        measurements.append(
            {
                "name": "cost_adjusted_directional_return",
                "unit": "basis_points",
                "value": outcome.cost_adjusted_signed_return_bps,
            }
        )
    return {
        "outcome_id": outcome_id,
        "record_schema_version": LIVE_SHADOW_RECORD_VERSION,
        "observation_id": observation_id,
        "hypothesis_id": item.feature.hypothesis.value,
        "hypothesis_version": item.feature.hypothesis.version,
        "instrument_id": item.instrument_id,
        "trading_day": item.feature.trading_day.isoformat(),
        "target_at": _timestamp(outcome.target_at),
        "observed_range_start": _timestamp(item.feature.available_at),
        "observed_range_end": _timestamp(outcome.target_at),
        "available": int(outcome.available),
        "reason_code": outcome.reason.value,
        "actual_value": outcome.forward_return_bps,
        "cost_adjusted_value": outcome.cost_adjusted_signed_return_bps,
        "model_loss": None,
        "benchmark_loss": None,
        "supported": (
            int(outcome.cost_adjusted_signed_return_bps > 0)
            if outcome.cost_adjusted_signed_return_bps is not None
            else None
        ),
        "target_metric": "forward_return",
        "effect_unit": "basis_points",
        "outcome_policy_version": H10_OUTCOME_POLICY_VERSION,
        "source_event_ids": list(item.source_event_ids),
        "source_window_start": _timestamp(item.feature.available_at),
        "source_window_end": _timestamp(outcome.target_at),
        "source_max_observed_at": _timestamp(outcome.target_at),
        "input_fingerprint": outcome.fingerprint,
        "evidence_fingerprint": outcome.fingerprint,
        "measurements_json": _json(
            {
                "feature_observation_id": item.feature.observation_id,
                "values": measurements,
            }
        ),
        "evaluated_at": _timestamp(evaluated_at),
        "payload_fingerprint": payload_fingerprint,
        "record_version": _record_version(evaluated_at),
    }


def _corporate_action_ticker_days(
    by_ticker,
) -> tuple[tuple[str, date], ...]:
    result: list[tuple[str, date]] = []
    for ticker, rows in by_ticker.items():
        by_day = defaultdict(list)
        for row in sorted(rows, key=lambda item: item.candle_at):
            by_day[row.trading_day].append(row)
        previous_close = None
        for trading_day, day_rows in sorted(by_day.items()):
            opening = next(
                (
                    item
                    for item in day_rows
                    if item.candle_at.astimezone(MOSCOW).hour == 10
                    and item.candle_at.astimezone(MOSCOW).minute == 0
                ),
                None,
            )
            if previous_close is not None and opening is not None:
                gap_bps = (
                    float(opening.close_price) / float(previous_close.close_price) - 1
                ) * 10_000
                if abs(gap_bps) >= _CORPORATE_ACTION_GAP_BPS:
                    result.append((ticker, trading_day))
            previous_close = day_rows[-1]
    return tuple(result)


def _h10_relevant_candles(rows):
    """Retain only daily close, opening anchor, and the two outcome anchors."""

    by_day = defaultdict(list)
    for row in sorted(rows, key=lambda item: item.candle_at):
        by_day[row.trading_day].append(row)
    selected = []
    for day_rows in by_day.values():
        for row in day_rows:
            local = row.candle_at.astimezone(MOSCOW)
            is_opening = (local.hour, local.minute) == (10, 0)
            is_30m_outcome = local.hour == 10 and 30 <= local.minute <= 35
            is_60m_outcome = local.hour == 11 and 0 <= local.minute <= 5
            if is_opening or is_30m_outcome or is_60m_outcome:
                selected.append(row)
        if day_rows[-1] not in selected:
            selected.append(day_rows[-1])
    return tuple(
        sorted(
            {item.source_event_id: item for item in selected}.values(),
            key=lambda item: item.candle_at,
        )
    )


def _source_event_ids(rows, trading_day: date):
    ordered = tuple(sorted(rows, key=lambda item: item.candle_at))
    current = tuple(item for item in ordered if item.trading_day == trading_day)
    prior = tuple(item for item in ordered if item.trading_day < trading_day)
    selected = []
    if prior:
        selected.append(prior[-1].source_event_id)
    opening = next(
        (
            item
            for item in current
            if item.candle_at.astimezone(MOSCOW).hour == 10
            and item.candle_at.astimezone(MOSCOW).minute == 0
        ),
        None,
    )
    if opening is not None:
        selected.append(opening.source_event_id)
    return tuple(dict.fromkeys(selected or (f"h10:{trading_day.isoformat()}",)))


def _live_observation_id(item: R2LiveShadowInput) -> str:
    return _fingerprint(
        (
            LIVE_SHADOW_RECORD_VERSION,
            item.instrument_id,
            item.feature.observation_id,
            H10_POLICY.version,
        )
    )


def _live_outcome_id(observation_id: str) -> str:
    return _fingerprint(
        (LIVE_SHADOW_RECORD_VERSION, observation_id, H10_OUTCOME_POLICY_VERSION)
    )


def _live_observation_payload_fingerprint(item: R2LiveShadowInput) -> str:
    return _fingerprint(
        (
            item.instrument_id,
            item.feature.fingerprint,
            item.dataset_fingerprint,
            item.source_event_ids,
        )
    )


def _live_outcome_payload_fingerprint(
    item: R2LiveShadowInput, *, observation_id: str
) -> str:
    return _fingerprint(
        (
            observation_id,
            item.outcome.fingerprint,
            H10_OUTCOME_POLICY_VERSION,
            item.source_event_ids,
        )
    )


def _record_version(value: datetime) -> int:
    return int(_aware_utc(value).timestamp() * 1_000_000)


def _timestamp(value: datetime) -> str:
    return _aware_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("H10 live-shadow timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _fingerprint(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + sha256(payload).hexdigest()
