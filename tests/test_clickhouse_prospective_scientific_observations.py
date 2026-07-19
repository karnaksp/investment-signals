from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from urllib.parse import parse_qs, urlparse

import pytest

from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveScientificStore,
    _observation_row,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
    ProspectiveEvidenceConflict,
    ProspectiveObservationProvenance,
    ProspectiveScientificOutcome,
    ProspectiveSourceKind,
    build_prospective_observation,
    deterministic_prospective_outcome_id,
    prospective_outcome_payload_fingerprint,
)
from tinvest_signal_engine.domain.scientific_candle_models import (
    ScientificCandlePolicy,
    relative_volume_activity_feature,
    variance_outcome,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 20, 9, 15, tzinfo=UTC)
POLICY = ScientificCandlePolicy()
SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64


class _Response:
    def __init__(self, payload: bytes = b"") -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._payload


def _store() -> ClickHouseProspectiveScientificStore:
    return ClickHouseProspectiveScientificStore(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
    )


def _observation():
    feature = relative_volume_activity_feature(
        ticker="SBER",
        trading_day=date(2026, 7, 20),
        observed_at=OBSERVED_AT,
        current_volume=1_000.0,
        historical_phase_volumes=tuple(float(value) for value in range(20)),
        baseline_future_variance=2.0,
        policy=POLICY,
    )
    return build_prospective_observation(
        instrument_id="SBER_TQBR",
        feature=feature,
        policy_version=POLICY.version,
        formula_version="relative-volume-activity-v2",
        provenance=ProspectiveObservationProvenance(
            source_kind=ProspectiveSourceKind.STREAM,
            source_event_ids=("candle-1", "candle-2"),
            source_window_start=OBSERVED_AT - timedelta(minutes=14),
            source_window_end=OBSERVED_AT,
            source_max_observed_at=OBSERVED_AT,
            input_fingerprint=SHA_A,
            dataset_fingerprint=SHA_B,
            scientific_source_ids=("Heston-Korajczyk-Sadka-2010",),
        ),
        recorded_at=OBSERVED_AT + timedelta(seconds=1),
    )


def _outcome(observation):
    result = variance_outcome(
        observation.feature,
        target_at=observation.target_at,
        actual_future_variance=3.0,
        policy=POLICY,
    )
    result = replace(result, observation_id=observation.observation_id)
    policy_version = "prospective-outcome-v1"
    fingerprint = prospective_outcome_payload_fingerprint(
        observation_id=observation.observation_id,
        hypothesis=observation.feature.hypothesis,
        hypothesis_version=observation.feature.hypothesis_version,
        instrument_id=observation.instrument_id,
        trading_day=observation.feature.trading_day,
        target=observation.feature.target,
        result=result,
        outcome_policy_version=policy_version,
        source_event_ids=("future-candle",),
        source_window_start=observation.feature.observed_at,
        source_window_end=observation.target_at,
        source_max_observed_at=observation.target_at,
        input_fingerprint=SHA_B,
    )
    return ProspectiveScientificOutcome(
        outcome_id=deterministic_prospective_outcome_id(
            observation_id=observation.observation_id,
            outcome_policy_version=policy_version,
        ),
        observation_id=observation.observation_id,
        hypothesis=observation.feature.hypothesis,
        hypothesis_version=observation.feature.hypothesis_version,
        instrument_id=observation.instrument_id,
        trading_day=observation.feature.trading_day,
        target=observation.feature.target,
        target_at=observation.target_at,
        result=result,
        outcome_policy_version=policy_version,
        source_event_ids=("future-candle",),
        source_window_start=observation.feature.observed_at,
        source_window_end=observation.target_at,
        source_max_observed_at=observation.target_at,
        input_fingerprint=SHA_B,
        evaluated_at=observation.target_at + timedelta(seconds=30),
        payload_fingerprint=fingerprint,
    )


def _mock_http(monkeypatch, responses):
    captured = []
    queue = list(responses)

    def fake_urlopen(request, *, timeout):
        captured.append((request, timeout))
        return _Response(queue.pop(0))

    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations.urlopen",
        fake_urlopen,
    )
    return captured


def _fingerprint_response(value: str) -> bytes:
    return (json.dumps({"payload_fingerprint": value}) + "\n").encode()


def test_clickhouse_observation_insert_is_verified_and_contract_aligned(
    monkeypatch,
) -> None:
    observation = _observation()
    captured = _mock_http(
        monkeypatch,
        (b"", b"", _fingerprint_response(observation.payload_fingerprint)),
    )

    disposition = _store().persist_observation(observation)

    assert disposition is PersistenceDisposition.INSERTED
    assert len(captured) == 3
    preflight_sql = captured[0][0].data.decode()
    insert_body = captured[1][0].data.decode()
    assert "scientific_hypothesis_observations" in preflight_sql
    assert observation.observation_id not in preflight_sql
    preflight_query = parse_qs(urlparse(captured[0][0].full_url).query)
    assert preflight_query["param_identity"] == [observation.observation_id]
    _, row_text = insert_body.split("FORMAT JSONEachRow\n", 1)
    row = json.loads(row_text)
    assert row["observation_id"].startswith("sha256:")
    assert row["payload_fingerprint"] == observation.payload_fingerprint
    assert row["formula_version"] == "relative-volume-activity-v2"
    assert row["scientific_source_ids"] == ["Heston-Korajczyk-Sadka-2010"]
    assert row["target_metric"] == "future_activity_uplift"
    assert row["source_kind"] == "stream"
    assert row["source_max_observed_at"] == "2026-07-20 09:15:00.000000"
    assert row["claim_scope"] == "shadow"
    assert row["record_version"] > 0
    assert captured[1][0].headers["X-clickhouse-key"] == "secret"


def test_clickhouse_observation_same_fingerprint_is_replay(monkeypatch) -> None:
    observation = _observation()
    captured = _mock_http(
        monkeypatch,
        (_fingerprint_response(observation.payload_fingerprint),),
    )

    disposition = _store().persist_observation(observation)

    assert disposition is PersistenceDisposition.REPLAYED
    assert len(captured) == 1


def test_clickhouse_observation_conflicting_fingerprint_is_rejected(
    monkeypatch,
) -> None:
    observation = _observation()
    _mock_http(monkeypatch, (_fingerprint_response(SHA_A),))

    with pytest.raises(ProspectiveEvidenceConflict):
        _store().persist_observation(observation)


def test_clickhouse_outcome_insert_carries_source_lineage(monkeypatch) -> None:
    outcome = _outcome(_observation())
    captured = _mock_http(
        monkeypatch,
        (b"", b"", _fingerprint_response(outcome.payload_fingerprint)),
    )

    disposition = _store().persist_outcome(outcome)

    assert disposition is PersistenceDisposition.INSERTED
    insert_body = captured[1][0].data.decode()
    _, row_text = insert_body.split("FORMAT JSONEachRow\n", 1)
    row = json.loads(row_text)
    assert row["outcome_id"].startswith("sha256:")
    assert row["observation_id"] == outcome.observation_id
    assert row["available"] == 1
    assert row["cost_adjusted_value"] is None
    assert row["target_metric"] == "future_activity_uplift"
    assert row["outcome_policy_version"] == "prospective-outcome-v1"
    assert row["source_event_ids"] == ["future-candle"]
    assert row["evaluated_at"] == "2026-07-20 09:45:30.000000"


def test_pending_query_excludes_policy_outcomes_but_not_by_wall_clock(
    monkeypatch,
) -> None:
    observation = _observation()
    row = _observation_row(observation)
    captured = _mock_http(monkeypatch, ((json.dumps(row) + "\n").encode(),))

    pending = _store().pending_observations(
        outcome_policy_version="prospective-outcome-v1",
        limit=25,
    )

    assert pending == (observation,)
    sql = captured[0][0].data.decode()
    query = parse_qs(urlparse(captured[0][0].full_url).query)
    assert "LEFT ANTI JOIN" in sql
    assert "scientific_hypothesis_outcomes" in sql
    assert "now()" not in sql
    assert "target_at <=" not in sql
    assert query["param_outcome_policy_version"] == ["prospective-outcome-v1"]
    assert query["param_limit"] == ["25"]


def test_pending_query_rejects_conflicting_replacing_tree_versions(
    monkeypatch,
) -> None:
    observation = _observation()
    canonical = dict(_observation_row(observation))
    conflict = dict(canonical)
    conflict["payload_fingerprint"] = SHA_A
    payload = (json.dumps(canonical) + "\n" + json.dumps(conflict) + "\n").encode()
    _mock_http(monkeypatch, (payload,))

    with pytest.raises(ProspectiveEvidenceConflict):
        _store().pending_observations(
            outcome_policy_version="prospective-outcome-v1",
            limit=25,
        )
