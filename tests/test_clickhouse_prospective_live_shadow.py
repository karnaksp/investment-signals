from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
import json
from pathlib import Path
from threading import Event
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import pytest

from tinvest_signal_engine.adapters.clickhouse_resilience import (
    BoundedExponentialBackoff,
    TransientClickHouseError,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_live_shadow import (
    _CANDLE_COLUMNS,
    _calendar_lookback_days,
    _compact_json_each_row,
    _series_point,
    ClickHouseProspectiveLiveOutcomeSource,
    ClickHouseProspectiveLiveSnapshotSource,
)
from tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations import (
    ClickHouseProspectiveLiveShadowStore,
    _live_observation_from_row,
    _live_observation_row,
    _live_outcome_from_row,
    _live_outcome_row,
)
from tinvest_signal_engine.adapters.in_memory_prospective_live_shadow import (
    InMemoryProspectiveLiveShadowStore,
)
from tinvest_signal_engine.application.prospective_live_shadow import (
    HarFeatureInput,
    JumpFeatureInput,
    ProspectivePortfolioSnapshot,
    RecordProspectivePortfolioSnapshot,
    RelativeVolumeFeatureInput,
    SemivarianceFeatureInput,
    VolatilityJumpFeatureInput,
)
from tinvest_signal_engine.domain.prospective_live_shadow import (
    LIVE_SHADOW_RECORD_VERSION,
    build_live_outcome,
)
from tinvest_signal_engine.domain.prospective_scientific_models import (
    ProspectiveHypothesis,
    ProspectiveScientificPolicy,
    TargetMetric,
    directional_outcome,
)
from tinvest_signal_engine.domain.prospective_scientific_observations import (
    PersistenceDisposition,
)
from tinvest_signal_engine.domain.scientific_candles import (
    ScientificCandle,
    scientific_candle_fingerprint,
)
from tinvest_signal_engine.services.prospective_live_shadow_worker import (
    PRODUCTION_LIVE_POLICY,
    ProspectiveSnapshotBatchSchedule,
    _instrument_ids,
    build_clickhouse_prospective_live_shadow_runtime,
    run_worker_loop,
)


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 7, 20, 9, 30, tzinfo=UTC)
RECORDED_AT = OBSERVED_AT + timedelta(seconds=2)
SHA_A = "sha256:" + "a" * 64
SHA_B = "sha256:" + "b" * 64


class _RecoveringRuntime:
    def __init__(self) -> None:
        self.calls = 0

    def run_once(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            raise ConnectionError("clickhouse is starting")
        raise KeyboardInterrupt


class _Response:
    def __init__(self, payload: bytes = b"") -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._payload


class _CandleClient:
    def __init__(self, rows: tuple[dict[str, object], ...]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, str]]] = []

    def _request(self, sql, *, parameters):
        self.calls.append((sql, dict(parameters)))
        return b"".join(
            (
                json.dumps(
                    [row[column] for column in _CANDLE_COLUMNS],
                    separators=(",", ":"),
                )
                + "\n"
            ).encode()
            for row in self.rows
        )


def _clickhouse_store() -> ClickHouseProspectiveLiveShadowStore:
    return ClickHouseProspectiveLiveShadowStore(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
        instrument_ids=("SBER_TQBR",),
    )


def test_live_worker_retries_after_transient_dependency_failure(caplog) -> None:
    runtime = _RecoveringRuntime()
    sleeps: list[float] = []

    with pytest.raises(KeyboardInterrupt):
        run_worker_loop(
            runtime,  # type: ignore[arg-type]
            snapshot_limit=25,
            outcome_limit=100,
            poll_seconds=0.25,
            now=lambda: OBSERVED_AT,
            sleep=sleeps.append,
        )

    assert runtime.calls == 2
    assert sleeps == [0.25]
    assert "retrying after dependency recovery" in caplog.text


def test_live_worker_uses_bounded_backoff_for_classified_clickhouse_failure() -> None:
    class Runtime:
        calls = 0

        def run_once(self, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TransientClickHouseError(
                    operation="scientific_evidence_request",
                    reason_code="http_500",
                )
            raise KeyboardInterrupt

    class RecordingEvent(Event):
        waits: list[float]

        def __init__(self):
            super().__init__()
            self.waits = []

        def wait(self, timeout=None):
            self.waits.append(timeout)
            return False

    runtime = Runtime()
    stop = RecordingEvent()

    with pytest.raises(KeyboardInterrupt):
        run_worker_loop(
            runtime,  # type: ignore[arg-type]
            snapshot_limit=25,
            outcome_limit=100,
            poll_seconds=60,
            stop_event=stop,
            backoff=BoundedExponentialBackoff(
                base_seconds=0.25,
                maximum_seconds=1,
            ),
            now=lambda: OBSERVED_AT,
            random_value=lambda: 0.5,
        )

    assert runtime.calls == 2
    assert stop.waits == [0.25]


def test_live_store_classifies_clickhouse_500_without_response_body(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "tinvest_signal_engine.adapters.clickhouse_prospective_scientific_observations.urlopen",
        lambda request, timeout: (_ for _ in ()).throw(
            HTTPError(
                request.full_url,
                500,
                "server error",
                {},
                BytesIO(b"secret-response-body"),
            )
        ),
    )

    with pytest.raises(TransientClickHouseError) as raised:
        _clickhouse_store().pending_observations(
            outcome_policy_version="prospective-live-outcomes-v1",
            limit=7,
        )

    assert raised.value.reason_code == "http_500"
    assert "secret" not in str(raised.value)


def _snapshot() -> ProspectivePortfolioSnapshot:
    return ProspectivePortfolioSnapshot(
        instrument_id="SBER_TQBR",
        ticker="SBER",
        trading_day=OBSERVED_AT.date(),
        observed_at=OBSERVED_AT,
        recorded_at=RECORDED_AT,
        source_event_ids=("candle-1",),
        dataset_fingerprint=SHA_A,
        input_fingerprint=SHA_B,
        trading_gap=False,
        jump=JumpFeatureInput(1.0, 1.0, 1.0, 1.0, (), None),
        relative_volume=RelativeVolumeFeatureInput(1.0, (), 0.0, None),
        har=HarFeatureInput(1.0, 1.0, 1.0, None),
        semivariance=SemivarianceFeatureInput(0.1, (), 0.0, None),
        volatility_jump=VolatilityJumpFeatureInput(0.1, 1.0, (), 0.0, None),
    )


def _live_observation(*, target: TargetMetric = TargetMetric.FORWARD_RETURN):
    store = InMemoryProspectiveLiveShadowStore()
    result = RecordProspectivePortfolioSnapshot(
        store=store,
        policy=PRODUCTION_LIVE_POLICY,
    ).execute(_snapshot())
    assert len(result.observation_ids) == 6
    return next(item for item in store.observations() if item.feature.target is target)


def _live_outcome(observation):
    result = directional_outcome(
        observation.feature,
        target_at=observation.target_at,
        forward_return_bps=20.0,
        round_trip_cost_bps=10.0,
    )
    return build_live_outcome(
        observation=observation,
        outcome=result,
        outcome_policy_version="prospective-live-outcomes-v1",
        evidence_fingerprint=SHA_A,
        evaluated_at=observation.target_at + timedelta(seconds=5),
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


def test_live_clickhouse_rows_round_trip_new_schema_columns() -> None:
    observation = _live_observation()
    outcome = _live_outcome(observation)

    observation_row = _live_observation_row(observation)
    outcome_row = _live_outcome_row(outcome, observation)

    assert observation_row["record_schema_version"] == LIVE_SHADOW_RECORD_VERSION
    assert json.loads(str(observation_row["feature_values_json"]))["values"]
    assert outcome_row["record_schema_version"] == LIVE_SHADOW_RECORD_VERSION
    assert outcome_row["evidence_fingerprint"] == SHA_A
    assert "values" in json.loads(str(outcome_row["measurements_json"]))
    assert _live_outcome_from_row(outcome_row) == outcome


def test_live_clickhouse_round_trip_preserves_integer_zero_fingerprint() -> None:
    store = InMemoryProspectiveLiveShadowStore()
    RecordProspectivePortfolioSnapshot(
        store=store,
        policy=PRODUCTION_LIVE_POLICY,
    ).execute(replace(_snapshot(), har=HarFeatureInput(0, 0, 0, None)))
    observation = next(
        item
        for item in store.observations()
        if item.feature.target.value == "future_realized_variance"
    )
    row = _live_observation_row(observation)

    assert _live_observation_from_row(row) == observation


def test_live_clickhouse_store_persists_and_reads_immutable_records(
    monkeypatch,
) -> None:
    observation = _live_observation()
    observation_row = _live_observation_row(observation)
    captured = _mock_http(
        monkeypatch,
        (b"", b"", _fingerprint_response(observation.payload_fingerprint)),
    )

    disposition = _clickhouse_store().persist_observation(observation)

    assert disposition is PersistenceDisposition.INSERTED
    inserted = json.loads(captured[1][0].data.decode().split("JSONEachRow\n", 1)[1])
    assert inserted["record_schema_version"] == LIVE_SHADOW_RECORD_VERSION
    assert inserted["observation_id"] == observation.observation_id
    insert_query = parse_qs(urlparse(captured[1][0].full_url).query)
    assert insert_query["param_async_insert"] == ["1"]
    assert insert_query["param_wait_for_async_insert"] == ["1"]

    captured = _mock_http(
        monkeypatch,
        ((json.dumps(observation_row) + "\n").encode(),),
    )
    assert _clickhouse_store().pending_observations(
        outcome_policy_version="prospective-live-outcomes-v1",
        limit=7,
    ) == (observation,)
    query = parse_qs(urlparse(captured[0][0].full_url).query)
    assert query["param_record_schema_version"] == [LIVE_SHADOW_RECORD_VERSION]
    assert query["param_limit"] == ["7"]


def test_live_clickhouse_store_persists_outcome_with_observation_lineage(
    monkeypatch,
) -> None:
    observation = _live_observation()
    outcome = _live_outcome(observation)
    captured = _mock_http(
        monkeypatch,
        (
            (json.dumps(_live_observation_row(observation)) + "\n").encode(),
            b"",
            b"",
            _fingerprint_response(outcome.payload_fingerprint),
        ),
    )

    disposition = _clickhouse_store().persist_outcome(outcome)

    assert disposition is PersistenceDisposition.INSERTED
    inserted = json.loads(captured[2][0].data.decode().split("JSONEachRow\n", 1)[1])
    assert inserted["observation_id"] == observation.observation_id
    assert inserted["evidence_fingerprint"] == SHA_A
    assert inserted["measurements_json"]


def test_snapshot_source_uses_only_causal_completed_candles_and_seals_six() -> None:
    first = OBSERVED_AT - timedelta(minutes=120)
    candles = tuple(
        _candle(first + timedelta(minutes=index), index) for index in range(120)
    )
    client = _CandleClient(tuple(_candle_row(item) for item in candles))

    snapshots = ClickHouseProspectiveLiveSnapshotSource(
        client,
        instrument_ids=("SBER_TQBR",),
    ).load_snapshots(
        as_of=RECORDED_AT,
        policy=PRODUCTION_LIVE_POLICY,
        limit=10,
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.observed_at == OBSERVED_AT
    assert snapshot.recorded_at == RECORDED_AT
    assert snapshot.source_event_ids[-1] == "candle-119"
    assert client.calls[0][1]["as_of"] == "2026-07-20 09:30:02.000000"
    sql, parameters = client.calls[0]
    assert "PREWHERE instrument_id =" in sql
    assert "trading_day >=" in sql
    assert "ORDER BY trading_day DESC, candle_at DESC" in sql
    assert "LIMIT 75001" in sql
    assert "max_rows_to_read = 200000" in sql
    assert "max_bytes_to_read = 268435456" in sql
    assert "max_result_rows = 75001" in sql
    assert "max_result_bytes = 33554432" in sql
    assert "result_overflow_mode = 'throw'" in sql
    assert "max_memory_usage = 100663296" in sql
    assert "max_threads = 1" in sql
    assert "max_execution_time = 30" in sql
    assert "timeout_before_checking_execution_speed = 0" in sql
    assert "FORMAT JSONCompactEachRow" in sql
    assert parameters["lookback_start"] == (
        RECORDED_AT - timedelta(days=98)
    ).strftime("%Y-%m-%d %H:%M:%S.%f")
    assert parameters["lookback_start"] < parameters["as_of"]
    assert parameters["instrument_id"] == "SBER_TQBR"
    store = InMemoryProspectiveLiveShadowStore()
    result = RecordProspectivePortfolioSnapshot(
        store=store,
        policy=PRODUCTION_LIVE_POLICY,
    ).execute(snapshot)
    assert result.stored == 6
    assert all(
        item.feature.feature_max_observed_at <= OBSERVED_AT
        for item in store.observations()
    )


def test_snapshot_source_accepts_clickhouse_decimal_storage_scale() -> None:
    first = OBSERVED_AT - timedelta(minutes=120)
    candles = tuple(
        _canonical_price_candle(first + timedelta(minutes=index), index)
        for index in range(120)
    )
    rows = []
    for candle in candles:
        row = _candle_row(candle)
        for column in ("open_price", "high_price", "low_price", "close_price"):
            row[column] = format(Decimal(str(row[column])), ".9f")
        rows.append(row)

    snapshots = ClickHouseProspectiveLiveSnapshotSource(
        _CandleClient(tuple(rows)),
        instrument_ids=("SBER_TQBR",),
    ).load_snapshots(
        as_of=RECORDED_AT,
        policy=PRODUCTION_LIVE_POLICY,
        limit=1,
    )

    assert len(snapshots) == 1
    assert snapshots[0].observed_at == OBSERVED_AT


def test_snapshot_source_rejects_tampered_scaled_decimal() -> None:
    candle = _candle(OBSERVED_AT - timedelta(minutes=1), 1)
    row = _candle_row(candle)
    row["close_price"] = format(candle.close_price + Decimal("0.01"), ".9f")

    with pytest.raises(ValueError, match="fingerprint does not match content"):
        ClickHouseProspectiveLiveSnapshotSource(
            _CandleClient((row,)),
            instrument_ids=("SBER_TQBR",),
        ).load_snapshots(
            as_of=RECORDED_AT,
            policy=PRODUCTION_LIVE_POLICY,
            limit=1,
        )


def test_backfill_row_restores_parquet_float_integer_scale() -> None:
    candle_at = datetime(2026, 7, 21, 20, 37, tzinfo=UTC)
    source_at = candle_at + timedelta(minutes=1)
    fields = {
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "exchange": "TQBR",
        "candle_at": candle_at,
        "open_price": Decimal("262.94"),
        "high_price": Decimal("263.0"),
        "low_price": Decimal("262.84"),
        "close_price": Decimal("262.89"),
        "volume": 1_000,
        "complete": True,
        "source_kind": "backfill",
        "source_at": source_at,
        "source_event_id": "backfill-v1:SBER_TQBR:2026-07-21T20:37:00+00:00",
        "has_gap": False,
        "schema_version": "scientific-candle-v1",
    }
    candle = ScientificCandle(
        **fields,
        trading_day=candle_at.date(),
        received_at=source_at,
        payload_fingerprint=scientific_candle_fingerprint(**fields),
    )
    row = _candle_row(candle)
    for column in ("open_price", "high_price", "low_price", "close_price"):
        row[column] = format(Decimal(str(row[column])), ".9f")

    restored = _series_point(row, cutoff=source_at + timedelta(seconds=1)).candle

    assert restored.payload_fingerprint == candle.payload_fingerprint
    assert str(restored.high_price) == "263.0"


def test_outcome_source_reads_as_of_now_but_seals_evidence_at_target() -> None:
    observation = _live_observation()
    first = OBSERVED_AT - timedelta(minutes=120)
    count_through_target = 120 + observation.feature.horizon_seconds // 60
    candles = tuple(
        _candle(first + timedelta(minutes=index), index)
        for index in range(count_through_target + 2)
    )
    client = _CandleClient(tuple(_candle_row(item) for item in candles))
    as_of = observation.target_at + timedelta(minutes=2)

    evidence = ClickHouseProspectiveLiveOutcomeSource(client).load(
        observation,
        as_of=as_of,
    )

    assert evidence.available is True
    assert evidence.actual_value is not None
    assert client.calls[0][1]["as_of"] == as_of.strftime("%Y-%m-%d %H:%M:%S.%f")
    sql, parameters = client.calls[0]
    assert "PREWHERE instrument_id =" in sql
    assert "LIMIT 4097" in sql
    assert "max_rows_to_read = 8192" in sql
    assert "max_bytes_to_read = 16777216" in sql
    assert "max_result_rows = 4097" in sql
    assert "max_result_bytes = 8388608" in sql
    assert "result_overflow_mode = 'throw'" in sql
    assert "max_memory_usage = 33554432" in sql
    assert "FORMAT JSONCompactEachRow" in sql
    assert parameters["instrument_id"] == "SBER_TQBR"
    assert parameters["lookback_start"] == (
        observation.feature.observed_at - timedelta(minutes=1)
    ).strftime("%Y-%m-%d %H:%M:%S.%f")
    assert parameters["candle_until"] == observation.target_at.strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    with pytest.raises(ValueError, match="before target_at"):
        ClickHouseProspectiveLiveOutcomeSource(client).load(
            observation,
            as_of=observation.target_at - timedelta(seconds=1),
        )


def test_har_outcome_keeps_bounded_historical_baseline_window() -> None:
    observation = _live_observation(target=TargetMetric.FUTURE_REALIZED_VARIANCE)
    first = OBSERVED_AT - timedelta(minutes=120)
    count_through_target = 120 + observation.feature.horizon_seconds // 60
    candles = tuple(
        _candle(first + timedelta(minutes=index), index)
        for index in range(count_through_target + 2)
    )
    client = _CandleClient(tuple(_candle_row(item) for item in candles))

    ClickHouseProspectiveLiveOutcomeSource(client).load(
        observation,
        as_of=observation.target_at + timedelta(minutes=2),
    )

    sql, parameters = client.calls[0]
    assert "LIMIT 75001" in sql
    assert "max_rows_to_read = 200000" in sql
    assert parameters["lookback_start"] == (
        observation.feature.observed_at - timedelta(days=98)
    ).strftime("%Y-%m-%d %H:%M:%S.%f")


def test_policy_seals_sufficient_history_for_every_supported_hypothesis() -> None:
    policy = ProspectiveScientificPolicy()
    requirements = dict(policy.required_history_trading_days_by_hypothesis())

    assert set(requirements) == set(ProspectiveHypothesis)
    assert requirements == {
        ProspectiveHypothesis.MORNING_LOW_VOLUME_REVERSION: 40,
        ProspectiveHypothesis.MORNING_HIGH_VOLUME_CONTINUATION: 40,
        ProspectiveHypothesis.JUMP_LOW_ACTIVITY_REVERSAL_V2: 40,
        ProspectiveHypothesis.JUMP_HIGH_ACTIVITY_CONTINUATION_V2: 40,
        ProspectiveHypothesis.SAME_PHASE_RETURN_RECURRENCE: 20,
        ProspectiveHypothesis.OPEN_CLOSE_MARKET_CONTINUATION: 1,
        ProspectiveHypothesis.RELATIVE_VOLUME_VOLATILITY_V3: 40,
        ProspectiveHypothesis.PAIR_RESIDUAL_REVERSION: 2,
        ProspectiveHypothesis.HAR_VOLATILITY_V2: 56,
        ProspectiveHypothesis.DOWNSIDE_SEMIVARIANCE_RISK: 40,
        ProspectiveHypothesis.VOLATILITY_JUMP_PERSISTENCE: 60,
    }
    assert policy.required_history_trading_days == 60
    assert _calendar_lookback_days(policy) == 98


def test_history_query_span_follows_policy_instead_of_fixed_snapshot_age() -> None:
    policy = ProspectiveScientificPolicy(jump_variance_history_days=75)

    assert policy.required_history_trading_days == 75
    assert _calendar_lookback_days(policy) == 119


def test_compact_candle_decoder_enforces_byte_row_and_shape_bounds() -> None:
    valid = json.dumps([None] * len(_CANDLE_COLUMNS)).encode() + b"\n"

    assert len(
        tuple(
            _compact_json_each_row(
                valid,
                max_payload_bytes=len(valid),
                max_rows=1,
            )
        )
    ) == 1
    with pytest.raises(ValueError, match="byte limit"):
        tuple(
            _compact_json_each_row(
                valid,
                max_payload_bytes=len(valid) - 1,
                max_rows=1,
            )
        )
    with pytest.raises(ValueError, match="row limit"):
        tuple(
            _compact_json_each_row(
                valid + valid,
                max_payload_bytes=len(valid) * 2,
                max_rows=1,
            )
        )
    malformed = json.dumps([None] * (len(_CANDLE_COLUMNS) - 1)).encode()
    with pytest.raises(ValueError, match="invalid JSONCompactEachRow"):
        tuple(
            _compact_json_each_row(
                malformed,
                max_payload_bytes=len(malformed),
                max_rows=1,
            )
        )


def test_snapshot_source_rejects_candle_metadata_from_after_as_of() -> None:
    candle = _candle(OBSERVED_AT - timedelta(minutes=1), 1)
    row = _candle_row(candle)
    row["received_at"] = (RECORDED_AT + timedelta(microseconds=1)).isoformat()
    client = _CandleClient((row,))

    with pytest.raises(ValueError, match="beyond cutoff"):
        ClickHouseProspectiveLiveSnapshotSource(
            client,
            instrument_ids=("SBER_TQBR",),
        ).load_snapshots(
            as_of=RECORDED_AT,
            policy=PRODUCTION_LIVE_POLICY,
            limit=1,
        )


def test_snapshot_schedule_does_not_require_exact_half_hour_poll() -> None:
    instruments = tuple(f"INSTRUMENT_{index}" for index in range(5))
    schedule = ProspectiveSnapshotBatchSchedule(
        instrument_ids=instruments,
        batch_size=2,
    )

    first = schedule.pending(
        now=datetime(2026, 7, 20, 9, 37, tzinfo=UTC),
        limit=25,
    )
    assert first is not None
    assert first.slot_at == datetime(2026, 7, 20, 9, 30, tzinfo=UTC)
    assert first.instrument_ids == instruments[:2]
    assert (
        schedule.pending(
            now=datetime(2026, 7, 20, 9, 38, tzinfo=UTC),
            limit=25,
        )
        == first
    )

    schedule.complete(first)
    second = schedule.pending(
        now=datetime(2026, 7, 20, 9, 39, tzinfo=UTC),
        limit=25,
    )
    assert second is not None
    assert second.instrument_ids == instruments[2:4]
    schedule.complete(second)
    third = schedule.pending(
        now=datetime(2026, 7, 20, 9, 40, tzinfo=UTC),
        limit=25,
    )
    assert third is not None
    assert third.instrument_ids == instruments[4:]
    schedule.complete(third)
    assert (
        schedule.pending(
            now=datetime(2026, 7, 20, 9, 59, tzinfo=UTC),
            limit=25,
        )
        is None
    )

    next_slot = schedule.pending(
        now=datetime(2026, 7, 20, 10, 7, tzinfo=UTC),
        limit=25,
    )
    assert next_slot is not None
    assert next_slot.slot_at == datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    assert next_slot.instrument_ids == instruments[:2]


def test_production_composition_wires_all_twenty_five_instruments() -> None:
    instrument_ids = tuple(f"INSTRUMENT_{index}" for index in range(25))

    runtime = build_clickhouse_prospective_live_shadow_runtime(
        base_url="http://clickhouse:8123",
        database="signal_engine",
        username="writer",
        password="secret",
        instrument_ids=instrument_ids,
    )

    assert runtime.snapshot_source._instrument_ids == instrument_ids
    assert runtime.store._live_instrument_ids == instrument_ids
    assert runtime.snapshot_schedule.instrument_ids == instrument_ids
    assert runtime.snapshot_schedule.batch_size == 1
    assert runtime.policy.jump_horizons_seconds == (900,)


def test_worker_uses_active_legacy_instruments_without_candle_subscription(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "instruments.yaml"
    config.write_text(
        """
instruments:
  - ticker: SBER
    class_code: TQBR
    subscriptions:
      trades: true
      last_price: true
      candles: false
      candle_interval: 1m
  - ticker: GAZP
    class_code: TQBR
    subscriptions:
      trades: false
      last_price: false
      candles: false
      candle_interval: 1m
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.delenv("PROSPECTIVE_LIVE_INSTRUMENT_IDS", raising=False)
    monkeypatch.setenv("INSTRUMENTS_CONFIG", str(config))

    assert _instrument_ids(25) == ("SBER_TQBR",)


def test_worker_prefers_one_minute_candle_instruments_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "instruments.yaml"
    config.write_text(
        """
instruments:
  - ticker: SBER
    class_code: TQBR
    subscriptions:
      trades: true
      candles: false
  - ticker: GAZP
    class_code: TQBR
    subscriptions:
      trades: true
      candles: true
      candle_interval: 1m
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.delenv("PROSPECTIVE_LIVE_INSTRUMENT_IDS", raising=False)
    monkeypatch.setenv("INSTRUMENTS_CONFIG", str(config))

    assert _instrument_ids(25) == ("GAZP_TQBR",)


def _candle(candle_at: datetime, index: int) -> ScientificCandle:
    price = Decimal("100") + Decimal(index) / Decimal("100")
    source_at = candle_at + timedelta(seconds=59)
    source_event_id = f"candle-{index}"
    fields = {
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "exchange": "MOEX",
        "candle_at": candle_at,
        "open_price": price,
        "high_price": price + Decimal("0.01"),
        "low_price": price - Decimal("0.01"),
        "close_price": price,
        "volume": 100 + index,
        "complete": True,
        "source_kind": "stream",
        "source_at": source_at,
        "source_event_id": source_event_id,
        "has_gap": False,
        "schema_version": "scientific-candle-v1",
    }
    return ScientificCandle(
        **fields,
        trading_day=candle_at.date(),
        received_at=source_at,
        payload_fingerprint=scientific_candle_fingerprint(**fields),
    )


def _canonical_price_candle(candle_at: datetime, index: int) -> ScientificCandle:
    def quotation(value: Decimal) -> Decimal:
        return Decimal(format(value.normalize(), "f"))

    price = quotation(Decimal("100") + Decimal(index) / 100)
    source_at = candle_at + timedelta(seconds=59)
    fields = {
        "instrument_id": "SBER_TQBR",
        "ticker": "SBER",
        "exchange": "MOEX",
        "candle_at": candle_at,
        "open_price": price,
        "high_price": quotation(price + Decimal("0.01")),
        "low_price": quotation(price - Decimal("0.01")),
        "close_price": price,
        "volume": 100 + index,
        "complete": True,
        "source_kind": "stream",
        "source_at": source_at,
        "source_event_id": f"candle-{index}",
        "has_gap": False,
        "schema_version": "scientific-candle-v1",
    }
    return ScientificCandle(
        **fields,
        trading_day=candle_at.date(),
        received_at=source_at,
        payload_fingerprint=scientific_candle_fingerprint(**fields),
    )


def _candle_row(candle: ScientificCandle) -> dict[str, object]:
    return {
        "instrument_id": candle.instrument_id,
        "ticker": candle.ticker,
        "exchange": candle.exchange,
        "trading_day": candle.trading_day.isoformat(),
        "candle_at": candle.candle_at.isoformat(),
        "open_price": str(candle.open_price),
        "high_price": str(candle.high_price),
        "low_price": str(candle.low_price),
        "close_price": str(candle.close_price),
        "volume": candle.volume,
        "is_complete": "1",
        "source_kind": candle.source_kind,
        "source_at": candle.source_at.isoformat(),
        "received_at": candle.received_at.isoformat(),
        "source_event_id": candle.source_event_id,
        "payload_fingerprint": candle.payload_fingerprint,
        "has_gap": "0",
        "schema_version": candle.schema_version,
        "record_version": 1,
    }
