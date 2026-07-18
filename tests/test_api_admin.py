"""HTTP API: /health, /ready, ограничения /admin/api."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from tinvest_signal_engine.services import api as api_module


@pytest.fixture
def client_ok(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.delenv("ADMIN_API_ALLOWED_IPS", raising=False)

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_instrument_activity.return_value = {}

    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *args, **kwargs: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        yield client


def test_health_and_ready(client_ok: TestClient) -> None:
    health = client_ok.get("/health").json()
    ready = client_ok.get("/ready").json()
    assert health["status"] == "ok"
    assert ready["status"] == "ready"
    assert health["runtime"]["app_version"]
    assert ready["runtime"]["commit_sha"]


def test_admin_requires_token(client_ok: TestClient) -> None:
    r = client_ok.get("/admin/api/instruments")
    assert r.status_code == 401


def test_admin_with_header(client_ok: TestClient) -> None:
    r = client_ok.get(
        "/admin/api/instruments",
        headers={"X-Admin-Token": "test-secret-token"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["count"] >= 50
    assert {item["instrument_id"] for item in data["items"]} >= {
        "SBER_TQBR",
        "VTBR_TQBR",
        "YDEX_TQBR",
        "OZON_TQBR",
    }


def test_admin_pipeline_status_reports_delivery_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("SIGNAL_DELIVERY_ENABLED", "true")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "123")
    monkeypatch.delenv("REDIS_URL", raising=False)

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_overview.return_value = {
        "totals": {
            "total": 12,
            "last_detected_at": "2026-06-26T09:20:00+00:00",
        }
    }
    mock_store.fetch_admin_delivery_overview.return_value = {
        "totals": {
            "delivered": 3,
            "suppressed": 9,
            "delivery_rate": 0.25,
        },
        "recent_delivered": [
            {"detected_at": "2026-06-26T09:19:00+00:00"}
        ],
    }
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )

    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/pipeline/status",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "warning"
    assert data["metrics"]["generated"] == 12
    assert data["metrics"]["delivered"] == 3
    statuses = {c["id"]: c["status"] for c in data["checks"]}
    for key, value in {
        "postgres": "ok",
        "telegram_config": "ok",
        "delivery_policy": "ok",
        "telegram_delivery": "ok",
        "redis": "warning",
    }.items():
        assert statuses[key] == value
    assert "Postgres" in data["incident_note"]


def test_ready_degraded_when_ping_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    bad = MagicMock()
    bad.ping.return_value = False
    bad.close = MagicMock()
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: bad,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get("/ready")
    assert r.status_code == 503


def test_admin_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "2")
    monkeypatch.delenv("ADMIN_API_ALLOWED_IPS", raising=False)

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_instrument_activity.return_value = {}
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    hdrs = {"X-Admin-Token": "test-secret-token"}
    with TestClient(app) as client:
        assert client.get("/admin/api/instruments", headers=hdrs).status_code == 200
        assert client.get("/admin/api/instruments", headers=hdrs).status_code == 200
        r3 = client.get("/admin/api/instruments", headers=hdrs)
    assert r3.status_code == 429


def test_admin_signal_filters_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_signals_page.return_value = ([], 0)
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    hdrs = {"X-Admin-Token": "test-secret-token"}
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/signals"
            "?delivery_status=suppressed&quality_min=40&quality_max=80"
            "&feedback=noise&severity=2&signal_type=volume_spike",
            headers=hdrs,
        )

    assert r.status_code == 200
    kwargs = mock_store.fetch_admin_signals_page.call_args.kwargs
    assert kwargs["delivery_status"] == "suppressed"
    assert kwargs["quality_min"] == 40
    assert kwargs["quality_max"] == 80
    assert kwargs["feedback"] == "noise"
    assert kwargs["severity"] == 2
    assert kwargs["signal_type"] == "volume_spike"


def test_admin_signal_unlabeled_filter_forwarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_signals_page.return_value = ([], 0)
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/signals?feedback=unlabeled",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    assert mock_store.fetch_admin_signals_page.call_args.kwargs["feedback"] == "unlabeled"


def test_admin_feedback_save_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.upsert_admin_feedback.return_value = None
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.post(
            "/admin/api/feedback",
            headers={"X-Admin-Token": "test-secret-token"},
            json={
                "signal_id": "00000000-0000-4000-8000-000000000001",
                "label": "useful",
                "note": "telegram-worthy",
            },
        )

    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    mock_store.upsert_admin_feedback.assert_called_once_with(
        signal_id="00000000-0000-4000-8000-000000000001",
        label="useful",
        note="telegram-worthy",
    )


def test_admin_instruments_merges_configured_universe_with_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_instrument_activity.return_value = {
        "VTBR_TQBR": {
            "total": 3,
            "delivered": 1,
            "suppressed": 2,
            "unknown": 0,
            "avg_quality": 67.5,
            "last_detected_at": "2026-06-01T10:00:00+00:00",
        }
    }
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )

    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/instruments?minutes=1440",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    data = r.json()
    by_id = {item["instrument_id"]: item for item in data["items"]}
    assert data["count"] >= 50
    assert data["active_count"] == 1
    assert by_id["VTBR_TQBR"]["total"] == 3
    assert by_id["VTBR_TQBR"]["delivery_rate"] == pytest.approx(1 / 3)
    assert by_id["OZON_TQBR"]["total"] == 0


def test_admin_delivery_and_calibration_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_delivery_overview.return_value = {"totals": {}}
    mock_store.fetch_admin_delivery_reasons.return_value = {"items": []}
    mock_store.fetch_admin_calibration.return_value = {"items": []}
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    hdrs = {"X-Admin-Token": "test-secret-token"}
    with TestClient(app) as client:
        assert client.get("/admin/api/delivery/overview", headers=hdrs).status_code == 200
        assert client.get("/admin/api/delivery/reasons", headers=hdrs).status_code == 200
        assert client.get("/admin/api/calibration", headers=hdrs).status_code == 200


def test_admin_feedback_overview_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_feedback_overview.return_value = {
        "summary": {"total": 2, "labeled": 1, "coverage_rate": 0.5},
        "by_type": [],
    }
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/feedback/overview?minutes=1440",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    assert r.json()["summary"]["coverage_rate"] == 0.5
    assert mock_store.fetch_admin_feedback_overview.call_args.kwargs["minutes"] == 1440


def test_admin_source_health_without_clickhouse_is_safe(
    client_ok: TestClient,
) -> None:
    r = client_ok.get(
        "/admin/api/source-health?minutes=1440",
        headers={"X-Admin-Token": "test-secret-token"},
    )

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "unknown"
    assert data["count"] >= 50
    assert data["items"][0]["source_health"]
    assert data["items"][0]["signal_availability"]


def test_admin_instrument_insights_returns_clickhouse_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_instrument_activity.return_value = {}
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    monkeypatch.setattr(
        api_module,
        "fetch_instrument_insights",
        lambda *a, **k: {
            "status": "ok",
            "instrument_id": "SBER_TQBR",
            "volume": {"today": {"turnover_raw": 1000}, "previous_same_time": {"turnover_raw": 800}},
            "aggressive_flow": {"buy_share": 0.6, "sell_share": 0.4},
            "orderbook": {"spread_bps": 2.5},
            "hidden_liquidity": {"possible_iceberg_score": 40},
            "unavailable": {"real_long_short_positions": {"status": "unavailable"}},
        },
    )

    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/instrument-insights/SBER_TQBR",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["instrument"]["ticker"] == "SBER"
    assert data["volume"]["today"]["turnover_raw"] == 1000
    assert data["unavailable"]["real_long_short_positions"]["status"] == "unavailable"


def test_admin_instrument_insights_without_clickhouse_is_safe(client_ok: TestClient) -> None:
    r = client_ok.get(
        "/admin/api/instrument-insights/SBER_TQBR",
        headers={"X-Admin-Token": "test-secret-token"},
    )

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "unavailable"
    assert data["reason_code"] == "clickhouse_not_configured"
    assert data["instrument"]["instrument_id"] == "SBER_TQBR"


def test_admin_accuracy_missing_returns_empty_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("SIGNAL_ACCURACY_JSON_PATH", str(tmp_path / "missing.json"))

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.get(
            "/admin/api/accuracy",
            headers={"X-Admin-Token": "test-secret-token"},
        )

    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "missing"
    assert data["summary"]["horizons"] == []


def test_admin_delivery_simulation_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS", "0")

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_signals_page.return_value = (
        [
            {
                "signal_id": "00000000-0000-4000-8000-000000000001",
                "detected_at": "2026-01-01T12:00:00+00:00",
                "instrument_id": "SBER_TQBR",
                "ticker": "SBER",
                "class_code": "TQBR",
                "alias": "sber",
                "source_event_type": "trade",
                "signal_type": "volume_spike",
                "severity": 3,
                "metric_value": 100.0,
                "baseline_value": 10.0,
                "z_score": 7.0,
                "window_seconds": 60,
                "summary": "x",
                "payload": {"quality_score": 95},
                "delivery_status": "unknown",
                "delivery_reason": "unknown",
            }
        ],
        1,
    )
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.post(
            "/admin/api/delivery/simulation",
            headers={"X-Admin-Token": "test-secret-token"},
            json={"preset": "current", "minutes": 1440, "limit": 10},
        )

    assert r.status_code == 200
    data = r.json()
    assert data["sampled"] == 1
    assert data["items"][0]["simulated_delivery_status"] == "delivered"


def test_admin_delivery_simulation_admin_only_rollout_preset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ADMIN_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ADMIN_API_RATE_LIMIT_PER_MINUTE", "0")
    monkeypatch.setenv("SIGNAL_DELIVERY_INSTRUMENT_COOLDOWN_SECONDS", "0")
    monkeypatch.setenv(
        "SIGNAL_DELIVERY_TYPE_RULES_JSON",
        json.dumps({"candle_range_spike": {"always": True}}),
    )

    mock_store = MagicMock()
    mock_store.ping.return_value = True
    mock_store.close = MagicMock()
    mock_store.fetch_admin_signals_page.return_value = (
        [
            {
                "signal_id": "00000000-0000-4000-8000-000000000002",
                "detected_at": "2026-01-01T12:00:00+00:00",
                "instrument_id": "SBER_TQBR",
                "ticker": "SBER",
                "class_code": "TQBR",
                "alias": "sber",
                "source_event_type": "candle",
                "signal_type": "candle_range_spike",
                "severity": 3,
                "metric_value": 100.0,
                "baseline_value": 10.0,
                "z_score": 7.0,
                "window_seconds": 300,
                "summary": "x",
                "payload": {"quality_score": 95},
                "delivery_status": "delivered",
                "delivery_reason": "type_rule_always",
            }
        ],
        1,
    )
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        r = client.post(
            "/admin/api/delivery/simulation",
            headers={"X-Admin-Token": "test-secret-token"},
            json={"preset": "admin_only_rollout", "minutes": 1440, "limit": 10},
        )

    assert r.status_code == 200
    item = r.json()["items"][0]
    assert item["simulated_delivery_status"] == "suppressed"
    assert item["simulated_delivery_reason"] == "type_rule_admin_only"
    assert item["simulated_delivery_channel"] == "admin_only"


def test_admin_settings_exposes_configured_signal_catalog(
    client_ok: TestClient,
) -> None:
    r = client_ok.get(
        "/admin/api/settings",
        headers={"X-Admin-Token": "test-secret-token"},
    )

    assert r.status_code == 200
    payload = r.json()
    assert payload["runtime"]["app_version"]
    signals = payload["signals"]
    enabled = {row["signal_type"] for row in signals["enabled_types"]}
    assert signals["enabled_count"] > 1
    assert {
        "volume_spike",
        "trade_rate_spike",
        "price_jump",
        "spread_widening",
        "orderbook_imbalance",
        "microstructure_combo_long",
        "microstructure_combo_short",
    } <= enabled
