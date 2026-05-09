"""HTTP API: /health, /ready, ограничения /admin/api."""

from __future__ import annotations

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

    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *args, **kwargs: mock_store,
    )
    app = api_module.create_app()
    with TestClient(app) as client:
        yield client


def test_health_and_ready(client_ok: TestClient) -> None:
    assert client_ok.get("/health").json() == {"status": "ok"}
    assert client_ok.get("/ready").json() == {"status": "ready"}


def test_admin_requires_token(client_ok: TestClient) -> None:
    r = client_ok.get("/admin/api/instruments")
    assert r.status_code == 401


def test_admin_with_header(client_ok: TestClient) -> None:
    r = client_ok.get(
        "/admin/api/instruments",
        headers={"X-Admin-Token": "test-secret-token"},
    )
    assert r.status_code == 503
    assert "TINVEST_TOKEN" in (r.json().get("detail") or "")


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
    monkeypatch.setattr(
        api_module,
        "create_postgres_signal_store_with_retry",
        lambda *a, **k: mock_store,
    )
    app = api_module.create_app()
    hdrs = {"X-Admin-Token": "test-secret-token"}
    with TestClient(app) as client:
        assert client.get("/admin/api/instruments", headers=hdrs).status_code == 503
        assert client.get("/admin/api/instruments", headers=hdrs).status_code == 503
        r3 = client.get("/admin/api/instruments", headers=hdrs)
    assert r3.status_code == 429
