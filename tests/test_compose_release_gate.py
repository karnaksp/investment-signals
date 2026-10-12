from pathlib import Path

import yaml


def _compose() -> dict[str, object]:
    path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_application_services_wait_for_release_migrations() -> None:
    services = _compose()["services"]
    migration = services["migrate"]

    assert migration["restart"] == "no"
    assert migration["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert (
        migration["depends_on"]["clickhouse-init"]["condition"]
        == "service_completed_successfully"
    )
    assert migration["command"] == [
        "tinvest-migrate",
        "--release-version",
        "${APP_VERSION:-0.0.0+local}",
        "--migrations-root",
        "/app/sql",
    ]
    for service_name in ("detector", "api", "dagster-daemon"):
        assert (
            services[service_name]["depends_on"]["migrate"]["condition"]
            == "service_completed_successfully"
        )


def test_dagster_uses_its_writable_volume_as_home() -> None:
    services = _compose()["services"]

    for service_name in ("dagster-webserver", "dagster-daemon"):
        environment = services[service_name]["environment"]
        assert environment["DAGSTER_HOME"] == "/opt/dagster/dagster_home"
        assert environment["HOME"] == environment["DAGSTER_HOME"]
