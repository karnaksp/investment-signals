import pytest

from scripts.snapshot_admin_ui import DEFAULT_ROUTES
from scripts.snapshot_admin_ui import selected_routes


def test_default_snapshot_routes_match_signal_cockpit_sections() -> None:
    assert [name for _, name in selected_routes(None)] == [
        "triage",
        "signals",
        "delivery",
        "calibration",
        "instruments",
        "accuracy",
        "settings",
    ]
    assert selected_routes(None) == DEFAULT_ROUTES


def test_snapshot_routes_can_be_limited_by_env_ordered_by_navigation() -> None:
    assert selected_routes("delivery, triage, signals") == [
        ("#/triage", "triage"),
        ("#/signals", "signals"),
        ("#/delivery", "delivery"),
    ]


def test_snapshot_routes_reject_unknown_names() -> None:
    with pytest.raises(ValueError, match="unknown"):
        selected_routes("triage,unknown")
