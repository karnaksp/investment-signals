from __future__ import annotations

import copy

from scripts.release_scope import (
    _normalized_toml,
    classify_paths,
    load_config,
    select_previous_release,
)


def test_indicator_change_uses_targeted_scope() -> None:
    scope, reason = classify_paths(
        [
            "src/tinvest_signal_engine/detector_core.py",
            "tests/test_detector.py",
            "CHANGELOG.md",
            "docs/releases/v1.0.1.md",
            "pyproject.toml",
            "uv.lock",
        ],
        load_config(),
        version_only_paths={"pyproject.toml", "uv.lock"},
    )

    assert scope == "indicators"
    assert "indicator component" in reason


def test_new_realtime_detector_regression_test_stays_in_indicator_scope() -> None:
    scope, _ = classify_paths(
        [
            "src/tinvest_signal_engine/domain/indicators/microstructure.py",
            "src/tinvest_signal_engine/detector_core.py",
            "tests/test_realtime_signal_detectors.py",
        ],
        load_config(),
    )

    assert scope == "indicators"


def test_unknown_runtime_change_fails_safe_to_full_scope() -> None:
    scope, reason = classify_paths(
        ["src/tinvest_signal_engine/services/api.py"],
        load_config(),
    )

    assert scope == "full"
    assert "services/api.py" in reason


def test_dependency_change_requires_full_scope() -> None:
    scope, _ = classify_paths(
        ["pyproject.toml", "uv.lock", "docs/releases/v1.1.0.md"],
        load_config(),
        version_only_paths=set(),
    )

    assert scope == "full"


def test_docs_and_release_metadata_have_lightweight_scopes() -> None:
    config = load_config()
    assert classify_paths(["docs/operations.md"], config)[0] == "docs"
    assert (
        classify_paths(
            ["CHANGELOG.md", "docs/releases/v1.0.1.md", "pyproject.toml", "uv.lock"],
            config,
            version_only_paths={"pyproject.toml", "uv.lock"},
        )[0]
        == "metadata"
    )


def test_toml_normalization_ignores_only_project_version() -> None:
    before = '[project]\nname = "demo"\nversion = "1.0.0"\ndependencies = ["a"]\n'
    after = '[project]\nname = "demo"\nversion = "1.0.1"\ndependencies = ["a"]\n'
    changed = '[project]\nname = "demo"\nversion = "1.0.1"\ndependencies = ["b"]\n'

    normalized = _normalized_toml(before, lockfile=False)
    assert normalized == _normalized_toml(after, lockfile=False)
    assert normalized != _normalized_toml(changed, lockfile=False)


def test_lock_normalization_keeps_dependency_changes_visible() -> None:
    base = {
        "version": 1,
        "package": [
            {
                "name": "tinvest-signal-engine",
                "version": "1.0.0",
                "dependencies": [{"name": "a"}],
            },
            {"name": "a", "version": "1"},
        ],
    }
    changed = copy.deepcopy(base)
    changed["package"][0]["version"] = "1.0.1"
    changed["package"][0]["dependencies"].append({"name": "b"})

    def as_toml(data: dict) -> str:
        dependencies = data["package"][0]["dependencies"]
        deps = ", ".join(f'{{ name = "{item["name"]}" }}' for item in dependencies)
        return (
            "version = 1\n"
            '[[package]]\nname = "tinvest-signal-engine"\n'
            f'version = "{data["package"][0]["version"]}"\n'
            f"dependencies = [{deps}]\n"
            '[[package]]\nname = "a"\nversion = "1"\n'
        )

    assert _normalized_toml(as_toml(base), lockfile=True) != _normalized_toml(
        as_toml(changed), lockfile=True
    )


def test_previous_release_uses_highest_lower_stable_semver() -> None:
    assert (
        select_previous_release(
            ["v0.2.0", "v1.0.0", "v1.0.1-rc1", "not-a-release"],
            "v1.1.0",
        )
        == "v1.0.0"
    )
