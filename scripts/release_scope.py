#!/usr/bin/env python3
"""Select the smallest safe CI/release scope for a Git diff."""

from __future__ import annotations

import argparse
import copy
import fnmatch
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / ".github" / "release-scopes.json"
SEMVER_TAG = re.compile(r"^v(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")
VERSIONED_METADATA = frozenset({"pyproject.toml", "uv.lock"})


@dataclass(frozen=True)
class ScopeDecision:
    scope: str
    base: str
    head: str
    changed_paths: tuple[str, ...]
    reason: str

    @property
    def docker_required(self) -> bool:
        return self.scope == "full"


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    config = json.loads(path.read_text(encoding="utf-8"))
    if config.get("schema_version") != 1:
        raise ValueError("unsupported release scope schema")
    for key in (
        "release_metadata_paths",
        "documentation_paths",
        "indicator_paths",
        "indicator_tests",
    ):
        values = config.get(key)
        if not isinstance(values, list) or not all(isinstance(item, str) for item in values):
            raise ValueError(f"{key} must be a list of strings")
    return config


def _matches(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _normalized_toml(document: str, *, lockfile: bool) -> dict[str, Any]:
    data = copy.deepcopy(tomllib.loads(document))
    if lockfile:
        for package in data.get("package", []):
            if package.get("name") == "tinvest-signal-engine":
                package["version"] = "<release-version>"
                break
    else:
        data.get("project", {})["version"] = "<release-version>"
    return data


def version_metadata_only(base: str, head: str, path: str) -> bool:
    if path not in VERSIONED_METADATA:
        return False
    try:
        before = _git("show", f"{base}:{path}")
        after = _git("show", f"{head}:{path}")
        return _normalized_toml(before, lockfile=path == "uv.lock") == _normalized_toml(
            after,
            lockfile=path == "uv.lock",
        )
    except (KeyError, subprocess.CalledProcessError, tomllib.TOMLDecodeError):
        return False


def classify_paths(
    paths: Sequence[str],
    config: dict[str, Any],
    *,
    version_only_paths: Iterable[str] = (),
) -> tuple[str, str]:
    changed = tuple(sorted(set(paths)))
    version_only = set(version_only_paths)
    metadata_patterns = config["release_metadata_paths"]
    docs_patterns = config["documentation_paths"]
    indicator_patterns = config["indicator_paths"]

    substantive = tuple(
        path
        for path in changed
        if path in VERSIONED_METADATA and path not in version_only
        or path not in VERSIONED_METADATA and not _matches(path, metadata_patterns)
    )
    if not substantive:
        return "metadata", "only release version or release-note metadata changed"
    if all(_matches(path, docs_patterns) for path in substantive):
        return "docs", "only documentation changed"
    if all(
        _matches(path, indicator_patterns) or _matches(path, docs_patterns)
        for path in substantive
    ) and any(_matches(path, indicator_patterns) for path in substantive):
        return "indicators", "changes are confined to the indicator component"

    unsafe = next(
        path
        for path in substantive
        if not _matches(path, indicator_patterns) and not _matches(path, docs_patterns)
    )
    return "full", f"full verification required by {unsafe}"


def changed_paths(base: str, head: str) -> tuple[str, ...]:
    output = _git("diff", "--name-only", f"{base}..{head}")
    return tuple(line for line in output.splitlines() if line)


def decide_scope(base: str, head: str, config: dict[str, Any]) -> ScopeDecision:
    paths = changed_paths(base, head)
    version_only = {
        path for path in paths if version_metadata_only(base, head, path)
    }
    scope, reason = classify_paths(paths, config, version_only_paths=version_only)
    return ScopeDecision(scope, base, head, paths, reason)


def _semver(tag: str) -> tuple[int, int, int] | None:
    match = SEMVER_TAG.fullmatch(tag)
    if not match:
        return None
    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


def select_previous_release(tags: Iterable[str], current_tag: str) -> str | None:
    current = _semver(current_tag)
    if current is None:
        raise ValueError(f"current tag is not stable SemVer: {current_tag}")
    candidates = [
        (version, tag)
        for tag in tags
        if tag != current_tag
        if (version := _semver(tag)) is not None and version < current
    ]
    return max(candidates, default=(None, None))[1]


def previous_release(head: str, current_tag: str) -> str | None:
    tags = _git("tag", "--merged", head, "--list", "v[0-9]*").splitlines()
    return select_previous_release(tags, current_tag)


def _write_key_values(path: Path, decision: ScopeDecision) -> None:
    values = {
        "RELEASE_SCOPE": decision.scope,
        "RELEASE_BASE": decision.base,
        "RELEASE_HEAD": decision.head,
        "RELEASE_CHANGED_COUNT": str(len(decision.changed_paths)),
        "RELEASE_DOCKER_REQUIRED": str(decision.docker_required).lower(),
        "RELEASE_REASON": decision.reason.replace("\n", " "),
    }
    with path.open("a", encoding="utf-8") as target:
        for key, value in values.items():
            target.write(f"{key}={value}\n")


def _write_summary(path: Path, decision: ScopeDecision) -> None:
    changed = "\n".join(f"- `{item}`" for item in decision.changed_paths)
    text = (
        "## Change-aware verification\n\n"
        f"- Scope: **{decision.scope}**\n"
        f"- Docker required: **{str(decision.docker_required).lower()}**\n"
        f"- Base: `{decision.base}`\n"
        f"- Reason: {decision.reason}\n\n"
        "<details><summary>Changed paths</summary>\n\n"
        f"{changed or '- none'}\n\n"
        "</details>\n"
    )
    with path.open("a", encoding="utf-8") as target:
        target.write(text)


def run_tests(scope: str, config: dict[str, Any]) -> int:
    if scope == "full":
        args: list[str] = []
    elif scope == "indicators":
        args = list(config["indicator_tests"])
    elif scope in {"docs", "metadata"}:
        print(f"No Python tests required for {scope} scope")
        return 0
    else:
        raise ValueError(f"unknown release scope: {scope}")
    return subprocess.run([sys.executable, "-m", "pytest", "-q", *args], cwd=ROOT).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--previous-release", action="store_true")
    parser.add_argument("--current-tag", default=os.getenv("GITHUB_REF_NAME", ""))
    parser.add_argument("--github-env", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--run-tests", choices=("full", "indicators", "docs", "metadata"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config()
    if args.run_tests:
        return run_tests(args.run_tests, config)

    base = args.base
    if args.previous_release:
        if not args.current_tag:
            raise SystemExit("--current-tag or GITHUB_REF_NAME is required")
        base = previous_release(args.head, args.current_tag)
        if base is None:
            decision = ScopeDecision(
                "full",
                "<none>",
                args.head,
                (),
                "no previous stable release tag exists",
            )
        else:
            decision = decide_scope(base, args.head, config)
    elif base:
        decision = decide_scope(base, args.head, config)
    else:
        raise SystemExit("provide --base or --previous-release")

    print(json.dumps(decision.__dict__ | {"docker_required": decision.docker_required}, indent=2))
    if args.github_env:
        _write_key_values(args.github_env, decision)
    if args.summary:
        _write_summary(args.summary, decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
