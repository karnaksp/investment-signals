#!/usr/bin/env python3
"""Enforce inward-only imports for Clean Architecture inner layers."""

from __future__ import annotations

import ast
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
PACKAGE = "tinvest_signal_engine"
INNER_LAYERS = ("domain", "application")


def _module_name(path: Path, source_root: Path = SOURCE_ROOT) -> str:
    return ".".join(path.relative_to(source_root).with_suffix("").parts)


def _layer(module: str) -> str | None:
    for layer in INNER_LAYERS:
        prefix = f"{PACKAGE}.{layer}"
        if module == prefix or module.startswith(prefix + "."):
            return layer
    return None


def _resolve_from(module: str, level: int, imported: str | None) -> str:
    if level == 0:
        return imported or ""
    package_parts = module.split(".")[:-1]
    keep = max(0, len(package_parts) - (level - 1))
    base = package_parts[:keep]
    if imported:
        base.extend(imported.split("."))
    return ".".join(base)


def _imports(tree: ast.AST, module: str) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            result.append(
                (node.lineno, _resolve_from(module, node.level, node.module))
            )
    return result


def _is_stdlib(imported: str) -> bool:
    root = imported.split(".", 1)[0]
    return root == "__future__" or root in sys.stdlib_module_names


def _allowed(layer: str, imported: str) -> bool:
    if not imported or _is_stdlib(imported):
        return True
    if layer == "domain":
        return imported == f"{PACKAGE}.domain" or imported.startswith(
            f"{PACKAGE}.domain."
        )
    return any(
        imported == f"{PACKAGE}.{allowed_layer}"
        or imported.startswith(f"{PACKAGE}.{allowed_layer}.")
        for allowed_layer in ("domain", "application")
    )


def _display_path(path: Path) -> Path:
    try:
        return path.relative_to(PROJECT_ROOT)
    except ValueError:
        return path


def find_violations(source_root: Path = SOURCE_ROOT) -> list[str]:
    violations: list[str] = []
    package_root = source_root / PACKAGE
    if not package_root.exists():
        return violations
    for path in sorted(package_root.rglob("*.py")):
        module = _module_name(path, source_root)
        layer = _layer(module)
        if layer is None:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for line, imported in _imports(tree, module):
            if not _allowed(layer, imported):
                relative = _display_path(path)
                violations.append(
                    f"{relative}:{line}: {layer} layer imports {imported!r}"
                )
    return violations


def main() -> int:
    source_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else SOURCE_ROOT
    violations = find_violations(source_root)
    if violations:
        print("Clean Architecture dependency violations:")
        print("\n".join(f"- {item}" for item in violations))
        return 1
    print("Clean Architecture dependency boundaries: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
