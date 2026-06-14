#!/usr/bin/env python3
"""Снимки админки для проверки вёрстки (десктоп + мобильная ширина).

  pip install -e ".[dev]"
  playwright install chromium
  set ADMIN_API_TOKEN=...   # опционально, чтобы подгрузились графики
  python scripts/snapshot_admin_ui.py

Переменные: ADMIN_UI_BASE (по умолчанию http://127.0.0.1:38000),
           SNAPSHOT_DIR (по умолчанию var/ui_snapshots),
           ADMIN_UI_ROUTES (опционально: triage,signals,delivery).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


DEFAULT_ROUTES = [
    ("#/triage", "triage"),
    ("#/signals", "signals"),
    ("#/delivery", "delivery"),
    ("#/calibration", "calibration"),
    ("#/instruments", "instruments"),
    ("#/accuracy", "accuracy"),
    ("#/settings", "settings"),
]


def selected_routes(raw: str | None) -> list[tuple[str, str]]:
    if not raw:
        return DEFAULT_ROUTES
    route_names = {name.strip().lower() for name in raw.split(",") if name.strip()}
    known = {name for _, name in DEFAULT_ROUTES}
    unknown = sorted(route_names - known)
    if unknown:
        raise ValueError(
            "Unknown ADMIN_UI_ROUTES values: "
            f"{', '.join(unknown)}. Expected one of: {', '.join(sorted(known))}"
        )
    return [(route, name) for route, name in DEFAULT_ROUTES if name in route_names]


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Установите: pip install -e \".[dev]\"", file=sys.stderr)
        print("И браузер: playwright install chromium", file=sys.stderr)
        return 1

    base = os.environ.get("ADMIN_UI_BASE", "http://127.0.0.1:38000").rstrip("/")
    token = (os.environ.get("ADMIN_API_TOKEN") or "").strip()
    out_dir = Path(os.environ.get("SNAPSHOT_DIR", "var/ui_snapshots"))
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        routes = selected_routes(os.environ.get("ADMIN_UI_ROUTES"))
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for width, tag in ((1280, "desktop"), (390, "mobile")):
            ctx = browser.new_context(
                viewport={"width": width, "height": 900},
                device_scale_factor=1,
            )
            if token:
                ctx.add_init_script(
                    "localStorage.setItem("
                    f"'tinvest_admin_token', {json.dumps(token)});"
                )
            page = ctx.new_page()
            for h, name in routes:
                page.goto(
                    f"{base}/admin/{h}",
                    wait_until="networkidle",
                    timeout=60_000,
                )
                page.wait_for_timeout(1500)
                path = out_dir / f"admin_{name}_{tag}.png"
                page.screenshot(path=str(path), full_page=True)
                print(path)
            ctx.close()
        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
