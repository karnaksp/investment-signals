#!/usr/bin/env python3
"""Снимки админки для проверки вёрстки (десктоп + мобильная ширина).

  pip install -e ".[dev]"
  playwright install chromium
  set ADMIN_API_TOKEN=...   # опционально, чтобы подгрузились графики
  python scripts/snapshot_admin_ui.py

Переменные: ADMIN_UI_BASE (по умолчанию http://127.0.0.1:38000),
           SNAPSHOT_DIR (по умолчанию var/ui_snapshots).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


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

    routes = [
        ("#/overview", "overview"),
        ("#/table", "table"),
        ("#/slices", "slices"),
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for width, tag in ((1280, "desktop"), (390, "mobile")):
            ctx = browser.new_context(
                viewport={"width": width, "height": 900},
                device_scale_factor=1,
            )
            page = ctx.new_page()
            page.goto(f"{base}/admin/", wait_until="networkidle", timeout=60_000)
            if token:
                page.evaluate(
                    """(t) => { localStorage.setItem('tinvest_admin_token', t); }""",
                    token,
                )
            for h, name in routes:
                page.evaluate(
                    """(hash) => { window.location.hash = hash; }""",
                    h,
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
