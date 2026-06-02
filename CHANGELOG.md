# Changelog

## Unreleased

- Added runtime fingerprint metadata (`app_version`, `commit_sha`, `build_time`) to health, readiness, and admin settings responses.
- Added delivery policy v3 metadata: `delivery_priority`, `delivery_channel`, and `delivery_explanation_ru`.
- Kept experimental rollout signal types `admin_only` by default and added explicit `admin_only`/`digest` custom delivery rules.
- Added quick `Useful`/`Noise`/`Unsure` feedback controls in Signal Cockpit Triage and Signals tables.
- Added admin APIs for feedback overview, source health, delivery simulation, and accuracy empty-state summaries.
- Added repeatable DuckDB accuracy JSON output for 1/5/15 minute horizons.
- Added CI, Dependabot, issue templates, and security guidance.
- Documented the manual intraday cockpit flow from raw signals to POI review, journal/paper trading, accuracy, and conservative delivery.
- Documented the `develop` multi-agent integration workflow and POI-focused QA expectations.
- Added a `develop` integration branch workflow and CI coverage for both `main` and `develop` while keeping GitHub Pages deployment on `main`.
- Added the POI v1 contract, read-time POI aggregation, `/admin/api/poi`, and `/admin/api/poi/{poi_id}`.
- Added Trading Radar as the default admin route with POI queue, tickers in play, scenario levels, source health, and quick journal actions.
- Added POI Journal storage/API/UI for `watch`, `dismiss`, paper long/short, missed, useful/noise/unsure, paper PnL, and win-rate.
- Added POI delivery v4 dry-run helper and `/admin/api/poi/delivery/simulation` with realtime/digest/admin-only candidates and no Telegram side effects.
- Added POI accuracy summary helper and `/admin/api/poi-accuracy` with a safe missing-file state.
