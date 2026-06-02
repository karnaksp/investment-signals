# Changelog

## Unreleased

- Added runtime fingerprint metadata (`app_version`, `commit_sha`, `build_time`) to health, readiness, and admin settings responses.
- Added delivery policy v3 metadata: `delivery_priority`, `delivery_channel`, and `delivery_explanation_ru`.
- Kept experimental rollout signal types `admin_only` by default and added explicit `admin_only`/`digest` custom delivery rules.
- Added admin APIs for feedback overview, source health, delivery simulation, and accuracy empty-state summaries.
- Added repeatable DuckDB accuracy JSON output for 1/5/15 minute horizons.
- Added CI, Dependabot, issue templates, and security guidance.
