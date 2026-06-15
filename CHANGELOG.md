# Changelog

## Unreleased

- Admin Cockpit screenshot script синхронизирован с текущими routes: `triage`, `signals`, `delivery`, `calibration`, `instruments`, `accuracy` и `settings`.
- Добавлена CI smoke validation для Docker/Compose production runtime image.
- Runtime fingerprint metadata (`app_version`, `commit_sha`, `build_time`) добавлена в health, readiness и admin settings responses.
- Добавлена delivery policy v3 metadata: `delivery_priority`, `delivery_channel` и `delivery_explanation_ru`.
- Experimental rollout signal types остаются `admin_only` по умолчанию; добавлены явные custom delivery rules для `admin_only`/`digest`.
- В Signal Cockpit Triage и Signals tables добавлены быстрые feedback controls `Useful`/`Noise`/`Unsure`.
- Добавлены admin APIs для feedback overview, source health, delivery simulation и accuracy empty-state summaries.
- Добавлен повторяемый DuckDB accuracy JSON output для горизонтов 1/5/15 минут.
- Добавлены CI, Dependabot, issue templates и security guidance.
