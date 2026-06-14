# Roadmap

Эта страница отслеживает production-hardening и portfolio-readiness задачи для T-Invest Signal Engine.

## Production Hardening

- Поддерживать branch protection и required CI checks на `main`.
- Поддерживать актуальные [release notes](release_notes.md) для изменений, которые влияют на эксплуатацию, delivery behavior, UI ревью или runtime verification.
- Держать synthetic-event smoke tests доступными для локальной проверки без live trading.
- Расширить ClickHouse и Postgres migration notes для существующих volumes.
- Держать Docker build и runtime smoke checks быстрыми и стабильными в CI.

## Observability And Quality

- Держать Prometheus/Grafana dashboards синхронизированными с detector и delivery metrics.
- Отслеживать полезность сигналов через Signal Cockpit feedback и accuracy exports.
- Добавить больше документированных примеров triage: useful signal, noisy signal, delivery failure.
- Документировать ожидаемые row counts и latency ranges для local smoke runs.

## Portfolio Evidence

- Держать screenshots рядом с первым README/docs path.
- Добавить короткий demo GIF или static image set для Admin Cockpit.
- Держать `scripts/snapshot_admin_ui.py` синхронизированным с текущими Signal Cockpit routes.
- Документировать, что можно проверить без реальных market credentials.
- Обновлять architecture diagrams при изменении topics, services или storage roles.
