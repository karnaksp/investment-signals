# Roadmap

This page tracks production-hardening and portfolio-readiness work for T-Invest Signal Engine.

## Production Hardening

- Enable branch protection and required CI checks on `main`.
- Add release notes for operationally meaningful changes.
- Keep synthetic-event smoke tests available for local verification without live trading.
- Expand ClickHouse and Postgres migration notes for existing volumes.
- Add a Docker build smoke check once CI runtime is acceptable.

## Observability And Quality

- Keep Prometheus/Grafana dashboards aligned with detector and delivery metrics.
- Track signal usefulness via Signal Cockpit feedback and accuracy exports.
- Add more documented examples of signal triage: useful signal, noisy signal, delivery failure.
- Document expected row counts and latency ranges for local smoke runs.

## Portfolio Evidence

- Keep screenshots close to the README/docs first path.
- Add a short demo GIF or static image set for the admin cockpit.
- Document what can be verified without real market credentials.
- Keep architecture diagrams current when topics, services, or storage roles change.

