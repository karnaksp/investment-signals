---
title: Adopt Clean Architecture for the Public Core
status: accepted
---

# Adopt Clean Architecture for the Public Core

## Context

The baseline exposes framework composition in
`@src/tinvest_signal_engine/services/api.py` and detection policy in
`@src/tinvest_signal_engine/detector_core.py`. Productization will add
provenance, migrations, idempotency, and configuration ports that need unit
tests without FastAPI, Kafka, PostgreSQL, ClickHouse, Redis, or T-Invest clients.

## Decision

Adopt Robert C. Martin's Clean Architecture dependency rule for every new or modified public-core production package.

## Alternatives Considered

1. Continue the current flat package — rejected because import direction remains
   implicit and framework types can enter detector policy.
2. Enforce network-service boundaries only — rejected because separate processes
   do not prevent source imports from policy into infrastructure.
3. Rewrite the core before product work — rejected because it replaces tested
   behavior before contract and migration foundations exist.

## Consequences

- [expected] New domain/application tests run without containers or network
  clients.
- Existing modules remain legacy until extracted behind ports and adapters.
- [expected] Each extracted use case adds DTO mapping and composition code.
- `@scripts/check_architecture.py` blocks inward imports from target layers.

## Superseded when

- A published Martin-style dependency-rule interpretation requires an inner
  layer to import a concrete runtime framework to preserve domain semantics.
- The boundary checker produces false positives on at least 20% of 100
  consecutive pull requests and no narrower import rule represents the layers.
