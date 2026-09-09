---
title: "Разделить научный и рабочий продукты"
status: draft
tags:
  - "clean-architecture"
  - "hypotheses"
  - "productization"
  - "research"
  - "tool-publication"
---

## Context

Исторические прогоны, научные данные и живая тень используют тяжёлые процессы из @src/tinvest_signal_engine/services/hypothesis_replay_api.py. Сейчас их жизненный цикл связан с рабочим Cockpit, хотя рабочему пользователю нужны только прошедшие проверку инструменты. [expected] Общий контур сохраняет отказоустойчивую и ресурсную связь между исследованием и ежедневной выдачей.

## Decision

Adopt two localhost products: Signal Research Lab on port 18444 with isolated stores, and Investment Signals Pro on port 18443 consuming Tool Publication Manifest v1 artifacts.

## Alternatives Considered

1. Shared Cockpit with an optional Compose profile — rejected because UI, storage ownership, and release lifecycle remain coupled.
2. Separate interfaces over shared PostgreSQL and ClickHouse — rejected because schema changes and retention operations cross both product boundaries.
3. Synchronous API access from the working product — rejected because daily operation would depend on laboratory availability.

## Consequences

### Positive

- [expected] The working installation starts zero scientific services and mounts zero scientific data volumes.
- [expected] A stopped laboratory does not interrupt signals, delivery, or working-product administration.
- Every applied tool retains a versioned explanation and evidence provenance through one immutable manifest.

### Tradeoffs

- [expected] Existing scientific state requires one checksum-verified migration into laboratory-owned stores.
- [expected] Promotion adds one owner-approved export and import operation per tool version.
- Two products require separate backup, restore, health, and port checks.

## Superseded when

- A future single-process runtime demonstrates isolated stores and independent lifecycle under failure injection.
- More than 100 active tool versions make file-based transfer exceed five owner actions per publication.
- A signed remote registry replaces localhost artifact transfer with the same no-shared-database invariant.