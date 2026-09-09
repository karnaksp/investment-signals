---
title: "Граница развертывания Signal Research Lab"
status: draft
tags:
  - "deployment"
  - "hypotheses"
  - "productization"
  - "research"
---

## Purpose & Scope

This spec governs the deployment and data boundary of Signal Research Lab. Operators and both products depend on it. Tool transfer is out of scope.

## Surface

- Laboratory composition: @../investment-signals-pro/deploy/research-lab.compose.yaml.
- Working composition: @../investment-signals-pro/deploy/compose.yaml.
- Installation lifecycle: @../investment-signals-pro/internal/adapters/runtimecompose/.
- Laboratory entry point: `http://localhost:18444`.
- Working entry point: `http://localhost:18443`.

## Normative Behavior

1. WHEN an operator installs the laboratory, the installer MUST create a distinct Compose project.
2. WHEN the working product starts, signalctl MUST NOT start laboratory services.
3. WHILE the laboratory is stopped, the working product MUST continue its health checks.
4. WHEN the laboratory starts, it MUST use laboratory-owned PostgreSQL and ClickHouse stores.
5. The working product MUST NOT mount laboratory data volumes.
6. The laboratory MUST NOT mount working-product data volumes.
7. WHEN either product stops, its lifecycle command MUST NOT stop the sibling product.

## Constraints & Invariants

- The laboratory port MUST bind only to loopback, preventing LAN exposure.
- Product database credentials MUST differ, limiting a compromised service to one data plane.
- Docker data networks MUST remain distinct, preventing direct cross-product database access.
- Scientific source data MUST remain under the laboratory backup boundary.
- Working backups MUST exclude scientific source data and run artifacts.

## Failure Behavior

1. IF port 18444 is occupied, THEN laboratory startup MUST fail before starting dependent services.
2. IF a laboratory store is unavailable, THEN laboratory health MUST report the failed dependency.
3. IF the laboratory fails, THEN working-product health MUST remain independent.
4. IF legacy scientific data exists, THEN setup MUST preserve it for the migration procedure.
5. IF cross-product volume mounts appear, THEN composition validation MUST fail.

## Conformance

An implementation conforms when lifecycle tests isolate start, stop, health, networks, credentials, volumes, and backups for both products.
