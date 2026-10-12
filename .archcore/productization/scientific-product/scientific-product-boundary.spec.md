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
8. WHEN the laboratory shadow profile is enabled, it MUST use its own T-Invest ingestion, broker, scientific-candle worker, ClickHouse, and runtime-health storage.
9. WHILE the shadow profile is disabled, the laboratory admin MUST remain available and MUST NOT collect or process live market data.
10. The only product-to-product transfer MUST be an explicitly exported, signed, evidence-bounded tool package that the working product verifies before import.

## Constraints & Invariants

- The laboratory port MUST bind only to loopback, preventing LAN exposure.
- Product database credentials MUST differ, limiting a compromised service to one data plane.
- Product database networks MUST remain distinct, preventing direct cross-product database access.
- The laboratory MUST NOT join a working-product broker network or consume a working-product Kafka topic.
- Laboratory T-Invest ingestion MUST have a dedicated credential mount and an egress network that is not attached to laboratory databases.
- Scientific source data MUST remain under the laboratory backup boundary.
- Working backups MUST exclude scientific source data and run artifacts.
- The working product MUST receive no scientific raw data, feature tables, run logs, or model artifacts through tool import.

## Failure Behavior

1. IF port 18444 is occupied, THEN laboratory startup MUST fail before starting dependent services.
2. IF a laboratory store is unavailable, THEN laboratory health MUST report the failed dependency.
3. IF the laboratory fails, THEN working-product health MUST remain independent.
4. IF legacy scientific data exists, THEN setup MUST preserve it for the migration procedure.
5. IF cross-product database, state-volume, broker-network, or credential mounts appear, THEN composition validation MUST fail.
6. IF the laboratory T-Invest stream or broker is unavailable, THEN only the optional laboratory shadow profile MUST stop making progress.

## Conformance

An implementation conforms when lifecycle tests isolate start, stop, health, networks, credentials, volumes, backups, and the signed tool-transfer boundary for both products.
