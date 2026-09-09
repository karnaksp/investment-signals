---
title: "Выделение Signal Research Lab"
status: draft
tags:
  - "clean-architecture"
  - "hypotheses"
  - "productization"
  - "research"
  - "tool-publication"
---

## Goal

Deliver a separately operated scientific product and a narrow publication path that leaves only verified runtime tools in Investment Signals Pro.

## Tasks

### Phase 1 — Freeze boundaries and transfer format

1. Define tool publication domain values beside @src/tinvest_signal_engine/domain/scientific_hypotheses.py.
2. Add application ports beside @src/tinvest_signal_engine/application/scientific_hypotheses.py.
3. Add the versioned JSON Schema under the product contract catalog.
4. Add canonicalization, digest, signature, and forbidden-field contract tests.

### Phase 2 — Create the laboratory API and admin

1. Extract scientific composition from @../investment-signals-pro/backend/src/investment_signals_pro/bootstrap/app.py into @../investment-signals-pro/backend/src/investment_signals_pro/bootstrap/research_lab_app.py.
2. Reuse scientific use cases under @../investment-signals-pro/backend/src/investment_signals_pro/application/scientific_hypotheses/.
3. Build the laboratory shell from @../investment-signals-pro/web/src/adapters/react/HypothesesPages.tsx.
4. Implement the approved screen from @../investment-signals-pro/design/scientific-lab-admin-concept.png.
5. Add portfolio, detail, run, data, publication, and system browser tests.

### Phase 3 — Separate deployment and stores

1. Split scientific services and one-shot imports from @../investment-signals-pro/deploy/compose.yaml.
2. Bind the laboratory API directly to loopback port 18444 in @../investment-signals-pro/deploy/research-lab.compose.yaml.
3. Remove scientific services from @../investment-signals-pro/internal/adapters/hostcmd/runner.go.
4. Add independent lifecycle commands beside @../investment-signals-pro/internal/adapters/runtimecompose/.
5. Split backup scopes, health snapshots, secrets, networks, and resource limits.
6. Validate that default working startup excludes every laboratory service.

### Phase 4 — Publish and import tools

1. Add manifest preparation to the scientific lifecycle application.
2. Add signed artifact storage behind a laboratory adapter.
3. Add working-product import and immutable file-catalog ports for built-in runtime tools.
4. Add owner reauthentication, signature verification, idempotency, and version-conflict rejection.
5. Replace working scientific pages with tool provenance summaries and a laboratory link.

### Phase 5 — Migrate existing scientific state

1. Inventory migrations 0310–0319 and scientific ClickHouse tables.
2. Stop scientific writers after recording their offsets and health timestamps.
3. Copy scientific rows, cache files, replay artifacts, and lifecycle audit records.
4. Compare row counts, file counts, offsets, and SHA-256 inventories.
5. Start laboratory services and verify the latest persisted checkpoints.
6. Remove migrated scientific mounts from the working product after verification.

### Phase 6 — Qualify the split

1. Run architecture, backend, frontend, Go, schema, and Compose checks.
2. Test both products with the sibling stopped.
3. Test manifest tampering, duplicate import, version conflict, activation failure, and rollback.
4. Verify desktop and mobile laboratory screens against the approved concept.
5. Verify backup and restore for each product independently.

## Acceptance Criteria

- Default Compose expansion contains no laboratory services or scientific data mounts.
- Laboratory Compose expansion exposes one loopback edge port and an independent data network.
- Contract fixtures produce stable canonical digests and Ed25519 signatures.
- Tampered manifests fail before artifact storage changes.
- An imported built-in tool remains available to the working product while the laboratory containers are stopped.
- Row-count and SHA-256 reports match before legacy scientific storage removal.
- Browser snapshots match the approved concept at 1586×992 and a mobile viewport.
- Architecture and full repository test suites pass without package-boundary violations.

## Dependencies

- Existing hypothesis gate and provenance catalog.
- Existing owner authentication and step-up verification.
- A publication signing key managed outside both databases.
- One maintenance window for scientific-state migration.

## Declared Delta

- Route: umbrella, size XL; raised by stone maturity and data-migration risk.
- Creates: scientific product, scientific admin, publication manifest, working-product tool import.
- Modifies: working Cockpit, installation lifecycle, backup boundary, scientific service composition.
- Retires: shared scientific UI/API and shared scientific data plane.
- Decision: isolated stores with immutable manifest exchange.
- Intent gap: a separately operated scientific product.
- Risks: external contract and data migration.
