---
title: "Перенос научного состояния в Signal Research Lab"
status: draft
tags:
  - "data-migration"
  - "guide"
  - "hypotheses"
  - "productization"
  - "research"
---

Reader: release integrator; task: migrate scientific state; step actor: local installation operator.

## Prerequisites

- A verified backup of the current installation exists.
- Free disk space exceeds twice the scientific-state inventory.
- The laboratory installation is present but stopped.
- The migration command version matches the installed product version.
- A maintenance window covers scientific writers only.

## Steps

1. Export a pre-migration inventory of scientific PostgreSQL/ClickHouse rows, Redpanda offsets, and research files to `evidence/lab-before.json`.

2. Stop only the legacy scientific writers with the legacy research Compose profile; keep the working services running.

3. Record Redpanda offsets and worker health timestamps.

4. Copy PostgreSQL scientific tables into the laboratory database.

5. Copy ClickHouse scientific tables into the laboratory database.

6. Copy candle caches, replay artifacts, and lifecycle audit files.

7. Export the same inventory from the laboratory to `evidence/lab-after.json`.

8. Compare row counts, offsets, file counts, and SHA-256 values.

9. Start the laboratory admin without the `compute` profile, so no replay or Dagster job starts.

`docker compose -f deploy/research-lab.compose.yaml up -d research-api`

10. Verify the latest hypothesis and worker checkpoints.

11. Mark the migrated source read-only.

12. Remove legacy mounts after one verified laboratory backup.

## Verification

- Every table count equals its pre-migration value.
- Every copied file matches its recorded SHA-256.
- Redpanda consumer offsets match the quiesced values.
- The laboratory shows the last completed runs and decisions.
- The working product starts without scientific mounts.
- Restore tests recover each product from its own backup.

## Common Issues

- Count mismatch: keep source data read-only and repeat the failed table copy.
- Digest mismatch: delete only the destination copy and recopy that file.
- Offset drift: stop the remaining scientific writer and regenerate both inventories.
- Missing lifecycle rows: retain legacy mounts and inspect migration filters.
- Working-product regression: restore the previous Compose configuration and keep laboratory data untouched.
