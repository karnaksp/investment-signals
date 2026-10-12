---
title: "Импорт научного инструмента в рабочий продукт"
status: draft
tags:
  - "contract"
  - "productization"
  - "runtime"
  - "tool-publication"
---

## Purpose & Scope

This spec governs importing a published tool into Investment Signals Pro. The owner, runtime catalog, and working UI depend on it. Scientific validation is out of scope.

## Surface

- Import API: @../investment-signals-pro/backend/src/investment_signals_pro/application/tools/.
- Tool catalog: @../investment-signals-pro/contracts/tools/.
- Working UI: @../investment-signals-pro/web/src/adapters/react/.
- Input media type: `application/vnd.investment-signals.tool-manifest.v1+json`.
- States: imported and rejected; a newer immutable version does not overwrite an earlier one.

## Normative Behavior

1. WHEN the owner imports a package, the product MUST verify the closed schema, canonical digest, Ed25519 signature, and identity/version binding.
2. WHEN verification passes, the product MUST require a built-in runtime entry point and store its immutable configuration in the working catalog.
3. WHEN import completes, the product MUST store purpose, limitations, and concise evidence provenance.
4. The product MUST NOT fetch scientific datasets or laboratory database records.
5. The product MUST reject executable files and research payloads; v1 accepts only configuration for a built-in implementation.
6. WHEN the runtime consumes a tool, it MUST load only the imported configuration and its built-in implementation.
7. The working UI MUST link provenance to the imported immutable summary.
8. WHILE the laboratory is offline, imported tools MUST remain operable.
9. WHEN a newer version imports, the product MUST preserve prior versions for rollback.

## Constraints & Invariants

- Import MUST be idempotent by manifest digest.
- One tool identity version MUST map to one manifest digest.
- Imported artifacts MUST remain inside the working-product backup boundary.
- Working-product credentials MUST grant no laboratory database access.
- The immutable import record MUST retain its import timestamp and manifest digest.

## Failure Behavior

1. IF signature verification fails, THEN the product MUST reject the manifest without storing its artifact.
2. IF the entry point is not a supported built-in implementation, THEN the product runtime MUST refuse to load it.
3. IF an identity/version conflicts with another digest, THEN the product MUST reject the import.
4. IF atomic catalog storage fails, THEN the product MUST leave the previous catalog unchanged.
5. IF provenance links are unreachable, THEN the product MUST display the stored summary.

## Conformance

An implementation conforms when import, idempotency, conflict, tamper rejection, offline-laboratory, and forbidden-research-data tests pass.
