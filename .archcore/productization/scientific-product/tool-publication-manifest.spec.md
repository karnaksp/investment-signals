---
title: "Контракт публикации проверенного инструмента"
status: draft
tags:
  - "contract"
  - "hypotheses"
  - "productization"
  - "tool-publication"
---

## Purpose & Scope

This spec governs Tool Publication Manifest v1 exchanged between both products. Exporters, importers, and the owner depend on it. Scientific datasets are out of scope.

## Surface

- JSON Schema: @contracts/tools/tool-publication-manifest.v1.schema.json.
- Canonical media type: `application/vnd.investment-signals.tool-manifest.v1+json`.
- Identity fields: `tool_id`, `tool_version`, `schema_version`.
- Runtime fields: built-in runtime kind, product-image artifact digest, entry point, and configuration digest.
- Explanation fields: purpose, market condition, output meaning, limitations.
- Provenance fields: hypothesis identity, source references, evidence identity/digest/time, independent-validation flag, and decision.
- Integrity fields: manifest digest, signature algorithm, signature, signing key identifier.

## Normative Behavior

1. WHEN every scientific gate passes, the laboratory MUST prepare one immutable manifest per tool version.
2. WHEN a manifest is prepared, the laboratory MUST refer to a built-in runtime artifact already shipped in the working-product image by its SHA-256 digest.
3. The laboratory MUST include Russian and English purpose and limitation text.
4. The laboratory MUST include the originating hypothesis identifier and version.
5. The laboratory MUST include the passed evidence identity, digest, generation time, and independent-validation flag.
6. The laboratory MUST include source identifiers and a concise derivation summary.
7. The laboratory MUST NOT include candles, feature rows, model training sets, or run logs.
8. The laboratory MUST calculate the manifest digest from canonical JSON excluding integrity fields.
9. The laboratory MUST sign the digest with the configured publication key.
10. WHEN manifest content changes, the laboratory MUST assign a new tool version.

## Constraints & Invariants

- A `tool_id` and `tool_version` pair MUST identify one content digest permanently.
- Runtime implementations MUST be built into the working product and identified by SHA-256; v1 does not transfer executable files.
- Manifest timestamps MUST use UTC RFC 3339.
- Provenance summaries MUST remain sufficient to explain the tool without laboratory access.
- Evidence summaries MUST describe results without embedding row-level scientific data.

## Failure Behavior

1. IF any scientific gate is incomplete, THEN the laboratory MUST reject manifest preparation.
2. IF required explanation text is missing, THEN the laboratory MUST reject manifest preparation.
3. IF canonicalization fails, THEN the laboratory MUST produce no manifest.
4. IF signing fails, THEN the laboratory MUST retain no publishable artifact.
5. IF an existing identity maps to another digest, THEN the laboratory MUST report a version conflict.

## Conformance

An implementation conforms when schema fixtures, canonicalization vectors, digest vectors, signatures, forbidden-field checks, and version-conflict tests pass.
