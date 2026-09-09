---
title: "Административная поверхность Signal Research Lab"
status: draft
tags:
  - "frontend"
  - "hypotheses"
  - "productization"
  - "research"
---

## Purpose & Scope

This spec governs the Signal Research Lab administrative UI and API. The product owner depends on it. Working-product signal administration is out of scope.

## Surface

- Web composition root: @../investment-signals-pro/web/src/bootstrap/research-main.tsx.
- API composition root: @../investment-signals-pro/backend/src/investment_signals_pro/bootstrap/research_lab_app.py.
- Navigation sections: overview, hypotheses, data, publications, and system; run state and controls live with the owning hypothesis.
- Lifecycle stages: candidate, historical validation, live shadow, and publishable.
- Supported locales: Russian and English.

## Normative Behavior

1. WHEN the owner opens port 18444, the UI MUST identify itself as Signal Research Lab.
2. WHEN a portfolio loads, the UI MUST show every hypothesis version and current lifecycle stage.
3. WHEN a hypothesis is selected, the UI MUST show claim, provenance, evidence, costs, and holdout result.
4. WHEN a run is active, the UI MUST show progress and its last completed checkpoint.
5. WHEN evidence fails a gate, the UI MUST show the blocking reasons.
6. WHEN evidence passes every gate, the UI MUST offer tool preparation.
7. The UI MUST show rejected and inconclusive versions alongside passed versions.
8. The UI MUST show the last manifest exchange without reading working-product storage.
9. The API process on port 18444 MUST expose only laboratory resources under `/api/v1`; the same paths on the working process MUST remain unavailable.

## Constraints & Invariants

- Publication actions MUST require owner reauthentication, preventing stale-session approval.
- The UI MUST preserve Russian and English labels for provenance summaries.
- A lifecycle stage MUST derive from stored evidence, not client-side inference.
- The web build MUST remain deployable without the working-product frontend.
- Source data and feature rows MUST NOT appear in working-product connection responses.

## Failure Behavior

1. IF a dependency is unavailable, THEN the UI MUST identify the failed laboratory dependency.
2. IF a run status is stale, THEN the UI MUST show its last recorded checkpoint.
3. IF publication becomes ineligible, THEN the API MUST reject preparation with current gate reasons.
4. IF the working product is offline, THEN laboratory research MUST remain available.
5. IF authentication expires, THEN mutation endpoints MUST return an authentication challenge.

## Conformance

An implementation conforms when API contract tests and browser tests cover portfolio, detail, runs, publication, errors, localization, and responsive layouts.
