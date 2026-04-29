# labelwatch — Architecture Overview

**Status**: v0 starter — expect expansion.
**Last updated**: 2026-04-28

## What this is

labelwatch is a meta-governance observatory for ATProto's label ecosystem. It watches *labelers*, not posters. It discovers labelers, polls their label events, classifies them by evidence-based visibility/reachability analysis, runs detection rules against their behavior, derives per-labeler regime/risk/coherence signals, detects cross-labeler boundary instability, and produces auditable alerts with receipt hashes.

It does not moderate content, judge truth, emit labels, or produce per-poster behavioral telemetry. See `PUBLIC_SURFACES.md` (TODO) and `../../NON_GOALS.md`.

## Five questions

1. **What is this system?** A read-only observatory of labeler behavior, with public per-DID climate reports (receiving-end accounting) and a static dashboard.
2. **What are its organs?** Three systemd services sharing one SQLite database (WAL mode, schema v19):
   - `labelwatch.service` — main loop: ingest, scan, derive, report
   - `labelwatch-discovery.service` — Jetstream sidecar for real-time labeler discovery
   - `labelwatch-api.service` — climate HTTP API
3. **What are the admissible outputs?** Receipted alerts, labeler classifications, regime/risk/coherence signals, boundary edges, per-DID climate reports (receiving-end), static HTML dashboard. See `PUBLICATION_MODEL.md`.
4. **What boundaries are intentional?** No content judgment, no truth adjudication, no labeler verdicts, no poster dossiers. See `PUBLIC_SURFACES.md` (TODO).
5. **What failure modes does the architecture already know about?** WAL bloat under long-lived readers, sparse-labeler warmup, polling gaps, drop-aware coverage, schema migration discipline. See `FAILURE_MODES.md` (TODO).

## System diagram

```
                ┌─────────────────────┐
                │  ATProto + Labelers │
                │  + Jetstream + PLC  │
                └──────────┬──────────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   ┌─────────┐        ┌─────────┐        ┌──────────┐
   │ Ingest  │        │Discovery│        │ Discovery│
   │ (poll)  │        │ (batch) │        │  Stream  │
   └────┬────┘        └────┬────┘        └────┬─────┘
        └────────┬─────────┴────────┬─────────┘
                 ▼                  ▼
          ┌──────────────────────────────┐
          │   SQLite (WAL, schema v19)   │
          │ label_events | labelers      │
          │ alerts | discovery_events    │
          │ boundary_edges | rollups     │
          └──────────────┬───────────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
   ┌─────────┐      ┌─────────┐      ┌─────────┐
   │  Scan   │─────▶│ Derive  │      │ Climate │
   │ (rules) │      │ (state) │      │   API   │
   └────┬────┘      └────┬────┘      └─────────┘
        ▼                ▼
   ┌──────────────────────────────┐
   │ Report (HTML + JSON)         │
   │ atomic dir swap              │
   └──────────────────────────────┘
```

See `diagrams/` for rendered mermaid versions: [system-overview](diagrams/system-overview.md), [dataflow](diagrams/dataflow.md), [publication-boundary](diagrams/publication-boundary.md).

## Core invariants

- **Observation only** — analysis is grounded in observable label application patterns. No semantic content judgment, no labeling correctness verdicts.
- **Append-only events** — `label_events` rows are never updated or deleted; deduplication via `INSERT OR IGNORE` on `event_hash`.
- **Receipt hashing** — every alert includes SHA-256 over `(rule_id, labeler_did, ts, inputs, evidence_hashes, config_hash)`. Reproducible.
- **Sticky evidence** — evidence fields (declared_record, observed_as_src, has_labeler_service, has_label_key) once set to 1 are never downgraded by transient probe failures.
- **Hysteresis** — regime state changes require N consecutive passes. No flapping.
- **Four-dial discipline** — `auditability_risk`, `inference_risk`, `temporal_coherence`, `regime_state` are *separate* signals. There is no single collapsed trust score. Operators reason about each axis independently; collapse hides which axis is bad.
- **Aggregate-first** — see `PUBLIC_SURFACES.md`.

## Where to go next

| Question | Doc |
|----------|-----|
| What does it publish, and what doesn't it? | `PUBLICATION_MODEL.md` |
| How does data flow through? | `DATAFLOW.md` (TODO) |
| What's the boundary against poster surveillance? | `PUBLIC_SURFACES.md` (TODO) |
| What does the architecture already know can go wrong? | `FAILURE_MODES.md` (TODO) |
| Component-level detail (tables, classes, error handling, deep dives) | `../../ARCHITECTURE.md` (deep reference) |
| What is and isn't a non-goal? | `../../NON_GOALS.md` |
| Operational ops hazards | `../OPS_HAZARDS.md` |
| Tier-0 hardening | `../HARDENING.md` |
