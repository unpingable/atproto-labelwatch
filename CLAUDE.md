# CLAUDE.md

labelwatch is an ATProto label behavior monitor. It polls `com.atproto.label.queryLabels` for configured labeler DIDs, discovers labelers via `listReposByCollection`, stores normalized label events in SQLite, runs detection rules (rate spikes, flip-flop patterns, target concentration, churn), and produces auditable alerts with SHA-256 receipt hashes. It observes labeler behavior — it does not moderate content, judge truth, or emit labels.

## Architecture rules

- **Observation only** — no content moderation, no truth adjudication, no "bad labeler" verdicts.
- **Append-only events** — `label_events` rows are never updated or deleted.
- **Receipt hashing** — every alert includes a SHA-256 receipt hash over (rule_id, labeler_did, ts, inputs, evidence_hashes, config_hash) for audit trail integrity.
- **Pure classifier** — `classify.py` is a pure function (no network, no DB). Classification is derived from structured evidence, not ad-hoc heuristics.
- **Sticky evidence** — once a labeler evidence field (observed_as_src, has_labeler_service, etc.) is set to true, it is never downgraded by transient failures.
- **Warm-up gating** — new labelers suppress alerts until they have sufficient scan history, age, and event volume.
- **Derive module** — `derive.py` is a pure module (no DB, no network). Produces regime state, auditability risk, inference risk, and temporal coherence from `LabelerSignals`. Four dials, not one trust score.
- **Derived receipts** — state changes in regime/risk are append-only receipted in `derived_receipts` table. Emit on change only.

## File structure

```
src/labelwatch/
  config.py     — Config dataclass, TOML loader
  db.py         — SQLite schema (v8), connect, init, migrations, evidence/probe/derive CRUD
  classify.py   — Pure classifier: EvidenceDict → Classification (visibility, reachability, auditability)
  derive.py     — Pure derive module: LabelerSignals → regime state, risk scores, temporal coherence
  discover.py   — Labeler discovery via listReposByCollection, DID resolution, endpoint probing
  ingest.py     — HTTP polling via queryLabels, event normalization, observed-only tracking
  resolve.py    — DID document resolution, service endpoint and label key extraction
  rules.py      — Detection rules: label_rate_spike, flip_flop, target_concentration, churn_index
  scan.py       — Orchestrator: runs rules, computes receipts, writes alerts, runs derive pass with hysteresis
  report.py     — Static HTML + JSON report: census page, triage views, evidence expanders
  receipts.py   — config_hash and receipt_hash helpers
  runner.py     — Continuous ingest/scan loop with heartbeat timestamps
  cli.py        — argparse CLI: ingest, scan, report, export, discover, labelers, census, reclassify, run
  utils.py      — Timestamps, hashing, sqlite_safe_text, git commit detection
tests/
  test_ingest.py, test_rules_spikes.py, test_concentration.py, test_churn.py,
  test_receipts_shape.py, test_rules_overlap.py, test_discovery.py,
  test_multi_ingest.py, test_schema_v4.py, test_classify.py,
  test_warmup.py, test_report_census.py, test_resolve.py, test_derive.py
```

## Key tables (schema v8)

- `labelers` — per-labeler profile with classification, derive scores (regime_state, auditability_risk, inference_risk, temporal_coherence), hysteresis state (regime_pending, regime_pending_count), prev scores for deltas, sticky evidence fields
- `label_events` — append-only label events
- `alerts` — detection results with receipt hashes; warmup_alert flag for legacy quarantine
- `labeler_evidence` — append-only evidence records per labeler
- `labeler_probe_history` — append-only endpoint probe results
- `derived_receipts` — append-only state change receipts for regime/risk derivations

## Commands

```bash
pytest tests/ -v          # run all tests
labelwatch --help         # CLI usage
labelwatch ingest --config config.toml
labelwatch scan --config config.toml
labelwatch discover --config config.toml
labelwatch labelers --visibility-class declared
labelwatch census
labelwatch reclassify --dry-run
labelwatch report --format html --out report --now max
```
