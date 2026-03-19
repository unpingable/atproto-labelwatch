# CLAUDE.md

Labelwatch is an observatory for ATProto's labeling infrastructure. It discovers labelers, ingests label events, detects anomalies, analyzes boundary instability between labelers, and surfaces hosting-locus distribution of labeled targets. It observes labeler behavior — it does not moderate content, judge truth, or emit labels.

Public at https://labelwatch.neutral.zone. Bluesky account: @labelwatch.neutral.zone.

## Architecture rules

- **Observation only** — no content moderation, no truth adjudication, no "bad labeler" verdicts.
- **Aggregate-first** — ecosystem-level and labeler-level analysis, not per-account profiling.
- **Append-only events** — `label_events` rows are never updated or deleted.
- **Receipt hashing** — every alert includes a SHA-256 receipt hash for audit trail integrity.
- **Pure classifier** — `classify.py` is a pure function (no network, no DB).
- **Sticky evidence** — once a labeler evidence field is set to true, never downgraded by transient failures.
- **Warm-up gating** — new labelers suppress alerts until sufficient scan history.
- **Coverage watermark** — anomaly rules suppressed when coverage < 0.5. Baselines frozen during gaps.
- **Four dials, not one trust score** — regime state, auditability risk, inference risk, temporal coherence.
- **Descriptive language** — "concentration anomaly" not "bot farm." Host family is not operator identity.

## Populations (know which one you're measuring)

- **Label events**: append-only ingested labels from all discovered labelers.
- **Labeled targets**: unique DIDs appearing as targets in label events (`target_did` column).
- **Resolved targets**: labeled-target DIDs that have been enriched via the driftwatch facts bridge.
- Coverage must always specify denominator. Actor coverage (unique DIDs) ≠ event coverage (label events).

## Cross-system integration

**Driftwatch facts bridge** (`driftwatch_facts_path` in config):
- Labelwatch ATTACHes `facts.sqlite` read-only as `drift` database.
- `drift.uri_fingerprint` — post URI → claim fingerprint (for lag analysis).
- `drift.actor_identity_facts` — DID → PDS host mapping (for hosting-locus analysis).
- Changes to the facts schema are data contract changes. See `docs/HOSTING-LOCUS-DATA-CONTRACT.md`.

**Hosting locus** (`hosting.py`):
- `provider_registry` table classifies hosts: exact/suffix matching → bluesky/known_alt/one_off/unknown.
- `extract_host_family()` derives registered domain from PDS hostname.
- Preview card on landing page — always shows coverage %, always shows missingness.
- Host family is not operator identity (rake #2). Current-state enrichment is not historical truth (rake #7).

## File structure

```
src/labelwatch/
  config.py            — Config dataclass, TOML loader
  db.py                — SQLite schema (v21), connect, init, migrations v0→v21
  classify.py          — Pure classifier: EvidenceDict → Classification
  derive.py            — Pure derive: LabelerSignals → regime state, risk scores, coherence
  discover.py          — Labeler discovery: listReposByCollection, DID resolution, probing
  discovery_stream.py  — Async Jetstream listener for app.bsky.labeler.service records
  ingest.py            — HTTP polling via queryLabels, multi-ingest for per-labeler endpoints
  resolve.py           — DID document resolution, service endpoint extraction
  rules.py             — Detection: label_rate_spike, flip_flop, target_concentration, churn_index
  scan.py              — Orchestrator: rules, receipts, alerts, derive pass, facts sync
  report.py            — Static HTML+JSON: census, triage views, boundary, hosting locus, robots/sitemap
  climate.py           — Per-DID label climate report generation
  server.py            — HTTP API: /v1/climate, /v1/registry, /health
  boundary.py          — Cross-labeler boundary instability: JSD, contradiction edges, disagreement types
  label_family.py      — Label family normalization and domain classification
  hosting.py           — PDS host classification, provider registry, hosting-locus queries
  publication.py       — Publication tier assessment: internal/reviewable/ready
  findings.py          — Boundary fight formatter for Bluesky posting
  posting.py           — ATProto SDK wrapper for posting findings
  provenance.py        — Labeler provenance scorecard
  read_health.py       — Per-labeler read health tracking
  signal_health.py     — Per-labeler EPS baseline monitoring
  receipts.py          — config_hash and receipt_hash helpers
  runner.py            — Continuous ingest/scan/report loop with report thread
  registry.py          — Labeler registry page generation
  whatsonme.py         — Account-level label lookup via network queryLabels
  cli.py               — argparse CLI: 16 subcommands
  utils.py             — Timestamps, hashing, DID resolution, git commit detection
```

## Key tables (schema v21)

- `label_events` — append-only ingested labels with `target_did` extraction
- `labelers` — registry with classification, regime state, risk scores, volume stats, reference flag
- `alerts` — detection results with receipt hashes; warmup_alert flag
- `labeler_evidence` — append-only classification evidence
- `discovery_events` — Jetstream/batch/backstop discovery audit trail
- `boundary_edges` — cross-labeler contradiction edges (JSD, shared targets, family version)
- `boundary_targets` — per-target boundary composition snapshots
- `provider_registry` — PDS host → provider group classification (exact/suffix matching)
- `derived_label_fp` — label events joined to driftwatch claim fingerprints (lag analysis)
- `derived_author_day` — rollup: label counts per author per day
- `derived_author_labeler_day` — rollup: per author/labeler/day
- `posted_findings` — deduplication ledger for Bluesky posts
- `ingest_outcomes` — per-labeler per-attempt fetch results for coverage tracking

## What not to do

- Don't treat host family as operator identity.
- Don't treat current-state PDS enrichment as historical-at-time-of-label truth.
- Don't produce accusation-shaped outputs.
- Don't add enrichment without a named analytic question.
- Don't let "coverage" appear without specifying the denominator.
- Don't modify the facts bridge schema without updating the data contract doc.

## Commands

```bash
pytest tests/ -v                                    # run all tests
labelwatch --help                                   # CLI usage
labelwatch ingest --config config.toml              # fetch label events
labelwatch scan --config config.toml                # run detection rules
labelwatch report --format html --out report/       # generate HTML report
labelwatch discover --backstop                      # scrape labeler lists
labelwatch discover-stream                          # Jetstream discovery sidecar
labelwatch climate --did did:plc:...                # label climate report
labelwatch whatsonme @handle.bsky.social            # account labels
labelwatch serve --port 8423                        # HTTP API server
labelwatch hosting-locus --facts /path/to/facts.sqlite  # hosting analysis
labelwatch assess                                   # publication readiness
labelwatch post "text"                              # post to Bluesky
labelwatch db-optimize                              # run ANALYZE
```
