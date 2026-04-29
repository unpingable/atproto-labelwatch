---

# labelwatch Architecture

**Version**: 0.9
**Last Updated**: 2026-04-28
**Owner**: James Beck / unpingable
**Status**: Deep architectural reference. For orientation, start at [`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md).

---

## 1. Quick Reference

For high-level orientation, see the architecture tree:

| Want | Go to |
|------|-------|
| One-paragraph purpose, five-questions frame | [`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md) |
| System diagram (rendered mermaid) | [`docs/architecture/diagrams/system-overview.md`](docs/architecture/diagrams/system-overview.md) |
| Data flow narrative | [`docs/architecture/DATAFLOW.md`](docs/architecture/DATAFLOW.md) |
| Publication model | [`docs/architecture/PUBLICATION_MODEL.md`](docs/architecture/PUBLICATION_MODEL.md) |
| Public surfaces, dossier prohibition | [`docs/architecture/PUBLIC_SURFACES.md`](docs/architecture/PUBLIC_SURFACES.md) |
| Failure modes | [`docs/architecture/FAILURE_MODES.md`](docs/architecture/FAILURE_MODES.md) |
| Specs (binding contracts) | [`specs/`](specs/) |
| Non-goals (bulleted) | [`NON_GOALS.md`](NON_GOALS.md) |

This document is the deep architectural reference: component inventory, data model, component deep dives, integration patterns, security, and evolution. The architecture/ tree is the orientation layer; this is its engineering-level companion.

### 1.1 Component Inventory

| Component | Responsibility | Source |
|-----------|---------------|--------|
| Config | TOML config loading, dataclass with defaults | `config.py` |
| DB | SQLite schema (v19), connection, migrations, all CRUD | `db.py` |
| Classify | Pure classifier: EvidenceDict → Classification (no network, no DB) | `classify.py` |
| Discover | Batch labeler discovery, DID resolution, endpoint probing, evidence collection | `discover.py` |
| Discovery Stream | Async Jetstream listener for real-time labeler discovery (sidecar) | `discovery_stream.py` |
| Resolve | DID document resolution, service endpoint and label key extraction | `resolve.py` |
| Ingest | HTTP polling via queryLabels, event normalization, multi-ingest, observed-only tracking | `ingest.py` |
| Rules | Detection logic: rate spike, flip-flop, concentration, churn; warmup gating | `rules.py` |
| Derive | Pure regime/risk/coherence classifiers from LabelerSignals (no DB, no network) | `derive.py` |
| Scan | Orchestrator: runs rules, computes receipts, writes alerts, runs derive + boundary pass | `scan.py` |
| Label Family | Label normalization (v2) and domain classification (moderation/metadata/novelty/political) | `label_family.py` |
| Boundary | Cross-labeler disagreement: JSD divergence, contradiction edges, shared-target finding | `boundary.py` |
| Climate | Per-DID label activity: rollups, daily series, top labelers/values, examples | `climate.py` |
| Server | HTTP API for climate reports: rate limiting, disk cache, concurrency gate | `server.py` |
| Report | Static HTML + JSON: census, triage views, evidence expanders, health cards, boundary cards | `report.py` |
| Receipts | config_hash and receipt_hash computation | `receipts.py` |
| Runner | Continuous ingest/scan/report loop with heartbeat timestamps, memory management | `runner.py` |
| CLI | argparse entry point: 13 subcommands | `cli.py` |
| Utils | Timestamps, hashing, git commit detection | `utils.py` |

---

## 2. Core Invariants

See [`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md) § Core invariants. Canonical there to avoid drift between two locations.

---

## 3. Design Rationale

### 3.1 Technology Choices

| Technology | Purpose | Trade-off Accepted |
|------------|---------|-------------------|
| SQLite | Embedded database, zero external dependencies | Single-writer; acceptable for single-process MVP |
| `com.atproto.label.queryLabels` | HTTP polling for label events | Not real-time; misses events between polls; no cursor persistence across restarts |
| Python stdlib (`urllib.request`) | HTTP client for polling | No async, no connection pooling; acceptable at MVP scale |
| SHA-256 hashing | Event dedup and receipt integrity | Not cryptographic signing — receipts are verifiable but not attributable to a specific signer |
| TOML config | Simple, human-readable configuration | No runtime config reloading |

### 3.2 Architectural Patterns

| Pattern | Where Used | Why |
|---------|------------|-----|
| Event sourcing (append-only) | `label_events` table | Need complete temporal history for retrospective analysis |
| Receipt chain | `alerts` table | Each alert is independently verifiable via its receipt hash |
| Static site generation | `report.py` | No web server dependency; host anywhere; works offline |

---

## 4. Data Model

### 4.1 Tables

| Table | Purpose | Key Fields | Mutability |
|-------|---------|------------|------------|
| `label_events` | Raw label events from queryLabels | labeler_did, uri, val, neg, ts, event_hash, target_did | Append-only (INSERT OR IGNORE) |
| `labelers` | Per-labeler profile + classification + derive scores + volume stats | labeler_did, visibility_class, reachability_state, auditability, regime_state, risk scores, events_7d/30d, unique_targets/subjects_7d/30d | Upsert (sticky fields never downgraded) |
| `alerts` | Detection results with receipts | rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash, warmup_alert | Append-only |
| `labeler_evidence` | Append-only evidence records | labeler_did, evidence_type, evidence_value, ts, source | Append-only (dedupe within run) |
| `labeler_probe_history` | Endpoint probe results | labeler_did, ts, endpoint, http_status, normalized_status, latency_ms, failure_type | Append-only |
| `derived_receipts` | State change receipts for regime/risk derivations | labeler_did, receipt_type, derivation_version, trigger, ts, input_hash, previous/new_value_json, reason_codes_json | Append-only |
| `discovery_events` | Jetstream/batch/backstop discovery audit trail | labeler_did, operation, source, time_us, commit_cid, record_json, record_sha256 | Append-only (UNIQUE on did+rev+op) |
| `boundary_edges` | Cross-labeler contradiction/divergence edges | labeler_a, labeler_b, edge_type, jsd, shared_targets, computed_at | Recomputed per derive pass |
| `boundary_targets` | Shared targets between labeler pairs | labeler_a, labeler_b, target_uri, computed_at | Recomputed per derive pass |
| `derived_author_day` | Rollup: label counts per author per day | target_did, day, total_labels, distinct_labelers, distinct_values | Upsert per derive pass |
| `derived_author_labeler_day` | Rollup: label counts per author/labeler/day | target_did, labeler_did, day, label_count, distinct_values | Upsert per derive pass |
| `meta` | Key-value store for schema version, build info, heartbeats, cursors | key, value | Upsert |

### 4.2 Labeler Classification

Each labeler has a `visibility_class` (what kind of thing it is) and a `reachability_state` (can we talk to it), derived from structured evidence by a pure classifier function (`classify.py`).

**Visibility classes:**
- `declared` — found via app declaration record (listReposByCollection)
- `protocol_public` — DID doc has labeler service/key, no app declaration
- `observed_only` — seen as `src` in labels, no declaration or DID doc labeler
- `unresolved` — DID seen but metadata fetch failed/incomplete

**Reachability states:** `accessible`, `auth_required`, `down`, `unknown`

**Auditability:** high (declared + accessible), medium (partial evidence), low (inference only)

**Sticky evidence fields:** `observed_as_src`, `has_labeler_service`, `has_label_key`, `declared_record` — once set to 1, never downgraded by transient probe failures.

### 4.3 Indexes

- `idx_label_events_labeler_ts` — (labeler_did, ts) for time-windowed queries per labeler
- `idx_label_events_uri_ts` — (uri, ts) for target-based queries
- `idx_alerts_rule_ts` — (rule_id, ts) for rule-based alert queries
- `idx_labeler_evidence_did` — (labeler_did, evidence_type) for evidence lookups
- `idx_probe_history_did_ts` — (labeler_did, ts) for probe history queries
- `idx_derived_receipts_did_type` — (labeler_did, receipt_type, ts) for derived state lookups

### 4.4 Schema Version

Schema version is tracked in the `meta` table (`key = "schema_version"`). Current version: 19. Migrations are handled in `db.init_db()` with automatic upgrades through all versions. See `NEXT.md` for the full schema history table.

---

## 5. Component Deep Dives

### 5.1 Ingest (`ingest.py`)

Polls `com.atproto.label.queryLabels` for configured labeler DIDs. Normalizes raw label JSON into `LabelEvent` dataclass, computes SHA-256 event hash for deduplication, and bulk-inserts into `label_events`. Supports pagination via cursor. Also accepts JSONL fixture files for testing.

### 5.2 Classify (`classify.py`)

Pure classifier function with no network or DB dependencies. Takes an `EvidenceDict` (boolean flags for declared_record, did_doc_service, did_doc_key, observed_src, plus probe result) and returns a `Classification` with visibility_class, reachability_state, auditability, confidence, and reason string. Decision tree: declared > protocol_public > observed_only > unresolved. Confidence is weighted by evidence independence (strong: probe/observed; medium: declarations).

Also includes `detect_test_dev()` for noise tagging based on handle/display_name patterns.

### 5.3 Rules (`rules.py`)

Four detection rules, all with warm-up gating:

- **`label_rate_spike`**: Compares label rate in a recent window (default 15 min) against a baseline period (default 24h). Fires when the ratio exceeds `spike_k` (default 10x) or when baseline is zero and current count exceeds `min_current_count`. Two-tier thresholds for reference vs non-reference labelers.

- **`flip_flop`**: Detects apply → negate → re-apply sequences on the same (uri, val) pair within a window (default 24h).

- **`target_concentration`**: HHI (Herfindahl-Hirschman Index) on target URI distribution. High HHI indicates fixation on few targets.

- **`churn_index`**: Jaccard distance of target sets across two adjacent half-windows. High distance indicates rapid target turnover.

**Warm-up gating:** Rules check `_warmup_state()` before emitting alerts. New labelers ("warming_up") can be suppressed or tagged. Labelers with insufficient event volume ("sparse") have rate-based rules (spike, churn) suppressed while pattern rules (flip_flop, concentration) can still fire.

All rules collect evidence hashes (capped at `max_evidence`) pointing to specific label_events rows.

### 5.4 Derive (`derive.py`)

Pure module with no DB or network dependencies. Takes a `LabelerSignals` dataclass (event counts, probe stats, classification state, churn metrics) and produces four independent signals:

- **`regime_state`**: Priority cascade classifier — warming_up / inactive / flapping / degraded / ghost_declared / dark_operational / bursty / stable. Each state has explicit reason codes.
- **`auditability_risk`** (0-100): Structural observability risk. High score means the labeler is hard to inspect, regardless of behavior quality.
- **`inference_risk`** (0-100): Epistemic risk. High score means dashboard conclusions are likely shaky (warmup, low volume, churn, sparse probes).
- **`temporal_coherence`** (0-100): History usability. High score means past behavior is a usable predictor.

Design constraint: four separate dials, not one collapsed trust score.

### 5.5 Scan (`scan.py`)

Orchestrator that runs all rules, computes config_hash and receipt_hash for each alert, writes results to the `alerts` table (with `warmup_alert` flag), increments `scan_count`, then runs the derive pass. The derive pass builds `LabelerSignals` from batched DB queries (~6 total), runs all four classifiers with regime hysteresis (requires N consecutive passes before state change), shifts current scores to prev for delta rendering, emits derived receipts on state change, and updates labeler rows. Entry points: `run_scan(conn, config, now)` and `run_derive(conn, config, now)`.

### 5.6 Discovery Stream (`discovery_stream.py`)

Async Jetstream listener running as a separate systemd service. Subscribes to `app.bsky.labeler.service` records via WebSocket. Worker queue architecture: receive loop only parses JSON and updates cursor; DID resolution and DB writes happen in a worker task off the event loop. Cursor persistence with 3s rewind on reconnect for gapless replay. Backstop loop scrapes `labeler-lists.bsky.social` every 6h as belt-and-suspenders. Crashes on DB write failures (let systemd restart) rather than running in a "dead but optimistic" state.

### 5.7 Boundary Analysis (`boundary.py`, `label_family.py`)

Cross-labeler disagreement detection. `label_family.py` normalizes label values into families (v2 scheme) and classifies them by domain (moderation / metadata / novelty / political). `boundary.py` finds shared targets between labeler pairs, computes JSD divergence of label distributions, identifies contradiction edges (same target, conflicting labels), and filters fight edges (moderation-vs-moderation conflicts with 2+ shared targets). Results stored in `boundary_edges` and `boundary_targets` tables, surfaced as report cards.

### 5.8 Climate (`climate.py`, `server.py`)

Per-DID label climate reporting. `climate.py` queries rollup tables (`derived_author_day`, `derived_author_labeler_day`) to produce summary stats, top labelers, top label values, daily time series, and example posts with clickable bsky.app links. `server.py` wraps this in an HTTP API (`/v1/climate/{did}`) with token bucket rate limiting, disk caching (5min TTL, atomic writes), concurrency semaphore, generation timeout, and a kill switch. Public payload whitelist strips `recent_receipts`.

### 5.9 Report (`report.py`)

Generates a static HTML + JSON site: overview page with triage views (Active/Alerts/New/Opaque/All tabs), census page with visibility/reachability/confidence/auditability breakdowns, per-labeler pages with evidence expanders and probe history, per-alert pages. Includes warm-up banner, staleness indicators, alert rollups for low-confidence alerts, build signature, clock-skew detection, and naive-timestamp warnings. Uses atomic directory swap for safe updates. Per-labeler URLs use slug format (`did-plc-abc123.html`).

### 5.7 Runner (`runner.py`)

Continuous loop for systemd deployment. Runs discovery, ingest, scan, and report on configurable intervals. Each subsystem is wrapped in try/except so a failure in one (e.g. scan crash) doesn't kill the others. Writes heartbeat timestamps (`last_ingest_ok_ts`, `last_scan_ok_ts`, `last_report_ok_ts`, `last_discovery_ok_ts`) to the meta table for half-dead state detection.

---

## 6. Integration Patterns

### 6.1 External System Integrations

| External System | Integration Type | Data Exchanged |
|-----------------|------------------|----------------|
| ATProto service (e.g. bsky.social) | HTTP GET polling | Label events (JSON) via `com.atproto.label.queryLabels` |
| Individual labeler endpoints | HTTP GET (multi-ingest) | Label events directly from labeler services |
| PLC Directory | HTTP GET | DID documents for labeler resolution |
| Jetstream (bsky.network) | WebSocket (JSON) | `app.bsky.labeler.service` records for discovery |
| labeler-lists.bsky.social | HTTP GET (AppView) | Curated labeler lists for backstop discovery |

### 6.2 Error Handling

| Error Class | Detection | Response |
|-------------|-----------|----------|
| HTTP timeout / network error | `urllib.request` exception | Ingest run fails; runner retries on next interval |
| Malformed label event | `normalize_label` ValueError | Event skipped |
| DB write failure | SQLite exception | Transaction not committed; retried on next run |
| Jetstream disconnect | WebSocket close | Reconnect with 3s cursor rewind, backoff+jitter |
| Discovery DB write failure | SQLite exception | Crash (let systemd restart) — no "dead but optimistic" |
| Climate API overload | Concurrency semaphore full | 503 response; prevents resource exhaustion |

---

## 7. Failure Modes & Resilience

- **Ingest failure**: Runner retries on next interval. Cursor persistence across restarts avoids redundant re-fetches; events still deduped by event_hash as a safety net.
- **Discovery stream failure**: Crash on DB write errors (let systemd restart with clean state). Cursor persistence with 3s rewind ensures gapless replay.
- **Subsystem isolation**: Runner wraps each subsystem (ingest, scan, report, discovery) in try/except — a crash in one doesn't kill the others.
- **DB corruption**: SQLite WAL mode with busy_timeout. No automated backup. Restore from external backup or re-ingest.
- **Service crash**: systemd `restart=on-failure` with 5-10s backoff.

---

## 8. Performance & Scaling

**Current state**: MVP. Not benchmarked.

- Single-threaded Python process
- SQLite single-writer
- HTTP polling with configurable intervals
- Adequate for monitoring <100 labelers at polling intervals of minutes

TBD for post-MVP: performance profiling, connection pooling, async I/O.

---

## 9. Security Architecture

- Climate API binds to loopback only; Caddy reverse proxy handles TLS and external access
- Token bucket rate limiting on `/v1/climate/{did}` (per-IP)
- Concurrency semaphore prevents resource exhaustion from concurrent report generation
- Generation timeout (10s) prevents slow queries from blocking the server
- Path traversal protection on DID parameters
- Kill switch (`CLIMATE_API_DISABLED`) for emergency shutdown of query layer
- Public payload whitelist strips `recent_receipts` from climate responses
- No secrets beyond the ATProto service URL
- SQLite database is local-only
- Report output is static files (no XSS vectors — HTML is escaped)
- systemd hardening: ProtectSystem=strict, NoNewPrivileges, ReadOnlyPaths

---

## 10. Operational Architecture

### 10.1 Deployment

Three systemd services on a shared host:
- `labelwatch.service` — main loop (ingest, scan, derive, report)
- `labelwatch-discovery.service` — Jetstream sidecar for real-time labeler discovery
- `labelwatch-api.service` — climate HTTP server

All run as `labelwatch:labelwatch` system user. Config at `/opt/labelwatch/config.toml`, DB at `/var/lib/labelwatch/labelwatch.db`. Deploy via rsync + pip install + systemctl restart.

### 10.2 Observability

- STATS heartbeat line from discovery stream (every 60s)
- Heartbeat timestamps in meta table (last_ingest_ok_ts, last_scan_ok_ts, etc.)
- Report includes build signature, clock-skew diagnostics, discovery health cards
- RSS memory tracking in runner with periodic `_release_memory()` passes
- `/health` endpoint on climate API

---

## 11. Known Tensions & Technical Debt

### 11.1 Unresolved Tensions

| Tension | Current State | Notes |
|---------|---------------|-------|
| HTTP polling vs streaming for labels | Polling via queryLabels for label events | Misses events between polls. Cursor persistence across restarts. Jetstream provides real-time discovery but not label events. |
| SHA-256 hashing vs cryptographic signing | Receipt hashes are SHA-256 digests | Verifiable (reproducible) but not attributable to a specific signer. True cryptographic signing would require key management. |
| Three processes vs one | Main + discovery + API as separate services | Good failure isolation but means three things to monitor. |

### 11.2 Technical Debt

| Item | Severity | Notes |
|------|----------|-------|
| ~~No cursor persistence across restarts~~ | ~~Medium~~ | Done — `ingest_cursors` table |
| No structured logging | Low | print() statements; hard to parse at scale |
| No integration tests against live ATProto | Medium | Only synthetic fixture tests exist |

---

## 12. Evolution

See `NEXT.md` for the full roadmap. Key upcoming work:

- **"What's on me?" view** — account-level labels via AppView `queryLabels` (different data source than local ingest)
- **Silence Adjudicator** — regime classifier for labeler silence (why quiet, not just is quiet)
- **Boundary Phase 2 synthesis** — BoundaryFightCard, conflict-only report cards
- **Public label query API** — `GET /v1/labels?subject={did}` — the inverse query ATProto doesn't provide
- **Receipt chain** — `prev_receipt_hash` for tamper-evident alert trail

---

## 13. Appendices

### 13.1 Terminology

| Term | Definition |
|------|------------|
| Labeler | ATProto service that applies labels to content (moderation infrastructure) |
| Label event | A single label application or retraction from a labeler |
| Receipt hash | SHA-256 digest over alert inputs, providing an audit trail |
| Flip-flop | Pattern where a labeler applies, negates, and re-applies the same label |

### 13.2 External Documentation

| Document | Link |
|----------|------|
| ATProto Label Spec | https://atproto.com/specs/label |
| com.atproto.label.queryLabels | https://docs.bsky.app/docs/api/com-atproto-label-query-labels |

### 13.3 Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 0.1 | 2025-02-13 | unpingable | Initial draft (aspirational) |
| 0.2 | 2026-02-13 | unpingable | Rewrite to match actual implementation |
| 0.3 | 2026-02-24 | unpingable | Schema v4: evidence-based classification, warm-up gating, census |
| 0.4 | 2026-02-25 | unpingable | Schema v5: derive module (regime/risk/coherence), derived receipts, runner hardening, heartbeats |
| 0.5 | 2026-02-26 | unpingable | Schema v6-v8: regime hysteresis, score deltas (prev columns), warmup alert quarantine, badge/score suppression during warmup, safe deploy script |
| 0.6 | 2026-03-05 | unpingable | Schema v16-v17: My Label Climate (rollup tables, HTTP API), Jetstream discovery sidecar, coverage delta |
| 0.7 | 2026-03-08 | unpingable | Schema v18-v19: Boundary instability (label families, contradiction edges, JSD divergence), volume/reach tiers |
| 0.8 | 2026-03-10 | unpingable | Architecture doc refresh: three-service model, updated diagrams and component inventory, security and operational sections |
| 0.9 | 2026-04-28 | unpingable | Folded into `docs/architecture/` tree. Sections 1.1-1.3 and 2 collapsed into pointers; deep reference content (component inventory, data model, deep dives) retained here. |
