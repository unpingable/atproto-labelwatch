---

# labelwatch Architecture

**Version**: 0.2
**Last Updated**: 2026-02-13
**Owner**: James Beck / unpingable
**Status**: Draft — reflects actual MVP implementation

---

## 1. Quick Reference

### 1.1 One-Paragraph Purpose

labelwatch is a meta-governance tool for ATProto's label ecosystem. It polls label events from configured labeler DIDs via `com.atproto.label.queryLabels`, stores them in SQLite, runs detection rules to flag temporal anomalies (rate spikes, flip-flop patterns), and produces auditable alerts with SHA-256 receipt hashes. It observes labeler behavior — it does not moderate content, judge truth, or emit labels.

### 1.2 System Diagram

```
┌─────────────────────┐
│  ATProto Service     │
│  (queryLabels HTTP)  │
└──────────┬──────────┘
           │ HTTP polling
           ▼
┌─────────────────────┐     ┌──────────────────┐
│  Ingest             │────▶│  SQLite DB       │
│  (ingest.py)        │     │  label_events    │
└─────────────────────┘     │  labelers        │
                            │  alerts          │
                            │  meta            │
                            └────────┬─────────┘
                                     │
                                     │ queries
                                     ▼
┌─────────────────────┐     ┌──────────────────┐
│  Rules              │────▶│  Scan            │
│  (rules.py)         │     │  (scan.py)       │
│  rate spike         │     │  receipts +      │
│  flip-flop          │     │  alert writes    │
└─────────────────────┘     └────────┬─────────┘
                                     │
                                     ▼
                            ┌──────────────────┐
                            │  Report          │
                            │  (report.py)     │
                            │  HTML + JSON     │
                            └──────────────────┘
```

### 1.3 Data Flow

```
1. Ingest polls com.atproto.label.queryLabels for configured labeler DIDs
2. Label events normalized, hashed (SHA-256), and stored in label_events table
3. Labeler profiles upserted in labelers table (first_seen / last_seen)
4. Scan runs detection rules against label_events
5. Alerts written with receipt hashes (SHA-256 over rule + inputs + evidence + config)
6. Report generates static HTML + JSON site from DB state
```

### 1.4 Component Inventory

| Component | Responsibility | Source |
|-----------|---------------|--------|
| Config | TOML config loading, dataclass with defaults | `config.py` |
| DB | SQLite schema, connection, migrations, CRUD | `db.py` |
| Ingest | HTTP polling via queryLabels, event normalization | `ingest.py` |
| Rules | Detection logic: label_rate_spike, flip_flop | `rules.py` |
| Scan | Orchestrator: runs rules, computes receipts, writes alerts | `scan.py` |
| Report | Static HTML + JSON report generation | `report.py` |
| Receipts | config_hash and receipt_hash computation | `receipts.py` |
| Runner | Continuous ingest/scan loop for docker | `runner.py` |
| CLI | argparse entry point: ingest, scan, report, export, run | `cli.py` |
| Utils | Timestamps, hashing, git commit detection | `utils.py` |

---

## 2. Core Invariants

- **Observation only**: All analysis is grounded in observable label application patterns from the queryLabels endpoint. The system does not evaluate content semantics, judge labeling correctness, or emit labels.

- **Append-only events**: Rows in `label_events` are never updated or deleted. Deduplication happens at insert time via `INSERT OR IGNORE` on `event_hash`.

- **Receipt hashing**: Every alert includes a SHA-256 receipt hash computed over `(rule_id, labeler_did, ts, inputs, evidence_hashes, config_hash)`. This provides an audit trail — given the same inputs, the same receipt hash must be reproducible.

- **Temporal coherence**: Every alert references a specific time window and includes evidence hashes pointing to actual label_events rows. No unfalsifiable claims.

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
| `label_events` | Raw label events from queryLabels | labeler_did, uri, val, neg, ts, event_hash | Append-only (INSERT OR IGNORE) |
| `labelers` | Per-labeler profile tracking | labeler_did, first_seen, last_seen | Upsert (last_seen updated) |
| `alerts` | Detection results with receipts | rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash | Append-only |
| `meta` | Key-value store for schema version, build info | key, value | Upsert |

### 4.2 Indexes

- `idx_label_events_labeler_ts` — (labeler_did, ts) for time-windowed queries per labeler
- `idx_label_events_uri_ts` — (uri, ts) for target-based queries
- `idx_alerts_rule_ts` — (rule_id, ts) for rule-based alert queries

### 4.3 Schema Version

Schema version is tracked in the `meta` table (`key = "schema_version"`). Current version: 1. Migrations are handled in `db.migrate()`.

---

## 5. Component Deep Dives

### 5.1 Ingest (`ingest.py`)

Polls `com.atproto.label.queryLabels` for configured labeler DIDs. Normalizes raw label JSON into `LabelEvent` dataclass, computes SHA-256 event hash for deduplication, and bulk-inserts into `label_events`. Supports pagination via cursor. Also accepts JSONL fixture files for testing.

### 5.2 Rules (`rules.py`)

Two detection rules:

- **`label_rate_spike`**: Compares label rate in a recent window (default 15 min) against a baseline period (default 24h). Fires when the ratio exceeds `spike_k` (default 10x) or when baseline is zero and current count exceeds `min_current_count`.

- **`flip_flop`**: Detects apply → negate → re-apply sequences on the same (uri, val) pair within a window (default 24h). Any such sequence triggers an alert.

Both rules collect evidence hashes (capped at `max_evidence`) pointing to specific label_events rows.

### 5.3 Scan (`scan.py`)

Orchestrator that runs all rules, computes config_hash and receipt_hash for each alert, and writes results to the `alerts` table. Single entry point: `run_scan(conn, config, now)`.

### 5.4 Report (`report.py`)

Generates a static HTML + JSON site: overview page, per-labeler pages, per-alert pages. Includes build signature (package version, schema version, git commit, config hash), clock-skew detection, and naive-timestamp warnings. Uses atomic directory swap for safe updates.

### 5.5 Runner (`runner.py`)

Continuous loop for Docker deployment. Runs ingest on a configurable interval (default 120s), scan on another (default 300s), and optionally regenerates the HTML report after each scan.

---

## 6. Integration Patterns

### 6.1 External System Integrations

| External System | Integration Type | Data Exchanged |
|-----------------|------------------|----------------|
| ATProto service (e.g. bsky.social) | HTTP GET polling | Label events (JSON) via `com.atproto.label.queryLabels` |

### 6.2 Error Handling

| Error Class | Detection | Response |
|-------------|-----------|----------|
| HTTP timeout / network error | `urllib.request` exception | Ingest run fails; runner retries on next interval |
| Malformed label event | `normalize_label` ValueError | Event skipped |
| DB write failure | SQLite exception | Transaction not committed; retried on next run |

---

## 7. Failure Modes & Resilience

**Current state**: MVP. Single-process, no redundancy.

- **Ingest failure**: Runner retries on next interval. No cursor persistence across restarts — may re-fetch events (deduped by event_hash).
- **DB corruption**: No automated backup. Restore from external backup or re-ingest.
- **Process crash**: Docker `restart: unless-stopped` handles restarts.

TBD for post-MVP: health checks, structured logging, backup automation.

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

**Current state**: MVP. Minimal attack surface.

- No authentication (no web server, no API endpoints)
- No secrets beyond the ATProto service URL
- SQLite database is local-only
- Report output is static files (no XSS vectors — HTML is escaped)

TBD for post-MVP: if a web endpoint is added, authentication and rate limiting will be needed.

---

## 10. Operational Architecture

### 10.1 Deployment

Single Docker Compose service running the `labelwatch run` loop. Config mounted read-only, data volume for SQLite DB and reports.

### 10.2 Observability

- stdout logging (print statements)
- Report includes build signature and clock-skew diagnostics

TBD for post-MVP: structured logging, metrics endpoint.

---

## 11. Known Tensions & Technical Debt

### 11.1 Unresolved Tensions

| Tension | Current State | Notes |
|---------|---------------|-------|
| HTTP polling vs streaming | Polling via queryLabels | Misses events between polls; no cursor persistence across restarts. Jetstream would provide real-time streaming but adds complexity. |
| SHA-256 hashing vs cryptographic signing | Receipt hashes are SHA-256 digests | Verifiable (reproducible) but not attributable to a specific signer. True cryptographic signing would require key management. |
| Single-threaded | One process does ingest + scan + report | Adequate for MVP scale. Would need separation for higher throughput. |

### 11.2 Technical Debt

| Item | Severity | Notes |
|------|----------|-------|
| No cursor persistence across restarts | Medium | Re-fetches events on restart; deduped but wasteful |
| No structured logging | Low | print() statements; hard to parse at scale |
| No integration tests against live ATProto | Medium | Only synthetic fixture tests exist |

---

## 12. Evolution

Potential future work (not committed):

- **Coordination / co-movement rules**: Detect synchronized spikes or shared-target overlap across multiple labelers.
- **Jetstream streaming**: Replace HTTP polling with ATProto Jetstream for real-time event ingestion.
- **Web inspection endpoint**: Simple `/health` and `/recent-alerts` HTTP endpoint for operational monitoring.
- **Metrics endpoint**: Expose ingest rates, alert counts, scan latency for monitoring dashboards.
- **Meta-labeler emission**: Optionally emit labels about labeler behavior back into ATProto.
- **Labeler policy checking**: Weak provenance signals from labeler service declarations.

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
