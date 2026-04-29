# labelwatch — Dataflow

**Status**: v0 starter.
**Last updated**: 2026-04-28

## The job

This doc traces how data moves through labelwatch — discovery to ingest to scan to derive to publication. Each stage names its inputs, outputs, gates, and where it can fail.

For component-level detail (functions, classes, table schemas), see `../../ARCHITECTURE.md`. This doc is the narrative.

## The pipeline (end-to-end)

```
[ATProto] → discovery → ingest → scan → derive → rollup → report / climate API
                  │         │      │       │        │
                  ▼         ▼      ▼       ▼        ▼
                 SQLite (WAL, schema v19) — single source of truth
```

## Stage 1 — Discovery

**Job**: find labelers worth polling.

**Three channels** (belt-and-suspenders):

1. **Batch enumeration** (`discover.py`) — list known labeler DIDs, resolve via PLC directory, probe endpoints, collect evidence (declared_record, did_doc_service, did_doc_key, observed_as_src).
2. **Jetstream sidecar** (`discovery_stream.py`, `labelwatch-discovery.service`) — async WebSocket listener for `app.bsky.labeler.service` records. Worker queue: receive loop only parses JSON + updates cursor; DID resolution and DB writes happen off the event loop. Cursor persistence with 3s rewind on reconnect.
3. **Labeler-lists backstop** — every 6h, scrape `labeler-lists.bsky.social` AppView. Catches anything Jetstream missed.

**Outputs**: rows in `labelers` (upsert with sticky fields), `labeler_evidence` (append-only), `discovery_events` (append-only audit trail with UNIQUE on did+rev+op).

**Gates**: discovery DB write failure → crash (let systemd restart). No "dead but optimistic" state.

**Coverage delta**: upstream count vs registry count surfaced in meta + report card.

## Stage 2 — Ingest

**Job**: poll label events from each labeler.

**Mechanism**: HTTP polling via `com.atproto.label.queryLabels` (`ingest.py`). Multi-ingest mode: queries each labeler endpoint individually if a primary aggregator returns 401 (e.g., bsky.social as of 2026-03-05).

**Cursor persistence**: per-labeler cursors in `ingest_cursors` table. Avoids redundant re-fetches across restarts.

**Normalization**: raw label JSON → `LabelEvent` dataclass → SHA-256 `event_hash` → bulk `INSERT OR IGNORE` into `label_events`.

**Outputs**: rows in `label_events` (append-only), updated `last_seen` on `labelers`.

**Gates**: HTTP timeout / 4xx → run fails; runner retries on next interval. Event hash dedupe is the safety net for cursor drift.

## Stage 3 — Scan (detection rules)

**Job**: detect anomalies in labeler behavior.

**Four rules** (`rules.py`), all warm-up gated:

- **`label_rate_spike`** — current window vs baseline, ratio > `spike_k` or zero-baseline + min count.
- **`flip_flop`** — apply → negate → re-apply within window.
- **`target_concentration`** — HHI on target URI distribution.
- **`churn_index`** — Jaccard distance of target sets across two adjacent half-windows.

**Receipt**: each alert gets `config_hash` + `receipt_hash` (SHA-256 over rule_id, labeler_did, ts, inputs, evidence_hashes, config_hash). Stored in `alerts` (append-only).

**Outputs**: alerts with `warmup_alert` flag, `scan_count` incremented in meta.

**Gates**: warm-up state suppresses rate-based rules; sparse state suppresses rate rules entirely; pattern rules can still fire.

## Stage 4 — Derive (state classification)

**Job**: produce per-labeler signals from observed behavior.

**Pure module** (`derive.py`): no DB / network. Takes `LabelerSignals` dataclass → produces four independent signals:

- **`regime_state`** — warming_up / inactive / flapping / degraded / ghost_declared / dark_operational / bursty / stable.
- **`auditability_risk`** (0-100) — structural observability risk. High = hard to inspect.
- **`inference_risk`** (0-100) — epistemic risk. High = conclusions likely shaky.
- **`temporal_coherence`** (0-100) — history usability. High = past behavior is a usable predictor.

**Hysteresis**: regime state changes require N consecutive passes before commit. No flapping.

**Boundary pass** (`boundary.py`, `label_family.py`): finds shared targets between labeler pairs, computes JSD divergence, identifies contradiction edges. Filtered to moderation-vs-moderation conflicts with ≥2 shared targets for "fight cards."

**Outputs**: updated `labelers` rows (regime_state, risk scores, coherence), `derived_receipts` on state change, `boundary_edges` and `boundary_targets` (recomputed each pass).

## Stage 5 — Rollups

**Job**: pre-compute per-author/labeler/day aggregates for the climate API.

**Tables**: `derived_author_day` (target_did, day, total_labels, distinct_labelers, distinct_values), `derived_author_labeler_day` (target_did, labeler_did, day, label_count, distinct_values).

**Population**: ~1M+ rows in author_day, ~1.2M+ in author_labeler_day (as of 2026-03-05). Updated each derive pass.

**These rollups power the climate API. They are *receiving-end* accounts.** See `PUBLIC_SURFACES.md`.

## Stage 6 — Report (static)

**Job**: render the dashboard.

**`report.py`** generates static HTML + JSON: overview with triage tabs (Active/Alerts/New/Opaque/All), census, per-labeler pages, per-alert pages. Atomic directory swap for safe updates.

**Includes**: warm-up banner, staleness indicators, alert rollups for low-confidence alerts, build signature, clock-skew detection, naive-timestamp warnings.

**No XSS surface**: HTML is escaped, no user-controlled JS.

## Stage 7 — Climate API (on-demand)

**Job**: serve per-DID receiving-end reports.

**`server.py`** wraps `climate.py` queries against the rollup tables. `GET /v1/climate/{did_or_handle}` → JSON with summary stats, top labelers, top label values, daily series, example posts.

**Gates**: token bucket per IP, disk cache (5min TTL, atomic writes), concurrency semaphore, generation timeout (10s), kill switch (`CLIMATE_API_DISABLED`), payload whitelist strips `recent_receipts`.

**Boundary**: this surface is receiving-end only. See `PUBLIC_SURFACES.md`.

## Heartbeats

Each subsystem writes a heartbeat to `meta`:

- `last_ingest_ok_ts`
- `last_scan_ok_ts`
- `last_report_ok_ts`
- `last_discovery_ok_ts`

Used by half-dead-state detection. If a heartbeat is stale, the report card flags it.

## Subsystem isolation

The runner (`runner.py`) wraps each subsystem in try/except. A scan crash doesn't kill ingest; a report crash doesn't kill scan. Each subsystem fails independently; systemd restarts the process if the runner itself dies.

## Cross-reference

- `OVERVIEW.md` — the system map.
- `PUBLICATION_MODEL.md` — the publication-side cut of stages 5–7.
- `PUBLIC_SURFACES.md` — what's exposed.
- `FAILURE_MODES.md` — what can go wrong at each stage.
- `../../ARCHITECTURE.md` — components, classes, tables.
- `../../THEORY_TO_CODE.md` — concept → file map (if exists; driftwatch has one).
