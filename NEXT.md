# Next

## Done

1. **Cursor persistence** — save/restore polling cursor across restarts. `ingest_cursors` table, wired into `ingest.py` and `runner.py`.

2. **Richer metrics** — target concentration (HHI), churn index (Jaccard distance), flip-flop detection, rate spike detection. All implemented as rules in `rules.py` with warm-up gating.

3. **Report UI pass** — health cards, sparklines, behavioral badges, anomaly highlighting, triage views (Active/Alerts/New/Opaque/All), census page, evidence expanders, dark mode, alert rollups, warmup banners. Static HTML + JSON site generation.

4. **Deploy** — systemd unit on Linode, Caddy reverse proxy for static reports, resource limits (MemoryMax=2G, CPUQuota=50%). Safe deploy script (`scripts/deploy.sh`) with config.toml protection.

5. **Evidence-based classification** — pure classifier (`classify.py`): visibility class, reachability state, auditability, confidence. Sticky evidence fields. DID-to-handle resolution via PLC directory.

6. **Derive module** — pure regime/risk/coherence classifiers (`derive.py`): regime state machine (warming_up / inactive / flapping / degraded / ghost_declared / dark_operational / bursty / stable), auditability risk (0-100), inference risk (0-100), temporal coherence (0-100). Four dials, not one trust score.

7. **Regime hysteresis** — pending regime counter requires N consecutive derive passes (default 2, ~1 hour) before a regime transition takes effect. Eliminates threshold jitter.

8. **Score deltas** — previous derive scores persisted (`*_prev` columns), rendered as deltas on labeler pages (e.g., "72 (+4)").

9. **Warmup UI suppression** — scores card and alert-based badges suppressed during warmup. Legacy warmup alerts quarantined via `warmup_alert` column (schema v8).

10. **My Label Climate Phase 1** — schema v16: `target_did` column + backfill, `derived_author_day` and `derived_author_labeler_day` rollup tables, rollups wired into derive loop.

11. **My Label Climate Phase 3** — CLI `labelwatch climate --did <did>` generates JSON + standalone HTML climate reports from rollup tables. Summary cards, top labelers, top values, daily sparkline, recent receipts.

12. **My Label Climate Phase 2** — HTTP query layer. `server.py` with `GET /v1/climate/{did}` (json/html) and `GET /health`. Token bucket rate limiter, disk cache (atomic writes), concurrency semaphore, generation timeout, kill switch (`CLIMATE_API_DISABLED`). Public payload whitelist strips `recent_receipts`. Loopback-only bind. `labelwatch serve` CLI subcommand. Systemd unit (`labelwatch-api.service`). Climate lookup form on index page. Tier-0 hardening pass with `docs/HARDENING.md`.

## Schema history

| Version | What changed |
|---------|--------------|
| v1-v3 | Initial schema through early iterations |
| v4 | Evidence-based classification, labeler_evidence table, probe_history table |
| v5 | Derive module: regime_state, risk scores, temporal_coherence, derived_receipts table |
| v6 | Regime hysteresis: regime_pending, regime_pending_count columns |
| v7 | Score deltas: auditability_risk_prev, inference_risk_prev, temporal_coherence_prev |
| v8 | Warmup alert quarantine: warmup_alert column on alerts table |

13. **Jetstream discovery + governance sensors** — sidecar process (`discovery_stream.py`) listens to Jetstream for `app.bsky.labeler.service/self` records. Schema v17: `discovery_events` audit trail. Shared `upsert_discovered_labeler()` helper. Backstop scrape of `labeler-lists.bsky.social` on timer. Five report health cards: stream liveness, last discovery, unknown DIDs, record mutations, coverage delta. `labelwatch discover-stream` CLI, systemd unit, `labelwatch coverage-delta` and `db-optimize` commands.

## Schema history

| Version | What changed |
|---------|--------------|
| v1-v3 | Initial schema through early iterations |
| v4 | Evidence-based classification, labeler_evidence table, probe_history table |
| v5 | Derive module: regime_state, risk scores, temporal_coherence, derived_receipts table |
| v6 | Regime hysteresis: regime_pending, regime_pending_count columns |
| v7 | Score deltas: auditability_risk_prev, inference_risk_prev, temporal_coherence_prev |
| v8 | Warmup alert quarantine: warmup_alert column on alerts table |
| v16 | target_did column, derived_author_day + derived_author_labeler_day rollup tables |
| v17 | discovery_events table, indexes on labeler_did and discovered_at |

## Up next

**Near-term (adds signal):**
- ~~Per-rule activation budgets~~ (done — 10 alerts per rule/labeler per 24h, gate at insertion)
- ~~Handle-to-DID resolution in Climate lookup~~ (done — resolve client-side, pass handle to renderer)
- ~~Climate form layout bug~~ (done — Firefox flex shorthand fix)
- ~~Climate empty state + back link~~ (done — centered layout, try-other-window links, nav link)
- **Milestone: Boundary Instability (B.3 synthesis)** — cross-labeler disagreement, lead/lag, JSD divergence, churn deltas, assembled into BoundaryFightCard compound signal. Two phases: primitives first, synthesis second. See [`docs/MILESTONE_BOUNDARY_INSTABILITY.md`](docs/MILESTONE_BOUNDARY_INSTABILITY.md).

**UI polish (from audit):** all done
- ~~Tables responsive~~ (overflow-x:auto wrapper)
- ~~Sparkline a11y~~ (role="img" + aria-label)
- ~~Dark mode --accent~~ (#3498db)
- ~~DID contrast~~ (opacity 0.6 → 0.75)
- ~~Merge Applies/Removes card~~

**My Label Climate:**
- ~~Phase 1: schema v16, target_did, rollup tables~~
- ~~Phase 3: CLI + JSON + HTML generation~~
- ~~Phase 2: HTTP query layer, hardening~~
- Phase 4: share card template (screenshot-ready)
- Phase 5: proof-of-control for private detail views (Tier-1 hardening)
- **"What's on me?" view** — show account-level labels (DID-targeted, not post-targeted) by querying AppView `com.atproto.label.queryLabels` directly, since many small/novelty labelers aren't in the registry and their labels were never ingested. Different data source than climate (AppView vs local DB). Could be a separate endpoint or a section within climate.

**Seams / spec work:**
- Align receipt hash canonicalization with PCAR-D profile (sorted keys, no whitespace, ASCII)
- Draft effect taxonomy shared with driftwatch (detect / warn / flag)
- Receipt chain (`prev_receipt_hash`) for tamper-evident alert trail

**Later:**
- **Silence Adjudicator v0** — regime classifier for labeler silence. Not "is it quiet?" (timestamp comparison) but "why is it quiet?" (regime taxonomy: normal silence, burst gap, upstream PDS issue, labeler death, behavioral drift, firehose issue, local ingest issue, unresolved). Cadence sketches (rolling inter-event intervals → behavioral classes: metronomic/bursty/sparse/dormant), peer comparison by PDS/hosting, governor-constrained probes, receipt trail with ground truth hooks for retrospective learning. Sidecar process, own SQLite, read-only against labelwatch DB. Spec pinned at `docs/SILENCE_ADJUDICATOR_V0.md`. Build when you find yourself staring at timestamps wishing for a better answer.
- **Monitoring / alerting** — something lightweight for service health (labelwatch, labelwatch-discovery, labelwatch-api). Uptime checks, restart detection, disk/memory. TBD what tool — not Prometheus, not Zabbix. Maybe just a simple healthcheck script + webhook, or a hosted service.
- Cross-project receipt verification with driftwatch
- Casefile / annotation ledger for human review notes
- Caddy-level rate limiting for `/v1/climate/*`
- CSP headers (requires moving inline JS out of HTML templates)
