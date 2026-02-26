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

## Schema history

| Version | What changed |
|---------|--------------|
| v1-v3 | Initial schema through early iterations |
| v4 | Evidence-based classification, labeler_evidence table, probe_history table |
| v5 | Derive module: regime_state, risk scores, temporal_coherence, derived_receipts table |
| v6 | Regime hysteresis: regime_pending, regime_pending_count columns |
| v7 | Score deltas: auditability_risk_prev, inference_risk_prev, temporal_coherence_prev |
| v8 | Warmup alert quarantine: warmup_alert column on alerts table |

## Up next

**Near-term (adds signal):**
- Per-rule activation budgets (cap alerts per window, quarantine on overshoot)
- Composition detectors: lead/lag, mirroring, rate synchrony across labelers
- Contradiction network mapping (labelers disagreeing on same subjects)

**Seams / spec work:**
- Align receipt hash canonicalization with PCAR-D profile (sorted keys, no whitespace, ASCII)
- Draft effect taxonomy shared with driftwatch (detect / warn / flag)
- Receipt chain (`prev_receipt_hash`) for tamper-evident alert trail

**Later:**
- Cross-project receipt verification with driftwatch
- Casefile / annotation ledger for human review notes
- Jetstream streaming (replace HTTP polling for real-time ingestion)
- Web inspection endpoint (`/health`, `/recent-alerts`)
