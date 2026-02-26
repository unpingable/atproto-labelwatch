# Next

Incremental work, in order. Core loop and receipts are done.

1. **Cursor persistence** — save/restore polling cursor across restarts. Small DB table, wire into ingest.

2. **Richer metrics** — target concentration, novel-target ratio, churn index, silence-then-surge. All SQL over label_events + new rules following existing pattern.

3. **Report UI pass** — health cards, sparklines, behavioral badges, anomaly highlighting. Static HTML, small JS charting lib. SRE dashboard feel, not accusation UI. Describe geometry, not intent.

4. **Deploy** — systemd unit or docker-compose entry on Linode, NGINX vhost for static reports, resource limits. One box, one SQLite, one generated site.

## Seams / Spec Work

Patterns from PCAR (agent_gov), adapted for labeler-watching.
Full analysis: driftwatch repo `docs/ATPROTO_SEAMS.md`.

**Near-term:**
- Align receipt hash canonicalization with PCAR-D profile (sorted keys, no whitespace, ASCII)
- Add per-rule activation budgets (cap alerts per window, quarantine on overshoot)
- Draft effect taxonomy shared with driftwatch (detect / warn / flag)

**Later:**
- Receipt chain (`prev_receipt_hash`) for tamper-evident alert trail
- Regime state machine (QUIET / ACTIVE / SURGE / ALARM) with hysteresis + cooldown
- Composition detectors: lead/lag, mirroring, rate synchrony across labelers
- Contradiction network mapping (labelers disagreeing on same subjects)
- Cross-project receipt verification with driftwatch
- Casefile / annotation ledger for human review notes
