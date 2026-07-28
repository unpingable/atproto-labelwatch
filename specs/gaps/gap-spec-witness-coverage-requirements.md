# GAP: Witness Coverage Requirements — Labelwatch

> Status: candidate / requirements only. Filed 2026-07-28, alongside
> driftwatch's `specs/gaps/gap-spec-witness-coverage-requirements.md`
> (same layering rules; shared provenance in the 2026-07-23..27 driftwatch
> outage, which the then-current witness missed entirely despite nominally
> watching this host).
> Scope: what "full" external witness coverage of labelwatch means. This doc
> constrains any witness implementation (NQ-ng or otherwise) and names the
> signals this repo is obliged to export. It does not design the witness.

## Why this exists

Labelwatch is the *better*-covered sibling — NQ's one source lives on this
host and watches its services — and the driftwatch incident still proved the
coverage model wrong in ways that transfer directly:

- The host collector reports the **root filesystem only**. Labelwatch's
  database (~40 GB, 41M+ rows) now lives ON the root fs
  (`/var/lib/labelwatch/db/`, symlinked from `/var/lib/labelwatch/
  labelwatch.db`) — so root-fs runway is now a labelwatch-critical signal
  that nothing models as such, and `/mnt/zonestorage` (still used for some
  artifacts) is invisible.
- Service-up is witnessed; *observing* is not. Labelwatch has three failure
  scars — WAL pinned by a long-lived reader, report generation pinning WAL
  exactly like an external reader, derive backlog — in which every systemd
  unit stayed green while the observatory degraded.
- Labelwatch's own health verdict reports `DEGRADED` as a **baseline
  weather state** (ratified disposition 2026-06-15: DEGRADED is not
  actionable per se). A witness alerting on the verdict string would
  false-alarm constantly; a witness ignoring it would miss real transitions.
  Baseline semantics must be explicit.

## Layering rule

Identical to the driftwatch doc, and binding here:

- **H-x (host witnesses)** — no labelwatch knowledge required.
- **A-x (application / APM witnesses)** — meaningful only against
  labelwatch's claims about what it does: per-labeler ingest recency, derive
  lag, API serviceability, reference-set integrity.
- **C-x composites** may join families; every finding names its contributing
  signals. Separately attributable, end to end.

## H — Host-level requirements

| ID | Requirement | Provenance |
|----|-------------|------------|
| H-1 | **Root filesystem as a labelwatch-critical disk subject**: used %, free bytes, inode %, growth rate, and runway (free/growth). The DB relocation (2026-06-14) moved the primary data risk from zonestorage to `/`; the witness model never followed. `/mnt/zonestorage` remains a subject for residual artifacts. | DB-on-root-fs since 06-14; root fs at 78% as of 07-27. |
| H-2 | The three systemd units (`labelwatch`, `labelwatch-discovery`, `labelwatch-api`) as individual service subjects: active state, restart count, and unit-failure events. All three run as `labelwatch:labelwatch`; a unit flapping under systemd auto-restart must be visible, not absorbed. | Baseline; necessary, never sufficient. |
| H-3 | **DB path integrity**: the `/var/lib/labelwatch/labelwatch.db` symlink resolves, and the resolved target is on the expected filesystem with write permission for the service user. `ReadWritePaths` interactions made a *relocated-but-unwritable* DB a real, silent state — service green, writes failing. | lesson: systemd ReadWritePaths blocks DB relocation; verify writes, not service state. |
| H-4 | External reachability of the public surface: `https://labelwatch.neutral.zone/health` (through Caddy) distinct from loopback :8423. Caddy runs in a separate container with its own failure modes; loopback-green + edge-dead is a real outage. | Caddy is a shared proxy for 7 sites; config reloads are routine disturbances. |
| H-5 | Free-space semantics per effective writer uid (labelwatch runs as a non-root system user — the ext4 5% root reserve is NOT available to it; its effective free space is the non-root `Avail` number). | Driftwatch recovery discovery, inverted: here the writer is non-root, so the smaller number is the true one. |

## A — Application-level (APM) requirements

| ID | Requirement | Provenance |
|----|-------------|------------|
| A-1 | **Per-labeler ingest recency vs that labeler's own cadence baseline.** Labelwatch's subject population emits at wildly different rates; one global "events seen recently" hides a silent reference labeler for weeks (hailey.at: 0 events for 30d before anyone moved). Per-labeler `last_event_age` against per-labeler baseline, with the reference set (`moderation.bsky.app`, `skywatch.blue`, `label.haus`) held to tighter thresholds than the long tail. | hailey.at retirement; skywatch.blue degradation watch item. |
| A-2 | **Reference-set integrity as a distinct signal**: `reference_issues` (the field the ratified health disposition names as the actual tripwire) surfaced to the witness as a first-class reading, not buried in the composite verdict. | Health-check disposition 2026-06-15: "Don't dig unless reference_issues trips." |
| A-3 | **Verdict-transition semantics, not verdict-state alerting**: the climate/health verdict's baseline is DEGRADED-as-weather. The witness alerts on *transitions and dwell changes* (DEGRADED→CRITICAL; DEGRADED dwell beyond its historical envelope), never on the raw string. Baseline definition lives in versioned witness config. | Same disposition; X-4 in the driftwatch doc. |
| A-4 | **WAL telemetry on the labelwatch DB**: size, growth rate, checkpoint progress. The two scars — long-lived external reader pinning WAL, and report generation pinning it from *inside* — both present as WAL growth with green services. Recovery procedure exists (stop 3 services, TRUNCATE, restart); detection does not. | lesson_wal_bloat; lesson_report_gen_is_workload. |
| A-5 | **Derive pipeline lag**: age of newest `derived_author_day` / `derived_author_labeler_day` rows vs ingest high-water mark. Derive falling behind is epistemic staleness the API then serves as fresh climate. Chunked derive (UAD/UALD) landed with 0 drops; the witness keeps it honest. | derive chunking track; MY_LABEL_CLIMATE rollups. |
| A-6 | **Ingest-mode drift as declared state**: primary `queryLabels` ingest auto-disables on 401 (has been disabled since 2026-03-05, by design). Which ingest paths are active is witnessed config-state; a silent transition (multi-ingest losing a path) is a finding, while known-disabled paths are not. | 401 auto-disable behavior; same class as driftwatch A-9. |
| A-7 | **API serviceability beyond liveness**: :8423 answering `/v1/climate/...` within latency budget, rate-limiter not saturated, disk cache writable, kill-switch state as declared config. `/health` returning 200 while the kill switch is on is a truthful liveness lie. | server.py Tier-0 hardening surface. |
| A-8 | **Discovery sidecar epistemic liveness**: Jetstream cursor/stream advancing vs wall clock, not just unit-active — the exact driftwatch A-1 signal, applied to `labelwatch-discovery`. | Direct transfer of the 07-23 lesson. |
| A-9 | **DB growth + runway** at the application layer (rows/day per major table, page metrics), feeding H-1 runway with freelist-aware numbers a bare `df` cannot see. | Driftwatch A-7 transfer; 40 GB and growing on the root fs. |
| A-10 | No A-x signal sourced solely from a liveness endpoint. Admissible sources: the API's structured verdict fields (with A-3 semantics), DB-derived readings, service logs. | Driftwatch A-11 transfer. |

## C — Composite findings

| ID | Finding shape | What it names |
|----|---------------|---------------|
| C-1 | *Evidence loss*: units up ∧ (per-labeler recency collapsing broadly ∨ discovery cursor frozen) ∧ WAL or derive lag rising. | The labelwatch expression of live-but-not-observing. |
| C-2 | *Health contradiction*: `/health` ok ∧ any of A-4/A-5/A-8/H-3 in finding state. The contradiction is itself reportable. | Direct transfer of driftwatch C-2. |
| C-3 | *Runway closure* on the root fs: H-1/A-9 runway below operator response time. The DB shares that fs with the OS — closure here is a host incident, not just a labelwatch one. | DB-on-root-fs reality. |
| C-4 | *Reference blindness*: ≥2 of the 3 reference labelers silent beyond baseline simultaneously. Distinct from A-1 per-labeler silence — the reference set going dark together suggests our ingest, not their emission. | Co-presence reasoning inverted: correlated silence points at the observer. |

## X — Custody and anti-requirements

Identical to the driftwatch doc's X-1..X-5, plus one local sharpening:

- X-1..X-5: witness reads never writes; testimony not standing; declaration-
  aware (annotate, never suppress; overrun is a new signal); baselines
  versioned in config; graded coverage but conjunctive acceptance.
- X-6 **The witness loop may not mint labelwatch standing.** Findings about
  labelwatch's health must never feed back into labelwatch's published
  climate, tiers, or registry as inputs. (NQ↔Labelwatch integration ladder:
  the loop may improve testimony, may not mint standing.)

## Acceptance test

| Incident / scar | Must be caught by |
|-----------------|-------------------|
| WAL bloat via long-lived reader (recovery-only scar) | A-4, C-1 |
| Report generation pinning WAL from inside | A-4 (source-agnostic by design) |
| Relocated-DB-unwritable (ReadWritePaths class) | H-3, C-2 |
| hailey.at-style reference silence (weeks unnoticed) | A-1, A-2 |
| Hypothetical: discovery cursor frozen behind green units | A-8, C-1 |
| Root-fs exhaustion with DB resident (driftwatch 07-23 transferred) | H-1, C-3 |

Same lab-substrate rule as the driftwatch doc: synthesized incident state is
sufficient for acceptance and must be labeled lab-backed; live authority
accrues only from real deployment.

## Non-goals

- Not a design for NQ-ng.
- Not the exporter implementation (labelwatch-side typed signal work would be
  its own gap spec if the API's current fields prove insufficient).
- Not alert routing/severity policy.
- Not a revision of the DEGRADED-as-weather disposition — A-3 encodes it.
- No hot-path coupling: witness outages invisible to labelwatch.
