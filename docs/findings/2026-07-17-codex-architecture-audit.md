# Codex architecture audit — 2026-07-17

**Status:** captured, NOT triaged, NOT acted on. External audit (codex) of the
labelwatch codebase. No files were changed. Recorded here so the findings
survive; each item still needs local verification before any fix — treat as
**testimony** (a detector's output), not ratified defects. Line anchors are
codex's; verify they still point where claimed before acting.

**Why parked:** filed mid-flight during the driftwatch cold-path campaign
(see driftwatch `specs/gaps/gap-spec-facts-snapshot-scale-containment.md`).
Pick up when there's time to spend on it.

Codex's own summary: *"Not stupid. Yes — but it needs targeted refactors, not
a rewrite."* Verdict: leave `classify.py`, `derive.py`, `rules.py`, and most
of `cli.py` alone (comparatively cohesive).

---

## Fix first (correctness)

### 1. Discovery persistence is unsafe (thread-affinity + cursor-before-commit)
- One SQLite connection created in `discovery_stream.py:407`, then passed into
  another thread at `discovery_stream.py:402`. SQLite connections are
  thread-affine by default → the periodic backstop can fail.
- More seriously: the cursor is saved **before** queued discoveries are
  committed (`discovery_stream.py:295` vs `discovery_stream.py:345`) → risks
  skipped events after a crash.
- **Proposed:** each task gets its own read connection; route all writes
  through one writer; advance the cursor only after durable acknowledgement.

### 2. API timeouts don't stop the work
- Registry and climate requests launch daemon threads, return 503 on timeout,
  then release their semaphore while the SQLite query keeps running:
  `server.py:444`, `server.py:456`, `server.py:579`. Repeated slow requests
  can exceed `max_concurrent`.
- **Proposed:** bounded executor whose capacity stays occupied until work
  exits, plus SQLite deadline cancellation.

### 3. Migration: possible infinite loop + missing indexes
- `_backfill_target_did()` repeatedly selects null targets at `db.py:399`, but
  unparseable AT URIs stay null and are selected forever.
  **Proposed:** keyset-page by row ID regardless of whether parsing succeeds.
- Migration history affects the final schema: codex claims a verified v0→v23
  upgrade **misses four declared indexes**.
  **Proposed:** split the ~500-line migration chain at `db.py:539` into
  registered one-version steps; add a final schema-reconciliation test.
  *(This one is verifiable cheaply: diff a v0→v23 migrated schema against a
  fresh init_db — do that first to confirm before refactoring.)*

### 4. Frontdoor misses account-level labels
- Ingest only derives `target_did` from `at://` URIs at `ingest.py:185`, while
  frontdoor queries exclusively by `target_did` at `frontdoor.py:289`. Bare-DID
  account labels therefore disappear — although `whatsonme` finds them
  correctly (cross-check that asymmetry to confirm).
- **Proposed:** a canonical indexed `subject_did` covering both bare DIDs and
  record authors.

---

## Structural refactors worth doing (not correctness)

### 5. Break up report generation
- `generate_report()` is ~2,353 lines (`report.py:1694`–`4046`), complexity
  ~161. Mixes SQL, aggregation, HTML, error policy, filesystem publication.
  Other modules import its private helpers — a hidden import cycle.
- **Proposed extraction:** a dependency-free presentation module; a bulk
  `ReportSnapshot`; independent section/page renderers; a small atomic
  publisher.

### 6. Move derive orchestration out of scan.py
- `scan.py:1563` coordinates ~twelve lock-sensitive jobs. The transaction/yield
  behavior is good and should stay centralized, but individual rollups, facts
  sync, metrics, and boundary analysis should become explicit `DeriveJob`s.

---

## Housekeeping notes (codex)
- Substantial test suite; compile checks pass.
- 83 Ruff findings; CI explicitly ignores lint failure at
  `.github/workflows/test.yml:32`.
- One stale discovery assertion.

---

## Triage guidance (when picked up)
- **Verify before fixing.** These are codex's claims with codex's line numbers;
  labelwatch has moved since. Confirm each anchor + repro before touching code.
- **Cheapest confirmations first:** #3's missing-indexes claim (schema diff
  test) and #4's account-label asymmetry (frontdoor vs whatsonme on a bare-DID
  label) are both quick to prove/disprove and would validate the audit's rigor.
- Items #1 and #2 are the load-bearing correctness risks (data loss on crash;
  concurrency-limit escape). #5/#6 are quality, not urgency.
