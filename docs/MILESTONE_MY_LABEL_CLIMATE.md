# Milestone: My Label Climate

**Goal:** make labeler activity *personal* — "what's being done to your posts?" — using existing labelwatch data + receipts, no content ingestion, self-only.

**Engagement model:** steal Judge/Wrapped stickiness (mirror + trendline + share card) but point it at governance infrastructure, not personality. Score *systems*, not users.

---

## Constraints

* No text / quotes / topic modeling
* No user trait inference or scalar "person score"
* No "posts you've seen" (requires client-side, different project)
* **Self-only by default** — no public DID lookup surface
* Build on existing derive/migration/report patterns
* No new dependencies

---

## Current schema context

`label_events` columns: `id, labeler_did, src, uri, cid, val, neg, exp, sig, ts, event_hash`

* `uri` = target AT URI (e.g., `at://did:plc:xyz/app.bsky.feed.post/abc`)
* `neg` = 0 for apply, 1 for remove/negation
* `ts` = ISO 8601 timestamp (text)
* Indexes: `(labeler_did, ts)`, `(uri, ts)`

Existing rollup pattern: `derived_val_dist_day` — recomputes last 7 days, prunes at 60 days, uses `(CAST(strftime('%s', le.ts) AS INTEGER) / 86400) * 86400` for day bucketing.

Schema version: 15.

---

## Phase 1 — Schema v16: Author pivot column + index

### Change

Add `target_did` to `label_events`. One column, one index — that's the only structural move.

### DDL (migration v15 → v16)

```sql
ALTER TABLE label_events ADD COLUMN target_did TEXT;

CREATE INDEX IF NOT EXISTS idx_label_events_target_did_ts
ON label_events(target_did, ts);
```

Skip `target_collection`, `target_kind`, `target_rkey` for v1. Filter posts with the existing `uri LIKE 'at://%/app.bsky.feed.post/%'` pattern.

### Backfill

Python backfill in the migration path (runs once):

```python
def _parse_target_did(uri: str) -> str | None:
    """Extract DID from AT URI: at://<did>/<collection>/<rkey>"""
    if not uri or not uri.startswith("at://"):
        return None
    parts = uri[5:].split("/", 2)  # strip "at://"
    if len(parts) < 1:
        return None
    did = parts[0]
    if not (did.startswith("did:plc:") or did.startswith("did:web:")):
        return None
    return did
```

Batch update in chunks of 10,000:

```python
while True:
    rows = conn.execute(
        "SELECT rowid, uri FROM label_events "
        "WHERE target_did IS NULL AND uri LIKE 'at://%' "
        "LIMIT 10000"
    ).fetchall()
    if not rows:
        break
    updates = [(parse_target_did(uri), rowid) for rowid, uri in rows]
    conn.executemany(
        "UPDATE label_events SET target_did = ? WHERE rowid = ?",
        updates,
    )
    conn.commit()
```

Also: set `target_did` on insert in `ingest.py` so new events arrive with the column populated.

### Acceptance criteria

* `target_did` filled for >99% of rows matching post URI pattern
* `EXPLAIN QUERY PLAN` for `WHERE target_did = ? AND ts >= ?` uses new index
* Migration is idempotent (re-running on already-migrated DB is a no-op)
* Unit test for DID parser (covers `did:plc:`, `did:web:`, malformed, non-AT URIs, missing rkey)

---

## Phase 2 — Derived rollups: author_day + author_labeler_day

### Why

30d climate queries should be O(days) not O(events). Same rationale as `derived_val_dist_day`.

### Tables (created in migration v16, alongside the column)

```sql
CREATE TABLE IF NOT EXISTS derived_author_day (
    author_did   TEXT NOT NULL,
    day_epoch    INTEGER NOT NULL,
    events       INTEGER NOT NULL DEFAULT 0,
    applies      INTEGER NOT NULL DEFAULT 0,
    removes      INTEGER NOT NULL DEFAULT 0,
    labelers     INTEGER NOT NULL DEFAULT 0,
    targets      INTEGER NOT NULL DEFAULT 0,
    vals         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (author_did, day_epoch)
);

CREATE TABLE IF NOT EXISTS derived_author_labeler_day (
    author_did   TEXT NOT NULL,
    day_epoch    INTEGER NOT NULL,
    labeler_did  TEXT NOT NULL,
    events       INTEGER NOT NULL DEFAULT 0,
    applies      INTEGER NOT NULL DEFAULT 0,
    removes      INTEGER NOT NULL DEFAULT 0,
    targets      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (author_did, day_epoch, labeler_did)
);
```

Index for "who's most active on this author" queries:

```sql
CREATE INDEX IF NOT EXISTS idx_derived_author_labeler_day_author
ON derived_author_labeler_day(author_did, day_epoch);
```

### Derive function

Follow the exact `_update_val_dist_day` pattern:

```python
def _update_author_day(conn) -> None:
    """Incrementally update derived_author_day from label_events.

    Recomputes last 7 days. Prunes rows older than 60 days.
    """
    now_epoch = int(time.time())
    start_day_epoch = ((now_epoch // 86400) - 6) * 86400
    cutoff_iso = time.strftime("%Y-%m-%dT00:00:00.000000Z",
                               time.gmtime(start_day_epoch))
    retention_cutoff = ((now_epoch // 86400) - 60) * 86400

    # Delete recompute window
    conn.execute(
        "DELETE FROM derived_author_day WHERE day_epoch >= ?",
        (start_day_epoch,),
    )

    # Reinsert from label_events
    conn.execute("""
        INSERT OR REPLACE INTO derived_author_day
            (author_did, day_epoch, events, applies, removes,
             labelers, targets, vals)
        SELECT
            le.target_did AS author_did,
            (CAST(strftime('%s', le.ts) AS INTEGER) / 86400) * 86400
                AS day_epoch,
            COUNT(*)                                    AS events,
            SUM(CASE WHEN le.neg = 0 THEN 1 ELSE 0 END) AS applies,
            SUM(CASE WHEN le.neg = 1 THEN 1 ELSE 0 END) AS removes,
            COUNT(DISTINCT le.labeler_did)              AS labelers,
            COUNT(DISTINCT le.uri)                      AS targets,
            COUNT(DISTINCT le.val)                      AS vals
        FROM label_events le
        WHERE le.target_did IS NOT NULL
          AND le.uri LIKE 'at://%/app.bsky.feed.post/%'
          AND le.ts >= :cutoff_iso
        GROUP BY le.target_did, day_epoch
    """, {"cutoff_iso": cutoff_iso})

    # Prune old rows
    conn.execute(
        "DELETE FROM derived_author_day WHERE day_epoch < ?",
        (retention_cutoff,),
    )
```

And the same pattern for `derived_author_labeler_day`.

Wire into the existing scan loop alongside `_update_val_dist_day`.

### Acceptance criteria

* Rollups populate for all authors touched in the last 7 days
* 30d climate summary query uses rollups only (sum over ≤30 rows per author)
* Derive time stays reasonable (benchmark on real DB)

---

## Phase 3 — CLI: `labelwatch climate`

### Command

```
labelwatch climate --did <did> [--window 30] [--out <dir>] [--json]
```

Outputs:
* `climate.json` — all computed metrics
* `climate.html` — rendered page (standalone, no server needed)
* optionally `climate_card.html` — screenshot-ready share card

### What it computes (from rollups)

**Summary stats (window, default 30d):**
* Total events, applies, removes
* Distinct labelers
* Distinct target URIs touched
* Distinct label values applied

**Top labelers touching you (top 10):**

```sql
SELECT labeler_did,
       SUM(events) AS events,
       SUM(applies) AS applies,
       SUM(removes) AS removes,
       SUM(targets) AS targets
FROM derived_author_labeler_day
WHERE author_did = ?
  AND day_epoch >= ?
GROUP BY labeler_did
ORDER BY events DESC
LIMIT 10;
```

Cross-reference against `labelers` table for handle, regime_state, badges.

**Top label values (top 10):**

Direct query against `label_events` (bounded by index):

```sql
SELECT val, COUNT(*) AS n,
       SUM(CASE WHEN neg = 0 THEN 1 ELSE 0 END) AS applies,
       SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END) AS removes
FROM label_events
WHERE target_did = ? AND ts >= ?
  AND uri LIKE 'at://%/app.bsky.feed.post/%'
GROUP BY val
ORDER BY n DESC
LIMIT 10;
```

**Daily time series (for chart):**

```sql
SELECT day_epoch, events, applies, removes, labelers
FROM derived_author_day
WHERE author_did = ? AND day_epoch >= ?
ORDER BY day_epoch;
```

**"Show me the receipts" — top 3 most recent events per top labeler:**

```sql
SELECT le.labeler_did, le.uri, le.val, le.neg, le.ts
FROM label_events le
WHERE le.target_did = ? AND le.ts >= ?
  AND le.labeler_did IN (... top labeler DIDs ...)
ORDER BY le.ts DESC
LIMIT 15;
```

Bounded, deterministic, receipt-grade.

### Output: climate.json

```json
{
    "author_did": "did:plc:xyz",
    "window_days": 30,
    "generated_at": "2026-03-05T12:00:00Z",
    "summary": {
        "events": 142,
        "applies": 130,
        "removes": 12,
        "labelers": 6,
        "targets": 89,
        "values": 14
    },
    "top_labelers": [
        {
            "labeler_did": "did:plc:...",
            "handle": "mod.bsky.social",
            "regime_state": "stable",
            "events": 45,
            "applies": 42,
            "removes": 3,
            "targets": 30
        }
    ],
    "top_values": [
        {"val": "porn", "applies": 50, "removes": 2},
        {"val": "spam", "applies": 30, "removes": 5}
    ],
    "daily_series": [
        {"day": "2026-02-03", "events": 5, "applies": 4, "removes": 1, "labelers": 2}
    ],
    "recent_receipts": [
        {"labeler_did": "...", "uri": "at://...", "val": "spam", "neg": 0, "ts": "..."}
    ]
}
```

### Acceptance criteria

* Running the CLI against local DB produces a complete JSON artifact
* Output is deterministic given DB state + window
* Handles "no data for this DID" gracefully (empty climate, not crash)

---

## Phase 4 — Share card template

One screenshot-ready HTML card that reads instantly.

### Card content

```
┌─────────────────────────────────────────┐
│  My Label Climate (30d)                 │
│  did:plc:xyz...                         │
│                                         │
│  6 labelers   142 events   12 reversals │
│                                         │
│  Top value: spam (30)                   │
│  Most active: mod.bsky.social (45)      │
│  ▁▂▃▅▃▂▁▂▅▇▅▃▂▁  ← 30d sparkline      │
│                                         │
│  Receipted. No content ingestion.       │
│  labelwatch • 2026-03-05                │
└─────────────────────────────────────────┘
```

### Implementation

HTML template rendered from `climate.json`. Sparkline via inline SVG (no JS dependencies). Same static generation pattern as existing report pages.

### Acceptance criteria

* Single self-contained HTML file (inline CSS, inline SVG)
* Looks good as a screenshot (fixed width, high contrast)
* No PII beyond the DID the user chose to query

---

## Phase 5 — Optional self-serve (pick one later)

Three options, increasing complexity:

1. **Allowlist static generation** — generate climate pages only for configured DIDs. Simplest. Run `labelwatch climate --did X` in a cron and serve the output.

2. **"Wrapped" export** — CLI generates a self-contained bundle the user hosts themselves. Medium. Already mostly built by Phases 3-4.

3. **Query endpoint** — authenticated DID → returns `climate.json`. Biggest lift. Requires auth layer. Only if demand warrants.

---

## Non-goals (explicit)

* Public "lookup any DID" endpoint
* Follow graph import (Phase 6 someday)
* Thread participation exposure
* Anything content-based
* Any scalar "score" of a person
* Browser extension / client integration

---

## Privacy guardrails

* **Self-only default**: CLI requires explicit `--did` flag; no batch generation of all authors
* **Share cards show your own climate only**: no "look up someone else" flow
* **Receipt links are to AT URIs**: public data, but we don't amplify or aggregate beyond the author's own posts
* **No DID → handle resolution in stored data**: handles are for display only, from the labelers table (which tracks labeler handles, not target handles)

---

## Tests

### Unit tests
1. **DID parser**: `did:plc:`, `did:web:`, malformed URIs, non-AT URIs, edge cases
2. **Rollup correctness**: synthetic fixture → verify counts match raw query
3. **Climate JSON**: verify all required fields present, types correct
4. **Empty climate**: DID with no label events → graceful empty response

### Integration tests
1. **Migration idempotency**: run v15→v16 twice, verify no errors
2. **Backfill**: insert events with known URIs, run backfill, verify `target_did` populated
3. **Index usage**: `EXPLAIN QUERY PLAN` for climate queries → verify index scan not table scan

---

## Build order

1. Schema v16 + backfill + index + ingest.py change
2. Rollup tables + derive functions (wire into scan loop)
3. CLI `labelwatch climate` + climate.json output
4. Share card HTML template
5. Optional self-serve (later)

---

## Schema history (updated)

| Version | What changed |
|---------|--------------|
| v15 | derived_val_dist_day, derived_labeler_entropy_7d |
| v16 | Author pivot: target_did column, derived_author_day, derived_author_labeler_day |
