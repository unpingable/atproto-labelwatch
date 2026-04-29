# Hosting Locus — Data Contract & Implementation Plan

## Overview

Surface PDS hosting distribution among labeled targets. Not "how many PDSes
exist" — "which hosts appear in the label surface, and how much?"

## Data flow

```
Driftwatch (actor_identity_current)
  → facts.sqlite (actor_identity_facts projection)
    → Labelwatch ATTACHes read-only
      → provider_registry classification
        → materialized daily rollups
          → report card / page
```

## 1) Facts bridge export: `actor_identity_facts`

Thin resolved-host view. No resolver internals.

```sql
CREATE TABLE actor_identity_facts (
    did TEXT PRIMARY KEY,
    handle TEXT,
    pds_endpoint TEXT,
    pds_host TEXT,
    resolver_status TEXT,
    resolver_last_success_at TEXT,
    is_active INTEGER
);
```

Populated by driftwatch facts export from `actor_identity_current`.

## 2) Provider registry (labelwatch)

```sql
CREATE TABLE provider_registry (
    host_pattern TEXT PRIMARY KEY,
    match_type TEXT NOT NULL,        -- 'exact' | 'suffix'
    provider_group TEXT NOT NULL,    -- bluesky | known_alt | one_off | unknown
    provider_label TEXT NOT NULL,    -- human-readable
    is_major_provider INTEGER NOT NULL DEFAULT 0
);
```

Seed data:

```sql
INSERT INTO provider_registry VALUES
('host.bsky.network', 'suffix', 'bluesky', 'Bluesky-hosted', 1),
('bsky.social',       'exact',  'bluesky', 'Bluesky-hosted', 1),
('blacksky.app',      'suffix', 'known_alt', 'Blacksky', 1);
```

### Fallback logic

- Registry match → use mapping
- No match + `resolver_status != 'ok'` or NULL host → `unknown` / "Unresolved/Unknown"
- No match + resolved + low volume (<10 targets or <5 accounts) → `one_off`
- No match + resolved + not-low → `unknown`

Volume threshold applied at rollup time, not per-row.

## 3) Materialized rollup: `labeled_targets_by_host_daily`

```sql
CREATE TABLE labeled_targets_by_host_daily (
    day TEXT NOT NULL,
    pds_host TEXT,
    provider_group TEXT NOT NULL,
    provider_label TEXT NOT NULL,
    is_major_provider INTEGER NOT NULL DEFAULT 0,
    labeled_target_count INTEGER NOT NULL,
    unique_accounts INTEGER NOT NULL,
    unique_labelers INTEGER NOT NULL,
    resolved_target_count INTEGER NOT NULL,
    unresolved_target_count INTEGER NOT NULL,
    coverage_resolved_pct REAL NOT NULL,
    PRIMARY KEY (day, pds_host, provider_group, provider_label)
);
```

Populated by report job. Join: `label_events.target_did → actor_identity_facts.did`.

## 4) Materialized rollup: `labeler_host_daily`

```sql
CREATE TABLE labeler_host_daily (
    day TEXT NOT NULL,
    labeler_did TEXT NOT NULL,
    pds_host TEXT,
    provider_group TEXT NOT NULL,
    provider_label TEXT NOT NULL,
    is_major_provider INTEGER NOT NULL DEFAULT 0,
    labeled_target_count INTEGER NOT NULL,
    unique_accounts INTEGER NOT NULL,
    share_of_labeler_targets REAL NOT NULL,
    PRIMARY KEY (day, labeler_did, pds_host, provider_group, provider_label)
);
```

Per-labeler host distribution. Where the interesting stories live.

## 5) First inspection queries (post-bake)

### Overall concentration
- Top-1 share, top-5 share, long-tail share excluding majors

### Resolved coverage
- % of labeled targets with `resolver_status = 'ok'`

### Non-major long tail
- Remove/fold Bluesky majors, see what remains

### Per-labeler host skew
- Reference labelers (skywatch, hailey, etc) vs overall distribution

## 6) UI: card first, page only if earned

### Hosting locus card (report section)

Five lines + small table:
- Resolved coverage %
- Top provider group + share
- Top non-major host + target count
- Non-major host share of resolved targets
- Freshness (resolver data last refreshed)

Small table: non-major hosts only, ranked by target count.

### Dedicated page (only if card proves there's a story)

Options:
- Long-tail ranking view (non-major hosts, counts, share, accounts, freshness)
- Treemap with major toggle

NOT a network graph.

## 7) Guardrails

- Always show resolved coverage (missingness visible)
- Always show freshness
- Don't imply host = operator intent
- Don't overread tiny counts on one-off hosts
- Don't let unresolveds disappear silently

## Implementation sequence

1. Extend facts export with `actor_identity_facts`
2. Add `provider_registry` + seed data in labelwatch
3. Build `labeled_targets_by_host_daily` rollup
4. Run raw queries, inspect the shape
5. Add hosting locus card to report
6. Build `labeler_host_daily` only if overall rollup shows signal
7. Dedicated page only if card proves there's a story
