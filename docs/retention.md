# Retention floor, cold catalog and trim

Status: implemented on `phase2-cold-history-20261002`, not deployed. The
deployed release (795bc44) has the floor and the `cold_history: unavailable`
refusal only.

## The invariant

Absence is never inferred from retention scope. When an answer claims
coverage that reaches before what Labelwatch can count, it says so
(`cold_history: {status: "unavailable"}`, refusal `cold_history_unavailable`)
rather than returning a zero, an empty list, or "no labels observed".

## The floor

The live SQLite database holds `label_events` rows with `ts >=` the retention
floor, `meta['retention:live_floor']` (an ISO-8601 UTC midnight, for example
`2026-08-14T00:00:00Z`). `LABELWATCH_RETENTION_FLOOR` overrides the meta value
and an invalid value fails closed. `meta['retention:archive_ref']` names where
the rows below the floor went.

Every comparison against the floor is a string comparison on the stored `ts`,
the same order the trim, the export, and the catalog's ownership predicate
use. For ISO-8601 timestamps with a `T` separator, string order and time order
agree. A timestamp such as `2026-08-21 00:00:01Z` (space separator) sorts
below `2026-08-21T00:00:00Z`. The catalog extender refuses partitions that
contain one (see below).

`meta['retention:history_start']` is the earliest instant Labelwatch claims
history for. The operator sets it by hand after confirming the earliest
archived `ts`. For generation 1 that is expected to be
`2026-02-24T00:00:00Z`: the v3 catalog covers [2026-02-24, 2026-08-14), and
total − working − cold = 0 rows. Confirm before setting it. If the key is
absent, coverage is reported as `gap`.

## Coverage rule (`retention.catalog_coverage`)

The cold catalog is configured with `LABELWATCH_FRONTDOOR_COLD_CATALOG` (path
to its manifest). The server, the runner and `labelwatch scan` verify it once
at startup: manifest shape, closed-day interval, logical size, full SHA-256,
`quick_check`, embedded metadata, and a local filesystem (NFS, CIFS and sshfs
are refused). A failed load does not stop the process; it is recorded and the
refusal stays in place.

Coverage is re-evaluated on every request and every scan cycle:

| status | when |
|---|---|
| `complete` | catalog `end_day_exclusive` + `T00:00:00Z` == live floor, catalog `start_day` <= `retention:history_start`, and the verified file is unchanged (dev, inode, size, mtime) |
| `gap` | `retention:history_start` is absent or invalid, or precedes the catalog start; or (frontdoor only) the subject has quarantined events |
| `stale` | the catalog ends before or after the floor, or there is no floor |
| `absent` | no catalog configured, manifest missing, or the catalog is on a network filesystem |
| `tampered` | verification failed at load, or the file changed after verification |

Only `complete` authorizes catalog aggregates:

- **Frontdoor** (`lookup_subject`) merges catalog aggregates with live rows
  when coverage is complete and returns
  `cold_history: {status: "catalog", ...}`. Otherwise it serves live rows and
  keeps the `cold_history_unavailable` refusal and banner.
- **Climate** and **registry** keep the refusal regardless of coverage. The v3
  catalog has no per-day counts and no `!hide` aggregates (`neg = 0`, distinct
  URI), so it cannot complete those answers.
- **Scan/derive lifetime totals** (`rules._build_event_count_cache`,
  `scan._fetch_event_stats`) add the catalog's `labeler_totals` only when
  coverage is complete. `_fetch_event_stats` raises if the catalog overlaps
  the 30-day window, so the floor must stay at least 31 days back.
  Production runs with `LABELWATCH_DERIVE_DISABLE=1`. Keep it that way until
  that path has been qualified against generation 1.
- **`/health`** has one `retention` block: `live_floor`, `archive`,
  `history_start`, `catalog` (range, watermark, sha256), `coverage`,
  `reason`, and `consistent`. `consistent` is true when coverage is complete,
  or when there is neither a floor nor a catalog.

## Ingest below the floor (quarantine)

`event_hash` deduplication only sees live rows. Once a row has been archived
and trimmed, a labeler cursor reset would re-deliver it as new, and the
catalog would count it twice. Ingest therefore writes any incoming event with
`ts < floor` to `quarantined_events` (reason `below_retention_floor`) and
never to `label_events`. A repeated delivery increments `seen_count`.
`labelwatch ops-status` reports the counts under
`extensions["labelwatch.retention"]`.

The switch is `below_floor_policy` in the config, overridden by
`LABELWATCH_BELOW_FLOOR_POLICY` (`quarantine`, the default, or `insert`). It
has no effect without a floor.

Quarantine is conservative. It also holds genuinely new events whose authored
`ts` is old, so neither side counts them. To keep that from becoming an
inferred absence, the frontdoor does not claim complete history (status
`gap`, reason `quarantined_events_for_subject`) for any subject with
quarantined rows. Reconciling quarantined events into the catalog is not
implemented.

`quarantined_events` is created by `init_db` without a schema-version bump.
Code from before this change ignores the extra table, so a rollback still
opens the database.

## Weekly trim procedure

Floors move only at UTC midnight. The order is: export, then extend and
install the catalog, then move the floor and delete. A reader therefore never
sees a hole, and nothing is counted twice.

1. **Export** on the host (read-only):
   `tools/label_events_trim_export.py --db <live.db> --floor F_new --out-dir <dir> --verify`.
   The tool opens one read transaction and records `W' = MAX(id)`. It writes
   rows with `ts < F_new AND id <= W'` to a plain SQLite partition, along with
   a manifest holding the floor, the previous floor, the watermark, the id
   range, the row count, per-day counts, a canonical row digest and the file's
   SHA-256. The long read transaction holds WAL checkpoints back while it
   runs. Copy the partition to the NFS archive first.
2. **Extend** off host (crow):
   `tools/label_events_catalog_extend.py --base-manifest <installed manifest> --summaries-dir <per-day summaries> --partition-manifest <M> --out-dir <dir>`.
   The tool verifies the partition and requires it to start exactly where the
   catalog ends. It builds one summary per new day and does a full re-merge of
   every per-day summary into a new catalog with watermark `W'`, then verifies
   the result. A full re-merge is required: top-50 URI pruning is only exact
   when counts are merged across all days before pruning.
3. **Install** the catalog on root, local disk, next to its manifest. Point
   `LABELWATCH_FRONTDOOR_COLD_CATALOG` at the manifest and restart the API
   and the runner, which hash the catalog in full at startup. Coverage now
   reads `stale` (catalog ahead of the floor), and the refusal is still
   served.
4. **Trim**:
   `labelwatch retention-trim --floor F_new --manifest M [--dry-run]`.
   The command refuses, changing nothing, unless all of the following hold:
   - the partition verifies and its floor is `F_new`;
   - `F_new` is a UTC midnight, later than the current floor, and at least
     `--min-age-days` (default 31) before now;
   - `LABELWATCH_RETENTION_FLOOR` is not set;
   - the partition was exported under the current floor and has no rows below
     it;
   - the live count, minimum id and maximum id of `ts < F_new AND id <= W'`
     equal the manifest;
   - if a catalog is configured, it loaded, it ends exactly at `F_new`, its
     watermark equals `W'`, and it reaches `retention:history_start`.

   If every check passes, the command marks the trim pending and moves the
   floor and `archive_ref` in a single commit. It then deletes in id-range
   batches (`--batch-ids`, default 50,000) with
   `PRAGMA wal_checkpoint(PASSIVE)` between batches (`--checkpoint` selects
   the mode) and never runs VACUUM. Rows that arrived after the export with
   `ts < F_new` (ids above `W'`) are moved into quarantine. Finally it
   asserts that no row below `F_new` remains. An interrupted run leaves
   `retention:trim_pending` in meta; rerunning with the same manifest
   resumes. The receipt is stored in `retention:last_trim`.

Without a catalog, the trim still runs. Pre-floor history is then reported as
unavailable, never as absent.

## Operational limits

- The first trim (planned for 2026-10-08) is deferred until this tooling is
  deployed and qualified. Root grows about 1.25 GB/day, which leaves a few
  weeks. The trim frees pages inside the file but does not shrink it.
- The catalog is about 6–8 GB on root and grows 0.3–0.5 GB/week. A swap
  needs twice that while both copies exist. Startup hashes the whole catalog.
- The v3 catalog's lineage (source sha `f9f2…`) differs from generation 1.
  Rebuild from generation 1 or record the mix before installing it. Trim
  partitions record their canonical row digest in `source_database_sha256`,
  because the live database is not hashed.
- Late events below the floor (quarantined) are not served. They are visible
  in ops-status.

## TODO: catalog v4

The extender writes v3. That is enough for the frontdoor but not for
climate, registry, or lifetime-collector work. A v4 catalog needs these
decisions, taken from the phase-2 prep:

- **Per-day segments in the catalog.** Keep the per-day summaries as the
  unit of custody, either embedded as day-keyed tables or as sibling files
  bound by the manifest. Per-day watermarks (`segment.watermark`) would let a
  late row be reconciled into its original day without regenerating other
  days, and would allow per-day ownership in place of one global watermark.
- **New tables for remaining consumers:**
  - `hide_uri_days`: labeler, uri, day, count, `neg = 0` only, so registry
    `!hide` totals and distinct-URI counts are exact.
  - `account_label_events`: bounded exact events per (target, labeler, val),
    so climate account-label state and `whatsonme` can be reproduced.
  - per-day `label_values` and `loci`: climate window counts.
- **Exactness markers.** Store the Nth URI count and the pruned count per
  (subject, labeler). A reader can then mark a merged answer inexact, or
  refuse it, when a dense subject active across the floor could reorder the
  top 50.
- **Climate's 60-day windows.** This is an owner decision: either a 61-day
  live window or a partial marking on the 60-day view.
- **Lineage.** Record each segment's source (generation-1 database sha or
  trim-partition row digest) explicitly, not under
  `source_database_sha256`.
