# Cold event archive experiment

Status: production-reader candidate qualified off-host. No production reader,
retention policy, or deletion path is enabled by this work.

`tools/label_events_cold_archive.py` exports one closed UTC day of
`label_events` into a content-bound Parquet partition. It preserves every
event column, signature, stable event ID/hash, and an ordered-row digest. Its
reconstruction command starts from an empty Labelwatch database and verifies
the reconstructed event identity.

`tools/label_events_frontdoor_archive.py` derives one immutable SQLite summary
from a verified partition. It preserves the four historical aggregate shapes
used by the public frontdoor:

- per-subject, labeler, and label-value counts plus first/last observation;
- distinct `(val, neg)` state sets;
- attachment-locus counts; and
- per-URI/value counts plus first/last observation, with top-URI ranking done
  only after all partitions are merged.

Catalog v3 carries the v2 exact per-labeler event counts and first/last times
plus the frozen source's maximum event ID. The scan and rule paths use those values only
for lifetime warmup/confidence semantics; their active 30-day window must be
entirely newer than the catalog boundary. The ID watermark prevents a new,
late-arriving event with an old authored timestamp from being mistaken for an
archived duplicate. After every day is merged, the
catalog retains only the globally ranked top 50 record URIs per
subject/labeler—the existing public output bound—and compacts away the
build-scale URI intermediates. Partition Parquet, not the catalog, remains
the complete reconstruction source.

The summary contains DIDs and AT URIs. It is private retained evidence, not a
publishable artifact. Production use requires an explicit custody location,
privacy review, bounded partition inventory, and a reader that combines cold
summaries with the live database without treating an unavailable archive as
an empty history.

The experiment deliberately does not solve online deletion. Before any
production row can be removed, qualification must bind the finalized archive
and summary to the exact delete set, preserve the current ingestion cursor,
prove all-history frontdoor equivalence, and define refusal behavior when cold
custody is missing or invalid. Recent-window scan/report paths and the
all-history frontdoor are separate compatibility classes.

## Reader candidate

`labelwatch.frontdoor_archive` loads a versioned catalog manifest and its
immutable SQLite summary at process startup. The configured catalog must be a
complete, contiguous interval of closed UTC days; source summaries must cover
that interval exactly. The loader verifies the catalog's logical size,
SHA-256, schema, embedded coverage metadata, and ownership watermark, then
reports the selected local copy's observed allocation for capacity accounting.
Allocation geometry is not content identity and may change on restore. It
refuses network-backed
serving filesystems (including NFS): archive custody may live remotely, but a
serving catalog must first be copied and verified onto the mounted Labelwatch
volume.

When `LABELWATCH_FRONTDOOR_COLD_CATALOG` is set, the public lookup merges the
verified catalog with the remaining live `label_events` working set. The
catalog owns its exact closed interval even if those rows still physically
overlap the live database during reader qualification; every live-side query
excludes a row as an archived duplicate only when both its timestamp is in the
catalog interval and its ID is at or below the frozen source watermark. This
preserves genuinely new late observations while preventing double counts.
Missing,
invalid, incomplete, or remotely mounted configured catalogs fail startup;
they never become an empty historical contribution. Health reports only the
selected coverage interval, day count, and local allocation. Requests open no
archive or NFS files.

The 2026-09-13 production-scale qualification used the retained source
database identified by
`f9f2ee2a34c3dcf3c0d7999c4d67381726aae4845d4a07980926c97748d216a8`.
It covered the two complete days 2026-09-01 and 2026-09-02 (853,360 source
events) with a 155,275,264-byte local catalog. The 50 densest archived
subjects matched raw all-history aggregates, and 50 frontdoor-admissible
subjects matched the production `lookup_subject` result. This qualifies the
reader shape and bounded serving footprint; it does not authorize production
row removal or establish filesystem reclamation without a separately
qualified database rebuild/cutover.

That two-day result remains the qualified v1 prototype at its recorded
revision and receipt. The complete bridge-exit experiment uses catalog v3 and
must independently qualify its larger interval; the older result is not
silently reinterpreted as v3 evidence.
