# Cold event archive experiment

Status: off-host prototype only. No production reader, retention policy, or
deletion path is enabled by this work.

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
