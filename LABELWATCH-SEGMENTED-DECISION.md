# Labelwatch segmented-storage decision

**NO_DECISION — SPECIFIC EVIDENCE MISSING**

The 12M-event candidate earns substantial isolated correctness and performance
evidence. It does not yet earn adoption. There is no demonstrated engine property
that justifies reopening PostgreSQL or another storage-engine comparison.

| Adoption gate | Disposition | Evidence / remaining boundary |
|---|---|---|
| State/cursor continuity | Established within isolated adapter | Stable global owner,19 tables/412 meta-key names, four cursor sources, journal/ID recovery and independent process cuts. Full production collector integration remains future implementation. |
| Archive-only dedupe | Finite semantics pass; capacity incomplete | Hot live-floor hashes; below-floor quarantine, no synchronous DuckDB.47-day keys and future-timestamp/global-state envelope not qualified. |
| Schema evolution | Finite contract passes | Explicit23–26reader window, actual SQLite changes, NULL/Unicode vectors, older/unknown reader refusal. No arbitrary/perpetual schemas. |
| Real query behavior | One current-scale week passes; full horizon missing | Actual frontdoor/report helpers on12M + hot, exact SQL parity, existing dense refusal.90-day 180k sample proves range semantics only. |
| Current-scale specimen | Pass |12,000,000 accepted events with60,000 separate replay offers; measured30k sample shapes, seven live indexes, disclosed synthetic timing/replay controls. |
| Local capacity bounded and operable | Not established for production horizon | File/page/queue/WAL/spill caps prove finite refusal; they do not prove40+7days of complete global state or historical catalog fits. |
| Failure/restart no silent loss | Finite successor passes | Original silent-history omission preserved; anchored expected coverage fixes it. Independent replay/cursor/retirement negatives pass. No physical power-loss claim. |
| Independent qualification | Finite claims accepted; adoption withheld | Separate reviewer reproduces original failure, falsifies corrected successor and names three remaining evidence gaps. |

## Exactly missing evidence, and why

1. **Complete admitted global-state envelope over the existing40-day horizon plus
   seven-day catch-up.** The 12M week contains3.221 GB of hash/global state with tiny
   seeded ordinary tables. Projected47-day keys alone require21.63 GB at specimen
   density. Bounded live observations establish only a2.927 GB lower bound on
   non-event tables, excluding many indexes/remaining tables. A complete sanitized
   global-state specimen or current restore/capacity receipt is unavailable in
   retained evidence; two bounded dbstat attempts were incomplete. Unknown
   append/quarantine/derived growth and future timestamp lifetimes prevent a
   credible worst-case normal-operation bound. A hard8 GiB fixture cap demonstrates
   refusal, not production operability. No broader production copy, policy change
   or emergency capacity allocation was performed to fill that gap.
2. **Actual current-volume query behavior across the full 40-day live horizon.**
   Actual product helpers pass on one 12M week plus hot state. The only retained
   multi-month distribution is a180k stratified90-day sample; no sanitized
   current-volume multi-week query specimen/distribution is available. Those
   fixtures cannot establish the10-second frontdoor budget over roughly six to
   seven current-rate weeks, especially sparse lookups, changing density and late
   event placement. Repeating identical weekly files would not establish the
   missing cross-period distribution/identity behavior. This is not a measured
   performance rejection.
3. **Expected-history catalog admission bound at the required historical span.**
   Independent review found10→9silent historical omission after local retry-ring
   pruning. The fix anchors immutable expected identity/receipt/Parquet custody
   in stable global state and passes missing/corrupt-object and commit-cut tests.
   Only nine closed periods exercise the full pruning contract. Startup
   checksums, manifest admission and reader-memory/scratch behavior at the actual
   required long-history cardinality remain unavailable; the legacy cold consumer
   still has required coverage. No claim that catalog costs are independent of
   historical lifetime is earned by the small fixture.

These are identifiable missing specimen/operational envelopes, not permission to
resume general research. Completing the candidate's finite tests is not a
substitute for them. Any producer filling the gaps needs aggregate admission,
measured provenance, durable dispatch and independent review, not another guessed
32GiBgate or temporary filesystem.

## Earned results

12Mevents: rollover0.0359s; direct Parquet conversion153.18 s; verification372.37 s.
Full verified retirement32.28 s, including0.886 s source unlink, returns11.113 GB.
Actual sparse frontdoor p95 = 4.090s; dense refusal p95 = 1.045s. Corrected paged key
expiry admits5,985 new events with exact cursor/payload/sequence preservation;
p95 five-event ingest probe0.724s. Key expiry still deletes hash rows and creates
reusable pages. Payload disposition is O(files); verification is O(bytes).

See [qualification](LABELWATCH-SEGMENTED-QUALIFICATION.md),
[failure matrix](LABELWATCH-SEGMENTED-FAILURE-MATRIX.md),
[state model](LABELWATCH-SEGMENTED-STATE-MODEL.md), and
[campaign report](LABELWATCH-SEGMENTED-CAMPAIGN-REPORT.md).

## Independent identities

Original source 29f3f76dd51cf5973615fcb5bce255f15c31ac31:
`ACCEPTANCE-27ee66da-0bb4-4e93-a338-687a281baeff.json`,
SHA256`bf85c9a623295cfdb325c4c876e7a372b89ac510b93071fd0593f3b71882dc44`:
NOT_ACCEPTED_COUNTEREXAMPLE. Original 158sealed hashes verified; changed live
source/doc paths are frozen from exact Git blobs, with a reconciliation receipt.

Corrected source f0161ecc44e38985e390211e0e3882f5cbb0e232:
`ACCEPTANCE-17b2ab45-7898-4eda-a2a6-6aea1f59518b.json`,
SHA256`fe4091d20067dfcaaa358ecc6099fed0f181c96d4b3e50dc2840b3cb439494c3`:
independent finite successor PASS; adoption not accepted. All 25 successor seal
hashes verified. Reviewer is the independently dispatched acceptance owner,
separate from the producer; full identity/scope/time live in private custody.

## Exact next action

Existing Labelwatch #7 owns a bounded successor:
`lane/labelwatch-segmented-horizon-capacity-20261005`.
First obtain an evidence-backed complete global-state/required-history specimen
and name current global table growth/replay lifetime contracts. Then admit one
serial qualification producer for40+7day state,40daycurrent-rate queries and
required-history catalog admission. Reuse this exact qualified prototype and
counterexamples; produce capacity/refusal/recovery and independent acceptance.
No new architecture item, new engine, retention-policy change or migration.

Prerequisites: current incident#6bounded independently; no incident capacity
consumed; complete specimen custody; per-filesystem reserve and aggregate tenant
admission; named operational owner for every mutable global growth source.
If no existing finite policy supports a bound, record that concrete forcing
property for owner decision rather than silently inventing a new policy.

Today's incident remains separate. Candidate remains **prepared, inactive**.
No migration sketch or deployment is authorized by this NO_DECISION result.
