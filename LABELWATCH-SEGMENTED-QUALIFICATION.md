# Labelwatch segmented decision-closure qualification

> Historical qualification/model at `b2b9a416`, preserved in its original scope.
> Current candidate is rejected by the [horizon decision](LABELWATCH-SEGMENTED-DECISION.md):
> mutable historical replay/quarantine lifetime and catalog custody are concrete defects.
> See [new evidence](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md).

Lane: `lane/labelwatch-segmented-storage-qualification-20261005`.
Decision and exact immutable receipts: [decision](LABELWATCH-SEGMENTED-DECISION.md),
[campaign report](LABELWATCH-SEGMENTED-CAMPAIGN-REPORT.md).

This lane qualifies one candidate: bounded hot SQLite → immutable Parquet →
DuckDB historical questions. It does not change production, policy, deployment,
reporting/derive activation or today's incident. No alternative engine reopened.

## Specimen and protocol

Target 12,000,000 events exceeds the observed production week (9,764,010).
The primary producer uses actual schema 23, every event index, actual normalized
hash and ingest helpers. Source: 20,000 current-ID events plus 10,000 events from
21 bounded timestamp-index seeks across Sept 28–Oct 4. Private event samples/DDL/meta key names were read in <1 second each under external deadlines. Two later dbstat global-footprint observations took 2.35/10.22 seconds, over their soft budgets but inside the external 12-second bound; both are incomplete and no further full-production scan was attempted.

Expansion is deterministic and synthetic. Event value/signature/size shapes come
from bounded observations, not an entire-week joint distribution. DID/subject
keys are pseudonymized; dense targets retain concentration across repetitions,
long tail gets independent namespaces. Timeline uniform across one week, 1%
72-hour lateness, 0.001% future outliers and 0.5% separate replay offers are
explicit qualification controls, **not measured production rates**. Mutated
signatures are shape evidence, not cryptographic-validity claims. Raw/private
identities never enter GitHub or public outputs.

Exact source/protocol/env/dataset hashes and commands are in immutable dispatch.
Primary run is durable user-systemd with 2 GiB RSS ceiling and two CPU quota,
32 GiB new-data envelope and 8 GiB archive payload estimate. The initial 100 MiB root allowance was exceeded by reference-query temporary files; an explicit amendment admitted 2 GiB, without lowering either hard reserve.
The host retains 60 GiB independently on `/` and `/data`. No cloud incident
capacity is used. The verified campaign NFS path is actually rw; bounded
write/fsync/close/rename/readback/remove succeeded. Archive receives direct output,
no staging filesystem. Corpus on /data has a named acceptance/replay dependency.

## Correctness gates

[State model](LABELWATCH-SEGMENTED-STATE-MODEL.md) inventories every table/state
class, global cursor/sequence ownership, acceptance transaction and rollover.
The 27 primary finite cases establish typed cut/retry behavior; 15 schema cases
exercise actual SQLite mutations and finite archive generations. Separate
supplements cover full observed meta-key names, external pilot checkpoint,
concurrent writers, reader lease, real 30/90-day history and cross-tier coverage.
[Failure matrix](LABELWATCH-SEGMENTED-FAILURE-MATRIX.md) distinguishes actual
process death, deterministic environmental fixture and unproved host failures.

The initial finite attempt placed “before commit” after `db.set_cursor`, which
actually commits internally. This qualification error is preserved; the adapter
now puts its cut at the actual boundary. It was not a production defect.

The local-only query adapter also exposed a coverage counterexample: retiring
recent vessels makes still-required rows disappear from a local UNION. Cross-tier
queries must prefer verified Parquet for archived vessels, stream mutable hot
rows in bounded Arrow batches, preserve the live-floor filter, and refuse missing
custody. No synchronous ingest DuckDB or dozens-of-SQLite historical fanout.

## Query sources

| Family | Proper source / acceptance |
|---|---|
| Current point / sparse subject | Indexed hot SQLite + verified archived rows still above live floor; actual frontdoor behavior unchanged. |
| Dense subject | Existing 2,000-event refusal retained; exact Q3/Q8 SQL parity tested in DuckDB. No cap uplift. |
| Recent multi-period | At most two local vessels; archived vessels use one Parquet engine. |
| Historical tail | Verified Parquet/DuckDB with explicit coverage; no absence inferred from missing catalog. |
| Report/derive | Batch historical query plus bounded streaming current rows; global receipts stay in stable SQLite. No job resurrection. |
| Operational/debug | Stable global state, catalog/lifecycle and explicit horizon; archive health separate from SQLite. |
| 30/90-day ranges | Reuse actual 180,000-row stratified 90-day evidence, with exact source hashes; not a full current-scale 90-day claim. |

Primary p50/p95 uses seven observations and nearest-rank p95. The actual server
has a 10-second generation timeout; sparse/dense current-scale cross-tier
frontdoor must fit it including integrity admission. Batch report work has no
invented interactive SLA. Record setup/admission separately from query execution.

## Capacity interpretation

Do not substitute another unexplained 32 GiB production constant. Derive local
admission from global state + bounded writable/sealed queue + one ingest page,
WAL under pinned readers, conversion/checkpoint work and query spill. Parquet
payload lives on archive; reserve its capacity before work. No `VACUUM` needed to
retire payload files. Hash-key expiry still creates reusable pages in global
SQLite; global quarantine/derived tables need distinct capacity ownership.

Primary between-batch WAL samples see closed/empty WAL and are **not peaks**.
A durable 1-second sampler starts after the first 500k events and observes WAL,
scratch, open-unlinked files and filesystem free blocks/inodes. Its maxima are
sampled lower bounds, not continuous/exact peaks. Record missing initial peak.
Page/queue/WAL fixture caps are finite refusal controls, not adopted policy.

## Constellation domain contract

| Machine condition | Evidence | Operator message / clear rule |
|---|---|---|
| labelwatch.segment.active | Global ACTIVE identity, period, rows/size, cursor advancement | Current period and newest accepted event; stale fact is unknown. |
| labelwatch.rollover.overdue / failed | Expected next period, prepared transition, deadline, last failure | “Labelwatch rollover is overdue; <old> remains active.” Clear only on successor activation. |
| labelwatch.segment.sealed_pending_archive | SEALED identity and age | “Segment <id> is sealed but not archived; local retirement is blocked.” |
| labelwatch.archive.verification_failed | Exact occurrence/receipt/digest mismatch | “Archive verification failed for <id>; source preserved.” |
| labelwatch.archive.custody_established | Verified identity/schema/count/content/file checksum | Distinct from retirement or all-history availability. |
| labelwatch.retirement.eligible / blocked | Custody, floor/coverage, reader lease and failure | Known cause such as archive admission or reader lease; never generic SQLite indeterminate. |
| labelwatch.capacity.local_insufficient | Derived required bytes, free bytes, queue/WAL/page state | Local required bound unavailable; stop before acceptance/export. |
| labelwatch.capacity.archive_unavailable | Actual mounted destination, writable I/O, free capacity, admission | Archive unavailable/full/read-only; local source remains. |
| labelwatch.history.query_unhealthy | Catalog completeness/schema/refusal, successful query time | Required history unavailable; never zero observed events. |
| labelwatch.ingest.rejected / stale | Accepted watermark/age, cursor advancement, quarantined/rejected counts | Distinguish accepted collection from offered or refused events. |

Fact → typed observation → persisted occurrence → explicit obligation/deadline →
evaluation → operator surface → acknowledgment/escalation. Input staleness is a
separate unknown condition, not recovery. Failure and later recovery append
immutable history; recovery supersedes current condition without deleting it.
Deterministic missing-custody / overdue-seal fixture must yield domain messages;
no intentional production breakage. This is a defined contract, not a claim that
Constellation deployment/enrollment has occurred. Owning integration remains
Monitor #18 and incident #6; acceptance of storage does not close either.

## Current-scale measurements

All values are measured on crow in isolated fixtures, with Python 3.12.3,
SQLite 3.45.1, DuckDB 1.4.3 and PyArrow 22.0.0. Archive is the verified campaign
path on mounted NFS; production Linode storage is never used by this producer.

| Measurement | Result | Scope |
|---|---:|---|
| Accepted specimen events | 12,000,000 | 1.23× observed 9,764,010-event week; 60,000 separate replay offers |
| Original sealed SQLite | 11,113,259,008 bytes | Current schema 23, all seven deployed event indexes |
| Stable global state before expiry | 3,221,364,736 bytes | Hash keys plus small seeded globals; not all production state |
| Parquet archive | 1,594,822,436 bytes | Full ordered-row equivalence, schema/count/file digest |
| Rollover writer interruption | 0.035855 s | Full state/cursor identity check; verification outside hot path |
| Conversion | 153.180 s | Direct archive writes; approximately 10.41 MB/s payload output |
| Verification | 372.371 s | Full ordered source/destination cell digest |
| Verified custody retirement | 32.278 s | Includes full source/archive rehash |
| Source unlink only | 0.886 s | Filesystem disposition; not whole verification lifecycle |
| Local bytes reclaimed | 11,113,271,296 | Allocated source file; no VACUUM |
| Original continuing active events | 482 | Accepted during/around archive; four cursor sources retained |
| Corrected expiry | 11,999,880 keys in 719.860 s | Writer released between 10,000-key pages |
| Ingest during corrected expiry | 5,985 events / 1,197 operations | All offered accepted; cursor/sequence/payload verified |
| Expiry-time ingest p50 / p95 | 0.464 / 0.724 s | Closed-loop five-row probe; not maximum throughput |
| Sampled maximum WAL | 267,025,504 bytes | 1-second sampler started after first 500k; lower bound |
| Sampled query scratch | 829,456,384 bytes | Same sampling limitation |
| Sampled open-unlinked reference temp | 1,318,268,928 bytes | Original /var/tmp placement defect; successor confirms /data |
| Sampled campaign local allocation | 16,132,313,088 bytes | 60-second attribution; not cumulative writes |
| Continuation process RSS peak | 799,342,592 bytes | Queries, retirement and original expiry combined |

The primary run FAILED on report-query memory refusal. Its receipts and failure
are preserved. A separately dispatched continuation, after verified replication
of unique pre-expiry global state, completed query parity and retirement. No
original RESULT was fabricated. The corrected per-page expiry ran against an
independent restoration of that exact global checkpoint, with continuing writes.

12M ingest took approximately 6,300 seconds from phase timestamps: roughly
1,900 accepted events/s. This is a phase-envelope estimate; the primary failure
lost its exact ingest timing array. Original continuing-ingest latency was also
not persisted and is not invented here. Conversion/verification overlap is
proved by accepted active events, not inferred from CPU activity.

## Query parity and latency

Current-scale SQL families have exact result parity. Milliseconds below are
p50/p95 over seven observations; recent_report SQLite is one explicitly
pathological observation, not a percentile.

| Actual query family | SQLite ms p50 / p95 | Parquet/DuckDB ms p50 / p95 |
|---|---:|---:|
| Q3 dense | 53.98 / 60.76 | 815.32 / 1032.18 |
| Q8a dense | 60.54 / 83.04 | 415.43 / 546.98 |
| Q8b dense | 52.29 / 58.58 | 904.43 / 961.52 |
| Q8c dense | 60.44 / 71.02 | 1475.03 / 1544.27 |
| Q3 sparse | 0.106 / 1.138 | 239.15 / 306.97 |
| Recent report | N=1: 126080.57 | 7556.41 / 8016.74 |
| Debug horizon | 2573.83 / 13893.76 | 753.21 / 849.70 |
| Derive value distribution | 78715.96 / 79735.89 | 283.96 / 345.29 |

Actual frontdoor.lookup_subject against the entire 12M Parquet plus 100 hot rows:
dense refusal p50/p95 0.965/1.045 seconds; sparse successful lookup 3.720/4.090
seconds, including catalog fingerprint admission and session setup. Full startup
archive checksum takes 1.098 seconds separately. Batch actual report helpers:
hourly-counts 1.268/1.330 seconds; labeler activity 0.588/0.601; top targets
0.970/1.027, seven observations each. Cross-tier alert evidence finds the new hot
event. This does not activate any job or lift the existing dense-subject cap.

The final guard also reuses 180,000 actual historical sample events across 90
files: 30-day query SQLite 124/196 ms vs DuckDB 22/77 ms, 90-day 581/604 vs
89/96 ms. These are semantic/range evidence, not a 90-day current-volume
performance claim. Existing cold-SQLite history has a named frontdoor consumer;
future migration must preserve or explicitly replace that coverage.

Query memory/spill is finite: one worker, insertion-order preservation off,
frontdoor 256 MB / 1 GB spill, batch 512 MB / 2 GB spill. The original two-worker
512 MB memory refusal is retained; exact SQL passed one-worker controls. Archive
catalog hashes every object at startup, then compares device/inode/size/mtime/ctime
per request. Mutation, replacement and missing new catalog entry refuse until
explicit readmission. This relies on a conforming immutable custodian; fingerprint
checking is not continuous bit-rot detection. Scheduled full integrity audits
remain an operational obligation, not a newly deployed service.

## Capacity bound and remaining deployment evidence

Physical bound: B_global + 2*B_segment + B_WAL_admission + one bounded transaction
overshoot + bounded query spill + bounded control/catalog. Every reopened writer
reapplies page ceilings; a third local vessel refuses. Acceptance before a vessel
fills remains journal custody and requires explicit bounded recovery; a global
file-full refuses before accepting. Pinned readers refuse further work rather
than accumulating unbounded WAL. Tests use 8 GiB global / 16 GiB vessel caps;
these are test ceilings, not an adopted production capacity plan.

Observed per-week hash state is about 268.45 bytes/key. Existing 40-day live
horizon plus seven-day scheduled catch-up at the 12M/week specimen margin projects
80.57M keys / 21.63 GB of key state, before production global tables and indexes.
That is an extrapolation, not a qualified 47-day corpus. Partial production
non-event table observations already total 2.927 GB, excluding many indexes and
remaining tables. Some append-only global tables have no located pruning path;
ingest_outcomes alone has an explicit seven-day cleanup. Reporting/derive remain
inactive where already disabled. A cap makes refusal finite; it does not prove
ordinary service continues throughout its production horizon.

Consequently a normal-horizon global budget and complete live global-state
footprint are unearned. They must not be hidden by the successful one-week
payload test. No gate is reduced, no global policy invented, and no claim that
8 GiB fits the production horizon is made. Independent acceptance must decide
whether this blocks adoption versus requiring another bounded qualification.

Payload retirement itself is O(number of files); full custody verification is
O(bytes), and bounded replay-key expiry remains O(keys). Each extra historical
week grows archive capacity, not local payload capacity. Quarantine, discovery,
metadata/derived state and catalog storage are separate global capacity owners.
The architecture only earns a finite local bound when those owners have explicit
admitted limits and backpressure/recovery semantics. Production admission must
also preserve independently owned host reserves and other active tenants.

## Reproduction and custody

Immutable DISPATCH-<UUID>.json contains exact argv, host, working directory,
revision, environment, unit, resource envelope, log, terminal and result paths.
CHECKPOINT.json tells a successor how to inspect the original producer without
restarting it. Use those commands only after current storage admission; replay is
not permission to start another 12M producer. Executable source lives under
`tools/segmented_qualification/`; source correspondence identifies which modules
were unchanged and which corrected deltas were qualified separately.

Private campaign root is
`/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-segmented-storage-qualification-20261005`.
Primary 82434791, continuation 01e6bd13, corrected expiry abb81a17 and actual tier
073fd2bf are distinct run identities. Their immutable receipts and counterexamples
are in the producer evidence seal. Parquet custody and unique pre-expiry global
checkpoint are on the verified NFS campaign path. No private sample values are
published. Independent acceptance is a separate record, never a producer notice.

## Independent coverage counterexample and scoped fix

Independent review of29f3f76 found an actual historical completeness failure after
local retry-ring pruning: removing the oldest archive pair yielded9 of 10 accepted
events without refusal. Successor f0161ec commits expected archive-manifest identity
in stable global state before pruning. Missing expected manifest/receipt/Parquet
then refuses; publication-before-commit death retains prior authority. This is
qualified with a separate successor receipt and full finite/schema regression.
The original158-pointer seal is unchanged; its old source/doc bytes are frozen
from exact29f3f76 Git blobs in an explicit path-reconciliation record. Original
acceptance and all counterexamples remain evidence. The final decision names
independent successor acceptance and all remaining exact gates.
