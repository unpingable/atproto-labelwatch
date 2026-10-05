# Labelwatch segmented decision-closure qualification

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
21 bounded timestamp-index seeks across Sept 28–Oct 4. Private samples/DDL/meta
key names were read in <1 second each under external deadlines; no full DB scan.

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
32 GiB new-data envelope, 8 GiB archive payload estimate and 100 MiB root allowance.
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
| Recent multi-period | At most eight local vessels; archived vessels use one Parquet engine. |
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
