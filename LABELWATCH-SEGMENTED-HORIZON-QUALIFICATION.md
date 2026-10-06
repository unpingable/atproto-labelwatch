# Labelwatch segmented horizon qualification — catalog repaired, horizon parked

**October 6 amendment:** the owner conditionally authorizes continuation of this
same #7 after production stability, with no recurring-cost increase. The
[amended decision gates](LABELWATCH-SEGMENTED-DECISION.md#amended-acceptance-gates)
add all-state lifetime, explicit workload envelope, catalog-growth accounting
and peak-space acceptance. They govern the next run; the sealed observations
below are unchanged historical evidence, not completion of the new gates.
No successor producer or production migration has started.

Lane `lane/labelwatch-segmented-horizon-capacity-20261005`; existing #7.
Root run `39657c30-4728-4598-9f48-262bbf154a63`;
sealed producer `04166ec3-e0d6-404c-9667-d02c47a9f363`.
Exact exercised source `97b459d1f2cd9e8dbdc0422fafbeb53c41378e9a`.

## Defect and repair

Previous `VerifiedCatalog` construction hashed every historical Parquet when
admitting a newly committed owner. Retain its exact source/counterexample at
`cae2d4b`; classify this as an implementation defect, not an architecture verdict.

The existing class now uses a local SQLite metadata index. Admission looks up
one committed custody receipt by primary key, checks its immutable binding and
hashes only the candidate when first admitted or explicitly readmitted after
replacement. Duplicate admission checks that binding without payload hashes.
Custody commit precedes derived catalog admission and success receipt publication;
query admission still requires complete committed-owner coverage and matching
published receipt content. A separate explicit `full_audit()` performs historical
payload integrity hashing. Catalog metadata is derived, never custody authority.

Immutable fingerprints retain pathname replacement/content mutation refusal.
They do not prove absence of physical bit rot: full audits remain a separate
operation. The shared custody implementation was not changed by this repair.

## Actual catalog cardinality qualification

The existing runner prepared current-schema sealed SQLite per identity, used
actual archive verification/canonical custody commit/catalog admission, and
retired each empty source through the existing protocol. These are real committed
archive owners with zero event rows; this isolates metadata scaling and makes
**no claim of current-volume event qualification**.

| Existing owners | Total admitted | New hashes / candidate bytes | New archive protocol read syscall bytes | Catalog bytes | Duplicate point p50 / p95 seconds |
|---:|---:|---:|---:|---:|---:|
| 128 | 129 | 1 /1427 | 361110 | 192512 | .001248 /.001391 |
| 1024 | 1025 | 1 /1427 | 397977 | 1462272 | .001605 /.001727 |
| 8192 | 8193 | 1 /1427 | 410272 | 11583488 | .001385 /.001462 |

Read syscall bytes cover the complete new archive protocol, including bounded
SQLite/candidate/metadata work. They remain below1MiB at every sampled count;
physical read bytes were0 on this cached host and are not a cold-storage latency
claim. New8193rd archive protocol took .09258s. All duplicate/restart payload hash
counts were0. Primary-key query plans use the covering identity index. Peak RSS
134557696 bytes. Metadata grows with segment count; admission avoids historical
payload bytes. These finite measurements are not a mathematical infinite-scale
constant-I/O proof; the indexed algorithm performs candidate plus logarithmic
metadata work.

The actual former class from preserved Git source hashed8193 payloads: the
bounded gate fails as expected. Explicit full audit deliberately read all8193
objects/11691411 bytes in a separately recorded phase. Do not erase this negative
control or treat the explicit audit as admission.

## Affected regressions

Existing catalog content-mutation, restored-mtime, pathname replacement,
explicit readmission, new-owner complete history, missing-entry refusal and
readmission recovery controls passed. All41 existing finite custody cases passed,
including the exact original10-event historical-coverage refusal/recovery case.
The finite rerun is justified by materially changed archive/catalog/publication
ordering. Prior12M rollover/reclamation qualification was not repeated.

Not yet earned: interrupted derived catalog commit recovery, long-history
range/orphan/corrupt-entry targeted controls, and independent acceptance of this
repair. Historical independent acceptance `4bf47479` describes the old defect;
it is not acceptance of this successor.

## Remaining horizon evidence

The47-day current-volume specimen and full40-day queries are **NOT_RUN**.
The previous12M-event immutable production-shaped specimen remains available
for reuse; do not confuse zero-row catalog owners with the volume gate.

Read-only observation acquired complete scalar counts for21 existing production
global tables and bounded aggregate type/null/UTF8-length profiles. The profiles
sample first/tail records, not a uniform joint distribution. No raw production
rows were exported; an attempted raw export was refused before execution and
replaced with aggregate-only observation. These inputs can support size-shaped
local fixtures, not claims of copied production semantic state.

Formalization consideration: reuse the earned custody model; targeted negative
controls plus source correspondence and read/hash counters suffice for this
bounded implementation change. Scope assumes conforming immutable custodians,
SQLite FULL/fsync, exact receipt binding and immutable fingerprint admission.
No new semantic model or architecture comparison was introduced.

## Park and custody

At operator request, stop at the sealed producer boundary. Durable user unit
`labelwatch-catalog-repair-04166ec3.service` is inactive/dead, Result success,
ExecMainStatus0. No horizon run was launched. Inspect the exact parked checkpoint
before any successor; never restart this completed producer.

Private campaign: `portfolio-private/campaigns/labelwatch-segmented-horizon-capacity-20261005`.
`CHECKPOINT-PARKED-39657c30-4728-4598-9f48-262bbf154a63.json` pins source,
terminal/result/dispatch hashes, outstanding work, capacity and next action.
`PARK-STORAGE-39657c30-4728-4598-9f48-262bbf154a63.json` records473694208 bytes
of retained new runtime substrate with named#7 replay/review dependency; no
cleanup. Both host60GiB reserves pass. No new credentials were generated.
Production mutations **NONE**. No migration, deployment or policy change.
