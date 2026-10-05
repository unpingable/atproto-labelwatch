# Labelwatch shared custody qualification — October 5

**CUSTODY_CONTRACT_QUALIFIED**, for the bounded shared contract at
`1fdfd028fd0dde2ca96210fc3cb5d450117efb89`. This does not enroll production,
qualify existing legacy identities, or adopt segmented storage.

## Evidence and implementation

Starting source: `872693a10baabeaec36a5f98e1bf72010db39385`.
Model accepted before implementation: private `evidence/MODEL-ACCEPTANCE.json`;
executable bounded relation `evidence/MODEL-CHECK.py`, 49 states / 2,548 checked
transitions / depth 8. Its atomic-commit assumptions are not a whole-program proof.
The operator explicitly confirmed immutable canonical source-record identity,
including authored time as data, and excluding the opaque transport cursor.

Shared implementation: [custody.py](src/labelwatch/custody.py).
The actual [single-file iterable/service/multi polling paths](src/labelwatch/ingest.py)
opt in after explicit fresh-lineage initialization. They use the same transaction,
identity cache and archive commitment functions as the existing
[segmented adapter](tools/segmented_qualification/storage.py).
There is no automatic schema migration or populated legacy enrollment.

Acceptance commits normalized effect, identity binding and source cursor together
with SQLite FULL. Segmented flushing remains its existing idempotent journal.
Only freshly accepted rows update observed-source evidence. Verification is
read-only; recovery validates an explicit authoritative checkpoint; reprocessing
requires a separate target and has no implicit current mutation authority.
Existing archive reconstruction tools still reconstruct inactive history only.

An immutable SQLite exact-membership index beside verified Parquet replaces
full-event historical identities in the mutable hot cache. Its hash/count is
additive metadata on the existing archive receipt. First custody commitment
requires exact exclusive owner bindings; retries cannot rewrite committed custody.
An owner cannot reopen for fresh acceptance. Hot keys expire only after verified
custody transfer, independent of authored timestamp. Missing coverage refuses
acceptance and preserves the source cursor. No synchronous DuckDB ingest path.

Archive owner and layout projection commit before external receipt publication.
The reader admits committed owners only, refuses missing committed publication,
and excludes orphan receipts. Retirement requires that same canonical authority.
Success receipts refuse an open staging transaction, deduplicate accepted IDs,
and preserve an existing conflicting publication instead of overwriting it.

## Runs and measurements

Private evidence root:
`/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-global-lifetime-custody-20261005`.
All runs used the retained architecture venv: Python 3.12.3, SQLite 3.45.1,
PyArrow 22.0.0, DuckDB 1.4.3. No packages, services or credentials were installed.

| Run | Source | Result | Measured substrate |
|---|---|---|---|
| Producer `cd4d0683-25c6-4158-89bc-817037d8429d` | c8c9e6b | 36 declared cases pass; independent review NOT ACCEPTED | 7.994 s; 10,096,640 allocated bytes; parent peak RSS 119,676 KiB |
| Independent `eae274c1-c0da-4317-bf18-c17f85ac4d98` | c8c9e6b | Three boundary counterexamples preserved | 622,592 allocated fixture bytes |
| Producer `2a3a1e4c-accc-476a-8f94-1499587d89f4` | 1fdfd028 | 41 cases pass | 7.587 s; 10,702,848 allocated bytes; parent peak RSS 120,696 KiB |
| Independent `0e64d667-20c4-4b91-874a-32bde31ada33` | 1fdfd028 | 41 cases rerun + seven targeted controls pass | 12,099,584 allocated bytes |
| Independent `0921d2f4-1d3b-4c2f-867d-d4b05e316187` | 1fdfd028 | Existing-quarantine / unknown delivery / altered index controls pass | 315,392 allocated bytes |

The 29 existing cursor/multi-ingest/quarantine tests pass. Receipt:
`evidence/LEGACY-TEST-RECEIPT.json`; command and exact tested source hashes retained.
These fixtures exercise small 1–3-record transition cases, a 12-owner sequential
lifetime specimen and the existing 10-event historical-coverage regression.
They are not another 12M throughput or 47-day current-volume campaign.
The independent source review verified every pointer in the 347-file successor seal.

First-repair failures were genuine: receipt survived rollback with zero accepted
rows; one accepted identity entered two archive owners; JSON tuple/list conversion
refused a valid checkpoint. All are now permanent executable regressions.
Known-bad quarantine, premature identity expiry and receipt-discovery variants are
also detected. Incorrect control fixtures remain evidence, not usable snapshots.

## Growth relationship and limits

Let H be identities accepted into untransferred hot owners, A committed archive
objects and E historical accepted identities. Mutable hot membership is O(H);
committed local metadata O(A); immutable archive membership O(E). Receipt/index
admission does not discharge an identity obligation: it transfers its evidence.
Exact unordered lifetime replay cannot be proved with an arbitrary hot TTL.
A missing immutable index never proves a record was unseen.

In the 12-owner specimen, every transferred/retired owner's hot count became zero,
including future-authored records. The terminal global file was 335,872 bytes;
12 committed receipts contained 10,098 JSON bytes; 12 immutable one-record indexes
allocated 98,304 bytes. These small-file measurements do not extrapolate a
production capacity coefficient. Index records carry two 64-character digests,
an accepted ordinal and SQLite index overhead, not full event payloads.

Other global tables have their own lifetimes. The specimen's `labeler_evidence`
contains 12 historical observation rows. That history is not an exact dedupe
ledger and has not been declared bounded by this repair. All-table accounting
belongs to the explicit 40+7-day gate, not a hidden acceptance claim.

Cold membership currently examines committed owner metadata and indexes on cache
miss. Immutable SHA admission has a 256-entry fingerprint cache; that is not a
long-history scaling solution. Whole-owner expiry scans and transaction/WAL/scratch
cost also changed. Do not transfer previous 12M throughput/expiry numbers to this
source. The three named horizon gates must measure these paths under fresh admission.

Full operational restoration requires an authoritative complete checkpoint outside
a stale backup. JSON round-trip succeeds; stale checkpoint mismatch, missing
identity evidence and populated history-only enrollment refuse. No claim covers
unreplicated physical storage loss, arbitrary legacy identity reconstruction,
nonconforming archive mutation, or all future source/schema generations.

## Independent acceptance and reproduction

Reviewer: `/root/independent_acceptance`; disposition:
`ACCEPTED_BOUNDED_SHARED_CUSTODY_CONTRACT`.
Record: `ACCEPTANCE-0e64d667-20c4-4b91-874a-32bde31ada33.json`, SHA-256
`53fd00e3ffb592934c79c7f408948638b219675707a00d9750b736aefc9a9131`.
Completion notices and acceptance remain separate. All producers/review units
are reconciled terminal; the unexecuted review dispatch has its own notice.

Use the existing durable user-systemd mechanism recorded in CHECKPOINT/DISPATCH.
For an admitted NEW occurrence only, the direct producer command is:

```sh
PYTHONPATH=src /data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005/runtime/venv/bin/python tools/segmented_qualification/custody_qualify.py --base /data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-global-lifetime-custody-20261005/runtime/contract-NEW-UUID
```

Do not restart or overwrite terminal specimens. Measure shared reserve before
allocating; source and performance-sensitive fixtures remain on admitted /data.
No NFS allocation, topology change or production operation occurred in this lane.
See [failure matrix](LABELWATCH-CUSTODY-FAILURE-MATRIX.md),
[model](LABELWATCH-CUSTODY-MODEL.md), [decision](LABELWATCH-CUSTODY-DECISION.md).
