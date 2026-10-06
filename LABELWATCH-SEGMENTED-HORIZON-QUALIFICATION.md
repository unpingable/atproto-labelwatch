# Labelwatch segmented horizon qualification — reopened custody-qualified candidate

Starting/exercised source: `6a88178f8d30fed13c582acccd532128c6b306ce`.
Existing branch `lane/labelwatch-segmented-horizon-capacity-20261005` was
fast-forwarded from `872693a`; the segmented adapter already uses the qualified
shared custody contract. No implementation, engine, semantic model, fixture
framework or storage topology was added. Owner: existing Labelwatch #7.
Root occurrence `28e39263-168f-46e7-8f22-25a7f29fc684`.

## Result

`REJECT_SEGMENTED_SQLITE_PARQUET_DUCKDB` for the current catalog admission design.
One required gate fails concretely. Custody remains qualified; rejection does
not identify a SQLite/Parquet/DuckDB defect or authorize another engine.

| Reopened gate | Result | Evidence |
|---|---|---|
| 40+7-day current-volume all-global capacity | NOT_RUN | No macro allocation after an independently confirmed admission failure. No extrapolated capacity acceptance. |
| Required full 40-day current-volume queries | NOT_RUN | Existing one-week query and exact coverage evidence retained, not promoted to full horizon. |
| Long-history catalog admission | FAIL | Every usable new-owner admission rehashes all historical Parquet. |

The explicit gate requires no rescan of every historical payload to admit today's
segment. The current candidate does require that rescan. The decision follows
that failed property; it is not NO_DECISION for more confidence.

## Exact existing path and specimen

`TierSession` consumes `VerifiedCatalog`. Its constructor enumerates receipt
files and hashes each corresponding Parquet file. It has no incremental admission
or indexed identity/time-range entry point. A cached catalog does not itself
rehash payloads, but excludes a newly committed owner; complete query admission
then refuses until a new catalog is constructed. Custody correctly prevents false
completeness. Repairing custody did not repair this admission cost.

The direct producer reuses `horizon_qualify.catalog`, `Store`, `qualify.event`,
archive receipts and SHA instrumentation. A new one-row source is archived using
the qualified custody path. 8,193 metadata identities hardlink that immutable
4,196-byte Parquet specimen. These are metadata-cardinality fixtures, not years
of production event history. No 47-day specimen or current-volume query latency
is claimed. One seed source event is accepted by the producer.

| Existing identities | Startup hashes / bytes read | Admit one new segment: hashes / bytes read | Admission seconds | Cached files p50 / p95 seconds | Receipt metadata bytes |
|---:|---:|---:|---:|---:|---:|
| 128 | 128 / 537,088 | 129 / 541,284 | 0.01353 | 0.00230 / 0.00368 | 15,351 |
| 1,024 | 1,024 / 4,296,704 | 1,025 / 4,300,900 | 0.11195 | 0.01907 / 0.01936 | 121,975 |
| 8,192 | 8,192 / 34,373,632 | 8,193 / 34,377,828 | 0.95077 | 0.15340 / 0.19008 | 974,967 |

Seven cached observations per size; p95 is the maximum of these seven samples.
Peak producer RSS: 157,646,848 bytes. Receipt metadata is about 119 bytes/identity
in this minimal fixture, not the size of real custody receipts. Hardlinks save
physical fixture space but do not change the code's hash invocation/bytes-read
recurrence. Total physical fixture allocation: 34,074,624 bytes (deduplicated).

For N old segments and payload bytes B_i, admitting segment N+1 reads
`sum(B_i, i=1..N+1)` and examines N+1 receipts. This is
`O(N + total historical payload bytes)`. Cached `files()` is O(N) stat work.
No capacity or throughput claim is inferred from the tiny test file timings.
The failure exists with two real owners; it is not a theoretical future-scale
objection.

## Independent falsification

Independent owner `/root/independent_acceptance` verifies the producer's 16,407
sealed pointers and exercises two real custody-committed owners. The first cached
catalog returns complete history before the second owner. After the second
commit it refuses `committed archive coverage unavailable`. Fresh catalog
admission recovers correct count 2, but hashes both 4,196-byte old and4,193-byte new
Parquet objects. Existing separate frontdoor summary-catalog machinery is not
an incremental canonical-row/identity admission path consumed by this tier.
The first independent preparation occurrence hit the existing two-local-vessel
queue limit; it is preserved as a setup failure. The successor uses normal
verified source retirement, with a fresh run identity. No candidate correctness
failure is inferred from that setup error.

Exact independent receipt identity is recorded in the terminal report and
[decision](LABELWATCH-SEGMENTED-DECISION.md). Independent review confirms the
current admission-design failure, not that a bounded incremental repair is
impossible. It does not repeat broad rollover/custody/engine review.

## Preserved acceptance and scope

The12M-event rollover/reclamation/crash-replay evidence remains immutable at
`b2b9a416`. Shared custody qualification at `1fdfd028` / published `6a88178`
already includes both layouts and the exact 10-event historical-coverage case.
No exercised implementation bytes changed in this reopened lane. Those accepted
cases are therefore not rerun; no previous unearned macro claim becomes earned.
Original horizon source/report bytes are retained from `872693a`, with exact
hash mappings. Old rejection remains historical evidence, superseded only for
custody by its separate qualified repair.

## Execution and recovery

Host crow; existing Python3.12.3/SQLite3.45.1/DuckDB1.4.3/PyArrow22.0.0 environment.
Durable unit `labelwatch-horizon-catalog-28e39263.service` is terminal, MainPID 0,
Result success/ExecMainStatus 0. Invocation was `fefdffdc1eb94962ba9e54dc70034cba`.
Private registry: `portfolio-private/campaigns/labelwatch-segmented-horizon-capacity-20261005`.
Exact dispatch `DISPATCH-28e39263-168f-46e7-8f22-25a7f29fc684.json` pins source,
command, environment, script hash, allocation, log and expected terminal result.
Existing venv and helper are reused; the direct script is campaign evidence,
not another runner/framework. Inspect terminal/checkpoint/acceptance records;
never restart the completed producer. Source drift during review is forbidden.

Storage owner /root; producer admitted 1 GiB/data and 256 MiB root, reviewer 64/16 MiB
aggregate envelope (planned 4/1 MiB). Both independent 60 GiB reserves remain.
No NFS or production-host allocation, production observations/mutations, new
keys/packages/services, migration, retention-policy change or cleanup.
All new evidence has existing#7 replay/repair dependency; review 2026-10-12 or
successor acceptance. Exact final allocation/capacity is in the closeout receipt.

Formalization consideration: counted calls/bytes and source-loop recurrence
suffice to falsify this cost bound. No new semantic model is needed. The finite
specimen does not establish full-horizon usability, physical-loss behavior or
production latency. Reviewer is decision owner for evidence acceptance.

## Required next change

Repair the existing catalog admission path to retain earned custody/completeness
while admitting today's owner without rehashing every historical payload.
Quantify identity/time-range/retirement lookup and interrupted-update recovery
using existing machinery. Then reopen the same three gates; no engine comparison,
new model, migration or automatic production activation follows this rejection.

Independent acceptance: `4bf47479-ebb8-4661-85d7-ee53f6c1044e`, disposition
`ACCEPTED_CONCRETE_GATE3_REJECTION`, SHA256
`dcd7302bd8a6bf55a76ab55cf03ebe1e65b56e225b05148d046afc4874628693`.
Exact private record: `ACCEPTANCE-4bf47479-ebb8-4661-85d7-ee53f6c1044e.json`
in the owning existing horizon campaign. Review source equals exercised `6a88178`;
final publication changes documents only.
