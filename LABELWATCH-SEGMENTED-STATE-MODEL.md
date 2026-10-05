# Labelwatch segmented state and cursor model

Qualification source and evidence are identified in the campaign report.
This is an isolated adapter; it is not a production collector integration.

## Ownership inventory

| State | Classification | Owner / continuation rule |
|---|---|---|
| Accepted event payload and event ID | segment-local after durable flush | Arrival-period vessel; immutable after seal. Authored `ts` is not the routing key. |
| Firehose / service / per-labeler cursors | hot-global | Existing `meta:ingest_cursor:<source>`, observed/advanced timestamps. Preserve exact source key; no segment-local cursor. |
| Monotonic ingestion IDs | hot-global | `state.sqlite:sqlite_sequence`. Archive preserves IDs; replacement never resets sequence. |
| Replay keys within live floor | hot-global | `q_hot_keys(hash,ts,id,segment)`; no DuckDB synchronous ingest dependency. |
| Below-floor replay / late event | hot-global | Existing `quarantined_events`, full original event plus seen count. Never silently reinsert into accepted live history. |
| Labeler classification, probe/evidence and provider identity | hot-global | `labelers`, `labeler_evidence`, `labeler_probe_history`, `provider_registry`. Actual collector mutations must continue in the global transaction. |
| Alerts, acquisition outcomes, derived receipts/checkpoints | hot-global | `alerts`, `ingest_outcomes`, `derived_receipts`, all existing meta keys. Segment retirement never deletes them. |
| Derived tables | hot-global / reconstructible | `derived_label_fp`, lag/reversal/boundary-load, value-day, entropy, author-day, author-labeler-day tables. Their update/retention policies are unchanged. |
| Discovery/boundary/publication state | hot-global | `discovery_events`, `boundary_edges`, `boundary_targets`, `posted_findings`; no move into immutable vessels. |
| Retention floor, archive coverage, generation/build identity | hot-global | Existing `retention:*`, `schema_version`, code identity and other meta; qualification prefix `q:*` owns lifecycle only. |
| Pending acceptance journal | hot-global, transient | Existing event table plus `q_pending`, at most one 10,000-row page per conforming writer transaction. |
| Segment schema, sealed marker, identity | segment-local | Explicit `segment_meta`; source refuses unqualified columns/generation before conversion. |
| Parquet identity, content, checksum, receipt | external immutable custody | Archive filesystem; verified before local unlink. Receipt/checkpoint are distinct. |
| ACTIVE.json | reconstructible | Projection of global q_segments; stale projection is ignored. |
| Pilot label_state cursor/build metadata | external / prepared | Existing sidecar schema 2, explicit pilot; no automatic activation. Retain independently, including admissibility gate. |
| Reporting/publication files and disabled jobs | external | Prepared/inactive state stays so. Qualification does not revive reporting or derive. |
| Obsolete state | none assumed | Existence alone never permits deletion. |

The bounded production observation collected table DDL and 412 metadata **key
names**, without copying private cursor values. Every ordinary global table is
seeded and digested in rollover/retirement fixtures; metadata and sidecar tests
are separate. Preserving opaque state does not qualify every future collector
adapter. The isolated adapter calls the real observed-source helper in stable global state; its flag and evidence updates have a guard case. Collector outcome adapters remain implementation work and must keep using the global owner.

## Acceptance and rollover

1. Hold writer fence; reconcile pending journal and any prepared transition.
2. Commit accepted payload, replay keys, destination and cursor in global SQLite.
   The real `db.set_cursor` helper commits internally. This is the acceptance
   boundary; a death before it accepts nothing, a death after it must recover.
3. Idempotently flush exact ID/full-column content to the active vessel.
4. Commit journal drain. Retry never assigns a new ID to the same accepted event.
5. Rollover commits a prepared old/new transition, drains writes, seals and
   checkpoints old, creates/validates next, atomically publishes one ACTIVE row.
6. New ingestion resumes. Old conversion/full verification is outside that fence.
7. Verified receipt becomes ARCHIVED; reader lease permits unlink; RETIRED follows.
   Death after unlink recovers only when existing archive custody verifies.

Exactly one ACTIVE row is enforced by a partial unique index. A prepared
transition can temporarily have the old target sealed, but no writer uses it:
recovery completes or refuses first. The final old writable moment precedes
seal under the fence; the first next writable moment follows global activation.
Backward clock refuses; duplicate same-period rollover is a no-op. Unexpected
forward boundary is explicit caller input, not an implicit host-clock mutation.
Late authored events above floor enter the current arrival vessel. Below-floor
events preserve the current quarantine contract. Arrival identity therefore
cannot alone prove timestamp-query coverage: catalog min/max and floor matter.

## Replay lifetime and boundedness

Dedupe needs precisely the current live-floor lookback: an archived event still
above floor remains in the hot hash table. Below floor the existing quarantine
rule applies without historical lookup. Floor advancement checks archive custody
first, commits the floor, then expires keys in retryable 10,000-row batches.
Death after floor commit leaves extra keys, not loss; retries finish expiry. Each page releases the global writer fence, allowing ingestion between pages; superseded-floor maintenance refuses. The original639.68-second whole-expiry fence is a preserved operability counterexample, corrected and tested against12M restored keys.

This replaces payload DELETE with file retirement but **does not eliminate
row-level maintenance**: hash-key expiry remains and quarantine/global state
need explicit capacity ownership. A page cap is a refusal ceiling, not proof
that a production horizon fits. These distinctions are adoption gates.

## Query coverage

The local SQLite adapter reads at most two non-retired vessels with fixed
UNION views and a reader lease. It is not an all-history adapter. A current or
recent answer cannot drop still-required rows merely because their vessel was
archived. Retain required vessels or route that coverage to verified Parquet;
otherwise refuse explicitly. The qualification counterexample tests this rule.

## Schema contract and model limits

Archive readers declare a finite generation window; vectors cover 23–26 with
NULL/Unicode, nullable additions, rename, representation change and old-reader
refusal. Auxiliary columns are preserved in custody even when the canonical
query projection does not consume them. An unqualified generation refuses;
immutable history is not automatically rewritten. A successor must qualify
vectors and retained-custody readers before old-reader retirement. Unsupported
required history causes refusal, never absence claims.

The bounded logical model considers one accepted event across journal, vessel,
seal, verification and retirement; process death preserves durable states.
It establishes only logical custody under honest SQLite FULL/fsync storage,
conforming writers and reader leases. Real cut tests supply correspondence;
physical host/NFS power loss and application integration are not proven here.

## Period and admission contract

The tested normal period is a UTC arrival week, Monday-start identity. The caller
supplies the expected next identity; the prototype validates its ISO date and
refuses backward movement. It does not silently rewrite files when wall time
changes. Monday alignment and the clock-to-period adapter must be explicit in
collector integration; unexpected forward boundary tests preserve cursor/state,
and gaps remain observation obligations. Authored timestamps remain payload and
query/retention keys, never a reason to reopen a sealed arrival vessel.

Local payload queue is exactly one ACTIVE plus at most one SEALED/ARCHIVED vessel.
A third non-retired vessel is refused. Immutable archive work takes its own
per-segment fence and a reader lease, independently of the ingestion writer
fence. Concurrent conversion retries return the original immutable receipt.

Every writer reapplies the owned SQLite page ceiling. The initial creation-only
limit was ineffective after reopen and is retained as a counterexample. A full
vessel after acceptance leaves the exact event/cursor in the global journal;
qualified recovery is explicit and bounded. A full global file refuses before
acceptance. WAL admission refuses when checkpointing is blocked; key expiry
checks before each 10,000-key page. Its small pinned-reader negative control
proves refusal and subsequent retry, not a continuous exact production WAL peak.

The physical bound is parameterized by admitted global state, two admitted
vessels, WAL admission plus one bounded transaction, and explicit query spill.
The tested 8 GiB global / 16 GiB vessel limits are fixture controls. They are not
a claim that all production global state plus a 40-day replay horizon fits 8 GiB.
No production capacity policy or retention floor changes in this lane.

## Finite schema generations

| Generation | Qualified change | Canonical reader behavior |
|---|---|---|
| 23 | Current event schema and seven deployed indexes | Full canonical fields; active writer stays 23. |
| 24 | Add nullable note and SQLite index | Preserve auxiliary column in custody; canonical reader ignores approved auxiliary field. |
| 25 | Rename val to label_value | Explicit lossless mapping to canonical val. |
| 26 | Integer neg to true/false/NULL text | Explicit mapping; unknown encoding refuses. |

Reader 26 accepts the qualified 23–26 window. Reader 23 refuses newer generations;
22/27 refuse. Actual SQLite ALTER/index operations and full physical-column
round trips accompany shared NULL/Unicode vectors. The production-style archive
writer deliberately refuses a new generation until its adapter is separately
qualified. This is a finite evolution contract, not a claim that active writer
upgrade/rollback has been deployed or arbitrary schemas remain supported.

## Formalization identity and scope

The one-event model in tools/segmented_qualification/supplement.py explores seven
states/eighteen edges under its declared assumptions. Exact result/source hashes
are in the producer seal. The proposition is accepted-event custody and verified
retirement across durable transition cuts. It does not prove the whole service,
physical power failure, arbitrary concurrent writers or future schema adapters.
Practical process-cut, concurrent-writer, mutation and finite-schema cases provide
bounded correspondence. The independent acceptance record names the decision
owner and any counterexamples; producer completion alone does not accept a claim.

## Historical expected coverage after retry-ring pruning

Independent acceptance found a missing-history counterexample at29f3f76:
pruning the ninth old local retry record erased its expected-coverage obligation.
If its receipt and Parquet disappeared, a new historical reader returned9 accepted
events instead of10 without refusal. Original evidence is preserved.

Successor f0161ec retains a compact hot-global coverage path/hash. Before deleting
old local retry rows, it writes an immutable archive-side generation listing
expected identities and receipt/Parquet hashes; the anchor and local pruning
commit in one SQLite transaction. Death before commit leaves an unreferenced
generation and the previous expected authority. Required history includes that
anchored manifest plus unpruned ARCHIVED/RETIRED local records. Missing or changed
manifest, old receipt or old Parquet causes refusal, never an empty-history answer.
Archive catalog metadata grows on archive storage; startup/read admission and
scratch at long horizon remain explicitly unqualified capacity evidence.
