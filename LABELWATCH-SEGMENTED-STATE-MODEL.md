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
adapter. In particular, observed-source evidence and ingestion-outcome updates
must be routed to stable global state at implementation time.

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
Death after floor commit leaves extra keys, not loss; retries finish expiry.

This replaces payload DELETE with file retirement but **does not eliminate
row-level maintenance**: hash-key expiry remains and quarantine/global state
need explicit capacity ownership. A page cap is a refusal ceiling, not proof
that a production horizon fits. These distinctions are adoption gates.

## Query coverage

The local SQLite adapter reads at most eight non-retired vessels with fixed
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
