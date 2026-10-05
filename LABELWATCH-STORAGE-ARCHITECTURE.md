# Labelwatch storage architecture — October 5 executable spike

Decision: **NO_DECISION — MORE EVIDENCE REQUIRED**. Whole-unit retirement earns
further work; neither replacement is a qualified Labelwatch backend.
This lane changes no production storage, service, gate or retention policy.
Immediate incident recovery remains [Labelwatch #6](https://github.com/unpingable/atproto-labelwatch/issues/6),
publication repair [API #1](https://github.com/unpingable/atproto-facts-api/issues/1),
and domain monitoring [Monitor #18](https://github.com/unpingable/constellation-monitor/issues/18).
Architecture execution belongs to [Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7).

## Workload and evidence

The selected core runtime is b5e1a5d; maintenance tool source is ff237ae2;
this spike starts at forward source 51f923e. The exact trim library is identical
between b5e1a5d and 51f923e. Schema version 23 has twelve event columns, global
integer event identity, unique canonical event_hash, and six explicit event
indexes plus its unique hash index. Event timestamp is authored time, not
necessarily arrival time. Cursors are committed with event ingestion.
The mutable database also contains labeler state, receipts, probes, alerts,
quarantine and derived state; moving only label_events does not move the service.

On October 5 at 13:25 UTC the live DB was 44,234,665,984 logical bytes,
WAL about 14.6 MB, auto_vacuum NONE, freelist zero. It had grown
1,964,486,656 bytes from the identified October 3 baseline. Collection kept
advancing; no new query-performance impact was demonstrated. The missed
August 15–21 partition contained 3,043,314 rows. Last successful trim was
October 3 06:51:54.071521 UTC. The live floor remained August 15 at 14:18 UTC.
Target horizon is 40 days with bounded catch-up of at most seven days per
occurrence; the actual floor lag is not a previously earned hard horizon.

A subsequent production read at 14:48 UTC found **44,315,507 live rows** and
**9,764,010 events for September 28–October 4**, about 1.39M/day by authored
time. The historical 3M partition is not a current weekly bound. The last
2,000 stored rows sampled no >1-day/future timestamps; that tiny sample does
not establish absence of late arrivals. The optimized whole-index COUNT took
15.8s despite the intended five-second SQLite progress-handler cap; the handler
did not provide a hard deadline for that path. It is recorded, not repeated.
Future bounded reads need an external process deadline; this expensive metadata
query does not establish a frontdoor regression.

The executable corpus contains **2,993,989 complete real events from August
3–9**, from existing immutable Labelwatch Parquet archives. Every full weekly
input was checked against its existing file hash; full row-content equivalence
was checked when reconstructing/retiring each SQLite segment. A separate
180,000-row ordinal-stratified corpus covers May 16–August 13, drawn from
40,424,325 archived events over 90 dates. It preserves the actual schema,
identities, NULLs, signatures, values, duplicate subject distributions and
ordering; it is private and never published in source or public artifacts.
It is not a full-volume 30/90-day latency qualification. Dataset paths, source
manifests, hashes, exact sampling and environment are in private receipts.

Observed weekly volume varies; do not turn 3M/week into a worst-case capacity
bound. Compressed source Parquet for the measured week is 310,412,213 bytes;
indexed weekly SQLite segments occupy 2,633,621,504 bytes. The stratified sample
uses roughly 70.2 MB for event pages and 92.4 MB for indexes/overhead; the
state-query index alone is 27.5 MB. These are corpus measurements, not a
current production dbstat census. Recent file growth and historical authored-
time weekly volume measure different things. Worst ingest burst and acceptable
power-loss/replay window are not established by these receipts.

Actual query classes include report 7/30-day event and distinct-subject counts,
subject Q3 grouped label values, ordered reversal histories, latest subject
state, lifetime counts and retention horizons. Dense-subject Q8/history had
historical 24–28 second p99 receipts; current source still defaults to a
2,000-event serving cap (`src/labelwatch/frontdoor.py:87`, overridable by
configuration, not separately established here). Parquet does
not by itself remove that cardinality cost. Report/derive remain disabled in
production. Their SQL can be tested without resurrecting those paths. Cold
history supports exact receipts/drill-down and lifetime semantics, not merely
lossy aggregate totals.

## The existing cold path is real, but narrower than the proposed backend

Driftwatch shipped Parquet capture at a40320d, DuckDB snapshot production at
7f29b8c, bounded streaming at 70109c8 and image requirements at e9370ec. The
202M-row production sizing forcing case led to bounded DuckDB processing;
that commit's finite memory measurement used an 8M-row tree. Those are different
claims. The current Driftwatch archive repair worktree remains dirty and
operator-paused; its unaccepted source is not a donor to merge casually.

Labelwatch already has complete-column Parquet export tooling and archived
daily history. Its existing manifests explicitly say direct cold reader
equivalence is unqualified and related mutable state is excluded. Production
still reads a SQLite cold catalog (about 5.53 GB), rather than direct historical
Parquet for every relevant query. The missing work is lifecycle/reader
integration, not discovery of the Parquet idea.

The useful institutional finding is: adjacent cold-capture/query primitives
were developed and qualified, but their complete application to Labelwatch's
primary store remained unfinished. Existing export/catalog receipts did not
earn rollover, direct-reader or complete service restore readiness.

## Current single-file SQLite lifecycle

A weekly coordinator checks archive mount, reads floor and source identity,
exports one bounded historical partition on cloud Zone storage at one read
snapshot/watermark, pulls it into NFS custody and verifies it. It rebuilds and
publishes the extended cold catalog and summaries on crow, copies catalog and
rollback material to Zone, stops the bounded service cohort, retires the old
root catalog under identity/open-consumer checks, installs the new catalog,
commits a pending floor transition, deletes archived rows in 50k ID spans,
checkpoints, quarantines later arrivals below the new floor, and accepts health.
Progress, producer/unit completion, manifests and source custody all matter.
An export snapshot can pin WAL while collection continues.

The October 5 ordering defect started export before root reserve refusal.
The unit correctly rejected signal completion despite an exit=0 shell marker;
progress remained stale. The repair candidate adds admission before dispatch
and truthful failure records (50 focused tests). That fixes control flow, not
capacity. See the [incident RCA](https://github.com/unpingable/atproto-labelwatch/blob/c476532/docs/incidents/2026-10-05-retention-api-rca.md)
in the separate recovery branch.

**32 GiB is a historical root reserve guard, not a demonstrated 32 GiB root
export scratch requirement.** Export staging is on Zone. Zone has a separate
47 GiB launch gate (32 reserve + 15 transient envelope). Crow catalog builds
have separate admission/workspace. Copying/rebuilding the history-sized cold
SQLite catalog remains coupled to total history. No production gate was
weakened. The restricted tool namespace's earlier ro NAS inference was rejected;
actual host mount and bounded write/fsync/rename/remove qualified writable NFS.

DELETE under auto_vacuum NONE creates reusable pages; ordinary file size does
not shrink. Incremental vacuum is inapplicable to that existing mode without
rebuilding into a suitable generation. Full-history VACUUM was not performed:
SQLite documents potentially up to twice the original size in additional free
space. A tiny empty-residual VACUUM is not evidence for a 44 GB operation.
[SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html)

## Candidate designs

### Single-file SQLite, repaired

Keep current event/state/cursor transaction semantics and archive-first proof.
Admit actual storage domains before any producer, bound batches/snapshot
lifetimes, preserve failure/phase facts and rollback. Archive payload should
reach its configured custody without root staging. In the actual topology
NFS is on crow, while the live DB is remote; a direct writer cannot simply
pretend that cloud has the NFS path mounted. A streaming/off-host export would
need an earned watermark/content/terminal transport contract, not another disk.

This is the immediate incident direction, and may be sufficient for ordinary
bounded retention if released and monitored. Remaining complexity: row/index
mutation, pinned WAL, cold catalog growth/replacement, pending trim and late
quarantine, reusable-page lifecycle and capacity admission. A fresh generation
with incremental auto-vacuum could be evaluated separately; it does not make
row retirement a whole-unit operation or authorize a production conversion.
No giant one-shot rebuild is justified by this spike.

### Segmented hot SQLite + Parquet + DuckDB

This is the lead replacement hypothesis, not an adoption decision.

Two period meanings must be separated. **Authored-time weekly/daily segments**
allow direct event-time range retirement, but late events require multiple
still-mutable periods or a verified supplemental-part protocol. **Arrival-time
vessels** allow exactly one writer and bounded rollover: late authored-time
records go into the current vessel, then immutable history is converted into
complete-column Parquet parts with authored-time bounds and exact identity.
The finite arrival prototype demonstrates this latter route. A file named by
arrival date cannot be retired under an authored-time predicate without proving
custody for all its records, including newer/future authored timestamps.

The proposed chain is `active → fence writer → commit seal → verify successor
metadata → durable active pointer → new writes`, followed by
`sealed → complete-column Parquet + metadata custody → independent verification
→ receipt → whole-file retirement`. SQLite owns mutable state; Parquet owns
verified historical event custody; DuckDB answers history questions; small
SQLite projections may serve current/publication facts. Projections are caches,
not a new authority. The immutable archive includes original ID/hash/timestamp,
not merely enough columns for today's report.

The prototype proves event/cursor atomic commit inside a vessel and finite
rollover recovery, but carries only the event/ingest-meta subset. Full mutable
state transfer, archive-only dedupe and metadata restore are not earned.
The current event hash includes authored ts; canonical hash validation can
help route duplicates, but cannot be assumed for every historical normalization
version. A growing global dedupe ledger or new outbox would reintroduce lifecycle
machinery; it must earn its own necessity and bound. Do not hide it.

Whole-unit local retirement is demonstrated for seven files. Fresh Parquet
conversion is demonstrated only on finite fixtures; the large measurement
reuses verified existing Parquet. That is valuable reuse, not a measured large
conversion throughput claim. Local scratch can be bounded by active-vessel
and conversion batch size rather than all history only if an admitted sealed-
vessel queue has a hard bound and backpressure/refusal when archive progress
stops. That queue bound is not implemented here. It also requires history-sized cold
SQLite rebuilding leaves the serving path. DuckDB spill must have a separately
admitted budget; a memory_limit is not a total process-memory guarantee.

For queries: cold Parquet/DuckDB matched six tested SQL shapes. Daily SQLite
ATTACH has default limit 10, insufficient for 30/90 daily files. Weekly open
segments may fit bounded ATTACH, but exact late/floor semantics still matter.
A streamed question-local hot union works on the sampled corpus and costs
2.25 seconds setup before query execution. That is not a suitable default
frontdoor path merely because subsequent queries are fast. Indexed per-file
point fanout or small admitted materialized projections may be preferable;
nonadditive distinct/state/order operations require true global merging.
No bespoke distributed query engine is proposed.

Immutable archives retain schema/format/source identity and content vectors.
Version 23 is the only exercised generation. Readers must explicitly declare
supported generations and exact nullable/default/conversion semantics; refuse
unsupported versions, do not rely on automatic union-by-name coercion.
An additive v24 adapter requires cross-engine golden vectors for NULLs, Unicode,
booleans, IDs, timestamp ordering and canonical hash semantics. Old archive
rewrites are unnecessary if a bounded reader remains a named current consumer.
Retirement of an old reader requires a qualified successor against retained
custody, not elapsed age. No v24 adapter or schema migration is claimed here.

### PostgreSQL native range partitions

Keep operational metadata, event inserts and cursor advancement in one database
transaction. Partition on the current canonical timestamp key, preserve raw
canonical timestamp text rather than silently normalize historic values, and
create future partitions before their admission window. Range routing rejects
missing/retired periods; below-floor quarantine remains an application contract.
Detach isolates a closed relation; independently verified dump/restore precedes
drop. Detach alone does not reclaim space. Relation drop removes relation files;
WAL/backup storage remains independently managed.

The full-week dump restored all original values/IDs exactly; detach/drop and
process-loss restart were exercised. The prototype's initial ingest omitted
one state-query index; a separate same-shape supplemental run corrects that
before comparison. PostgreSQL composite uniqueness includes the partition key.
`(event_hash,ts)` is not an unconditional substitute for existing global hash
uniqueness; canonical validation/normalization and import identity validation
must establish the mapping. The fixture preserves valid input values but does
not replicate every NOT NULL/default constraint; full schema refusal/default
parity is also an adoption gate. Global sequence IDs and rollback semantics need
explicit qualification. The fixture does not grant production credentials.
[PostgreSQL partitioning](https://www.postgresql.org/docs/17/ddl-partitioning.html)

Introduced ownership includes daemon/service, private credentials/roles,
version upgrades, configuration, WAL/checkpoint capacity, schema/partition
creation, tested backup/restore, monitoring and recovery. Replication is not
required by this workload evidence; do not invent a production replication
promise. PostgreSQL removes row-level lifecycle/compaction and cross-file
cursor atomicity machinery, but does not remove archive verification, operator
obligations, temporary-space admission or migration custody.

## Measurements and failure behavior

| Metric | Current SQLite | Repaired SQLite | Segmented SQLite + Parquet/DuckDB | PostgreSQL |
|---|---:|---:|---:|---:|
| Matched 180k ingest, rows/s | 8,941 NORMAL | 10,711 NORMAL; identical writer, timing variation | 45,926 FULL, 90 small indexes/files | 28,255, all event-index shapes |
| Complete historical week ingest, rows/s | exact NORMAL original timing lost; separate FULL fixture 9,237 | no different writer benchmark | 14,615 FULL | 14,627 with all indexes; initial 17,909 omitted state index |
| Representative query p50/p95 | see query table | same query code | see query table; hot-union setup +2.248s | see query table; index-complete distinct supplement |
| Seven-day retirement | exact trim library 191.496s, 60 batches | unchanged trim library | 0.538s unlink, after 72.770s verification | detach 0.020s + drop 0.231s, after backup verification; initial index subset |
| Peak extra local bytes | observed 1,447,194,624 including durable archive and WAL; earlier export peak unknown | no new production-sized measurement; admission fixed | large fresh conversion peak UNKNOWN; finite fixture only | WAL/temporary peak UNKNOWN |
| Filesystem bytes reclaimed immediately | 0; 642,932 pages reusable | 0 for same DELETE semantics | 2,633,621,504, seven files | detached: 0; dropped relation files: 3,225,411,584; global free delta unattributed |
| Archive bytes | 1,134,129,152 allocated SQLite export | same measured library, transport unmodified | 310,412,213 existing compressed Parquet; fresh large write not measured | 385,008,041 dump; 24.593s dump, 83.525s restore |
| Recovery steps after process cut | inspect source/archive/pending floor; verified resume; whole production cohort not tested here | truthful refusal/failure receipt; existing resume protocol | fixed-source: inspect custody and explicit retry; arrival subset: recover source-bound successor/pointer | engine restart, then reconcile source/dump/catalog/receipt and retry |
| Additional persistent database services | 0 | 0 | 0; DuckDB embedded question engine | 1 PostgreSQL service |
| Custom maintenance code | five directly relevant Python files: 1,473 lines, excluding wrappers/tests | same lifecycle + bounded admission/failure correction | isolated combined spike: 945 Python lines; production state/reader composition still missing | shares 945-line combined spike; production roles/backup/partition integration still missing |
| Cross-period query complexity | live + history-sized cold SQLite catalog | same | bounded union/fanout or Parquet/DuckDB; global distinct/order/state must merge exactly | native partition pruning/querying; identity and late-period routing remain contracts |

The matched current/repaired ingest paths use identical storage code: their timing
difference is run variation, not a repair speedup. Small daily indexes and excluded
schema/rollover setup make the 180k segmented ingest especially unsuitable as a
production capacity claim. The all-index PostgreSQL week is retained rather than
repeating retirement; the original detach/drop/dump measurement is explicitly a
different index profile. Source code line counts are a scope census, not equivalent
production implementations or promised savings. The primary point-delete fixture
completed in 181.287s; the 191.496s figure above is the actual trim library including
its internal verification, with another 22.600s prior independent verification.


Query p50/p95 in milliseconds on the **180k stratified 90-date sample**. All six result comparisons passed. Current and repaired single-file SQL are identical.

| Actual query shape | SQLite | Cold Parquet/DuckDB | Sampled hot union/DuckDB, excluding setup | PostgreSQL |
|---|---:|---:|---:|---:|
| latest_subject_state | 0.05/0.23 | 30.27/31.85 | 7.02/7.12 | 6.68/7.16 |
| report_30d_count | 1.14/1.44 | 14.26/16.55 | 3.36/4.51 | 45.06/55.07 |
| report_90d_distinct_subjects | 94.40/106.79 | 32.03/37.94 | 5.15/7.46 | 136.55/192.45 |
| retention_horizon | 45.05/46.49 | 15.21/15.82 | 2.82/3.17 | 85.40/93.06 |
| reversal_ordered | 9.78/10.25 | 18.48/24.84 | 8.71/11.92 | 18.29/19.56 |
| subject_Q3 | 3.24/3.66 | 29.90/58.53 | 6.21/8.30 | 9.78/21.32 |

The PostgreSQL distinct-subject timing is the corrected all-index supplement.
Other initial PostgreSQL query timings retain the initial missing-state-index
profile; they are not silently relabeled as complete-index results. No tested
query generated DuckDB spill on this sample; full-volume spill is unqualified.

These measurements use crow NVMe and serialized candidates, not the cloud VM's
concurrent production workload. SQLite cache defaults, PostgreSQL 128 MB shared
buffers and separate process limits are recorded. Seven repetitions give a
nearest-rank p95 equal to the largest observation; they do not establish an SLO.
No full-history VACUUM, worst-case ingest burst, archive/NAS power failure,
full-volume dense Q8, or complete mutable-state migration was qualified.

Thirty-five primary lifecycle cases include explicit unqualified outcomes;
34 pass/refusal results and one explicitly UNQUALIFIED naive late-arrival case.
The nine arrival rollover cases and ten-row typed-schema round trip are separate
receipts, not an end-to-end backend qualification. The PostgreSQL restart also
passed a retained ten-row digest audit; it is not a full 3M-row restart audit.
Cuts cover before/after seal, verify, archive write/publish, receipt, local
retirement and catalog. Storage-unavailable/read-only/full/local-admission
fixtures preserve source; corrupted published archive refuses; retries reverify;
stale progress does not authorize deletion. PostgreSQL process SIGKILL/restart
preserved its fixture namespaces. This is process loss, not physical host reboot.
PostgreSQL calendar partition precreation/rollover under concurrent ingest,
full cursor/state commits and interrupted DDL at the clock boundary are NOT
QUALIFIED; generic closed-partition custody cuts do not prove those paths.
Arrival tests cover clock boundary/backward clock, accepted late authored-time
records, cursor/data commit interruption and stale active pointer.

| Scenario | Automatic / refusal / operator boundary |
|---|---|
| Process loss at retained boundaries | re-open source and verified custody; idempotent explicit retry |
| Archive unavailable/read-only/full | source preserved; operator/storage prerequisite before retry |
| Archive complete, retirement refused | keep source plus verified archive; retry later |
| Published archive corrupted | refuse; preserve source; operator custody reconciliation |
| Duplicate retry / stale progress | source/content receipt reverified, no duplicate destructive transition |
| PostgreSQL process restart | engine recovers fixture; independently recheck archive/catalog |
| Late arrival in closed authored period | arrival-vessel subset accepts; retired PG range refuses; production policy/integration unqualified |
| Clock crossing / rollback | finite arrival boundary qualified; backward rotation refuses |
| Stale/corrupt schema or checkpoint | unsupported generation refuses; typed archive schema bound; progress is not authority |
| Physical host/NFS restart, concurrent accepted writes during seal | NOT QUALIFIED; writer fence/metadata custody/composition acceptance remains |

## Constellation obligations for every candidate

Observations must bind host, service, source/release, occurrence, period,
watermark, schema, observation time and evidence receipt identity. Direct facts:
newest accepted ingest age and advancement; failed/rejected ingest; expected
active/closed period; overdue rollover/seal; archive incomplete or failed
verification; verified custody; live horizon/floor and exemptions; distinct
local, scratch and archive headroom; last successful occurrence and deadline.

For current SQLite: `expected successful occurrence overdue OR known failed
occurrence OR unexempted rows below admitted live floor after maintenance`.
For segmented storage: `closed vessel older than local live horizon is verified
in archive custody, or carries an operator-visible blocking reason by deadline`.
For PostgreSQL: equivalent retained-partition/custody/failed-occurrence facts.
Do not conflate detach with archival success, a process running with collection
advancing, or input recovery with domain recovery.

Trace `service fact → typed observation → durable evidence → obligation →
evaluation → operator surface/delivery → acknowledgment`. Missing/stale evidence
remains UNKNOWN. A known failed occurrence must still say its consequence,
e.g. “Labelwatch retention is overdue; the August 15–21 partition remains live
because root reserve admission failed.” Generic SQLite indeterminacy is not
that condition. Preserve machine IDs and history when recovery supersedes it.
Use deterministic overdue/failed/corrupt evidence fixtures, never deliberate
production breakage. Monitor #18 owns enrollment, direct domain wording and
negative-control delivery. Watches observe; no service repair authority or
private/DID-bearing public API expansion is introduced.

## Migration feasibility, without migration recommendation

Segmented migration can retain the current DB as read-only history at a durable
watermark, briefly fence writes/cursors, create a next vessel with exact metadata,
then permit bounded dual-read of disjoint coverage. Convert older custody in
bounded periods; prove counts, full row digests, IDs/hashes, time bounds and
metadata before disposition. Rollback after new accepted writes requires replay
of those writes/cursors, not merely switching an old pointer. Keep the old
history donor until an independently accepted reader/restore replaces it.

PostgreSQL can import bounded historical periods before a brief fenced live
cutover. Avoid unqualified dual-write. Snapshot watermarks/cursor positions and
canonical identity bind the handoff. Rollback after cutover must export/replay
new accepted rows back through an earned schema/identity path; it is not dropping
the new database and resuming an old cursor. Both designs need a bounded
compatibility window and independently tested restore/reconnect, not one giant
unreviewable transformation. No migration sketch file is emitted because no
architecture change is recommended.

Potentially removable machinery under an earned segmented design: row export
reserialization from an immortal hot file, large row DELETE/compaction phases,
history-sized cold SQLite transport/rebuild/rollback. Retain archive integrity,
receipts, metadata custody, floor/quarantine rules and operational checks.
PostgreSQL may remove hot row retirement/compaction and cross-file atomicity
code; it retains archive/validation and adds service/WAL/backup ownership.
No current code is deleted on the strength of a prototype.

## Reproduction, custody and formalization

Source and commands: [tools/storage_spike/README.md](tools/storage_spike/README.md).
Private campaign: `portfolio-private/campaigns/labelwatch-storage-architecture-20261005`.
CHECKPOINT.json binds every attempt, source, unit/container, logs, terminal and
resume instruction. Initial socket setup attempts are preserved failures with
no workload imported; primary measurement and supplemental identities differ.
Raw private datasets stay outside Git. See CAMPAIGN-REPORT.md for sealed result
hashes, storage disposition and independent acceptance status.

Formal proposition: retirement implies complete independently verified archive
and durable receipt. A six-state, 24-transition executable model establishes
that proposition only under a fixed accepted set, stable archive and atomic
publication, with no concurrent writer/correlated custody loss. Practical
process/receipt/NULL/schema/cursor tests cover stated implementation subsets.
Composition with the whole Labelwatch service, host power loss, evolving schemas
and archive-only dedupe remains unproved. Decision owner: Codex /root;
independent acceptance is pending. Tests and this bounded model are appropriate
for a non-production architectural spike; no broader safety or migration
approval follows from them.
