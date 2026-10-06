# Labelwatch segmented state and cursor model

## Current v5 recent-observation candidate (2026-10-06)

This section supersedes the lifetime claims of the historical model below for
new `RecentStore` stores only. Source correspondence is the sealed `02a3a5d`
implementation: `tools/segmented_qualification/{recent_storage,storage,tier,recent_provider}.py`,
`src/labelwatch/{recent_collector,recent_observations}.py`, and the existing
collector/daily command wrappers. The historical all-history adapter is not the
current service contract. No production enrollment, old-store conversion, or
production capacity acceptance follows from this inventory. `require()` refuses
profiles other than v5; existing evidence remains under its original authority.

### Named writer graph and window

The successor collector discovers/resolves sources into a sticky roster, claims
a bounded scheduled source, records an attempt, then accepts at most 100 strictly
normalized rows from that source with an opaque provider cursor and attempt-token
comparison in the global transaction. Pending accepted rows flush idempotently to
the active daily vessel. The daily driver recovers existing journals, drains the
sealed queue, rotates, archives/verifies/retires, and maintains the rolling window.
`RecentProvider` and the account/export product consume this authority. The
qualification-only direct `ingest` seam and offline constructor are separately
scoped: neither establishes the collector's HTTP service rate or provider continuity.

The service interval is `[now - 30 days, end)`, on trusted UTC observation time,
with microsecond comparisons. Source-authored timestamps remain event content.
Physical daily owners include the partially overlapping boundary day: normally
at most 31 daily owners including active, with explicit control headroom below.
New enrollment records acquisition start; pre-enrollment coverage is unknown,
not reconstructed from old authored timestamps. Only 30 days is admitted by
`RecentStore.create`; a generic product option accepting 45 does not qualify a
45-day store. No lifetime deduplication guarantee survives window retirement.

### Global tables and indexes

All listed SQL indexes share the lifetime and physical compaction of their parent
tables, including primary-key autoindexes. `policies()` refuses an unknown global
table. Trigger-maintained row counters enforce admission; maintenance audits
counter/table correspondence. These are fail-closed ceilings, not measured capacity.

| Family | Current writer and lifetime | Bound / exhaustion / recovery |
|---|---|---|
| `q_recent_seen`, `recent_account_clock`, `recent_owner` | Acceptance adds event ID, integer owner day, integer observation microseconds, subject DID. Maintenance deletes observations before the exact rolling floor. | 100M rows; account/time and owner indexes count toward global bytes. Boundary-owner files may retain older rows that queries exclude. |
| `label_events` in global state, `q_pending`; vessel `label_events` and existing event indexes | One acceptance journal; flush copies exact ID/content to current vessel, then drains global rows. | Collector page100; generic qualification ingest page10,000. Interrupted post-commit flush recovers rather than reallocating IDs. Vessel pressure can leave accepted journal work pending. |
| `q_hot_keys` and custody indexes | Active/local replay lookup; verified source retirement calls `expire_owner`. Retained archive identity index carries archived lookup authority. | 100M hot-key row ceiling; not a second full-window lookup by design. Retirement failure retains authority and refuses progression. |
| `q_segments`, `q_archive`, `custody_archives` | Daily ownership and committed custody; expire only after verified retired owner leaves the window. | `q_segments`33, custody32; at most two non-retired local vessels. `q_archive` is owner-correlated, not independently capped. Missing/mutated custody refuses. |
| `q_transition`, `q_recent_transition` | Singleton rollover and maintenance plans. Maintenance journals exact file hashes before authority expiry/unlink. | Singleton checks; normal reads/writes refuse unresolved recent maintenance. Daily retry completes the same plan; no silent authority reconstruction. |
| `q_recent_protected` | Explicit retained-evidence dependencies. | 32 rows; a protected expired owner refuses the whole retirement transition. No automatic protection expiry. |
| `q_recent_sources` | Discovery/configuration enrollment, source cursor, bounded last-page identity/content digests, last observation, attempt token/start, scheduler state. | Sticky lifetime-global2048 sources. DID1024 bytes, endpoint2048, cursor4096, last page100 identities/digests. No automatic source retirement or eviction; endpoint replacement requires explicit cursor-reset handling. Churn can exhaust enrollment despite bounded event retention. |
| `labelers`, `provider_registry` | Accepted source tracking writes `labelers`; roster membership survives event absence. Provider registry has no new successor producer in this graph. | Lifetime-global labelers100,000; registry10,000. No invented expiry based on absent events. These are distinct from the2048 scheduled-source bound. |
| `q_recent_gaps` | Known interruption intervals plus explicit unknown acquisition coverage. | 128 stored intervals, window expiry; overflow sets persistent unknown status rather than silently claiming complete coverage. Gaps do not prove upstream completeness. |
| `q_recent_counts` | One trigger counter/ceiling per explicitly listed bounded table. | Fixed schema-derived family count; negative/count mismatch refuses. Replacements use qualified recursive-trigger semantics. |
| `meta`, `sqlite_sequence` | Fixed recent authority/frontier/generation/acquisition/scheduler/discovery keys, retained schema/control keys, monotonic ID allocation. Direct qualification ingest also writes source-specific cursor keys. | Meta4096 entries and64KiB value ceiling; direct-ingest source churn can hit this independently. IDs do not reset after physical retirement. Product frontier includes committed observation state. |
| `labeler_evidence` | Current observed-source helper writes evidence per source acceptance page, not per arbitrary mixed-source batch. | Time expiry by its recorded clock, 10M rows. Real source-homogeneous100-row density must be measured. |
| `alerts`, `labeler_probe_history`, `derived_receipts`, `ingest_outcomes`, `discovery_events`, `posted_findings`, `quarantined_events` | Classified timed compatibility tables; no obligatory successor producer/consumer established by this graph. | Each10M rows; expiry uses `TIMED`'s named timestamp column. Keep unused tables empty in successor capacity qualification; their existence does not require legacy import. |
| `derived_label_fp`, `derived_labeler_lag_7d`, `derived_labeler_reversal_7d`, `derived_labeler_boundary_load_7d`, `derived_val_dist_day`, `derived_labeler_entropy_7d`, `derived_author_day`, `derived_author_labeler_day`, `boundary_edges`, `boundary_targets` | No live successor derive/report writer or consumer. | Each10M refusal ceiling; all invalidated on maintenance. Empty-schema/index cost is included, fabricated full legacy populations are not a readiness requirement. |

Scheduler fields are bounded state, not a history log: hot0/1/2 (cold/progress/warm),
next-due clock and failures0–6. Persistent class pointers and streak allow at most
four eligible hot claims before one eligible cold claim. Warm delay1s, quiet30s,
failure backoff30–900s. Successful acceptance clears its exact pending attempt;
failed completion compares its token. A later attempt reconciles an interrupted
attempt to unknown coverage. This is process-interruption reporting, not proof of
provider execution recovery. The independently qualified service-rate examples
retain their disclosed startup backlog/latency; fairness does not imply universal
real-time service at arbitrary roster size and response time.

### Files, sidecars, catalogs and transient products

| Family | Lifetime and bound | Refusal / residual scope |
|---|---|---|
| `state.sqlite`, WAL/SHM and all global indexes | Global SQLite ceiling8GiB, reapplied on writer connections; maintenance checkpoints and VACUUMs after authority expiry. | Page cap is not an8GiB total-filesystem bound. WAL admission threshold256MiB plus a bounded transaction, SHM, VACUUM overlap and other files require separate space admission. Pinned checkpoint refuses. |
| Daily vessel SQLite, `segment_meta`, WAL/SHM | Vessel ceiling16GiB each; active plus at most one sealed/archived local queue member. Verified archive precedes unlink. | Exact source retirement under archive/writer/reader fences removes only owned validated sidecars; nonempty WAL, changed links or ownership refuses. Direct unfenced SQLite consumers are outside this ownership contract. |
| Daily Parquet, identity-index SQLite, receipt JSON, retired JSON | One bounded group per retained daily archive owner; final partial boundary retained physically. | Maintenance checks namespace, links and hashes, removes authority, prunes catalog, then exact files. Hash mismatch preserves objects and leaves recoverable refusal. |
| Reader catalog SQLite `entries`, `entries_time`, WAL/SHM | One archive-path-derived catalog on the current fixed owned archive path; entries follow retained owner membership and are pruned/checkpointed/VACUUMed. | Derived admission cache, not custody authority. Generic alternate archive roots could create more catalogs, but are not admitted by `RecentStore.archive`. No lifetime-growing historical generation manifests are produced by the current daily path. |
| `ACTIVE.json`, transition markers and receipts | Reconstructible active projection; singleton journals and per-retained-owner receipts. | Stale projection is not authority; unknown/missing custody is not repaired by inventing a receipt. |
| `writer.lock`, `reader.lock`, `archive.lock`, `daily.lock` | Stable store-wide lock domains. | No per-date archive-lock growth in v5. Never unlink lock authority merely because a waiter may exist; historical per-owner locks are not automatically enrolled or removed. |
| Archive `.incomplete`, SQLite temp/journal files and compaction copies | Finite current operation, admission before archive/VACUUM work; interrupted operation uses existing custody repair/refusal paths. | Peak includes temporary copies, filesystem blocks/inodes, WAL and concurrent tenants. Deployment scratch placement remains separately unqualified. Qualifier-only pinned scratch is not production policy. |
| Account snapshots and export cursors | Process memory: default four combined snapshots/exports,120s fixed TTL,1MiB snapshot budget, account view1000 rows, page100. Export keyset fixes upper ID and releases read lease between requests; adaptive bounded pages retain only finite cursor/page state. |64 retry/request budget applies as implemented; forward export progress is not an unlimited retained snapshot. Retirement/expiry refuses; no cursor pins old owners. Dense export completion within TTL is a measurement gate, not implied by bounded memory. No product export files are created by this API. |
| Provider query workspace | Explicit row/byte/deadline limits, relevant owner selection, DuckDB128MB/one thread/no spill. | Refuses unavailable coverage or unsupported schema; bounded response does not prove acceptable full-window latency. |
| Service logs and campaign artifacts | CLI output goes to the configured execution/logging owner; qualification produces finite run checkpoints, progress, samples and receipts. | Store retention does not bound an external journal or repeated campaign history. Deployment log policy and exact campaign closeout remain owner obligations; no background cleanup is introduced here. |

Event input bounds are16KiB per string field,64KiB serialized row and16MiB per
accepted generic batch. These limit admitted values but are not an aggregate
production shape estimate. Shared admission preserves60GiB free independently on
`/` and `/data`; local page limits do not replace that reserve or root's aggregate
allocation ownership. Daily admission is conservative; full constructed qualifier
charges extra space to the actual destination device and separately checks both
floors. Its deprecated process-global SQLite scratch pragma is isolated to the
one-shot qualification process and verified against an actual temp descriptor.

### Evidence, unknowns and formalization correspondence

The compact100k measurement found12,890,112 lookup/index bytes, approximately
128.9 bytes/event, after hot-key retirement. Extrapolation alone is not acceptance
of51.43M observations in8GiB. Small90-day qualification at source1b33724 showed
physical owner/sidecar plateau under its declared100-event/day synthetic shape;
it does not qualify current-volume capacity. V5 scheduler controls, strict
constructor differential and scratch/admission controls have separate independent
receipts. The full-window producer `c9f0144b` on archived02a3a5d is **pending**;
its original invocation is `9062cd1fea12464d9339c83ffeb56b72`. No result is asserted
here. It measures actual current supporting-state density, retained bytes,
compaction and top-account export completion; constructor throughput is not actual
collector acceptance throughput. Earlier stopped v3 scale work remains scoped
prefix evidence, not a completed12M or current-v5 capacity result.

Formalization consideration: the proposition is finite current-writer state and
recoverable authority-before-file retirement under conforming fenced readers and
writers, not arbitrary filesystem activity or provider continuity. The historical
one-event model below supplies only its original journal/custody proposition.
Current correspondence comes from explicit v5 schema/limits, trigger counter
checks, exact transition-cut controls, source/attempt CAS controls, independent
strict-identity differential, and physical plateau measurements. Practical bounded
models/tests and this source-linked inventory suffice for this readiness update;
a new model campaign would not resolve the outstanding measured-capacity gate.
Root is the independent acceptance/decision owner. Protected evidence, sticky
source churn, external logs, full-volume query deadlines and production packaging,
owned-root admission and scratch configuration remain explicit limits. This
static update creates no runtime or operational authorization.

## Historical model and earned evidence (unchanged scope)


> Historical qualification/model at `b2b9a416`, preserved in its original scope.
> The historical candidate was rejected by the [horizon decision](LABELWATCH-SEGMENTED-DECISION.md):
> mutable historical replay/quarantine lifetime and catalog custody are concrete defects.
> See [new evidence](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md).

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
