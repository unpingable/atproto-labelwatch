# Labelwatch event lifetime and custody model

Model status: ACCEPTED_FOR_IMPLEMENTATION by lane owner `/root`, after operator
identity clarification and the bounded transition check recorded below. Starting source
`872693a10baabeaec36a5f98e1bf72010db39385`; lane
`lane/labelwatch-global-lifetime-custody-20261005`; existing Labelwatch #7.
No storage-engine comparison or production change.

## Current contract established by code

The three failures share a missing distinction between observing source evidence,
accepting an effect, transferring its custody, and publishing a representation.
Current code gives these different operations overlapping authority.

| Input state → operation | Durable writes / visible output | Crash ambiguity / retry |
|---|---|---|
| Raw label → normalize | Hash of labeler/src/uri/cid/val/neg/exp/sig/ts; supplied ts or ingestion-clock fallback | Same missing-ts raw label can acquire a different hash on retry. Standard source cts is not consumed here. |
| Normalized row → single-file insert | event_hash UNIQUE plus raw event and observed-source/labeler evidence; cursor helper commits internally | After commit, retry only dedupes while event row remains. Production connection uses WAL/NORMAL, process-restart durability only; power-loss survival not earned. |
| Below-floor row → quarantine | Full historical row keyed by hash/reason; repeat changes seen_count/last delivery/floor | Poll/replay can mutate current global state even when its event was already archived. No quarantine lifetime policy. |
| Accepted event → segmented flush | Global event/journal/hash and cursor committed first; vessel copy then journal drain | After global commit, exact ID/content flush is retryable; before commit not accepted. Hot key expiration by authored ts creates wrong lifetime. |
| Sealed vessel → archive | Parquet fsync/rename, verification, receipt fsync/rename, then global q_archive/status commit | Receipt can exist while source is SEALED; discovery reader includes both hot and cold. Retry resolves but intermediate query can double-count. |
| Verified archive → source retirement | Unlink source, global RETIRED commit, external retirement marker | Reconcile missing source only against committed verified custody; receipt alone must not imply authoritative ownership. |
| Single-file trim | Floor/pending commit; payload DELETE batches; late rows copied to global quarantine; final trim receipt commit | Verified archive preserves event rows, not complete current/global state. Deletion loses live hash membership; replay re-enters quarantine. |
| Archive → reconstruct | Explicit event-column import preserves id/hash into fresh DB; some tools use partial commits in incomplete file then atomic rename | History-only restore is not a complete authoritative operational restore: source cursors/global metadata are absent. Never infer production enrollment from it. |
| Iterable replay → ingestion | ingest_from_iter mutates labeler/evidence/quarantine and commits | No explicit verification/recovery/reprocessing mode; historical inspection can affect global operational state. |

Source paths: `src/labelwatch/ingest.py`, `db.py`, `retention.py`, `trim.py`,
`tools/segmented_qualification/storage.py`, `tier.py`,
`tools/label_events_cold_archive.py`, `label_events_working_set_reconstruct.py`.
Existing alert/derive receipt hashes in `src/labelwatch/receipts.py` describe those
outputs; they are not accepted-event custody records and must not be repurposed.

## Observable source identity boundary

The active polling helper calls queryLabels, which returns labels and an opaque
page cursor. Its published label definition contains src/uri/val/cts, optional
cid/neg/exp/sig. It does not supply a stable per-label occurrence sequence.
SubscribeLabels has an envelope seq, but no active subscribeLabels consumer was
found; that protocol does not silently grant the polling path a sequence.

Sources: [label definitions](https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/defs.json),
[queryLabels](https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/queryLabels.json),
[subscribeLabels](https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/label/subscribeLabels.json).
These are source-interface facts, not a claim that signatures are verified or
that an opaque cursor proves event completeness.

The operator confirmed on October 5: preserve distinct immutable source records.
Canonical source fields, including authored `cts` (legacy fixture `ts` alias),
determine a versioned record digest. Transport cursor is excluded. Identical
canonical records without a source occurrence ID are one observable record; the
system cannot distinguish two otherwise identical occurrences. A later assertion
with a different authored timestamp is a distinct record. Missing source time
refuses strict acceptance rather than manufacturing ingestion-clock identity.
Existing legacy hashes require explicit enrollment/alias reconciliation; this lane
qualifies new isolated lineages and does not silently migrate deployed identities. A timestamp correction under an explicit unchanged source occurrence
identity is not a new acceptance; inconsistent immutable content must refuse.
Without such a source token, different record contents cannot be proven to be the
same occurrence merely because some fields coincide. Ignoring all author fields
would silently merge legitimate repeated label assertions. Author timestamps may
be immutable record data; they are never identity-expiry or custody clocks.
The model must record this distinction rather than promise an unavailable ID.

## Proposed semantic states and authority

- SEEN: input observed; no acceptance, cursor advancement or global effect implied.
- VALIDATED: canonical identity/content/schema and requested replay authority are
  understood; unknown or conflicting identity refuses, not a manufactured event.
- ACCEPTED = CUSTODY_COMMITTED: one transaction commits identity binding, accepted
  effect or recoverable journal, and continuation position. This is the promise
  that retry reconstructs the same effect once. Storage allocation/final receipts
  cannot imply this before the transaction commits.
- RECEIPTED: an externally materialized projection of that committed transaction.
  Missing/uncertain publication does not revoke acceptance or permit reapplication.
- ARCHIVED: verified immutable evidence receives committed query/custody ownership.
  Discovery, conversion and receipt presence alone do not grant that ownership.
- RETIRED: local payload has been disposed only after custody and identity
  obligations have transferred. Event identity/acceptance is unchanged.

Recovery replay reconciles the same committed identities and restores current
state only under explicit authority. Verification replay is read-only against
current state. Reprocessing writes a separately attributable projection, never
another acceptance of its inputs. A history-only restore is inactive until its
identity/custody lineage and missing current state are reconciled; an older
backup must not silently roll accepted history back into absence.

## Lifetime and information requirement

A hot exact-membership cache can expire by trusted acceptance/arrival progression
only when complete durable identity information survives elsewhere. An authored
clock, TTL or physical segment retirement cannot discharge that obligation.
If arbitrary historical replay remains possible forever and inputs have no
stable ordered sequence fence, exact prior-membership information grows with
accepted identity count. A finite hot window cannot prove an old event is new.
A compact immutable identity representation can avoid retaining full events in
mutable global SQLite, but it does not make lifetime information disappear.
Unavailable/unfinished historical identity coverage must refuse or be explicitly
indeterminate; it cannot be interpreted as an empty history set.

No arbitrary retention policy or new source-sequence guarantee is admitted here.
The model acceptance record must decide how existing immutable archive custody
supplies complete identity coverage without recreating a mutable immortal event
store. This requirement applies equally to single-file and segmented storage.

## Global state classification

| Structure | Role | Required lifetime / boundary |
|---|---|---|
| Source cursor, monotonic accepted ID, active generation | Durable global continuation | Until explicit source/generation retirement; not an author-time TTL. |
| Exact hot membership cache | Reconstructible current state | Bounded acceptance window; expiry only after durable identity coverage transfer. |
| Accepted historical identity bindings | Durable identity evidence | Archive/replay obligation lifetime; compact immutable custody outside hot mutable state. |
| Current labeler/provider/classification state | Reconstructible current state plus owned discovery policy | Preserve actual current owners; this lane does not erase other state or invent their retention. |
| Labeler/probe/derive/alert histories | Historical projections | Existing owner lifecycle; verification replay must not append them to current state. |
| Quarantine candidate produced by historical verification | Replay-local temporary state | Process/qualification occurrence; no current-global mutation authority. |
| Current unknown below-floor delivery | Unaccepted observation / coverage obligation | Explicitly attributable bounded observation or immutable custody; do not confuse with already accepted replay. Current exact counters cannot be silently dropped. |
| Receipt file | Publication projection | Does not outrank committed custody identity; preserve evidence through retries. |
| Archive catalog | Committed custody projection | Per-object metadata and identity coverage; orphan artifacts remain unadmitted. |
| Archive restore temporary tables | Replay-local reconstruction | Explicit target/generation, then validated publication; no implicit current enrollment. |

## Crash classification

Before acceptance commit: NOT_ACCEPTED; prepared writes roll back or remain
unpublished. After acceptance commit: ACCEPTED_ONCE, even if receipt is absent,
partial or acknowledgment/checkpoint is stale. If the authoritative identity or
custody coverage is unavailable: INDETERMINATE; stop acceptance, recover and verify
its named lineage, then reconcile. Retry never guesses absence from an unavailable
archive or an uncertain receipt. Concurrent workers use the same uniqueness and
commit boundary, not independent acceptance ledgers.

## Operator observations

`labelwatch.custody.indeterminate`: prior acceptance identity/custody is unavailable;
replay refused until named recovery completes.
`labelwatch.custody.receipt_pending`: effect committed, receipt publication incomplete;
event remains accepted and must not be reapplied.
`labelwatch.replay.authority_refused`: historical verification/reprocessing requested
current global mutation without explicit recovery authority.
`labelwatch.archive.custody_pending`: artifact prepared but not committed to archive
ownership; it must not enter served history.
These are observer facts, not automatic actuation or a claim of live Constellation
notification enrollment.

## Formalization consideration

Record proposition: accepted identity/effect survives custody movement exactly
once; observation/receipt is not acceptance; cache expiry preserves identity
obligation; unavailable identity is not absence. Use a small bounded state model
plus actual SQLite commit/crash/substitution histories. Assumptions include honest
SQLite FULL/fsync storage and conforming writers; no physical host/NFS power-loss
proof. Identity distinguishability and exact membership information requirements
are explicit model limits, not a proof that another engine is needed.

## Implementation contract accepted before source changes

One shared opt-in custody implementation will own the hot exact identity cache,
acceptance transaction, archive ownership and receipt projection. Initialization
refuses a populated legacy DB without explicit enrollment proof. Production
initialization/configuration is unchanged; no live enrollment is authorized.
The actual single-file ingest path opts in only after fixture initialization.
The segmented adapter uses the same core and retains its existing durable journal.

Archive verification will produce an immutable exact identity SQLite index beside
Parquet. This is necessary because the existing cold summary has no exact record
membership; it is not another storage backend. The existing archive receipt gains
index hash/count metadata. The index stores record hash and accepted ordinal,
never full events or quarantine. Source membership must match before transfer.
Archive identity metadata is committed in global SQLite before receipt publication.
Readers select only committed owners; orphan receipts cannot admit history.
Missing committed evidence refuses acceptance/queries, never proves absence.

Hot keys may be removed only for owners whose verified archive identity index is
committed. Removal is owner/custody-based, never authored-time-based. Exact cold
membership may require historical index lookups for cache misses; this forcing
case is explicit. Index/catalog count grows with archive objects, immutable exact
identity bytes with event count, mutable event keys with untransferred hot owners.
Long-history lookup/admission cost is deliberately left to the named horizon gates.
An unavailable archive blocks unknown acceptance, without silently losing a
source page or advancing its cursor. Old unaccepted below-floor deliveries refuse
with an operator-visible reason rather than mutating global quarantine.

Recovery requires the declared authoritative checkpoint from a complete backup
plus its archive identities. An old backup cannot assert it is latest. A supplied
checkpoint mismatch refuses; a history-only restore remains inactive. Verification
is read-only. Reprocessing uses a separate projection target; this lane refuses
attempts to give it normal acceptance authority. Existing current quarantine is
preserved, but replay never appends/clears it.

Acceptance and cursor changes share one SQLite FULL transaction. Event publication
is a deterministic projection after commit; retries return the same accepted ID.
SQLite uniqueness/BEGIN IMMEDIATE serialize duplicate workers. Full events may
move through an idempotent journal into storage vessels; movement never grants a
second acceptance. Archive publication and archive owner commit have separate
crash points, and committed publication can be reconstructed exactly on retry.

The bounded model covers two record identities, two owners, receipt/no receipt,
hot/cold membership and every allowed transition/crash prefix. It proves only
conservation under its transition relation. Actual process-death tests must check
implementation correspondence. Physical storage failure, unverified existing
legacy identity conversion, and older-backup completeness remain explicit refusal
boundaries, not earned universal restoration guarantees.

## Earned implementation correspondence

Source `1fdfd028` implements this opt-in contract. Independent acceptance record
`0e64d667-20c4-4b91-874a-32bde31ada33` accepts the bounded shared contract,
not production enrollment or macro capacity. First source `c8c9e6b` is preserved
but not accepted: unpublished transaction visibility, overlapping archive
ownership and JSON checkpoint representation each falsified its correspondence.
Practical tests plus the bounded model are sufficient for this lane's stated
properties; no whole-service or physical power-loss theorem is claimed.

Canonical fields are ver/src/uri/cid/val/neg/cts/exp/sig, with normal optional-field
canonicalization and a versioned digest. Legacy fixture ts is an explicit authored
time alias; conflicting aliases refuse. Source time distinguishes records but
never governs identity expiry. Current acceptance time is cache metadata only;
owner closure and verified custody govern retirement. Accepted ordinal plus
lineage is continuation metadata; the canonical record digest is semantic identity.

Custody ownership is exclusive per identity. A first archive index must exactly
prove the selected owner's accepted bindings; an unscoped snapshot mixing owners
refuses. A closed owner cannot accept a fresh record. Repeated publication carries
unique semantic acceptance IDs, never an increment instruction. Consumers correlate
identity/lineage and do not infer new acceptance from repeated receipt delivery.
No success publication can run inside an open staging transaction. Archive
publication absent after commit is pending evidence, not lost acceptance.

A history-only restore preserves its original evidence and remains inactive.
A complete operational backup must be reconciled with a declared authoritative
checkpoint, including archive identity coverage and transport positions.
Recovery replay validates committed identities; reconstruction writes an explicit
inactive target using existing tools. It does not enroll that target as production.
Reprocessing also requires a separately owned target; implicit current mutation
is refused. Missing authoritative state is INDETERMINATE, requiring recovery of
that named checkpoint/coverage or explicit operator retirement of the lineage;
it is never guessed clear.

## Complete current global-table inventory

| Actual structures | Classification / lifetime / continuation dependency |
|---|---|
| meta ingest_cursor:* / observed_at / advanced_at; custody lineage/generation/active owner | Hot-global continuation; explicit source/lineage retirement only; cursor is transport, not identity |
| meta discovery/derive/report/publication/retention/floor/pending/watermark keys | Owned subsystem checkpoints; do not expire with event payloads; history-only reconstruction cannot recreate them |
| sqlite_sequence for label_events | Global accepted ordinal allocation; survives event payload deletion; incomplete restore cannot claim its high-water mark |
| q_hot_keys | Exact current identity/effect binding; hot owner until complete verified identity custody transfer |
| custody_archives | Global committed owner/index/receipt authority, O(archive objects); archive/replay lifetime |
| q_archive / q_segments | Layout/custody projections and bounded local retry ring, never independent acceptance authority |
| q_pending / label_events in segmented global DB | Recoverable current effect journal; drain only after exact durable vessel copy; not an indefinite history store |
| q_transition / active vessel metadata / ACTIVE.json | Rollover intent and projection; reconcile before accepting into exactly one target |
| label_events in single-file DB / event vessels | Accepted payload; hot lifecycle only; retirement does not retire semantic identity |
| quarantined_events | Existing current observation/historical gap evidence; no replay mutation authority; existing full-row lifetime not silently changed |
| labelers / provider_registry | Current source/classification/provider state; source lifecycle, not author-time TTL |
| labeler_evidence / labeler_probe_history / discovery_events | Historical observations/projections; existing owner policies, no complete new bound claimed; all-table horizon audit required |
| alerts / derived_receipts / posted_findings | Historical output/publication identity; owning subsystem replay/publication obligations; not accepted-event dedupe |
| derived_label_fp | Reconstructible derived state tied to derive checkpoints; no event-acceptance authority |
| derived_val_dist_day / derived_author_day / derived_author_labeler_day | Reconstructible day projections; existing derived horizon policies; verification cannot rewrite them |
| derived_labeler_lag_7d / reversal_7d / boundary_load_7d / entropy_7d | Reconstructible bounded-window projections; derive-owner lifetime; not collector acceptance proof |
| boundary_edges / boundary_targets | Reconstructible boundary projections, existing owner prune policies; not archive identity |
| ingest_outcomes | Operational observation history, existing seven-day pruning; success collection is not receipt custody |

No current replay-local full record is allowed into those global structures.
No existing unique quarantine/history is deleted by this lane. Full projected-table
capacity/lifetimes remain explicitly measured in the next global-state horizon gate.

## Direct Constellation observation contract

| Machine condition | Fact / operator message / reconciliation |
|---|---|
| labelwatch.custody.indeterminate | Authoritative checkpoint or committed identity evidence missing/altered: “Labelwatch prior event acceptance cannot be proved; ingestion/replay refused.” Recover the named lineage/coverage; never clear from receipt absence. |
| labelwatch.custody.receipt_pending | Committed effect/owner lacks materialized receipt: “Labelwatch custody committed; receipt publication is incomplete. The event must not be applied again.” Re-publish the same committed projection. |
| labelwatch.custody.receipt_conflict | Existing publication differs from committed projection: “Labelwatch receipt identity conflicts; original evidence preserved.” Operator reconciles exact source/custody identities. |
| labelwatch.replay.authority_refused | Verification/reprocessing requests current mutation, or recovery lacks authoritative checkpoint: “Labelwatch historical replay lacks current-state authority; replay refused.” Select an explicit inactive target or valid recovery checkpoint. |
| labelwatch.replay.identity_unavailable | Unknown below-floor delivery: “Labelwatch historical record has no provable prior acceptance; source cursor was not advanced.” Recover coverage or explicitly reconcile the source page. |
| labelwatch.archive.custody_pending | Prepared artifact/receipt has no committed owner: “Labelwatch archive is prepared but unadmitted; served history remains with its committed owner.” Finish verified custody commit or preserve orphan evidence. |
| labelwatch.archive.owner_conflict | Overlap / sealed-owner reuse: “Labelwatch archive contains identities owned elsewhere; custody transfer refused.” Scope the object to its proven owner; never double-count. |

Observation remains read-only. A stale observation stays UNKNOWN; absence does not
mean repair. These are domain fact/operator contracts and qualification evidence,
not a claim of deployed Constellation sensing/notification. Preserve failed and
superseding observations. Production integration belongs to the implementation/
monitoring owner, retaining incident #6 / Monitor #18 separation.
