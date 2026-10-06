# Labelwatch bounded recent-observation product — existing #7

## Approved product contract — October 6, 2026

Labelwatch is a bounded recent-observation service, not an indefinite-history
custody service. This owner-approved contract governs existing #7 and supersedes
conflicting prospective requirements in the historical record below. Existing
archives, sealed receipts, qualified custody work and earned evidence remain
preserved; this amendment authorizes no historical evidence deletion.

1. **One service window.** Select exactly N days, 30 <= N <= 45, from measured
   capacity and operational margin. Record the choice and admission evidence;
   do not choose N merely because a prior fixture used40days. A shorter UI
   default and a smaller indexed hot tier may coexist with the one exact
   query/export window. Define window boundaries, observation versus source
   timestamps, late arrivals and clock behavior explicitly. Outside-window
   requests receive an explicit out-of-window response, not apparent empty
   completeness. Gaps remain visible; never synthesize observation continuity.
   Window first, optimization second: N is the guaranteed service window subject
   to disclosed observation gaps, not a promise that every source was observed.
   Longer history may be explicitly best-effort only within a separate measured
   resource cap; its presence never expands the guarantee or delays retirement.
   A future larger guaranteed window is an earned capacity improvement requiring
   qualification, explicit publication and the applicable semantic-change gate.
   No90day implementation or speculative expansion is admitted now.
2. **Account-centered product.** Deliver summary, timeline, filters, attribution,
   coverage gaps and Export this period. Retain the raw event/table view as a
   secondary expert surface. Summary counts and timelines must share the same
   coverage and window semantics as exports. Observed application/removal events
   are distinct from qualified current state. Absence of an observed removal
   does not establish still active; older state needs qualified evidence or an
   explicit unknown. No indefinite effective-state ledger is implied.
3. **Bounded public export.** Expose intentionally public observations within N
   days, never internal probes, quarantine, operational evidence or arbitrary
   tables. Bind every export to a consistent snapshot/frontier across pages,
   requested interval, actual coverage/gaps, schema/version and content identity.
   Define completeness/count/terminal semantics and unambiguous expiry/refusal.
   Use bounded lifetimes, concurrency, bytes and retry/resource budgets. An
   export cannot extend its generation lease indefinitely; slow/expired exports
   terminate explicitly without silently switching snapshots. Expiration and
   physical retirement must be coordinated. No indefinite download hosting.
4. **Finite production obligation.** Preserve historical archives already in
   custody. Future indefinite archival is not a production requirement, nor a
   prerequisite for expiring future observations under an accepted lifecycle.
   User exports transfer retention responsibility; production need not track
   their perpetual custody. Optional older aggregates must justify their
   semantics and explicit lifetime/capacity bound. A separate research archive
   is outside this work unless required and separately scoped.
5. **All-state bounds and physical retirement.** Inventory every persistent
   state family, including indexes, cursors/replay authority, identities, gaps,
   catalog metadata, receipts, logs, caches, summaries and export leases/jobs.
   Assign a measured lifetime/cardinality/byte rule and exhaustion behavior.
   Finite days with unbounded ingest or identities is not finite space. Prove
   repeated whole-unit/generation retirement returns filesystem blocks, with
   descriptor/link/dependency checks. Bound steady-state and peak WAL,
   checkpoint, export, compaction, query spill, interruption/recovery and
   overlapping source/successor/rollback generations per filesystem. Preserve
   admitted reserves and other tenants. Reuse existing candidate/custody work;
   no new engine comparison absent a concrete hard requirement.

## Execution and acceptance boundary

The production safe hold remains in force. This message supplies explicit
operator return and conditional continuation authority; it does not establish
capacity or recovery readiness. Existing hold restart prerequisites are NOT_MET:
last measured root31,438,950,400bytes is below32GiB; full maintenance/recovery
peaks are unqualified. No new producer, migration, public service restart or
retention-policy activation is admitted by this documentation amendment.

Once documented prerequisites are satisfied, the integration owner continues
existing #7 through implementation, independent qualification, migration
planning and production readiness without another routine permission request.
Do not remove hold guards to obtain a stability receipt. If prerequisites cannot
be met within existing authority, retain the hold and report the specific
boundary; do not start another capacity/architecture lane. The previous
production-stability receipt remains an execution dependency, not something this
product decision claims to have earned.

One integration/storage owner coordinates bounded executors/reviewers and
independent acceptance. Reconcile sealed producer04166ec3 and its checkpoint;
never restart it. Reuse earned41-case custody and12M physical-reclamation
results within their stated scope. Historical7-hot/40-history and47-day fixtures
are evidence, not a mandatory new product window. Recompute only changed or
missing qualification for measured N and its workload. Catalog repair independent
acceptance, query scale and all-state resource proof remain unfinished.

Escalate only material product-semantic changes, recurring cost/new paid
capacity, destructive retirement of retained historical evidence outside an
accepted lifecycle, or materially different production risk/availability.
Existing archives do not become disposable because they exceed N. Migration
plans must preserve exact rollback/recovery boundaries and disclose limits after
successor writes; production readiness is not a migration receipt.

## Formalization consideration

Decision owner: existing #7 integration owner. Source baseline444d48a; this
amendment changes requirements only. Durable propositions are bounded physical
space under an admitted workload, snapshot consistency through concurrent
retirement/expiry, and evidence-qualified claims across gaps. No new proof is
claimed. On execution, prefer a small bounded state-transition model for export
leases/retirement and interruption; bind assumptions to implementation and
independent negative qualification cases. Practical prose/source review suffices
for this requirements amendment. Tests/models must cover expired cursors,
concurrent ingest, missing coverage, interrupted generation replacement, and
non-extensible export lifetime; models alone do not establish measured capacity.

<details>
<summary>Historical decisions and qualification requirements — superseded where conflicting above</summary>

# Labelwatch segmented-storage decision — parked continuation

**PENDING — CONDITIONALLY AUTHORIZED AFTER PRODUCTION STABILITY.**
No producer has resumed and no adoption verdict is earned.

## October 6 execution amendment — existing #7

The owner forbids every recurring-cost increase and a fresh architecture
campaign. The existing incident owner first evaluates safe reallocation within
the already attached 200 GiB Zone volume. Existing #7 is then the permanent
capacity solution to qualify, not optional optimization. This is a continuation
of the same candidate and evidence; it does not select a new engine or authorize
production migration. The prior pause is replaced by **conditional resume**,
not an instruction to launch while production is unstable.

Before any successor producer, the integration owner must name a reviewed
production-stability receipt: admitted root/Zone capacity including concurrent
maintenance and expected WAL/checkpoint demand, useful collector/API progress,
fresh public aggregate publication, no unresolved active/pending transition,
and an exact recovery boundary. The receipt must give observation interval,
resource minima, workload, service/release identities and current monitoring
that detects renewed exhaustion during qualification. A process being active,
a paid allocation, or source-test success is not that receipt. Full incident
closure still separately requires actual retention recovery, fresh public facts,
and the missed obligations enrolled and demonstrated under Monitor #18.

No new generic approval is required once this condition and existing storage
admission hold. Read the sealed catalog producer/checkpoint first; never restart
producer 04166ec3. Use a fresh unique successor run under the existing campaign,
one integration/storage owner, serialized large runtime, and independent review.

## Amended acceptance gates

These extend the existing three gates below; they are not another research
program. A successful 47-day fixture alone does not prove lifetime bounds.

1. **All-state lifetime.** Inventory every actual production table, index,
   file/queue and persistent state family, and its successor representation.
   Reconcile the existing 21-table counts against the exact schema; do not treat
   sampled first/tail lengths or currently empty tables as growth bounds.
   Include alerts, probes, receipts, outcomes, discovery/boundary history,
   quarantine, rollups, cursors, identity/replay authority, catalogs and logs.
   For each family name its owner, semantic/replay obligation, location,
   expiration or segment-retirement rule, refusal/recovery behavior and a
   conservative byte bound. Demonstrate physical block release for retired
   local data with live descriptor/link checks; SQLite freelist reuse alone
   does not count as filesystem reclamation. Off-host custody must be committed
   and recoverable before local retirement. No silent loss of required history.
2. **Explicit workload envelope.** Bind measured sustained/peak ingest and
   bytes/event, active source/identity cardinality, update/delete amplification,
   lateness/replay rules, maximum admitted archive-unavailable duration, query
   concurrency and required full-40-day query families. Give numeric limits and
   an observation basis before allocating a specimen. Specify observable
   refusal/backpressure and recovery at each limit without silently redefining
   completeness. Retaining a finite number of days at an unbounded rate is not
   a storage bound. Reuse production scalar/aggregate profiles and the immutable
   12M specimen; do not export raw production records or build a new corpus
   merely to restate established evidence.
3. **Catalog growth and historical projections.** Qualify indexed candidate
   admission and targeted interruption/range/orphan/missing/corrupt-entry
   recovery, including independent acceptance of the repaired successor.
   Separately account for all retained owner/receipt/index bytes and full-audit
   and rebuild costs. The existing metadata index grows with segment count;
   zero historical payload hashes does not establish bounded metadata storage.
   State and enforce a local metadata/projection cap, or identify an explicit
   lifetime-global exception with bytes/unit, growth rate, admitted operating
   horizon, numeric budget and owner acceptance. An exception is not a strict
   history-independent claim. Derived caches remain rebuildable and bounded;
   canonical custody/completeness authority cannot be evicted as if it were a
   cache. Preserve missing-history refusal and exact recovery.
4. **Steady-state and peak space.** Derive the cloud-local bound from the
   admitted envelope and each state family's rule, then verify repeated
   rollover and recovery transitions with current-volume measurements.
   Account per filesystem for live/hot data, WAL/checkpoint headroom, sealed
   local segments, archive outage backlog, catalogs/projections, query spill,
   rebuild space, staging, and simultaneous predecessor/successor/rollback
   copies. Measure peak allocated blocks, inodes and minimum available capacity
   across seal/archive/retire/query/restart/interruption, not just final sizes.
   Exact existing root/Zone reserves and aggregate shared-host budgets remain;
   another filesystem's free space is not a substitute. No recurring-cost
   increase, provider allocation, hidden other-tenant consumption or lowering
   a reserve is a passing result. Explicitly account for replay/readmission of
   old data so it cannot silently recreate unbounded mutable state.
5. **Physical and data-model compaction.** After required historical state is
   durably sealed under the existing custody contract, demonstrate that retiring
   whole segments or replacing a bounded hot generation actually returns local
   filesystem blocks to the admitted bound. Reuse the existing mechanism;
   select a rebuild only if needed for its residual mutable tables/indexes.
   Do not mandate VACUUM, a new engine or another architecture campaign.
   Measure separately logical retained rows/bytes (with an explicit encoding),
   database/index logical sizes, physical allocated blocks, free space and
   open/unlinked files before seal, at peak coexistence and after retirement.
   A low row count, compressed archive or SQLite freelist is not physical
   reclamation. Report observed compression/index-amplification ratios on the
   retained production-shaped specimen; do not assume a compression ratio.

   Exact archived events and canonical custody/replay authority remain available
   for their required semantics. Smaller summaries/projections may answer only
   the queries they preserve; they cannot silently replace exact history for
   queries, replay, late arrivals, deletes, counts or provenance that need it.
   Qualify per-family conversion preconditions and any refusal/loss conditions.
   Preserve the existing 7-hot/40-historical qualification arrangement and
   required query coverage unless measured consumer requirements justify an
   explicit scope change; a full-40-day query requirement does not by itself
   require 40 days of fully indexed local hot storage.

   Generation replacement, if needed, must fence writers/readers, bind a
   consistent source snapshot and cursor/authority frontier, verify candidate
   completeness and schema/query behavior, atomically select the successor,
   and test interrupted copy/selection/retirement and recovery. Old-generation
   retirement requires accepted custody and named rollback-dependency discharge,
   including mount/hard-link/open-descriptor checks. After the successor accepts
   writes, selecting the old copy is not a lossless rollback; recovery must
   account for those writes. Include duplication and conversion workspace in
   the peak-space gate, with no cost increase or reserve reduction.

The deliverable is a reviewed state-family ledger, numeric workload/resource
contract, source-corresponding lifetime/capacity argument and retained executable
evidence. A strict history-independent cloud-local claim requires all local terms
to be bounded under the stated envelope; otherwise report the exact limited
operating horizon and unresolved exception. No permanent-capacity acceptance is
earned by merely moving an unbounded database to the existing volume.

Retain the earned shared custody, 41 affected cases, exact coverage regression
and 12M rollover/reclamation evidence. Rerun only materially affected properties;
complete the remaining 47-day all-global capacity, full-40-day queries and
catalog controls under these amended gates. Independent acceptance must name
exact source/results and limits. ADOPT only when all applicable gates pass;
implementation defects are repaired within this candidate. Production migration
still requires a separate concrete cutover/recovery receipt.

### Formalization consideration for the amendment

Decision owner: incident/#7 integration owner, with independent gate review.
Source base: bf7ffd2b4a8596deac1a763c4f4992e987f7249c; executed catalog source
97b459d1f2cd9e8dbdc0422fafbeb53c41378e9a; shared custody 6a88178.
The proposition is conditional bounded cloud-local allocation across admitted
ingest, archive outage, compaction and recovery, preserving committed custody
and coverage.
Reuse the earned custody model. Add a small source-corresponding lifetime and
capacity argument over state families and phases, with finite transition tests
and measured byte maxima; a new model framework is unnecessary. If an exact
bound cannot be established, refuse that claim instead of extrapolating a short
run. Counterexamples include lifetime owner-index growth, repeated archived replay,
unbounded outage backlog, lossy summary substitution, stale-generation rollback
after successor writes, and overlapping rollback/query/rebuild allocation.
The amended requirements themselves do not establish any of these properties.

## Preserved October 6 parked evidence

Lane `lane/labelwatch-segmented-horizon-capacity-20261005`, existing
[Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7).
Root run `39657c30-4728-4598-9f48-262bbf154a63`.
Starting source `cae2d4b490a8ad852accbed21f33997e4b82841c`;
repaired/exercised source `97b459d1f2cd9e8dbdc0422fafbeb53c41378e9a`.

The prior catalog finding was a repairable implementation defect. Its
architecture-level rejection is superseded as the current execution frontier;
the exact counterexample and prior records remain evidence in Git at `cae2d4b`.
No engine comparison is reopened. Shared custody remains qualified at `6a88178`.

| Gate | Current evidence | Remaining |
|---|---|---|
| 40+7-day all-global capacity | Complete21-table production scalar counts and aggregate shape profiles acquired read-only; macro specimen NOT_CREATED | Construct current-volume specimen and measure actual resource/growth bounds |
| Full40-day current-volume queries | Prior accepted one-week/coverage evidence retained | Execute required complete-horizon query families |
| Long-history catalog admission | Producer PASS at8192 existing /8193 total real committed archive identities: one candidate hash; zero duplicate/startup historical hashes | Targeted interruption/range/orphan/corrupt-entry controls and independent review |

All41 affected custody cases and exact historical-coverage regression passed.
The large horizon producer has not been launched. The operator requested a safe
pause; no evidence-unavailable decision is asserted. No adoption is earned yet.

Resume from `portfolio-private/ATPROTO-RESUME.md`, which points to immutable
`CHECKPOINT-PARKED-39657c30-4728-4598-9f48-262bbf154a63.json` in the existing
private horizon campaign. Do not restart the completed catalog producer.

After explicit resume, finish only these gates and independent acceptance.
Emit ADOPT if all pass. Repair implementation defects within this candidate;
reject architecture only for a required property that cannot be satisfied
without materially changing it. No extra confidence-seeking campaign.
Production mutations **NONE**; no migration, activation or destructive cleanup.

</details>
