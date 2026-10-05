# Labelwatch storage decision — October 5

**NO_DECISION — MORE EVIDENCE REQUIRED**

Physical period retirement is supported by executable evidence. A production
architecture change is not yet earned. Keep immediate repair in its incident
lane; this decision does not justify leaving current retention broken.

| Criterion, in priority order | Repaired single SQLite | Segmented hot SQLite + Parquet/DuckDB | PostgreSQL time partitions |
|---|---|---|---|
| Accepted data/cursor correctness | existing one-DB semantics; source repair qualified, undeployed | arrival-vessel event/cursor subset and process cuts pass; full mutable state/archive-only dedupe open | one-DB transaction support; full-week value restore passes; global ID/hash mapping and service integration open |
| Boring retention | still row/index DELETE and catalog replacement | seven-file retirement 0.54s after custody proof, 2.63GB reclaimed; large fresh conversion not timed | detach 0.02s, drop 0.23s after verified backup; detach itself reclaims zero |
| Crash recovery | current archive/pending-floor/batched-resume protocol | bounded finite predecessor/successor handoff passes; metadata custody and complete composition open | native transaction/engine recovery plus external archive receipt protocol; physical host loss not qualified |
| Capacity bound | 32GiB guard is reserve policy; cold catalog still history-sized | can remove history-sized local catalog if readers earn it; spill/projection budget and current-volume bounds open | partition payload/WAL/dump capacity plus owned service; WAL peak not measured |
| Operator legibility | direct failed occurrence/horizon obligation missing | direct vessel/custody/floor facts available to model; not enrolled | same direct partition/custody/deadline facts; not enrolled |
| Archive/restore | current content-bound event export plus catalog/state dependencies | current Parquet reuse is real; event rows alone are not full operational restore | complete weekly dump/restore exact values/IDs; production backup owner absent |
| Queries | known existing contracts and dense-history gap | six sampled shapes match; hot union has 2.25s setup tax; dense/full-volume and mixed-generation reads unearned | sampled shapes match; corrected equal-index measurement retained separately; dense/full-volume unearned |
| Maintenance/ownership | substantial custom export/catalog/trim/recovery | can delete some existing machinery, but rollover/state/dedupe/reader machinery could replace it | outsources row retirement and same-engine atomicity; introduces daemon/roles/upgrades/WAL/backup ownership |
| Performance | exact trim 191.5s; zero immediate filesystem reclamation; matched ingest timings do not establish production throughput | real historical-week ingest measured; not current maximum workload | real historical-week ingest/dump/restore measured; index-complete supplement separate |

The current live read found **9,764,010 events in September 28–October 4** and
44,315,507 live rows. The missed August partition and executable complete week
are about 3M rows. That difference invalidates treating 3M/week as current
capacity or worst-case admission evidence. The 90-day comparison is stratified,
not full-volume. No candidate should win from those limited timings.

The segmented/Parquet/DuckDB design is the lead hypothesis because whole-unit
reclamation removes the current page-lifecycle problem. PostgreSQL remains a
credible comparator, especially if same-engine state/cursor guarantees avoid
new custom cross-file machinery. SQLite itself has not been convicted: the
missed occurrence was an admission/control-flow failure, and SQLite DELETE
behaved as specified. Direct archive readers and physical period boundaries
address different parts of the lifecycle; their composition must be earned.

Required decision gates, in this order:

1. Finish the current incident's admitted repair and direct obligation visibility;
   this is independent of adoption. Do not weaken existing reserves or migrate.
2. Qualify segmented arrival-vessel handoff for the actual mutable Labelwatch
   state, cursor advancement, canonical duplicate handling across archived/live
   coverage, complete metadata/event restore, and bounded sealed-vessel backlog. Compare the equivalent PG
   global ID/hash/sequence rules and NOT NULL/default refusal parity. Include concurrent writers and physical-host
   interruption correspondence, not merely process-exit fixtures.
3. Bind full-volume current workload and dense Q8/lifetime/reversal queries,
   active + cold reads, bounded spill/projection costs and schema-generation
   vectors. Preserve unknown/unsupported reader refusal. Do not restore derive
   or scheduled reporting to gather this evidence.
4. Compare actual owned service/recovery machinery after those semantic gates;
   select one design only when it deletes more durable complexity than it adds.

No migration sketch is emitted: the prerequisite architecture-change
recommendation is absent. Incremental migration feasibility and rollback
requirements are recorded in LABELWATCH-STORAGE-ARCHITECTURE.md, without a
production cutover or policy change.

Evidence and commands: [architecture](LABELWATCH-STORAGE-ARCHITECTURE.md),
[prototype](tools/storage_spike/README.md), [campaign closeout](CAMPAIGN-REPORT.md).
Independent acceptance remains pending; a completion notice is not acceptance.


| Acceptance question | Answer earned here |
|---|---|
| SQLite engine or single-file lifecycle? | Engine failure not demonstrated. Admission ordering failed; page reuse and history-sized catalog lifecycle remain costs. |
| Whole-unit retirement possible? | Yes: seven verified files retired by seven unlinks. PostgreSQL detach/drop also works after verified backup. |
| History-independent scratch bound? | Plausible if active/queued vessels, conversion and spill are bounded and cold catalog rebuilding leaves the path; complete bound not proved. |
| Real queries under segmentation? | Six sampled shapes match; hot setup costs 2.25s, dense/full-current-volume reads remain open. |
| Does PostgreSQL remove more machinery than it adds? | Undetermined until full service state/identity and owned restore integration are compared. |
| Easiest correct recovery? | No winner: finite archive-first recovery passes; full backend composition and host loss remain open. |
| Clearest Constellation invariants? | Physical periods expose direct custody/deadline facts; current SQLite can also emit failed-occurrence/floor facts now. |
| Incremental migration possible? | Credible bounded watermark/dual-read designs exist; neither handoff nor rollback is qualified. |
| Existing code becomes deletable? | Candidate row DELETE/compaction and history-sized catalog paths only after accepted replacement; none is presently authorized for removal. |
