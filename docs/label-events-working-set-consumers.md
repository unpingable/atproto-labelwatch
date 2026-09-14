# Label-event working-set consumer inventory

Status: design research on `experiment/labelwatch-archive-reconstruction-20260913`.
The qualified 2026-09-01..03 prototype remains evidence only. This document
does not authorize production deletion, reader deployment, or a retention
horizon change.

## Ownership invariant

A finalized cold catalog owns the exact closed interval declared by its
manifest. The online database owns rows outside that interval. Physical
overlap during qualification or rollback does not imply semantic overlap:
every hybrid query must exclude the catalog-owned interval from its live
side. Missing, gapped, or invalid catalog custody refuses an all-history
answer. NFS holds archive/recovery custody; request-time readers use only a
verified local catalog.

Commit `bf6c935` implements and tests this boundary for the frontdoor reader.
It replaces the prototype test's temporary SQL view with an enforced query
predicate, including rows immediately before and at the end boundary.

## Consumers and required semantics

| Consumer | Current raw-event dependency | Required cold semantics | Candidate seam |
|---|---|---|---|
| Public frontdoor (`frontdoor.py`, `whatsonme.py`) | All-history per-subject counts, first/last times, state tuples, loci, top record/value histories; `whatsonme` also exposes up to 500 exact account-label events and computes effective state | Aggregates alone qualify the frontdoor card, but not exact raw-event display or latest-state ordering | Local frontdoor catalog plus a separately content-bound latest/account-event projection; refuse raw-history detail when that projection is unavailable |
| Scan census and regime (`scan.py`) | 24h/7d/30d counts, time sequences, target diversity, recent reversal/lag candidates; some lifetime totals | Keep the complete maximum live analysis window raw; carry lifetime totals and prior state explicitly | 30-day live raw floor plus versioned cumulative summaries; incremental cursors must never advance across absent raw input |
| Rules/findings (`rules.py`, `boundary.py`, `lifetime.py`) | Windowed counts, distinct URIs, event hashes used as evidence, ordered state transitions, and explicit historical interval studies | Evidence-bearing findings need the exact source events for their declared window; lifetime research needs retrievable archives, not merely totals | Generate findings while their full window is live, retain cited hashes and provenance, and run older/lifetime research off-host through verified archive reconstruction |
| Report/derive (`report.py`, `runner.py`) | Recent-window counts and event timing plus direct evidence-hash lookups; report freshness uses maximum event time | Preserve every input needed by the report's declared window and preserve cited event rows for verification | Bound normal reports to the live window; resolve older evidence hashes through a local evidence index or mark unavailable—never fetch NFS during a request |
| Incremental derived state (`state.py`, `scan.py` derived tables) | Rows after a durable event-id cursor, normally with a recent-time lower bound | No historical replay is needed after the derived checkpoint is sealed, but gaps and rebuild prerequisites must remain explicit | Seal derived checkpoint/source identity before removal; archive permits off-host rebuild when a derived table must be recreated |
| Authority/provenance research (`authority_inventory.py`, `authority_posture.py`, `provenance.py`) | Windowed distributions plus several all-history totals, active-day counts, removals, and distinct pairs | Compact cumulative aggregates preserve routine totals; ad-hoc historical recomputation requires raw archive | Versioned local aggregate catalog for serving; content-bound archive reconstruction for research runs |
| Hosting analysis (`hosting.py`) | Target-DID joins to Driftwatch identity facts, both recent and all-history variants | Aggregate counts must stay bound to the identity/facts revision used; raw joins are research work, not request-time NFS work | Materialize revision-bound hosting summaries locally or run off-host from reconstructed events and pinned facts |
| Acquisition/operational probes (`load_probe.py`, CLI status) | Current size/density and maximum timestamps | Must describe the online working set separately from archived history | Report live and archived coverage as separate fields; never add their row counts without the ownership interval |
| Backup/recovery tools | Complete canonical events, stable IDs/hashes, cursor and schema | Full event reconstruction and relationship preservation | Content-bound Parquet partitions plus generation backup, manifest, empty-state restore, and application-reader proof |

## Candidate live/archive boundary

The permanent candidate should retain at least the largest production
computation window in raw SQLite. Current rules and report paths reach 30
days, so the working hypothesis is a 30-complete-day live raw floor, not the
two-day prototype interval. A final cutoff must be calculated from the frozen
source timestamp and must account for late events and UTC day closure. It is
not established until:

1. every day before the cutoff has a contiguous content-bound archive;
2. the local catalog covers that identical interval without gaps;
3. exact-event/effective-state and evidence-hash needs are either represented
   locally or explicitly routed to off-host reconstruction;
4. report/derive over the reduced generation matches the full generation;
5. the rebuilt database, catalog, WAL allowance, migration overlap, and root
   reserve fit the proposed destination.

Rows remaining physically live during reader-only qualification are excluded
from the live half by the manifest interval. This makes a reader deployment
possible before deletion without double counting, but does not itself justify
deploying it.

## Promotion evidence

Promotion from prototype to bridge-exit candidate requires a production-scale
frozen source and one uninterrupted archive interval, exact per-partition row
digests, empty-state reconstruction, complete catalog build, full/reduced
hybrid equivalence, report-plus-derive comparison, missing-archive refusal,
and measured source/candidate/catalog/migration peak allocations. Cloud-volume
throughput remains separately unqualified by crow measurements.
