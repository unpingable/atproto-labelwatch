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
Catalog v3 extends the v2 exact cumulative per-labeler counts with a frozen
source event-ID watermark. A live row is excluded as an archived duplicate
only when both its timestamp falls in the catalog interval and its ID is at or
below that watermark. This preserves genuinely new late-arriving observations
whose authored timestamp falls inside an older archived day. Scan/rule warmup
and confidence decisions combine the catalog ledger with only live-owned rows;
the active 30-day derive window is required to begin at or after the catalog
boundary. This prevents a reduced database from silently making a historically
active but currently dormant labeler look new or sparse.

## Consumers and required semantics

| Consumer | Current raw-event dependency | Required cold semantics | Candidate seam |
|---|---|---|---|
| Public frontdoor (`frontdoor.py`, `server.py`) | All-history per-subject counts, first/last times, state tuples, loci, and top record/value histories | Exact catalog aggregates reproduce the bounded public result; live rows in the catalog interval are excluded even before physical removal | Verified local catalog loaded at startup; missing/tampered/NFS-hosted catalog refuses the all-history lookup |
| Legacy/local `whatsonme.py` and CLI | Up to 500 exact account-label events ordered by time, then effective-state reduction | The aggregate catalog cannot reproduce exact raw-event display or latest-state ordering | Keep this path on the full archive/reconstruction toolchain or add a separately content-bound account-event projection before claiming reduced-generation equivalence; do not silently return only warm rows |
| Scan census and regime (`scan.py`, runner and standalone CLI) | 24h/7d/30d counts, time sequences, target diversity, recent reversal/lag candidates; lifetime totals affect warmup | Keep the complete maximum live analysis window raw; carry exact lifetime counts and prior state explicitly | 30-day live raw floor plus catalog-v3 `labeler_totals`; both runner and `labelwatch scan` load the same catalog; runtime refuses a catalog overlapping the active 30-day window; incremental cursors never advance across absent raw input |
| Rules/findings (`rules.py`, `boundary.py`, `lifetime.py`) | Windowed counts, distinct URIs, event hashes used as evidence, ordered state transitions, threshold-only lifetime counts, and explicit historical interval studies | Evidence-bearing findings need exact source events for their declared window; threshold-only warmup/confidence uses exact cumulative counts; lifetime research needs retrievable archives, not merely totals | Keep every online rule window raw, use catalog-v3 totals only for threshold decisions, retain cited hashes/provenance, and run older/lifetime research off-host through verified archive reconstruction |
| Report/derive (`report.py`, `runner.py`) | Recent-window counts and timing, lifetime warmup totals, direct evidence-hash lookups, and persisted derived tables; report freshness uses maximum event time | Preserve every active window raw and combine catalog-v2 lifetime totals; cited cold evidence remains reconstructible but is not fetched from NFS at request time | Qualify report and derive on the reduced generation at a fixed source time; separately compare any historical/report section that still queries raw all-history rows and mark unsupported sections unavailable rather than changing their meaning |
| Incremental derived state (`state.py`, `scan.py` derived tables) | Rows after a durable event-id cursor, normally with a recent-time lower bound | No historical replay is needed after the derived checkpoint is sealed, but gaps and rebuild prerequisites must remain explicit | Seal derived checkpoint/source identity before removal; archive permits off-host rebuild when a derived table must be recreated |
| Authority/provenance research (`authority_inventory.py`, `authority_posture.py`, `authority_triage.py`, `provenance.py`, `scope_axis.py`) | Windowed distributions plus several all-history totals, active-day counts, removals, and distinct pairs | Catalog v2 preserves only the totals explicitly declared by its schema; it does not make every exploratory query online-equivalent | Run historical research against content-bound reconstruction; only promote a local aggregate after a named consumer and equivalence test bind its semantics |
| Hosting analysis (`hosting.py`) | Target-DID joins to Driftwatch identity facts, both recent and all-history variants | Aggregate counts must stay bound to the identity/facts revision used; raw joins are research work, not request-time NFS work | Materialize revision-bound hosting summaries locally or run off-host from reconstructed events and pinned facts; catalog v2 is not a substitute |
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
