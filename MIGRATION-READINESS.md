# Migration readiness

Status: **NOT READY FOR PRODUCTION MIGRATION**. This document records gates for
the existing issue #7 candidate; it authorizes no production read, deployment,
retirement, new engine or changed production reserve. Root owns integration and
admission; the qualification lane owns independent review.

The previous catalog repair, global lifetime custody repair, 12M replay and
production-shaped sample receipts support only their named source revisions and
scopes. A new 30-day contract deliberately changes answer reach. It requires an
explicit inventory of current consumers: frontdoor subject queries, public
aggregate facts, report/derive computations, CLI history, exports and retained
replay obligations. A dated document without a current consumer is not a
compatibility requirement. Conversely, a named current consumer cannot receive
a window-only answer labeled complete history.

| Gate | Required evidence | Present status |
| --- | --- | --- |
| Semantic contract | Exact window clock/boundary, future/late/replay rules, unknown/missing coverage, negative-claim standing | Candidate work; independently review integrated revision |
| Physical model | Raw columns/dictionaries/indexes/views mapped to every supported query and reconstruction need | Candidate work; no capacity acceptance |
| Workload | Retained production aggregate counts/shapes plus 12M specimen, explicit synthetic distribution | Available with sampling limitations |
| Capacity | Measured integrated payload/global/index/WAL/scratch/overlap peaks, independent host reserves, refusal behavior | New 30-day current-volume run NOT_RUN |
| Product views | Fresh computation identities; disabled/stale derivation cannot become calm or zero; bounded export completeness | Candidate work; qualification pending |
| Migration conversion | Exact source snapshot/cursor/generation; deterministic mapping; row/count/digest parity within promised scope; deliberate omissions enumerated | Not qualified |
| Cutover | External acceptance owner; writer fencing/quiescence; successor validation; atomic selection; reader generation handling | Not qualified |
| Recovery | Before-commit refusal/rollback; after-effect recovery boundaries; old generation/authority not silently revived | Not qualified |
| Custody | Exact receipt and replay dependencies retained; physical deletion only after independent verification | No new deletion authorized |
| Operations | Failed/overdue admitted maintenance and publication visible; intentionally disabled jobs distinct; owner/cadence | Separate live incident remains open |

Migration must preserve stable logical source/cursor identity separately from its
implementation generation. Freeze or reconcile source writes through an admitted
consistent snapshot, qualify conversion offline, verify output with independent
queries and manifest identities, then admit a separately reviewed cutover. Old
source remains required rollback evidence until the acceptance boundary explicitly
retires it. A retained old file is not proof that reversal after new externally
visible effects is safe. Downgrade needs its own scope and loss/refusal rules.

Initial conversion peak must include old live data, new bounded generation,
indexes/views, scratch, reader leases and any rollback generation simultaneously.
Daily steady-state compactness does not qualify this migration peak. Shared
200 GiB layout feasibility and root/Zone allocation changes require their own
exact admitted plan; no extrapolated reclaim or directory rename counts as free
space on another filesystem.

The smallest next evidence is integrated finite semantic/product controls, then
measured reuse of the existing 12M specimen under fresh storage admission. Full
30-day capacity and migration rehearsal follow only if those controls pass and
root admits the measured envelope. Do not rerun retired 47-day/40-day criteria as
though they remained the selected contract; retain their counterexamples and
label any supersession with the new decision identity.

Formalization consideration: state transfer preserves exactly the declared
windowed contract and refuses missing authority/custody. A small transition model
can bound prepare/validate/commit/refuse/retire under explicit assumptions; tests
must connect that model to source and failure cuts. Native SQLite layouts are not
an implicit migration schema. Decision owner: independent acceptance owner. This
document contains a proposed gate inventory, not acceptance or a proof.

## Trusted observation clock and warm start

Legacy `label_events.ts` is source-authored time. Its schema does not establish a
trusted per-event acquisition timestamp. Do not relabel `ts`, file mtime,
publication time, or the migration run time as historical `observed_at`.
A legacy import is admissible only where a separately qualified source binds
that exact event identity to a trusted observation time; prove correspondence,
clock scope and missing-record treatment. No such legacy mapping is established
by the existing sampled shape/capacity receipts.

A new empty bounded generation is a distinct candidate migration path: retain
all legacy evidence in its existing custody, begin assigning trusted observation
time to newly accepted events, and declare acquisition start and the preceding
unobserved interval explicitly. A computed 30-day retention floor does not mean
30 days were observed. During warmup, queries before acquisition start must
refuse or return a specifically disclosed gap; an empty in-window result means
only no recorded events under the declared observation coverage, never no labels
or a clean/current label state. First-seen and source-time fields remain separate.
Exports and HTML must share this same acquisition frontier and gap statement.

Qualify: empty startup queried over the full nominal window; first accepted event
at the exact acquisition boundary; gaps during stopped collection; restart
preserving acquisition identity; missing/unknown acquisition metadata refusal;
and expiry that advances retention without rewriting historical acquisition.
The current provider's generic `coverage=unknown` is truthful but does not by
itself establish an explicit warmup interval or a ready migration. Initial
allocation can be far smaller than reindexing the legacy database, but sustained
capacity, old-evidence custody and the actual cutover still require acceptance.

## Held capacity evidence and exact admission inequalities

This section uses held evidence only; it is not a current host sample or a
production layout decision. The October 6 15:18:10 UTC operator-hold receipt
`labelwatch-retention-api-recovery-20261005/safe-hold-20261006/HOLD-RECEIPT.json`
and its `final-verification.txt`, after the writer stop, record root free
**31,438,950,400 B** and Zone free **50,748,563,456 B**.
Root is **2,920,787,968 B below 32 GiB** (34,359,738,368 B). Zone exceeds the
existing 47 GiB gate (50,465,865,728 B) by only **282,697,728 B**. These retained
values supersede rounded “31.4/50.7 GB” arithmetic for this example, not a later
actual capacity observation. Concurrent host allocations remain unassigned.

Let F_f be freshly measured available bytes on filesystem f, R_f its applicable
reserve, C_f independently accepted physical reclamation already completed,
A_f(t) aggregate additional allocation by this migration and concurrent admitted
work, and U_f(t) the explicitly bounded growth of retained producers. Then:

`min_t [F_f + C_f - A_f(t) - U_f(t)] >= R_f`

must hold independently for root, Zone, crow root/data and any archive destination.
Free inodes and path/mount suitability are separate gates. Where future tenant
growth is unknown, record the uncertainty and serialize or obtain its envelope;
do not assign it zero by omission. F already reflects the held old database:
**do not subtract its size twice, and do not add its prospective retirement**.
C is zero until exact authority/dependency review, offhost custody, independent
readable recovery, accepted retirement and measured physical release are complete.
No old database, catalog, Driftwatch staging or evidence deletion is authorized
by this design.

For successor steady state, decompose A into measured bounded global/lookup state,
active hot payload and indexes, retained compressed segments and membership
indexes, WAL/journal, materialized public snapshots, conversion/sort scratch, and
maximum reader/rollback overlap. Daily retirement does not make simultaneous
active/sealed/archived generations disappear from this sum. The root-owned
allocation coordinator must bound concurrency explicitly. The 100k index slope
and subsequent compact candidate are measurements/estimates, not a chosen
production state cap or permission to consume the reserve.

For initial migration, use the **peak new** successor allocation plus conversion
scratch, any new rollback copy, publication overlap and old-producer growth while
the held old generation remains. Reusing an existing retained old generation for
rollback does not allocate another full copy; actually making a new copy does.
A warm start avoids copying the approximately 45 GB held database into a new
index, but does not free that database or erase its evidence/recovery obligation.
After independently accepted old-generation retirement, steady state can be
re-evaluated using measured new free space rather than estimated reclaimed bytes.

### Existing guards and successor work

The current production root 32 GiB guard remains unsatisfied. The current Zone
47 GiB gate comprises its 32 GiB reserve plus a 15 GiB old export/catalog/rollback
workspace envelope. Replacing that workload can make the old 15 GiB workspace
term inapplicable **only after** its producer is fenced/retired and the successor's
actual conversion/publication/recovery overlap is measured and admitted. It does
not waive the reserve or admit an overlapping old and new producer. While the
old path remains usable, both workload envelopes must be reconciled.

At the held sample, Zone spare above its 32 GiB reserve is 16,388,825,088 B.
That is arithmetic, not successor capacity admission: the old 15 GiB workspace
claim and other tenants already consume nearly all of it. Conversely, treating
47 GiB as a timeless successor reserve would preserve an obsolete scratch demand.
Root still has a 2.92 GB deficit against the current production guard regardless
of where a new small database is placed. An empty initial generation alone does
not satisfy that guard or authorize restarting the old path. A genuinely different
successor workload must separately justify its actual required root reserve and
peak envelope through operator admission; the old 32 GiB figure is not silently
a mathematical requirement of every possible bounded warm start. Preserve the
genuine operational reserve and independent host policy; do not lower a guard
merely to admit the old failed workload. No alternative reserve is selected here.

### Minimal alternatives to qualify, without selecting a layout

| Alternative | Initial overlap and gates | Current disposition |
| --- | --- | --- |
| Empty new bounded generation, old evidence held | Small initial generation plus admitted growth; explicit acquisition start/warmup gap; old producer fenced by separately authorized cutover; applicable independent reserves and newly qualified workload envelope met | Lowest conversion demand; no reclamation credit; not admitted |
| Qualified legacy event import | Same overlap plus source snapshot/reader WAL, conversion/index scratch and exact observed_at correspondence | Trusted historical observation times not established; cannot fabricate them |
| Root bounded hot/global plus Zone compressed segments | Root must fit its measured state/WAL/scratch above reserve; Zone must fit retained segments, immutable membership indexes, export/lease/rollback overlap alongside current tenants | Candidate placement only; current guard fails; any different successor envelope separately unqualified |
| Root bounded hot/global plus configured NFS immutable segments | Separately justified successor root gate; actual NFS mount/capacity/custody and read latency/refusal/recovery semantics qualified; bounded local metadata and any cache budget included | No current NFS performance/availability admission from these held receipts |

No alternative moves a live SQLite database to NFS or selects an unqualified
cross-filesystem catalog path. Current incident apply/catalog code uses existing
fixed destination assumptions; any future path change requires exact source and
configuration qualification, consumer correspondence and rollback. Copying
catalog bytes alone is not a validated relocation. A temporary volume, recurring
capacity purchase or new engine is outside this plan.

The next admission path is therefore: finish the off-production measured compact
successor envelope; choose the actual deployment path only through existing
owner review; obtain fresh host/tenant/mount observations; reconcile the old
producer and evidence custody without assuming deletion; and independently
accept migration semantics and peak inequalities. If those inequalities cannot
be met, remain off-production. Do not begin relocation merely to discover the
missing capacity or transfer-state assumptions.

Formalization consideration: the inequalities account for simultaneously live
allocations on independent filesystems under declared concurrency and growth
assumptions. Their algebra is straightforward; correctness depends on measured
allocation identities and complete tenant/transition correspondence. Retained
samples do not establish current capacity, and a steady-state bound does not
imply a feasible migration transition. Decision owner: root and independent
migration acceptance owner. No capacity or retirement acceptance is asserted.


## Packaging and whole-invocation readiness gate

The compact f4e818d 100k result improves the estimate but establishes no deployed
service or 30-day capacity. RecentStore still resides under
`tools/segmented_qualification`; storage.owned() accepts campaign runtime roots
only. Preserve those safeguards. A production adapter/install must be explicitly
reviewed, with exact source/schema/dependency custody, admitted store location,
permissions, acquisition identity, owner rotation/expiry and bounded configuration.
Do not remove the campaign path restriction merely to describe the candidate as
production-ready. The CLI imports labelwatch before qualification storage adjusts
sys.path: an installed package or qualified explicit source import path is an
installation gate, not implied by WorkingDirectory. No production service is
selected and no timer is installed by the example.

The noninstalled `deploy/labelwatch-recent-collect.service.example` uses oneshot
TimeoutStartSec=45, TimeoutStopSec=5, KillMode=control-group and SendSIGKILL=yes.
That gives the enclosing service manager authority to stop a stuck process and
its descendants independently of async HTTP cooperation, including DNS executor
shutdown. It is a configured operational deadline, not a hard real-time proof.
The direct CLI invocation alone lacks this enclosing guarantee. Before acceptance,
qualify a local deterministic blocked-resolver/TERM-resistant child case using
an equivalent transient unit, verify the original invocation reaches terminal
state and leaves no unit-owned processes, and independently observe persisted
attempt recovery as an explicit gap. Missing terminal evidence is indeterminate.
Do not run this qualification against production or install the example here.

Review pending-attempt atomicity, discovery compare-and-set, stopped-round gaps,
source cursor correspondence and public warmup semantics at their next exact
sealed revision. The example's safe-hold and activation conditions remain gates;
changing a marker is a separately authorized cutover, not packaging. Operational
readiness also needs selected scheduling, accepted maintenance/publication failure
observations and recovery ownership. Current held capacity and old-evidence custody
constraints above still apply. No new activation or deletion is authorized.
