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
