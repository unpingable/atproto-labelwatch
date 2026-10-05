# Labelwatch segmented decision-closure campaign report

Lane: `lane/labelwatch-segmented-storage-qualification-20261005`.
Owner/storage owner: Codex /root. Registry is the private campaign root named in
[qualification](LABELWATCH-SEGMENTED-QUALIFICATION.md). Root run identity:
`a5e6e1b8-2276-4c42-a521-3fd43b105c21`.

Starting source: `999b349845c6066ee5f839ede34b423761c9be36`.
Primary scale source: `011a8d646247affe85988c984ee1bbe69cae9773`.
Continuation source: `84270e50cb204c413d2527ff510d349c07c623df`.
Final corrected prototype qualified source:
`31e5b871ef3133746e728288d5b256c59f795419` in an isolated linked worktree.
Exact qualified core integrated into the lane branch at `321280b`; final document
revision is recorded by the terminal notice rather than a self-referential SHA.

Production mutations: **NONE**. No migration, deployment, production retention,
policy change, incident capacity allocation, reporting/derive activation, new
engine, production service restart, DNS/TLS change or database write occurred.
Read-only production observations establish shape/index/state evidence, with
bounded-query timing limitations explicitly recorded. Incident remains #6.

## Producer chain

| Run prefix | Purpose | Terminal / important evidence |
|---|---|---|
| d072f662 | Finite process cuts and schema vectors | PASS; original misplaced precommit counterexample preserved separately. |
| 82434791 | 12M-event scale ingest/archive/query | FAILED_QUERY_OOM; ingest/archive/cursor receipts retained, not rewritten. |
| 5b6facf4 | Durable capacity sampler | Terminal after primary;7426 samples, initial500k unobserved. |
| b7ce9425 | Global state, concurrent writers, real90-day sample | PASS_WITH_COUNTEREXAMPLE for insufficient local-only coverage. |
| 5931697e / 08ddf301 / 5f53b75c / e2c02083 / 4cc9d2b6 | Successive explicit guard qualifications | PASS; latest27 finite plus10resource/schema/concurrency/expiry guards. |
| f1036f6e | Exact report SQL memory controls | PASS_CONTROLS; one-worker configuration retained. |
| 349c7ce8 | Unique state replication | Resource kill; partial never custody, failure reconciled. |
| 996f0498 | Invalid copy dispatch | REFUSED before execution; corrected attempt gets new identity. |
| 4a9bd356 | Bounded streaming state replication | PASS; full source/destination SHA equality and link/mode receipt. |
| 01e6bd13 | Qualified handoff of original fixture | PASS_CONTINUATION; exact query parity,11.113 GB retirement, global/cursor preservation. |
| 1ecd6b04 / db40d84b | Immutable catalog negative controls | PASS; mutation/replacement/new-entry refusal and explicit readmission. |
| abb81a17 | Corrected12M replay-key expiry with ingestion | PASS;11,999,880 expired,5,985 new events exact cursor/payload/sequence. |
| 073fd2bf | Actual frontdoor/report current-scale reads | PASS;12,000,100 cross-tier events; existing10-second budget preserved. |

Specimen: deterministic12M expansion of bounded30k private measured samples,
actualschema23/allseven live event indexes. Real sizes/cardinality/density seed;
synthetic uniform week, lateness/future/replay controls explicitly labeled.
Specimen and unique-state hashes, source/environment/protocol identities and
commands live in immutable dispatch/seal. See [measurements](LABELWATCH-SEGMENTED-QUALIFICATION.md),
[state model](LABELWATCH-SEGMENTED-STATE-MODEL.md), and
[failure matrix](LABELWATCH-SEGMENTED-FAILURE-MATRIX.md).

Discovered/fixed only inside isolated prototype: actual commit-cut placement;
local-only missing archived coverage; Arrow thread-affinity; creation-only page
cap; resource/spill bootstrap; report memory configuration; bounded NFS checkpoint
copy; whole-expiry writer hold; stale admitted-catalog coverage. Counterexamples
remain separately identified. No fixes are production repairs.

## Acceptance and terminal disposition

Independent review completed. Terminal decision:
**NO_DECISION — SPECIFIC EVIDENCE MISSING**. Original acceptance 27ee66da
reproduced silent historical omission and rejected source 29f3f76. Corrected
source f0161ecc44e38985e390211e0e3882f5cbb0e232 independently passes in record
17b2ab45; adoption remains unaccepted for complete 40+7-day global-state capacity,
full 40-day current-volume queries and required-history catalog admission. Exact
acceptance hashes and prerequisites are in [decision](LABELWATCH-SEGMENTED-DECISION.md).
Producer f2e430a4 passes the added coverage commit-cut/missing/corrupt-object
cases plus full finite/guard/schema regression. No engine reopening.
The one-week results do not prove full 40+7day global-state capacity; no deployment
budget, arbitrary schema compatibility or physical power-loss proof is claimed.

## Custody / closeout

Unique receipts, logs, raw private samples, provenance, source identities and
pre-expiry state stay in private custody. Qualified exact source worktree and
corpus have explicit independent-review/replay dependencies. Exact-object classification and initial closeout census identify approximately
9.39 GB runtime plus16.1 MB evidence locally,8.54 MB lane worktree, and4.06 GB
allocated on NFS (physical allocation differs from apparent file length).
Payload lifecycle reclaimed11,113,271,296bytes; administrative cleanup reclaimed0.
The failed535,146,496byte partial is exactly a prefix of the verified full
checkpoint and DISPOSABLE_STAGING, with the failed receipt retained. It stays
because this lane explicitly forbids destructive cleanup. Remaining qualified
corpora/source/fixtures have named #7 acceptance/horizon replay dependencies and
October 12 disposition review; review is not automatic deletion authority.
Final measurements are in STORAGE-CLOSEOUT.json and the artifact ledger. No other tenant's artifacts or
source worktree are cleanup targets. No SSH key/agent was generated/enrolled;
no unrelated identities were enumerated or modified.

GitHub execution owner remains existing Labelwatch #7 in private ATProto Project
#2. No duplicate architecture issue. Canonical private resume points outward to
this lane and preserves prior reports as historical evidence. Source repository
owns prototype/release claims; Project owns sequencing; Cartography owns mapping.


## Follow-on and tracking

Existing #7 stays OPEN, Qualification / Qualification / Candidate / Next. Old engine
comparison title/status is superseded by explicit segmented adoption gates;
previous issue text is archived in a historical details block. Project remains
canonical for sequencing. The exact successor is
`lane/labelwatch-segmented-horizon-capacity-20261005` with the three evidence gates
above; immediate retention/API recovery and Monitor#18 remain independent.
No alternative database was added, no duplicate issue was created, and no merge
into main or production deployment occurred. The qualified G source lineage is
preserved in the published lane history without changing its final source bytes.
