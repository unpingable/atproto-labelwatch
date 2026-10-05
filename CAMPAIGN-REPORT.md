# Terminal campaign report — Labelwatch storage architecture

Terminal disposition: **COMPLETED_NO_DECISION_READY_FOR_REVIEW**.
Independent acceptance: **PENDING**. Completion notices are not acceptance.
Branch: `lane/labelwatch-storage-architecture-20261005`.
Production mutations performed: **NONE**. No production deployment, storage
migration, retention-policy change, gate change, derive/report activation or
estate cleanup occurred. Immediate incident remains OPEN.

Outputs: [architecture and measurements](LABELWATCH-STORAGE-ARCHITECTURE.md),
[decision](LABELWATCH-STORAGE-DECISION.md),
[executable prototype](tools/storage_spike/README.md),
[artifact custody/closeout](LABELWATCH-STORAGE-ARTIFACT-LEDGER.md).
No migration sketch: no architecture-change recommendation was earned.

## Source custody

| Repository/worktree | Starting source | Final state |
|---|---|---|
| Isolated Labelwatch spike | 51f923e6f28bc9e5cb7550b5f4895c8a91c5cca9 | executable f5c5cea2532c8e87a850eeee8d1d3ff5749cb48b; final documentation revision is the commit containing this report, also recorded in private RESULT-CUSTODY.json |
| Primary Labelwatch checkout | 490b17290e5c3a8c0f2de0f7e4b9741b0ad8a417 | unchanged by spike; deployed core separately b5e1a5dac29733b2d83920d1737c35873446a6a9 |
| Driftwatch donor inspection | a066133312e2cd523d9e183a79caf0d6851c61b8 plus existing dirty repairs | no edits/merges; published historical ancestry checked; dirty successor not accepted donor |
| Incident branch, coordination only | c476532c48a2c1845a9a23ea626cfe56e8a503c5 | unchanged by spike; published, 50 focused cases pass, undeployed |
| portfolio-private | untracked private custody; no invented Git SHA | additive current resume/checkpoint/results; historical seals unchanged |

This source is a forward branch, not a claim that the spike base is main or
deployed source. Canonical worktree/remotes and deployed lineage are in the
separate published portfolio census. No unrelated branch or dirty tree was reset.

## Durable attempts and qualification

Campaign root is `portfolio-private/campaigns/labelwatch-storage-architecture-20261005`.
Each dispatch binds source, unit/container, log and terminal before unattended
work. The final matched-ingest run used the durable mutable recovery checkpoint, but
a separate immutable dispatch copy was not preserved. Its post-run
DISPATCH-RECONCILIATION-203ea416.json is explicitly reconstructed evidence, not
a pre-run receipt; independent review must assess that provenance gap.
All seven producer/dependency units have MainPID=0; all three exact owned
fixture containers are stopped. Failed systemd states are retained as evidence.

| Run | Source | Terminal / evidence |
|---|---|---|
| abf37f3e-1d46-4284-980d-d3537f6e0f57 | e2aeadfc2fa8996689b54d0a3c57ec1cdc2c85f3 | FAILED fixture bootstrap socket; accepted workload 0 |
| 310e5b37-a19d-44d0-90fc-0084f751c3aa | a03089b73b24392727724d505bfd9f09be4bfafd | FAILED Unix pathname >107 bytes; accepted workload 0 |
| fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19 | 19b10ac7fa5054a008bf5454910a738cdc2ba5bd | PASS producer; six sampled query parity checks, complete historical 2.99M week, 35 lifecycle cases including one UNQUALIFIED |
| d211d6f2-a04f-4903-a7c8-4ff93b497d31 | c33fdaf8831ef8e5c0f105f6e381098169dcac48 | FAILED fixture Row factory before trim; unchanged floor/no pending trim; export retained; original timing/peak gaps UNKNOWN |
| eb9b4248-e1dc-457f-a86e-a1764f1f8bfc | d6fc28ed1934965050830e3e093560f8290792df | PASS reconciled original pretrim fixture, exact library cycle and all-index PG week; no duplicate load/export |
| 203ea416-b876-45a4-8690-bd48566467bf | f5c5cea2532c8e87a850eeee8d1d3ff5749cb48b | PASS matched 180k equal-index ingest profiles |

Additional c33fdaf arrival-vessel suite: nine cases pass including refusal;
ten-row typed optional-NULL round trip passes. Current finite model: six states,
24 transitions pass under its limited assumptions. PostgreSQL process SIGKILL
restart and retained ten-row digest audit pass. Physical host restart and full
backend composition are unqualified. Generic closed-partition custody cuts do
not prove PG clock-driven creation/rollover, concurrent ingest or full cursor state.

Environment: crow Linux 6.5.0-44, Python 3.12.3, SQLite 3.45.1, DuckDB 1.4.3,
PyArrow 22.0.0, psycopg 3.2.13, PostgreSQL 17.11 immutable image
`postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652`.
No network/TCP listener in fixture clusters; private Unix socket only. Corpus
uses existing immutable private Parquet custody; no raw identity-bearing rows
were committed/published. Exact source manifests, hashes, commands and execution
identities remain in private receipts and immutable dispatch records.

## Findings and fact-quality corrections

Seven-file retirement returned 2.63GB after independent content custody
verification; actual row trim took 191.5 seconds and returned zero filesystem
bytes. These prove different lifecycle properties, not a migration decision.
The lead hypothesis is segmented hot SQLite with existing Parquet/DuckDB cold
machinery. PostgreSQL remains a credible same-engine state/cursor comparator.

The historical 3M week is not today's workload: a production read found
9,764,010 events in September 28–October 4 and 44,315,507 live rows.
The SQLite progress-handler cap did not bound optimized COUNT to five seconds;
actual 15.8s is recorded and not repeated. Future bounded reads need an external
process deadline. No query regression is inferred from the metadata read.

32GiB root headroom was reserve policy, not established export scratch. Root
also carries a history-sized cold SQLite catalog. Actual crow NFS is writable;
the restricted mount namespace's older ro inference is rejected. Fixing public
file traversal alone would expose stale publication rather than current facts.
Those incident findings are in the separate RCA, not repaired by this spike.

The historical storage-runway note now explicitly marks its absent-cold-export/
retention claims as superseded; its original text is retained. Driftwatch 202M
sizing and finite 8M memory qualification are distinguished. Initial PG results
with a missing state index remain historical evidence, alongside a separately
labeled all-index correction. No failure, historical receipt or seal was erased.

## Tracker and resume

[Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7) owns the
architecture work: ATProto Project Labelwatch / Research / Exploratory / Next;
Status Qualification for independent review. Gate explicitly states NO_DECISION
and full state/archive-dedupe/schema/current-volume prerequisites.
[Monitor #18](https://github.com/unpingable/constellation-monitor/issues/18) owns
the separate direct-obligation gap: Constellation Project Ready / Observability /
Integration / Candidate / Next. Generic monitoring unknowns worked; direct
maintenance/deadline identity enrollment and operator consequences remain missing.

Existing retention #6 and API #1 were annotated, not closed. No production hold
or research issue is declared complete from prototype evidence. Private Project
change receipts preserve exact item/field identities and verified final values.
Single operator resume: `portfolio-private/ATPROTO-RESUME.md`, whose additive
current supplement links these findings and existing issue homes. The sealed
portfolio map is supplemented by current evidence rather than silently rewritten.

## Unresolved unknowns and next lanes

1. `lane/labelwatch-retention-api-recovery-20261005` — NOW. Establish permanent
   owned cloud capacity/admission, review the candidate, run an admitted
   archive-first occurrence, repair fresh publication and prove soak. No
   temporary filesystem topology or reporting/derive activation.
2. `lane/atproto-status-acceptance-20261005` — independent reviewer falsifies
   retention failure, 503, deployed lineage, paused Driftwatch, mount correction
   and cleanup classifications. Acceptance does not block read-only diagnosis.
3. `lane/constellation-labelwatch-obligations-20261005` — Monitor #18: direct
   occurrence/floor/custody/deadline evidence to operator surface and safe
   negative control, or owned bounded substitute with retirement condition.
4. `lane/labelwatch-storage-semantic-qualification-20261005` — #7 continuation,
   separately admitted: full mutable state/cursor/identity/archive-only dedupe,
   metadata restore, queue bounds, schema/default/refusal vectors, concurrent
   writer/clock/host-loss correspondence, then current-volume/dense queries.
   No adoption before the NO_DECISION gates close.

Acceptable data-loss window, physical NFS/host recovery, worst ingest burst,
full-volume DuckDB spill, PostgreSQL WAL peak, mixed-generation reader semantics
and ownership of a hypothetical PG deployment remain UNKNOWN. No engine winner
or clean portfolio/storage state is declared.

## Evidence and storage closeout

The following receipt hashes bind the reported measurements. Campaign-relative
paths and hashes are added below; private seals include dispatches, terminals,
source identities and logs. Qualified dump/export hashes are separately recorded
in RETAINED-QUALIFIED-ARTIFACTS.json. No private data rows are in this report.

| Campaign-relative receipt | SHA256 |
|---|---|
| `runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19/evidence/FAILURE-QUALIFICATION.json` | `42352f229bdd6447a26bffa8a915d70577cc52575ccd169fde87263bab585821` |
| `runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19/evidence/QUERIES.json` | `1c402481482b0b42d70638352a06ac5c7b18342026967891578d801a4c17424c` |
| `runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19/evidence/RETENTION.json` | `b4e31a8d97dd562b9c48cf91b1cec09dc89bb15adaba45967c448a72fae8f5a8` |
| `runtime/recovery-eb9b4248-e1dc-457f-a86e-a1764f1f8bfc/evidence/CURRENT-SQLITE.json` | `e8695af7a6ea30822c46136ec1b54711faf228194577f55b8d0797bd0db82e18` |
| `runtime/recovery-eb9b4248-e1dc-457f-a86e-a1764f1f8bfc/evidence/POSTGRES-ALL-INDEXES.json` | `8fd66508873376fa9b521888b7e114bcfb1fb27b46368c4295807306f16e2871` |
| `runtime/matching-ingest-203ea416-b876-45a4-8690-bd48566467bf/RESULT.json` | `888c573ffd181ed136e0f5fb9c3085b552e1d249506be5b6ab0d2787abd30222` |
| `runtime/rollover-qualification-20261005/RESULT.json` | `99048c3ba6388913ddf0d8dda785ebbeef1e58ef9ce399e9d122baef0e62788d` |
| `runtime/typed-schema-qualification/RESULT.json` | `bd9854e05c9812719210bf076308817268a1776ad551fee9cb785280f0e0e34c` |
| `evidence/PRODUCTION-WORKLOAD.json` | `2c5cf8ae72ba352bd2057ff22ec85b535e08c8e5a2a4a3c1647e32cb77c09f71` |
| `evidence/FORMAL-MODEL.json` | `709df5267727edc2b5d34b1a0b0672b1ca3cddc6192e4895ea97db9d7e8b93cc` |

Immutable completion notices live beneath the declared shared campaign registry:
`.lanes/labelwatch-storage-architecture-20261005/<run>/COMPLETE.json`. They are
deduplicated by run identity; conflicts require reconciliation, never overwrite.
An independent acceptance owner/record has not been assigned or manufactured.

Identified runtime allocation retained: 13.67GB, inside the 16GiB envelope.
Crow root and /data independently pass 60GiB reserves. Artifact ledger records
exact paths/classes/dependencies and disposition review by October 12.
Cleanup bytes reclaimed: **0**. User scope excludes destructive cleanup; even
positively disposable staging is retained for an exact follow-on disposition.
Prototype retention effects are reported separately from cleanup.

No VM, compiler target/cache, campaign SSH key or agent identity was created.
Failed clusters are stopped and classified. Shared image-layer ownership is
unknown and no image prune was performed. See the ledger for actual allocated
bytes, free blocks/inodes, attribution limits and remaining large consumers.
