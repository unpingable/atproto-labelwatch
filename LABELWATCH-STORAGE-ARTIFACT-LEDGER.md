# Labelwatch storage spike — artifact custody and closeout

Owner: Codex /root, sole campaign/storage owner. Custody root `P` is
`/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-storage-architecture-20261005`.
`O` = `P/runtime/occurrence-fd3a3c43-872d-40fb-8ad3-4fdd3fba7e19`;
`S` = `P/runtime/supplement-d211d6f2-a04f-4903-a7c8-4ff93b497d31`;
`M` = `P/runtime/matching-ingest-203ea416-b876-45a4-8690-bd48566467bf`.
All dates below are 2026-10-05. Exact per-attempt sources are in the campaign
report and immutable dispatch records. No raw private corpus is publicized.

Review/disposition by **2026-10-12**, through Labelwatch #7, after independent
review of exact evidence identities. This date initiates ownership/dependency
review; it does not authorize deletion. The user's architecture lane excludes
destructive cleanup, so even positively reproducible staging is classified and
retained for an exact follow-on action. This scope takes precedence over the
local default to remove disposable products at campaign closeout. Evidence
retention is explicitly separate from build/fixture retention.

| Path | Generating source | Purpose / allocated bytes | Disposition | Current dependency / safe next action |
|---|---|---|---|---|
| Architecture worktree | 51f923e → f5c5cea executable + final report commit | source/review; 8,122,368B at sample, slight later doc growth | ACTIVE | published branch review; retain unique commits; no worktree removal |
| P/evidence and immutable DISPATCH files | six recorded attempts | logs, refusal/error receipts, environment, measurement and enrollment identities | UNIQUE_EVIDENCE | seal/retain; independent audit/replay and incident premises |
| P/runtime/dependencies.sh | base 51f923e | exact dependency producer; 4,096B | UNIQUE_EVIDENCE | hash/terminal bind qualified binaries; retain script |
| P/runtime/venv | dependency attempt abf37f3e | pinned Python dependencies; 250,949,632B | REPRODUCIBLE_ARCHIVE | current replay uses exact native binaries; retain through review, then dependency-checked removal |
| P/runtime/pgstate | e2aeadf failed init | empty fixture cluster, no workload accepted; 40,050,688B | DISPOSABLE_STAGING | exact stopped container 7562b8af…; archive diagnostic receipt retained; separately authorized exact removal after dependency check |
| P/runtime/occurrence-310e5b37… | a03089b failed socket path | no workload accepted; 40,583,168B incl cluster | DISPOSABLE_STAGING | exact stopped container 55585ce4…; preserve logs/terminal, review fixture retirement separately |
| O/runtime/pgstate | 19b10ac plus d6fc28e/f5c5cea | exact digest/restore/all-index comparison state; 8,021,082,112B | REPRODUCIBLE_ARCHIVE | container 6a629765… stopped; retained accepted replay dataset, inspect archived dump/source before exact cluster retirement |
| O/runtime/corpus | 19b10ac | copied hash-verified full week and deterministic stratified sample; 337,276,928B | REPRODUCIBLE_ARCHIVE | DATASET.json names canonical NFS sources/hashes; private query/custody replay until review |
| O/runtime/queries.sqlite and WAL/SHM | 19b10ac | private sampled query replay; 162,611,200B | REPRODUCIBLE_ARCHIVE | exact sampled result/index replay; remove only after review and open-consumer checks |
| O/runtime/query-segments | 19b10ac | 90 private daily sampled query files; 167,358,464B | REPRODUCIBLE_ARCHIVE | ATTACH/fanout/setup replay; same bounded review dependency |
| O/runtime/qualification | 19b10ac | process/refusal/corruption case substrates; 942,080B | UNIQUE_EVIDENCE | retained source/custody boundary examples; preserve case receipts, review substrate separately |
| O/runtime/weekspike.dump | 19b10ac | exact verified full-week dump/restore; 385,015,808B allocated | UNIQUE_EVIDENCE | independent restore dependency; SHA in RETAINED-QUALIFIED-ARTIFACTS.json; retain through review |
| O/runtime/week.sqlite | 19b10ac | empty residual after qualified delete/VACUUM; 53,248B | DISPOSABLE_STAGING | scalar receipt retains results; no accepted events remain; cleanup scope hold |
| O/runtime/retention-segments | 19b10ac | empty directory after seven-file retirement; 4,096B | DISPOSABLE_STAGING | test complete; cleanup scope hold |
| O/runtime/duck-spill | 19b10ac | empty sample spill directory; 4,096B | DISPOSABLE_STAGING | no actual spill measured; cleanup scope hold |
| S/work/current.sqlite | c33fdaf → reconciled resume d6fc28e | empty event set, reusable pages after exact trim; 2,633,502,720B | DISPOSABLE_STAGING | receipts plus verified archive preserve accepted rows; exact retirement proposed after review, not performed |
| S/work/archive | c33fdaf | exact verified SQLite event export + manifest; 1,134,133,248B | UNIQUE_EVIDENCE | independent current-library verification/restore; SHA bound; retain through review |
| S/evidence + recovered R/evidence | c33fdaf / d6fc28e | failure/pretrim reconciliation and corrected measurement | UNIQUE_EVIDENCE | keep original failure; missing ingest/export timing and original export peak remain UNKNOWN |
| M/current_normal, repaired_normal, segmented_full | f5c5cea | equal-index 180k replay fixtures; 492,523,520B | REPRODUCIBLE_ARCHIVE | matched ingest/profile review; corpus reconstructible; exact dependency-checked retirement after review |
| M/RESULT, TERMINAL, PROGRESS and recovery receipts | f5c5cea / d6fc28e | terminal matched-ingest and equal-index facts | UNIQUE_EVIDENCE | seal; these replace rough earlier ingest comparisons, not earlier raw evidence |
| P/runtime/rollover-qualification-20261005 | c33fdaf | nine finite arrival-vessel cases; 1,048,576B | UNIQUE_EVIDENCE | predecessor/successor/cursor fixtures for review; preserve result and finite boundary correspondence |
| P/runtime/typed-schema-qualification | c33fdaf | ten-row NULL/schema vector; 28,672B | UNIQUE_EVIDENCE | exact optional-field round-trip evidence; preserve |
| P/runtime/local_smoke | e2aeadf | finite initial fixture; 24,576B | DISPOSABLE_STAGING | no active producer; cleanup scope hold |
| P/runtime/pgsocket and /data/git/.lane-sockets/labelwatch-storage-pg-fd3a3c43 | respective init attempts | fixture sockets, 8,192B / 4,096B directory allocations | DISPOSABLE_STAGING | all exact consumers stopped; proposed exact removal only with cluster/replay disposition |
| /tmp/labelwatch-storage-spike-20261005.md and /tmp/labelwatch-workload-observe-20261005.py | dispatch/body creation | small own issue/observation scripts; exact source copied to P/evidence, sizes/hashes in TEMPORARY-OBJECTS.json | DISPOSABLE_STAGING | no continuing execution; retained original pending exact authorized cleanup |
| PostgreSQL immutable image | dependency attempt | image ID 248efd5e…; reported logical image size 437,879,853B | REPRODUCIBLE_ARCHIVE | local replay requires it; shared image-layer ownership unknown, no image prune |

Whole-unit unlink, test row DELETE, fixture partition DROP and tiny-residual
VACUUM were authorized **prototype qualification effects**, not estate cleanup.
No campaign evidence, worktree, branch, production DB, VM or other tenant was
removed. New fixture containers are stopped, not silently restarted or removed.
No generated SSH key, loaded agent key, VM image, ISO or compiler target tree
was created. Dependency pip used `--no-cache-dir`; image layers are retained,
not claimed as exclusively owned physical bytes. Temporary command scripts in
`/tmp` are small, campaign-scoped and contain no copied private keys; their
custody/cleanup remains explicit in private receipts.

## Storage receipt

At 2026-10-05T15:30:22.770342+00:00, `/` had 93,069,127,680B free / 55,561,132 free inodes;
`/data` had 796,143,919,104B / 120,767,761 free inodes.
Both independently pass the 64,424,509,440B reserve. Identified runtime retained
allocation is **13,667,393,536B** (about 12.73 GiB), inside the 16 GiB
admitted envelope; worktree, small evidence and socket allocations are additional.
Protected UID999 cluster bytes were measured by a read-only no-network helper;
the directory blocks already counted by host du were subtracted.

Dispatch baseline free bytes: `/` 94,130,020,352; `/data` 808,093,253,632.
These are comparable free-space samples, not attribution: other tenants and
shared image-layer allocation remain incompletely observed. Current campaign
allocation is measured directly. Cumulative bytes ever written / peak total
campaign allocation were not instrumented and remain UNKNOWN. Measured transient
qualification reclamation includes 2,633,621,504B of seven SQLite files,
2,633,449,472B from a separate empty-residual VACUUM, and 3,225,411,584B of
PostgreSQL relation files; no combined host-free gain is attributed to those
steps. **Cleanup bytes reclaimed: 0.** Retained runtime bytes are the figure
above; no reserve-restoration exception is needed on crow.

Largest campaign consumers are the 8.02GB stopped PG replay cluster and
3.77GB failed/resumed current-SQLite occurrence. Largest other host consumers
remain owned by their existing campaigns; this closeout is not an estate audit
or deletion authority. Cloud-root admission remains an incident follow-up,
independent of crow's reserves. Closeout is complete with retained custody and
an explicit exact-object disposition review, not a claim that storage is clean.
