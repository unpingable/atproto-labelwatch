# Labelwatch horizon-capacity terminal campaign report

Decision: **REJECT_SEGMENTED_SQLITE_PARQUET_DUCKDB**, current state/custody design.
Existing issue: [Labelwatch #7](https://github.com/unpingable/atproto-labelwatch/issues/7).
Lane: `lane/labelwatch-segmented-horizon-capacity-20261005`.
Root run: `3fcdf829-e8d8-4a30-b8d7-400775a92d4e`.

## Source and execution custody

- Starting source: `b2b9a416761b6e9daace4c8f4d5beef3ca282f2c`.
- Final executed/accepted qualification source: `01cb5f610796fefdf5e2a783b626424001ccb559`.
- Final documentation/publication HEAD is separately recorded exactly in the
  private terminal receipt, completion notice, existing #7 and canonical resume;
  no qualification source bytes change after independent acceptance.
- Source worktree: `/data/git/.worktrees/labelwatch-segmented-horizon-capacity-20261005`.
- Common repository/canonical tree: `/data/git/atproto-nutrition/labelwatch`;
  tree HEAD `490b17290e5c3a8c0f2de0f7e4b9741b0ad8a417` stays unchanged.
- Private registry/custody: `portfolio-private/campaigns/labelwatch-segmented-horizon-capacity-20261005`.
- Producer `225859f1-ea73-4ad5-9af5-747229cc8ba0`, crow user-systemd,
  launch invocation `df3c3ab01d26426498f531c0d482391c`.
  Terminal PASS_COUNTEREXAMPLE_REPRODUCTION, MainPID0/exit0. Final collected
  invocation/MemoryPeak are unavailable; launch and process RSS receipts remain.
- Earlier attempt `46fe81f3-2b6b-4b0e-a399-32a97bdcbb56` refused the new fixture
  archive namespace before any product gate; failed source/terminal preserved.
- Independent runs `d1246d42`, `e6f08c73`, `4534598a` all terminal success/MainPID0.
  Independent source edits NONE; separate acceptance hash
  `08e1b7e410fdc50264abd1022757bc7b8c6214e062af6bc7d57cc4ece065a1ef`.

## Evidence obtained and limits

The source reuses the previous Store, generators, catalog, helpers, receipt format
and qualification machinery. One direct horizon executable adds the necessary
lifetime/metadata cases. Exact campaign path admission is the only Store change;
no new adapter, evidence schema, framework, engine or lifecycle repair.

Producer lifetime fixture:96-days,13closedweeks+oneactive,49,152 accepted events
before diagnostic substitution,24,576 unique below-floor offers. At day 47:
24,064 accepted,12,032 future keys,10,240 normal keys,12,032 full quarantine rows,
12,558,336 global SQLite page bytes. At day 96:24,576 future keys,10,240 normal
keys,24,576 quarantine rows,22,986,752 global page bytes. One fixed labeler,
subject and source. Synthetic timestamp controls and low volume are explicit.

Independent ordinary archived replay confirms the lifetime defect without
requiring future timestamps:384 ordinarily dated accepted events; quarantine
28→224 across day 47→96, normal keys 160; repeated historical delivery mutates
seen_count to 2. Future-key removal separately admits duplicate history.

Catalog identities 128/1,024/8,192; metadata fixture uses hard links to one verified
one-row artifact. At 8,192:receipt metadata 974,967 bytes after admission,
startup 0.9223s, cached listing p50/p95 0.1541/0.1907s, new admission 0.9643s
and 8,193 whole-artifact hashes. Peak producer RSS 194,686,976bytes.
These are metadata/cardinality timings with a shared cacheable inode, not
historical volume throughput or a full root/archive working-space bound.
Independent catalog 5→6hashes confirms algorithmic read amplification.

Independent catalog custody substitution returns 2 for one accepted event when
receipt publication precedes global ARCHIVED commit; explicit retry returns 1.
This is a real source-order state boundary, tested by equivalent owned SQLite
state changes; an actual process kill at that exact point is not claimed.

Permanent missing-history regression:exact original generator bytes execute with
sealed new modules;10→explicit missing-history refusal→restored10. Original
static source label is disambiguated by EXECUTION-IDENTITY.json. Prior failing
specimen/source/golden receipt remain unchanged.

Full47-day current-volume specimen and40-day product query benchmarks: **NOT RUN**
after independently accepted hard failure. No new hot/dense/full-horizon p50/p95
or current-volume capacity acceptance is claimed. Previous 12M/36ms/11.11GB and
one-week query evidence retain their original scope at immutable `b2b9a416`.

## Fixes, decisions and follow-on

Fixture namespace admission corrected after the first refused attempt; original
failure preserved. No storage-semantics fixes performed. Defects are preserved as
executable counterexamples in the existing private evidence custody.

The exact decision follows the user hard-failure rule. This is not an engine
impossibility claim or an unexplained NO_DECISION. Current mutable global state
recreates historical event ownership; catalog discovery can also double-count
before custody commit. These must be repaired before adoption.

Next scope under #7: `lane/labelwatch-segmented-global-lifetime-repair-20261005`:
bounded archived replay/quarantine and authored/arrival lifetime contract;
committed query/custody ownership; incremental catalog admission. Preserve or
explicitly reapprove current consumer semantics, then qualify the originally
requested47-day state/40-day query conjunction against those changed boundaries.
No automatic PostgreSQL campaign; another engine does not inherently fix lifetime.

Existing #7 remains OPEN. Project updates and canonical resume publication are
recorded with exact responses/hashes in private PROJECT-RECONCILIATION and
RESUME-RECONCILIATION receipts; final publication pointers are in TERMINAL-CAMPAIGN-REPORT.json.
Prior NO_DECISION is archived as historical, superseded by concrete hard failure.

## Storage and authority closeout

Storage owner/key owner: /root. Admission512 MiB new `/data`,256 MiB root;
independent allocation budget32 MiB. Shared60 GiB free reserve on both `/` and `/data`
never lowered. First admission is after small directory/worktree creation;
exact pre-creation host baseline unavailable. Concurrent host-wide changes are
not attributed to the campaign. No incident cloud/production margin consumed.

STORAGE-CLOSEOUT.json records exact per-object distinct-inode allocated bytes,
final free blocks/inodes, current replay dependencies and review date. Hard-linked
catalog artifacts share one inode; per-path sums are not filesystem consumption.
Reproducible fixtures/cache remain retained because destructive cleanup is
expressly forbidden. Current counterexamples, exact source and receipt identities
have #7 replay/design dependencies with review by 2026-10-12 or successor acceptance.
Review is not automatic deletion. Prior campaign tenants/venv/seals untouched.

Known source-file retirement inside isolated lifecycle qualification is separately
receipted; administrative cleanup deleted NONE / reclaimed 0. No VM, compiler target,
new package/download cache, new identity, SSH enrollment, agent change, remote
revocation or production contact. Ambient agent/remote authorization state not
inspected and no claim made about it.

Production mutations: **NONE**. Deployment/migration/retention-policy changes,
report/derive activation, destructive cleanup and new engine comparison: **NONE**.

[Decision](LABELWATCH-SEGMENTED-DECISION.md) and
[full horizon evidence](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md) are the
operator-readable artifacts. Private checkpoint is the exact supervision/custody
resume pointer; GitHub Project remains authoritative for execution.

## Measured closeout sample

Distinct-inode retained allocation across new runtime/evidence/source: 73396224 bytes.
Known isolated qualification source disposition: 34562048 bytes; administrative cleanup0.
Final-sample free bytes: `/` 84230651904, `/data` 785472679936.
Final free inodes: `/` 54966041, `/data` 120718891.
Subsequent terminal/board/resume receipt metadata adds small bytes; no further large allocation.
The exact final terminal sample is in private TERMINAL-CAMPAIGN-REPORT.json.
