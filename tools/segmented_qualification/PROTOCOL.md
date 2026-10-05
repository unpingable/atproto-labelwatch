# Decision-closure qualification protocol

Owner: Codex /root. Source: exact commit in immutable per-run dispatch. Registry:
`/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-segmented-storage-qualification-20261005`.

Production mutation is forbidden. The old incident and storage gate are unchanged.
Use only a new campaign-owned occurrence. Current-scale producer is serialized;
32 GiB /data envelope, 100 MiB root allowance, 8 GiB archive payload estimate.
Both host filesystems retain the operator 60 GiB reserve. Archive admission is
fresh and verifies write/fsync/close/rename/remove before conversion. The actual
archive admission may exceed the payload estimate because it reserves twice the
sealed SQLite size; capacity is checked independently. No temporary filesystem.
The source Parquet corpus remains on /data for the named current-scale replay and
acceptance dependency; disposition is due at acceptance/issue #7 closeout.

## Predeclared properties

* At least 9,764,010 accepted events: target 12,000,000.
* Real schema 23 and every label_events index. Bounded private current/week
  samples supply event shapes; expansion, timestamps and replay rates are synthetic.
* Every accepted event has durable journal, vessel or verified archive custody.
* Exactly one authoritative active target; ACTIVE.json is only a projection.
* Cursor commit occurs at the actual db.set_cursor transaction boundary.
* Historical retirement cannot remove global state. Replay within the live floor
  uses hot hash state; below-floor replay retains existing quarantine semantics.
* Verification does not block new-segment ingestion. No retirement before full
  ordered-row equivalence, identity, schema and file-integrity checks.
* Actual frontdoor SQL parity, dense refusal at the existing 2,000-event cap.
  Do not claim the cap is lifted. Query generation has an existing 10-second
  frontdoor budget; report/derive queries are separately timed, not assigned an
  invented interactive SLA. p95 is nearest rank over seven observations.
* Schema vectors include NULL, Unicode, optional values, additive column/index,
  rename, encoding change and older-reader refusal. Unknown schemas refuse;
  fixed generations are examples of a finite contract, not universal migration.
* Local storage has explicit file/page/queue/WAL/work limits. Report actual peaks
  and derive a workload bound; fixture caps are not adopted production policy.
* Injected process death and deterministic unavailable/read-only/full fixtures
  must recover safely or refuse visibly. Physical power loss is outside scope.
* Adoption requires a separate reviewer to attempt falsification after evidence
  is sealed. A producer PASS is never adoption or independent acceptance.

## Durable execution

Use a unique transient user-systemd unit, RuntimeMaxSec=7200, MemoryMax=2G,
CPUQuota=200%. Write immutable DISPATCH-<UUID>.json before launch with source,
protocol, command, dependencies, budget, log, progress, terminal and notice.
Persist checkpoint and unit InvocationID. A new supervisor inspects the existing
unit, log, PROGRESS and TERMINAL; it does not restart an unresolved occurrence.
SIGKILL without a terminal record is indeterminate until unit/evidence reconcile.

## Formalization consideration

Proposition: journal-first acceptance plus exact idempotent flush, writer fencing
at seal, one-active uniqueness, and verified archive-before-unlink preserve each
accepted event across the modeled process-death cuts. State/cursor authority is
separate from retired vessels. Bounded model enumerates logical transitions;
SQLite FULL/fsync, honest storage, protocol-conforming writers and independent
reader leases are assumptions. It does not model kernel crash, NFS server loss,
unknown writer access or application semantic updates. Practical cut tests and
full-cell digests provide correspondence evidence; concurrency and schema vectors
must be reviewed independently. Decision owner: lane acceptance reviewer.

## Closeout

Seal results and counterexamples, then exact-object custody/dependency review.
Retain unique receipts, source identities and acceptance records. Reproducible
large fixtures require explicit disposition, not indefinite preservation.
Update existing atproto-labelwatch #7 / ATProto Project #2, no duplicate item.
