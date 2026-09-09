# M3 finite SQLite resource relief — implementation candidate

Not independently qualified. No production maintenance or installation is
authorized by this document. The beta requirement remains owned by Constellation.
This implementation descends from companion campaign0a79ef7 (deployed rc10 code
5ed0a00 plus documentation), preserving the intervening operational repairs.
The primary1200308 and Murex2ad2710 worktrees are not changed.

## Mechanisms and ownership

- `maintenance_manifest`: schema23, every ordinary table, exact typed logical
  row contents and all meta/cursor records. All columns define a storage-class
  then BINARY ordering; implicit rowids/physical page layout are excluded from
  the logical claim. Virtual tables refuse; no derived/cache table is silently
  skipped. Integrity alone is not sufficient. Restore is an actual SQLite
  backup API restore followed by real application read/rule verification.
- `maintenance_artifacts`: exclusive new outputs, retained failures, separately
  budgeted backup+restore filesystem, compact staging with source owner/mode,
  and exact content/inode checks. No source replacement/deletion here.
- `maintenance_hold`: main and discovery start before writable initialization,
  read-verify behind the persistent hold and emit PID/start-ticks-bound readiness.
  A writable connection after release commits a distinct maintenance generation;
  this actual post-cut write makes stale-original rollback ineligible.
- `maintenance_step`: one finite effect only, no scheduling/retry/controller.
  Canonical input bytes are bound by `--expected-sha256`; externally authorized
  steps serialize through one enrolled journal lock. A started step without a
  terminal record is unknown, not permission to execute it again. Reconciliation
  observations never authorize recovery.
- Independent `maintenance_observation` reads current SQLite/filesystem/process
  facts rather than trusting completion receipts. Additive NQ-ng compiled
  `labelwatch-relief` qualifies those source-labeled facts, preserving missing,
  false, stale and mismatched evidence. Post-release qualification replays exact
  qualified pre-ingest evidence, not a worker's success assertion.
- AG-ng authorizes each exact root-enrolled digest-named systemd unit; Docket
  owns execution custody/reconciliation. Local helper files do not grant either.

## Explicit enrollment premises

Before installing the hold, stop every enrolled writer, including prior runtime
processes, discovery, lock/checkpoint jobs and one-shot writers. Direct SQLite
writers not using this application guard must be stopped/excluded by operational
custody; a stopped main service alone is insufficient. The API's readonly role
is separate and must not be presented as ingestion liveness.

The enrolled host, database/file/device identity, source revision, unit definitions,
helper interpreter/imports, step inputs, journal and parent directories remain
under trusted local custody. Each immutable unit has fixed ExecStart and exact
step/hash arguments; no mutable argument file or shared changing unit name is
allowed. Existing AG systemd execution does not measure fragment bytes: immutable
root enrollment is the explicit premise, not a newly claimed mechanism.

Hold/release parent must be controller-owned and not service-writable; only its
separate `ready/` directory permits the enrolled service's readiness records.
The containing database directory also needs protection against hold-directory
replacement under the declared custody. Trusted app readiness is not independent
authority. A release record requires prior external authorization and qualified
evidence before the enrolled helper may publish it.

No source sidecars may remain at the quiescence cut. A missing sidecar is directly
checked; an inaccessible source is not absence. Source/backup observations are
not an atomic global snapshot and assume stable parent/cut custody. The backup
may be verified yet still fail the separately identified filesystem prerequisite.

## Finite path and recovery

`stage` → STAGED_NOT_INSTALLED; `replace` → REPLACEMENT_ACCEPTED;
`verify-service` → SERVICE_ACCEPTED_PRE_INGEST; separately authorized `cleanup`
records CLEANUP_AUTHORIZED before deletion and CLEANUP_COMPLETED_NOT_RELIEF after;
`release` checks a fresh final-availability floor then yields
INGRESS_RELEASED_POSTCONDITION_PENDING. The v2 step keeps temporary workspace
capacity (`temporary_operating_margin`) separate from an exact enrolled
pre-operation filesystem/device/free-space observation. Its final floor is
closed as `pre_operation_available + minimum_net_gain`; a stale, mismatched, or
internally inconsistent baseline refuses before STARTED. The release record is
not written unless fresh available bytes meet that exact floor. The same floor
is the NQ post-release request threshold, so hold release and factual
qualification cannot silently use different resource claims.
Fresh independent NQ post-release qualification establishes only its exact
`resource_relief_postcondition`; process exit/enactment alone never establishes it.

Every downstream input pins predecessor bytes and the common operation/source/
artifact/manifest/policy binding. `verify-installed` is read-only. A distinct
authorized `rollback-pre-ingest` may restore an exact retained original while
held, preserving replacement/staging and backup. `reconcile-cleanup` can reopen
actual absence and verified remaining data after lost cleanup completion. Neither
is an automatic retry. Once the committed release generation is observed,
recovery is FORWARD_RECOVERY_ONLY; unavailable write-resumption evidence remains
INDETERMINATE. Do not restore an old backup after release as ordinary rollback.

## Fixture commands and remaining gates

Focused local tests: `python3 -m pytest tests/test_maintenance_*.py`.
Separate-filesystem cases require an explicitly disposable `M3_BACKUP_ROOT`.
Actual native composition additionally requires `M3_NQ_BIN` and its exact
`M3_NQ_SHA256`. Skipped cases do not earn integration qualification.

Current tests cover typed contents/identical-count substitution, real restore,
compaction, incomplete custody, duplicate/refused steps, interrupted swap with
held rollback, held application startup, cleanup/release separation, post-cut
writes, separate temporary/final space refusal, exact-threshold acceptance,
stale/mismatched baselines, and fresh NQ final-floor refusal despite successful
enactment.

Still required before candidate acceptance: complete interruption/control matrix,
independent review, exact real AG/Docket per-step unit qualification, retained
native source/consumer evidence and controlled dogfood readiness. Existing
discovery classification test failure reproduces on the untouched baseline;
it is not silently deleted or counted as passing. Fixture success earns readiness
for controlled dogfooding, not an observed reduction in production operator toil.
