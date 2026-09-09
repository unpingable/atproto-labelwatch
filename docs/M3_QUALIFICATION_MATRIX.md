# M3 qualification matrix and remaining gates

This is the complete mapping of the fifteen agreed cases, not a claim that
M3 is accepted. Historical independent helper acceptance is b92eeb4 + NQa94ca44
(35 cases). Cleanup v2 and its AG resolver are newer candidates; native execution
remains NOT_RUN while the explicit build-space reserve is unavailable.

`I` below means `tests/test_maintenance_interruptions.py`; `S` means
`tests/test_maintenance_step.py`; `N` means `tests/test_maintenance_native.py`.
These are source/test locations, not substitutes for exact run records.

| Required case | Implemented control / observed evidence | Remaining qualification |
|---|---|---|
| 1 Normal operation | S distinct service/cleanup/release; N actual main/discovery and fresh NQ pre/post; helper b92 independent pass | New cleanup-v2 native + resolver, actual governed units and retained VM evidence |
| 2 Insufficient temporary space | I `test_insufficient_actual_target_space_refuses_before_backup_or_staging`; zero available space substituted at filesystem boundary; source preserved, no backup/staging | New test independent rerun; actual VM admission refusal |
| 3 Source changes before cut | I changed-source test; real SQLite contents mutation prevents staging | Integrated source pin rerun |
| 4 Copy exists but restore/application fails | Artifacts copy-success-not-restore test; I restore_application interruption; retained incomplete artifacts | Actual per-step VM refusal/reconciliation evidence |
| 5 Equal counts/cursors, changed rows | Manifest typed-content controls; artifacts same-count reopen refusal; cleanup observer rereads changed actual backup despite integrity ok | Cleanup-v2 native execution on actual observed copies |
| 6 Compaction failure | I compact interruption preserves original source and partial backup, no install | Governed failed attempt retained without retry |
| 7 Controller loss at custody seams | Hold partial-install, S between-renames, I stage/cleanup/release completion loss and explicit reconciliation | NOT COMPLETE: reviewed VM interruptions must enumerate each admitted step's before-start, STARTED, effect-before-terminal and terminal cuts; no blanket all-seams claim |
| 8 Pathname replacement during recovery | I same-bytes/new-inode original refuses recovery; original retained separately and not silently re-enrolled | Independent rerun and VM recovery exercise |
| 9 Service start failure/no retry | I missing-service refuses acceptance; generated unit Restart=no; source hold remains | Actual systemd failed-start case with Docket custody and no new attempt |
| 10 Post-start verification failure | I post_start_contents real mutation refuses service acceptance and retains original/hold | Integrated governed candidate rerun |
| 11 Released writes/stale rollback | S real post-cut writes produce FORWARD_RECOVERY_ONLY; I released hold refuses rollback | Actual governed release + resulting generation and refusal record |
| 12 Unknown resumption | I release record before terminal/no writes remains INDETERMINATE, not safe rollback | Actual interrupted release custody/reconciliation |
| 13 Cleanup without sufficient final relief | I final-floor case keeps hold; S accepts the exact threshold and refuses stale/mismatched baselines; N fresh filesystem evidence refutes an unmet required floor | Cleanup-v2 and integrated final output must not infer success from effect completion |
| 14 Duplicates/concurrency | S exact duplicate returns prior result; I concurrent operation lock refuses before STARTED; distinct held process identities; no overwrite candidate generation | Real AG/Docket duplicate/no-retry path; direct non-enrolled writers remain excluded operational premise, not tested universal exclusion |
| 15 NQ disagrees with enactment | N actual fresh insufficient-space observation REFUTED after completed cleanup; independent copy observer ignores helper claims | New cleanup-v2 native and AG refusal at exact freshness/identity/unknown boundaries |

## Source-preparation results

New NQ tests-only0f1a87b adds final freshness equality, just-before-boundary,
future final witness and opening/final PID/start substitution. Native cases are
NOT_RUN, not credited as passes. The small Python interruption + copy-observer
selection passed14 cases after adding cases2/8; no compile or production access.
The exact subsequent all-source/integration pin still needs its own evidence.

## Fixture readiness

1. Restore explicit build reserve before compiling; use existing target only.
2. Build/test exact NQ cleanup-v2, run actual app observations through that binary,
   retain cleanup source/request/receipt, then exercise exact AG resolver tests.
3. Independent review includes executed controls, not merely static source review.
4. Stage owner integrates the existing AG→Docket→digest-named unit chain. Native
   applicability must remain capped at original final-witness expiry through
   issuance/execution-standing; a fixture60s standing cannot renew a30s fact.
5. Freeze package/driver/checker pins, require the existing24GiB VM preflight and
   root port/custody approval, run the finite positive/refusal/interruption matrix,
   retain all owner records and teardown. No existing terminated VM is reusable.

## Production dogfood readiness — not authorized

Current operational inventory is NOT_OBSERVABLE: the privileged read-only SSH
query was rejected before execution pending explicit user approval. Companion
deployment/handoff records are historical, not current machine facts.

Before a production request, identify actual host, deployment/source/interpreter,
all writer units and direct jobs, database/schema/journal/size, mount/device/free
space, separately durable backup destination, and exact held source cut. Measure
full manifest/backup/restore/helper read duration to choose finite enrolled
acquisition and execution budgets; the small fixture25s/30s deadlines and7200s
contract ceiling are not evidence of production sufficiency. Specify the exact
root-enrolled immutable per-step units/helper/imports/input custody, restart=no
overrides for both writers, service readiness, deletion target and explicit
rollback/forward-recovery boundaries. Request approval for that exact procedure
only after fixture qualification; no production effect follows from this note.
