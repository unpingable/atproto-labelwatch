# Final qualification checkpoint — 2026-10-10

**Current disposition: PRODUCTION_ACTIVE_AND_VERIFIED, 2026-10-12T00:35Z.** See [production receipt](PRODUCTION-20261012.md) for exact installed revisions, accepted evidence, resource measurements and recovery limitations. Earlier held/failure records below are historical and remain preserved.

## Scheduled failure and bounded correction — 2026-10-11 02:31Z

**WAITING_FOR_REAL_OBSERVATION.** The original replica timer fired at 00:05Z, invocation
`276537e3b7ca46878cd6c9212835a37a`, and exited 1. Installed `RecentStore.archive` reached the base
campaign-only archive destination lookup; `ARCHIVE-DESTINATION.json` was absent. No successful
maintenance or advancement was earned by that occurrence. The 702-sample terminal and failure
journal remain intact (216,346,624-byte sampled store peak; 586,979,585 network bytes).

Correction `aef40df17c17ef9746e4d03cd080ad096808945c` confines recent archives to the exact admitted
store/archive directory without reading the campaign receipt. Base campaign validation remains
unchanged. Thirty-six focused tests passed locally and on installed CPython 3.10, including a store
outside the campaign with its receipt absent and foreign/symlink refusal cases. Independent source
review accepted the bounded change. The requested static sibling check across five runtime entrypoints,
their 24-module import closure and Facts runtime found no other mandatory development-file dependency;
this is a scoped static result, not a claim about every repository file.

Manual recovery of the original failed replica store passed at 02:28:14Z, after preserving its stopped
216,338,432-byte copy. It archived/retired the October 10 segment, persisted maintenance generation 1
and frontier `2026-09-11T02:28:03.334974Z`; current store allocation became 69,541,888 bytes.
Receipt `maintenance-recovery-f5739b4f-b383-4fe3-b273-40b64070af6d/` and independent acceptance
`.lanes/evidence_reconcile/cbfcb7ae-16a2-45b4-a67e-66a31e77d0d2/ACCEPTANCE.json` establish **manual recovery only**.

Corrected source is installed on production under the unchanged hold; offline lock/import/dependency
checks passed. Facts remains `f320f8e2cb4794a2539542f8e89e65b982c88f36`. Collection/maintenance timers remain
disabled; held overview refresh remains active. Root free after correction: **80,592,015,360 bytes**.
Previous source/configuration and verified historical recovery custody remain retained.

The next genuine replica maintenance is **2026-10-12T00:05Z (October 11, 8:05 p.m. EDT)**.
Existing sampler reused as occurrence `0d0b39db-3f6c-4870-a10a-7d644d495bc6`; unit
`labelwatch-maintenance-followup-0d0b39db.service`, invocation `a0d8e86e98824a56b0354bea75562685`.
It ends 01:20Z with unchanged 2 GiB store/4 GiB network limits and host reserve guards. Original VM PID
909951 remains, with finite runtime extended to 50 hours from its original start, without restart.
No new VM, key or framework. `CURRENT-MAINTENANCE-FOLLOWUP.json` and master `RECOVERY.json` bind
admission, producer and resumption instructions. Preserve the prior failed occurrence. Once real
scheduled success is accepted, resume the existing authorized cutover; manual recovery is not its substitute.


## Execution update — 2026-10-10 05:38Z

Disposition: **WAITING_FOR_REAL_OBSERVATION**. Owner explicitly approved the ten-path manifest at
`ba15c34f686aab1d2e23d073dd3795a91f0581c6`. Retirement run
`75742698-0f0f-4a4d-be81-aac480cde279` completed at 05:36:49Z after exact metadata/content checks,
no symlinked parent traversal, and root descriptor/mapped-file checks found no active consumers.
Exactly two symlinks and eight regular files were removed; no directories or additional historical
material were removed. Legacy services/timers are masked with original unit files retained.
The crow retention timer is masked and the existing occurrence has a separate supersession record.

Exact allocated file bytes removed: **50,892,075,008**. Root available bytes increased from
30,280,138,752 to 81,225,633,792; the extra 53,420,032-byte difference is concurrent filesystem change,
not attributed to retirement. After held installation root had **80,735,043,584 bytes** and
9,193,602 free inodes; the existing 32 GiB reserve is unchanged.

Installed Labelwatch `397d98e00213a0bd787872f5d5d8b5a6d64c9b62` and Facts API
`f320f8e2cb4794a2539542f8e89e65b982c88f36` from verified immutable archives. Offline Python 3.10
installation passed exact 24-package lock, dependency and import gates. Seven successor units are
installed. Only the overview timer is enabled to refresh explicit held status every 30 minutes;
collection and maintenance timers remain disabled. Facts API was replaced with its qualified source and existing reviewed
drop-in; its previous source/configuration and Caddy configuration are retained for rollback.
Public instruments at 05:38:23Z report Labelwatch PRESENT with freshness_state held; weatherwatch
remains PRESENT. No collection, production store initialization, activation marker, hold removal or
public product-route cutover occurred. Historical recovery custody remains available independently.

The original replica observer is active, with 115 samples, 19,755,008-byte observed store peak and
145,865,311 network bytes at 05:37:43Z. Its required authentic maintenance is due
**2026-10-11T00:05Z (October 10, 8:05 p.m. EDT)**; terminal observation is expected by 01:20Z.
Do not restart, duplicate or accelerate it. Inspect its actual maintenance/horizon/terminal receipts,
then resume the existing conditionally authorized activation and public checks after acceptance and
fresh admission. No new owner approval is required for that already-authorized path.

Detailed private evidence: existing campaign `final-qualification-20261009/retirement-75742698-0f0f-4a4d-be81-aac480cde279/`
(`RESULT.json`, `run.log`, `ADMISSION.json`, `INSTALL-RESULT.json`, `install.log`, `HELD-POSTCHECK.txt`,
`PUBLIC-FACTS-HELD.json`). Master `RECOVERY.json` records exact recovery/supervision instructions.
Runtime/recovery artifacts remain retained for activation and rollback; this is an interim custody
handoff, not whole-campaign cleanup. Only sanitized status is published to GitHub.


**BLOCKED_WITH_EXACT_OBLIGATIONS. Production unchanged.** This is a checkpoint while the existing
durable overnight producer remains active, not acceptance or campaign closeout.

Custody update, 2026-10-10T05:21Z: generation-2 backup is verified on the separate NAS and recovery on
crow passed for 45,137,618 events. State and cold-catalog recovery also passed. The exact ten-path
[retirement manifest](RETIREMENT-MANIFEST.md) proposes 50,892,075,008 bytes of reclamation; **owner
approval is still required and nothing has been deleted**. Production remains held. The original real
maintenance observation remains scheduled for 2026-10-11T00:05Z; no new qualification is added.

**Latest owner amendment: closure mode.** Keep the existing Constellation Slack route, whose trigger and
resolve messages the owner supplied and confirmed visible. No new notification integration or further
delivery tests. PagerDuty-specific enrollment/acknowledgement gates are withdrawn, not passed. Native
Monitor #18/NQ #27 work is not a Labelwatch activation dependency. Existing protective behavior and
monitoring remain intact. Remaining essential work is production capacity admission, rollback custody,
the authentic scheduled maintenance observation, and bounded cutover/health verification.

Operational update at 2026-10-10T03:07Z: conditional non-destructive cutover authority is recorded in
`OVERNIGHT-OWNER-AUTHORITY.json`; historical production retirement remains unauthorized. Production
root free space measured at 02:48:03Z is 30,266,048,512 bytes, below the existing 32 GiB reserve. The public
Facts API returned Labelwatch `UNAVAILABLE` at 03:07:27Z (`PUBLIC-FACTS-PREFLIGHT-20261010.json`). The operator reports repairing PagerDuty plan eligibility;
one escalation policy must serve both services. Actual incident visibility and acknowledgement remain
unverified. HTTP 202 for test run `9682993e` establishes provider acceptance only.

## Immutable inputs

| Repository | Branch | Identity |
|---|---|---|
| unpingable/atproto-labelwatch | lane/labelwatch-final-qualification-20261009 | release candidate `a6cbccc93914460b1b4aaaf3d6df99fb53ffdba9`; installed runtime `397d98e00213a0bd787872f5d5d8b5a6d64c9b62`; only reviewed cutover-plan documentation differs |
| Labelwatch historical lane | lane/labelwatch-clean-start-20261008 | `7c96c6407cf490416b97ac0e2f57749053d6c0a5` |
| unpingable/atproto-facts-api | lane/labelwatch-freshness-20261008 | `f320f8e2cb4794a2539542f8e89e65b982c88f36` |

Candidate archive SHA256: `20334159e7fc7756b1582dfe1ba2748975650b64f6524290c7d62d72f5d01e20`.
No merge, push or main-branch change. Runtime requirements remain Ubuntu22.04.5/CPython3.10.12/SQLite3.37.2,
with24 hash-locked wheels. No dependency upgrade or schema redesign.

Evidence root E is the existing private campaign:
`/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-segmented-horizon-capacity-20261005/clean-start-20261008/final-qualification-20261009`.
`E/RELEASE-INPUTS-a6cbccc.json` binds archive, locks, every wheel and acceptance pointers.
`E/RECOVERY.json` binds the active producers and next authorized actions. Local receipts are evidence;
GitHub Projects remains the execution tracker.

## Qualification matrix

| Obligation | Earned result and limit | Evidence under E |
|---|---|---|
| Locked Python3.10 |baseline source3c92f465 (runtime unchanged through installed63c9e5c):387 Labelwatch tests;90 Facts API tests,28 external-surface skips; installation/startup/init/held response passed | `cp310/`, `SOURCE-CORRESPONDENCE.json` |
| Corrected collector |16 local/independent regressions;16 under locked Python3.10 in5.20s; WAL/hold2 checks passed | `cp310-capacity/`; acceptance `f8c5a2d5` |
| Installed exhaustion refusal |actual impossible reserve: exactreason, exit1, unchanged1178events/index/cursors/acquisition; temporary policy removed and collection resumed | `controls-e132637e-5228-45fc-9e25-c75ccf323c86/`; acceptance `d4a53813` |
| Restart, hold, actual kill |restart/hold passed; persisted attempt followed by realSIGKILL and65s stale interval disclosed one unknown gap without extending it on repeat | `controls-17d2e3a3-d942-45e7-b934-b49a93993a52/` |
| Default export boundaries |355600events, four complete intervals, independent correspondence | `exports-87e663cd-1cb0-45b7-89c5-39b815698048/` |
| Full-scale account exports |39.2Mretained substitution store;543589rows/5440pages over6intervals passed raw-input oracle; no original-store mutation or product-migration claim | `exports-full-resume-f8f3c5f9-703c-4c31-812e-20cdcba14716/`; acceptance `0e88916b` |
| Predictive capacity / retention conditions |90fixture cases; real310s+311s samples, projected-budget notice, monitor-only exhaustion page, unknown incident retention, six deterministic forecast controls; L3missed deadline/L4stalled retirement covered by fixtures | `.lanes/monitor_reconcile/11d74c2d-86d0-4160-9faf-1be46080d0ee/` |
| Installed refusal to monitor |exact failed collect journal evaluated by exact monitor source yielded local PAGE; live failed-invocation sampling window was missed, so this is receipt substitution | same profile `RESERVE-RECEIPT-EVALUATION.json` |
| Unattended controls |Actual NC1 failure, 16-minute missing-evidence and overdue intervals, fresh recovery, and 31-minute held interval accepted. Original terminal FAIL is preserved: final NC6 command could not find runuser. Separate NC6-only successor passed four fixture cases; no earlier observation was repeated | `.lanes/monitor_reconcile/9f5c6b94-1a12-4895-a803-915cd6036b86/`; successor `c1cfbcb8-6a54-4de6-a1c6-fe3ca5cb478d`; acceptance `23e6e6cd-6097-4c44-8e88-295761575c8d` |
| Actual UTC maintenance / sustained soak |pending real2026-10-11T00:05Z maintenance; guarded observation ends01:20Z; no fabricated clock acceptance | `live-soak/`, `RECOVERY.json` |
| Operator notification route |Owner accepts existing Constellation Slack delivery and supplied visible trigger/resolve messages; no new integration or delivery test required. PagerDuty gate withdrawn, not passed | Owner closure-mode amendment; Monitor#18 remains independently OPEN |

Independent acceptance records live under `E/.lanes/evidence_reconcile/<full-run-id>/ACCEPTANCE.json`;
short IDs above are lookup prefixes, not substitutes for exact recorded identities. The original broad
host collector suite stalled, was diagnostically interrupted once (exit130), and has no pass claim;
`COLLECTOR-TEST-RECOVERY.json` preserves that limitation. Its earlier locked-runtime baseline and current
focused correction results remain separately scoped.

Historical C2 remains PARTIAL:42Mconstructed lifetime/39.2Mretained; old export TTL failure. Its measured
transition peak includes open-unlinked scratch:29,489,385,472B, not only24.3GB visible files. C1's ten
accepted rows were not sustained final-source evidence. C3-full9.9M/threekills and C4eightreaders retain
historical exact-source scope. No expensive constructor was repeated.

## Measurements and limits

- Full-scale export:700.108s; peak processRSS1,373,777,920B; cgroup peak4GiB including pagecache;
  final sampled allocation16,940,351,488B. Six actual intervals5.545–129.466s versus120–599.5s deadlines.
- Smaller export:325.68s; processRSS377,114,624B; allocated121,769,984B.
- First real forecast sample:499712B visible growth over310s and0.919rows/s; conditional30day estimate
  4,180,784,822B exceeded replica2GiB budget, projected crossing15.4days. Later311s sample likewise warned.
  This estimates recent net growth; it is not measured30day capacity and gives no future-retirement credit.
- Visible inventory excludes open-unlinked/outside-root allocation. Shared filesystem depletion includes
  other tenants; do not attribute it to Labelwatch. Qualified transition headroom remains separately required.
- At02:37Z, crow free blocks were397,115,973,632B on`/` and674,636,095,488B on`/data`, both above60GiB.
  Concurrent host changes are not campaign consumption. Detailed dated custody measurements remain in E.

## Exact remaining obligations

1. Preserve the accepted original NC1–5 receipts, original harness-failure terminal, and NC6-only successor
   terminal with acceptance `23e6e6cd`. Both producers are terminal; do not relaunch. Retained control
   configuration has a named custody dependency pending campaign closeout.
2. Observe the existing VM/guard through actual UTC maintenance and inspect terminal/journal/coverage,
   generation advancement, retention and resource evidence. No extra approval is needed for this admitted
   offproduction work. Never duplicate or restart the unresolved producer after supervisor loss.
3. Retain the owner-accepted existing Slack notification path and existing monitoring. No PagerDuty
   enrollment, incident acknowledgement or new integration work; Monitor#18/NQ#27 remain independent.
4. Confirm production store/WAL budgets, applicable filesystem reserves and fresh aggregate admission.
   Replica2GiB/256MiB settings are not production values. Fixed30day coverage never shrinks silently.
5. Complete verified gen2/cold/unique-evidence custody. Non-destructive cutover is conditionally authorized
   by the overnight mandate only after all A–H conditions pass. Destructive historical retirement still
   requires separate exact owner approval. Snapshot/staging/fencing/publication retain their safety gates.
6. Finish storage closeout:17.18GB of exact temporary fixtures are disposal candidates with protocols,
   results, seals and small custody receipts retained. Full consumer inspection was blocked by326
   inaccessible process-FD directories and unavailable sudo authentication. Retain pending an authorized
   complete consumer check; no broad deletion or lowered reserve. VM/key remain needed by the active soak.

Deployment, rollback, unsafe-restore conditions, interruption and retirement prerequisites are in
[CUTOVER-PLAN.md](CUTOVER-PLAN.md). The installed DNS allowance and collector capacity correction are bounded
fixes; the predictive watcher adds observations without changing retention or installing automatic actions.
Production inspection was read-only. One authorized test-service PagerDuty trigger received HTTP 202;
operator visibility is not yet established. No production mutation, snapshot destruction or data retirement
was performed.
