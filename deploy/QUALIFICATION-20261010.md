# Final qualification checkpoint — 2026-10-10

**BLOCKED_WITH_EXACT_OBLIGATIONS. Production unchanged.** This is a checkpoint while the existing
durable overnight producer remains active, not acceptance or campaign closeout.

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
| External operator delivery / enrollment |not earned; local intents do not establish provider delivery or human acknowledgement | Monitor#18 remains OPEN |

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
3. Owner must accept a concrete operator path: native Monitor#18 enrollment or the bounded substitute with
   the explicit PhaseC amendment, authorized test-route delivery/acknowledgement, and production enrollment.
   NQ#27 is a separate hosting concern; neither issue closes on local watcher evidence.
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
