# Labelwatch production disposition — 2026-10-12

**PRODUCTION_ACTIVE_AND_VERIFIED.** Activated at 00:24:18 UTC (October 11, 8:24 p.m. EDT).
Public product: https://labelwatch.neutral.zone/ . Existing essential acceptance passed; no further activation approval requested.

| Release | Exact identity |
|---|---|
| Labelwatch installed source | `aef40df17c17ef9746e4d03cd080ad096808945c` |
| Labelwatch branch | `lane/labelwatch-final-qualification-20261009` |
| Facts API installed source | `f320f8e2cb4794a2539542f8e89e65b982c88f36` |
| Facts branch | `lane/labelwatch-freshness-20261008` |
| Production acquisition | `384010d6-09bb-4185-beb2-74027da95a97`, actual acceptance clock, no imported history/cursors |
| Cutover run | `a563f22b-74af-446f-9251-63462c8da4d3` |

Private evidence root: existing `final-qualification-20261009/`; the cutover subdirectory is `cutover-a563f22b-74af-446f-9251-63462c8da4d3/`.
Source archives, wheel hashes, authority, admission and script identity are bound in its `AUTHORITY.json` and the existing release-input receipts.

| Obligation | Accepted evidence |
|---|---|
| Locked Python 3.10 install; full-scale exports; refusal/recovery | Existing qualification receipts retained; no expensive qualification repeated |
| Archive-path correction | `aef40df`; 36 focused cases locally and on CPython3.10; independent review `2feba853`; manual original-store recovery `cbfcb7ae` |
| Authentic scheduled maintenance | October12 00:05:00–00:05:12, invocation `782badc6d2194e2d94feb91f7d32b30e`; generation2/frontier advanced, October11 segment retired, ingestion resumed; acceptance `4df1933b` |
| Physical reclamation in scheduled occurrence | Sampled store allocation 278,740,992 → 133,943,296 bytes; original failure retained separately |
| Production collection | Four natural samples over six minutes: accepted rows 0→14, five provider cursors changed; fresh subsequent observations, truthful partial/unknown coverage |
| Public product | Account HTML, bounded export creation/page, and the account's own download link return200; `supported-probes/PRODUCT-CHECKS-SUPPORTED-LINKS.json` |
| Public facts and boundaries | Anonymous fresh overview/instruments200; administrative/per-subject Facts routes and product `/health`404; `PUBLIC-BOUNDARIES.json` |
| Monitoring | Qualified watcher timer active; L1/L2/L5–L9/L11 ok, L3/L4 correctly skip a due time preceding this acquisition; `WATCH-REPORT.json` |
| Operator route | Existing accepted Slack operations route only; truthful activation notice HTTP200 at00:30:02; `SLACK-ACTIVATION.json`. No PagerDuty configuration/test |
| Independent production acceptance | `.lanes/evidence_reconcile/03d98d3a-7531-4b54-a50b-ed4c9d8972e2/ACCEPTANCE.json` |

Probe limitations are retained honestly: the first probe incorrectly selected a legacy nullable target column; successors requested an unsupported one-day interval and used an export cursor for an account download. Those failures were not service defects and are not relabeled as passes. Final probes use the successor account index, supported seven-day interval and actual published download link. No runtime correction followed those probe errors.

## Capacity and custody

Cutover admitted 79,342,313,472 free bytes against 76,986,542,080 required: existing32GiB reserve + measured29,489,385,472-byte transition peak +2,400,000,000 overlap +2GiB stage +8GiB tenant/uncertainty allowance. No reserve weakened. Warning budgets:32GiB visible store,256MiB WAL,one-day lookahead; no automatic history reduction.
At00:35 root free79,338,233,856 bytes,9,111,676 free inodes; actual production store942,080 bytes, watcher86,016 bytes. These are measurements of initial operation, not extrapolated full-window success.

Owner-approved exact ten-path retirement recovered50,892,075,008 allocated bytes. NAS historical custody and verified offline recovery remain intact. Previous executable/configuration and current cutover Caddy/hold copies remain available. `STORAGE-CUSTODY-CLOSEOUT.json` records current filesystem samples and retained artifact dependencies. No new VM or SSH identity was created; existing owner keys were not loaded into an agent.

## Operation and recovery

Collection, daily maintenance, overview and watcher timers are active; product and Facts services are active. Fixed30-day history fills from actual acquisition; unavailable providers and known gaps remain explicit. The first production scheduled maintenance is due **October13 00:05UTC (October12 8:05p.m. EDT)** and is ordinary monitored operation, not a claim already earned. Existing seven-day operational observation/issue-closure milestone remains; no new engineering gate is introduced.

On critical failure use the existing [held recovery procedure](CUTOVER-PLAN.md): restore hold, stop in-flight successor services/timers, preserve new store/effects, remove activation under recorded authority, publish held facts and restore only the Labelwatch public route after verifying current configuration identity. Historical restore needs separate capacity admission and does not restore legacy operational authority. A retained executable is not evidence that reversing new writes is safe.

The original off-production observer continues to its already scheduled01:20UTC terminal; its original VM has a finite runtime. Inspect that terminal without duplicating/restarting it. Failed-store copy, exact runtime and recovery artifacts retain named acceptance/replay dependencies; ambiguous17.18GB fixture remains untouched pending ownership resolution. Detailed closeout stays local; only sanitized outcomes go to existing GitHub issues/project.

No owner decision is required for continued normal operation. No new features, integration or architecture work is proposed.
