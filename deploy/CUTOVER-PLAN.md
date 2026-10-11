# Labelwatch clean-start cutover and retirement plan — deployment readiness revision 6 (2026-10-10)

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


Retirement execution scope is now the exact [verified manifest](RETIREMENT-MANIFEST.md) and its bound
JSON target list. Generation-2 backup and local recovery completed at 2026-10-10T05:21Z; cold catalog and
state recovery passed. No historical production deletion has occurred. The owner's approval of that
manifest is required before removing its ten enumerated paths; older directory-total estimates below
do not authorize broader removal. Recheck actual root free space against the existing 32 GiB threshold
after approved retirement, then resume this qualified deployment procedure under the existing authority.

**Owner scope amendment, 2026-10-10: closure mode.** No new integrations, investigations, or acceptance
gates. Existing Constellation Slack notifications are retained; the owner supplied visible trigger and
resolve messages and accepts that channel. PagerDuty enrollment, incident acknowledgement and test
delivery are removed from Labelwatch readiness requirements, not marked passed. No further PagerDuty
tests. Native Monitor #18 and NQ #27 work remain with their owning projects and do not block Labelwatch
on notification infrastructure work. Preserve existing monitoring, predictive warnings, missed-maintenance
detection, capacity refusal and truthful recovery. Older P2/P3 and Phase C requirements for a new
notification integration are superseded by this explicit owner decision. Essential production capacity,
rollback custody, scheduled maintenance and post-cutover health checks remain. Stop after those pass.

Status: engineering lanes `lane/labelwatch-clean-start-20261008` (labelwatch) and `lane/labelwatch-freshness-20261008`
(facts API). **This document does not grant authority.** The owner's overnight mandate now authorizes routine
operational acceptance. It also authorizes a non-destructive cutover only after all eight stated acceptance
conditions pass.
`final-qualification-20261009/OVERNIGHT-OWNER-AUTHORITY.json` records that mandate. References below to
separate approval for non-destructive work are subject to that later mandate; they do not require repeated
permission for already authorized work. Historical production deletion and destructive retirement remain
separate owner-only gates. Production mutations so far: NONE.

Current admission is **REFUSED**: the 2026-10-10T02:48:03Z read-only preflight measured
30,266,048,512 bytes available on Zone `/`, below its established 32 GiB reserve by 4,093,689,856 bytes.
Even the bounded 2 GiB held installation requires 36,507,222,016 bytes free, a 6,241,173,504-byte shortfall.
See `STAGED-AUTHORIZATION.md` and the timestamped production receipts. The conditional cutover authority
does not waive this gate. No historical data may be removed to make the arithmetic pass without approval.
PagerDuty must use the owner's **one shared escalation policy**; no second policy or paid plan is required
or authorized. Provider HTTP acceptance alone does not establish operator delivery.
Revision 2 folds in the Phase D acceptance review (`REVIEW-PHASE-D-OPUS-20261009.md`, 12 corrections), the target-host
facts (`CAPACITY-ADMISSION-20261009.md`) and the release engineering of 2026-10-09 (cp310 lock, installer).

Candidate runtime and installed-unit source: Labelwatch `397d98e00213a0bd787872f5d5d8b5a6d64c9b62`,
branch `lane/labelwatch-final-qualification-20261009`, based on the historical clean-start lane at
`7c96c6407cf490416b97ac0e2f57749053d6c0a5`. Facts API remains
`f320f8e2cb4794a2539542f8e89e65b982c88f36`. Bind any subsequent documentation-only commit and its
archive in the owner-reviewed release manifest; do not deploy a moving branch. The finalization delta
contains corrected operator procedures, the preserved offproduction substitute-monitor draft and its
regressions, and the installed collection unit's exact `127.0.0.53/32` DNS-stub allowance. No dependency
upgrade or store-format change is introduced. The allowance is address-scoped; validate the destination's
resolver configuration before staging rather than broadening the network policy implicitly.

Final-qualification evidence is under the existing private campaign's
`clean-start-20261008/final-qualification-20261009/`. The CPython 3.10.12 / SQLite 3.37.2 replica passed
387 Labelwatch tests and 90 Facts API tests (28 external-surface skips), installed from the hash-locked
24-wheel environment. Source3c92f465 carried the same runtime modules as installed63c9e5c; the later
installed-unit and draft-monitor deltas have separate regression and independent-review receipts.
A demonstrated replica reserve refusal exposed collector misclassification: per-source errors and
round exit zero. Candidate397d98e corrects only trusted store-capacity refusal classification and
pre-network admission. Sixteen focused regressions passed independently and on CPython3.10 (5.20s);
installed-refusal successor e132637e passed with exit1, exact reason and unchanged1,178events/index/cursors. The broader host collector suite
was interrupted after demonstrated stalled progress (exit130); its partial output is not a pass.
The accepted export run `87e663cd-1cb0-45b7-89c5-39b815698048` completed four default-cap intervals,
355,600 rows total, with independent row/count/hash correspondence. Its 121,769,984-byte fixture and
377,114,624-byte RSS peak do not establish full-store latency. Replica initialization and held Facts API
refusal passed. Corrected installed-unit ingestion, recovery controls and the real UTC maintenance soak
remain subject to their terminal receipts. These results are qualification evidence, not production approval.

Historical qualification limits: C1 at bd21cf5 observed only ten accepted rows and disclosed bootstrap,
request-spacing and lost-attempt defects, subsequently fixed with finite tests. It does not prove sustained
final-source collection. C2 at 7bcb655 earned 42M capacity only: three exports expired under its old TTL,
and its pre-v8 store cannot qualify the final source. Its transition sampler including open-unlinked scratch
peaked at **29,489,385,472 B**, exceeding the 24.3 GB visible-file sample. C3-full at 5a0cc9e earned
three simulated replenished days. C4-full rerun at 70d3471 earned eight-reader concurrency and completed
50,400-row exports with about 5% deadline headroom. Neither automatically binds post-run fixes or proves
target-host sustained capacity. Record final bounded tests and their limits rather than re-label these runs.

## 0. Preconditions

| # | Precondition | Evidence | Status 2026-10-09 |
|---|---|---|---|
| P1 | Successor independently accepted off-production (controls, C1, C2 capacity, C3-full, C4-full) and on the Ubuntu 22.04 / CPython 3.10 replica (host soak) | receipts under `evidence/`, `host-soak/` | OPEN — Python3.10 tests, original producer reconciliation and targeted export acceptance complete; real-host soak and controls pending; retain historical limits above |
| P2 | NQ #27 resolved or an explicitly accepted bounded substitute operator path | NQ #27 closure or substitute receipt | OPEN — separate NQ #27 remains open; historical crash-loop evidence is not a current recovery receipt |
| P3 | Monitor #18 obligations enrolled for the successor with a proven negative control | `PHASE-C-NQ-MONITOR-RECONCILIATION-20261009.md` + receipt | OPEN |
| P4 | Generation-2 final NFS snapshot complete and verified (step 1) | Final NAS `MANIFEST.json` SHA256 `af9b1d738d53b335fb62e02fce0ac549247951f4e85da865bd9092afcaa692bb`; both verification records; local recovery | PASS at 2026-10-10T05:21Z, continuation `6bf6b04e`; prior setup failures preserved; retirement separately awaits approval |
| P5 | Authority for the exact action, with admission and acceptance conditions satisfied | overnight mandate and action receipts; separate destructive approval | Conditional non-destructive authority received; conditions remain open; retirement NOT authorized |

## 1. Generation-2 NFS snapshot (non-destructive; a production READ and a 45.4 GB egress)
Procedure: `LABELWATCH_SNAPSHOT_AUTHORIZED=<approval receipt path> generation2-nfs-snapshot.sh --execute`. The receipt
must contain `"approves": "generation-2-nfs-snapshot"` and `"script_sha256": "<sha256 of the script>"`; any other value
refuses. Files: labelwatch.db (45,043,810,304 B, inode 525204), -wal (14,605,432 B), -shm (32,768 B, rebuildable),
labelwatch_state.db (293,515,264 B; header-identical to the gen-1 archive state db df299df1…, unproven until hashed),
build-result.json (`accepted:false`, `non_event_tables_equal:false` — carried into the manifest), .labelwatch-generation,
plus `receipts/frontdoor/` (5 index-audit receipts, 57 KB). Method: files quiescent under the hold (labelwatch/api/
lock-watcher stopped 15:14:27Z, discovery SIGKILLed 15:14:42Z Result=timeout, last WAL write 15:14:36Z, -shm last written
15:16:32Z by a read-only open); NO checkpoint; verbatim rsync over ssh into `…/generation-2-final.partial`; preconditions
asserted before and after the pull with a stat-record diff; sha256 on zone and on NFS; `quick_check`, event range and
`last id > 112989394` asserted on a **crow-local working copy** of db+wal (the NFS copy is never opened by SQLite: a
read-only open rewrites -shm and WAL over NFS is unsupported); manifest written only after all assertions; NFS hashes
re-checked; archive made read-only; rename to `/tank/nfs/labelwatch-archive/2026-10-06/generation-2-final/`.
Estimates (Oct 1 pull): zone sha256 ≈ 2–3 min, pull ≈ 15–20 min at ≈ 49 MB/s, NFS sha256 ≈ 5 min, local quick_check unmeasured
(the 45 GB copy on /data, 709 GB free). Known facts recorded in the manifest: legacy collector gap 2026-10-01T21:00:41Z →
2026-10-02T02:00:56Z; NFS `deltas/20261005T060809Z-98c7308f/` is **empty** (Oct 5 evidence lives only on zone
`/root/labelwatch-retention/…`, `/mnt/zonestorage/labelwatch-maintenance/…`, crow `/data/git/.scratch/labelwatch-retention/`).
Not in the snapshot (unique, lost only if a later sibling disposition removes it): `/var/www/labelwatch` last render
(430 MB, not on NFS), `/var/log/labelwatch-lock-traces`, `/var/log/labelwatch-soak.log`, the failed transient unit
`labelwatch-delta-export-20261005T060809Z-98c7308f` (capture `systemctl show` before any reboot), `cache/climate`.
Rollback: none needed (zone unchanged).

## 2. Existing producer and reader fencing (verify; the first change is owner-separate)
Today the fence is ONE file: `/etc/labelwatch-operator-safe-hold` (198 B, Oct 6 15:14:27Z) via `zzzz-operator-safe-hold.conf`
drop-ins. All four legacy services are still `enabled` (`WantedBy=multi-user.target`) with `Restart=always` (labelwatch, api,
lock-watcher) / `on-failure` (discovery); `labelwatch-soak-check.timer` is disabled but has a Persistent stamp; the crow
user timer `labelwatch-retention.timer` is disabled, `Persistent=true`, stamp 2026-10-05 06:08:09Z exists ⇒ re-enabling fires
immediately. Verification commands: the exact list in `REVIEW-PHASE-D-OPUS-20261009.md` §5 (unit show/cat, wants symlinks,
`lsof +D`, `fuser`, `lsof +L1`, cron/at, docker hostconfig, factsapi ReadOnlyPaths; crow timer/stamp/ACTIVE.json sha).
Owner-separate fencing step (reversible with `unmask`): `systemctl disable --now` + `mask` the four legacy services and
`labelwatch-soak-check.timer`; `systemctl --user mask labelwatch-retention.timer` on crow. The hold marker STAYS.
After retirement, `90-root-generation.conf` makes labelwatch/api/discovery refuse (exit 71) but NOT lock-watcher or
soak-check — masking is what holds "no automatic re-enrollment of the old collection path".

## 3. Supersession record for the October 5 occurrence (#6)
Write, never edit or delete: `SUPERSEDED.json` beside crow `/data/git/.scratch/labelwatch-retention/ACTIVE.json`
(ACTIVE.json retained byte-unchanged, sha256 126a3f17… recorded in the receipt — it is itself the fence that refuses a fresh
occurrence). Runtime release `7c96c64` makes `tools/label_events_retention_cycle.py` refuse both fresh and `--resume` runs when
SUPERSEDED.json exists (commit d49b158, tested); the crow timer is masked in step 2. Zone evidence under
`/root/labelwatch-retention/20261005T060809Z-98c7308f/` and the NFS delta dir (empty) are untouched. Comment on #6.

## 4. Old-generation retirement on zone root (DESTRUCTIVE — its own approval class)
Scope: generation-2 (45,362,495,488 B alloc) and the cold catalog `cold/catalog-cc5ab1c4…` (5,529,591,808 B alloc),
total 50,892,087,296 B. Expected free on `/` after: 30,496,239,616 + 50,892,087,296 = **81.39 GB** (measured 14:25Z; re-measure
at execution; 7.5 GB of deleted-but-open files make avail drift). Explicitly NOT retired here: `/var/www/labelwatch`
(factsapi has `ReadOnlyPaths=/var/www/labelwatch` without `-`; deleting it breaks factsapi on its next restart until the
step 5 drop-in is in place), `/var/www` group ownership (weatherwatch writes via the `labelwatch` group), `.prev`,
`.before-*`, `.report-tmp-*` (2.92 GB), `review-ceac8c8-correct`, `pre-volume-20260913` (ambiguous duplicate of the state
db), `/opt/labelwatch` releases (keep b5e1a5 while legacy rollback is claimed), `/mnt/zonestorage/labelwatch*` stubs —
owner disposition separately. Procedure when approved: re-run the full §2 fencing list; `stat -c '%i %s %Y'` of each file
equals the snapshot PRESTAT; capture `systemctl show labelwatch-delta-export-20261005T060809Z-98c7308f`; re-hash the NFS
cold-catalog copy `deltas/delta-20261003-001/frontdoor-cold-catalog.sqlite` against cc5ab1c4… (no receipt exists for the
claimed 2026-10-08 re-hash) and the gen-2 snapshot `sha256sum -c`; remove the two root symlinks first; `rm` the files;
`df -B1 /` before/after → receipt with measured reclamation. **Rollback = data-custody restore only**: copying 45.4 GB back
from NFS requires a new reserve/headroom admission while the successor store exists; public pages and legacy operation are
not restorable (hold removal would also un-gate the successor units). Label it so in the approval.

## 5. Successor install (non-activating; Python 3.10 host)
Before staging, the owner-reviewed manifest must bind exact source revisions, archive/wheelhouse hashes,
lock ABI, evidence receipts, complete admitted delta and rollback configuration. Resolve immutable source inputs
on the source host; these commands prepare inputs only after allocation admission and staging approval:

```bash
LABELWATCH_BASE=$(git -C /data/git/atproto-nutrition/labelwatch rev-parse --verify '7c96c6407cf490416b97ac0e2f57749053d6c0a5^{commit}')
FACTSAPI_SHA=$(git -C /data/git/atproto-nutrition/factsapi rev-parse --verify 'f320f8e2cb4794a2539542f8e89e65b982c88f36^{commit}')
# OWNER_REVIEWED_RELEASE_SHA is the exact full commit from the reviewed manifest.
LABELWATCH_SHA=$(git -C /data/git/atproto-nutrition/labelwatch rev-parse --verify "${OWNER_REVIEWED_RELEASE_SHA:?reviewed manifest required}^{commit}")
git -C /data/git/atproto-nutrition/labelwatch merge-base --is-ancestor "$LABELWATCH_BASE" "$LABELWATCH_SHA"
# Reviewer must match every delta since BASE to the admitted corrections and their qualification receipts.
git -C /data/git/atproto-nutrition/labelwatch diff --stat "$LABELWATCH_BASE" "$LABELWATCH_SHA"
git -C /data/git/atproto-nutrition/labelwatch archive --format=tar --prefix=source/ "$LABELWATCH_SHA" | gzip -n > "labelwatch-$LABELWATCH_SHA.tar.gz"
git -C /data/git/atproto-nutrition/factsapi archive --format=tar --prefix=factsapi/ "$FACTSAPI_SHA" | gzip -n > "factsapi-$FACTSAPI_SHA.tar.gz"
sha256sum "labelwatch-$LABELWATCH_SHA.tar.gz" "factsapi-$FACTSAPI_SHA.tar.gz"
```

Compare hashes against the reviewed manifest before transfer and after arrival. New documentation archives
have new hashes; never apply the runtime-base hash to them. Refuse archive identity mismatch.
Stage `/opt/labelwatch/releases/$LABELWATCH_SHA/source` and build its venv offline with
`WHEELHOUSE=<verified cp310 wheelhouse> PYTHON=/usr/bin/python3 source/release/build-venv.sh <release>`.
Require exact cp310 lock hashes, `pip check`, dependency gate and source import checks.
Back up installed unit files, env, Facts API source/config/drop-ins and Caddy configuration with hashes and
owner/mode metadata before any replacement. Preserve the previous exact executable and lock for rollback.
Stop all successor timers and services before sequential installer writes, including in-flight overview.
Run the staged installer as root, using reviewed literal paths:

```bash
"/opt/labelwatch/releases/$LABELWATCH_SHA/source/deploy/install-recent.sh" \
  --release-sha "$LABELWATCH_SHA" --admitted-root /var/lib/labelwatch-recent \
  --pubdir /var/www/labelwatch-recent --reserves "${OWNER_CONFIRMED_RESERVES_JSON:?receipt required}" \
  --round-seconds 30
```

Runtime `LABELWATCH_RECENT_RESERVES` contains the owner's confirmed **unallocated reserve**,
not reserve plus the store's expected allocation. Admission separately requires reserve plus aggregate
new/peak allocations; double-counting the peak inside runtime reserves would prevent use of planned capacity.
Installer creates the admitted root 0750, pubdir 0755 and env 0640 root:labelwatch; installs only seven
successor units, reloads/verifies systemd, enables nothing and touches no marker. Confirm env paths,
read/write sandbox paths, round 30 (maximum 30), stale 60 > TimeoutStartSec 45, offline locks and all seven
installed units before any start. If placement is interrupted, complete it with all services stopped.
facts API: stage exact `f320f8e2cb4794a2539542f8e89e65b982c88f36` to `/opt/factsapi` (same layout as today), install
`deploy/factsapi.service.d-labelwatch-recent.conf.example` as `/etc/systemd/system/factsapi.service.d/labelwatch-recent.conf`
(overview path → `/var/www/labelwatch-recent/overview.json`; `ReadOnlyPaths` reset to include the new pubdir and
`-/var/www/labelwatch`), daemon-reload, restart factsapi. Rollback: restore hashed prior Facts API source and config/drop-ins, daemon-reload, restart and verify its prior route. Keep successor stopped. Missing successor pubdir causes namespace failure: create/verify it before Facts API restart. The overview publisher is intentionally ungated and can publish hold:true before activation.

## 6. Init (acquisition boundary; non-public)
As documented in `deploy/README-recent.md` (systemd-run under `EnvironmentFile=`, wall clock ±300 s, roster 580 DIDs only,
receipt into a separately writable evidence directory outside the admitted root). Init is permitted while held and has no !hold service condition. Leave both markers unchanged. Pass PYTHONPATH and all env settings through the documented transient unit, never shell-source the JSON env. Refuses a non-empty root and a failing reserve policy. Replica initialization passed at 2026-10-10T01:46:49Z with a fresh acquisition identity; this does not initialize or qualify production.

```bash
systemd-run --wait --pipe --uid=labelwatch --gid=labelwatch \
  -p EnvironmentFile=/etc/labelwatch-recent.env \
  -p "Environment=PYTHONPATH=/opt/labelwatch/releases/$LABELWATCH_SHA/source/src:/opt/labelwatch/releases/$LABELWATCH_SHA/source" \
  "/opt/labelwatch/releases/$LABELWATCH_SHA/venv/bin/python" \
  "/opt/labelwatch/releases/$LABELWATCH_SHA/source/tools/segmented_qualification/recent_init.py" \
  --root /var/lib/labelwatch-recent/epoch-2 --now "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --roster "/opt/labelwatch/releases/$LABELWATCH_SHA/source/deploy/roster-epoch2-seed.json" \
  --receipt "${OWNER_APPROVED_EVIDENCE_DIR:?receipt destination required}/epoch-2-init-receipt.json"
```

This is a finite operator action, not an unattended supervisor session. Before long snapshot/soak work,
use the campaign-approved durable execution mechanism and recovery checkpoint naming run/host, revision,
execution identity, artifacts/logs, expected terminal records and next authorized action. Verify evidence
collection survives supervisor loss; do not duplicate the producer after interruption.

## 7. Selection / activation (conditional owner authority; exact action receipt required)
Precondition: P1–P5 and all A–H conditions in the overnight mandate. Before any hold removal, verify every
legacy service/timer and crow retention timer is masked, inactive and has no in-flight consumer. Record
the evidence satisfying those conditions and the exact hold transition under the existing conditional
authority; do not infer permission from installation success. Remove the shared
`/etc/labelwatch-operator-safe-hold`; then create `/etc/labelwatch-recent-qualified-activation`
(content: approval receipt id + init generation_id) and start:

```bash
systemctl enable --now labelwatch-recent-collect.timer labelwatch-recent-maintain.timer \
  labelwatch-recent-overview.timer labelwatch-recent-product.service
```

The successor collect, maintain and product require hold ABSENT and activation PRESENT. Creating
activation while held cannot start them. Overview and timers are intentionally ungated. Expected first hour: bootstrap of 580 sources; overview `warming_up`; facts API 503 `source_unavailable` until
the first accepted page, then 200 `warming_up` for 30 days. Rollback: restore the shared hold under owner authority, disable/stop all three timers, and explicitly stop all four services (including in-flight collect, maintain and overview). Remove activation only under owner authority. Publish a held overview after work has stopped; restore held Caddy configuration and verify Facts API refusal. Keep legacy units masked. Retain the exact store/evidence if a public effect occurred; an old executable or custody restore does not undo effects.

## 8. Website/API publication
Caddy (`/home/jbeck/atproto/Caddyfile` on zone, served by the `caddy` docker container, `/var/www` mounted at `/srv/www`):
replace the hold block with `phase-e/Caddyfile.labelwatch-successor.snippet` (validated with caddy v2.10.2: routes only
`/ /about /previous-epoch /account /exports /download /lookup /instruments/*` to 127.0.0.1:8431 with a 630 s read timeout
for exports; everything else 404; `/health` and the legacy tree are not served); `docker exec caddy caddy reload`.
`api.neutral.zone/atproto/*` unchanged (already proxies factsapi on 8425). Rollback: restore `Caddyfile.hold` from
`/root/labelwatch-safe-hold-20261006/` and reload.

## 9. Monitoring enrollment and negative controls (P3)
See `PHASE-C-NQ-MONITOR-RECONCILIATION-20261009.md` for the obligation table (seven distinctions), the negative-control chain
and the temporary-monitor proposal if NQ #27 stays blocked.

### Fixed-window capacity and retention-health gate

Before activation, bind the exact watcher source and independently accepted receipts for all four
conditions: L4 stalled window/retirement advancement, L3 missed maintenance deadline, L11 projected
budget breach, and L5/L11 exhausted capacity. Unknown or held evidence cannot resolve an incident.
The current substitute monitor source is included in candidate397d98e; 90 deterministic cases passed
at6228bf5, with unchanged monitor bytes in the candidate. Offproduction local intents establish no
external delivery, native enrollment or production-route acceptance. Monitor #18 remains open.

Record owner-admitted `WATCH_STORE_BUDGET_BYTES`, `WATCH_WAL_BUDGET_BYTES`, per-filesystem reserve and
inode floors, and `WATCH_CAPACITY_LOOKAHEAD_SECONDS` (default86400) before enrollment. The replica's
2GiB store/256MiB WAL settings are qualification policy, never production sizing. An unset or
unidentifiable policy yields unknown. Monitor budget observations do not install an ingestion gate;
physical reserve admission supplies the collector refusal. Operators must intervene on forecast
warnings before reaching that hard boundary.

The fixed30-day estimate uses current visible allocated blocks plus recent positive net growth over
30days, with no future-retirement credit. It excludes open-unlinked/outside-root allocations; use
measured transition peaks and shared filesystem headroom separately. Record accepted-row velocity,
WAL usage and conditional time to each ceiling. A projected breach raises notice, escalating to page
within lookahead; actual exhaustion pages. Keep the30-day contract: do not shorten retention or sample
observations to quiet alerts. On a forecast breach, verify source rate, inventory and filesystem
consumers, admit additional capacity or use the qualified hold/refusal procedure. On maintenance
failure or stalled retirement, retain exact occurrence/journal evidence and hold if headroom cannot
cover recovery. No automatic resize, restart or retirement follows a monitor verdict.

## 10. Acceptance soak and declaration
7 days after step 7: first UTC rotation observed; first maintenance occurrence seen as an operator condition; bootstrap
terminal for all reachable sources; steady-state bytes vs reserve recorded daily; no gap overflow; then "serving (warming
up)" until day 30, then "serving". Only then may #6 be closed as superseded and #7 moved to maintenance.

## 11. Fresh admission, thresholds and current blockers

Crow shared storage has a hard **60 GiB (64,424,509,440 B) unallocated reserve on BOTH `/` and `/data`**.
The old Zone capacity record documents a distinct **32 GiB root reserve from its Oct 6 hold marker**;
this is a historical destination-specific record, not authorization to lower crow's policy or to infer a
new Zone policy. Before ANY destination allocation the owner must confirm the applicable reserve and
filesystem/inode floor, record free blocks/inodes, active tenants, retained occurrences and aggregate
expected allocation, and remeasure immediately before allocation. Refuse if unknown consumers prevent
credible admission. Root owns storage coordination. No alternate filesystem fallback is implicitly approved.

Replace the old 25 GB peak arithmetic with at least 29,489,385,472 B measured C2 transition peak;
include additional VACUUM overlap only to the extent not already inside that peak, plus release staging,
logs, backups and explicit uncertainty/tenant-growth allowance. A conservative separately budgeted
2.4 GB overlap gives 31,889,385,472 B new allocation before those extras. With the historic Zone
32 GiB reserve, the illustrative minimum is **66,249,123,840 B**, not the old 61.76 GB.
With 60 GiB it is **96,313,894,912 B**. Neither figure is current admission or an exact hardware peak.
At old 81.39 GB free after hypothetical retirement, 60 GiB plus that allocation would refuse;
32 GiB would leave about 15.14 GB before extras and concurrent growth. Both rely on fresh measurement
and explicit destination policy confirmation. Never set a lower reserve override merely to make this fit.

Before retirement, verified custody must cover exact gen-2 db+wal and state, cold catalog, frontdoor
receipts, generation/build records, unique incident logs/receipts, and prior configuration/release rollback
inputs. Verify mounts, backing relationships, hard links and live descriptors for every exact removable
object. Read failures are unknown, not proof of quiescence. Use individually enumerated removal targets
from the approval receipt; no broad recursive cleanup. Required private recovery identities, if any,
have separate protected custody and retirement records, never ordinary evidence archives.
A legacy restore needs at least 50,892,087,296 B for gen-2 plus cold catalog, reserve and restore working
space **in addition to** retained successor and other tenants. Allocate only under a fresh admission;
if unavailable, remain held and report restoration blocked. Custody restore is not legacy service activation.

Before publication require the owner-approved monitoring path and deterministic negative-control
receipts. Track distinct held, unknown, collection-stalled (>15 min), maintenance-failed/overdue
(after 00:35 UTC), overview-stale (>45 min), reserve-failure and routing/Facts API states. Missing evidence
means unknown. Reserve refusal, maintenance failure or product stale >1 h requires owner escalation
and the qualified hold procedure; do not repeatedly restart a producer or relax bounds.
Verify exported interval suggestions, expiration refusals, no-store 503 freshness responses and loopback-only
health against final qualified identities; never claim the historical C2 export criterion passed.

Current blockers: authentic scheduled maintenance observation, Monitor #18 operator-path acceptance,
failed numerical production resource admission, and verified gen-2/cold/unique-evidence custody.
Full-scale exports and installed refusal/recovery controls are accepted; see `QUALIFICATION-20261010.md`
and the exact campaign receipts. NQ #27 remains separate; an accepted bounded substitute must satisfy
the recorded monitoring gate before activation. The owner has authorized conditional non-destructive
work, but destructive retirement still requires an exact separate decision.
The seven-day observation and 30-day warm-up declarations are future milestones, not completed qualifications.
This plan executes none of these actions.

Formalization consideration: marker transition claim is bounded to systemd start conditions: held or
unactivated skips collect/maintain/product; removal of a shared hold cannot release legacy work only if
all legacy consumers were separately fenced. Conditions do not stop running services, hence explicit
full-stop rollback. Reviewed source identity is candidate397d98e, whose collection-unit and draft-monitor corrections have
separate regression/acceptance receipts; exact plan result identity is its immutable documentation commit/notice. Counterexamples
include unmasked legacy units, in-flight work, sequential partial installation and insufficient restore space.
Practical source inspection, target-VM marker tests and review suffice for this operational plan; no new
runtime mechanism is introduced. Decision/acceptance owner is the root supervisor and operator.
