# Labelwatch clean-start cutover and retirement plan — deployment readiness revision 3 (2026-10-09), awaiting owner authorization

Status: engineering lanes `lane/labelwatch-clean-start-20261008` (labelwatch) and `lane/labelwatch-freshness-20261008`
(facts API). **No step below is authorized by this document.** Each production step needs the owner's explicit approval
recorded as a receipt under `clean-start-20261008/evidence/` before execution. Production mutations so far: NONE.
Revision 2 folds in the Phase D acceptance review (`REVIEW-PHASE-D-OPUS-20261009.md`, 12 corrections), the target-host
facts (`CAPACITY-ADMISSION-20261009.md`) and the release engineering of 2026-10-09 (cp310 lock, installer).

Bound runtime: Labelwatch `7c96c6407cf490416b97ac0e2f57749053d6c0a5`; documentation candidate
`3c92f465b76020f4936096acc6b26e226e1763b2` plus this plan's subsequent documentation commit.
Facts API `f320f8e2cb4794a2539542f8e89e65b982c88f36`. Later documentation commit identities must
come from the owner-reviewed release manifest, not a moving branch. Archive correspondence is verified for
runtime Labelwatch SHA256 `6c57a24be0c871d04dd86276d2b6bf89e8bb62bdec77d4a8ba3c6836ab6af252`
and Facts API SHA256 `2822aff0e5886fbccfbf569c70b6efde79242fe8613881b8168bb8bd1d1c4fa5`.
Root's final tests and independent acceptance remain pending; this plan records readiness, not approval.

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
| P1 | Successor independently accepted off-production (controls, C1, C2 capacity, C3-full, C4-full) and on the Ubuntu 22.04 / CPython 3.10 replica (host soak) | receipts under `evidence/`, `host-soak/` | OPEN — root final tests, original producer reconciliation and independent acceptance pending; retain historical limits above |
| P2 | NQ #27 resolved or an explicitly accepted bounded substitute operator path | NQ #27 closure or substitute receipt | OPEN — nqd-ops still XFSZ-looping today (store 1,102,094,336 B > 1 GiB) |
| P3 | Monitor #18 obligations enrolled for the successor with a proven negative control | `PHASE-C-NQ-MONITOR-RECONCILIATION-20261009.md` + receipt | OPEN |
| P4 | Generation-2 final NFS snapshot complete and verified (step 1) | `MANIFEST.json`, `VERIFY.txt`, `VERIFY-POSTCHECK.txt` | prepared; revised script's dry run asserts every precondition (2026-10-09 14:50Z: hold marker present, six legacy units inactive/failed, no descriptors, stat record captured) |
| P5 | Owner approval of this exact plan, per approval class (snapshot/staged, retirement, activation) | signed receipts | OPEN |

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
lock ABI, evidence receipts, documentation delta and rollback configuration. Resolve immutable source inputs
on the source host; these commands prepare inputs only after allocation admission and staging approval:

```bash
LABELWATCH_BASE=$(git -C /data/git/atproto-nutrition/labelwatch rev-parse --verify '7c96c6407cf490416b97ac0e2f57749053d6c0a5^{commit}')
FACTSAPI_SHA=$(git -C /data/git/atproto-nutrition/factsapi rev-parse --verify 'f320f8e2cb4794a2539542f8e89e65b982c88f36^{commit}')
# OWNER_REVIEWED_DOC_SHA is the exact full commit from the reviewed manifest.
LABELWATCH_SHA=$(git -C /data/git/atproto-nutrition/labelwatch rev-parse --verify "${OWNER_REVIEWED_DOC_SHA:?reviewed manifest required}^{commit}")
git -C /data/git/atproto-nutrition/labelwatch merge-base --is-ancestor "$LABELWATCH_BASE" "$LABELWATCH_SHA"
# Reviewer must verify all changes since BASE are the admitted documentation/comments only.
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
receipt into a separately writable evidence directory outside the admitted root). Init is permitted while held and has no !hold service condition. Leave both markers unchanged. Pass PYTHONPATH and all env settings through the documented transient unit, never shell-source the JSON env. Refuses a non-empty root and a failing reserve policy. Replica rehearsal status requires root reconciliation; it is not asserted complete here.

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

## 7. Selection / activation (the one deliberate, reviewed step — its own approval class)
Precondition: P1–P5. Before any hold removal, verify every legacy service/timer and crow retention timer
is masked, inactive and has no in-flight consumer. Owner separately authorizes removal of the shared
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

Current blockers: final root test results/independent acceptance, original replica execution reconciliation,
bounded export-fix evidence or explicitly accepted scope limit, Monitor #18/NQ #27 path acceptance,
fresh owner-confirmed destination reserve/headroom, verified gen-2/cold/unique-evidence custody and
separate snapshot/staging, fencing/supersession, destructive retirement, activation and publication approvals.
The seven-day observation and 30-day warm-up declarations are future milestones, not completed qualifications.
This plan executes none of these actions.

Formalization consideration: marker transition claim is bounded to systemd start conditions: held or
unactivated skips collect/maintain/product; removal of a shared hold cannot release legacy work only if
all legacy consumers were separately fenced. Conditions do not stop running services, hence explicit
full-stop rollback. Reviewed source identity is runtime 7c96c64 plus documented comment-only delta and
this plan; exact result identity is the subsequent immutable documentation commit/notice. Counterexamples
include unmasked legacy units, in-flight work, sequential partial installation and insufficient restore space.
Practical source inspection, target-VM marker tests and review suffice for this operational plan; no new
runtime mechanism is introduced. Decision/acceptance owner is the root supervisor and operator.
