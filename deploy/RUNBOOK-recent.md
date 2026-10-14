# Labelwatch rolling observatory — operator runbook

Scope: the recent-observation successor (30-day rolling window, fresh acquisition epoch) as installed by
`deploy/install-recent.sh`. Everything here is read against `deploy/README-recent.md` (layout, markers, environment)
and `release/DEPENDENCY_CUSTODY.md` (locks). No historical migration exists and none is planned.

## Units

| Unit | Cadence | What it does | Normal result |
|---|---|---|---|
| `labelwatch-recent-collect.timer/.service` | every 2 min (`Persistent=false`) | one finite collection round: bounded discovery page, bootstrap probes for new sources, cursor pages for established ones, status write | `Result=success`; exit 75 (lock held by maintenance) counts as success |
| `labelwatch-recent-maintain.timer/.service` | 00:05 UTC daily (`Persistent=true`) | rotate the daily owner, archive yesterday's vessels, expire days older than 30, release blocks | `Result=success`, `last_maintenance.at` advances in `/health` |
| `labelwatch-recent-overview.timer/.service` | every 30 min | publish `<pubdir>/overview.json` (`labelwatch.recent-overview.v1`) for the facts API; while held it publishes an explicit `hold:true` artifact | file mtime advances; facts API serves 200 `fresh`/`warming_up`, or 503 `source_held` while held |
| `labelwatch-recent-product.service` | always on, 127.0.0.1:8431 | read-only site: `/`, `/about`, `/previous-epoch`, `/account`, `/lookup`, `/exports`, `/download`, `/instruments/*`; operator `/health` (loopback only, never routed) | `active (running)`; `/health` 200 with `status: ok` |

The collect, maintain and product services carry
`ConditionPathExists=!/etc/labelwatch-operator-safe-hold` and
`ConditionPathExists=/etc/labelwatch-recent-qualified-activation`. A failed condition is a skipped start, not a failure.
Timers and the overview service are ungated; the overview publishes `hold:true`
when the hold marker exists or the activation marker is absent.

## Start, stop, restart

```
systemctl status 'labelwatch-recent-*'                       # all four services and three timers
systemctl restart labelwatch-recent-product.service           # safe at any time; readers reconnect
systemctl start labelwatch-recent-collect.service             # one extra round now (skips if units.lock is held)
systemctl start labelwatch-recent-overview.service            # republish the overview now
systemctl stop labelwatch-recent-collect.timer labelwatch-recent-maintain.timer labelwatch-recent-overview.timer  # prevent new scheduled starts
systemctl stop labelwatch-recent-collect.service labelwatch-recent-maintain.service labelwatch-recent-overview.service labelwatch-recent-product.service  # stop any running work (markers untouched)
```

Never `kill -9` maintenance by hand: it is idempotent per UTC day and resumes an interrupted expiry journal on the
next run, but a SIGKILL mid-archive costs one recovery pass. A killed collection round is harmless: its persisted
attempt is swept at the next round start and disclosed as one store-wide `unknown` gap.

## Safe hold (held is not down)

```
touch /etc/labelwatch-operator-safe-hold      # new rounds, maintenance and product starts are skipped
systemctl stop labelwatch-recent-product.service   # also stop the running product if the hold must be total
systemctl start labelwatch-recent-overview.service # publishes hold:true immediately (facts API → 503 source_held)
# Resume only after owner approval and verification that legacy units are masked:
rm /etc/labelwatch-operator-safe-hold              # activation marker must also be present
systemctl start labelwatch-recent-product.service # a stopped product needs an explicit start
```

The held interval is a disclosed gap; cursors continue on the next collection round.
The hold marker is shared with the legacy units; removing it never re-enables them (they are masked after retirement).

## Expected behaviour

- **Bootstrap**: a new or seeded source is probed (gallop + bisect, at most 68 probes, usually 1–2) to find its head; its
  coverage starts at establishment, not at the epoch start. With 580 seeded sources and `LABELWATCH_RECENT_ROUND_SECONDS=30`
  the roster establishes in roughly 15–20 minutes; set the round back to 10 s after the first day.
- **Discovery**: one bounded discovery page per round; new labelers enter the roster as `probing`.
- **Acceptance floor**: labels earlier than max(epoch start − 48 h, accepted_at − 48 h) are counted (`pre_epoch_skipped`,
  `late_skipped`), never stored; labels more than 300 s in the future are `future_skipped`.
- **Per-source failures** stay per-source (`unavailable`, `refused`, `order_unverified`); only lost attempts, maintenance
  and holds produce store-wide gaps. Gap table overflow is disclosed as `gap_overflow`.
- **Midnight**: rounds between 00:00 and the 00:05 maintenance are refused by the store and disclosed as one
  `maintenance` gap of about five minutes. Labels issued then are accepted late, never dropped.
- **Throughput**: ≈ 95–155 events/s at 30-day scale, 6–10× the observed live rate (≈ 16 ev/s); bursts queue.

## Window advancement and storage

Maintenance moves the window one day forward at 00:05 UTC: yesterday becomes an archived day, the day older than 30
days is expired and its blocks released (measured ≈ 0.6 GB per advance at 42 M events). Steady state ≈ 432 B per retained
event (≈ 19.5 GB for 30 days at the live rate), sampled peak with the retirement overlap ≈ 24.3 GB, `state.sqlite`
≈ 2.4 GB (VACUUM needs 2× transiently). `LABELWATCH_RECENT_RESERVES` is checked on every open, write and maintenance
step; a refusal is an explicit error (`shared_reserve_unavailable`, `reserve_policy_*`), never a silent drop. Never weaken
the recorded policy without the documented override ack. `/health.coverage` and `df` are the two numbers to watch.

Resource budgets (units): collect `MemoryMax=2G`, maintain `4G`, product `2G`, overview `1G`; `CPUQuota=100%`
(overview 50 %). Logs: journald only, rate-limited (20 lines / 60 s per unit). The store keeps `STATUS.json` (cached
`/health`), `reader.lock`, SQLite state and daily vessels; nothing else grows.

## Public freshness

- Product `/health.freshness.state`: `fresh` while the last accepted page is younger than 900 s, `stale` beyond,
  `unknown` until the first accepted page. `/health` itself returns 503 `stale` when `STATUS.json` is older than two
  collection cadences (the collector stopped writing).
- facts API (`/atproto/v1/labelwatch/overview`): 200 only for `fresh` or `warming_up` (epoch younger than 30 days); 503
  `source_stale` (artifact older than 2 h or collection stalled), `source_held` (hold artifact), `source_unavailable`
  (no readable artifact), all `Cache-Control: no-store`.

## Export limits

One export ≈ 1,140 pages ≈ 114,000 rows within 600 s; a larger request is refused up front as `export_too_large` with
server-computed `suggested_intervals` that cover the requested window exactly once. Exports are snapshot-consistent,
expire explicitly and disclose gaps. No export is ever truncated silently; only an hour denser than the cap is
unexportable, and that is reported as such.

## Monitoring and escalation

Facts to enrol (see the Monitor #18 reconciliation record): collect `Result` + `/health.freshness`
(collection-stalled > 15 min), maintain `Result` and `last_maintenance.at` (maintenance-failed / overdue after
00:35 UTC), overview mtime (overview-stale > 45 min), `df` against the reserve (reserve-failure, distinct from generic
filesystem health), facts API freshness state, the hold marker (held, never a down page), and the absence of any of
these (unknown, never healthy). Escalate a `maintenance-failed`, a reserve refusal or a `stale` product older than an
hour; a `held` state is informational.

## Backup and recovery boundaries

The store is a 30-day rolling window with a fresh epoch; it is not backed up and no historical archive is kept. Recovery
from a corrupt or lost store = `recent_init.py` into a new, empty root (new acquisition generation; coverage restarts and
the public pages say so for 30 days). Init reads the last `$LABELWATCH_RECENT_PUBDIR/overview.json` and, if it names a generation, records that epoch as
observed, then lost; otherwise continuity is reported as unknown. Override with `--predecessor-receipt <old init receipt,
overview.json or STATUS.json>`, `--continuity-break` or `--published-overview PATH`; pass `--first-install` only for a genuine first installation. Readers refuse a store directory without `state.sqlite` and leave nothing in it; a
`reader.lock` left there by an older release must be removed before re-init. Process interruption and host reboot preserve epoch, cursors and gaps (qualified).
The previous epochs' archives on NFS are custody only, never restorable into this product.

## Source and release update procedure

1. Review and merge the change; run the acceptance subset on the cp312 lock and the cp310 replica.
2. `git archive` the sha, build `/opt/labelwatch/releases/<sha>/venv` with `release/build-venv.sh` from the reviewed
   wheelhouse (the lock is selected by interpreter ABI; any new pin needs a custody entry).
3. Stop timers and services using the full-stop commands above. Run
   `deploy/install-recent.sh --release-sha <sha> …` (rewrites units and env sequentially; store untouched; no marker change).
   If interrupted, complete installation before any start. After verifying the installed release, restart the three
   timers and product service; marker conditions still apply.
4. Rollback = re-run the installer with the previous sha. Schema migrations are forward-only (v8 today); an older
   release refuses a newer store rather than corrupting it.

## Known limitations (accepted)

Export cap ≈ 114 k rows per request; coverage per source starts at establishment; two Ozone instances return
non-id-ordered pages and stay `order_unverified` (coverage unknown, not a loss claim); handle lookup is off
(`/lookup` takes DIDs); the first 30 days are `warming_up`; ingest rate bounded as above; units need the memory caps
listed. Nothing here is a defect to fix without a named operational failure.
