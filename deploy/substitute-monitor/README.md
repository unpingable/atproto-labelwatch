# DRAFT / OFFPRODUCTION — not enrolled or accepted

# labelwatch-recent-watch: temporary substitute monitor

This is a temporary operational monitor for the Labelwatch rolling-observatory successor. It runs only
while Constellation NQ #27 (the ops store crash loop) and Monitor #18 (the successor obligations) block
native enrollment. It is not a framework. It has no daemon, database, UI or plugin points, and it is
not an NQ or Monitor deliverable. The spec is
`../PHASE-C-NQ-MONITOR-RECONCILIATION-20261009.md`: §3.2 (obligations L1-L10), §4 (negative control
NC1-NC6) and §5 (this substitute).

| File | Role |
|---|---|
| `labelwatch-recent-watch.py` | one stdlib-only Python 3.10 script: gather, evaluate, persist, emit |
| `labelwatch-recent-watch.service` / `.timer` | oneshot every 5 min (`*:01/5`, `Persistent=false`) as `labelwatch-watch` |
| `labelwatch-recent-control.service` / `.timer` | NC1 control: `ExecStart=/bin/false` every 10 min, with its own hold and activation markers |
| `tests/test_watch.py` | fixture tests for every L1-L10 transition and the NC1-NC6 semantics |

Run the tests with:
`../../runtime/clean-start-venv/bin/python -m pytest -q -p no:cacheprovider tests`

## Owner

The Labelwatch #7 successor operator owns day-to-day operation of this monitor; the repository
owner accepts it, as Phase C §5 specifies. The NQ owner independently owns NQ #27 recovery.
The substitute grants no authority to NQ or Monitor. Installing it on zone, copying a route secret, or making
the production PagerDuty route live are host or credential changes, and each one needs explicit owner
approval.

## What it reads and judges (cadence: every 5 min)

Each run reads every source once. It records the source path or URL, the sha256 of the bytes and the
read time, then evaluates the following:

| Id | Source | Condition → class |
|---|---|---|
| L1 | `<store>/STATUS.json` `coverage.observed_through`, `round`; `/health` freshness (corroboration) | `collection-stalled` > 900 s → notice, page when the stall reaches 3600 s; 00:00-00:35 UTC with `store_hold` or maintain `activating` → held |
| L2 | `systemctl show labelwatch-recent-collect.service` / `.timer` | `collection-failing` (2 consecutive observed failed invocations), `collection-not-scheduled` (timer > 600 s) → notice |
| L3 | `systemctl show labelwatch-recent-maintain.service` / `.timer`, maintain journal | judged at 00:35 for UTC day D: `maintenance-failed` → page, `maintenance-overdue` → page, `maintenance-running-late` → notice |
| L4 | STATUS.json `last_maintenance` (≥ 2 min after a successful exit) | baseline missing → unknown; `window-not-advanced` → notice day 1, page day 2; `expiry-stalled` (recovery_pending on two runs in a row) → page |
| L5 | STATUS.json `round` reserve refusals, maintain journal `… allocation not admitted`, `statvfs` vs `LABELWATCH_RECENT_RESERVES` | `reserve-refused` → page; `reserve-headroom-low` (free < reserve + 2 GiB, or inodes < floor) → notice |
| L6 | `<pubdir>/overview.json`, `systemctl show labelwatch-recent-overview.service` | `overview-stale` > 2700 s, `overview-refused` (exit 2 twice), `overview-unavailable`, `overview-hold-contradiction` → notice |
| L7 | `GET 127.0.0.1:8425/atproto/v1/instruments` (+ overview route status and `error.code`) | `public-stale` / `public-unavailable` → notice, page after 2 h; `hold-contradiction` → notice |
| L8 | `stat` of `/etc/labelwatch-operator-safe-hold`, `/etc/labelwatch-recent-qualified-activation`; product `ActiveState` | the hold makes every other obligation `held` and never pages; `hold-incomplete` (product still running) → notice |
| L9 | `GET 127.0.0.1:8431/health`; product `ActiveState`, `NRestarts` | `product-down` (connection refused) → page after 300 s; `health-stale` (503) → notice |
| L10 | its own `state.json` `evaluated_at` | `obligation-evaluator-stale` (> 900 s since the last run) → notice; daily digest at 00:40Z; `ExecStopPost` posts `watcher-run-failed` |
| C1 | `systemctl show labelwatch-recent-control.service` / `.timer` | NC1 obligation "control succeeded within 15 min": `scheduled-occurrence-failed` → page, `scheduled-occurrence-overdue` → page |

These rules apply to every obligation:

- **Hold first.** Only the L8 notice is ever emitted during a hold, and a hold never resolves anything.
- **Unknown is not healthy.** An unreadable, absent, unparseable or stale input makes each dependent
  obligation `unknown`. Unknown never resolves an incident. After 15 minutes it raises one
  `evidence-unavailable` notice per input. A connection refused by the facts API is unknown. A
  connection refused by the product is `product-down`. An absent overview is `overview-unavailable`.
- **Recovery needs a fresh success.** Only an `ok` verdict resolves an obligation's open incidents,
  and it resolves all of them, for example both `failed` and `overdue`.
- **Exit status.** A run exits non-zero only on an internal error, never because an obligation failed.

Evidence lives in `StateDirectory=/var/lib/labelwatch-recent-watch`:

- `observations.jsonl` is append-only, one record per run. Each record holds the sources (path, sha256,
  read time, error), the markers, every obligation's typed facts and verdict, the intents and the
  `obligations_digest`. The file rotates once at 10 MB to `observations.jsonl.1`.
- `state.json` holds condition rows (`first_seen`, `last_seen`, `persisted_seconds`, `observation`,
  `response_class`, `notified`, `dedup_key`, `ids`), unknown-input rows, sampled occurrences, L4 day
  records and pending deliveries.
- `report.json` holds `evaluated_at`, the verdicts and the open conditions.
- `intents/` holds one retained JSON file per transition (trigger, resolve or notice), with its delivery
  outcomes appended.

The `dedup_key` is `labelwatch-recent:<obligation>:<condition>:<target>`, where the target is the unit
or input. Unit and InvocationID go in `ids`, so a resolve reuses the same key.

## Alert destinations

Delivery is **off unless configured**. Without it, intent files are the local inbox. Configuration goes
in `/etc/labelwatch-recent-watch.env` (0640 `root:labelwatch-watch`, read through `EnvironmentFile=`):

| Variable | Value to reuse (reference only; do not copy secrets into this repository) |
|---|---|
| `WATCH_SLACK_WEBHOOK_URL` | the existing `operations` Slack webhook, `NQ_OPERATIONS_WEBHOOK_URL` in `/etc/constellation-attention/nq-routes.env` on labelwatch-host |
| `WATCH_PAGERDUTY_ROUTING_KEY` | qualification: `NQ_PAGERDUTY_TEST_ROUTING_KEY` (service `constellation-test`, low urgency; on crow `~/pagerduty-test-routing-key`). Production, after owner approval only: `NQ_PAGERDUTY_OPS_ROUTING_KEY` (service `constallation-beta`) |
| `WATCH_CONTROL_PAGERDUTY_ROUTING_KEY` | optional override for C1 only, so the control can stay on the test route while L1-L9 page production |

Background is in the memory notes `pagerduty-routing-semantics`, `pagerduty-test-key-shared` and
`pagerduty-sink-lane`. Slack receives every intent. PagerDuty Events v2 receives only page-class
`trigger` and `resolve` events (severity `critical`), never notices. The watcher deliberately avoids
`nq notification` because that path writes to the crash-looping ops store. Keys and URLs never appear in
retained evidence.

The unit is loopback-only (`IPAddressDeny=any`, `IPAddressAllow=localhost`). systemd 249 cannot name
`hooks.slack.com` or `events.pagerduty.com` by hostname, and their addresses are not stable. When
delivery is configured, install this drop-in, which opens public egress only:

```ini
# /etc/systemd/system/labelwatch-recent-watch.service.d/delivery.conf
[Service]
IPAddressDeny=
IPAddressDeny=link-local multicast 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10 fc00::/7
```

A failed delivery is retained in the intent file and retried on the next 12 runs, using only the routes
that failed. PagerDuty deduplicates on `dedup_key`.

## Install (replica first; zone only with owner approval)

```sh
groupadd --system labelwatch-watch
useradd --system --gid labelwatch-watch --groups labelwatch,systemd-journal --no-create-home --shell /usr/sbin/nologin labelwatch-watch
install -D -m 0755 labelwatch-recent-watch.py /opt/labelwatch-recent-watch/labelwatch-recent-watch.py
install -m 0644 labelwatch-recent-watch.service labelwatch-recent-watch.timer /etc/systemd/system/
# edit ReadOnlyPaths= to the admitted root and the pubdir (documentation; ProtectSystem=strict already applies)
install -m 0640 -o root -g labelwatch-watch /dev/null /etc/labelwatch-recent-watch.env   # routes, if any
systemctl daemon-reload && systemctl enable --now labelwatch-recent-watch.timer
```

The watcher learns the store, pubdir and reserves from `/etc/labelwatch-recent.env`. `WATCH_STORE`,
`WATCH_PUBDIR` and `WATCH_RESERVES` override them, and so do the other `WATCH_*` defaults at the top of
the script.

## Retirement condition

Retire the monitor only when all of the following hold:

1. NQ #27 is closed with the §1.4 evidence.
2. The Monitor #18 obligations L1-L9 are enrolled natively, with the §4 negative control proven on the
   operator surface.
3. Both paths have run in parallel for at least **7 days with matching verdicts**.

Then disable and remove both timers, both services, `/etc/labelwatch-recent-watch.env`, the control
markers and the state directory, after archiving it. Record the retirement against #18 in the receipt,
as #18 requires for any temporary substitute.

## What it cannot do

- It has no NQ evidence custody: no admission, no profile semantic identity and no tamper-evident
  history. It keeps only local JSON with sha256 digests and an obligation digest.
- It has no Constellation status-page integration, no governed remediation, and no correlation or
  dedup with the attention evaluator. Legacy `service-down` incidents remain the evaluator's.
- Nothing independent watches it. It relies on `ExecStopPost`, the L10 gap notice on the next run and
  the missing 00:40Z digest. It shares zone's fate, so a host outage silences it along with the service.
- It sees only the host it runs on: not crow and not the NFS snapshot. It cannot tell a provider-side
  outage from a local network failure beyond what `/health` reports.
- L2 and L6 count consecutive invocations as **sampled** every 5 min through `systemctl show`. With
  collection every 2 min, a failure that alternates with successes between samples can go unseen. L1
  still carries the page.
- The L3/L5 reason comes from the last 50 lines of the maintain journal filtered by the exact
  maintenance InvocationID. Missing or invalid identity makes the journal source unknown.
- Generic disk health stays with the existing `host-disk` rule. L5 judges only the configured reserve.

## Negative control NC1-NC6 (replica first; test routes only)

**Preconditions.** The successor and this watcher are installed on the replica. The control units are
installed in `/etc/systemd/system/`. `touch /etc/labelwatch-recent-control-activation`. In
`/etc/labelwatch-recent-watch.env`, `WATCH_CONTROL_PAGERDUTY_ROUTING_KEY` is set to the
**test** key, and `WATCH_PAGERDUTY_ROUTING_KEY` is unset or also set to the test key. No production
route is configured. The control never creates or removes `/etc/labelwatch-operator-safe-hold` or
`/etc/labelwatch-recent-qualified-activation`. Set `S=/var/lib/labelwatch-recent-watch`. To evaluate
immediately instead of waiting for the timer, run `systemctl start labelwatch-recent-watch.service`.

**Evidence per hop** (§4.2), collected for each scenario:

| Hop | Command / artifact |
|---|---|
| host fact | `systemctl show labelwatch-recent-control.service -p Result,ExecMainStatus,ExecMainExitTimestamp,InvocationID,ConditionResult` and `journalctl -u labelwatch-recent-control -o json \| grep <InvocationID>` (`_SYSTEMD_INVOCATION_ID`) |
| typed observation | last line of `$S/observations.jsonl`: `obligations.C1` (verdict, facts) and `sources["systemd:labelwatch-recent-control.service"]` (source, sha256, read_at) |
| persisted evidence | `sha256sum $S/observations.jsonl $S/state.json` recorded in the receipt |
| obligation | the `obligations_digest` in the record, which equals `DIGEST` of the deployed script |
| evaluation | `$S/state.json` `conditions["C1:<condition>:labelwatch-recent-control"]`: `observation`, `first_seen`, `persisted_seconds`, `response_class` |
| operator notification | `$S/intents/*-C1-<condition>-<action>.json` with `delivery` = slack `http 200` and pagerduty `http 202`, plus `dedup_key`; the incident in PagerDuty `constellation-test` and the Slack line; record the human acknowledgement time |

**Scenarios**, run in this order:

| # | Setup | Expected evidence | Must NOT happen |
|---|---|---|---|
| NC1 failed occurrence | `systemctl enable --now labelwatch-recent-control.timer; systemctl start labelwatch-recent-control.service` | host fact `Result=exit-code ExecMainStatus=1`. `C1` = `scheduled-occurrence-failed`. Row `observation=present`, `response_class=page`. Intent `…-C1-scheduled-occurrence-failed-trigger.json` with key `labelwatch-recent:C1:scheduled-occurrence-failed:labelwatch-recent-control` and `ids.invocation_id` = the host InvocationID. Slack 200, PagerDuty 202. The incident is open in `constellation-test` | any PagerDuty call with the production key (`constallation-beta` shows nothing) |
| NC4 missing evidence (NC1 still open) | `mv /etc/systemd/system/labelwatch-recent-control.service /root/ && systemctl daemon-reload` (the control status source now reads `LoadState=not-found`) | `C1` = `unknown` on every run. The NC1 row stays `notified=page` with `observation=unknown`. After 15 min there is exactly one `…-evidence-evidence-unavailable-trigger.json` (target `systemd:labelwatch-recent-control.service`), Slack only. The PagerDuty incident is still triggered | any `…-resolve.json`, any PagerDuty resolve, removal of the NC1 row |
| | restore: `mv /root/labelwatch-recent-control.service /etc/systemd/system/ && systemctl daemon-reload` | `evidence-unavailable` resolve notice. `C1` may show `scheduled-occurrence-overdue` until the next occurrence, which is correct because there has been no fresh success | |
| NC5 recovery | `mkdir -p /etc/systemd/system/labelwatch-recent-control.service.d && printf '[Service]\nExecStart=\nExecStart=/bin/true\n' > /etc/systemd/system/labelwatch-recent-control.service.d/recover.conf && systemctl daemon-reload && systemctl start labelwatch-recent-control.service` | host fact `Result=success`. `C1` = `ok`. `…-resolve.json` for every open C1 condition, with PagerDuty `event_action=resolve` 202 on the same `dedup_key`. NC1's trigger file is still in `intents/`. The C1 rows leave `state.json` | a resolve before the fresh success (check the order of `observations.jsonl` against the host exit timestamp) |
| NC2 overdue | after one NC5 success: `systemctl stop labelwatch-recent-control.timer`, then wait ≥ 15 min from the last exit | `C1` = `scheduled-occurrence-overdue`, a separate row and key (`…:scheduled-occurrence-overdue:…`), page intent, PagerDuty 202 | classification as `scheduled-occurrence-failed` or `unknown` |
| | `systemctl start labelwatch-recent-control.timer labelwatch-recent-control.service` | the overdue incident resolves | |
| NC3 held | `ls $S/intents > /root/nc3-before; touch /etc/labelwatch-recent-control-hold`, timer running, for ≥ 30 min | host fact `ConditionResult=no`, no new exec. `C1` = `held` on every run. `ls $S/intents` shows no new `…-C1-…` file (with healthy production obligations, no new file at all). `journalctl -u labelwatch-recent-watch` and `delivery` show no provider call for C1 | **any** C1 intent file or provider call |
| | `rm /etc/labelwatch-recent-control-hold` | | |
| NC6 progress/publication fixtures | see below; real store, STATUS.json and pubdir untouched | L1 `collection-stalled` at 15 min (page at 60). L6 `overview-stale` at 45 min. `overview-hold-contradiction` for `hold:true` without the control hold marker | any read of the real store or pubdir (`sources.status.source` and `sources.overview.source` name only the fixture directory) |

Afterwards, run `systemctl disable --now labelwatch-recent-control.timer`, remove both control units, the
`recover.conf` drop-in and both control markers, and archive `$S`. After the replica, and only with the
owner's announcement, run one NC1 on the production route (set `WATCH_CONTROL_PAGERDUTY_ROUTING_KEY` to
the production key). Pages then land on the owner's phone. Remove the control afterwards.

**NC6 procedure.** NC6 is a separate evaluation by hand, with its own state directory and fixture
directory. It never uses the real store or pubdir, and it uses `--now` to step the clock:

```sh
F=/var/lib/labelwatch-recent-control-fixture; N=/var/lib/labelwatch-recent-watch-nc6
install -d -o labelwatch-watch -g labelwatch-watch -m 0750 $F $N
cat > /etc/labelwatch-recent-watch-nc6.env <<EOT
WATCH_STORE=$F
WATCH_PUBDIR=$F
WATCH_STATE_DIR=$N
WATCH_HOLD_MARKER=/etc/labelwatch-recent-control-hold
WATCH_ACTIVATION_MARKER=/etc/labelwatch-recent-control-activation
WATCH_CONTROL_UNIT=
WATCH_SYSTEMCTL=/bin/false
WATCH_HEALTH_URL=http://127.0.0.1:9/health
WATCH_FACTS_BASE=http://127.0.0.1:9
WATCH_RESERVES=[{"mount": "/", "bytes": 1}]
WATCH_SLACK_WEBHOOK_URL=
WATCH_PAGERDUTY_ROUTING_KEY=
EOT
T0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
fx() {  # fx <minutes-after-T0> <overview-age-min> <hold true|false>: frozen observed_through=T0, STATUS written 30 s before now
  python3 - "$T0" "$1" "$2" "$3" "$F" <<'PY'
import json, sys; from datetime import datetime, timedelta, timezone
t0 = datetime.strptime(sys.argv[1], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc); now = t0 + timedelta(minutes=int(sys.argv[2]))
z = lambda d: d.strftime('%Y-%m-%dT%H:%M:%SZ'); hold = sys.argv[4] == 'true'
open(sys.argv[5] + '/STATUS.json', 'w').write(json.dumps({'schema': 'labelwatch.recent-status.v1', 'generated_at': z(now - timedelta(seconds=30)),
    'coverage': {'status': 'warming_up', 'observed_through': z(t0)}, 'last_maintenance': {'generation': 0, 'recovery_pending': False, 'at': None},
    'round': {'status': 'ok'}}))
open(sys.argv[5] + '/overview.json', 'w').write(json.dumps({'schema_version': 'labelwatch.recent-overview.v1', 'hold': hold,
    'generated_at': z(now - timedelta(minutes=int(sys.argv[3]))), 'coverage': {'status': 'held' if hold else 'warming_up'}}))
print(z(now))
PY
}
nc6() { env -i runuser -u labelwatch-watch -- /usr/bin/python3 -I /opt/labelwatch-recent-watch/labelwatch-recent-watch.py \
          --config /etc/labelwatch-recent-watch-nc6.env --now "$1"; }
nc6 "$(fx 14 44 false)"   # expect L1 ok, L6 ok
nc6 "$(fx 16 46 false)"   # expect L1 collection-stalled (notice intent), L6 overview-stale (notice intent)
nc6 "$(fx 61 46 false)"   # expect L1 page intent (collection-stalled, response_class page)
nc6 "$(fx 62 1 true)"     # expect L6 overview-hold-contradiction (control hold marker absent)
```

The control hold marker and the other markers have to be readable by `labelwatch-watch`, and
`chown labelwatch-watch $F/*` may be needed after `fx` writes the fixtures. For evidence, collect the
printed verdicts and `$N/observations.jsonl` (`sources.status.source` = `$F/STATUS.json` with its
sha256), plus the `$N/state.json` rows and the `$N/intents/*-L1-…`, `*-L6-…` files. In this profile the
L2, L3, L7 and L9 rows read as `unknown` or `product-down` by construction (`/bin/false` stands in for
systemctl, and port 9 refuses connections). Those rows are not NC6 evidence. Delivery is off. Remove
`$F`, `$N` and `/etc/labelwatch-recent-watch-nc6.env` afterwards.

## Draft fixed-window capacity qualification addendum

The authorized predictive-capacity requirement adds L11 to this existing watcher; it remains a draft
offproduction substitute. The observation contract stays **30 days**. No window change or automatic
service/storage action follows any verdict. L3 now uses fresh `acquisition.started_at`: a due time
preceding acquisition enrollment is skipped; missing enrollment evidence is unknown. L4 continues to
report `window-not-advanced` and `expiry-stalled` separately from maintenance failure and capacity.
L5 observes physical reserve/inode exhaustion as `capacity-exhausted` (page) even when STATUS.round was
overwritten; a failed collect occurrence's reserve reason is read from its exact InvocationID journal,
independently of the exact maintain InvocationID. Missing required journal evidence is unknown.

L11 reads a bounded stat inventory of the configured store (4096 entries maximum; regular files and
directories counted by distinct inode; symlinks/unsupported entries/read failures → unknown). It
records allocated blocks, aggregate logical/allocated `*-wal` bytes and the inventory digest, without
opening a database or scanning observation rows. Filesystem reserves/free blocks/inodes are separate
observations; the inventory is a sampled estimate under concurrent writes, not an atomic store seal.

Configure only owner-admitted policy: `WATCH_STORE_BUDGET_BYTES` and `WATCH_WAL_BUDGET_BYTES` are unset
by default, yielding unknown forecasting. `WATCH_CAPACITY_LOOKAHEAD_SECONDS` defaults to 86400.
The admitted replica values are 2147483648 store bytes and 268435456 aggregate WAL bytes; production
values require owner confirmation and matching scope. The WAL observation uses aggregate logical WAL
size, conservatively distinct from SQLite's per-database uncheckpointed-frame guard.

A same-store/acquisition/maintenance-generation/roster/policy baseline of 300–900 seconds is required.
Short samples preserve the original baseline; identity, roster, maintenance generation, policy,
counter decrease or a stale gap resets it. Complete enrolled-source accepted_rows counters yield
ingestion rows/second; missing/truncated roster evidence is unknown. Net allocated bytes/second is a
separate measurement; it is not gross ingestion cost per row. Nonpositive net growth gives unknown
forecast, never a healthy-capacity conclusion.

The 30-day estimate uses net growth over a rolling history of hourly samples, at least 6 hours
and at most 25 hours old (`capacity_history` in `state.json`). History survives a stale gap and a
maintenance-generation change, so retirement inside the window is credited. Any other identity
change resets it. Until 6 hours of comparable history exist, the estimate is unknown. A single
300-second sample is never extrapolated 30 days. On 2026-10-13/14 that produced 33–67 GiB notices
against a 0.15 GiB store whose median projection was 0.7 GiB. The 300-second rate still decides
urgency (crossing within lookahead), so a real burst still pages.

The explicit estimate is `current allocated + observed positive net bytes/second × 30 days`. It
assumes repetition of the recent rate and gives no credit for future unobserved retirement. It is
**not measured full-window capacity**, nor proof that the deployment can safely serve 30 days.
Time to store budget and each same-mount reserve are conditional estimates; filesystem free-space
depletion includes other tenants and is not attributed to this store.
When a filesystem forecast causes a page, the notification leads with each affected mount,
its configured reserve, time to that reserve, lookahead and short sample duration. It then gives
the separate store forecast (or says it is unknown). A small store forecast does not rule out
shared-filesystem depletion; inspect the named mount and all of its tenants. Forecast thresholds,
notice/page timing and resolution rules are unchanged by this message formatting.

- `projected-budget-breach`: notice when the 30-day estimate reaches the admitted store budget, even
  when crossing is beyond one day; page when estimated store/reserve crossing is within lookahead.
- `capacity-exhausted`: page when actual allocated bytes reach store budget, logical aggregate WAL
  reaches WAL budget, or physical reserve/inode floor is exhausted.
- Unknown/cold/short/nonpositive forecasts preserve open conditions. An `ok` conditional estimate
  resolves only L11's observation, never the independent 30-day capacity qualification.

Deterministic controls retain distinct IDs for missed maintenance (L3), stalled retirement/window
advancement (L4), projected budget breach (L11) and exhausted capacity (L5/L11), plus baseline/reset,
hold, round overwrite, exact journal identity and persisted observation/intent checks. All fixture
transport is mocked. The earlier replica NC1–NC6 producer remains pinned to its original committed
source; those receipts do not qualify this new forecast observation. No provider acceptance or native
Monitor #18 closure is implied.

Independent review refinements: reserve observations must be nonempty and cover the exact validated
store device; device/inode identities and per-source counters are validated. An individual counter
decrease resets the baseline even if other sources mask it in the aggregate. A future-generated
STATUS is unknown. Filesystem and store estimates have separate explicit states: an urgent observed
filesystem depletion estimate pages even when nonpositive store growth leaves the store footprint
unknown. No store estimate is invented in that case.

The allocation observation counts **visible namespace blocks** only. It excludes open-unlinked
scratch and allocations outside the configured store; it is not total campaign allocation. The prior
C2 qualification's measured29.49GB peak versus24.3GB visible inventory illustrates this limitation.
Shared filesystem free-space depletion captures aggregate consumption without assigning an owner.
Prior qualified peak demand and admission headroom remain separate prerequisites; a stat inventory
and recent linear forecast cannot establish future full-window capacity.
