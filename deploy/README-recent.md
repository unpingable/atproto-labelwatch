# Recent observatory units (clean-start successor)

The unit and environment files are templates. `install-recent.sh` installs them
for a named release; it enables or starts no unit and changes no marker file.

## Files

| File | Role |
|---|---|
| `labelwatch-recent-collect.service` / `.timer` | one finite collection round every 2 min (`Persistent=false`, `RandomizedDelaySec=15`) |
| `labelwatch-recent-maintain.service` / `.timer` | daily rotation/archive/expiry at 00:05 UTC (`Persistent=true`) |
| `labelwatch-recent-product.service` | read-only rolling-observatory site on 127.0.0.1:8431 |
| `labelwatch-recent-overview.service` / `.timer` | aggregate overview publisher for the facts API, every 30 min (publishes a held artifact while held) |
| `labelwatch-recent.env.example` | shared environment, installed as `/etc/labelwatch-recent.env` |
| `roster-epoch2-seed.json` | 580 legacy labeler DIDs from the 2026-10-01 archive (`labelers.labeler_did`) |

`labelwatch-recent-collect.service.example` is the earlier single-unit draft;
`labelwatch-recent-collect.service` supersedes it.

## Install layout

```
/opt/labelwatch/releases/<sha>/source   git archive of <sha> (contains deploy/, tools/, src/)
/opt/labelwatch/releases/<sha>/venv     built by release/build-venv.sh (below); labelwatch is
                                        not installed, units use PYTHONPATH=source/src:source
/etc/labelwatch-recent.env              from labelwatch-recent.env.example
<admitted root>/                        LABELWATCH_RECENT_ADMITTED_ROOT, owned by labelwatch
<admitted root>/<store>/                LABELWATCH_RECENT_STORE, created only by recent_init.py
<admitted root>/units.lock              collect/maintain serialisation (created on first use)
```

Build the venv offline from exactly the lock, then prove the unit gate:

```
WHEELHOUSE=<wheelhouse> PYTHON=/usr/bin/python3 \
  /opt/labelwatch/releases/<sha>/source/release/build-venv.sh /opt/labelwatch/releases/<sha>
```

which runs `<python> -m venv <release>/venv` and
`<release>/venv/bin/python -m pip install --no-index --require-hashes --no-deps
--find-links <wheelhouse> -r <release>/source/release/requirements-<abi>-linux-x86_64.lock`
(`cp310` on the Ubuntu 22.04 production host, `cp312` on the qualification
host; any other interpreter is refused),
then `pip check`, an exact name/version comparison of the venv against the
lock, a source import under the unit `PYTHONPATH`, and the collect unit's
`ExecStartPre` dependency gate. It refuses an existing venv. The reviewed wheelhouses are
`wheelhouse-clean-start-20261008` (cp312) and
`wheelhouse-clean-start-cp310-20261009` (cp310); see release/DEPENDENCY_CUSTODY.md.

`deploy/install-recent.sh --release-sha <sha> --admitted-root <dir> --pubdir <dir>
--reserves '<json>'` (as root) performs the substitutions below for exactly the
seven `labelwatch-recent-*` units, writes `/etc/labelwatch-recent.env`, creates
the admitted root (0750) and the publication directory (0755), reloads systemd
and verifies the units. It creates no marker and enables nothing.
Units name the release directory explicitly (`REPLACE_WITH_RELEASE_SHA`); there
is no `current` symlink, so a release change is a reviewed re-run of the
installer with the new sha plus the restart of the product service.
Placement of env and units is sequential, not atomic. Stop the timers and
services before an update; if installation is interrupted, complete it before
starting any service. `ReadWritePaths=` must equal `LABELWATCH_RECENT_ADMITTED_ROOT`
literally (systemd does not expand environment variables there).

## Environment and storage admission

- `LABELWATCH_RECENT_ADMITTED_ROOT`: absolute, existing, resolved directory, no
  symlinked component, not `/`, and neither inside nor containing a
  qualification campaign root. Stores must be strict children of it.
  System prefixes are refused: `/etc`, `/usr`, `/bin`, `/sbin`, `/lib*`,
  `/boot`, `/proc`, `/sys`, `/dev`, `/run`, `/root`, `/srv`, `/var/www`,
  `/var/lib/labelwatch/generations`, `/var/lib/labelwatch/cold` and
  `/home/*/.config` (and anything below them).
- Contents rule, checked on every open: the admitted root may contain only
  epoch stores (directories holding `ACTIVE.json` or `state.sqlite`), empty
  directories (a pending or interrupted init) and the regular file
  `units.lock`. Any other entry, including symlinks and receipts, refuses
  every store under it. Keep init receipts in the evidence directory.
- `LABELWATCH_RECENT_RESERVES` (or `LABELWATCH_RECENT_RESERVES_FILE`): JSON
  `[{"mount":"/","bytes":N}, ...]`. Each mount must exist; the store's
  filesystem must be one of them. Unset, empty or malformed refuses every
  open, write and maintenance step. `/data` is never assumed.
- `LABELWATCH_RECENT_MIN_FREE_INODES` (default 1000000) applies to every
  listed filesystem.
- At init the policy is recorded in the store meta as `q:recent_reserves`
  (canonical JSON). Every later open refuses a running policy that is weaker
  (a recorded mount missing or lower, or a lower inode floor) unless
  `LABELWATCH_RECENT_RESERVES_OVERRIDE_ACK` equals
  `sha256(recorded_canonical + "\n" + running_canonical)`, where both are
  canonical JSON (`sort_keys`, separators `,` and `:`) of the policy object
  `{"schema","min_free_inodes","reserves"}`. The ack binds one reviewed
  weakening: any further change to the running policy voids it. It never
  rewrites the record.

## Initialisation (operator, once)

```
systemd-run --wait --pipe --uid=labelwatch --gid=labelwatch \
  -p EnvironmentFile=/etc/labelwatch-recent.env \
  -p Environment=PYTHONPATH=/opt/labelwatch/releases/<sha>/source/src:/opt/labelwatch/releases/<sha>/source \
  /opt/labelwatch/releases/<sha>/venv/bin/python \
  /opt/labelwatch/releases/<sha>/source/tools/segmented_qualification/recent_init.py \
  --root <admitted root>/epoch-2 --now <UTC Z> \
  --roster /opt/labelwatch/releases/<sha>/source/deploy/roster-epoch2-seed.json \
  --receipt <evidence dir>/epoch-2-init-receipt.json
```

Initialisation is permitted while held: leave the hold and activation markers
unchanged. It creates the store without starting collection or publication.
Running under `EnvironmentFile=` keeps the reserves JSON intact (shell
sourcing would strip its quotes). `PYTHONPATH` makes the archived source
importable without installing the package. Paths are passed literally.
It refuses a non-empty root, and refuses `--now` more than 300 s from the
wall clock in either direction: a backdated start would erase warm-up and
claim a fully observed window. `--allow-clock-skew-seconds N` admits up to N s
explicitly; the receipt's `acceptance_clock` records the requested clock, the
wall clock, the skew, the bound and whether it was explicit. Before any effect
it also requires the reserve policy to hold now (not merely parse). The
receipt's `source.dirty` counts untracked files. It records the acquisition generation and reserves,
and seeds the roster as DIDs only: no endpoint, no cursor. A seeded DID has
unknown coverage until the collector bootstraps it. No history is imported.

## Hold and activation markers

The collect, maintain and product services carry both conditions:

```
ConditionPathExists=!/etc/labelwatch-operator-safe-hold
ConditionPathExists=/etc/labelwatch-recent-qualified-activation
```

These three services start only when the hold marker is absent **and** the
activation marker is present. The timers are ungated. The overview service is
also ungated: it publishes `hold:true` without reading the store whenever the
hold marker exists or the activation marker is absent. A failed condition skips the start (logged, not a unit
failure); timers keep firing and keep skipping. Conditions are evaluated only
at start: creating the hold marker stops new collection and maintenance
rounds but does not stop a running product server or an in-flight round; the
operator stops those explicitly. Removing the hold marker and creating the
activation marker are separate owner acts. Install, upgrade and rollback never
touch either file.

## Collection round timing

- `LABELWATCH_RECENT_ROUND_SECONDS` (collector default 10, max 30) is the
  per-round collection deadline. Use 30 for the first day of an epoch so the
  580 seeded sources bootstrap in about 15-20 minutes, then return to 10.
  (584 was the scheduler-fixture figure; the committed seed holds 580.)
- The collect unit's `TimeoutStartSec=45` (with `TimeoutStopSec=5`) must
  exceed ROUND plus about 10 s of grace for setup and commit, so ROUND may
  not exceed 35 under this unit; the collector caps it at 30.
- `LABELWATCH_RECENT_STALE_SECONDS` (default 60) must exceed
  `TimeoutStartSec`: an attempt older than it is swept at the next round start
  and disclosed as a store-wide `unknown` gap, so a smaller value would sweep
  a round that systemd has not yet killed.

## Daily boundary

The store refuses ingestion whose UTC day differs from the active owner
(`recent_storage.py` "rotate daily observation owner before ingestion"), so
between 00:00 and the 00:05 maintenance run (plus timer accuracy) every
collection round is refused by the store, not by any source. No event is
accepted and no cursor advances; the next round after rotation resumes from
the persisted cursors at a later acceptance clock, so labels issued in that
interval are accepted late, never dropped or backdated. The collector records
the interval as a store-wide gap labelled `maintenance` (a local refusal, not
`source_unavailable`), and per-source failures do not consume the store-wide
gap table. Readers therefore see roughly five minutes after each UTC midnight
disclosed as a maintenance gap. Maintenance takes
`units.lock` (waiting up to 120 s for an in-flight round); a collection round
that finds the lock held exits 75, treated as success, and is not queued.
`advance_day` is idempotent per UTC day: rotation to an already active owner
is a no-op and an interrupted expiry journal is resumed; `Persistent=true`
therefore only adds a catch-up run after downtime.

## Public routing of the product (127.0.0.1:8431)

Route publicly only `/`, `/about`, `/previous-epoch`, `/account`, `/exports`,
`/download`, `/lookup` and `/instruments/*`. `/health` carries operator detail
(refused, unavailable, order-unverified and undated-only source DIDs with
reasons/evidence, and per-source status) and must stay loopback/operator-only:
do not route it through Caddy. Every other path answers an explicit refusal.
The previous epoch's report tree is withdrawn and must not be served.
