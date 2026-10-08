# Recent observatory units (clean-start successor)

Everything here is an example. Nothing in this directory installs, enables or
starts a unit, and no install step creates or removes a marker file.

## Files

| File | Role |
|---|---|
| `labelwatch-recent-collect.service` / `.timer` | one finite collection round every 2 min (`Persistent=false`, `RandomizedDelaySec=15`) |
| `labelwatch-recent-maintain.service` / `.timer` | daily rotation/archive/expiry at 00:05 UTC (`Persistent=true`) |
| `labelwatch-recent-product.service` | skeleton for the read-only product on 127.0.0.1:8431 (content owned by the product lane) |
| `labelwatch-recent.env.example` | shared environment, installed as `/etc/labelwatch-recent.env` |
| `roster-epoch2-seed.json` | 580 legacy labeler DIDs from the 2026-10-01 archive (`labelers.labeler_did`) |

`labelwatch-recent-collect.service.example` is the earlier single-unit draft;
`labelwatch-recent-collect.service` supersedes it.

## Install layout

```
/opt/labelwatch/releases/<sha>/source   git archive of <sha> (contains deploy/, tools/, src/)
/opt/labelwatch/releases/<sha>/venv     python3.12 venv, `pip install --require-hashes --no-index`
                                        from release/requirements-cp312-linux-x86_64.lock
/etc/labelwatch-recent.env              from labelwatch-recent.env.example
<admitted root>/                        LABELWATCH_RECENT_ADMITTED_ROOT, owned by labelwatch
<admitted root>/<store>/                LABELWATCH_RECENT_STORE, created only by recent_init.py
<admitted root>/units.lock              collect/maintain serialisation (created on first use)
```

Units name the release directory explicitly (`REPLACE_WITH_RELEASE_SHA`); there
is no `current` symlink, so a release change is a reviewed unit edit plus
`daemon-reload`. `ReadWritePaths=` must equal `LABELWATCH_RECENT_ADMITTED_ROOT`
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
  -p ConditionPathExists=!/etc/labelwatch-operator-safe-hold \
  /opt/labelwatch/releases/<sha>/venv/bin/python \
  /opt/labelwatch/releases/<sha>/source/tools/segmented_qualification/recent_init.py \
  --root <admitted root>/epoch-2 --now <UTC Z> \
  --roster /opt/labelwatch/releases/<sha>/source/deploy/roster-epoch2-seed.json \
  --receipt <evidence dir>/epoch-2-init-receipt.json
```

Running under `EnvironmentFile=` keeps the reserves JSON intact (shell
sourcing would strip its quotes). Paths are passed literally.
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

Every unit carries both conditions:

```
ConditionPathExists=!/etc/labelwatch-operator-safe-hold
ConditionPathExists=/etc/labelwatch-recent-qualified-activation
```

A unit starts only when the hold marker is absent **and** the activation
marker is present. A failed condition skips the start (logged, not a unit
failure); timers keep firing and keep skipping. Conditions are evaluated only
at start: creating the hold marker stops new collection and maintenance
rounds but does not stop a running product server or an in-flight round; the
operator stops those explicitly. Removing the hold marker and creating the
activation marker are separate owner acts. Install, upgrade and rollback never
touch either file.

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
