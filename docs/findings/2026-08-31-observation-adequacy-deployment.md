# Observation adequacy — production deployment receipt

Date: 2026-08-31
Qualified commit: `114aa419a24bcd33419cbbe16bd26d47c401e681`
Deployed: **YES**
Author of receipt: independent of the commit's authorship path. Every claim
below was established against production directly, not inferred from the diff.

## What changed in production

Labelwatch may no longer report `calm` from a window that did not observe
enough to support the claim. When observation is inadequate, the negative
finding is withheld and replaced with `under-observed` (partial coverage) or
`unobserved` (no successful ingest at all), carrying the reason with it.
Positive findings are unaffected: triggers that did fire still fire.

## Provenance correction (material)

Production carried three disagreeing build markers:

| Marker | Value | Status |
|---|---|---|
| `/opt/labelwatch/GIT_SHA` | `b81b97b` | **stale** — consumed by `utils.py` and published in `overview.json` as `git_commit` |
| `/opt/labelwatch/.git_sha` | `25a6e23` | stale, read by no code |
| `/opt/labelwatch/.git` HEAD | `f9c579b` (main, 103 dirty) | abandoned checkout, not the deploy path |

None described the running code. The running code was established by content,
not by marker: `/opt/labelwatch/src` was **byte-identical to `04e5068`**, the
commit immediately preceding `114aa41`.

This matters for outage accounting. Read from `GIT_SHA`, this deployment looks
like a 15-commit / 7,610-insertion catch-up spanning four new subsystems. It
was not. The actual delta deployed was `04e5068..114aa41`:

    src/labelwatch/frontdoor.py        | 219 +++-
    src/labelwatch/report.py           |  76 +-
    src/labelwatch/runner.py           |  18 +-
    src/labelwatch/weather_digest.py   |  11 +-
    src/labelwatch/cli.py              |   2 +-
    tests/test_observation_adequacy.py | 366 +++
    docs/MUREX-PREREQUISITES.md        | 146 +++

That is the observation-adequacy repair and nothing else. `GIT_SHA` has been
updated to `114aa41`. `.git_sha` was left alone — nothing reads it, and
churning it is not this campaign's business; it is recorded here so the next
operator is not misled by it.

## Deployment method, and why not `scripts/deploy.sh`

`scripts/deploy.sh` was **not** used. Two defects made it unsafe here:

1. It rsyncs from `./` — the local working tree. That tree carries unrelated
   operator work in flight. Running it would have shipped uncommitted
   retention/ops-visibility changes to production alongside the repair.
2. It runs `rsync --delete` against `/opt/labelwatch/`, which would have
   deleted production-only state that is not in the repository: `data/`,
   `receipts/`, `out/`, `build/`, `UNKNOWN.egg-info`, `GIT_SHA`, `.git_sha`,
   `.claude/`, `.mcp.json`, and the `/opt/labelwatch/.git` checkout.

Instead the seven changed files were copied from a clean detached worktree at
`114aa41` (verified 0 dirty entries). After the copy, production `src/` was
re-fetched and diffed against `git archive 114aa41` — **byte-identical**.

Both are pre-existing deploy-tooling hazards, not defects introduced by this
change. Neither was repaired here; repairing the deploy script was not in
scope. They are filed as follow-ups below.

## The restart gap, closed

`scripts/deploy.sh` restarts `labelwatch.service` only. `labelwatch-api.service`
is a second unit running the same editable install from `/opt/labelwatch/src`
(`__editable__.labelwatch-0.1.0.pth` → `/opt/labelwatch/src`). Restarting one
and not the other leaves two processes serving different code from the same
source directory.

Both units were restarted, at `2026-08-31T17:07:27Z`. Both report `active`.
Both resolve `labelwatch.frontdoor` to `/opt/labelwatch/src/labelwatch/frontdoor.py`
with `observation_adequacy` and `supports_negative_claim` present.

## Verification against live production data

Computed by the deployed code, read-only (`mode=ro`) against
`/var/lib/labelwatch/labelwatch.db`:

    adequacy             = partial
    reason               = 2 of 135 labelers below 50% ingest coverage in the last 30m
    window_minutes       = 30          (Config default; absent from config.toml)
    coverage_threshold   = 0.5         (Config default; absent from config.toml)
    labelers_attempted   = 135
    labelers_adequate    = 133
    labelers_degraded    = 2
    labelers_unattempted = 0
    attempts             = 405
    successes            = 399

    signals     = ['noisy', 'churny', 'degraded', 'under-observed']
    attribution = 85 rate-spike alerts (24h) · 231 churn alerts (24h) ·
                  6 labelers unreachable ·
                  2 of 135 labelers below 50% ingest coverage in the last 30m
    supports_negative_claim = False

Coverage is reported as per-labeler counts against a named denominator (135).
No estate-wide scalar coverage was synthesised, no arithmetic mean was taken,
and no single synthetic window was fabricated.

### No negative claim was lost

Published weather before deployment was `noisy, churny`. `calm` was **not**
being emitted, so nothing true was withdrawn. The transition is additive:
`noisy, churny` → `noisy, churny, degraded, under-observed`. `degraded` comes
from the pre-existing unreachable-labeler path, not from this change.

This is the expected and desired direction. Production is now advertising that
its observation is partial. That is not a regression to roll back; it is the
instrument telling the truth about itself for the first time.

## Published-artifact verification

The first report generated by the deployed code was published at
`2026-08-31T17:45:50Z`. Read back from `/var/www/labelwatch/`:

    overview.json  signals  = ['noisy', 'conflicted', 'churny', 'degraded', 'under-observed']
    overview.json  adequacy = partial
    overview.json  reason   = 2 of 135 labelers below 50% ingest coverage in the last 30m
    index.html     contains "under-observed", does not contain "calm"

`overview.json` now carries the `network_weather` and `observation` blocks; the
pre-deployment artifact had neither key. The machine artifact and the human page
agree on the thing this repair is about: adequacy is `partial`, the negative
claim is withheld, and `under-observed` is stated in both with the same reason.

### A pre-existing parity gap this exposed but did not cause

Signal-by-signal, the page carries every published signal except one:

    noisy True · conflicted FALSE · churny True · degraded True · under-observed True

`conflicted` is emitted by `report.py`'s inline weather block (line ~2920) and
does not exist in `frontdoor.network_weather` at all. The two are **separate
implementations** of the same verdict: `overview.json` is built from the inline
block, `index.html` from `fd.network_weather`. Their signal vocabularies have
diverged.

This is not a defect introduced here. `conflicted` was added on 2026-03-24 in
`8a5ff46`, and `frontdoor` never had it — verified against `04e5068`, the
pre-deployment tree. This deployment made the divergence *visible* by being the
first change to assert machine/human parity as a property worth checking.

It was deliberately not repaired. Unifying the two implementations is a
refactor of code unrelated to observation adequacy, and this campaign's change
discipline forbids that. It is filed as follow-up 6. The adequacy gate itself
was added to **both** paths, which is why adequacy, reason, and the
withholding of `calm` agree exactly.


## Rollback

Artifact: `/root/labelwatch-rollback-20260831T170606Z.tar.gz` on the production
host — the five source files plus `GIT_SHA` as they stood immediately before
deployment. To roll back: extract into `/opt/labelwatch/`, then restart **both**
`labelwatch.service` and `labelwatch-api.service`.

Rollback triggers: service failing to start, report generation raising, or
positive findings being suppressed. **Not** a rollback trigger: production
revealing that observation quality is worse than previously advertised.

## Scope discipline

- Deployed exactly `114aa41`. No local unique commit was created.
- No operator working-tree changes were deployed; the source was a clean
  detached worktree.
- Driftwatch was not touched. This deployment has no custody, outage
  accounting, rollback, or severity relationship to the Driftwatch finding.
- Weatherwatch has no production change; it was pushed and left alone.

## Follow-ups filed, not done

1. `scripts/deploy.sh` rsyncs from the working tree rather than a committed
   ref — it can ship uncommitted work.
2. `scripts/deploy.sh` uses `--delete` against a destination holding
   production-only state it does not know about.
3. `scripts/deploy.sh` restarts one of the two units that run the code.
4. `GIT_SHA` is written by a deploy path no longer in use, so published
   `git_commit` provenance drifted silently. Nothing detects the drift.
5. `config.toml` does not set `coverage_window_minutes` or
   `coverage_threshold`; production runs on dataclass defaults. Working as
   intended, but the values are invisible to an operator reading config.
6. `report.py` reimplements the network-weather verdict inline rather than
   calling `frontdoor.network_weather`. The two have drifted: `conflicted`
   exists only in the `report.py` copy, so `overview.json` can publish a signal
   the homepage cannot render. Any future signal added to one and not the other
   reopens the same gap.
