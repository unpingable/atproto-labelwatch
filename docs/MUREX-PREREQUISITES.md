# Murex prerequisites — Labelwatch

Campaign: **Murex** / `public-readonly-instrument-fact-api`
Track: public-observation-surface
Decision: **B — SHARED-SURFACE-DESIGN-ONLY**. Nothing here is authorised or
built; this file records what Labelwatch would need before it could honestly
publish a shared reduced-fact artifact.

Full campaign record, contract, and privacy analysis:
`atproto-weatherwatch/docs/MUREX-PUBLIC-FACT-SURFACE.md`.

## What Murex would add, in one line

A static `facts.json` + `contracts.json` beside the artifacts `report.py`
already writes to `/var/www/labelwatch` — **not** new routes on the
`/v1/*` server. Labelwatch's HTTP namespace is deliberately per-identity
(`/v1/climate/{did}`, `/v1/whatsonme/{did}`), and `NON_GOALS.md` calls the
distinction between receiving-end accounting and aggregate observation
load-bearing. Aggregate instrument facts do not belong in that namespace.

## Status

Items 1 and 2 were **repaired by the follow-up campaign Breakwater**
(`observation-adequacy-and-public-boundary-hardening`, 2026-08-28) because they
were live defects in their own right, not merely Murex prerequisites. They are
retained below as the record of what was wrong and why. Items 3–6 are open.

Murex itself remains **design-only**; no public fact surface was implemented.

## Blocking repairs

### 1. `network_weather` reports `calm` during a total ingest outage — **REPAIRED**

**This was a live defect, independent of Murex.**

`frontdoor.network_weather` (`src/labelwatch/frontdoor.py:744`) derives its
signals from alert counts in the last 24h. A dead ingest produces no alerts, so
it produces no signals, so the function falls through to `calm` / "no triggers
crossed".

Reproduced against a real initialised schema with twelve labelers and a total
24-hour outage — **0 of 576 poll attempts successful**:

```
ingest coverage last 24h   : 0/576 successful
network_weather signals    : ['calm']
network_weather attribution: no triggers crossed
envelope keys              : attribution, computed_at, emitting_this_week,
                             events_7d_total, signals, total_labelers, unreachable
coverage/staleness keys    : NONE
```

`unreachable` does not save it: it counts `endpoint_status = 'down'`, which is
maintained by discovery probing, not by ingest, so it stays at zero while
ingest is entirely dead.

This converts absence of evidence into evidence of absence. It is the hardest
blocker because `network_weather` feeds `weather_digest.build_digest`, the
homepage strip, and the report — any Murex fact built on it inherits the
defect.

**Repair (Breakwater).** `frontdoor.observation_adequacy` states standing from
the same facts and the same threshold the detection rules already apply —
`ingest_outcomes` over `Config.coverage_window_minutes`, cut at
`Config.coverage_threshold`. No new threshold was introduced. States are
`adequate` / `partial` / `unobserved`; `frontdoor.supports_negative_claim`
gates the negative finding. `calm` now requires standing; without it the strip
says `unobserved` or `under-observed` and carries the coverage facts as its
attribution. Positive signals are untouched — an observed spike was observed.
Two further sites carried the same defect and were repaired with it: the strip
renderer defaulted an empty signal list to `calm`, and `report.py` held a
second, independent copy of the weather logic. Pinned by
`tests/test_observation_adequacy.py`.

### 2. `overview.json` is epistemically weaker than the page beside it — **REPAIRED**

`report.py:2017-2036` computes and renders a "Platform coverage (24h)" card
from `ingest_outcomes`. The `overview` dict built at `report.py:1804` carries
`alerts_by_rule_24h`, `alerts_by_rule_7d`, `top_labelers_7d`, `census` and
heartbeats — and **no coverage field**.

A machine consumer therefore received counts stripped of the qualification a
human reading the page received. This is the "API consumer obtains a stronger
claim than the instrument may publish" failure, and it already existed.

**Repair (Breakwater).** `overview.json` now carries `observation` (per-labeler
coverage counts against a named denominator — never one fabricated estate-wide
scalar) and `network_weather` (the verdict plus `supports_negative_claim`). The
page and the artifact are computed from one shared adequacy call rather than
two independent derivations, and a parity test asserts they cannot disagree.
`weather_digest` carries the same block; its `receipt_schema_version` moved to
`2` because the hash-sealed content shape changed. A constellation-wide search
found no external consumer of either artifact.

### 3. No closed, addressable windows — open

24h / 7d / 30d lookbacks are computed relative to render-time `now`. Two
fetches a minute apart describe different intervals under identical labels. A
published fact needs a window a consumer can name, cite, and re-fetch.

### 4. No minimum-count suppression — open

A labeler-scoped issuance count of `1` over a narrow window, combined with the
public `queryLabels` endpoint, is a near-pointer to a specific labeling action.
Weatherwatch's PLC reducer is the estate precedent for the mechanism (0–9
collapse to `UNKNOWN`) and, equally importantly, for its honest qualification:
per-fact suppression is claimed, compositional non-disclosure across facts or
revisions is explicitly **not** claimed.

Choosing a threshold requires sampling the live per-labeler per-window issuance
distribution, which this campaign did not do.

### 5. `alerts.json` is unbounded — open

`_stream_alerts_json` (`report.py:492`) writes every alert ever recorded.
Weatherwatch solved the identical problem with a bounded recent window plus a
dated archive and a digest index (`weatherwatch/src/weatherwatch/archive.py`);
the pattern transfers directly.

### 6. Artifact versioning is thin — open

`api_version: "v0"` on `overview.json` alone is not a per-artifact schema
contract. `labelers.json`, `alerts.json`, `labeler/<slug>.json` and
`alert/<id>.json` carry no schema identifier at all.

## What must not happen

- No new query dimensions, and no `author`/`target`/`did` dimension ever.
  `derived_author_day`, `derived_author_labeler_day` and `boundary_targets`
  are author- and target-keyed; none may reach a published fact. The shared
  contract's enumerated-dimension rule makes this structural rather than
  advisory.
- No aggregate endpoint that can be crossed with windows to reconstruct a
  small cohort of labeled accounts.
- No disturbance of the `/v1/climate/{did}` boundary in either direction.
- No second interpretation of the measurements: a Murex producer emits from the
  existing reducers, it does not re-derive.

## Non-blocking, worth noting

`weather_digest.build_digest` (`src/labelwatch/weather_digest.py`) is already
the closest thing Labelwatch has to a Murex producer — windowed, bounded,
receipt-hashed, and documented as "structured for cron / RSS / external
consumers." With repair 1 landed it now carries observation adequacy too, which
makes it the natural place to project facts from **if and when** a consumer
justifies building the surface. None has been identified, so nothing was built.
