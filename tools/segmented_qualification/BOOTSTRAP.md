# Clean-start source bootstrap (head establishment)

Scope: `src/labelwatch/recent_collector.py` (`Collector._bootstrap`, `_accept`) and the
store side in `recent_storage.py` (`bootstrap_step`, `collector_source_state`,
`collector_source_status`, `collector_bootstrap_summary`, `collector_epoch`, the
`boot_*`/`*_skipped` columns of `q_recent_sources`). No deployment is implied.

## Problem

On a new epoch every source has `provider_cursor` NULL. `queryLabels` without a cursor
starts at the beginning of that labeler's history, so ordinary paging would traverse
the full history (44M ids on moderation.bsky.app) before reaching current records, and
filtering old rows does not bound that traversal.

## Verified protocol facts (2026-10-08, crow, read-only)

Evidence: `portfolio-private/campaigns/labelwatch-segmented-horizon-capacity-20261005/
clean-start-20261008/evidence/protocol-bootstrap-55f079ff-469c-45ef-bae3-3aaa1a6d8d60.json`
(182 GETs, 6 websocket connections). Responding labelers: 5 Ozone (mod.bsky.app,
ozone.fran.pw, labeler.urbanism.plus, bonker.stealthbadger.net, pkflash.glitchypsi.xyz),
2 skyware-style (constituencies.mplabeller.uk, label.bettertori.fi); several more
were unreachable (TLS alert, timeout, 404, 302) or required auth (401).

* Cursor: decimal integer (row id) on every responding labeler. Malformed cursor:
  400 InvalidRequest on both implementations. No opaque cursor was observed.
* Order: on every non-empty page, the returned cursor was greater than the request
  cursor. Verified id-ordered on mod.bsky.app and skyware samples (review r2: 31
  mid-history mod.bsky.app cursors, disjoint pages, returned minus request 50-61).
  NOT id-ordered on ozone.fran.pw and labeler.urbanism.plus (review r2, 2026-10-08:
  fran.pw 1200 -> 8321 but 2000 -> 5089; pages at 1200 and 2000 share 43 of 50 rows;
  following the chain from 1200 to the head never returned 6 rows of the page at
  2000). Labels carry no id, so per-page id checks are impossible; see "Order".
* `cts` is NOT a monotone key. Ozone renders current label state for old ids:
  fran.pw and urbanism.plus pages 1-2 were out of `cts` order and repeated rows across
  pages. Near-head pages were monotone. So `cts` monotonicity is not an admission
  condition here; only the id cursor is.
* Beyond the head (cursor 10^12): Ozone returns 200, `labels: []`, no cursor.
  Skyware-style returns 200, `labels: []`, cursor `"0"`; following it would restart the
  whole history. The collector now never moves the position on an empty page.
* `limit`: 250 accepted, 251 rejected (400) on both implementations.
* `subscribeLabels` exists on both. Without a cursor it starts at the live head, but a
  quiet labeler sends nothing (0 messages in 15 s on two labelers), so it cannot
  establish a head for quiet sources. Its `seq` is the same namespace as the
  `queryLabels` cursor (checked on mod.bsky.app, urbanism.plus, mplabeller). It is not
  used; probing works for quiet and busy sources with the existing HTTP transport.
* Measured head search (limit 50, x16 gallop then bisection, stop at first short
  page): 8, 10, 11, 13 and 22 probes; 22 for mod.bsky.app.

## Mechanism

Per source, phase `unstarted -> probing -> established | refused`, persisted in
`q_recent_sources` (`boot_phase, boot_probes, boot_lo, boot_hi, boot_cursor,
boot_started, boot_at, boot_reason`). A source with a non-NULL `provider_cursor` is
collected forward as before and never bootstrapped.

1. First probe: no cursor, limit 50. Empty or short (< 50 rows) or terminal (no
   cursor): the whole history is in hand; establish at the beginning and accept the
   page normally (an opaque continuation is kept as-is here).
2. Full page: the next cursor must be a canonical decimal <= 2^53 that advances, else
   `refused` (`opaque_cursor`, `cursor_regression`, `cursor_out_of_range`). Then
   `lo` = that position.
3. While `hi` is unknown, probe `x = lo*16` (capped at 2^53); afterwards bisect
   `x = (lo+hi)//2`. Empty page: `hi = x`. Full page: `lo = x`. Short page at `x`:
   establish at `x` and accept that page as the first collected page.
4. When `hi - lo <= 1`, establish at `lo` (accepting the full page for `lo` if it is
   the probe in hand).

Invariant: `lo` is a provider position at or below the head when observed, so starting
there never skips a later-created record; the cost of a stale `lo` is only re-reading
old rows, which step 5 refuses. Each outcome is written by `bootstrap_step` (CAS on the
attempt token and the expected `phase/probes/lo/hi`) before the next request. A lost
process between request and persist leaves the attempt open; the next attempt records
an `unknown` gap (existing semantics) and repeats only that probe.

5. Epoch-only acceptance (all collection, not only bootstrap): a row whose
   source-authored `cts` is older than `acquisition.started_at - 48 h` is never
   accepted; it is counted in `pre_epoch_skipped`. A row without a parseable,
   zone-qualified `cts` is counted in `undated_skipped`. These are disclosures, not
   coverage gaps. Accepted rows keep the existing semantics: observed time is the local
   acceptance clock, never `cts`.

## Bounds

* Requests per source: <= 1 + ceil(53/4) + 53 = 68 successful probes (one of which is
  the first collected page); the store refuses beyond `BOOTSTRAP_POLICY.total_probes`
  (72) as `bootstrap_refused:probe_budget_exhausted`. Failed requests (transport
  errors, invalid pages) do not consume probes; they are per-source state
  (`unavailable_since`, `last_failure_reason`; never a store-wide gap, review r1) and
  use the existing failure backoff (30 s doubling to 900 s).
* Per tick: at most `Limits.bootstrap_probes` (default 8, max 16) probes per source;
  probes share `Limits.pages` and the tick deadline with ready sources. A capped
  source yields its slot; the tick breaks after `len(roster)+1` consecutive capped
  claims, so it cannot spin.
* Bytes: each probe asks for 50 rows and is bounded by `Limits.response_bytes`
  (1 MiB); observed probe pages were about 15-25 KB.
* Time: each probe is bounded by `Limits.request_seconds`; worst case about 9 ticks of
  8 probes; observed 1-3 ticks.
* Memory: one page per request; no history is held.
* Fairness: probing sources are scheduled cold and due immediately; established active
  sources stay hot (up to 4 consecutive claims before one cold), so ready sources are
  not starved (tested).

## Refusal and disclosure

`refused` is persistent and the source is excluded from claims; its coverage is unknown
(`boot_reason = bootstrap_refused:<why>`). `collector_bootstrap_summary()` returns counts
per phase, refused sources with reasons, total `pre_epoch_skipped`/`undated_skipped`,
and `coverage: unknown` while any source is unstarted, probing or refused.
`collector_source_status(limit)` returns the per-source rows. `Collector.tick()` includes
the summary as `bootstrap`. Only `provider_rejects` refusals re-arm automatically
(bounded: once per 24 h, at most 3 times, then `bootstrap_refused_final`, review r3 F6);
every other refusal needs an explicit operator rule, which does not exist.

## Failure and gap policy (review r1, 2026-10-08)

* A single source's failed attempt (transport, invalid page, bootstrap probe) is
  per-source state: `unavailable_since` (first failed attempt start, cleared by the
  next success), `last_failure_at`, `last_failure_reason`, `failures`. It never
  writes `q_recent_gaps`. The summary reports `unavailable` and `unavailable_sources`
  and `coverage: unknown` while any source is unavailable.
* Store-wide gaps mean collection as a whole was not observing: discovery outage,
  lost process (pending attempt), maintenance. The daily-owner hold (00:00 UTC until
  rotation) is checked before any claim; it records one `maintenance` gap from
  midnight to now and stops the round. Overlapping same-reason gaps merge.
* Gap overflow remembers the dropped interval; when expiry brings the table under
  128 rows the flag resets and the interval is re-recorded once as `unknown` if it is
  still in the window.
* `recent_collect.py` exits 0 when the round ran (per-source refusals are listed in
  `collection.refused_sources`), and 1 only when the round was refused (store hold,
  deadline before any attempt, store open/reserve refusal).

## Review r2 rulings (2026-10-08)

* Order: a page reached by following the previous page's cursor that repeats rows
  already served (tail or custody duplicates) sets `cursor_order_unverified=1` with
  first-seen `cursor_order_evidence` {request, previous_request, overlap, rows, at}.
  Rows are still accepted; the source is never refused for this, but its coverage is
  reported `unknown` and no continuity is claimed. Bisection still relies on
  `id > x` filtering: for such providers only the head position is trusted. A provider
  that re-renders one label identity at a new id (Ozone current-state) can raise the
  flag falsely; the error is in the conservative direction.
* Post-establishment history: 16 consecutive full pages whose rows were all pre-epoch
  or late re-arm probing from the current position (`boot_lo`, probe budget applies);
  with the budget spent or a non-numeric position the source is
  `bootstrap_refused:premature_head`.
* A row without `cts` is skipped and counted in `undated_skipped`; the page is
  accepted. A source with `undated_skipped > 0` and `accepted_rows = 0` is
  `undated_only` and reported as coverage `unknown`.
* Late allowance at acceptance time: a row is accepted only if its source time is at
  least max(acquisition start - 48 h, acceptance - 48 h). Older than the epoch floor:
  `pre_epoch_skipped`; inside the epoch but older than acceptance - 48 h:
  `late_skipped`.
* Endpoints: a transport-resolved endpoint is persisted (`endpoint_resolved_at`) and
  re-resolved after 24 h; each probe or page is then 1 GET instead of 2. One HTTP client
  is reused per invocation. A changed resolved endpoint restarts bootstrap for that
  source (different provider namespace). Configured endpoints are not replaced.
* Deterministic provider rejections (HTTP 400/401/403/404/410, attribution mismatch)
  on 6 consecutive attempts refuse the source as
  `bootstrap_refused:provider_rejects:<status>`; transient errors (5xx, timeouts, TLS)
  reset the count and keep the backoff.
* Schema: see review r4 (now v8). Probe bound: 68 proven, 72 enforced by the store.

## Review r3 rulings (2026-10-08)

* Endpoint custody: the transport reports its own DID resolution on a side channel
  (`HTTPTransport.resolutions`), cleared before every request; a `_resolved_endpoint`
  key in a provider body is stripped and ignored. A page body never changes the
  endpoint, the bootstrap state or the probe budget.
* Short pages are confirmed: a probe page with fewer rows than requested and a numeric
  continuation is the head only if the probe at that continuation returns fewer
  rows. If it returns at least as many (and at least 10), the provider caps pages:
  that size becomes the full-page size for the rest of the search. Cost: at most
  one extra probe per establishment (still under the 72 store cap).
* History cap: after establishment, 16 consecutive pages of any size with zero
  accepted rows (pre-epoch, late, undated or future) and an advancing cursor re-probe
  from the current position, or refuse `premature_head`. Only a page with accepted
  rows resets the count.
* `cursor_jump` (disclosure heuristic, not a loss detector): on a full page with
  numeric cursors, `next - request > 2 x rows` sets `cursor_order_unverified` with
  evidence kind `cursor_jump` (overlap evidence is kind `overlap`). Verified
  id-ordered providers advance about one id per row (mod.bsky.app 50-61 per 50 rows,
  skyware 50 per 50); urbanism.plus jumped 1728 ids for 50 rows. The factor is 2, not
  the suggested 8, because a provider skipping two thirds of a page (the r3 fixture)
  stays under 8. Public wording: "page order could not be verified; rows may have
  been skipped by the provider".
* Future rows: source time more than 300 s after acceptance is skipped and counted as
  `future_skipped`.
* Fair bootstrap: at least ceil(attempts/3) of each tick's attempts go to cursorless
  bootstrapping sources (round-robin among them) while any are due; the 8-probe
  per-source per-tick cap is unchanged. Collection for each source runs forward from
  its `established_at`, not from the epoch; the summary exposes
  `established_at: {earliest, latest}`.
* `provider_rejects` refusals clear `unavailable_since` (counted once, as refused) and
  re-arm once after 24 h (`rearm_at`) without resetting `boot_probes`; after 3
  re-arms the reason becomes `bootstrap_refused_final:provider_rejects:<status>`. A
  successful page resets `rearms`.
* `recent_collect.py` exits 75 for the expected daily owner hold (unit
  `SuccessExitStatus=75`); other whole-round refusals stay 1.

## Live C1 addendum (2026-10-08)

* Per-source spacing: every claim (probes included) requires `last_request <= now - 1 s`
  as well as `next_due <= now`. When nothing is due, a round with `sleep` (the service
  passes `time.sleep`) waits for the next due source inside its own deadline, without
  spending a page slot; otherwise the round ends.
* Bootstrap throughput: superseded by review r4 below.
* Round deadline: `LABELWATCH_RECENT_ROUND_SECONDS` (default 10, max 30) in the unit
  environment. Longer rounds give more spaced probes per round at the cost of a longer
  run; the unit stop deadline (45 s) must stay above it plus grace. The time estimate
  is in review r4 below (the earlier 15-20 minute figure did not hold).
* Lost attempts: at round start attempts older than `LABELWATCH_RECENT_STALE_SECONDS`
  (default 60 = 45 s stop deadline + grace) are cleared and disclosed once as a merged
  store-wide `unknown` gap (process loss). The summary reports `pending_attempts`.
* Endpoint resolution is reported on the side channel before the labels request, so a
  failed labels request still keeps our own resolved endpoint (1 GET afterwards).
* Origin: a source's first enrollment fixes `origin`: discovery pages are `discovered`,
  explicit enrollment (init seed) is `seeded`; the summary reports both.

## Review r4 (2026-10-08)

* Reserved share: only claims made through the reserved probing share count toward
  it (at least ceil(attempts/3)). The share claim orders in-progress sources first
  (`probing`, most probes used), then never-failed `unstarted` sources, then failing
  ones, so a source mid-search keeps priority across ticks (persisted phase and probe
  count; no in-memory sticky list). Per tick a source is excluded after its 8th probe
  or after any failure (backoff applies; never re-claimed in the same tick). The cold
  rotation also takes sources without a failure streak before backed-off ones.
* Measured on the r4 first-day fixture (584 cursorless sources: 400 unreachable, 149
  small, 34 growing, one busy source needing about 22 probes; 0.22 s per reachable
  request, 30 s rounds every 2 minutes): with 0.05 s connect failures the busy source
  is established in round 3 (8, 8, 4 probes; about 6 minutes). With 1 s timeouts for
  the unreachable sources it is still established in round 3, but the first pass over
  the 400 unreachable sources (about 1.2 s each, about 25 per round) occupies rounds
  4-16; growing sources get their first pages in round 18 (about 36 minutes) and all
  reachable sources are established by round 27 (about 54 minutes). After that the
  backed-off unreachable sources take about 19 requests per round.
* `cursor_jump` applies to any accepted page with at least 10 rows (forward collection
  and the bootstrap's first collected page), whatever the provider's page cap: more
  than 2 ids per row discloses `cursor_order_unverified`. Probe pages that are not
  accepted are not checked (sparse old history would raise it falsely).
* Short-page confirmation: a short page is the head only if its continuation returns
  fewer rows (or none); otherwise the larger size is adopted as the provider page cap
  and probing continues.
* Schema `q:recent_version` = 8. A store with another version, or missing any column
  the collector uses, is refused: "recent schema v8 required; no implicit conversion;
  old evidence preserved". Nothing is deployed; there is no migration.
* Non-public endpoints: `source()` and the did:web authority check refuse literal IPs
  in loopback, private, link-local, unique-local, reserved, multicast or unspecified
  ranges and the names `localhost`, `*.localhost`, `*.local`, `*.internal`. On the real
  network (no test transport) every request's hostname is also resolved first and
  refused if any address is non-public. DNS rebinding between that check and the
  connect remains possible; the unit's `IPAddressDeny` (storage lane) is the second
  layer.
* WAL: before ingestion the store runs a PASSIVE checkpoint when `state.sqlite-wal`
  exceeds half its budget. If lease-free readers still pin it over budget, the base
  check refuses `wal_budget_pinned`; the collector records that as a store-level
  `maintenance` gap and stops the round, never as a per-source failure.
* Roster size on day one is 580 seeded sources (C1 live), not 584.

## Unknown / not guaranteed

* Per-source coverage starts at `boot_at` (established time). Labels issued between
  acquisition start and establishment whose ids are at or below the established cursor
  are not collected. The window is short (observed minutes) but is not zero.
* Whether providers commit ids in id order is not observable by GET (Postgres sequences
  can commit out of order). A record committed late with an id at or below a stored
  position is missed; this applies to ordinary forward collection too.
* An empty page that advances its cursor is ignored; a provider that relies on that
  would stall at the same position (safe, but no progress). Not observed.
* Unreachable, auth-gated or non-XRPC labelers stay probing or failing with coverage
  unknown; nothing here makes them observable.
* Implementations other than Ozone and skyware-style were not sampled. Their cursors
  are accepted only if they pass the same numeric/advancing checks.
* Stores created before these columns existed fail closed on the bootstrap queries;
  there is no migration (clean start only).
