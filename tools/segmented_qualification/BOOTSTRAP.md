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
  cursor (the last row id). The id namespace is monotone.
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
  errors, invalid pages) do not consume probes; they record a `source_unavailable`
  gap and use the existing failure backoff (30 s doubling to 900 s).
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
the summary as `bootstrap`. Re-arming a refused source needs an explicit operator
rule; none exists yet.

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
