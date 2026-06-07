"""Label lifetime analysis.

v1 metric: **add-cohort lifetime by authority effect** in a rolling N-day
window. NOT "label lifetime" globally — see the contract below for the
distinction that prevents the graph from writing checks the ontology
cannot cash.

Contract
--------
For each stateful label key:
    (labeler_did, uri, val)

A NEW ADD is a positive event (neg=0) that transitions the key from
inactive to active. The state machine is per-key:

    inactive -> positive  = add        (starts a lifetime cohort)
    active   -> positive  = reassert   (counted separately; NOT a new add)
    active   -> negation  = removal    (closes the active cohort)
    inactive -> negation  = stray      (ignored; predates window or noise)

A new add's lifetime is closed by the FIRST SUBSEQUENT negation for the
same key. If no negation is observed before window_end, the cohort is
RIGHT-CENSORED — reported as `still_open` and EXCLUDED from
median/p90 lifetime calculations. Median/p90 are over CLOSED intervals
only; this is not survival analysis with censoring.

v1 simplification (documented bias)
-----------------------------------
Pre-window state is unknown. A positive event at the start of the
window with no prior in-window event is treated as a new add even if
the label was actually active pre-window. This slightly inflates
`added_cohort` and slightly deflates median lifetime (because a
real-reassertion-that-looks-like-an-add will get an artificially short
"lifetime" if a removal follows within the window). v2 should query
last-event-per-key before window_start to disambiguate.

Acceptance invariants (asserted in compute path)
------------------------------------------------
  removed_closed + still_open == added_cohort      (per effect bucket)
  all closed lifetimes >= 0
  unknown authority_effect bucket is included, not silently dropped
"""
from __future__ import annotations

import logging
import statistics
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple

from .authority_inventory import _resolve_val_effect
from .label_family import AUTHORITY_EFFECT_ORDER, normalize_family

log = logging.getLogger(__name__)

# --- Result cache ---------------------------------------------------------
# Coarse cache: store the full computed result keyed by `days`, with a TTL.
# Cheaper than the per-day shard pattern used for authority because lifetime
# is fundamentally a cross-day state machine — we can't honestly assemble
# 30d from 30 cached single-day shards (a label added on day 1 might close
# on day 28; per-day shards lose that pairing).
#
# TTL is set to match the report cycle (3600s). Sequential cycles within
# the same process serve from cache. Stale entries trigger a recompute
# (logged). On restart the cache is empty and the first request pays full
# cold cost (~270s on prod 30d).
#
# Build label_state incrementally is the v2+ path if this cache + cycle
# interval ever stop being enough.
_LIFETIME_RESULT_CACHE: Dict[int, Tuple[Dict[str, Any], datetime]] = {}
_LIFETIME_CACHE_LOCK = threading.Lock()
_LIFETIME_CACHE_TTL_S = 3600


def _parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # Accept the two ts shapes labelwatch stores: "...Z" and "...+00:00".
    # Strip trailing Z and parse as UTC.
    s = s.rstrip("Z")
    try:
        # Try common formats fast; fall back to fromisoformat.
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def label_lifetime_by_effect(conn, days: int = 30) -> Dict[str, Any]:
    """Add-cohort lifetime by authority_effect over the last `days` days.

    Cached: identical days arg served from process memory until TTL expiry
    (see _LIFETIME_CACHE_TTL_S). Cache hits log at INFO with age; misses
    log compute duration. Cold cost is high (~270s on prod 30d); the cache
    keeps subsequent in-process cycles cheap.

    See module docstring for the v1 contract. Returns:

    {
      "window_days": int,
      "window_start": ISO,
      "window_end": ISO,
      "by_effect": {
        effect: {
          "added_cohort":            int,
          "removed_closed":          int,
          "still_open":              int,
          "open_share":              float,  # still_open / added_cohort
          "median_closed_lifetime_s": float | None,
          "p90_closed_lifetime_s":    float | None,
          "reassertions_ignored":    int,    # positives counted as reassert
          "positive_events_raw":     int,    # total positives observed
        },
        ...  # one entry per AUTHORITY_EFFECT_ORDER (unknown included)
      },
      "totals": {
        "added_cohort": int, "removed_closed": int, "still_open": int,
      },
      "sql_query_seconds":   float,
      "compute_seconds":     float,
      "served_from_cache":   bool,
      "cache_age_seconds":   float | None,
    }
    """
    now = datetime.now(timezone.utc)
    with _LIFETIME_CACHE_LOCK:
        cached = _LIFETIME_RESULT_CACHE.get(days)
    if cached is not None:
        result, computed_at = cached
        age = (now - computed_at).total_seconds()
        if age < _LIFETIME_CACHE_TTL_S:
            log.info(
                "label_lifetime_by_effect(days=%d): cache HIT (age=%.0fs, ttl=%ds)",
                days, age, _LIFETIME_CACHE_TTL_S,
            )
            # Annotate with serve-from-cache + age so callers can render
            # freshness in the section footer without re-storing the dict.
            return {**result, "served_from_cache": True, "cache_age_seconds": round(age, 1)}
        log.info(
            "label_lifetime_by_effect(days=%d): cache STALE (age=%.0fs, ttl=%ds), recomputing",
            days, age, _LIFETIME_CACHE_TTL_S,
        )

    log.info("label_lifetime_by_effect(days=%d): cache MISS, computing", days)
    result = _compute_label_lifetime_by_effect(conn, days)
    log.info(
        "label_lifetime_by_effect(days=%d): computed in sql=%.2fs compute=%.2fs",
        days, result.get("sql_query_seconds", 0.0), result.get("compute_seconds", 0.0),
    )
    with _LIFETIME_CACHE_LOCK:
        _LIFETIME_RESULT_CACHE[days] = (result, datetime.now(timezone.utc))
    return {**result, "served_from_cache": False, "cache_age_seconds": 0.0}


def _compute_label_lifetime_by_effect(conn, days: int) -> Dict[str, Any]:
    """Compute the lifetime result without caching. See label_lifetime_by_effect."""
    now = datetime.now(timezone.utc)
    end = now.replace(microsecond=0)
    start = end - timedelta(days=days)
    end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Stream events ordered by key + ts so we can groupby() per-key without
    # materializing all rows. SQLite will use a temp btree for the sort
    # (no composite index on labeler_did+uri+val+ts); the cost is captured
    # in sql_query_seconds and surfaced.
    t_sql = time.perf_counter()
    cursor = conn.execute(
        """
        SELECT labeler_did, uri, val, ts, neg
        FROM label_events
        WHERE ts >= ? AND ts < ?
        ORDER BY labeler_did, uri, val, ts
        """,
        (start_iso, end_iso),
    )

    # Per-val accumulators (re-keyed to effect after classification).
    per_val_labelers: Dict[str, set] = defaultdict(set)
    closed_lifetimes_by_val: Dict[str, list] = defaultdict(list)
    still_open_by_val: Dict[str, int] = defaultdict(int)
    added_cohort_by_val: Dict[str, int] = defaultdict(int)
    removed_closed_by_val: Dict[str, int] = defaultdict(int)
    reassertions_by_val: Dict[str, int] = defaultdict(int)
    positives_raw_by_val: Dict[str, int] = defaultdict(int)

    def _key_fn(r):
        return (r["labeler_did"], r["uri"], r["val"])

    for key, events in groupby(cursor, key=_key_fn):
        labeler_did, _uri, val = key
        per_val_labelers[val].add(labeler_did)
        active = False
        pending_add_ts: Optional[str] = None
        for e in events:
            n = int(e["neg"] or 0)
            if n == 0:
                positives_raw_by_val[val] += 1
                if not active:
                    # state: inactive -> positive = NEW ADD
                    pending_add_ts = e["ts"]
                    active = True
                    added_cohort_by_val[val] += 1
                else:
                    # state: active -> positive = REASSERT (not a new cohort)
                    reassertions_by_val[val] += 1
            else:
                if active and pending_add_ts is not None:
                    # state: active -> negation = REMOVAL (closes cohort)
                    add_dt = _parse_ts(pending_add_ts)
                    close_dt = _parse_ts(e["ts"])
                    if add_dt and close_dt:
                        delta = (close_dt - add_dt).total_seconds()
                        if delta >= 0:
                            closed_lifetimes_by_val[val].append(delta)
                            removed_closed_by_val[val] += 1
                        # else: non-monotonic ts — shouldn't happen given
                        # ORDER BY ts but skip defensively
                    active = False
                    pending_add_ts = None
                # else: stray negation (predates window or noise) — ignore
        if active and pending_add_ts is not None:
            still_open_by_val[val] += 1

    sql_seconds = time.perf_counter() - t_sql

    t_compute = time.perf_counter()
    # Classify each val ONCE using the global per-val labeler set so the
    # fallback resolution matches build_authority_effect_inventory and
    # unknown_decomposition.
    val_effect: Dict[str, str] = {}
    for v, labelers in per_val_labelers.items():
        family = normalize_family(v)
        effect, _ = _resolve_val_effect(family, labelers)
        val_effect[v] = effect if effect in AUTHORITY_EFFECT_ORDER else "unknown"

    # Aggregate per-effect; include every declared effect so unknown isn't
    # silently dropped when no vals classify to it.
    by_effect: Dict[str, Dict[str, Any]] = {
        eff: {
            "added_cohort": 0,
            "removed_closed": 0,
            "still_open": 0,
            "_closed_lifetimes": [],
            "reassertions_ignored": 0,
            "positive_events_raw": 0,
        }
        for eff in AUTHORITY_EFFECT_ORDER
    }
    for v, eff in val_effect.items():
        slot = by_effect[eff]
        slot["added_cohort"] += added_cohort_by_val.get(v, 0)
        slot["removed_closed"] += removed_closed_by_val.get(v, 0)
        slot["still_open"] += still_open_by_val.get(v, 0)
        slot["_closed_lifetimes"].extend(closed_lifetimes_by_val.get(v, []))
        slot["reassertions_ignored"] += reassertions_by_val.get(v, 0)
        slot["positive_events_raw"] += positives_raw_by_val.get(v, 0)

    out: Dict[str, Dict[str, Any]] = {}
    for eff, slot in by_effect.items():
        lifetimes = slot["_closed_lifetimes"]
        median_s = statistics.median(lifetimes) if lifetimes else None
        p90_s = None
        if lifetimes:
            sl = sorted(lifetimes)
            idx = max(0, min(len(sl) - 1, int(round(0.90 * (len(sl) - 1)))))
            p90_s = sl[idx]
        added = slot["added_cohort"]
        # Acceptance invariant: removed_closed + still_open == added_cohort.
        # If violated, log and surface in the output so a debug consumer
        # notices instead of trusting silently.
        if slot["removed_closed"] + slot["still_open"] != added:
            log.warning(
                "lifetime invariant violated for effect=%s: "
                "removed_closed=%d + still_open=%d != added_cohort=%d",
                eff, slot["removed_closed"], slot["still_open"], added,
            )
        out[eff] = {
            "added_cohort": added,
            "removed_closed": slot["removed_closed"],
            "still_open": slot["still_open"],
            "open_share": (slot["still_open"] / added) if added else 0.0,
            "median_closed_lifetime_s": median_s,
            "p90_closed_lifetime_s": p90_s,
            "reassertions_ignored": slot["reassertions_ignored"],
            "positive_events_raw": slot["positive_events_raw"],
        }

    compute_seconds = time.perf_counter() - t_compute

    return {
        "window_days": days,
        "window_start": start_iso,
        "window_end": end_iso,
        "by_effect": out,
        "totals": {
            "added_cohort": sum(s["added_cohort"] for s in out.values()),
            "removed_closed": sum(s["removed_closed"] for s in out.values()),
            "still_open": sum(s["still_open"] for s in out.values()),
        },
        "sql_query_seconds": round(sql_seconds, 2),
        "compute_seconds": round(compute_seconds, 2),
    }
