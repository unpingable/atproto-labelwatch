"""Boundary instability primitives (Phase 1).

Cross-labeler disagreement detection on shared targets.
Computes JSD divergence, contradiction edges, lead/lag edges,
and per-target divergence summaries.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta

from .config import Config
from .label_family import FAMILY_VERSION, normalize_family
from .receipts import config_hash
from .utils import format_ts, parse_ts, stable_json

_log = logging.getLogger("labelwatch.boundary")

# ── Math helpers ──────────────────────────────────────────────────────


def _kl_div(p: dict[str, float], q: dict[str, float]) -> float:
    """KL divergence D(P||Q) in bits. Returns inf if Q has zero where P is nonzero."""
    d = 0.0
    for k, pk in p.items():
        if pk > 0:
            qk = q.get(k, 0.0)
            if qk > 0:
                d += pk * math.log2(pk / qk)
            else:
                return float("inf")
    return d


def jsd(p: dict[str, float], q: dict[str, float]) -> float:
    """Jensen-Shannon divergence (base 2, range [0, 1])."""
    all_keys = set(p) | set(q)
    m = {k: (p.get(k, 0.0) + q.get(k, 0.0)) / 2.0 for k in all_keys}
    return (_kl_div(p, m) + _kl_div(q, m)) / 2.0


def _counts_to_dist(counts: dict[str, int]) -> dict[str, float]:
    """Convert raw counts to a probability distribution."""
    total = sum(counts.values())
    if total == 0:
        return {}
    return {k: v / total for k, v in counts.items()}


def _top_family(dist: dict[str, float]) -> tuple[str | None, float]:
    """Return (top_family, share) from a distribution."""
    if not dist:
        return None, 0.0
    top = max(dist, key=dist.get)  # type: ignore[arg-type]
    return top, dist[top]


# ── Canonical pair ordering ───────────────────────────────────────────


def _ordered_pair(a: str, b: str) -> tuple[str, str]:
    """Canonical lexicographic ordering for labeler pairs."""
    return (a, b) if a <= b else (b, a)


# ── Shared target discovery ──────────────────────────────────────────


def find_shared_targets(conn, window_start: str, window_end: str,
                        min_labelers: int = 2,
                        max_targets: int = 500) -> list[dict]:
    """Find URIs labeled by multiple distinct labelers in the window.

    Returns list of {uri, n_labelers, n_events}, ordered by n_labelers desc.
    Only counts applies (neg=0).
    """
    rows = conn.execute("""
        SELECT uri, COUNT(DISTINCT labeler_did) AS n_labelers,
               COUNT(*) AS n_events
        FROM label_events
        WHERE ts >= ? AND ts < ? AND neg = 0
        GROUP BY uri
        HAVING COUNT(DISTINCT labeler_did) >= ?
        ORDER BY n_labelers DESC
        LIMIT ?
    """, (window_start, window_end, min_labelers, max_targets)).fetchall()
    return [dict(r) for r in rows]


# ── Distribution building ────────────────────────────────────────────


def build_distributions(conn, uris: list[str], window_start: str,
                        window_end: str) -> dict[str, dict[str, dict[str, int]]]:
    """Build per-URI, per-labeler family presence maps.

    Returns: {uri: {labeler_did: {family: 1}}}

    Uses binary presence (1 per family per labeler per target) rather than
    raw event counts. This prevents "who spams updates" from dominating
    the distribution — what matters is the *decision*, not repetition count.

    Only counts applies (neg=0).
    """
    if not uris:
        return {}

    # Batch in chunks of 400 to stay under SQLite variable limit
    result: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    for i in range(0, len(uris), 400):
        chunk = uris[i:i + 400]
        placeholders = ",".join("?" * len(chunk))
        # Use DISTINCT to get unique (uri, labeler, val) tuples
        rows = conn.execute(f"""
            SELECT DISTINCT uri, labeler_did, val
            FROM label_events
            WHERE ts >= ? AND ts < ? AND neg = 0
              AND uri IN ({placeholders})
        """, (window_start, window_end, *chunk)).fetchall()

        for r in rows:
            family = normalize_family(r["val"])
            # Binary: each family counts as 1 per (target, labeler)
            result[r["uri"]][r["labeler_did"]][family] = 1

    return dict(result)


# ── First-seen timestamps (for lead/lag) ──────────────────────────────


def _fetch_first_seen(conn, uris: list[str], window_start: str,
                      window_end: str) -> dict[str, dict[str, str]]:
    """Fetch first label timestamp per (uri, labeler_did).

    Returns: {uri: {labeler_did: first_ts_iso}}
    """
    if not uris:
        return {}

    result: dict[str, dict[str, str]] = defaultdict(dict)

    for i in range(0, len(uris), 400):
        chunk = uris[i:i + 400]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(f"""
            SELECT uri, labeler_did, MIN(ts) AS first_ts
            FROM label_events
            WHERE ts >= ? AND ts < ? AND neg = 0
              AND uri IN ({placeholders})
            GROUP BY uri, labeler_did
        """, (window_start, window_end, *chunk)).fetchall()

        for r in rows:
            result[r["uri"]][r["labeler_did"]] = r["first_ts"]

    return dict(result)


# ── Contradiction edges ──────────────────────────────────────────────


def compute_contradiction_edges(
    distributions: dict[str, dict[str, dict[str, int]]],
    config: Config,
) -> list[dict]:
    """Compute pairwise JSD contradiction edges for shared targets.

    Returns list of edge dicts ready for DB insertion.
    """
    edges = []
    min_events = config.boundary_min_events_per_labeler
    jsd_min = config.boundary_jsd_min
    min_top_share = config.boundary_min_top_share
    top_k = config.boundary_participant_top_k

    for uri, labeler_counts in distributions.items():
        # Filter labelers with enough events
        qualified = {
            did: counts for did, counts in labeler_counts.items()
            if sum(counts.values()) >= min_events
        }
        if len(qualified) < 2:
            continue

        # Cap participants
        if len(qualified) > top_k:
            qualified = dict(
                sorted(qualified.items(),
                       key=lambda x: sum(x[1].values()), reverse=True)[:top_k]
            )

        # Convert to distributions
        dists = {did: _counts_to_dist(c) for did, c in qualified.items()}
        dids = list(dists.keys())

        for i in range(len(dids)):
            for j in range(i + 1, len(dids)):
                did_a, did_b = _ordered_pair(dids[i], dids[j])
                pa, pb = dists[did_a], dists[did_b]

                # Check top family shares
                top_a, share_a = _top_family(pa)
                top_b, share_b = _top_family(pb)
                if share_a < min_top_share or share_b < min_top_share:
                    continue

                divergence = jsd(pa, pb)
                if divergence < jsd_min:
                    continue

                edges.append({
                    "edge_type": "contradiction",
                    "target_uri": uri,
                    "labeler_a": did_a,
                    "labeler_b": did_b,
                    "jsd": round(divergence, 6),
                    "top_family_a": top_a,
                    "top_share_a": round(share_a, 4),
                    "top_family_b": top_b,
                    "top_share_b": round(share_b, 4),
                    "delta_s": None,
                    "overlap": None,
                    "leader_did": None,
                    "n_events_a": sum(qualified[did_a].values()),
                    "n_events_b": sum(qualified[did_b].values()),
                })

    return edges


# ── Lead/lag edges ───────────────────────────────────────────────────


def compute_lead_lag_edges(
    distributions: dict[str, dict[str, dict[str, int]]],
    first_seen: dict[str, dict[str, str]],
    config: Config,
) -> list[dict]:
    """Compute lead/lag edges based on first-seen timestamps + family overlap.

    Returns list of edge dicts ready for DB insertion.
    """
    edges = []
    min_events = config.boundary_min_events_per_labeler
    lag_max_s = config.boundary_lag_max_s
    min_overlap = config.boundary_lag_min_overlap
    top_k = config.boundary_participant_top_k

    for uri, labeler_counts in distributions.items():
        uri_first_seen = first_seen.get(uri, {})
        if not uri_first_seen:
            continue

        # Filter labelers with enough events and first-seen data
        qualified = {
            did: counts for did, counts in labeler_counts.items()
            if sum(counts.values()) >= min_events and did in uri_first_seen
        }
        if len(qualified) < 2:
            continue

        if len(qualified) > top_k:
            qualified = dict(
                sorted(qualified.items(),
                       key=lambda x: sum(x[1].values()), reverse=True)[:top_k]
            )

        dists = {did: _counts_to_dist(c) for did, c in qualified.items()}

        # Sort by first-seen timestamp
        ordered = sorted(qualified.keys(), key=lambda d: uri_first_seen[d])

        for i in range(len(ordered)):
            for j in range(i + 1, len(ordered)):
                leader = ordered[i]
                follower = ordered[j]

                ts_leader = parse_ts(uri_first_seen[leader])
                ts_follower = parse_ts(uri_first_seen[follower])
                delta = (ts_follower - ts_leader).total_seconds()

                if delta <= 0 or delta > lag_max_s:
                    continue

                # Compute overlap (1 - JSD) as similarity measure
                overlap_score = 1.0 - jsd(dists[leader], dists[follower])
                if overlap_score < min_overlap:
                    continue

                did_a, did_b = _ordered_pair(leader, follower)
                edges.append({
                    "edge_type": "lead_lag",
                    "target_uri": uri,
                    "labeler_a": did_a,
                    "labeler_b": did_b,
                    "jsd": round(1.0 - overlap_score, 6),
                    "top_family_a": _top_family(dists[did_a])[0],
                    "top_share_a": round(_top_family(dists[did_a])[1], 4),
                    "top_family_b": _top_family(dists[did_b])[0],
                    "top_share_b": round(_top_family(dists[did_b])[1], 4),
                    "delta_s": round(delta, 1),
                    "overlap": round(overlap_score, 4),
                    "leader_did": leader,
                    "n_events_a": sum(qualified[did_a].values()),
                    "n_events_b": sum(qualified[did_b].values()),
                })

    return edges


# ── Divergence summary (per target) ──────────────────────────────────


def compute_divergence_summaries(
    distributions: dict[str, dict[str, dict[str, int]]],
    config: Config,
) -> list[dict]:
    """Compute per-target divergence summaries.

    Returns list of summary dicts ready for DB insertion.
    """
    summaries = []
    min_events = config.boundary_min_events_per_labeler

    for uri, labeler_counts in distributions.items():
        qualified = {
            did: counts for did, counts in labeler_counts.items()
            if sum(counts.values()) >= min_events
        }
        if len(qualified) < 2:
            continue

        dists = {did: _counts_to_dist(c) for did, c in qualified.items()}

        # Centroid distribution (average across all labelers)
        all_families: set[str] = set()
        for d in dists.values():
            all_families.update(d.keys())
        n = len(dists)
        centroid = {f: sum(d.get(f, 0.0) for d in dists.values()) / n
                    for f in all_families}

        # Mean JSD to centroid
        jsds_to_centroid = [jsd(d, centroid) for d in dists.values()]
        mean_jsd = sum(jsds_to_centroid) / len(jsds_to_centroid)

        # Max pairwise JSD
        max_pair_jsd = 0.0
        dids = list(dists.keys())
        for i in range(len(dids)):
            for j in range(i + 1, len(dids)):
                pair_jsd = jsd(dists[dids[i]], dists[dids[j]])
                if pair_jsd > max_pair_jsd:
                    max_pair_jsd = pair_jsd

        # Top families across all events
        total_counts: dict[str, int] = defaultdict(int)
        for counts in qualified.values():
            for f, c in counts.items():
                family = normalize_family(f) if f == f else f  # already normalized
                total_counts[f] += c
        grand_total = sum(total_counts.values())
        top_families = sorted(total_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        top_families_list = [
            {"family": f, "share": round(c / grand_total, 4)}
            for f, c in top_families
        ]

        summaries.append({
            "target_uri": uri,
            "n_labelers": len(qualified),
            "n_events": sum(sum(c.values()) for c in qualified.values()),
            "mean_jsd_to_centroid": round(mean_jsd, 6),
            "max_jsd_pair": round(max_pair_jsd, 6),
            "top_families_json": json.dumps(top_families_list, separators=(",", ":")),
        })

    return summaries


# ── Storage ──────────────────────────────────────────────────────────


def _store_edges(conn, edges: list[dict], window_start: str, window_end: str,
                 cfg_hash: str, computed_at: str) -> int:
    """Write edges to boundary_edges table. Returns count inserted."""
    inserted = 0
    for e in edges:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO boundary_edges (
                    edge_type, target_uri, window_start, window_end,
                    labeler_a, labeler_b, jsd, top_family_a, top_share_a,
                    top_family_b, top_share_b, delta_s, overlap, leader_did,
                    n_events_a, n_events_b, family_version, config_hash, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                e["edge_type"], e["target_uri"], window_start, window_end,
                e["labeler_a"], e["labeler_b"], e["jsd"],
                e["top_family_a"], e["top_share_a"],
                e["top_family_b"], e["top_share_b"],
                e["delta_s"], e["overlap"], e["leader_did"],
                e["n_events_a"], e["n_events_b"],
                FAMILY_VERSION, cfg_hash, computed_at,
            ))
            inserted += 1
        except Exception as exc:
            _log.warning("Failed to insert boundary edge: %s", exc)
    return inserted


def _store_summaries(conn, summaries: list[dict], window_start: str,
                     window_end: str, cfg_hash: str, computed_at: str) -> int:
    """Write divergence summaries to boundary_targets table. Returns count inserted."""
    inserted = 0
    for s in summaries:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO boundary_targets (
                    target_uri, window_start, window_end,
                    n_labelers, n_events, mean_jsd_to_centroid, max_jsd_pair,
                    top_families_json, family_version, config_hash, computed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                s["target_uri"], window_start, window_end,
                s["n_labelers"], s["n_events"],
                s["mean_jsd_to_centroid"], s["max_jsd_pair"],
                s["top_families_json"],
                FAMILY_VERSION, cfg_hash, computed_at,
            ))
            inserted += 1
        except Exception as exc:
            _log.warning("Failed to insert boundary target summary: %s", exc)
    return inserted


# ── Retention ────────────────────────────────────────────────────────


def _prune_old(conn, now: datetime, retention_days: int = 30) -> None:
    """Delete boundary data older than retention period."""
    cutoff = format_ts(now - timedelta(days=retention_days))
    conn.execute("DELETE FROM boundary_edges WHERE computed_at < ?", (cutoff,))
    conn.execute("DELETE FROM boundary_targets WHERE computed_at < ?", (cutoff,))


# ── Orchestrator ─────────────────────────────────────────────────────


def run_boundary_pass(conn, config: Config, now: datetime) -> dict:
    """Run the full boundary instability pass.

    Returns summary stats dict.
    """
    window_hours = config.boundary_window_hours
    window_start = format_ts(now - timedelta(hours=window_hours))
    window_end = format_ts(now)
    computed_at = format_ts(now)
    cfg_hash = config_hash(config.to_receipt_dict())

    # 1. Find shared targets
    shared = find_shared_targets(
        conn, window_start, window_end,
        min_labelers=config.boundary_min_labelers,
        max_targets=config.boundary_max_targets,
    )
    if not shared:
        empty_stats = {
            "shared_targets": 0,
            "contradiction_edges": 0,
            "lead_lag_edges": 0,
            "edges_stored": 0,
            "summaries_stored": 0,
        }
        from . import db as _db
        _db.set_meta(conn, "boundary_last_run_at", computed_at)
        _db.set_meta(conn, "boundary_last_counts_json", stable_json(empty_stats))
        return empty_stats

    uris = [t["uri"] for t in shared]

    # 2. Build distributions
    distributions = build_distributions(conn, uris, window_start, window_end)

    # 3. Fetch first-seen timestamps (for lead/lag)
    first_seen = _fetch_first_seen(conn, uris, window_start, window_end)

    # 4. Compute edges
    contradiction_edges = compute_contradiction_edges(distributions, config)
    lead_lag_edges = compute_lead_lag_edges(distributions, first_seen, config)
    all_edges = contradiction_edges + lead_lag_edges

    # 5. Compute divergence summaries
    summaries = compute_divergence_summaries(distributions, config)

    # 6. Store results (DELETE + INSERT for current window to be idempotent)
    conn.execute(
        "DELETE FROM boundary_edges WHERE window_start = ? AND family_version = ?",
        (window_start, FAMILY_VERSION),
    )
    conn.execute(
        "DELETE FROM boundary_targets WHERE window_start = ? AND family_version = ?",
        (window_start, FAMILY_VERSION),
    )

    n_edges = _store_edges(conn, all_edges, window_start, window_end, cfg_hash, computed_at)
    n_summaries = _store_summaries(conn, summaries, window_start, window_end, cfg_hash, computed_at)

    # 7. Prune old data
    _prune_old(conn, now)

    stats = {
        "shared_targets": len(shared),
        "contradiction_edges": len(contradiction_edges),
        "lead_lag_edges": len(lead_lag_edges),
        "edges_stored": n_edges,
        "summaries_stored": n_summaries,
    }
    _log.info(
        "boundary pass: %d shared targets, %d contradiction edges, "
        "%d lead/lag edges, %d summaries",
        stats["shared_targets"], stats["contradiction_edges"],
        stats["lead_lag_edges"], stats["summaries_stored"],
    )

    # Write bake-visibility meta keys
    from . import db as _db
    _db.set_meta(conn, "boundary_last_run_at", computed_at)
    _db.set_meta(conn, "boundary_last_counts_json", stable_json(stats))

    return stats
