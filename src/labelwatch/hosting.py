"""Hosting-locus analysis: PDS host classification and labeled-target enrichment.

Consumes actor_identity_facts from the driftwatch facts bridge and classifies
PDS hosts into provider groups for analysis.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Optional

_log = logging.getLogger(__name__)


def extract_host_family(pds_host: Optional[str]) -> Optional[str]:
    """Extract the registerable domain / host family from a PDS hostname.

    Examples:
        mahmouds.pds.rip -> pds.rip
        stropharia.us-west.host.bsky.network -> host.bsky.network
        blacksky.app -> blacksky.app
        pds.example.com -> example.com
        localhost:8080 -> localhost

    Simple heuristic: take the last two labels (or three if the second-to-last
    is a known infrastructure segment like 'host' or 'us-west').
    """
    if not pds_host:
        return None

    # Strip port if present
    host = pds_host.split(":")[0].rstrip(".")

    parts = host.split(".")
    if len(parts) <= 2:
        return host

    # For *.host.bsky.network pattern: the "host" segment is infra, go deeper
    # General rule: take last 2 labels, unless that gives us a known TLD-like
    # suffix, in which case take 3.
    # Special case: if second-to-last part looks like a region/infra label
    # (contains a hyphen like us-west, us-east), take one more.
    candidate = ".".join(parts[-2:])

    # Known multi-level suffixes
    multi_level = {"bsky.network", "bsky.social"}
    if candidate in multi_level:
        if len(parts) >= 3:
            return ".".join(parts[-3:])
        return candidate

    return candidate


def classify_host(
    conn: sqlite3.Connection,
    pds_host: Optional[str],
    resolver_status: Optional[str],
) -> tuple[str, str, bool]:
    """Classify a PDS host into (provider_group, provider_label, is_major).

    Checks provider_registry for exact then suffix matches.
    Falls back to 'unknown' for unresolved, 'one_off' for resolved unknowns.
    """
    if not pds_host or resolver_status != "ok":
        return ("unknown", "Unresolved/Unknown", False)

    # Exact match first
    row = conn.execute(
        "SELECT provider_group, provider_label, is_major_provider "
        "FROM provider_registry WHERE match_type = 'exact' AND host_pattern = ?",
        (pds_host,),
    ).fetchone()
    if row:
        return (row[0], row[1], bool(row[2]))

    # Suffix match: check if pds_host ends with any suffix pattern
    rows = conn.execute(
        "SELECT host_pattern, provider_group, provider_label, is_major_provider "
        "FROM provider_registry WHERE match_type = 'suffix' "
        "ORDER BY length(host_pattern) DESC"
    ).fetchall()
    for pattern, group, label, is_major in rows:
        if pds_host == pattern or pds_host.endswith("." + pattern):
            return (group, label, bool(is_major))

    return ("one_off", pds_host, False)


@dataclass
class HostingLocusRow:
    pds_host: Optional[str]
    host_family: Optional[str]
    provider_group: str
    provider_label: str
    is_major_provider: bool
    labeled_target_count: int
    unique_accounts: int
    unique_labelers: int
    resolved_count: int
    unresolved_count: int
    invalid_handle_count: int


def query_labeled_targets_by_host(
    conn: sqlite3.Connection,
    days: int = 7,
    exclude_majors: bool = False,
) -> list[HostingLocusRow]:
    """Join labeled targets with identity facts and classify by provider.

    Requires facts.sqlite attached as 'drift'.
    """
    # Check if drift is attached and has actor_identity_facts
    try:
        conn.execute("SELECT 1 FROM drift.actor_identity_facts LIMIT 1")
    except sqlite3.OperationalError:
        _log.warning("drift.actor_identity_facts not available")
        return []

    cutoff = f"-{days} days"

    rows = conn.execute("""
        SELECT
            aif.pds_host,
            aif.resolver_status,
            aif.handle,
            le.labeler_did,
            le.target_did
        FROM label_events le
        LEFT JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
        WHERE le.target_did IS NOT NULL
          AND le.ts >= datetime('now', ?)
    """, (cutoff,)).fetchall()

    if not rows:
        return []

    # Classify each row and aggregate
    from collections import defaultdict

    # key: (pds_host, provider_group, provider_label, is_major)
    agg: dict[tuple, dict] = defaultdict(lambda: {
        "targets": 0,
        "accounts": set(),
        "labelers": set(),
        "resolved": 0,
        "unresolved": 0,
        "invalid_handle": 0,
    })

    for pds_host, resolver_status, handle, labeler_did, target_did in rows:
        group, label, is_major = classify_host(conn, pds_host, resolver_status)
        key = (pds_host, group, label, is_major)
        bucket = agg[key]
        bucket["targets"] += 1
        bucket["accounts"].add(target_did)
        bucket["labelers"].add(labeler_did)
        if resolver_status == "ok":
            bucket["resolved"] += 1
        else:
            bucket["unresolved"] += 1
        if handle == "handle.invalid" or (resolver_status == "ok" and not handle):
            bucket["invalid_handle"] += 1

    results = []
    for (pds_host, group, label, is_major), bucket in agg.items():
        if exclude_majors and is_major:
            continue
        results.append(HostingLocusRow(
            pds_host=pds_host,
            host_family=extract_host_family(pds_host),
            provider_group=group,
            provider_label=label,
            is_major_provider=is_major,
            labeled_target_count=bucket["targets"],
            unique_accounts=len(bucket["accounts"]),
            unique_labelers=len(bucket["labelers"]),
            resolved_count=bucket["resolved"],
            unresolved_count=bucket["unresolved"],
            invalid_handle_count=bucket["invalid_handle"],
        ))

    results.sort(key=lambda r: r.labeled_target_count, reverse=True)
    return results


def query_hosting_summary(
    conn: sqlite3.Connection,
    days: int = 7,
) -> dict:
    """High-level hosting locus summary stats."""
    rows = query_labeled_targets_by_host(conn, days=days)
    if not rows:
        return {"status": "no_data"}

    total_targets = sum(r.labeled_target_count for r in rows)
    total_resolved = sum(r.resolved_count for r in rows)
    total_unresolved = sum(r.unresolved_count for r in rows)
    total_resolved_accounts = sum(r.unique_accounts for r in rows if r.provider_group != "unknown")
    major_targets = sum(r.labeled_target_count for r in rows if r.is_major_provider)
    non_major = [r for r in rows if not r.is_major_provider
                 and r.provider_group != "unknown"]
    invalid_handles = sum(r.invalid_handle_count for r in rows)

    # Host family rollup for non-majors
    from collections import defaultdict
    family_counts: dict[str, int] = defaultdict(int)
    for r in non_major:
        fam = r.host_family or r.pds_host or "unknown"
        family_counts[fam] += r.labeled_target_count
    top_families = sorted(family_counts.items(), key=lambda x: -x[1])[:10]

    # Actor coverage: unique resolved DIDs / unique target DIDs
    # (not event-based — avoids inflating coverage because heavily-labeled accounts are resolved)
    cutoff = f"-{days} days"
    try:
        cov_row = conn.execute("""
            SELECT
                (SELECT COUNT(DISTINCT target_did) FROM label_events
                 WHERE target_did IS NOT NULL AND ts >= datetime('now', ?)) as total_dids,
                (SELECT COUNT(DISTINCT le.target_did) FROM label_events le
                 JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
                 WHERE aif.resolver_status = 'ok'
                   AND le.target_did IS NOT NULL AND le.ts >= datetime('now', ?)) as resolved_dids
        """, (cutoff, cutoff)).fetchone()
        total_actor_dids = cov_row[0] if cov_row else 0
        resolved_actor_dids = cov_row[1] if cov_row else 0
    except Exception:
        total_actor_dids = 0
        resolved_actor_dids = 0

    event_coverage_pct = round(100.0 * total_resolved / total_targets, 1) if total_targets else 0

    return {
        "status": "ok",
        "days": days,
        "total_labeled_targets": total_targets,
        "total_target_dids": total_actor_dids,
        "resolved_target_dids": resolved_actor_dids,
        "resolved_pct": round(100.0 * resolved_actor_dids / total_actor_dids, 1) if total_actor_dids else 0,
        "event_coverage_pct": event_coverage_pct,
        "major_provider_pct": round(100.0 * major_targets / total_resolved, 1) if total_resolved else 0,
        "non_major_targets": sum(r.labeled_target_count for r in non_major),
        "non_major_hosts": len(set(r.pds_host for r in non_major if r.pds_host)),
        "non_major_host_families": len(set(r.host_family for r in non_major if r.host_family)),
        "invalid_handle_count": invalid_handles,
        "unresolved_count": total_unresolved,
        "top_non_major_families": top_families,
        "top_non_major_hosts": [
            {
                "host": r.pds_host,
                "family": r.host_family,
                "group": r.provider_group,
                "targets": r.labeled_target_count,
                "accounts": r.unique_accounts,
                "invalid_handles": r.invalid_handle_count,
            }
            for r in non_major[:20]
        ],
        "labeler_host_skew": query_labeler_host_skew(conn, days=days),
        "non_major_baseline_pct": round(
            100.0 * sum(r.unique_accounts for r in non_major)
            / total_resolved_accounts, 2
        ) if total_resolved_accounts else 0,
    }


def query_labeler_host_skew(
    conn: sqlite3.Connection,
    days: int = 7,
    min_targets: int = 10,
) -> list[dict]:
    """Per-labeler hosting skew: what % of each labeler's resolved targets are on non-major hosts.

    Requires facts.sqlite attached as 'drift'.
    """
    try:
        conn.execute("SELECT 1 FROM drift.actor_identity_facts LIMIT 1")
    except sqlite3.OperationalError:
        return []

    cutoff = f"-{days} days"

    rows = conn.execute("""
        WITH labeler_host AS (
            SELECT
                le.labeler_did,
                CASE
                    WHEN aif.pds_host LIKE '%.bsky.network' OR aif.pds_host = 'bsky.social'
                    THEN 'major' ELSE 'non_major'
                END as host_class,
                COUNT(DISTINCT le.target_did) as unique_targets
            FROM label_events le
            JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
            WHERE le.target_did IS NOT NULL
              AND le.ts >= datetime('now', ?)
              AND aif.resolver_status = 'ok'
            GROUP BY le.labeler_did, host_class
        ),
        labeler_totals AS (
            SELECT labeler_did, SUM(unique_targets) as total_targets
            FROM labeler_host
            GROUP BY labeler_did
            HAVING total_targets >= ?
        )
        SELECT
            lt.labeler_did,
            lt.total_targets,
            COALESCE(nm.unique_targets, 0) as non_major_targets,
            ROUND(100.0 * COALESCE(nm.unique_targets, 0) / lt.total_targets, 2) as non_major_pct
        FROM labeler_totals lt
        LEFT JOIN labeler_host nm
            ON nm.labeler_did = lt.labeler_did AND nm.host_class = 'non_major'
        ORDER BY non_major_pct DESC
    """, (cutoff, min_targets)).fetchall()

    # Resolve handles
    results = []
    for labeler_did, total, non_major, pct in rows:
        handle_row = conn.execute(
            "SELECT handle FROM labelers WHERE labeler_did = ?", (labeler_did,)
        ).fetchone()
        handle = handle_row[0] if handle_row else None
        results.append({
            "labeler_did": labeler_did,
            "handle": handle,
            "total_resolved_targets": total,
            "non_major_targets": non_major,
            "non_major_pct": pct,
        })

    return results


@dataclass
class HostDistributionRow:
    host_family: str
    provider_group: str
    provider_label: str
    is_major_provider: bool
    overall_accounts: int
    overall_pct: float
    labeled_accounts: int
    labeled_pct: float
    delta_pct: float  # labeled_pct - overall_pct (positive = over-represented in labels)


def query_population_comparison(
    conn: sqlite3.Connection,
    days: int = 7,
    min_accounts: int = 5,
) -> dict:
    """Compare host distribution: labeled targets vs overall resolved population.

    Returns dict with:
      - overall_resolved: total resolved accounts in facts bridge
      - labeled_resolved: labeled targets with resolved host (in time window)
      - coverage_pct: labeled_resolved / overall_resolved
      - rows: list of HostDistributionRow, sorted by abs(delta) descending
      - caveats: list of strings about data quality
    """
    try:
        conn.execute("SELECT 1 FROM drift.actor_identity_facts LIMIT 1")
    except sqlite3.OperationalError:
        return {"status": "no_facts", "caveats": ["facts.sqlite not attached"]}

    cutoff = f"-{days} days"

    # Overall population: all resolved accounts in facts bridge, grouped by host family
    overall_rows = conn.execute("""
        SELECT pds_host, COUNT(*) as n
        FROM drift.actor_identity_facts
        WHERE resolver_status = 'ok'
          AND pds_host IS NOT NULL AND pds_host != ''
        GROUP BY pds_host
    """).fetchall()

    # Labeled population: resolved targets in label_events within window
    labeled_rows = conn.execute("""
        SELECT aif.pds_host, COUNT(DISTINCT le.target_did) as n
        FROM label_events le
        JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
        WHERE le.target_did IS NOT NULL
          AND le.ts >= datetime('now', ?)
          AND aif.resolver_status = 'ok'
          AND aif.pds_host IS NOT NULL AND aif.pds_host != ''
        GROUP BY aif.pds_host
    """, (cutoff,)).fetchall()

    if not overall_rows:
        return {"status": "no_data", "caveats": ["no resolved accounts in facts bridge"]}

    # Roll up by host family + classify
    from collections import defaultdict

    def _rollup(raw_rows):
        family_totals: dict[str, dict] = defaultdict(lambda: {"count": 0})
        for pds_host, count in raw_rows:
            family = extract_host_family(pds_host) or pds_host
            group, label, is_major = classify_host(conn, pds_host, "ok")
            key = family
            bucket = family_totals[key]
            bucket["count"] += count
            bucket["group"] = group
            bucket["label"] = label
            bucket["is_major"] = is_major
        return family_totals

    overall_by_family = _rollup(overall_rows)
    labeled_by_family = _rollup(labeled_rows)

    overall_total = sum(b["count"] for b in overall_by_family.values())
    labeled_total = sum(b["count"] for b in labeled_by_family.values())

    if overall_total == 0:
        return {"status": "no_data", "caveats": ["zero resolved accounts"]}

    # Build comparison rows for all families with enough accounts in either population
    all_families = set(overall_by_family) | set(labeled_by_family)
    results = []
    for family in all_families:
        o = overall_by_family.get(family, {"count": 0, "group": "unknown", "label": "?", "is_major": False})
        l = labeled_by_family.get(family, {"count": 0})
        if o["count"] < min_accounts and l.get("count", 0) < min_accounts:
            continue
        o_pct = round(100.0 * o["count"] / overall_total, 2)
        l_pct = round(100.0 * l.get("count", 0) / labeled_total, 2) if labeled_total else 0
        results.append(HostDistributionRow(
            host_family=family,
            provider_group=o.get("group", "unknown"),
            provider_label=o.get("label", "?"),
            is_major_provider=o.get("is_major", False),
            overall_accounts=o["count"],
            overall_pct=o_pct,
            labeled_accounts=l.get("count", 0),
            labeled_pct=l_pct,
            delta_pct=round(l_pct - o_pct, 2),
        ))

    results.sort(key=lambda r: abs(r.delta_pct), reverse=True)

    # Caveats
    caveats = []
    coverage = round(100.0 * labeled_total / overall_total, 1) if overall_total else 0
    if coverage < 10:
        caveats.append(f"low coverage ({coverage}%) — labeled population is small relative to overall")
    if overall_total < 50000:
        caveats.append(f"overall population is {overall_total:,} — partial resolver coverage")

    return {
        "status": "ok",
        "days": days,
        "overall_resolved": overall_total,
        "labeled_resolved": labeled_total,
        "coverage_pct": coverage,
        "rows": results,
        "caveats": caveats,
    }


def query_host_family_drilldown(
    conn: sqlite3.Connection,
    host_family: str,
    days: int = 7,
) -> dict:
    """Drilldown for a specific host family: which labelers label its accounts, activity over time.

    Requires facts.sqlite attached as 'drift'.
    """
    try:
        conn.execute("SELECT 1 FROM drift.actor_identity_facts LIMIT 1")
    except sqlite3.OperationalError:
        return {"status": "no_facts"}

    cutoff_7d = f"-{days} days"
    cutoff_30d = "-30 days"

    # Find all PDS hosts in this family
    all_hosts = conn.execute(
        "SELECT DISTINCT pds_host FROM drift.actor_identity_facts "
        "WHERE resolver_status = 'ok' AND pds_host IS NOT NULL"
    ).fetchall()
    family_hosts = [h[0] for h in all_hosts if extract_host_family(h[0]) == host_family]

    if not family_hosts:
        return {"status": "no_data", "host_family": host_family}

    placeholders = ",".join("?" * len(family_hosts))

    # Overall accounts on this host family
    overall_count = conn.execute(
        f"SELECT COUNT(*) FROM drift.actor_identity_facts "
        f"WHERE pds_host IN ({placeholders}) AND resolver_status = 'ok'",
        family_hosts,
    ).fetchone()[0]

    # Labeled targets in window, broken down by labeler
    labeler_rows = conn.execute(f"""
        SELECT le.labeler_did, COUNT(DISTINCT le.target_did) as targets
        FROM label_events le
        JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
        WHERE aif.pds_host IN ({placeholders})
          AND aif.resolver_status = 'ok'
          AND le.target_did IS NOT NULL
          AND le.ts >= datetime('now', ?)
        GROUP BY le.labeler_did
        ORDER BY targets DESC
    """, family_hosts + [cutoff_7d]).fetchall()

    # Resolve labeler handles
    labelers = []
    for labeler_did, targets in labeler_rows:
        handle_row = conn.execute(
            "SELECT handle FROM labelers WHERE labeler_did = ?", (labeler_did,)
        ).fetchone()
        labelers.append({
            "labeler_did": labeler_did,
            "handle": handle_row[0] if handle_row else None,
            "targets": targets,
        })

    # 7d vs 30d labeled target counts
    labeled_7d = conn.execute(f"""
        SELECT COUNT(DISTINCT le.target_did)
        FROM label_events le
        JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
        WHERE aif.pds_host IN ({placeholders})
          AND aif.resolver_status = 'ok'
          AND le.target_did IS NOT NULL
          AND le.ts >= datetime('now', ?)
    """, family_hosts + [cutoff_7d]).fetchone()[0]

    labeled_30d = conn.execute(f"""
        SELECT COUNT(DISTINCT le.target_did)
        FROM label_events le
        JOIN drift.actor_identity_facts aif ON aif.did = le.target_did
        WHERE aif.pds_host IN ({placeholders})
          AND aif.resolver_status = 'ok'
          AND le.target_did IS NOT NULL
          AND le.ts >= datetime('now', ?)
    """, family_hosts + [cutoff_30d]).fetchone()[0]

    # Concentration: does one labeler dominate?
    total_labeled_targets = sum(l["targets"] for l in labelers)
    top_labeler_share = (
        round(100.0 * labelers[0]["targets"] / total_labeled_targets, 1)
        if labelers and total_labeled_targets > 0
        else 0
    )

    return {
        "status": "ok",
        "days": days,
        "host_family": host_family,
        "pds_hosts": family_hosts,
        "overall_accounts": overall_count,
        "labeled_targets_7d": labeled_7d,
        "labeled_targets_30d": labeled_30d,
        "labelers": labelers[:15],
        "total_contributing_labelers": len(labelers),
        "top_labeler_share_pct": top_labeler_share,
        "concentrated": top_labeler_share > 70 and len(labelers) > 1,
    }


def attach_facts(conn: sqlite3.Connection, facts_path: str) -> bool:
    """Attach facts.sqlite as 'drift' database. Returns True on success."""
    if not facts_path:
        return False
    import os
    if not os.path.exists(facts_path):
        _log.warning("facts path not found: %s", facts_path)
        return False
    if "'" in facts_path or ";" in facts_path:
        _log.warning("facts path contains unsafe characters")
        return False

    import time
    for attempt in range(2):
        try:
            conn.execute(f"ATTACH DATABASE 'file:{facts_path}?mode=ro' AS drift")
            return True
        except sqlite3.OperationalError:
            if attempt == 0:
                time.sleep(1)
                continue
            _log.warning("failed to attach facts DB", exc_info=True)
            return False
    return False


def detach_facts(conn: sqlite3.Connection) -> None:
    """Detach the drift database if attached."""
    try:
        conn.execute("DETACH DATABASE drift")
    except sqlite3.OperationalError:
        pass
