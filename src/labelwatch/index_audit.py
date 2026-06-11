"""labelwatch.index_audit.v1 — admissibility audit for consumer-surface queries.

Audits the hot read paths a public consumer surface depends on, BEFORE the
surface ships. Emits a receipt (`labelwatch.index_audit.v1`) describing per-query
EXPLAIN QUERY PLAN output, observed runtime, index coverage, and a per-query
verdict drawn from:

    admissible
    admissible_with_debt
    admissible_external_resolution   # PLC/DID-web lookup; not a SQL query
    admissible_internal_classifier   # pure-function map; no DB scan
    folded_into_parent               # subsumed by another query's work
    refused_index_missing
    refused_query_shape_unbounded
    refused_cardinality_unknown
    refused_explain_error
    refused_execution_error

The audit MUTATES NOTHING. It opens the DB read-only, runs EXPLAIN QUERY PLAN
and one (empty-result) probe execution per SQL query, and emits the receipt.
It does NOT create indexes, ANALYZE, VACUUM, init schema, or otherwise touch
the DB.

Slice boundary (chatty 2026-06-09): measurement and refusal only. Index
remediation, if needed, is a separate slice gated on this audit's verdicts.
"""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from . import db as db_mod
from .utils import format_ts, hash_sha256, now_utc, stable_json, get_git_commit


RECEIPT_KIND = "labelwatch.index_audit.v1"
RECEIPT_SCHEMA_VERSION = 1

DEFAULT_CONSUMER_SURFACE = "whatsonme.frontdoor.v0"

# Dummy DID used to drive EXPLAIN QUERY PLAN + a no-match probe execution.
# The all-zeros suffix is intentionally not a valid PLC; matching probability
# is effectively zero, so the probe exercises the planner without scanning rows.
PROBE_SUBJECT_DID = "did:plc:" + "0" * 24

# Tables we consider "large fact tables" — unbounded SCAN against any of these
# during a publication-blocking query is a refusal.
LARGE_FACT_TABLES = ("label_events",)

# Runtime threshold for downgrading admissible -> admissible_with_debt. The
# probe DID returns zero rows, so any sane indexed path is sub-ms; this
# threshold catches "the planner technically used an index but the path is
# still slow" cases without overfitting.
DEBT_RUNTIME_MS = 100.0


# ---------------------------------------------------------------------------
# Query inventory — matches Q1..Q8 in
# labelwatch/docs/analysis/subject-lookup-frontdoor-001.md
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Query:
    query_id: str
    query_name: str
    purpose: str
    sql: str | None  # None for non-SQL queries (external resolution, pure classifier, folded)
    kind: str  # 'sqlite' | 'external_resolution' | 'internal_classifier' | 'folded'
    publication_blocking: bool
    bounded_by_subject_or_time: bool
    # Additional bind params appended after probe_subject_did. Used by queries
    # that take more than the subject parameter (e.g. Q8c's top-N row cap).
    extra_params: tuple = ()


QUERIES: tuple[Query, ...] = (
    Query(
        query_id="Q1",
        query_name="subject_identity_resolution",
        purpose="Resolve handle/DID -> canonical DID (PLC/DID-web).",
        sql=None,
        kind="external_resolution",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q2",
        query_name="labelers_touching_subject",
        purpose="Per-labeler event count against subject.",
        sql=(
            "SELECT labeler_did, COUNT(*) AS event_count "
            "FROM label_events "
            "WHERE target_did = ? "
            "GROUP BY labeler_did"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q3",
        query_name="label_values_touching_subject",
        purpose="Per (labeler, val) rollup against subject (count + first_seen + last_seen).",
        sql=(
            "SELECT labeler_did, val, "
            "COUNT(*) AS event_count, "
            "MIN(ts) AS first_seen, "
            "MAX(ts) AS last_seen "
            "FROM label_events "
            "WHERE target_did = ? "
            "GROUP BY labeler_did, val"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q4",
        query_name="latest_seen_per_labeler_value",
        purpose="MAX(ts) per (labeler_did, val) — folded into Q3 (MAX in same GROUP BY).",
        sql=None,
        kind="folded",
        publication_blocking=False,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q5",
        query_name="authority_effect_breakdown",
        purpose="Pure-function classify_authority_effect over (labeler_did, val) tuples from Q3.",
        sql=None,
        kind="internal_classifier",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q6",
        query_name="emitter_stability_summary",
        purpose="Per labeler regime_state + auditability lookup (Q6+Q7).",
        sql=(
            "SELECT labeler_did, regime_state, auditability, "
            "last_seen, events_7d, events_30d "
            "FROM labelers "
            "WHERE labeler_did = ?"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q7",
        query_name="auditability_summary",
        purpose="Auditability dial per labeler — folded into Q6 (same row).",
        sql=None,
        kind="folded",
        publication_blocking=False,
        bounded_by_subject_or_time=True,
    ),
    # Q8 (per-row fetch + Python aggregation) retired 2026-06-11; replaced by
    # Q8a/Q8b/Q8c per gap-spec subject-lookup-sql-aggregation-001. Each new
    # query is bounded by labeler count or by per-labeler top-N; together they
    # remove the O(events) Python walk that forced the subject_too_dense gate.
    Query(
        query_id="Q8a",
        query_name="classification_changed_per_labeler",
        purpose="COUNT(DISTINCT val||'|'||neg) per labeler — drives classification_changed flag without per-row fetch.",
        sql=(
            "SELECT labeler_did, COUNT(DISTINCT val || '|' || neg) AS distinct_states "
            "FROM label_events "
            "WHERE target_did = ? "
            "GROUP BY labeler_did"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q8b",
        query_name="locus_bucket_per_labeler",
        purpose="Per (labeler, locus) event count via CASE-bucketed GROUP BY. Drives locus_counts dict directly.",
        sql=(
            "SELECT labeler_did, "
            "CASE "
            "WHEN uri LIKE 'did:%' THEN 'account' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.post/%' THEN 'post' "
            "WHEN uri LIKE 'at://%/app.bsky.actor.profile/%' THEN 'profile' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.list/%' THEN 'list' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.listitem/%' THEN 'list_item' "
            "WHEN uri LIKE 'at://%/app.bsky.feed.generator/%' THEN 'feed_generator' "
            "WHEN uri LIKE 'at://%/app.bsky.graph.starterpack/%' THEN 'starterpack' "
            "WHEN uri LIKE 'at://%' THEN 'record' "
            "ELSE 'unknown' "
            "END AS locus, "
            "COUNT(*) AS event_count "
            "FROM label_events "
            "WHERE target_did = ? "
            "GROUP BY labeler_did, locus"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
    ),
    Query(
        query_id="Q8c",
        query_name="top_n_labeled_records_per_labeler",
        purpose="Per (labeler, uri, val) rollup over non-account events, capped via window function to top-N URIs per labeler.",
        sql=(
            "WITH per_uri_val AS ( "
            "  SELECT labeler_did, uri, val, "
            "         COUNT(*) AS val_count, "
            "         MIN(ts) AS first_seen, "
            "         MAX(ts) AS last_seen "
            "  FROM label_events "
            "  WHERE target_did = ? AND uri NOT LIKE 'did:%' "
            "  GROUP BY labeler_did, uri, val "
            "), "
            "uri_totals AS ( "
            "  SELECT labeler_did, uri, SUM(val_count) AS uri_total "
            "  FROM per_uri_val "
            "  GROUP BY labeler_did, uri "
            "), "
            "ranked AS ( "
            "  SELECT labeler_did, uri, uri_total, "
            "         ROW_NUMBER() OVER ("
            "           PARTITION BY labeler_did ORDER BY uri_total DESC, uri"
            "         ) AS rn "
            "  FROM uri_totals "
            ") "
            "SELECT puv.labeler_did, puv.uri, puv.val, puv.val_count, "
            "       puv.first_seen, puv.last_seen, r.uri_total "
            "FROM per_uri_val puv "
            "JOIN ranked r ON r.labeler_did = puv.labeler_did AND r.uri = puv.uri "
            "WHERE r.rn <= ? "
            "ORDER BY puv.labeler_did, r.rn, puv.val"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=True,
        # Top-N row cap; matches frontdoor.MAX_LABELED_RECORDS_PER_LABELER.
        extra_params=(50,),
    ),
)


# ---------------------------------------------------------------------------
# EXPLAIN QUERY PLAN parsing
# ---------------------------------------------------------------------------

def _format_plan_row(row: Any) -> str:
    """Extract the `detail` column from an EXPLAIN QUERY PLAN row."""
    if isinstance(row, sqlite3.Row):
        # SQLite returns (id, parent, notused, detail)
        try:
            return str(row["detail"])
        except (IndexError, KeyError):
            pass
    if isinstance(row, (list, tuple)) and len(row) >= 4:
        return str(row[3])
    return str(row)


def _scan_against_large_fact_table(plan_rows: list[str]) -> bool:
    """Detect an unbounded SCAN over a large fact table.

    SQLite's EXPLAIN QUERY PLAN distinguishes two shapes:

        SEARCH <table> USING INDEX <idx> (col=?)   -- bounded key lookup
        SCAN   <table> [USING [COVERING] INDEX ..] -- full walk

    A SCAN walks every row (or every index entry, which is still O(n)), even
    if SQLite picked a covering index to avoid heap reads. For a marginal-
    storage table like label_events, "covering scan" is just a faster way of
    being unbounded; the audit must refuse it on shape, not bless it because
    the planner found a smaller index to walk.
    """
    for line in plan_rows:
        # We only care about explicit SCAN of a large fact table. SEARCH of
        # the same table via an index is the bounded form and is admissible.
        if "SCAN" not in line:
            continue
        for tbl in LARGE_FACT_TABLES:
            if tbl in line:
                return True
    return False


def _index_used(plan_rows: list[str]) -> str | None:
    """Return the index name SQLite picked, if any. Handles both SEARCH ... and SCAN ... using an index."""
    for line in plan_rows:
        for marker in ("USING COVERING INDEX", "USING INDEX"):
            idx = line.find(marker)
            if idx == -1:
                continue
            tail = line[idx + len(marker):].strip()
            # tail is like "idx_name (target_did=?)" or "idx_name"
            name = tail.split()[0] if tail else ""
            name = name.rstrip("(")
            return name or None
    return None


# ---------------------------------------------------------------------------
# Per-query audit
# ---------------------------------------------------------------------------

def _verdict_from_plan(
    query: Query,
    plan_rows: list[str],
    runtime_ms: float | None,
) -> tuple[str, dict]:
    """Apply the detection rule to one query's plan + runtime."""
    full_scan = _scan_against_large_fact_table(plan_rows)
    idx = _index_used(plan_rows)
    details = {
        "covering_index_present": idx is not None,
        "full_scan_detected": full_scan,
        "index_used": idx,
    }

    if full_scan and query.publication_blocking:
        return ("refused_query_shape_unbounded", details)
    if full_scan and not query.publication_blocking:
        return ("admissible_with_debt", details)

    # No full scan against large tables. If no index was used at all and the
    # query touches a large fact table, that's missing-index territory.
    touches_large = any(t in (query.sql or "") for t in LARGE_FACT_TABLES)
    if touches_large and not idx and query.publication_blocking:
        return ("refused_index_missing", details)

    # Runtime guard: an indexed plan that still takes too long is debt, not refusal.
    if runtime_ms is not None and runtime_ms > DEBT_RUNTIME_MS:
        return ("admissible_with_debt", details)

    return ("admissible", details)


def audit_query(
    conn: sqlite3.Connection,
    query: Query,
    probe_subject_did: str,
) -> dict:
    """Audit one query. Returns a per-query result dict for the receipt."""
    base = {
        "query_id": query.query_id,
        "query_name": query.query_name,
        "purpose": query.purpose,
        "kind": query.kind,
        "publication_blocking": query.publication_blocking,
        "bounded_by_subject_or_time": query.bounded_by_subject_or_time,
    }

    if query.kind == "external_resolution":
        return {
            **base,
            "sql_fingerprint": None,
            "sql": None,
            "observed_runtime_ms": None,
            "explain_query_plan": [],
            "covering_index_present": False,
            "full_scan_detected": False,
            "index_used": None,
            "verdict": "admissible_external_resolution",
            "note": "PLC/DID-web lookup; not a label_events query. Audit confirms by inspection that the resolution path does not fall back to a label_events scan on cache miss.",
        }
    if query.kind == "internal_classifier":
        return {
            **base,
            "sql_fingerprint": None,
            "sql": None,
            "observed_runtime_ms": None,
            "explain_query_plan": [],
            "covering_index_present": False,
            "full_scan_detected": False,
            "index_used": None,
            "verdict": "admissible_internal_classifier",
            "note": "Pure-function map (classify_authority_effect) over Q3 result set. No DB scan.",
        }
    if query.kind == "folded":
        return {
            **base,
            "sql_fingerprint": None,
            "sql": None,
            "observed_runtime_ms": None,
            "explain_query_plan": [],
            "covering_index_present": False,
            "full_scan_detected": False,
            "index_used": None,
            "verdict": "folded_into_parent",
            "note": "Subsumed by another query in this inventory.",
        }

    assert query.kind == "sqlite" and query.sql is not None
    sql = query.sql
    fingerprint = hash_sha256(sql)[:16]

    params = (probe_subject_did,) + tuple(query.extra_params)

    explain_sql = f"EXPLAIN QUERY PLAN {sql}"
    try:
        raw_plan = conn.execute(explain_sql, params).fetchall()
    except sqlite3.Error as exc:
        return {
            **base,
            "sql_fingerprint": fingerprint,
            "sql": sql,
            "observed_runtime_ms": None,
            "explain_query_plan": [],
            "covering_index_present": False,
            "full_scan_detected": False,
            "index_used": None,
            "verdict": "refused_explain_error",
            "error": str(exc),
        }
    plan_rows = [_format_plan_row(r) for r in raw_plan]

    runtime_ms: float | None = None
    try:
        t0 = time.perf_counter()
        # fetchall to fully execute the plan; probe DID returns zero rows
        conn.execute(sql, params).fetchall()
        runtime_ms = (time.perf_counter() - t0) * 1000.0
    except sqlite3.Error as exc:
        return {
            **base,
            "sql_fingerprint": fingerprint,
            "sql": sql,
            "observed_runtime_ms": None,
            "explain_query_plan": plan_rows,
            "covering_index_present": False,
            "full_scan_detected": False,
            "index_used": None,
            "verdict": "refused_execution_error",
            "error": str(exc),
        }

    verdict, details = _verdict_from_plan(query, plan_rows, runtime_ms)
    return {
        **base,
        "sql_fingerprint": fingerprint,
        "sql": sql,
        "observed_runtime_ms": round(runtime_ms, 3),
        "explain_query_plan": plan_rows,
        "covering_index_present": details["covering_index_present"],
        "full_scan_detected": details["full_scan_detected"],
        "index_used": details["index_used"],
        "verdict": verdict,
    }


# ---------------------------------------------------------------------------
# Overall verdict + receipt assembly
# ---------------------------------------------------------------------------

_OVERALL_PRIORITY = (
    "refused_index_missing",
    "refused_query_shape_unbounded",
    "refused_cardinality_unknown",
    "refused_execution_error",
    "refused_explain_error",
)


def _overall_verdict(query_results: list[dict]) -> str:
    blocking = [r for r in query_results if r["publication_blocking"]]
    for v in _OVERALL_PRIORITY:
        if any(r["verdict"] == v for r in blocking):
            return v
    if any(r["verdict"] == "admissible_with_debt" for r in blocking):
        return "admissible_with_debt"
    return "admissible"


def _table_counts(conn: sqlite3.Connection, tables: tuple[str, ...]) -> dict:
    out: dict[str, int | None] = {}
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()
            out[t] = row["c"] if row else None
        except sqlite3.Error:
            out[t] = None
    return out


def _file_size_bytes(path: str) -> int | None:
    try:
        return os.path.getsize(path)
    except OSError:
        return None


def run_audit(
    db_path: str,
    consumer_surface: str = DEFAULT_CONSUMER_SURFACE,
    probe_subject_did: str = PROBE_SUBJECT_DID,
) -> dict:
    """Run the audit against `db_path`. Returns the receipt dict.

    Opens the DB read-only. Does NOT mutate. Does NOT init schema.
    """
    conn = db_mod.connect(db_path, readonly=True)
    try:
        table_counts = _table_counts(conn, ("label_events", "labelers", "alerts"))
        query_results = [audit_query(conn, q, probe_subject_did) for q in QUERIES]
        overall = _overall_verdict(query_results)

        receipt = {
            "receipt_kind": RECEIPT_KIND,
            "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
            "consumer_surface": consumer_surface,
            "generated_at": format_ts(now_utc()),
            "git_commit": get_git_commit(),
            "db_path": db_path,
            "db_size_bytes": _file_size_bytes(db_path),
            "table_counts": table_counts,
            "probe_subject_did": probe_subject_did,
            "query_results": query_results,
            "overall_verdict": overall,
        }
        # receipt_hash is computed over the canonical receipt minus itself
        receipt_for_hash = {k: v for k, v in receipt.items() if k != "receipt_hash"}
        receipt["receipt_hash"] = hash_sha256(stable_json(receipt_for_hash))
        return receipt
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Human-readable rendering
# ---------------------------------------------------------------------------

def render_text(receipt: dict) -> str:
    lines: list[str] = []
    lines.append(f"=== {receipt['receipt_kind']} ===")
    lines.append(f"consumer_surface : {receipt['consumer_surface']}")
    lines.append(f"generated_at     : {receipt['generated_at']}")
    git = receipt.get("git_commit")
    lines.append(f"git_commit       : {git[:12] if git else 'unknown'}")
    lines.append(f"db_path          : {receipt['db_path']}")
    size = receipt.get("db_size_bytes")
    lines.append(f"db_size_bytes    : {size:,}" if isinstance(size, int) else "db_size_bytes    : unknown")
    counts = receipt.get("table_counts", {}) or {}
    counts_str = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}=?" for k, v in counts.items())
    lines.append(f"table_counts     : {counts_str}")
    lines.append(f"overall_verdict  : {receipt['overall_verdict']}")
    lines.append("")
    lines.append(f"{'ID':<4} {'verdict':<34} {'runtime_ms':>10}  query_name")
    lines.append(f"{'-'*4} {'-'*34} {'-'*10}  {'-'*40}")
    for r in receipt["query_results"]:
        rt = r.get("observed_runtime_ms")
        rt_s = f"{rt:>10.3f}" if isinstance(rt, (int, float)) else f"{'-':>10}"
        lines.append(f"{r['query_id']:<4} {r['verdict']:<34} {rt_s}  {r['query_name']}")
        for line in (r.get("explain_query_plan") or []):
            lines.append(f"      | {line}")
        if r.get("full_scan_detected"):
            lines.append(f"      ! full scan against large fact table")
        elif r.get("index_used"):
            lines.append(f"      . index: {r['index_used']}")
        if r.get("note"):
            lines.append(f"      ~ {r['note']}")
        if r.get("error"):
            lines.append(f"      X error: {r['error']}")
    return "\n".join(lines)
