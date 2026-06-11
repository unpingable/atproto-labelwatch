"""Acceptance tests for labelwatch.index_audit.v1.

Per chatty's spec (2026-06-09), six acceptance tests:
1. known indexed fixture passes
2. missing subject index refuses
3. unbounded scan refuses
4. JSON schema stable
5. text output is operator-readable
6. no mutation
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import sys
import tempfile

import pytest

from labelwatch import db, index_audit
from labelwatch.index_audit import (
    DEFAULT_CONSUMER_SURFACE,
    PROBE_SUBJECT_DID,
    QUERIES,
    RECEIPT_KIND,
    Query,
    audit_query,
    run_audit,
    render_text,
)


def _fresh_db_path(tmp_path) -> str:
    """Create a fresh DB at full schema (with indexes) and return its path."""
    p = str(tmp_path / "labelwatch.db")
    conn = db.connect(p)
    db.init_db(conn)
    conn.close()
    return p


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Test 1 — known indexed fixture passes
# ---------------------------------------------------------------------------

def test_admissible_against_indexed_schema(tmp_path):
    """Fresh DB at full schema should produce admissible / admissible_with_debt
    for every publication-blocking SQLite query."""
    p = _fresh_db_path(tmp_path)
    receipt = run_audit(p)

    assert receipt["receipt_kind"] == RECEIPT_KIND
    assert receipt["consumer_surface"] == DEFAULT_CONSUMER_SURFACE
    assert receipt["overall_verdict"] in {"admissible", "admissible_with_debt"}

    by_id = {r["query_id"]: r for r in receipt["query_results"]}
    # Q1/Q5 are non-DB
    assert by_id["Q1"]["verdict"] == "admissible_external_resolution"
    assert by_id["Q5"]["verdict"] == "admissible_internal_classifier"
    # Q4/Q7 are folded into their parents
    assert by_id["Q4"]["verdict"] == "folded_into_parent"
    assert by_id["Q7"]["verdict"] == "folded_into_parent"

    # SQL queries: admissible (or admissible_with_debt under load — empty DB
    # here, so should be admissible). Q8 was retired 2026-06-11; Q8a/Q8b/Q8c
    # replaced it via gap-spec subject-lookup-sql-aggregation-001.
    for qid in ("Q2", "Q3", "Q6", "Q8a", "Q8b", "Q8c"):
        r = by_id[qid]
        assert r["verdict"] in {"admissible", "admissible_with_debt"}, (
            f"{qid} returned {r['verdict']}; plan: {r.get('explain_query_plan')}"
        )
        # SQLite picked an index for label_events queries
        if "label_events" in (r.get("sql") or ""):
            assert r["index_used"] is not None, f"{qid}: no index used; plan {r['explain_query_plan']}"
            assert r["full_scan_detected"] is False


# ---------------------------------------------------------------------------
# Test 2 — missing subject index refuses
# ---------------------------------------------------------------------------

def test_refuses_when_subject_index_missing(tmp_path):
    """Drop the (target_did, ts) index → Q2/Q3/Q8 must refuse."""
    p = _fresh_db_path(tmp_path)

    # Drop the index that backs target_did lookups.
    conn = db.connect(p)
    conn.execute("DROP INDEX IF EXISTS idx_label_events_target_did_ts")
    conn.commit()
    conn.close()

    receipt = run_audit(p)
    by_id = {r["query_id"]: r for r in receipt["query_results"]}

    # Q2/Q3/Q8a/Q8b/Q8c are publication_blocking + touch label_events filtered by target_did
    for qid in ("Q2", "Q3", "Q8a", "Q8b", "Q8c"):
        r = by_id[qid]
        assert r["verdict"].startswith("refused_"), (
            f"{qid} should refuse after dropping target_did index; got {r['verdict']}; "
            f"plan: {r['explain_query_plan']}"
        )

    # Overall verdict must be one of the refused_* codes.
    assert receipt["overall_verdict"].startswith("refused_")


# ---------------------------------------------------------------------------
# Test 3 — unbounded scan refuses
# ---------------------------------------------------------------------------

def test_unbounded_scan_refuses(tmp_path):
    """Synthetic query without a target_did/ts bound → refused_query_shape_unbounded."""
    p = _fresh_db_path(tmp_path)

    # Note: the Q1 SQL contract uses a single ? placeholder. We mirror that
    # so audit_query()'s probe_subject_did binding works, but the WHERE clause
    # ignores it — guaranteeing an unbounded scan over the whole table.
    unbounded = Query(
        query_id="ZZ",
        query_name="synthetic_unbounded_for_test",
        purpose="Test fixture: query without a subject/time bound.",
        sql=(
            "SELECT labeler_did, COUNT(*) "
            "FROM label_events "
            "WHERE ? IS NOT NULL "
            "GROUP BY labeler_did"
        ),
        kind="sqlite",
        publication_blocking=True,
        bounded_by_subject_or_time=False,
    )

    conn = db.connect(p, readonly=True)
    try:
        result = audit_query(conn, unbounded, PROBE_SUBJECT_DID)
    finally:
        conn.close()

    assert result["verdict"] == "refused_query_shape_unbounded", (
        f"got {result['verdict']}; plan: {result['explain_query_plan']}"
    )
    assert result["full_scan_detected"] is True


# ---------------------------------------------------------------------------
# Test 4 — JSON schema stable
# ---------------------------------------------------------------------------

def test_json_schema_stable(tmp_path):
    """Receipt JSON must carry receipt_kind, consumer_surface, query_results,
    overall_verdict, receipt_hash, table_counts, db_path."""
    p = _fresh_db_path(tmp_path)
    receipt = run_audit(p)

    # Top-level keys.
    required_top = {
        "receipt_kind",
        "receipt_schema_version",
        "consumer_surface",
        "generated_at",
        "db_path",
        "db_size_bytes",
        "table_counts",
        "probe_subject_did",
        "query_results",
        "overall_verdict",
        "receipt_hash",
    }
    assert required_top.issubset(set(receipt.keys())), (
        f"missing keys: {required_top - set(receipt.keys())}"
    )
    assert receipt["receipt_kind"] == RECEIPT_KIND
    assert receipt["receipt_schema_version"] == 1

    # Per-query fields.
    required_per_query = {
        "query_id",
        "query_name",
        "purpose",
        "kind",
        "publication_blocking",
        "bounded_by_subject_or_time",
        "sql_fingerprint",
        "sql",
        "observed_runtime_ms",
        "explain_query_plan",
        "covering_index_present",
        "full_scan_detected",
        "index_used",
        "verdict",
    }
    for r in receipt["query_results"]:
        assert required_per_query.issubset(set(r.keys())), (
            f"{r.get('query_id')}: missing per-query keys: "
            f"{required_per_query - set(r.keys())}"
        )

    # JSON round-trip is byte-stable (no non-serializable values).
    rendered = json.dumps(receipt, indent=2)
    parsed = json.loads(rendered)
    assert parsed["receipt_hash"] == receipt["receipt_hash"]

    # The contract queries plus no extras. Q8 retired 2026-06-11 in favor of
    # Q8a/Q8b/Q8c per gap-spec subject-lookup-sql-aggregation-001.
    assert {r["query_id"] for r in receipt["query_results"]} == {
        "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8a", "Q8b", "Q8c"
    }


# ---------------------------------------------------------------------------
# Test 5 — text output is operator-readable
# ---------------------------------------------------------------------------

def test_text_output_is_operator_readable(tmp_path):
    """Text rendering must show every Q with verdict + runtime, and surface
    the EXPLAIN QUERY PLAN detail line under each SQLite query."""
    p = _fresh_db_path(tmp_path)
    receipt = run_audit(p)
    text = render_text(receipt)

    assert RECEIPT_KIND in text
    assert receipt["consumer_surface"] in text
    assert "overall_verdict" in text

    # Every query ID is mentioned.
    for qid in ("Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8a", "Q8b", "Q8c"):
        assert qid in text, f"missing {qid} from text output"

    # At least one EXPLAIN QUERY PLAN detail line surfaces for SQLite queries.
    # SQLite plans start with SCAN or SEARCH.
    assert ("SEARCH" in text) or ("SCAN" in text), (
        "expected at least one EXPLAIN QUERY PLAN line (SEARCH/SCAN) in text output"
    )


# ---------------------------------------------------------------------------
# Test 6 — no mutation
# ---------------------------------------------------------------------------

def test_no_mutation(tmp_path):
    """SHA-256 of the DB file must be byte-identical before and after the audit."""
    p = _fresh_db_path(tmp_path)

    # Force WAL into the main DB so the on-disk image is the full DB.
    conn = db.connect(p)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()

    # Drop any sidecar WAL/SHM files so the byte-identity check is clean.
    for sidecar in (f"{p}-wal", f"{p}-shm"):
        if os.path.exists(sidecar):
            os.remove(sidecar)

    before = _sha256_file(p)
    receipt = run_audit(p)
    after = _sha256_file(p)

    assert before == after, "audit must not modify the DB file"
    assert receipt["overall_verdict"] in {"admissible", "admissible_with_debt"}

    # Sidecar files: -wal / -shm may be created by SQLite on read, but the
    # main file image (which carries all the data) must not change. We've
    # already asserted that. Just sanity-check no INDEX/TABLE was created.
    conn = db.connect(p, readonly=True)
    indexes_after = [
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        )
    ]
    tables_after = [
        r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
    ]
    conn.close()
    # No table/index named anything audit-y should have appeared.
    assert not any("audit" in n.lower() for n in indexes_after + tables_after)


# ---------------------------------------------------------------------------
# Bonus: every query in the inventory is reachable end-to-end
# ---------------------------------------------------------------------------

def test_every_inventory_query_has_a_verdict(tmp_path):
    """Defensive: if someone adds a new Query but forgets to wire a kind,
    audit_query() must still produce a verdict (not crash, not silently skip)."""
    p = _fresh_db_path(tmp_path)
    receipt = run_audit(p)
    assert len(receipt["query_results"]) == len(QUERIES)
    for r in receipt["query_results"]:
        assert r["verdict"], f"{r['query_id']}: empty verdict"
