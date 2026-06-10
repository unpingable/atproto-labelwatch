"""Tests for labelwatch.load_probe.v1 — real-subject load characterization."""

from __future__ import annotations

import json
import os

import pytest

from labelwatch import db, load_probe


def _admissible_receipt() -> dict:
    return {
        "receipt_kind": "labelwatch.index_audit.v1",
        "consumer_surface": "whatsonme.frontdoor.v0",
        "overall_verdict": "admissible",
        "generated_at": "2026-06-10T01:00:00Z",
        "_receipt_path": "<test>",
    }


def _seed_db(path: str, *, subjects: int = 5, events_per_subject: int = 10):
    """Seed a DB with N subjects, each touched by 1 labeler with M events."""
    conn = db.connect(path)
    db.init_db(conn)
    conn.execute(
        "INSERT INTO labelers (labeler_did, handle, regime_state, auditability) "
        "VALUES (?,?,?,?)",
        ("did:plc:probelabeler", "probe.test", "stable", "high"),
    )
    for i in range(subjects):
        subject_did = f"did:plc:probesubject{i:05d}xxxxxxxxxxxx"
        for j in range(events_per_subject):
            conn.execute(
                "INSERT INTO label_events (labeler_did, src, uri, val, neg, "
                "ts, event_hash, target_did) VALUES (?,?,?,?,?,?,?,?)",
                ("did:plc:probelabeler", "did:plc:probelabeler",
                 subject_did, "spam", 0,
                 f"2026-06-01T00:{j:02d}:00Z", f"h-{i}-{j}", subject_did),
            )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Percentile math
# ---------------------------------------------------------------------------

def test_percentile_empty():
    assert load_probe.percentile([], 50) is None


def test_percentile_singleton():
    assert load_probe.percentile([5.0], 50) == 5.0
    assert load_probe.percentile([5.0], 99) == 5.0


def test_percentile_known_distribution():
    # 0,1,2,...,99 — p50 should be 49.5, p99 should be 98.01 (linear interp).
    values = sorted(float(i) for i in range(100))
    assert abs(load_probe.percentile(values, 50) - 49.5) < 0.001
    assert abs(load_probe.percentile(values, 90) - 89.1) < 0.001
    assert abs(load_probe.percentile(values, 99) - 98.01) < 0.001


def test_percentile_bounds():
    values = sorted([1.0, 2.0, 3.0])
    assert load_probe.percentile(values, 0) == 1.0
    assert load_probe.percentile(values, 100) == 3.0
    assert load_probe.percentile(values, -5) == 1.0  # clamps low
    assert load_probe.percentile(values, 105) == 3.0  # clamps high


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def test_sample_top_labeled(tmp_path):
    """Sampling returns subjects ordered by event count desc."""
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=3, events_per_subject=5)
    conn = db.connect(p, readonly=True)
    try:
        sampled = load_probe.sample_top_labeled_subjects(conn, n=10)
    finally:
        conn.close()
    # All 3 subjects, each with 5 events.
    assert len(sampled) == 3
    for s in sampled:
        assert s["event_count"] == 5
        assert s["did"].startswith("did:plc:probesubject")


# ---------------------------------------------------------------------------
# Per-subject probe
# ---------------------------------------------------------------------------

def test_probe_subject_returns_metrics(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=1, events_per_subject=3)
    conn = db.connect(p, readonly=True)
    try:
        result = load_probe.probe_subject(
            conn,
            "did:plc:probesubject00000xxxxxxxxxxxx",
            sampled_event_count=3,
            audit_receipt=_admissible_receipt(),
        )
    finally:
        conn.close()
    assert result.refusal is None
    assert result.labelers == 1
    assert result.observed_event_count == 3
    assert result.wall_ms > 0
    assert result.wall_ms < 1000  # tiny DB, should be sub-second


def test_probe_subject_refuses_with_no_audit(tmp_path):
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=1, events_per_subject=3)
    conn = db.connect(p, readonly=True)
    try:
        result = load_probe.probe_subject(
            conn,
            "did:plc:probesubject00000xxxxxxxxxxxx",
            sampled_event_count=3,
            audit_receipt=None,
        )
    finally:
        conn.close()
    # Audit-gate refusal — should be captured, not crash.
    assert result.refusal == "index_audit_missing"


# ---------------------------------------------------------------------------
# Full receipt
# ---------------------------------------------------------------------------

def test_run_probe_receipt_shape(tmp_path, monkeypatch):
    """Receipt must have the documented schema."""
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=4, events_per_subject=2)

    # Place an admissible audit receipt in a temp directory so the probe
    # doesn't refuse.
    receipts_dir = tmp_path / "receipts"
    receipts_dir.mkdir()
    (receipts_dir / "labelwatch.index_audit.whatsonme.frontdoor.v0.20260601T000000Z.json").write_text(
        json.dumps({
            "receipt_kind": "labelwatch.index_audit.v1",
            "consumer_surface": "whatsonme.frontdoor.v0",
            "overall_verdict": "admissible",
            "generated_at": "2026-06-01T00:00:00Z",
        })
    )
    monkeypatch.setenv("LABELWATCH_AUDIT_RECEIPTS_DIR", str(receipts_dir))

    receipt = load_probe.run_probe(p, sample_size=10)

    required_keys = {
        "receipt_kind",
        "receipt_schema_version",
        "consumer_surface",
        "generated_at",
        "db_path",
        "sample_size_requested",
        "sample_size_observed",
        "sample_method",
        "sample_query_ms",
        "audit_verdict",
        "successful_probes",
        "percentiles",
        "subjects",
        "verdict",
        "rationale",
        "receipt_hash",
    }
    assert required_keys.issubset(set(receipt.keys()))
    assert receipt["receipt_kind"] == "labelwatch.load_probe.v1"
    assert receipt["sample_size_observed"] == 4  # only 4 subjects seeded
    assert receipt["successful_probes"] == 4
    # Tiny DB, sub-ms wall times → admissible_for_publication.
    assert receipt["verdict"] == "admissible_for_publication"

    # Percentile structure.
    for metric in ("wall_ms", "labelers_per_subj", "events_per_subj"):
        assert metric in receipt["percentiles"]
        assert "p50" in receipt["percentiles"][metric]
        assert "p99" in receipt["percentiles"][metric]


def test_render_text_runs(tmp_path, monkeypatch):
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=2, events_per_subject=1)
    receipts_dir = tmp_path / "receipts"
    receipts_dir.mkdir()
    (receipts_dir / "labelwatch.index_audit.whatsonme.frontdoor.v0.20260601T000000Z.json").write_text(
        json.dumps({
            "receipt_kind": "labelwatch.index_audit.v1",
            "consumer_surface": "whatsonme.frontdoor.v0",
            "overall_verdict": "admissible",
            "generated_at": "2026-06-01T00:00:00Z",
        })
    )
    monkeypatch.setenv("LABELWATCH_AUDIT_RECEIPTS_DIR", str(receipts_dir))
    receipt = load_probe.run_probe(p, sample_size=10)
    text = load_probe.render_text(receipt)
    assert "labelwatch.load_probe.v1" in text
    assert "p50" in text
    assert "verdict" in text
    assert receipt["verdict"] in text


def test_verdict_thresholds():
    """Synthesize probes at different wall_ms to exercise the verdict rule."""
    Probe = load_probe.SubjectProbe

    # Fast probes — admissible_for_publication
    fast = [Probe("d1", 100, 100, 1, 1, wall_ms=5.0, refusal=None)] * 10
    v, _ = load_probe._verdict(fast)
    assert v == "admissible_for_publication"

    # Medium probes — admissible_with_debt
    medium = [Probe("d1", 100, 100, 1, 1, wall_ms=1500.0, refusal=None)] * 10
    v, _ = load_probe._verdict(medium)
    assert v == "admissible_with_debt"

    # Slow probes — refused_unbounded
    slow = [Probe("d1", 100, 100, 1, 1, wall_ms=10000.0, refusal=None)] * 10
    v, _ = load_probe._verdict(slow)
    assert v == "refused_unbounded"

    # All refused — refused_no_data
    refused = [Probe("d1", 0, 0, 0, 0, wall_ms=1.0, refusal="no_observed_labels")] * 5
    v, _ = load_probe._verdict(refused)
    assert v == "refused_no_data"


# ---------------------------------------------------------------------------
# No mutation
# ---------------------------------------------------------------------------

import hashlib


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def test_load_probe_no_mutation(tmp_path, monkeypatch):
    """SHA-256 of the DB file must be unchanged after a probe run."""
    p = str(tmp_path / "lw.db")
    _seed_db(p, subjects=3, events_per_subject=2)
    # Truncate WAL to a clean state, then snapshot.
    conn = db.connect(p)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    for sidecar in (f"{p}-wal", f"{p}-shm"):
        if os.path.exists(sidecar):
            os.remove(sidecar)

    receipts_dir = tmp_path / "receipts"
    receipts_dir.mkdir()
    (receipts_dir / "labelwatch.index_audit.whatsonme.frontdoor.v0.20260601T000000Z.json").write_text(
        json.dumps({
            "receipt_kind": "labelwatch.index_audit.v1",
            "consumer_surface": "whatsonme.frontdoor.v0",
            "overall_verdict": "admissible",
            "generated_at": "2026-06-01T00:00:00Z",
        })
    )
    monkeypatch.setenv("LABELWATCH_AUDIT_RECEIPTS_DIR", str(receipts_dir))

    before = _sha256_file(p)
    load_probe.run_probe(p, sample_size=10)
    after = _sha256_file(p)
    assert before == after, "load probe must not modify the DB file"
