"""Tests for boundary instability primitives (Phase 1)."""
from datetime import datetime, timedelta, timezone

from labelwatch import db
from labelwatch.boundary import (
    _ordered_pair,
    build_distributions,
    compute_contradiction_edges,
    compute_divergence_summaries,
    compute_lead_lag_edges,
    find_shared_targets,
    jsd,
    run_boundary_pass,
)
from labelwatch.config import Config
from labelwatch.label_family import FAMILY_VERSION, normalize_family
from labelwatch.utils import format_ts


def _make_db():
    """Create an in-memory DB with schema."""
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _insert_event(conn, labeler_did, uri, val, ts, neg=0):
    """Insert a label event with auto-generated hash."""
    from labelwatch.utils import hash_sha256
    event_hash = hash_sha256(f"{labeler_did}:{uri}:{val}:{ts}:{neg}")
    target_did = None
    if uri.startswith("at://"):
        parts = uri[5:].split("/", 1)
        if parts[0].startswith("did:"):
            target_did = parts[0]
    conn.execute(
        """INSERT OR IGNORE INTO label_events
           (labeler_did, uri, val, neg, ts, event_hash, target_did)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (labeler_did, uri, val, neg, ts, event_hash, target_did),
    )


def _insert_labeler(conn, did, ts):
    conn.execute(
        "INSERT OR IGNORE INTO labelers(labeler_did, first_seen, last_seen) VALUES(?, ?, ?)",
        (did, ts, ts),
    )


# ── Label family tests ───────────────────────────────────────────────


def test_normalize_family_basic():
    assert normalize_family("Porn") == "adult-sexual"  # mapped synonym
    assert normalize_family("  SPAM  ") == "spam"
    assert normalize_family("Adult Content") == "adult-sexual"  # mapped synonym
    assert normalize_family(None) == "<null>"
    assert normalize_family("") == "<null>"


def test_normalize_family_passthrough():
    """Unmapped values pass through as their canonical form."""
    assert normalize_family("custom_label") == "custom_label"
    assert normalize_family("My Special Tag") == "my_special_tag"


def test_family_version():
    assert FAMILY_VERSION == "v1"


# ── JSD math tests ───────────────────────────────────────────────────


def test_jsd_identical():
    """Identical distributions should have JSD = 0."""
    p = {"a": 0.5, "b": 0.5}
    assert jsd(p, p) == 0.0


def test_jsd_disjoint():
    """Completely disjoint distributions should have JSD = 1."""
    p = {"a": 1.0}
    q = {"b": 1.0}
    assert abs(jsd(p, q) - 1.0) < 1e-10


def test_jsd_symmetric():
    """JSD should be symmetric."""
    p = {"a": 0.7, "b": 0.3}
    q = {"a": 0.3, "b": 0.7}
    assert abs(jsd(p, q) - jsd(q, p)) < 1e-10


def test_jsd_bounded():
    """JSD should always be between 0 and 1."""
    p = {"a": 0.6, "b": 0.2, "c": 0.2}
    q = {"a": 0.1, "b": 0.8, "c": 0.1}
    result = jsd(p, q)
    assert 0.0 <= result <= 1.0


def test_jsd_known_value():
    """JSD between uniform and one-hot should be ~0.5 (log2 based)."""
    p = {"a": 0.5, "b": 0.5}
    q = {"a": 1.0}
    # M = (P+Q)/2 = {a: 0.75, b: 0.25}
    # KL(P||M) = 0.5*log2(0.5/0.75) + 0.5*log2(0.5/0.25) = 0.5*(-0.585) + 0.5*(1.0) = 0.2075
    # KL(Q||M) = 1.0*log2(1.0/0.75) = 0.415
    # JSD = (0.2075 + 0.415) / 2 = 0.311
    result = jsd(p, q)
    assert 0.3 < result < 0.35


# ── Pair ordering ────────────────────────────────────────────────────


def test_ordered_pair():
    assert _ordered_pair("did:plc:b", "did:plc:a") == ("did:plc:a", "did:plc:b")
    assert _ordered_pair("did:plc:a", "did:plc:b") == ("did:plc:a", "did:plc:b")


# ── Schema v18 ───────────────────────────────────────────────────────


def test_schema_v18():
    """Schema v18 creates boundary_edges and boundary_targets tables."""
    conn = _make_db()
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert "boundary_edges" in tables
    assert "boundary_targets" in tables


# ── Shared target finding ────────────────────────────────────────────


def test_find_shared_targets():
    """Finds targets labeled by multiple labelers."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    uri = "at://did:plc:target1/app.bsky.feed.post/abc"

    for did in ["did:plc:a", "did:plc:b", "did:plc:c"]:
        _insert_labeler(conn, did, ts)
        _insert_event(conn, did, uri, "spam", ts)
    conn.commit()

    w_start = format_ts(now - timedelta(hours=24))
    w_end = format_ts(now + timedelta(hours=1))
    shared = find_shared_targets(conn, w_start, w_end, min_labelers=2)
    assert len(shared) == 1
    assert shared[0]["n_labelers"] == 3
    assert shared[0]["uri"] == uri


def test_find_shared_targets_excludes_single_labeler():
    """Targets with only one labeler should not appear."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    uri = "at://did:plc:target1/app.bsky.feed.post/abc"

    _insert_labeler(conn, "did:plc:a", ts)
    _insert_event(conn, "did:plc:a", uri, "spam", ts)
    conn.commit()

    w_start = format_ts(now - timedelta(hours=24))
    w_end = format_ts(now + timedelta(hours=1))
    shared = find_shared_targets(conn, w_start, w_end, min_labelers=2)
    assert len(shared) == 0


# ── Distribution building ────────────────────────────────────────────


def test_build_distributions():
    """Distributions are built per (uri, labeler) with normalized families."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)
    uri = "at://did:plc:target1/app.bsky.feed.post/abc"

    _insert_labeler(conn, "did:plc:a", ts)
    _insert_labeler(conn, "did:plc:b", ts)

    # Labeler A: 3 spam, 1 adult_content
    for i in range(3):
        _insert_event(conn, "did:plc:a", uri, "Spam",
                      format_ts(now - timedelta(minutes=i)))
    _insert_event(conn, "did:plc:a", uri, "Adult Content",
                  format_ts(now - timedelta(minutes=5)))

    # Labeler B: 2 porn
    for i in range(2):
        _insert_event(conn, "did:plc:b", uri, "porn",
                      format_ts(now - timedelta(minutes=i)))
    conn.commit()

    w_start = format_ts(now - timedelta(hours=24))
    w_end = format_ts(now + timedelta(hours=1))
    dists = build_distributions(conn, [uri], w_start, w_end)

    assert uri in dists
    assert "did:plc:a" in dists[uri]
    # Binary: each family counts as 1 (not raw event count)
    assert dists[uri]["did:plc:a"]["spam"] == 1
    assert dists[uri]["did:plc:a"]["adult-sexual"] == 1  # "Adult Content" maps to "adult-sexual"
    assert dists[uri]["did:plc:b"]["adult-sexual"] == 1  # "porn" maps to "adult-sexual"


# ── Contradiction edges ──────────────────────────────────────────────


def test_contradiction_edge_high_jsd():
    """Two labelers with completely different families produce an edge."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"porn": 10},
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_jsd_min=0.1,
        boundary_min_top_share=0.3,
    )
    edges = compute_contradiction_edges(distributions, cfg)
    assert len(edges) == 1
    e = edges[0]
    assert e["edge_type"] == "contradiction"
    assert e["labeler_a"] == "did:plc:a"
    assert e["labeler_b"] == "did:plc:b"
    assert e["jsd"] > 0.9  # disjoint families


def test_no_edge_below_jsd_threshold():
    """Same families should not produce a contradiction edge."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"spam": 8},
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_jsd_min=0.15,
        boundary_min_top_share=0.3,
    )
    edges = compute_contradiction_edges(distributions, cfg)
    assert len(edges) == 0


def test_no_edge_insufficient_events():
    """Labelers below min_events_per_labeler should not create edges."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"porn": 1},  # below threshold
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_jsd_min=0.1,
        boundary_min_top_share=0.3,
    )
    edges = compute_contradiction_edges(distributions, cfg)
    assert len(edges) == 0


def test_canonical_pair_ordering_in_edge():
    """Edges should always have labeler_a < labeler_b lexicographically."""
    distributions = {
        "at://target/post/1": {
            "did:plc:z": {"spam": 10},
            "did:plc:a": {"porn": 10},
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_jsd_min=0.1,
        boundary_min_top_share=0.3,
    )
    edges = compute_contradiction_edges(distributions, cfg)
    assert len(edges) == 1
    assert edges[0]["labeler_a"] == "did:plc:a"
    assert edges[0]["labeler_b"] == "did:plc:z"


# ── Lead/lag edges ───────────────────────────────────────────────────


def test_lead_lag_edge():
    """Clear leader/follower with same family produces lead/lag edge."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"spam": 8},
        }
    }
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    first_seen = {
        "at://target/post/1": {
            "did:plc:a": format_ts(now - timedelta(hours=2)),
            "did:plc:b": format_ts(now - timedelta(hours=1)),
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_lag_max_s=21600,
        boundary_lag_min_overlap=0.3,
    )
    edges = compute_lead_lag_edges(distributions, first_seen, cfg)
    assert len(edges) == 1
    e = edges[0]
    assert e["edge_type"] == "lead_lag"
    assert e["leader_did"] == "did:plc:a"
    assert abs(e["delta_s"] - 3600.0) < 1.0


def test_lead_lag_no_edge_too_far():
    """No lead/lag edge if delta exceeds lag_max_s."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"spam": 8},
        }
    }
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    first_seen = {
        "at://target/post/1": {
            "did:plc:a": format_ts(now - timedelta(hours=24)),
            "did:plc:b": format_ts(now),
        }
    }
    cfg = Config(
        boundary_min_events_per_labeler=3,
        boundary_lag_max_s=21600,  # 6h
        boundary_lag_min_overlap=0.3,
    )
    edges = compute_lead_lag_edges(distributions, first_seen, cfg)
    assert len(edges) == 0


# ── Divergence summaries ─────────────────────────────────────────────


def test_divergence_summary_identical():
    """Identical distributions across labelers → mean JSD = 0."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"spam": 10},
            "did:plc:c": {"spam": 10},
        }
    }
    cfg = Config(boundary_min_events_per_labeler=3)
    summaries = compute_divergence_summaries(distributions, cfg)
    assert len(summaries) == 1
    assert summaries[0]["mean_jsd_to_centroid"] == 0.0
    assert summaries[0]["max_jsd_pair"] == 0.0
    assert summaries[0]["n_labelers"] == 3


def test_divergence_summary_high_disagreement():
    """Completely different families → high divergence."""
    distributions = {
        "at://target/post/1": {
            "did:plc:a": {"spam": 10},
            "did:plc:b": {"porn": 10},
        }
    }
    cfg = Config(boundary_min_events_per_labeler=3)
    summaries = compute_divergence_summaries(distributions, cfg)
    assert len(summaries) == 1
    assert summaries[0]["mean_jsd_to_centroid"] > 0.3
    assert summaries[0]["max_jsd_pair"] > 0.9


# ── Integration: run_boundary_pass ───────────────────────────────────


def test_run_boundary_pass_stores_results():
    """Full boundary pass finds shared targets, computes edges, stores them."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    uri = "at://did:plc:target1/app.bsky.feed.post/abc"

    for did in ["did:plc:a", "did:plc:b"]:
        _insert_labeler(conn, did, format_ts(now))

    # Labeler A labels spam, labeler B labels porn — disagreement
    for i in range(5):
        ts = format_ts(now - timedelta(minutes=i))
        _insert_event(conn, "did:plc:a", uri, "spam", ts)
        _insert_event(conn, "did:plc:b", uri, "porn", ts)
    conn.commit()

    cfg = Config(
        boundary_enabled=True,
        boundary_window_hours=24,
        boundary_min_labelers=2,
        boundary_min_events_per_labeler=1,
        boundary_jsd_min=0.1,
        boundary_min_top_share=0.3,
        boundary_lag_max_s=21600,
        boundary_lag_min_overlap=0.3,
    )

    stats = run_boundary_pass(conn, cfg, now)
    conn.commit()

    assert stats["shared_targets"] == 1
    assert stats["contradiction_edges"] == 1

    # Check DB
    edges = conn.execute("SELECT * FROM boundary_edges").fetchall()
    assert len(edges) >= 1
    assert edges[0]["edge_type"] == "contradiction"
    assert edges[0]["family_version"] == FAMILY_VERSION

    targets = conn.execute("SELECT * FROM boundary_targets").fetchall()
    assert len(targets) == 1
    assert targets[0]["n_labelers"] == 2


def test_run_boundary_pass_idempotent():
    """Running boundary pass twice produces same results (no duplicates)."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    uri = "at://did:plc:target1/app.bsky.feed.post/abc"

    for did in ["did:plc:a", "did:plc:b"]:
        _insert_labeler(conn, did, format_ts(now))

    for i in range(5):
        ts = format_ts(now - timedelta(minutes=i))
        _insert_event(conn, "did:plc:a", uri, "spam", ts)
        _insert_event(conn, "did:plc:b", uri, "porn", ts)
    conn.commit()

    cfg = Config(
        boundary_enabled=True,
        boundary_window_hours=24,
        boundary_min_labelers=2,
        boundary_min_events_per_labeler=1,
        boundary_jsd_min=0.1,
        boundary_min_top_share=0.3,
    )

    run_boundary_pass(conn, cfg, now)
    conn.commit()
    run_boundary_pass(conn, cfg, now)
    conn.commit()

    edge_count = conn.execute("SELECT COUNT(*) AS c FROM boundary_edges").fetchone()["c"]
    target_count = conn.execute("SELECT COUNT(*) AS c FROM boundary_targets").fetchone()["c"]

    # Should not have doubled
    assert edge_count == 1
    assert target_count == 1


def test_run_boundary_pass_no_shared_targets():
    """Boundary pass with no shared targets produces empty results."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    ts = format_ts(now)

    _insert_labeler(conn, "did:plc:a", ts)
    _insert_event(conn, "did:plc:a", "at://did:plc:t/post/1", "spam", ts)
    conn.commit()

    cfg = Config(
        boundary_enabled=True,
        boundary_min_labelers=2,
        boundary_min_events_per_labeler=1,
    )
    stats = run_boundary_pass(conn, cfg, now)
    assert stats["shared_targets"] == 0
    assert stats["edges_stored"] == 0
