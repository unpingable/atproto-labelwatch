"""Tests for boundary instability primitives (Phase 1 + Phase 2)."""
from datetime import datetime, timedelta, timezone

from labelwatch import db
from labelwatch.boundary import (
    _ordered_pair,
    boundary_summary_for_report,
    build_distributions,
    classify_edge_domains,
    compute_contradiction_edges,
    compute_divergence_summaries,
    compute_lead_lag_edges,
    filter_fight_edges,
    find_shared_targets,
    jsd,
    run_boundary_pass,
)
from labelwatch.config import Config
from labelwatch.label_family import (
    FAMILY_VERSION,
    classify_domain,
    normalize_family,
)
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
    assert FAMILY_VERSION == "v2"


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


# ── Phase 2: Domain classification ──────────────────────────────────


def test_classify_domain_moderation_families():
    """Mapped moderation families return 'moderation'."""
    for fam in ("spam", "adult-sexual", "nudity", "harassment", "hate",
                "violence", "impersonation", "mod-warn", "mod-hide",
                "mod-takedown", "mod-gate", "misleading", "graphic-media"):
        assert classify_domain(fam) == "moderation", f"{fam} should be moderation"


def test_classify_domain_metadata_families():
    """Mapped metadata families return 'metadata'."""
    for fam in ("handle-changed", "bot-reply", "site-standard",
                "some-blocks", "modlist-author"):
        assert classify_domain(fam) == "metadata", f"{fam} should be metadata"


def test_classify_domain_political():
    assert classify_domain("uspol") == "political"
    assert classify_domain("government") == "political"


def test_classify_domain_novelty_fallback():
    """Unmapped families without moderation keywords default to novelty."""
    assert classify_domain("cool-badge") == "novelty"
    assert classify_domain("emoji-collector") == "novelty"
    assert classify_domain("") == "novelty"


def test_classify_domain_keyword_heuristic():
    """Unmapped families with moderation keywords get 'moderation'."""
    assert classify_domain("custom-spam-filter") == "moderation"  # "spam" in name
    assert classify_domain("nsfw-detector") == "moderation"
    assert classify_domain("abuse-report") == "moderation"


def test_classify_domain_bang_prefix():
    """ATProto mod actions (! prefix) are moderation even if unmapped."""
    assert classify_domain("!custom-action") == "moderation"


# ── Phase 2: Edge domain classification ─────────────────────────────


def test_classify_edge_domains_both_moderation():
    edge = {"top_family_a": "spam", "top_family_b": "harassment"}
    assert classify_edge_domains(edge) == ("moderation", "moderation")


def test_classify_edge_domains_mixed():
    edge = {"top_family_a": "spam", "top_family_b": "cool-badge"}
    assert classify_edge_domains(edge) == ("moderation", "novelty")


def test_classify_edge_domains_null():
    """Missing families default to novelty (empty string)."""
    edge = {"top_family_a": None, "top_family_b": "spam"}
    assert classify_edge_domains(edge) == ("novelty", "moderation")


# ── Phase 2: Fight-edge filtering ───────────────────────────────────


def _setup_boundary_edges(conn, now):
    """Insert boundary edges for fight-edge tests. Returns window params."""
    ts = format_ts(now)
    w_start = format_ts(now - timedelta(hours=24))
    w_end = format_ts(now + timedelta(hours=1))

    # 3 moderation-vs-moderation edges between same pair on different targets
    for i in range(3):
        conn.execute("""
            INSERT INTO boundary_edges (
                edge_type, target_uri, window_start, window_end,
                labeler_a, labeler_b, jsd, top_family_a, top_share_a,
                top_family_b, top_share_b, delta_s, overlap, leader_did,
                n_events_a, n_events_b, family_version, config_hash, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "contradiction", f"at://did:plc:t/post/{i}", w_start, w_end,
            "did:plc:a", "did:plc:b", 0.95,
            "spam", 0.8, "harassment", 0.9,
            None, None, None, 5, 5,
            FAMILY_VERSION, "test", ts,
        ))

    # 1 novelty-vs-novelty edge (different pair)
    conn.execute("""
        INSERT INTO boundary_edges (
            edge_type, target_uri, window_start, window_end,
            labeler_a, labeler_b, jsd, top_family_a, top_share_a,
            top_family_b, top_share_b, delta_s, overlap, leader_did,
            n_events_a, n_events_b, family_version, config_hash, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "contradiction", "at://did:plc:t/post/99", w_start, w_end,
        "did:plc:c", "did:plc:d", 1.0,
        "cool-badge", 1.0, "emoji-star", 1.0,
        None, None, None, 3, 3,
        FAMILY_VERSION, "test", ts,
    ))

    # 1 moderation-vs-moderation edge with only 1 target (below threshold)
    conn.execute("""
        INSERT INTO boundary_edges (
            edge_type, target_uri, window_start, window_end,
            labeler_a, labeler_b, jsd, top_family_a, top_share_a,
            top_family_b, top_share_b, delta_s, overlap, leader_did,
            n_events_a, n_events_b, family_version, config_hash, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "contradiction", "at://did:plc:t/post/solo", w_start, w_end,
        "did:plc:e", "did:plc:f", 0.85,
        "spam", 0.9, "misleading", 0.8,
        None, None, None, 4, 4,
        FAMILY_VERSION, "test", ts,
    ))

    conn.commit()
    return w_start, w_end


def test_filter_fight_edges_moderation_only():
    """Only moderation-vs-moderation edges with 2+ shared targets pass."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start, w_end = _setup_boundary_edges(conn, now)

    fights = filter_fight_edges(conn, w_start, w_end, min_shared_targets=2)
    # Only the 3 edges from did:plc:a vs did:plc:b qualify
    assert len(fights) == 3
    for e in fights:
        assert e["labeler_a"] == "did:plc:a"
        assert e["labeler_b"] == "did:plc:b"


def test_filter_fight_edges_excludes_novelty():
    """Novelty-vs-novelty edges never appear in fight edges."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start, w_end = _setup_boundary_edges(conn, now)

    fights = filter_fight_edges(conn, w_start, w_end, min_shared_targets=1)
    labeler_pairs = {(e["labeler_a"], e["labeler_b"]) for e in fights}
    # c vs d is novelty — should not appear
    assert ("did:plc:c", "did:plc:d") not in labeler_pairs


def test_filter_fight_edges_min_targets_threshold():
    """Pairs with only 1 shared target are excluded at min_shared_targets=2."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start, w_end = _setup_boundary_edges(conn, now)

    fights = filter_fight_edges(conn, w_start, w_end, min_shared_targets=2)
    labeler_pairs = {(e["labeler_a"], e["labeler_b"]) for e in fights}
    # e vs f has 1 target — excluded
    assert ("did:plc:e", "did:plc:f") not in labeler_pairs


# ── Phase 2: Boundary summary for report ────────────────────────────


def test_boundary_summary_domain_counts():
    """Summary correctly counts edges by domain."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start, w_end = _setup_boundary_edges(conn, now)

    summary = boundary_summary_for_report(conn, w_start, w_end)
    assert summary["total_edges"] == 5  # 3 mod + 1 novelty + 1 mod (single target)
    assert summary["moderation_edges"] == 4  # 3 (a vs b) + 1 (e vs f)
    assert summary["novelty_edges"] == 1
    assert summary["metadata_edges"] == 0


def test_boundary_summary_fight_edges_filtered():
    """Fight edges in summary only include pairs with 2+ targets."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start, w_end = _setup_boundary_edges(conn, now)

    summary = boundary_summary_for_report(conn, w_start, w_end)
    # Only a vs b qualifies (3 targets), e vs f has only 1
    assert len(summary["fight_edges"]) == 3
    assert len(summary["top_fight_pairs"]) == 1
    pair, count = summary["top_fight_pairs"][0]
    assert pair == ("did:plc:a", "did:plc:b")
    assert count == 3


def test_boundary_summary_empty():
    """Summary with no edges returns zeros."""
    conn = _make_db()
    now = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    w_start = format_ts(now - timedelta(hours=24))
    w_end = format_ts(now)

    summary = boundary_summary_for_report(conn, w_start, w_end)
    assert summary["total_edges"] == 0
    assert summary["fight_edges"] == []
    assert summary["top_fight_pairs"] == []
