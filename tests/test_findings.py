"""Tests for the findings formatter (boundary fight pairs → FindingPost)."""
import sqlite3
from datetime import datetime, timezone

from labelwatch import db
from labelwatch.db import has_been_posted, record_posted
from labelwatch.findings import (
    _classify_disagreement,
    _dedupe_key,
    _handle_or_short_did,
    _is_protocol_action,
    find_postable_fights,
    format_fight_pair,
)
from labelwatch.label_family import FAMILY_VERSION, classify_kind, classify_polarity


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_db(conn)
    return conn


def _insert_labeler(conn, did, handle):
    conn.execute("""
        INSERT OR REPLACE INTO labelers (labeler_did, handle, first_seen, last_seen)
        VALUES (?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
    """, (did, handle))
    conn.commit()


def _insert_edge(conn, labeler_a, labeler_b, target_uri,
                 family_a, family_b, jsd=0.8, computed_at="2026-03-13T12:00:00Z"):
    conn.execute("""
        INSERT INTO boundary_edges (
            edge_type, target_uri, window_start, window_end,
            labeler_a, labeler_b, jsd,
            top_family_a, top_share_a, top_family_b, top_share_b,
            delta_s, overlap, leader_did,
            n_events_a, n_events_b, family_version, config_hash, computed_at
        ) VALUES (
            'contradiction', ?, '2026-03-06T12:00:00Z', '2026-03-13T12:00:00Z',
            ?, ?, ?,
            ?, 1.0, ?, 1.0,
            0, 0.0, NULL,
            5, 5, ?, 'test', ?
        )
    """, (target_uri, labeler_a, labeler_b, jsd,
          family_a, family_b, FAMILY_VERSION, computed_at))
    conn.commit()


# --- classify_disagreement ---

def test_polarity_negative():
    for f in ["spam", "harassment", "hate", "violence", "adult-sexual",
              "misleading", "impersonation", "inauthenticity", "mod-hide", "mod-takedown"]:
        assert classify_polarity(f) == "negative", f


def test_polarity_cautionary():
    for f in ["nudity", "graphic-media", "mod-warn", "mod-gate"]:
        assert classify_polarity(f) == "cautionary", f


def test_polarity_badge():
    # Novelty-domain families fall back to badge
    assert classify_polarity("crushed-piano") == "badge"
    assert classify_polarity("oracle-pick") == "badge"


def test_polarity_unknown():
    # Metadata/political/identity families get unknown
    assert classify_polarity("handle-changed") == "unknown"
    assert classify_polarity("trumpface") == "unknown"
    assert classify_polarity("gay-post") == "unknown"


def test_kind_policy_claim():
    for f in ["spam", "harassment", "hate", "violence", "adult-sexual",
              "misleading", "impersonation", "inauthenticity", "nudity", "graphic-media"]:
        assert classify_kind(f) == "policy_claim", f


def test_kind_protocol_action():
    for f in ["mod-warn", "mod-hide", "mod-gate", "mod-takedown"]:
        assert classify_kind(f) == "protocol_action", f
    # ! prefix fallback
    assert classify_kind("!classification-forced") == "protocol_action"


def test_kind_status_signal():
    for f in ["handle-changed", "some-blocks", "bulk-following", "bot",
              "posting-daily-made-over-25-posts-yesterday"]:
        assert classify_kind(f) == "status_signal", f


def test_kind_decorative():
    assert classify_kind("crushed-piano") == "decorative"
    assert classify_kind("oracle-pick") == "decorative"


def test_kind_political_identity_are_claims():
    assert classify_kind("trumpface") == "policy_claim"
    assert classify_kind("gay-post") == "policy_claim"


def test_classify_claim_vs_action():
    # Policy claim vs protocol action in same domain
    assert _classify_disagreement("spam", "mod-hide") == "claim_vs_action"
    assert _classify_disagreement("harassment", "mod-warn") == "claim_vs_action"
    assert _classify_disagreement("nudity", "mod-gate") == "claim_vs_action"


def test_classify_disagreement_is_commutative():
    """Argument order must not affect disagreement type."""
    pairs = [
        ("spam", "inauthenticity"),
        ("spam", "mod-hide"),
        ("nudity", "adult-sexual"),
        ("mod-warn", "mod-hide"),
        ("spam", "trump"),
        ("harassment", "!classification-forced"),
        ("graphic-media", "violence"),
    ]
    for a, b in pairs:
        assert _classify_disagreement(a, b) == _classify_disagreement(b, a), \
            f"Not commutative: {a} vs {b}"


def test_classify_taxonomy_shear():
    # Same domain (moderation), different families
    assert _classify_disagreement("spam", "inauthenticity") == "taxonomy_shear"


def test_classify_substantive_disagreement():
    # Different domains: moderation vs political
    assert _classify_disagreement("spam", "trump") == "substantive_disagreement"


def test_classify_same_family():
    # Same family = taxonomy_shear (trivially)
    assert _classify_disagreement("spam", "spam") == "taxonomy_shear"


def test_classify_severity_difference():
    # Same domain, cautionary vs negative = severity difference
    assert _classify_disagreement("nudity", "adult-sexual") == "severity_difference"
    assert _classify_disagreement("graphic-media", "violence") == "severity_difference"
    assert _classify_disagreement("mod-warn", "mod-hide") == "severity_difference"


def test_classify_negative_vs_negative_is_taxonomy():
    # Same polarity (both negative) = taxonomy shear, not severity
    assert _classify_disagreement("spam", "harassment") == "taxonomy_shear"
    assert _classify_disagreement("mod-hide", "mod-takedown") == "taxonomy_shear"


def test_classify_protocol_action_vs_claim():
    # !classification-forced is protocol_action, harassment is policy_claim
    # Both moderation domain, but different kinds → claim_vs_action
    assert _classify_disagreement("!classification-forced", "harassment") == "claim_vs_action"


# --- dedupe_key ---

def test_dedupe_key_stable():
    k1 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    k2 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    assert k1 == k2


def test_dedupe_key_order_independent():
    k1 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    k2 = _dedupe_key("did:b", "did:a", "inauthenticity", "spam")
    assert k1 == k2


def test_dedupe_key_changes_with_families():
    k1 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    k2 = _dedupe_key("did:a", "did:b", "spam", "hate")
    assert k1 != k2


def test_dedupe_key_same_across_days():
    """Same fight = same key regardless of when you look at it."""
    k1 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    k2 = _dedupe_key("did:a", "did:b", "spam", "inauthenticity")
    assert k1 == k2


# --- handle_or_short_did ---

def test_handle_lookup():
    conn = _make_conn()
    _insert_labeler(conn, "did:plc:abc123", "skywatch.blue")
    assert _handle_or_short_did(conn, "did:plc:abc123") == "skywatch.blue"


def test_handle_fallback_to_did():
    conn = _make_conn()
    result = _handle_or_short_did(conn, "did:plc:verylongdidthatdoesnotexistinthedatabase")
    assert result.endswith("...")
    assert len(result) <= 36


# --- format_fight_pair ---

def test_format_fight_pair_basic():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "skywatch.blue")
    _insert_labeler(conn, "did:b", "labeler.hailey.at")

    edges = [
        {"target_uri": "at://t1", "top_family_a": "inauthenticity",
         "top_family_b": "spam", "jsd": 0.9,
         "top_share_a": 1.0, "top_share_b": 1.0,
         "n_events_a": 3, "n_events_b": 3},
        {"target_uri": "at://t2", "top_family_a": "inauthenticity",
         "top_family_b": "spam", "jsd": 0.8,
         "top_share_a": 1.0, "top_share_b": 1.0,
         "n_events_a": 2, "n_events_b": 2},
    ]

    finding = format_fight_pair(conn, "did:a", "did:b", edges)
    assert finding is not None
    assert "skywatch.blue" in finding.headline
    assert "labeler.hailey.at" in finding.headline
    assert "\u201cinauthenticity\u201d" in finding.summary
    assert "\u201cspam\u201d" in finding.summary
    assert "2 shared targets" in finding.summary
    assert finding.dedupe_key is not None
    assert len(finding.dedupe_key) == 16


def test_format_fight_pair_empty_edges():
    conn = _make_conn()
    assert format_fight_pair(conn, "did:a", "did:b", []) is None


def test_format_fight_pair_includes_disagreement_type():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "labeler-a.test")
    _insert_labeler(conn, "did:b", "labeler-b.test")

    edges = [
        {"target_uri": "at://t1", "top_family_a": "spam",
         "top_family_b": "inauthenticity", "jsd": 0.7,
         "top_share_a": 1.0, "top_share_b": 1.0,
         "n_events_a": 1, "n_events_b": 1},
    ]

    finding = format_fight_pair(conn, "did:a", "did:b", edges)
    assert finding is not None
    # taxonomy_shear: both moderation domain
    assert "categorize it differently" in finding.summary


def test_format_substantive_disagreement_text():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "labeler-a.test")
    _insert_labeler(conn, "did:b", "labeler-b.test")

    # moderation vs political = substantive disagreement
    edges = [
        {"target_uri": "at://t1", "top_family_a": "spam",
         "top_family_b": "trump", "jsd": 1.0,
         "top_share_a": 1.0, "top_share_b": 1.0,
         "n_events_a": 1, "n_events_b": 1},
    ]

    finding = format_fight_pair(conn, "did:a", "did:b", edges)
    assert finding is not None
    assert "different claims" in finding.summary


def test_finding_renders_to_post_text():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "skywatch.blue")
    _insert_labeler(conn, "did:b", "labeler.hailey.at")

    edges = [
        {"target_uri": f"at://t{i}", "top_family_a": "inauthenticity",
         "top_family_b": "spam", "jsd": 0.9,
         "top_share_a": 1.0, "top_share_b": 1.0,
         "n_events_a": 3, "n_events_b": 3}
        for i in range(5)
    ]

    finding = format_fight_pair(conn, "did:a", "did:b", edges)
    text = finding.render_text()
    # Should have headline, summary, and URL
    assert "skywatch.blue" in text
    assert "labelwatch.neutral.zone" in text
    # Should be short enough for Bluesky
    assert len(text) <= 600  # generous; real limit is 300 graphemes


# --- find_postable_fights (integration) ---

def test_find_postable_fights_with_data():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "skywatch.blue")
    _insert_labeler(conn, "did:b", "labeler.hailey.at")

    # Insert moderation-domain edges with 12 shared targets (above min_targets=10)
    for i in range(12):
        _insert_edge(conn, "did:a", "did:b", f"at://target/{i}",
                     "inauthenticity", "spam")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 1
    assert "skywatch.blue" in findings[0].headline


def test_find_postable_fights_filters_novelty():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "oracle.test")
    _insert_labeler(conn, "did:b", "stechlab.test")

    # Insert novelty-domain edges (should be filtered out)
    # Unmapped families without ! prefix default to novelty domain
    for i in range(5):
        _insert_edge(conn, "did:a", "did:b", f"at://target/{i}",
                     "oracle-pick", "stech-sticker")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 0


def test_find_postable_fights_filters_metadata_vs_moderation():
    """Metadata-domain families (some-blocks, bulk-following) must not leak into fights."""
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "skywatch.test")
    _insert_labeler(conn, "did:b", "hailey.test")

    # Insert metadata-vs-moderation edges (should be filtered out)
    for i in range(12):
        _insert_edge(conn, "did:a", "did:b", f"at://target/{i}",
                     "inauthenticity", "some-blocks")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 0, "metadata-vs-moderation pairs should not be postable fights"


def test_find_postable_fights_min_targets():
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "labeler-a.test")
    _insert_labeler(conn, "did:b", "labeler-b.test")

    # Only 1 shared target — below default min_targets=10
    _insert_edge(conn, "did:a", "did:b", "at://single",
                 "spam", "harassment")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 0


def test_find_postable_fights_empty_db():
    conn = _make_conn()
    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert findings == []


def test_find_postable_fights_filters_protocol_actions():
    """Pairs where the dominant family is a protocol action (!-prefix) are filtered."""
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "nunnybabbit.test")
    _insert_labeler(conn, "did:b", "skywatch.test")

    # !classification-forced is a protocol action, not a policy claim
    for i in range(15):
        _insert_edge(conn, "did:a", "did:b", f"at://target/{i}",
                     "!classification-forced", "harassment")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 0


def test_is_protocol_action():
    assert _is_protocol_action("!classification-forced") is True
    assert _is_protocol_action("!hide") is True
    assert _is_protocol_action("!warn") is True
    assert _is_protocol_action("spam") is False
    assert _is_protocol_action("inauthenticity") is False


# --- sent-post ledger ---

def test_has_been_posted_false():
    conn = _make_conn()
    assert has_been_posted(conn, "nonexistent") is False


def test_record_and_check_posted():
    conn = _make_conn()
    record_posted(conn, "abc123", "boundary_fight", post_uri="at://post/1")
    conn.commit()
    assert has_been_posted(conn, "abc123") is True
    assert has_been_posted(conn, "other") is False


def test_record_posted_idempotent():
    conn = _make_conn()
    record_posted(conn, "abc123", "boundary_fight", post_uri="at://post/1")
    conn.commit()
    # Second call with different uri should replace
    record_posted(conn, "abc123", "boundary_fight", post_uri="at://post/2")
    conn.commit()
    row = conn.execute(
        "SELECT post_uri FROM posted_findings WHERE dedupe_key = ?", ("abc123",)
    ).fetchone()
    assert row["post_uri"] == "at://post/2"


def test_finding_dedupe_key_checks_ledger():
    """Integration: produce a finding, record it, confirm it's filtered."""
    conn = _make_conn()
    _insert_labeler(conn, "did:a", "skywatch.blue")
    _insert_labeler(conn, "did:b", "labeler.hailey.at")

    for i in range(12):
        _insert_edge(conn, "did:a", "did:b", f"at://target/{i}",
                     "inauthenticity", "spam")

    now = datetime(2026, 3, 13, 12, 30, 0, tzinfo=timezone.utc)
    findings = find_postable_fights(conn, now=now)
    assert len(findings) == 1

    # Record it as posted
    record_posted(conn, findings[0].dedupe_key, "boundary_fight")
    conn.commit()

    # Now check: the key is in the ledger
    assert has_been_posted(conn, findings[0].dedupe_key) is True


def test_cooldown_recent_post_blocked():
    """A finding posted today is still in cooldown at 7 days."""
    conn = _make_conn()
    record_posted(conn, "recent", "boundary_fight")
    conn.commit()
    assert has_been_posted(conn, "recent", cooldown_days=7) is True


def test_cooldown_old_post_eligible():
    """A finding posted 10 days ago is outside a 7-day cooldown."""
    conn = _make_conn()
    # Manually insert with an old posted_at
    conn.execute(
        "INSERT INTO posted_findings (dedupe_key, finding_type, posted_at) VALUES (?, ?, ?)",
        ("old-fight", "boundary_fight", "2026-03-01T00:00:00Z"),
    )
    conn.commit()
    # With cooldown=7 and "now" being 2026-03-13, 12 days ago = eligible
    assert has_been_posted(conn, "old-fight", cooldown_days=7) is False
    # Without cooldown, still blocked
    assert has_been_posted(conn, "old-fight", cooldown_days=0) is True
