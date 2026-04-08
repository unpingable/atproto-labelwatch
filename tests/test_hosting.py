"""Tests for labelwatch.hosting — PDS host classification and hosting-locus analysis."""

import sqlite3

import pytest

from labelwatch.hosting import (
    attach_facts,
    classify_host,
    detach_facts,
    extract_host_family,
    query_hosting_summary,
    query_labeled_targets_by_host,
    query_population_comparison,
)


class TestExtractHostFamily:
    def test_simple_two_part(self):
        assert extract_host_family("blacksky.app") == "blacksky.app"

    def test_three_part(self):
        assert extract_host_family("pds.example.com") == "example.com"

    def test_bsky_network_infra(self):
        assert extract_host_family("stropharia.us-west.host.bsky.network") == "host.bsky.network"

    def test_pds_rip_subdomain(self):
        assert extract_host_family("mahmouds.pds.rip") == "pds.rip"

    def test_deep_subdomain(self):
        assert extract_host_family("foo.bar.baz.example.com") == "example.com"

    def test_none(self):
        assert extract_host_family(None) is None

    def test_empty(self):
        assert extract_host_family("") is None

    def test_single_label(self):
        assert extract_host_family("localhost") == "localhost"

    def test_with_port(self):
        assert extract_host_family("localhost:8080") == "localhost"

    def test_bsky_social(self):
        # bsky.social is a known multi-level suffix, but only 2 parts
        assert extract_host_family("bsky.social") == "bsky.social"

    def test_jellybaby_bsky(self):
        assert extract_host_family("jellybaby.us-east.host.bsky.network") == "host.bsky.network"

    def test_onrender(self):
        assert extract_host_family("aaa55aaa.onrender.com") == "onrender.com"


def _make_db_with_registry():
    """Create in-memory DB with provider_registry seeded."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE provider_registry (
            host_pattern TEXT PRIMARY KEY,
            match_type TEXT NOT NULL,
            provider_group TEXT NOT NULL,
            provider_label TEXT NOT NULL,
            is_major_provider INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.executemany(
        "INSERT INTO provider_registry VALUES (?, ?, ?, ?, ?)",
        [
            ("host.bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("bsky.social", "exact", "bluesky", "Bluesky-hosted", 1),
            ("bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("blacksky.app", "suffix", "known_alt", "Blacksky", 1),
            ("atproto.brid.gy", "exact", "known_alt", "Bridgy Fed", 1),
            ("pds.rip", "suffix", "known_alt", "pds.rip", 0),
        ],
    )
    conn.commit()
    return conn


class TestClassifyHost:
    def test_bsky_network_suffix(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "stropharia.us-west.host.bsky.network", "ok")
        assert group == "bluesky"
        assert is_major is True

    def test_exact_match(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "bsky.social", "ok")
        assert group == "bluesky"
        assert label == "Bluesky-hosted"

    def test_blacksky(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "pds.blacksky.app", "ok")
        assert group == "known_alt"
        assert label == "Blacksky"

    def test_bridgy_fed(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "atproto.brid.gy", "ok")
        assert group == "known_alt"
        assert label == "Bridgy Fed"

    def test_pds_rip_family(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "mahmouds.pds.rip", "ok")
        assert group == "known_alt"
        assert label == "pds.rip"
        assert is_major is False

    def test_unknown_host(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "weird.self-hosted.example", "ok")
        assert group == "one_off"
        assert is_major is False

    def test_unresolved(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, None, None)
        assert group == "unknown"
        assert label == "Unresolved/Unknown"

    def test_error_status(self):
        conn = _make_db_with_registry()
        group, label, is_major = classify_host(conn, "foo.bar", "error")
        assert group == "unknown"


def _make_full_test_db(tmp_path):
    """Create labelwatch DB + facts DB for integration tests."""
    # Main labelwatch DB
    main_db = sqlite3.connect(str(tmp_path / "labelwatch.db"))
    main_db.execute("""
        CREATE TABLE label_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labeler_did TEXT NOT NULL,
            src TEXT,
            uri TEXT NOT NULL,
            cid TEXT,
            val TEXT NOT NULL,
            neg INTEGER DEFAULT 0,
            exp TEXT,
            sig TEXT,
            ts TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE,
            target_did TEXT
        )
    """)
    main_db.execute("""
        CREATE TABLE provider_registry (
            host_pattern TEXT PRIMARY KEY,
            match_type TEXT NOT NULL,
            provider_group TEXT NOT NULL,
            provider_label TEXT NOT NULL,
            is_major_provider INTEGER NOT NULL DEFAULT 0
        )
    """)
    main_db.executemany(
        "INSERT INTO provider_registry VALUES (?, ?, ?, ?, ?)",
        [
            ("host.bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("pds.rip", "suffix", "known_alt", "pds.rip", 0),
        ],
    )
    # Insert label events with various targets
    main_db.executemany(
        "INSERT INTO label_events (labeler_did, uri, val, ts, event_hash, target_did) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("did:plc:labeler1", "at://did:plc:alice/post/1", "spam", "2026-03-18T00:00:00Z", "h1", "did:plc:alice"),
            ("did:plc:labeler1", "at://did:plc:bob/post/2", "spam", "2026-03-18T00:00:00Z", "h2", "did:plc:bob"),
            ("did:plc:labeler1", "at://did:plc:carol/post/3", "spam", "2026-03-18T00:00:00Z", "h3", "did:plc:carol"),
            ("did:plc:labeler2", "at://did:plc:carol/post/4", "nsfw", "2026-03-18T00:00:00Z", "h4", "did:plc:carol"),
            ("did:plc:labeler1", "at://did:plc:dave/post/5", "spam", "2026-03-18T00:00:00Z", "h5", "did:plc:dave"),
            ("did:plc:labeler1", "at://did:plc:eve/post/6", "spam", "2026-03-18T00:00:00Z", "h6", "did:plc:eve"),
        ],
    )
    main_db.commit()

    # Facts DB
    facts_db = sqlite3.connect(str(tmp_path / "facts.sqlite"))
    facts_db.execute("""
        CREATE TABLE actor_identity_facts (
            did TEXT PRIMARY KEY,
            handle TEXT,
            pds_endpoint TEXT,
            pds_host TEXT,
            resolver_status TEXT,
            resolver_last_success_at TEXT,
            is_active INTEGER
        )
    """)
    facts_db.executemany(
        "INSERT INTO actor_identity_facts VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("did:plc:alice", "alice.bsky.social", "https://pds1.host.bsky.network", "pds1.host.bsky.network", "ok", "2026-03-18", 1),
            ("did:plc:bob", "bob.bsky.social", "https://pds2.host.bsky.network", "pds2.host.bsky.network", "ok", "2026-03-18", 1),
            ("did:plc:carol", "handle.invalid", "https://mahmouds.pds.rip", "mahmouds.pds.rip", "ok", "2026-03-18", 1),
            ("did:plc:dave", "dave.test", "https://weird.self-hosted.example", "weird.self-hosted.example", "ok", "2026-03-18", 1),
            ("did:plc:eve", None, None, None, None, None, 1),
        ],
    )
    facts_db.commit()
    facts_db.close()

    return main_db, str(tmp_path / "facts.sqlite")


class TestQueryLabeledTargetsByHost:
    def test_basic_join(self, tmp_path):
        conn, facts_path = _make_full_test_db(tmp_path)
        assert attach_facts(conn, facts_path)
        try:
            rows = query_labeled_targets_by_host(conn, days=30)
            assert len(rows) > 0

            # Find the bluesky rows (alice + bob)
            bluesky = [r for r in rows if r.provider_group == "bluesky"]
            assert len(bluesky) > 0

            # pds.rip should show up
            pds_rip = [r for r in rows if r.host_family == "pds.rip"]
            assert len(pds_rip) > 0
            assert pds_rip[0].invalid_handle_count > 0  # carol has handle.invalid

            # Unresolved should show (eve)
            unknown = [r for r in rows if r.provider_group == "unknown"]
            assert len(unknown) > 0
        finally:
            detach_facts(conn)

    def test_exclude_majors(self, tmp_path):
        conn, facts_path = _make_full_test_db(tmp_path)
        assert attach_facts(conn, facts_path)
        try:
            rows = query_labeled_targets_by_host(conn, days=30, exclude_majors=True)
            for r in rows:
                assert not r.is_major_provider
        finally:
            detach_facts(conn)


class TestQueryHostingSummary:
    def test_summary(self, tmp_path):
        conn, facts_path = _make_full_test_db(tmp_path)
        assert attach_facts(conn, facts_path)
        try:
            summary = query_hosting_summary(conn, days=30)
            assert summary["status"] == "ok"
            assert summary["total_labeled_targets"] == 6
            assert summary["resolved_pct"] > 0
            assert summary["invalid_handle_count"] >= 1
            assert summary["unresolved_count"] >= 1
            assert len(summary["top_non_major_hosts"]) > 0
        finally:
            detach_facts(conn)

    def test_no_data(self, tmp_path):
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE label_events (
                id INTEGER PRIMARY KEY, labeler_did TEXT, uri TEXT, val TEXT,
                ts TEXT, event_hash TEXT UNIQUE, target_did TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE provider_registry (
                host_pattern TEXT PRIMARY KEY, match_type TEXT,
                provider_group TEXT, provider_label TEXT,
                is_major_provider INTEGER DEFAULT 0
            )
        """)
        # No facts attached — should return no_data gracefully
        summary = query_hosting_summary(conn, days=7)
        assert summary["status"] == "no_data"


def _make_comparison_test_db(tmp_path):
    """Create DBs with a larger overall population to test skew detection."""
    main_db = sqlite3.connect(str(tmp_path / "labelwatch.db"))
    main_db.execute("""
        CREATE TABLE label_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            labeler_did TEXT NOT NULL, src TEXT, uri TEXT NOT NULL,
            cid TEXT, val TEXT NOT NULL, neg INTEGER DEFAULT 0,
            exp TEXT, sig TEXT, ts TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE, target_did TEXT
        )
    """)
    main_db.execute("""
        CREATE TABLE provider_registry (
            host_pattern TEXT PRIMARY KEY, match_type TEXT NOT NULL,
            provider_group TEXT NOT NULL, provider_label TEXT NOT NULL,
            is_major_provider INTEGER NOT NULL DEFAULT 0
        )
    """)
    main_db.executemany(
        "INSERT INTO provider_registry VALUES (?, ?, ?, ?, ?)",
        [
            ("host.bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
            ("pds.rip", "suffix", "known_alt", "pds.rip", 0),
        ],
    )

    # Labeled targets: 3 on bsky, 3 on pds.rip (50/50 split)
    events = [
        ("did:plc:l1", f"at://did:plc:a{i}/post/1", "spam", "2026-03-18T00:00:00Z", f"h{i}", f"did:plc:a{i}")
        for i in range(3)
    ] + [
        ("did:plc:l1", f"at://did:plc:b{i}/post/1", "spam", "2026-03-18T00:00:00Z", f"g{i}", f"did:plc:b{i}")
        for i in range(3)
    ]
    main_db.executemany(
        "INSERT INTO label_events (labeler_did, uri, val, ts, event_hash, target_did) VALUES (?, ?, ?, ?, ?, ?)",
        events,
    )
    main_db.commit()

    # Facts DB: overall population is 90% bsky, 10% pds.rip
    facts_db = sqlite3.connect(str(tmp_path / "facts.sqlite"))
    facts_db.execute("""
        CREATE TABLE actor_identity_facts (
            did TEXT PRIMARY KEY, handle TEXT, pds_endpoint TEXT,
            pds_host TEXT, resolver_status TEXT,
            resolver_last_success_at TEXT, is_active INTEGER
        )
    """)
    # 90 accounts on bsky
    bsky_rows = [
        (f"did:plc:bsky{i}", f"u{i}.bsky.social", "https://pds1.host.bsky.network",
         "pds1.host.bsky.network", "ok", "2026-03-18", 1)
        for i in range(90)
    ]
    # 10 accounts on pds.rip
    rip_rows = [
        (f"did:plc:rip{i}", f"u{i}.pds.rip", "https://mahmouds.pds.rip",
         "mahmouds.pds.rip", "ok", "2026-03-18", 1)
        for i in range(10)
    ]
    # Include the labeled targets in the overall population
    labeled_bsky = [
        (f"did:plc:a{i}", f"a{i}.bsky.social", "https://pds1.host.bsky.network",
         "pds1.host.bsky.network", "ok", "2026-03-18", 1)
        for i in range(3)
    ]
    labeled_rip = [
        (f"did:plc:b{i}", f"b{i}.test", "https://mahmouds.pds.rip",
         "mahmouds.pds.rip", "ok", "2026-03-18", 1)
        for i in range(3)
    ]
    facts_db.executemany(
        "INSERT INTO actor_identity_facts VALUES (?, ?, ?, ?, ?, ?, ?)",
        bsky_rows + rip_rows + labeled_bsky + labeled_rip,
    )
    facts_db.commit()
    facts_db.close()

    return main_db, str(tmp_path / "facts.sqlite")


class TestPopulationComparison:
    def test_detects_skew(self, tmp_path):
        """pds.rip is 50% of labeled but ~12% of overall → positive delta."""
        conn, facts_path = _make_comparison_test_db(tmp_path)
        assert attach_facts(conn, facts_path)
        try:
            result = query_population_comparison(conn, days=30, min_accounts=1)
            assert result["status"] == "ok"
            assert result["overall_resolved"] == 106  # 90 + 10 + 3 + 3
            assert result["labeled_resolved"] == 6

            rows = result["rows"]
            assert len(rows) >= 2

            # Find pds.rip — should be over-represented in labeled
            rip = [r for r in rows if r.host_family == "pds.rip"]
            assert len(rip) == 1
            assert rip[0].delta_pct > 0  # over-labeled
            assert rip[0].labeled_pct == pytest.approx(50.0, abs=1)

            # Find bsky — should be under-represented in labeled
            bsky = [r for r in rows if r.host_family == "host.bsky.network"]
            assert len(bsky) == 1
            assert bsky[0].delta_pct < 0  # under-labeled

        finally:
            detach_facts(conn)

    def test_no_facts(self):
        conn = sqlite3.connect(":memory:")
        result = query_population_comparison(conn, days=7)
        assert result["status"] == "no_facts"

    def test_caveats_on_low_coverage(self, tmp_path):
        conn, facts_path = _make_comparison_test_db(tmp_path)
        assert attach_facts(conn, facts_path)
        try:
            result = query_population_comparison(conn, days=30, min_accounts=1)
            # 6 labeled out of 106 overall = ~5.7% coverage → should flag
            assert any("low coverage" in c for c in result["caveats"])
        finally:
            detach_facts(conn)
