"""Tests for publication readiness assessment."""
import sqlite3

import pytest

from labelwatch.publication import (
    MIN_MEDIAN_JSD_READY,
    MIN_MEDIAN_JSD_REVIEWABLE,
    MIN_SHARE_READY,
    MIN_SHARE_REVIEWABLE,
    MIN_TARGETS_READY,
    MIN_TARGETS_REVIEWABLE,
    MIN_WINDOWS_READY,
    MIN_WINDOWS_REVIEWABLE,
    PublicationAssessment,
    assess_finding,
    format_assessment,
)
from labelwatch.posting import FindingPost


def _make_edges(
    n: int,
    family_a: str = "spam",
    family_b: str = "harassment",
    jsd: float = 0.8,
    n_windows: int = 1,
) -> list[dict]:
    """Build a list of fake edges for testing."""
    edges = []
    for i in range(n):
        window_day = i % n_windows
        edges.append({
            "target_uri": f"at://did:plc:target{i}/app.bsky.feed.post/abc{i}",
            "labeler_a": "did:plc:labelerA",
            "labeler_b": "did:plc:labelerB",
            "jsd": jsd,
            "top_family_a": family_a,
            "top_share_a": 0.8,
            "top_family_b": family_b,
            "top_share_b": 0.7,
            "n_events_a": 10,
            "n_events_b": 8,
            "computed_at": f"2026-03-{10 + window_day:02d}T00:00:00Z",
        })
    return edges


def _make_finding(headline: str = "Test finding") -> FindingPost:
    return FindingPost(
        headline=headline,
        summary="test summary",
        detail_url="https://labelwatch.neutral.zone/v1/registry",
        card_title="Test",
        card_description="test",
        dedupe_key="abc123",
    )


def _make_conn():
    """Minimal in-memory DB for assessment (no tables needed for assess_finding)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


class TestAssessFinding:
    def test_empty_edges_returns_internal(self):
        a = assess_finding(_make_conn(), "a", "b", [], _make_finding())
        assert a.tier == "internal"
        assert "no edges" in a.reasons

    def test_low_volume_is_internal(self):
        edges = _make_edges(5)  # below MIN_TARGETS_REVIEWABLE
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.tier == "internal"
        assert any("low volume" in r for r in a.reasons)

    def test_high_volume_high_jsd_persistent_is_ready(self):
        edges = _make_edges(
            n=MIN_TARGETS_READY,
            jsd=MIN_MEDIAN_JSD_READY + 0.1,
            n_windows=MIN_WINDOWS_READY,
        )
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.tier == "ready"
        assert any("high volume" in p for p in a.promotions)

    def test_moderate_volume_is_reviewable(self):
        edges = _make_edges(
            n=MIN_TARGETS_REVIEWABLE,
            jsd=MIN_MEDIAN_JSD_REVIEWABLE + 0.1,
            n_windows=MIN_WINDOWS_REVIEWABLE,
        )
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.tier == "reviewable"

    def test_previously_posted_is_internal(self):
        edges = _make_edges(
            n=MIN_TARGETS_READY,
            jsd=MIN_MEDIAN_JSD_READY + 0.1,
            n_windows=MIN_WINDOWS_READY,
        )
        a = assess_finding(
            _make_conn(), "a", "b", edges, _make_finding(),
            previously_posted=True,
        )
        assert a.tier == "internal"
        assert any("previously posted" in r for r in a.reasons)

    def test_weak_jsd_is_internal(self):
        edges = _make_edges(n=30, jsd=0.1)
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.tier == "internal"
        assert any("weak divergence" in r for r in a.reasons)

    def test_unmapped_family_blocks_promotion(self):
        edges = _make_edges(
            n=MIN_TARGETS_READY,
            family_a="spam",
            family_b="totally-unknown-xyz",
            jsd=MIN_MEDIAN_JSD_READY + 0.1,
            n_windows=MIN_WINDOWS_READY,
        )
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        # unmapped family prevents ready tier
        assert a.tier in ("internal", "reviewable")
        assert any("unmapped" in r for r in a.reasons)

    def test_disagreement_type_captured(self):
        edges = _make_edges(n=20, family_a="spam", family_b="harassment")
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.disagreement_type == "taxonomy_shear"

    def test_severity_difference_type(self):
        # nudity is cautionary, spam is negative — same kind (policy_claim), different polarity
        edges = _make_edges(n=20, family_a="spam", family_b="nudity")
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.disagreement_type == "severity_difference"

    def test_claim_vs_action_type(self):
        edges = _make_edges(n=20, family_a="spam", family_b="mod-hide")
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        # mod-hide is protocol_action, spam is policy_claim
        assert a.disagreement_type == "claim_vs_action"

    def test_median_jsd_computed_correctly(self):
        edges = _make_edges(n=3, jsd=0.5)
        edges[0]["jsd"] = 0.1
        edges[2]["jsd"] = 0.9
        # sorted: 0.1, 0.5, 0.9 → median = 0.5
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.median_jsd == 0.5

    def test_n_windows_from_computed_at(self):
        edges = _make_edges(n=20, n_windows=4)
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.n_windows == 4


class TestFormatAssessment:
    def test_format_includes_tier(self):
        edges = _make_edges(n=5)
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        text = format_assessment(a)
        assert "[INTERNAL]" in text

    def test_format_reviewable_includes_draft(self):
        edges = _make_edges(
            n=MIN_TARGETS_REVIEWABLE,
            jsd=MIN_MEDIAN_JSD_REVIEWABLE + 0.1,
            n_windows=MIN_WINDOWS_REVIEWABLE,
        )
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        assert a.tier == "reviewable"
        text = format_assessment(a)
        assert "Draft:" in text

    def test_format_internal_no_draft(self):
        edges = _make_edges(n=5)
        a = assess_finding(_make_conn(), "a", "b", edges, _make_finding())
        text = format_assessment(a)
        assert "Draft:" not in text
