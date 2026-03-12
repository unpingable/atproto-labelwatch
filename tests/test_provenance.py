"""Tests for provenance scorecard."""
from labelwatch.provenance import (
    BskyClient,
    CreatorProfile,
    DidServiceInfo,
    EvidenceClass,
    GovernanceMode,
    LabelValueDefinition,
    LabelerPolicy,
    LabelerServiceRecord,
    ObservedMetrics,
    OperatorLegibility,
    build_provenance_snapshot,
    derive_governance_mode,
    derive_observed_metrics,
    derive_operator_legibility,
)
from labelwatch import db


def _make_service(**overrides):
    defaults = dict(
        did="did:plc:testlabeler123",
        uri="at://did:plc:testlabeler123/app.bsky.labeler.service/self",
        cid="bafytest",
        creator=CreatorProfile(
            did="did:plc:testlabeler123",
            handle="test-labeler.bsky.social",
            display_name="Test Labeler",
            description="A moderation service",
            avatar=None,
        ),
        policies=LabelerPolicy(
            label_values=["spam", "scam", "custom-risk"],
            label_value_definitions=[
                LabelValueDefinition(
                    identifier="custom-risk",
                    severity="alert", blurs="none",
                    default_setting="warn",
                    locales=[{"lang": "en", "name": "Custom Risk",
                              "description": "Example risk label"}],
                )
            ],
        ),
        indexed_at="2026-03-12T00:00:00Z",
        reason_types=["com.atproto.moderation.defs#reasonSpam"],
        subject_types=["account", "record"],
        subject_collections=["app.bsky.feed.post"],
    )
    defaults.update(overrides)
    return LabelerServiceRecord(**defaults)


def _make_did_info(**overrides):
    defaults = dict(
        endpoint="https://labeler.example.com",
        signing_key="zQ3shokFTS3brHcDQrn82RUDfCZESWL1Z",
        raw_doc={"service": []},
    )
    defaults.update(overrides)
    return DidServiceInfo(**defaults)


def _make_observed(**overrides):
    defaults = dict(
        total_labels_emitted=1000,
        active_days=30,
        scope_adherence=0.9,
    )
    defaults.update(overrides)
    return ObservedMetrics(**defaults)


# ---------------------------------------------------------------------------
# Snapshot build
# ---------------------------------------------------------------------------

def test_snapshot_smoke():
    snap = build_provenance_snapshot(
        _make_service(), _make_did_info(), _make_observed(),
    )
    assert snap.did == "did:plc:testlabeler123"
    assert snap.scores.total > 0
    assert snap.did_service_endpoint == "https://labeler.example.com"
    assert snap.governance_mode == GovernanceMode.MODERATION
    assert "did-endpoint-missing-or-unreachable" not in snap.red_flags


def test_snapshot_to_dict():
    snap = build_provenance_snapshot(
        _make_service(), _make_did_info(), _make_observed(),
    )
    d = snap.to_dict()
    assert isinstance(d["operator_legibility"], str)
    assert isinstance(d["governance_mode"], str)
    assert d["scores"]["identity"] > 0


def test_snapshot_no_endpoint():
    snap = build_provenance_snapshot(
        _make_service(),
        _make_did_info(endpoint=None, signing_key=None, raw_doc=None),
        _make_observed(),
    )
    assert "did-endpoint-missing-or-unreachable" in snap.red_flags
    assert snap.scores.infrastructure < 10


def test_snapshot_empty_labeler():
    """Labeler with nothing declared."""
    service = _make_service(
        creator=CreatorProfile("did:plc:empty", None, None, None, None),
        policies=LabelerPolicy([], []),
        reason_types=[], subject_types=[], subject_collections=[],
    )
    snap = build_provenance_snapshot(
        service,
        _make_did_info(endpoint=None, signing_key=None, raw_doc=None),
        ObservedMetrics(),
    )
    assert snap.scores.total < 30
    assert len(snap.red_flags) > 3


# ---------------------------------------------------------------------------
# Governance mode
# ---------------------------------------------------------------------------

def test_governance_moderation():
    mode = derive_governance_mode(
        _make_service(), _make_observed(),
    )
    assert mode == GovernanceMode.MODERATION


def test_governance_badge():
    mode = derive_governance_mode(
        _make_service(policies=LabelerPolicy(["custom-badge"], [])),
        _make_observed(badge_ratio=0.85),
    )
    assert mode == GovernanceMode.BADGE_STATUS


def test_governance_novelty():
    mode = derive_governance_mode(
        _make_service(policies=LabelerPolicy(["fun"], [])),
        _make_observed(novelty_ratio=0.9, scope_adherence=0.3),
    )
    assert mode == GovernanceMode.NOVELTY


def test_governance_unknown():
    mode = derive_governance_mode(
        _make_service(policies=LabelerPolicy([], [])),
        ObservedMetrics(),
    )
    assert mode == GovernanceMode.UNKNOWN


# ---------------------------------------------------------------------------
# Operator legibility
# ---------------------------------------------------------------------------

def test_legibility_legible():
    leg = derive_operator_legibility(
        _make_service(), _make_did_info(),
        _make_observed(docs_url_present=True, appeals_or_contact_present=True),
    )
    assert leg == OperatorLegibility.LEGIBLE


def test_legibility_opaque():
    service = _make_service(
        creator=CreatorProfile("did:plc:x", None, None, None, None),
    )
    leg = derive_operator_legibility(
        service, _make_did_info(endpoint=None), ObservedMetrics(),
    )
    assert leg == OperatorLegibility.OPAQUE


# ---------------------------------------------------------------------------
# SQLite adapter
# ---------------------------------------------------------------------------

def test_derive_observed_metrics():
    conn = db.connect(":memory:")
    db.init_db(conn)

    did = "did:plc:testlabeler"
    db.upsert_labeler(conn, did, "2026-01-01T00:00:00Z")

    # Insert some label events
    for i in range(10):
        conn.execute(
            "INSERT INTO label_events "
            "(labeler_did, src, uri, cid, val, neg, ts, event_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (did, did, f"at://did:plc:user/app.bsky.feed.post/{i}",
             f"cid{i}", "spam", 0, f"2026-01-0{i+1}T00:00:00Z", f"hash{i}"),
        )
    # Add one negation
    conn.execute(
        "INSERT INTO label_events "
        "(labeler_did, src, uri, cid, val, neg, ts, event_hash) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (did, did, "at://did:plc:user/app.bsky.feed.post/0",
         "cidneg", "spam", 1, "2026-01-11T00:00:00Z", "hashneg"),
    )
    # Add one account-level label
    conn.execute(
        "INSERT INTO label_events "
        "(labeler_did, src, uri, cid, val, neg, ts, event_hash) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (did, did, "did:plc:targetuser", "cidacct", "cool", 0,
         "2026-01-12T00:00:00Z", "hashacct"),
    )
    conn.commit()

    metrics = derive_observed_metrics(conn, did)
    assert metrics.total_labels_emitted == 12
    assert metrics.active_days > 0
    assert metrics.negation_rate > 0
    assert metrics.account_label_ratio > 0
    assert metrics.record_label_ratio > 0


def test_derive_observed_metrics_empty():
    conn = db.connect(":memory:")
    db.init_db(conn)
    metrics = derive_observed_metrics(conn, "did:plc:nobody")
    assert metrics.total_labels_emitted == 0
    assert metrics.active_days == 0
