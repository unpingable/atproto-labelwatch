"""Tests for the pure classifier: visibility, reachability, auditability, confidence."""
from labelwatch.classify import (
    CLASSIFIER_VERSION,
    Classification,
    EvidenceDict,
    classify_labeler,
    detect_test_dev,
)


# --- Visibility class: declared ---

def test_declared_accessible():
    ev = EvidenceDict(declared_record_present=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert c.reachability_state == "accessible"
    assert c.auditability == "high"
    assert "declared" in c.reason
    assert "probe_accessible" in c.reason


def test_declared_auth_required():
    ev = EvidenceDict(declared_record_present=True, probe_result="auth_required")
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert c.reachability_state == "auth_required"
    assert c.auditability == "medium"


def test_declared_down():
    ev = EvidenceDict(declared_record_present=True, probe_result="down")
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert c.reachability_state == "down"
    assert c.auditability == "medium"
    assert "probe_down" in c.reason


def test_declared_not_probed():
    ev = EvidenceDict(declared_record_present=True, probe_result=None)
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert c.reachability_state == "unknown"
    assert c.auditability == "medium"
    assert "not_probed" in c.reason


def test_declared_with_did_service():
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_labeler_service_present=True,
        probe_result="accessible",
    )
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert "did_service" in c.reason


def test_declared_with_label_key():
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_label_key_present=True,
        probe_result="accessible",
    )
    c = classify_labeler(ev)
    assert "did_label_key" in c.reason


def test_declared_full_evidence():
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_labeler_service_present=True,
        did_doc_label_key_present=True,
        observed_label_src=True,
        probe_result="accessible",
    )
    c = classify_labeler(ev)
    assert c.visibility_class == "declared"
    assert c.reachability_state == "accessible"
    assert c.auditability == "high"
    assert c.classification_confidence == "high"
    assert "observed_src" in c.reason


# --- Visibility class: protocol_public ---

def test_protocol_public_accessible():
    ev = EvidenceDict(did_doc_labeler_service_present=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.visibility_class == "protocol_public"
    assert c.reachability_state == "accessible"
    assert c.auditability == "medium"
    assert "protocol_public" in c.reason


def test_protocol_public_down():
    ev = EvidenceDict(did_doc_labeler_service_present=True, probe_result="down")
    c = classify_labeler(ev)
    assert c.visibility_class == "protocol_public"
    assert c.reachability_state == "down"
    assert c.auditability == "medium"


def test_protocol_public_with_label_key():
    ev = EvidenceDict(
        did_doc_labeler_service_present=True,
        did_doc_label_key_present=True,
        probe_result="accessible",
    )
    c = classify_labeler(ev)
    assert c.visibility_class == "protocol_public"
    assert "did_label_key" in c.reason


def test_protocol_public_not_probed():
    ev = EvidenceDict(did_doc_labeler_service_present=True, probe_result=None)
    c = classify_labeler(ev)
    assert c.visibility_class == "protocol_public"
    assert c.reachability_state == "unknown"


# --- Visibility class: observed_only ---

def test_observed_only():
    ev = EvidenceDict(observed_label_src=True)
    c = classify_labeler(ev)
    assert c.visibility_class == "observed_only"
    assert c.reachability_state == "unknown"
    assert c.auditability == "low"
    assert "observed_only_no_declaration" in c.reason


def test_observed_only_with_probe_down():
    """Even with a probe result, if no declaration, still observed_only."""
    ev = EvidenceDict(observed_label_src=True, probe_result="down")
    c = classify_labeler(ev)
    assert c.visibility_class == "observed_only"
    assert c.auditability == "low"


# --- Visibility class: unresolved ---

def test_unresolved_no_evidence():
    ev = EvidenceDict()
    c = classify_labeler(ev)
    assert c.visibility_class == "unresolved"
    assert c.reachability_state == "unknown"
    assert c.auditability == "low"
    assert "unresolved" in c.reason


def test_unresolved_label_key_only():
    """Label key alone without service doesn't make it protocol_public."""
    ev = EvidenceDict(did_doc_label_key_present=True)
    c = classify_labeler(ev)
    assert c.visibility_class == "unresolved"


# --- Reachability states ---

def test_reachability_accessible():
    ev = EvidenceDict(declared_record_present=True, probe_result="accessible")
    assert classify_labeler(ev).reachability_state == "accessible"


def test_reachability_auth_required():
    ev = EvidenceDict(declared_record_present=True, probe_result="auth_required")
    assert classify_labeler(ev).reachability_state == "auth_required"


def test_reachability_down():
    ev = EvidenceDict(declared_record_present=True, probe_result="down")
    assert classify_labeler(ev).reachability_state == "down"


def test_reachability_unknown_none():
    ev = EvidenceDict(declared_record_present=True, probe_result=None)
    assert classify_labeler(ev).reachability_state == "unknown"


# --- Confidence weighting ---

def test_confidence_high_two_strong():
    """Two strong evidence sources → high."""
    ev = EvidenceDict(observed_label_src=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.classification_confidence == "high"


def test_confidence_high_strong_plus_two_medium():
    """One strong + two medium → high."""
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_labeler_service_present=True,
        probe_result="accessible",
    )
    c = classify_labeler(ev)
    assert c.classification_confidence == "high"


def test_confidence_medium_strong_plus_medium():
    """One strong + one medium → medium."""
    ev = EvidenceDict(declared_record_present=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.classification_confidence == "medium"


def test_confidence_medium_two_medium():
    """Two medium → medium."""
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_labeler_service_present=True,
    )
    c = classify_labeler(ev)
    assert c.classification_confidence == "medium"


def test_confidence_low_single_medium():
    """Single medium → low."""
    ev = EvidenceDict(declared_record_present=True)
    c = classify_labeler(ev)
    assert c.classification_confidence == "low"


def test_confidence_low_no_evidence():
    ev = EvidenceDict()
    c = classify_labeler(ev)
    assert c.classification_confidence == "low"


def test_confidence_low_single_strong():
    """Single strong (observed_src with nothing else) → low."""
    ev = EvidenceDict(observed_label_src=True)
    c = classify_labeler(ev)
    assert c.classification_confidence == "low"


# --- Version ---

def test_classification_version():
    ev = EvidenceDict()
    c = classify_labeler(ev)
    assert c.version == CLASSIFIER_VERSION


# --- Reason strings ---

def test_reason_declared_probe_accessible():
    ev = EvidenceDict(declared_record_present=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.reason == "declared+probe_accessible"


def test_reason_observed_only():
    ev = EvidenceDict(observed_label_src=True)
    c = classify_labeler(ev)
    assert c.reason == "observed_only_no_declaration"


def test_reason_declared_did_service_probe_down():
    ev = EvidenceDict(
        declared_record_present=True,
        did_doc_labeler_service_present=True,
        probe_result="down",
    )
    c = classify_labeler(ev)
    assert c.reason == "declared+did_service+probe_down"


def test_reason_protocol_public_probe_accessible():
    ev = EvidenceDict(did_doc_labeler_service_present=True, probe_result="accessible")
    c = classify_labeler(ev)
    assert c.reason == "protocol_public+probe_accessible"


# --- Noise detection ---

def test_detect_test_dev_handle_test():
    assert detect_test_dev("test.bsky.social", None) is True
    assert detect_test_dev("test-labeler.bsky.social", None) is True
    assert detect_test_dev("my-test.bsky.social", None) is True


def test_detect_test_dev_handle_dev():
    assert detect_test_dev("dev.bsky.social", None) is True
    assert detect_test_dev("dev-labeler.bsky.social", None) is True


def test_detect_test_dev_display_name():
    assert detect_test_dev(None, "Test Labeler") is True
    assert detect_test_dev(None, "My Dev Labels") is True
    assert detect_test_dev(None, "Demo Service") is True
    assert detect_test_dev(None, "Sandbox Labels") is True


def test_detect_test_dev_example():
    assert detect_test_dev("example.bsky.social", None) is True
    assert detect_test_dev(None, "Example Labeler") is True


def test_detect_test_dev_false_for_normal():
    assert detect_test_dev("moderation.bsky.app", None) is False
    assert detect_test_dev("community-labels.bsky.social", None) is False
    assert detect_test_dev(None, "Bluesky Moderation") is False
    assert detect_test_dev(None, "Anti-Spam Labels") is False


def test_detect_test_dev_none_inputs():
    assert detect_test_dev(None, None) is False


def test_detect_test_dev_foo_bar():
    assert detect_test_dev("foo.bsky.social", None) is True
    assert detect_test_dev(None, "bar test") is True
