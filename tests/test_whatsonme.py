"""Tests for whatsonme module — account-level label lookup."""
import json
from unittest.mock import patch, MagicMock
import pytest

from labelwatch.whatsonme import (
    compute_label_state,
    resolve_identifier,
    generate_whatsonme,
    _render_whatsonme_html,
)


# ---------------------------------------------------------------------------
# compute_label_state tests
# ---------------------------------------------------------------------------

def test_active_label():
    labels = [{"src": "did:plc:a", "val": "spam", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"}]
    state = compute_label_state(labels)
    assert len(state["active"]) == 1
    assert state["active"][0]["val"] == "spam"
    assert len(state["cleared"]) == 0
    assert len(state["expired"]) == 0


def test_negated_label_is_cleared():
    labels = [
        {"src": "did:plc:a", "val": "spam", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"},
        {"src": "did:plc:a", "val": "spam", "uri": "did:plc:x", "cts": "2025-01-02T00:00:00Z", "neg": True},
    ]
    state = compute_label_state(labels)
    assert len(state["active"]) == 0
    assert len(state["cleared"]) == 1
    assert state["cleared"][0]["val"] == "spam"


def test_expired_label():
    labels = [
        {"src": "did:plc:a", "val": "temp", "uri": "did:plc:x",
         "cts": "2020-01-01T00:00:00Z", "exp": "2020-02-01T00:00:00Z"},
    ]
    state = compute_label_state(labels)
    assert len(state["active"]) == 0
    assert len(state["expired"]) == 1


def test_multiple_sources_same_val():
    labels = [
        {"src": "did:plc:a", "val": "spam", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"},
        {"src": "did:plc:b", "val": "spam", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"},
    ]
    state = compute_label_state(labels)
    assert len(state["active"]) == 2


def test_empty_labels():
    state = compute_label_state([])
    assert state == {"active": [], "cleared": [], "expired": []}


def test_mixed_state():
    """One active, one cleared, one expired."""
    labels = [
        {"src": "did:plc:a", "val": "good", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"},
        {"src": "did:plc:b", "val": "bad", "uri": "did:plc:x", "cts": "2025-01-01T00:00:00Z"},
        {"src": "did:plc:b", "val": "bad", "uri": "did:plc:x", "cts": "2025-01-02T00:00:00Z", "neg": True},
        {"src": "did:plc:c", "val": "temp", "uri": "did:plc:x",
         "cts": "2020-01-01T00:00:00Z", "exp": "2020-02-01T00:00:00Z"},
    ]
    state = compute_label_state(labels)
    assert len(state["active"]) == 1
    assert len(state["cleared"]) == 1
    assert len(state["expired"]) == 1


# ---------------------------------------------------------------------------
# resolve_identifier tests
# ---------------------------------------------------------------------------

def test_resolve_did_passthrough():
    assert resolve_identifier("did:plc:abc123") == "did:plc:abc123"


def test_resolve_did_with_whitespace():
    assert resolve_identifier("  did:plc:abc123  ") == "did:plc:abc123"


@patch("labelwatch.whatsonme.resolve_handle_to_did", return_value="did:plc:resolved")
def test_resolve_handle(mock_resolve):
    assert resolve_identifier("@alice.bsky.social") == "did:plc:resolved"
    mock_resolve.assert_called_once_with("@alice.bsky.social")


@patch("labelwatch.whatsonme.resolve_handle_to_did", return_value=None)
def test_resolve_bad_handle(mock_resolve):
    assert resolve_identifier("@nonexistent.invalid") is None


# ---------------------------------------------------------------------------
# generate_whatsonme tests
# ---------------------------------------------------------------------------

@patch("labelwatch.whatsonme.resolve_handle", return_value=None)
@patch("labelwatch.whatsonme.fetch_profile", return_value={
    "handle": "alice.bsky.social", "displayName": "Alice"
})
@patch("labelwatch.whatsonme.fetch_account_labels", return_value=[
    {"src": "did:plc:lab1", "val": "spam", "uri": "did:plc:alice", "cts": "2025-06-01T00:00:00Z"},
])
def test_generate_whatsonme_with_did(mock_labels, mock_profile, mock_rh):
    payload = generate_whatsonme("did:plc:alice")
    assert payload["did"] == "did:plc:alice"
    assert payload["handle"] == "alice.bsky.social"
    assert payload["total_active"] == 1
    assert len(payload["active_labels"]) == 1
    assert payload["active_labels"][0]["val"] == "spam"


@patch("labelwatch.whatsonme.resolve_handle_to_did", return_value=None)
def test_generate_whatsonme_bad_identifier(mock_resolve):
    payload = generate_whatsonme("@bad.invalid")
    assert payload.get("error") is True


@patch("labelwatch.whatsonme.resolve_handle", return_value=None)
@patch("labelwatch.whatsonme.fetch_profile", return_value={"handle": "bob.bsky.social"})
@patch("labelwatch.whatsonme.fetch_account_labels", return_value=[])
def test_generate_whatsonme_no_labels(mock_labels, mock_profile, mock_rh):
    payload = generate_whatsonme("did:plc:bob")
    assert payload["total_active"] == 0
    assert payload["active_labels"] == []


# ---------------------------------------------------------------------------
# HTML rendering tests
# ---------------------------------------------------------------------------

def test_render_error_html():
    payload = {"error": True, "message": "Could not resolve"}
    html = _render_whatsonme_html(payload)
    assert "Could not resolve" in html


def test_render_active_html():
    payload = {
        "did": "did:plc:test",
        "handle": "test.bsky.social",
        "display_name": "Test User",
        "avatar": None,
        "active_labels": [
            {"val": "spam", "src": "did:plc:lab1", "cts": "2025-01-01T00:00:00Z"},
        ],
        "cleared_labels": [],
        "expired_labels": [],
        "total_active": 1,
        "total_sources": 1,
        "sources": [{"did": "did:plc:lab1", "handle": None}],
        "raw_events": [
            {"val": "spam", "src": "did:plc:lab1", "cts": "2025-01-01T00:00:00Z",
             "uri": "did:plc:test"},
        ],
        "generated_at": "2025-01-02T00:00:00Z",
    }
    html = _render_whatsonme_html(payload)
    assert "Active Now" in html
    assert "spam" in html
    assert "test.bsky.social" in html


def test_render_empty_state_html():
    payload = {
        "did": "did:plc:empty",
        "handle": "empty.bsky.social",
        "display_name": None,
        "avatar": None,
        "active_labels": [],
        "cleared_labels": [],
        "expired_labels": [],
        "total_active": 0,
        "total_sources": 0,
        "sources": [],
        "raw_events": [],
        "generated_at": "2025-01-02T00:00:00Z",
    }
    html = _render_whatsonme_html(payload)
    assert "No active account labels" in html
