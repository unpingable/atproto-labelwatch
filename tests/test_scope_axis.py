"""Tests for labelwatch.scope_presentation.v0 (scope-axis).

Covers the six acceptance criteria in specs/gaps/gap-spec-scope-axis-v0.md.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from labelwatch import db, scope_axis
from labelwatch.utils import format_ts

NOW = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
IN_WINDOW = format_ts(NOW - timedelta(days=1))
OUT_OF_WINDOW = format_ts(NOW - timedelta(days=30))


@pytest.fixture()
def conn():
    c = db.connect(":memory:")
    db.init_db(c)
    yield c
    c.close()


_counter = {"n": 0}


def _add_events(c, labeler, val, count, *, ts=IN_WINDOW, neg=0):
    for _ in range(count):
        _counter["n"] += 1
        c.execute(
            "INSERT INTO label_events (labeler_did, uri, val, neg, ts, event_hash) "
            "VALUES (?,?,?,?,?,?)",
            (labeler, "at://post", val, neg, ts, f"h{_counter['n']}"),
        )
    c.commit()


def _add_service_record(c, labeler, defs, *, discovered_at=IN_WINDOW):
    rec = {"policies": {"labelValueDefinitions": defs}}
    c.execute(
        "INSERT INTO discovery_events (labeler_did, operation, source, record_json, discovered_at) "
        "VALUES (?,?,?,?,?)",
        (labeler, "create", "test", json.dumps(rec), discovered_at),
    )
    c.commit()


def _def(identifier, default_setting=None, severity="inform", blurs="none"):
    d = {"identifier": identifier, "severity": severity, "blurs": blurs}
    if default_setting is not None:
        d["defaultSetting"] = default_setting
    return d


# --- Criterion 1: pure band mapping --------------------------------------

def test_scope_band_mapping():
    assert scope_axis.scope_band({"defaultSetting": "hide"}) == scope_axis.BAND_VERDICT
    assert scope_axis.scope_band({"defaultSetting": "warn"}) == scope_axis.BAND_NUDGE
    assert scope_axis.scope_band({"defaultSetting": "ignore"}) == scope_axis.BAND_WEATHER
    # def present but defaultSetting omitted -> weather (explicit-only convention)
    assert scope_axis.scope_band({"identifier": "x"}) == scope_axis.BAND_WEATHER
    # no definition -> ungraded (warrant-gap), never weather
    assert scope_axis.scope_band(None) == scope_axis.BAND_UNGRADED
    assert scope_axis.scope_band({}) == scope_axis.BAND_UNGRADED


# --- Criterion 4: classify_cell carries verbatim cited metadata ----------

def test_classify_cell_evidence_is_verbatim():
    d = _def("spam", default_setting="hide", severity="alert", blurs="content")
    result = scope_axis.classify_cell("did:plc:a", "spam", d)
    assert result["band"] == scope_axis.BAND_VERDICT
    ev = result["evidence"]
    assert ev["defaultSetting"] == "hide"
    assert ev["severity"] == "alert"
    assert ev["blurs"] == "content"
    assert ev["definition_present"] is True
    # ungraded cell still carries provenance shape, with nulls
    u = scope_axis.classify_cell("did:plc:a", "mystery", None)
    assert u["band"] == scope_axis.BAND_UNGRADED
    assert u["evidence"]["defaultSetting"] is None
    assert u["evidence"]["definition_present"] is False


# --- Criterion 2: ungraded is never summed into weather ------------------

def test_ungraded_does_not_move_weather(conn):
    # 'breeze' is declared weather (ignore); 'mystery' has no definition.
    _add_service_record(conn, "did:plc:a", [_def("breeze", default_setting="ignore")])
    _add_events(conn, "did:plc:a", "breeze", 10)     # graded weather
    _add_events(conn, "did:plc:a", "mystery", 7)     # ungraded warrant-gap

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    em = m["emission"]
    assert em["by_band"][scope_axis.BAND_WEATHER] == 10
    assert em["ungraded_events"] == 7
    # the ungraded volume is its own bucket, not folded into weather
    assert em["graded_events"] == 10
    assert em["active_label_events"] == 17


# --- Criterion 5: bang-labels are deferred, not banded -------------------

def test_bang_labels_go_to_protocol_reserved(conn):
    _add_service_record(conn, "did:plc:a", [_def("breeze", default_setting="ignore")])
    _add_events(conn, "did:plc:a", "breeze", 5)   # graded weather
    _add_events(conn, "did:plc:a", "!hide", 9)    # protocol-reserved, deferred

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    em = m["emission"]
    assert em["protocol_reserved_deferred_events"] == 9
    # not in any band, not ungraded
    assert em["by_band"][scope_axis.BAND_WEATHER] == 5
    assert em["ungraded_events"] == 0
    assert em["distinct_cells"]["protocol_reserved_deferred"] == 1


# --- Criterion 3: no subject / target_did in output ----------------------

def test_output_has_no_target_did(conn):
    _add_service_record(conn, "did:plc:a", [_def("spam", default_setting="hide")])
    _add_events(conn, "did:plc:a", "spam", 3)
    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    serialized = json.dumps(m)
    assert "target_did" not in serialized
    assert "target" not in serialized  # aggregate-first: no subject field at all


# --- Criterion 6: coverage canary suppresses the headline ----------------

def test_coverage_canary_suppresses_headline(conn):
    # graded=4, ungraded=20 -> coverage 0.167 < floor 0.5 -> suppressed
    _add_service_record(conn, "did:plc:a", [_def("spam", default_setting="hide")])
    _add_events(conn, "did:plc:a", "spam", 4)        # graded verdict
    _add_events(conn, "did:plc:a", "mystery", 20)    # ungraded

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    em = m["emission"]
    assert em["graded_coverage"] < scope_axis.COVERAGE_FLOOR
    assert em["verdict_scope_share_suppressed"] is True
    assert em["verdict_scope_share"] is None          # not published bare
    assert em["verdict_scope_share_raw"] == pytest.approx(1.0)  # raw preserved for audit


def test_headline_published_when_coverage_ok(conn):
    # graded=8 (4 verdict + 4 weather), ungraded=1 -> coverage 0.89 -> published
    _add_service_record(
        conn, "did:plc:a",
        [_def("spam", default_setting="hide"), _def("breeze", default_setting="ignore")],
    )
    _add_events(conn, "did:plc:a", "spam", 4)
    _add_events(conn, "did:plc:a", "breeze", 4)
    _add_events(conn, "did:plc:a", "mystery", 1)

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    em = m["emission"]
    assert em["verdict_scope_share_suppressed"] is False
    assert em["verdict_scope_share"] == pytest.approx(0.5)  # 4 verdict / 8 graded


# --- window + negation hygiene (freshness belongs elsewhere) -------------

def test_window_and_negation_excluded(conn):
    _add_service_record(conn, "did:plc:a", [_def("spam", default_setting="hide")])
    _add_events(conn, "did:plc:a", "spam", 3)                       # in-window, active
    _add_events(conn, "did:plc:a", "spam", 5, ts=OUT_OF_WINDOW)     # too old
    _add_events(conn, "did:plc:a", "spam", 4, neg=1)               # negation (freshness-axis)

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    assert m["emission"]["by_band"][scope_axis.BAND_VERDICT] == 3


# --- declaration cut counts distinct published values --------------------

def test_declaration_cut(conn):
    _add_service_record(
        conn, "did:plc:a",
        [
            _def("spam", default_setting="hide"),
            _def("warnme", default_setting="warn"),
            _def("breeze", default_setting="ignore"),
            _def("noset"),  # omitted -> weather
        ],
    )
    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    dec = m["declaration"]
    assert dec["defined_label_values"] == 4
    assert dec["by_band"][scope_axis.BAND_VERDICT] == 1
    assert dec["by_band"][scope_axis.BAND_NUDGE] == 1
    assert dec["by_band"][scope_axis.BAND_WEATHER] == 2
    assert dec["verdict_scope_share"] == pytest.approx(0.25)


def test_assumptions_block_and_omitted_sensitivity(conn):
    # 'noset' omits defaultSetting (-> weather, counted as omitted);
    # 'breeze' is explicit ignore (-> weather, NOT omitted).
    _add_service_record(
        conn, "did:plc:a",
        [_def("noset"), _def("breeze", default_setting="ignore")],
    )
    _add_events(conn, "did:plc:a", "noset", 6)
    _add_events(conn, "did:plc:a", "breeze", 4)

    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    # assumption is obnoxiously visible
    assert m["assumptions"]["default_setting_omitted"] == "weather_scope_explicit_only"
    assert m["assumptions"]["not_client_behavior_simulation"] is True
    # omitted population is observable on both cuts...
    assert m["declaration"]["default_setting_omitted"] == 1            # only 'noset'
    assert m["emission"]["weather_from_omitted_default_events"] == 6   # only 'noset' volume
    # ...without changing band semantics: both are still weather
    assert m["emission"]["by_band"][scope_axis.BAND_WEATHER] == 10
    assert m["declaration"]["by_band"][scope_axis.BAND_WEATHER] == 2


def test_renderers_run(conn):
    _add_service_record(conn, "did:plc:a", [_def("spam", default_setting="hide")])
    _add_events(conn, "did:plc:a", "spam", 3)
    m = scope_axis.compute_scope_presentation(conn, window_days=7, now=NOW)
    text = scope_axis.render_text(m)
    assert "verdict-scope" in text
    html = scope_axis.render_html_figure(m)
    assert "<table>" in html and "verdict-scope" in html
