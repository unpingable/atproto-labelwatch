"""Failure to observe is not observation of failure.

Before this repair, a total ingest outage produced no alerts, and
`network_weather` read "no alerts" as "no triggers crossed" and reported
`calm`. The coverage watermark was wired to *suppress* positive findings
(`rules._build_coverage_cache` -> every detection rule skips an insufficient
labeler) but nothing was wired to *qualify* the negative one, so degraded
observation manufactured the very reading it should have blocked.

These tests pin the law:

    A substantive negative finding may be emitted only when the observation
    window has standing to support it.

and its converse, which matters just as much:

    The repair must not make `calm` unreachable. Adequately observed quiet is
    a real, reportable finding.
"""
from __future__ import annotations

import datetime as dt
import html as html_lib
import inspect
import json
import os
import re
import tempfile

import pytest

from labelwatch import db, frontdoor, report
from labelwatch.config import Config
from labelwatch.label_family import FAMILY_VERSION
from labelwatch.utils import format_ts, now_utc

NOW = dt.datetime(2026, 8, 28, 12, 0, 0, tzinfo=dt.timezone.utc)
CFG = Config()


def _conn():
    path = os.path.join(tempfile.mkdtemp(), "lw.db")
    conn = db.connect(path)
    db.init_db(conn)
    return conn


def _labelers(conn, n, *, endpoint_status="accessible", events_7d=500):
    for i in range(n):
        conn.execute(
            "INSERT INTO labelers(labeler_did, first_seen, last_seen, events_7d,"
            " events_30d, endpoint_status) VALUES (?,?,?,?,?,?)",
            (f"did:plc:lab{i:03d}", format_ts(NOW - dt.timedelta(days=60)),
             format_ts(NOW - dt.timedelta(hours=1)), events_7d, 3000,
             endpoint_status))
    conn.commit()


def _polls(conn, did, *, ok, bad, window_minutes=None):
    """Record `ok` successful and `bad` failed attempts inside the window."""
    span = window_minutes or CFG.coverage_window_minutes
    step = span / max(ok + bad + 1, 2)
    n = 0
    for outcome, count in (("success", ok), ("error", bad)):
        for _ in range(count):
            n += 1
            conn.execute(
                "INSERT INTO ingest_outcomes(labeler_did, ts, attempt_id, outcome)"
                " VALUES (?,?,?,?)",
                (did, format_ts(NOW - dt.timedelta(minutes=step * n)),
                 f"{did}-{outcome}-{n}", outcome))
    conn.commit()


# ---------------------------------------------------------------------------
# 1. Total outage cannot be calm
# ---------------------------------------------------------------------------

def test_total_outage_is_not_calm():
    """The Murex counterexample: every poll failed, so no alert fired, so the
    old code reported calm. Expected polls > 0, successful polls == 0."""
    conn = _conn()
    _labelers(conn, 12)
    for i in range(12):
        _polls(conn, f"did:plc:lab{i:03d}", ok=0, bad=48)

    w = frontdoor.network_weather(conn, now=NOW)

    assert "calm" not in w["signals"]
    assert w["signals"] == ["unobserved"]
    assert w["observation"]["adequacy"] == "unobserved"
    assert w["observation"]["successes"] == 0
    assert w["observation"]["attempts"] == 576
    assert not frontdoor.supports_negative_claim(w["observation"])
    # The attribution must say what went wrong, not "no triggers crossed".
    assert "no triggers crossed" not in w["attribution"]
    assert "0 of 576" in w["attribution"]


def test_no_attempts_at_all_is_not_calm():
    """Nothing even tried to observe. That is not quiet either."""
    conn = _conn()
    _labelers(conn, 5)

    w = frontdoor.network_weather(conn, now=NOW)

    assert "calm" not in w["signals"]
    assert w["observation"]["adequacy"] == "unobserved"
    assert w["observation"]["attempts"] == 0
    assert "no ingest attempts" in w["attribution"]


# ---------------------------------------------------------------------------
# 2. Partial observation preserves the limitation
# ---------------------------------------------------------------------------

def test_partial_coverage_is_not_calm():
    """Degraded but nonzero coverage. Something was seen; "nothing happened"
    still is not established."""
    conn = _conn()
    _labelers(conn, 4)
    _polls(conn, "did:plc:lab000", ok=10, bad=0)
    _polls(conn, "did:plc:lab001", ok=10, bad=0)
    _polls(conn, "did:plc:lab002", ok=1, bad=9)    # below threshold
    _polls(conn, "did:plc:lab003", ok=0, bad=10)   # dead

    w = frontdoor.network_weather(conn, now=NOW)

    assert "calm" not in w["signals"]
    assert w["signals"] == ["under-observed"]
    obs = w["observation"]
    assert obs["adequacy"] == "partial"
    assert obs["labelers_attempted"] == 4
    assert obs["labelers_adequate"] == 2
    assert obs["labelers_degraded"] == 2
    assert obs["successes"] > 0


def test_partial_coverage_does_not_suppress_observed_triggers():
    """Positive findings ride on observed evidence and must survive. What is
    withheld is only the claim that nothing *else* happened."""
    conn = _conn()
    _labelers(conn, 2)
    _polls(conn, "did:plc:lab000", ok=10, bad=0)
    _polls(conn, "did:plc:lab001", ok=0, bad=10)
    for i in range(11):
        conn.execute(
            "INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json,"
            " evidence_hashes_json, config_hash, receipt_hash)"
            " VALUES ('label_rate_spike','did:plc:lab000',?,'{}','[]','c','r')",
            (format_ts(NOW - dt.timedelta(hours=1)),))
    conn.commit()

    w = frontdoor.network_weather(conn, now=NOW)

    assert "noisy" in w["signals"]          # observed evidence stands
    assert "under-observed" in w["signals"]  # and carries its limit
    assert "calm" not in w["signals"]


def test_accessible_but_unpolled_labeler_removes_standing():
    """An accessible labeler the instrument should have polled and did not is
    unobserved, the same as one whose polls all failed."""
    conn = _conn()
    _labelers(conn, 3)
    _polls(conn, "did:plc:lab000", ok=10, bad=0)
    _polls(conn, "did:plc:lab001", ok=10, bad=0)
    # lab002 is accessible but has no ingest_outcomes rows at all.

    w = frontdoor.network_weather(conn, now=NOW)

    assert "calm" not in w["signals"]
    assert w["observation"]["adequacy"] == "partial"
    assert w["observation"]["labelers_unattempted"] == 1


# ---------------------------------------------------------------------------
# 3. Adequate observation + no triggers still yields calm
# ---------------------------------------------------------------------------

def test_adequate_observation_and_no_triggers_is_calm():
    """The repair must not make calm unreachable."""
    conn = _conn()
    _labelers(conn, 6)
    for i in range(6):
        _polls(conn, f"did:plc:lab{i:03d}", ok=10, bad=0)

    w = frontdoor.network_weather(conn, now=NOW)

    assert w["signals"] == ["calm"]
    assert w["attribution"] == "no triggers crossed"
    assert w["observation"]["adequacy"] == "adequate"
    assert frontdoor.supports_negative_claim(w["observation"])


def test_calm_survives_coverage_exactly_at_the_threshold():
    """The cut is the existing `coverage_threshold`, inclusive, as
    `rules._build_coverage_cache` applies it. No new threshold is introduced."""
    conn = _conn()
    _labelers(conn, 2)
    for i in range(2):
        _polls(conn, f"did:plc:lab{i:03d}", ok=5, bad=5)   # ratio == 0.5

    assert CFG.coverage_threshold == 0.5
    w = frontdoor.network_weather(conn, now=NOW)
    assert w["signals"] == ["calm"]
    assert w["observation"]["adequacy"] == "adequate"


def test_empty_polls_count_as_observation():
    """`empty` means "we asked and there was nothing", which is observed quiet
    — the distinction the whole repair rests on. It must not read as failure."""
    conn = _conn()
    _labelers(conn, 2)
    for i in range(2):
        did = f"did:plc:lab{i:03d}"
        for n in range(10):
            conn.execute(
                "INSERT INTO ingest_outcomes(labeler_did, ts, attempt_id, outcome)"
                " VALUES (?,?,?,'empty')",
                (did, format_ts(NOW - dt.timedelta(minutes=n)), f"{did}-{n}"))
    conn.commit()

    w = frontdoor.network_weather(conn, now=NOW)
    assert w["signals"] == ["calm"]
    assert w["observation"]["successes"] == 20


# ---------------------------------------------------------------------------
# 4. Existing trigger semantics are unchanged
# ---------------------------------------------------------------------------

def test_triggers_retain_their_semantics_under_adequate_observation():
    conn = _conn()
    _labelers(conn, 2)
    for i in range(2):
        _polls(conn, f"did:plc:lab{i:03d}", ok=10, bad=0)
    for i in range(51):
        conn.execute(
            "INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json,"
            " evidence_hashes_json, config_hash, receipt_hash)"
            " VALUES ('churn_index','did:plc:lab000',?,'{}','[]','c','r')",
            (format_ts(NOW - dt.timedelta(hours=2)),))
    conn.commit()

    w = frontdoor.network_weather(conn, now=NOW)
    assert w["signals"] == ["churny"]
    assert "calm" not in w["signals"]
    assert "under-observed" not in w["signals"]


# ---------------------------------------------------------------------------
# 5. Staleness
# ---------------------------------------------------------------------------

def test_calm_does_not_remain_current_once_its_evidence_goes_stale():
    """A window that supported calm must stop supporting it as its evidence
    ages out of the coverage window. The same database, read later, must not
    keep answering with the older window's standing."""
    conn = _conn()
    _labelers(conn, 3)
    for i in range(3):
        _polls(conn, f"did:plc:lab{i:03d}", ok=10, bad=0)

    fresh = frontdoor.network_weather(conn, now=NOW)
    assert fresh["signals"] == ["calm"]

    later = NOW + dt.timedelta(minutes=CFG.coverage_window_minutes + 5)
    stale = frontdoor.network_weather(conn, now=later)

    assert "calm" not in stale["signals"]
    assert stale["observation"]["adequacy"] == "unobserved"
    assert stale["observation"]["attempts"] == 0
    # The last success is still reported as a fact — it is simply no longer
    # current enough to license a negative claim.
    assert stale["observation"]["last_success_ts"] is not None


# ---------------------------------------------------------------------------
# 6. Machine / human parity
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("outage", [True, False])
def test_overview_json_and_page_agree_about_adequacy(outage):
    """For equivalent source state the artifact and the rendered page must not
    disagree about whether the evidence was adequate."""
    conn = _conn()
    _labelers(conn, 3)
    for i in range(3):
        _polls(conn, f"did:plc:lab{i:03d}",
               ok=0 if outage else 10, bad=10 if outage else 0)

    out = tempfile.mkdtemp()
    report.generate_report(conn, out, now=NOW, config=CFG)

    with open(os.path.join(out, "overview.json"), encoding="utf-8") as fh:
        overview = json.load(fh)
    with open(os.path.join(out, "index.html"), encoding="utf-8") as fh:
        page = fh.read()

    assert "observation" in overview, "overview.json must carry observation quality"
    assert "network_weather" in overview

    verdict = overview["network_weather"]
    assert verdict["supports_negative_claim"] is (not outage)
    if outage:
        assert "calm" not in verdict["signals"]
        assert overview["observation"]["adequacy"] == "unobserved"
    else:
        assert "calm" in verdict["signals"]
        assert overview["observation"]["adequacy"] == "adequate"

    # The page must not say calm when the artifact says the window had no
    # standing, nor the reverse.
    page_says_calm = "calm" in page.lower()
    assert page_says_calm is bool(verdict["supports_negative_claim"]), (
        "rendered page and overview.json disagree about observation standing"
    )


def test_overview_json_carries_the_denominator_not_a_scalar():
    """Coverage is heterogeneous, so the artifact reports counts against a
    named denominator rather than one fabricated estate-wide ratio."""
    conn = _conn()
    _labelers(conn, 4)
    _polls(conn, "did:plc:lab000", ok=10, bad=0)
    _polls(conn, "did:plc:lab001", ok=10, bad=0)
    _polls(conn, "did:plc:lab002", ok=1, bad=9)
    _polls(conn, "did:plc:lab003", ok=0, bad=10)

    out = tempfile.mkdtemp()
    report.generate_report(conn, out, now=NOW, config=CFG)
    with open(os.path.join(out, "overview.json"), encoding="utf-8") as fh:
        obs = json.load(fh)["observation"]

    for field in ("labelers_attempted", "labelers_adequate", "labelers_degraded",
                  "labelers_accessible", "labelers_unattempted",
                  "coverage_threshold", "window_minutes"):
        assert field in obs, f"missing denominator context: {field}"

    # No single estate-wide coverage scalar is published.
    assert "coverage_ratio" not in obs
    assert "coverage_pct" not in obs


# ---------------------------------------------------------------------------
# 7. Presentation layer cannot reintroduce the defect
# ---------------------------------------------------------------------------

def test_empty_signal_list_does_not_render_as_calm():
    """The strip renderer used to default an empty signal list to "calm"."""
    html = frontdoor._render_weather_strip_html({
        "signals": [], "total_labelers": 3, "emitting_this_week": 0,
        "events_7d_total": 0, "unreachable": 0,
    })
    assert "calm" not in html
    assert "unobserved" in html


def test_missing_ingest_outcomes_table_refuses_rather_than_assumes():
    """A pre-migration database cannot answer the adequacy question. Refusing
    is correct; claiming calm is not."""
    path = os.path.join(tempfile.mkdtemp(), "bare.db")
    conn = db.connect(path)
    conn.execute("CREATE TABLE labelers (labeler_did TEXT, endpoint_status TEXT)")
    conn.commit()

    obs = frontdoor.observation_adequacy(conn, now=NOW)
    assert obs["adequacy"] == "unobserved"
    assert not frontdoor.supports_negative_claim(obs)


# ---------------------------------------------------------------------------
# 8. One verdict implementation and one signal vocabulary
# ---------------------------------------------------------------------------

WEATHER_SIGNAL_ORDER = frontdoor.NETWORK_WEATHER_SIGNAL_VOCABULARY


def _insert_alerts(conn, rule_id, count):
    for i in range(count):
        conn.execute(
            "INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json,"
            " evidence_hashes_json, config_hash, receipt_hash)"
            " VALUES (?,?,?,'{}','[]','c',?)",
            (rule_id, "did:plc:lab000",
             format_ts(NOW - dt.timedelta(hours=1)), f"{rule_id}-{i}"),
        )


def _insert_moderation_conflict(conn):
    conn.execute(
        "INSERT INTO boundary_edges("
        " edge_type, target_uri, window_start, window_end, labeler_a, labeler_b,"
        " top_family_a, top_family_b, family_version, config_hash, computed_at)"
        " VALUES ('contradiction',?,?,?,?,?,?,?,?,?,?)",
        (
            "at://did:plc:target/app.bsky.feed.post/1",
            format_ts(NOW - dt.timedelta(days=7)),
            format_ts(NOW),
            "did:plc:lab000",
            "did:plc:lab001",
            "spam",
            "harassment",
            FAMILY_VERSION,
            "test",
            format_ts(NOW),
        ),
    )


def _weather_state(state):
    conn = _conn()
    if state == "unobserved":
        _labelers(conn, 2)
        return conn, ["unobserved"]
    if state == "calm":
        _labelers(conn, 2)
        for i in range(2):
            _polls(conn, f"did:plc:lab{i:03d}", ok=10, bad=0)
        return conn, ["calm"]
    if state == "under-observed":
        _labelers(conn, 2)
        _polls(conn, "did:plc:lab000", ok=10, bad=0)
        return conn, ["under-observed"]

    assert state == "all-positive"
    _labelers(conn, 6, endpoint_status="down")
    for i in range(6):
        _polls(conn, f"did:plc:lab{i:03d}", ok=10, bad=0)
    _insert_alerts(conn, "label_rate_spike", 11)
    _insert_moderation_conflict(conn)
    _insert_alerts(conn, "churn_index", 51)
    conn.commit()
    return conn, ["noisy", "conflicted", "churny", "degraded"]


def _rendered_weather_signals(page):
    match = re.search(
        r'<span class="weather-label">Network weather:</span>\s*'
        r'<strong>([^<]+)</strong>',
        page,
    )
    assert match, "index.html did not render the canonical weather strip"
    rendered = html_lib.unescape(match.group(1))
    return set(rendered.split(", "))


def test_overview_and_index_share_exact_weather_signals_for_all_states():
    seen = set()
    states = ("unobserved", "calm", "under-observed", "all-positive")
    for state in states:
        conn, expected = _weather_state(state)
        try:
            with tempfile.TemporaryDirectory() as out:
                report.generate_report(conn, out, now=NOW, config=CFG)
                with open(os.path.join(out, "overview.json"), encoding="utf-8") as fh:
                    verdict = json.load(fh)["network_weather"]
                with open(os.path.join(out, "index.html"), encoding="utf-8") as fh:
                    page = fh.read()
        finally:
            conn.close()

        assert verdict["signals"] == expected
        assert set(verdict["signals"]) == _rendered_weather_signals(page)
        seen.update(verdict["signals"])

        if state == "all-positive":
            assert verdict["attribution"] == (
                "11 rate-spike alerts (24h) · "
                "1 surfaced moderation-conflict edge (7d) · "
                "51 churn alerts (24h) · 6 labelers unreachable"
            )

    assert seen == set(WEATHER_SIGNAL_ORDER)


def test_report_delegates_both_weather_verdicts_to_frontdoor():
    source = inspect.getsource(report.generate_report)

    assert "weather_signals" not in source
    assert "weather_attributions" not in source
    assert not set(re.findall(r'\.append\("([^"]+)"\)', source)) & set(
        WEATHER_SIGNAL_ORDER
    )
    assert source.count("fd.network_weather(") == 2
    assert source.count("conn, now=now,") == 2
    assert source.count(
        "coverage_window_minutes=_wx_cfg.coverage_window_minutes"
    ) == 2
    assert source.count("coverage_threshold=_wx_cfg.coverage_threshold") == 2


def test_full_canonical_vocabulary_is_renderable_by_the_weather_strip():
    source = inspect.getsource(frontdoor.network_weather)
    implemented = set(re.findall(r'signals\.append\("([^"]+)"\)', source))
    assert implemented == set(WEATHER_SIGNAL_ORDER)

    html = frontdoor._render_weather_strip_html({
        "signals": WEATHER_SIGNAL_ORDER,
        "total_labelers": 6,
        "emitting_this_week": 6,
        "events_7d_total": 1,
        "unreachable": 6,
    })
    assert _rendered_weather_signals(html) == implemented
