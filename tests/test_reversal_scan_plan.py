"""Guards for the _compute_reversal_stats_7d query repair (2026-09-02).

Background: the 7-day reversal computation was planning as a full scan of
idx_label_events_state (labeler_did, uri, val, ts). ts is that index's fourth
column, so the `ts >= ?` predicate could not restrict the scan and the step
read every index entry in the database — ~81.5M entries / ~12GB on prod — to
return roughly 4% of them. The repair pins the query to idx_label_events_ts so
the window drives the scan.

Two things are tested here, and they fail for different reasons:

  * the *plan* tests fail if the query regresses to historical-size work;
  * the *equivalence* tests fail if the repair changed what the step computes.

The legacy SQL is kept verbatim below as the equivalence oracle. Do not
"tidy" it to match the new query — its whole job is to be the old one.
"""

from __future__ import annotations

import time

from labelwatch import db
from labelwatch.scan import _REVERSAL_STATS_7D_SQL, _compute_reversal_stats_7d


# The pre-repair query, exactly as it shipped before 2026-09-02.
_LEGACY_SQL = """
        WITH e AS (
            SELECT labeler_did, uri, val, neg,
                   CAST(strftime('%s', ts) AS INTEGER) AS ts_epoch
            FROM label_events
            WHERE uri LIKE 'at://%/app.bsky.feed.post/%'
              AND ts >= ?
        )
        SELECT * FROM e
        WHERE ts_epoch IS NOT NULL AND ts_epoch >= ?
        ORDER BY labeler_did, uri, val, ts_epoch
"""

_POST = "at://did:plc:author{a}/app.bsky.feed.post/{p}"


def _iso(epoch: float) -> str:
    """Match the production ts format exactly: fixed-width ISO-8601 UTC, 27 chars."""
    whole = int(epoch)
    micros = int(round((epoch - whole) * 1_000_000))
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(whole)) + f".{micros:06d}Z"


def _mkdb():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _insert(conn, labeler, uri, val, neg, epoch):
    conn.execute(
        "INSERT INTO label_events (labeler_did, src, uri, cid, val, neg, ts, event_hash) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            labeler, labeler, uri, "bafycid", val, neg, _iso(epoch),
            f"{labeler}|{uri}|{val}|{neg}|{epoch}",
        ),
    )


def _populate(conn, now_epoch):
    """A corpus with reversals, non-post URIs, and out-of-window history."""
    for a in range(4):
        labeler = f"did:plc:labeler{a}"
        for p in range(6):
            uri = _POST.format(a=a, p=p)
            base = now_epoch - (86400 * (p % 5)) - 3600
            _insert(conn, labeler, uri, "spam", 0, base)
            if p % 2 == 0:                      # half the groups get reversed
                _insert(conn, labeler, uri, "spam", 1, base + 900)
            if p % 3 == 0:                      # a second val on some groups
                _insert(conn, labeler, uri, "nsfw", 0, base + 30)
        # non-post URIs must be excluded by the LIKE filter
        _insert(conn, labeler, f"did:plc:account{a}", "spam", 0, now_epoch - 7200)
        _insert(conn, labeler, f"at://did:plc:x{a}/app.bsky.feed.generator/g", "spam", 0, now_epoch - 7200)
        # well outside the 7-day window — must never be read into the result
        _insert(conn, labeler, _POST.format(a=a, p=99), "spam", 0, now_epoch - (86400 * 40))
    conn.commit()


def _run(conn, sql, now_epoch):
    cutoff_epoch = int(now_epoch) - (7 * 86400)
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_epoch))
    return conn.execute(sql, (cutoff_iso, cutoff_epoch)).fetchall()


def _plan(conn, sql, now_epoch):
    cutoff_epoch = int(now_epoch) - (7 * 86400)
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_epoch))
    rows = conn.execute(
        "EXPLAIN QUERY PLAN " + sql, (cutoff_iso, cutoff_epoch)
    ).fetchall()
    return "\n".join(str(r[3]) for r in rows)


# --------------------------------------------------------------------------
# Plan regression — these fail if the query goes back to historical-size work
# --------------------------------------------------------------------------

def test_plan_seeks_the_time_index_and_never_scans_the_state_index():
    """The shipped query must range-seek on ts, not walk the whole corpus.

    This is the regression guard. If someone drops the INDEXED BY clause, the
    planner reverts to a full scan of idx_label_events_state and this fails.
    """
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    plan = _plan(conn, _REVERSAL_STATS_7D_SQL, now_epoch)

    assert "idx_label_events_ts" in plan, plan
    # A range seek, not a walk. SQLite writes range-restricted access as SEARCH.
    assert "SEARCH" in plan, plan
    assert "(ts>?)" in plan.replace(" ", ""), plan
    # The corpus-spanning index must not appear at all.
    assert "idx_label_events_state" not in plan, plan
    assert "SCAN label_events" not in plan, plan


def test_legacy_plan_demonstrates_the_pathology():
    """Characterisation test: the old query really does scan the state index.

    Without this, the guard above could pass for the wrong reason (e.g. the
    state index quietly disappearing from the schema), and we would lose the
    evidence that the repair was necessary at all.
    """
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    plan = _plan(conn, _LEGACY_SQL, now_epoch)

    assert "idx_label_events_state" in plan, plan
    assert "SCAN" in plan, plan


def test_repair_does_not_depend_on_planner_statistics():
    """INDEXED BY is a hard constraint, so the plan holds with stale stats.

    Prod's sqlite_stat1 is badly stale (39.4M rows recorded against 81.5M
    actual). A repair that only worked because of good statistics would be no
    repair at all, so pin the behaviour with deliberately wrong ones.
    """
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    conn.execute("ANALYZE")
    conn.execute("DELETE FROM sqlite_stat1")
    conn.execute(
        "INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES "
        "('label_events', 'idx_label_events_state', '99000000 1'),"
        "('label_events', 'idx_label_events_ts', '99000000 99000000')"
    )
    conn.commit()
    conn.execute("ANALYZE sqlite_master")  # force the planner to reload stat1

    plan = _plan(conn, _REVERSAL_STATS_7D_SQL, now_epoch)
    assert "idx_label_events_ts" in plan, plan
    assert "idx_label_events_state" not in plan, plan


# --------------------------------------------------------------------------
# Semantic equivalence — these fail if the repair changed the computation
# --------------------------------------------------------------------------

def test_repaired_query_returns_the_same_rows_as_the_legacy_query():
    """Same multiset of (labeler, uri, val, neg, ts_epoch) tuples."""
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    legacy = _run(conn, _LEGACY_SQL, now_epoch)
    repaired = _run(conn, _REVERSAL_STATS_7D_SQL, now_epoch)

    def key(r):
        return (r["labeler_did"], r["uri"], r["val"], r["neg"], r["ts_epoch"])

    assert len(repaired) == len(legacy)
    assert sorted(map(key, repaired)) == sorted(map(key, legacy))
    assert legacy, "fixture produced no in-window rows; the test proves nothing"


def test_repaired_ordering_refines_the_legacy_ordering():
    """The new ORDER BY must be a refinement, not a different order.

    Dropping the added tiebreaker from the new result must reproduce an
    ordering that is valid under the old ORDER BY — i.e. the (labeler, uri,
    val, ts_epoch) sequence is still non-decreasing.
    """
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    repaired = _run(conn, _REVERSAL_STATS_7D_SQL, now_epoch)
    seq = [
        (r["labeler_did"], r["uri"], r["val"], r["ts_epoch"]) for r in repaired
    ]
    assert seq == sorted(seq)


def test_same_second_events_are_ordered_deterministically():
    """Events sharing a whole second must come back in a stable order.

    This is the behaviour the added `ts` tiebreaker buys. Under the old query
    the order of an apply and a negate landing in the same second was whatever
    the index happened to yield, which changes the dwell computation below.
    """
    conn = _mkdb()
    now_epoch = time.time()
    base = int(now_epoch) - 3600
    uri = _POST.format(a=0, p=0)
    # apply at .100000, negate at .900000 — same whole second
    _insert(conn, "did:plc:labeler0", uri, "spam", 0, base + 0.1)
    _insert(conn, "did:plc:labeler0", uri, "spam", 1, base + 0.9)
    conn.commit()

    rows = _run(conn, _REVERSAL_STATS_7D_SQL, now_epoch)
    assert [r["neg"] for r in rows] == [0, 1]
    assert rows[0]["ts_epoch"] == rows[1]["ts_epoch"], "fixture must share a second"


def test_out_of_window_history_is_excluded():
    """The window filter still applies — the repair must not widen the range."""
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    cutoff_epoch = int(now_epoch) - (7 * 86400)
    rows = _run(conn, _REVERSAL_STATS_7D_SQL, now_epoch)
    assert rows
    assert all(r["ts_epoch"] >= cutoff_epoch for r in rows)
    assert all("/app.bsky.feed.post/" in r["uri"] for r in rows)


def test_derived_table_contents_match_across_the_repair():
    """End-to-end: the step's persisted output is unchanged by the repair.

    Runs the real function, snapshots derived_labeler_reversal_7d, then
    recomputes the same statistics from the legacy query's row stream and
    compares. This is the check that matters — the query is an implementation
    detail, the derived table is the product.
    """
    conn = _mkdb()
    now_epoch = time.time()
    _populate(conn, now_epoch)

    _compute_reversal_stats_7d(conn)
    conn.commit()
    produced = {
        r["labeler_did"]: (r["n_reversals"], r["pct_reversed"])
        for r in conn.execute(
            "SELECT labeler_did, n_reversals, pct_reversed "
            "FROM derived_labeler_reversal_7d"
        )
    }
    assert produced, "step produced no rows; the test proves nothing"

    # Independent recomputation from the legacy row stream.
    expected: dict[str, list] = {}
    current_group = None
    last_apply = None
    pair_found = False
    for r in _run(conn, _LEGACY_SQL, now_epoch):
        did = r["labeler_did"]
        st = expected.setdefault(did, [0, 0])  # [apply_groups, reversals]
        gk = (did, r["uri"], r["val"])
        if gk != current_group:
            if current_group is not None and last_apply is not None:
                expected[current_group[0]][0] += 1
            current_group = gk
            last_apply = None
            pair_found = False
        if r["neg"] == 0:
            last_apply = r["ts_epoch"]
        elif r["neg"] == 1 and last_apply is not None and not pair_found:
            st[1] += 1
            pair_found = True
    if current_group is not None and last_apply is not None:
        expected[current_group[0]][0] += 1

    for did, (n_reversals, pct) in produced.items():
        exp_groups, exp_reversals = expected[did]
        assert n_reversals == exp_reversals, did
        expected_pct = round(exp_reversals / exp_groups, 4) if exp_groups else 0.0
        assert pct == expected_pct, did
