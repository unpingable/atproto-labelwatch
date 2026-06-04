"""Tests for chunked _update_author_labeler_day behavior.

Mirrors test_update_author_day_chunked.py. UALD shares the chunking loop with
UAD (_run_per_day_chunked) but writes to a different table and carries
labeler_did in the GROUP BY; tests below pin both the table-specific output
shape and the defer/backlog state under the UALD meta-key namespace.
"""
import time

import pytest

from labelwatch import db, scan
from labelwatch.scan import _update_author_labeler_day


TARGET_A = "did:plc:authorA"
TARGET_B = "did:plc:authorB"
LABELER1 = "did:plc:labeler1"
LABELER2 = "did:plc:labeler2"


def _make_db():
    conn = db.connect(":memory:")
    db.init_db(conn)
    return conn


def _seed_event(conn, *, labeler_did, target_did, day_offset, idx=0, val="spam", neg=0):
    """Insert one label_event N days ago."""
    epoch = int(time.time()) - day_offset * 86400
    ts = time.strftime("%Y-%m-%dT12:00:00Z", time.gmtime(epoch))
    uri = f"at://{target_did}/app.bsky.feed.post/{day_offset}-{idx}"
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (labeler_did, uri, val, neg, ts, f"h_{day_offset}_{idx}_{time.monotonic_ns()}", target_did),
    )


def _seed_week(conn):
    """Seed one event per (author, labeler) per day for the last 7 days.

    UALD's PK is (author_did, day_epoch, labeler_did) — seeding two authors
    and two labelers gives 4 rows per day, 28 across the window.
    """
    for day in range(7):
        for labeler in (LABELER1, LABELER2):
            _seed_event(conn, labeler_did=labeler, target_did=TARGET_A, day_offset=day)
            _seed_event(conn, labeler_did=labeler, target_did=TARGET_B, day_offset=day)
    conn.commit()


def test_chunked_output_is_per_author_labeler_day():
    """Full-completion path produces one row per (author, day_epoch, labeler)."""
    conn = _make_db()
    _seed_week(conn)
    _update_author_labeler_day(conn)

    # 7 days * 2 authors * 2 labelers = 28 rows
    n = conn.execute("SELECT COUNT(*) FROM derived_author_labeler_day").fetchone()[0]
    assert n == 28

    rows = conn.execute(
        "SELECT events FROM derived_author_labeler_day"
    ).fetchall()
    for r in rows:
        assert r["events"] == 1


def test_chunked_only_counts_feed_post_uris():
    """Non-feed.post URIs must be excluded."""
    conn = _make_db()
    _seed_event(conn, labeler_did=LABELER1, target_did=TARGET_A, day_offset=0)
    epoch = int(time.time())
    ts = time.strftime("%Y-%m-%dT12:00:00Z", time.gmtime(epoch))
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (LABELER1, f"at://{TARGET_A}/app.bsky.feed.like/skipme",
         "spam", 0, ts, "h_skip", TARGET_A),
    )
    conn.commit()
    _update_author_labeler_day(conn)

    rows = conn.execute(
        "SELECT events FROM derived_author_labeler_day WHERE author_did = ?",
        (TARGET_A,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["events"] == 1


def test_retention_prunes_old_rows_on_full_completion():
    """Rows older than 60 days are pruned on a clean cycle."""
    conn = _make_db()
    _seed_week(conn)
    stale_day_epoch = ((int(time.time()) // 86400) - 90) * 86400
    conn.execute(
        "INSERT INTO derived_author_labeler_day(author_did, day_epoch, labeler_did, events, applies, removes, targets) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day_epoch, LABELER1, 1, 1, 0, 1),
    )
    conn.commit()

    _update_author_labeler_day(conn)

    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (stale_day_epoch,),
    ).fetchone()[0]
    assert stale_remaining == 0


def test_defer_on_time_budget_keeps_retention_intact(monkeypatch, caplog):
    """Defer mid-loop must NOT prune retention — pre-prune state may still be needed."""
    conn = _make_db()
    _seed_week(conn)

    stale_day_epoch = ((int(time.time()) // 86400) - 90) * 86400
    conn.execute(
        "INSERT INTO derived_author_labeler_day(author_did, day_epoch, labeler_did, events, applies, removes, targets) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day_epoch, LABELER1, 1, 1, 0, 1),
    )
    conn.commit()

    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 0.0)

    import logging
    with caplog.at_level(logging.WARNING, logger="labelwatch.scan"):
        _update_author_labeler_day(conn)

    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (stale_day_epoch,),
    ).fetchone()[0]
    assert stale_remaining == 1

    assert any("update_author_labeler_day deferred" in r.message for r in caplog.records)


def test_at_least_one_chunk_always_runs(monkeypatch):
    """Defer never blocks the first chunk."""
    conn = _make_db()
    _seed_week(conn)

    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 0.0)
    _update_author_labeler_day(conn)

    today_epoch = (int(time.time()) // 86400) * 86400
    n = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (today_epoch,),
    ).fetchone()[0]
    assert n >= 1


def test_chunks_process_newest_first(monkeypatch):
    """Deferred work degrades the back of the window."""
    conn = _make_db()
    _seed_week(conn)

    call_count = {"n": 0}
    original_yield = scan._yield_between_derive_steps

    def stop_after_two():
        call_count["n"] += 1
        if call_count["n"] >= 2:
            monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 0.0)
        original_yield()

    monkeypatch.setattr(scan, "_yield_between_derive_steps", stop_after_two)
    _update_author_labeler_day(conn)

    today_epoch = (int(time.time()) // 86400) * 86400
    today_count = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (today_epoch,),
    ).fetchone()[0]
    six_days_ago = today_epoch - 6 * 86400
    oldest_count = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (six_days_ago,),
    ).fetchone()[0]

    assert today_count >= 1
    assert oldest_count == 0


def test_repeat_call_idempotent():
    """Two clean cycles produce the same rows."""
    conn = _make_db()
    _seed_week(conn)
    _update_author_labeler_day(conn)
    first = conn.execute(
        "SELECT author_did, day_epoch, labeler_did, events FROM derived_author_labeler_day "
        "ORDER BY author_did, day_epoch, labeler_did"
    ).fetchall()
    _update_author_labeler_day(conn)
    second = conn.execute(
        "SELECT author_did, day_epoch, labeler_did, events FROM derived_author_labeler_day "
        "ORDER BY author_did, day_epoch, labeler_did"
    ).fetchall()
    assert [tuple(r) for r in first] == [tuple(r) for r in second]


# ---- Resume-semantics tests (separate meta-key namespace from UAD) ----

_ORIGINAL_YIELD = scan._yield_between_derive_steps


def _restore_yield(monkeypatch):
    monkeypatch.setattr(scan, "_yield_between_derive_steps", _ORIGINAL_YIELD)


def _stop_yield_after(monkeypatch, n_yields: int):
    """Force a budget defer after `n_yields` _yield_between_derive_steps calls."""
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 600.0)
    call_count = {"n": 0}

    def stop_after():
        call_count["n"] += 1
        if call_count["n"] >= n_yields:
            monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 0.0)
        _ORIGINAL_YIELD()

    monkeypatch.setattr(scan, "_yield_between_derive_steps", stop_after)


def test_defer_persists_pending_range(monkeypatch):
    """Defer in normal mode persists the UALD pending range under its own namespace."""
    conn = _make_db()
    _seed_week(conn)
    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)

    # UALD state is under its own prefix — must not collide with UAD's.
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "1"
    assert db.get_meta(conn, "update_author_day:backlog_active") in ("", "0", None)

    today_epoch = (int(time.time()) // 86400) * 86400
    oldest = int(db.get_meta(conn, "update_author_labeler_day:pending_oldest"))
    newest = int(db.get_meta(conn, "update_author_labeler_day:pending_newest"))
    assert oldest == today_epoch - 6 * 86400
    assert newest == today_epoch - 2 * 86400
    assert db.get_meta(
        conn, "update_author_labeler_day:last_defer_reason"
    ).startswith("budget:")
    assert int(db.get_meta(
        conn, "update_author_labeler_day:last_completed_day_epoch"
    )) == today_epoch - 86400


def test_backlog_mode_processes_only_pending_range(monkeypatch):
    """Cycle 2 in backlog mode skips already-fresh days from cycle 1."""
    conn = _make_db()
    _seed_week(conn)

    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)

    today_epoch = (int(time.time()) // 86400) * 86400
    conn.execute(
        "DELETE FROM derived_author_labeler_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    )
    conn.commit()

    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 600.0)
    _update_author_labeler_day(conn)

    skipped = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    ).fetchone()[0]
    assert skipped == 0
    landed = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_labeler_day "
        "WHERE day_epoch BETWEEN ? AND ?",
        (today_epoch - 6 * 86400, today_epoch - 2 * 86400),
    ).fetchone()[0]
    assert landed == 5
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"


def test_backlog_drains_in_multiple_cycles(monkeypatch):
    """A backlog that doesn't complete in one cycle still advances each time."""
    conn = _make_db()
    _seed_week(conn)

    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)
    today_epoch = (int(time.time()) // 86400) * 86400
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_oldest"
    )) == today_epoch - 6 * 86400
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_newest"
    )) == today_epoch - 2 * 86400

    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "1"
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_oldest"
    )) == today_epoch - 4 * 86400
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_newest"
    )) == today_epoch - 2 * 86400

    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 600.0)
    _update_author_labeler_day(conn)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"


def test_backlog_clamped_when_oldest_falls_out_of_window(caplog):
    """Out-of-window pending_oldest is logged and clamped."""
    conn = _make_db()
    _seed_week(conn)
    today_epoch = (int(time.time()) // 86400) * 86400

    db.set_meta(conn, "update_author_labeler_day:backlog_active", "1")
    db.set_meta(
        conn, "update_author_labeler_day:pending_oldest",
        str(today_epoch - 10 * 86400),
    )
    db.set_meta(
        conn, "update_author_labeler_day:pending_newest",
        str(today_epoch - 3 * 86400),
    )
    conn.commit()

    import logging
    with caplog.at_level(logging.WARNING, logger="labelwatch.scan"):
        _update_author_labeler_day(conn)

    assert any("backlog cursor stale" in r.message for r in caplog.records)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"


def test_backlog_evaporates_when_entire_range_out_of_window():
    """Both bounds out of window → drop backlog, run a fresh normal cycle."""
    conn = _make_db()
    _seed_week(conn)
    today_epoch = (int(time.time()) // 86400) * 86400

    db.set_meta(conn, "update_author_labeler_day:backlog_active", "1")
    db.set_meta(
        conn, "update_author_labeler_day:pending_oldest",
        str(today_epoch - 12 * 86400),
    )
    db.set_meta(
        conn, "update_author_labeler_day:pending_newest",
        str(today_epoch - 10 * 86400),
    )
    conn.commit()

    _update_author_labeler_day(conn)

    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"
    landed = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_labeler_day "
        "WHERE day_epoch BETWEEN ? AND ?",
        (today_epoch - 6 * 86400, today_epoch),
    ).fetchone()[0]
    assert landed == 7


def test_full_completion_clears_state_and_prunes():
    """Clean cycle clears backlog meta AND runs retention prune."""
    conn = _make_db()
    _seed_week(conn)

    today_epoch = (int(time.time()) // 86400) * 86400
    db.set_meta(conn, "update_author_labeler_day:backlog_active", "1")
    db.set_meta(
        conn, "update_author_labeler_day:pending_oldest",
        str(today_epoch - 4 * 86400),
    )
    db.set_meta(
        conn, "update_author_labeler_day:pending_newest",
        str(today_epoch - 2 * 86400),
    )
    stale_day = today_epoch - 90 * 86400
    conn.execute(
        "INSERT INTO derived_author_labeler_day(author_did, day_epoch, labeler_did, events, applies, removes, targets) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day, LABELER1, 1, 1, 0, 1),
    )
    conn.commit()

    _update_author_labeler_day(conn)

    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"
    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_labeler_day WHERE day_epoch = ?",
        (stale_day,),
    ).fetchone()[0]
    assert stale_remaining == 0


def test_normal_mode_after_drain_returns_to_newest_first(monkeypatch):
    """After backlog drains, the next cycle is normal mode."""
    conn = _make_db()
    _seed_week(conn)

    _stop_yield_after(monkeypatch, 1)
    _update_author_labeler_day(conn)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "1"

    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_LABELER_DAY_BUDGET_S", 600.0)
    _update_author_labeler_day(conn)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "0"

    today_epoch = (int(time.time()) // 86400) * 86400
    conn.execute(
        "DELETE FROM derived_author_labeler_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    )
    conn.commit()
    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)

    landed_recent = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_labeler_day "
        "WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    ).fetchone()[0]
    assert landed_recent == 2
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_oldest"
    )) == today_epoch - 6 * 86400
    assert int(db.get_meta(
        conn, "update_author_labeler_day:pending_newest"
    )) == today_epoch - 2 * 86400


def test_uald_and_uad_state_are_independent(monkeypatch):
    """Defer in UALD must not touch UAD's backlog state (separate namespaces)."""
    from labelwatch.scan import _update_author_day

    conn = _make_db()
    _seed_week(conn)

    # Pre-stage UAD state to look "clean" (explicit, not just default empty).
    db.set_meta(conn, "update_author_day:backlog_active", "0")
    conn.commit()

    _stop_yield_after(monkeypatch, 2)
    _update_author_labeler_day(conn)

    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "1"
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"

    # And the reverse: cleanly running UAD must not touch UALD's backlog.
    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 600.0)
    _update_author_day(conn)
    assert db.get_meta(conn, "update_author_labeler_day:backlog_active") == "1"
