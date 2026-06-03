"""Tests for chunked _update_author_day behavior.

Covers: correctness of chunked path (same output as single-pass), defer-on-WAL-
pressure, defer-on-time-budget, retention-prune-only-on-full-completion,
explicit defer logging.
"""
import os
import time

import pytest

from labelwatch import db, scan
from labelwatch.scan import _update_author_day


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
    """Seed one event per author per day for the last 7 days."""
    for day in range(7):
        _seed_event(conn, labeler_did=LABELER1, target_did=TARGET_A, day_offset=day)
        _seed_event(conn, labeler_did=LABELER2, target_did=TARGET_B, day_offset=day)
    conn.commit()


def test_chunked_output_matches_per_day_rollup():
    """Full-completion chunked path produces one row per (author, day_epoch)."""
    conn = _make_db()
    _seed_week(conn)
    _update_author_day(conn)

    # 7 days * 2 authors = 14 rows
    n = conn.execute("SELECT COUNT(*) FROM derived_author_day").fetchone()[0]
    assert n == 14

    # Each (author, day) has events=1
    rows = conn.execute(
        "SELECT author_did, events FROM derived_author_day"
    ).fetchall()
    for r in rows:
        assert r["events"] == 1


def test_chunked_only_counts_feed_post_uris():
    """Non-feed.post URIs must be excluded from the rollup."""
    conn = _make_db()
    # One feed.post event for author A today
    _seed_event(conn, labeler_did=LABELER1, target_did=TARGET_A, day_offset=0)
    # One non-feed.post event (different collection) — must be ignored
    epoch = int(time.time())
    ts = time.strftime("%Y-%m-%dT12:00:00Z", time.gmtime(epoch))
    conn.execute(
        "INSERT INTO label_events(labeler_did, uri, val, neg, ts, event_hash, target_did) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)",
        (LABELER1, f"at://{TARGET_A}/app.bsky.feed.like/skipme",
         "spam", 0, ts, "h_skip", TARGET_A),
    )
    conn.commit()
    _update_author_day(conn)

    rows = conn.execute(
        "SELECT events FROM derived_author_day WHERE author_did = ?", (TARGET_A,)
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["events"] == 1


def test_retention_prunes_old_rows_on_full_completion():
    """Rows older than 60 days are pruned when the chunk loop completes fully."""
    conn = _make_db()
    _seed_week(conn)
    # Seed a stale derived_author_day row directly (simulating an old retained
    # day that the prune should remove)
    stale_day_epoch = ((int(time.time()) // 86400) - 90) * 86400
    conn.execute(
        "INSERT INTO derived_author_day(author_did, day_epoch, events, applies, removes, labelers, targets, vals) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day_epoch, 1, 1, 0, 1, 1, 1),
    )
    conn.commit()

    _update_author_day(conn)

    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (stale_day_epoch,),
    ).fetchone()[0]
    assert stale_remaining == 0


def test_defer_on_time_budget_keeps_retention_intact(monkeypatch, caplog):
    """When the time-budget defers mid-loop, retention prune must NOT run —
    deferred chunks may need pre-prune state for next-cycle reconciliation."""
    conn = _make_db()
    _seed_week(conn)

    # Seed a row that retention would normally prune
    stale_day_epoch = ((int(time.time()) // 86400) - 90) * 86400
    conn.execute(
        "INSERT INTO derived_author_day(author_did, day_epoch, events, applies, removes, labelers, targets, vals) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day_epoch, 1, 1, 0, 1, 1, 1),
    )
    conn.commit()

    # Force defer after the first chunk by setting budget to 0
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 0.0)

    import logging
    with caplog.at_level(logging.WARNING, logger="labelwatch.scan"):
        _update_author_day(conn)

    # Retention row must survive the defer
    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (stale_day_epoch,),
    ).fetchone()[0]
    assert stale_remaining == 1

    # Explicit defer log must have been emitted
    assert any("update_author_day deferred" in r.message for r in caplog.records)


def test_at_least_one_chunk_always_runs(monkeypatch):
    """Defer never blocks the first chunk — derive always makes some progress."""
    conn = _make_db()
    _seed_week(conn)

    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 0.0)
    _update_author_day(conn)

    # First chunk = today (day_offset=0), so today's row must exist
    today_epoch = (int(time.time()) // 86400) * 86400
    n = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (today_epoch,),
    ).fetchone()[0]
    assert n >= 1  # at least one author's today row landed


def test_chunks_process_newest_first(monkeypatch):
    """Deferred work degrades the back of the 7-day window, not the front."""
    conn = _make_db()
    _seed_week(conn)

    # Run just 2 chunks (days), then defer
    call_count = {"n": 0}
    original_yield = scan._yield_between_derive_steps

    def stop_after_two():
        call_count["n"] += 1
        if call_count["n"] >= 2:
            # Force time-budget defer on the next pressure check
            monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 0.0)
        original_yield()

    monkeypatch.setattr(scan, "_yield_between_derive_steps", stop_after_two)
    _update_author_day(conn)

    today_epoch = (int(time.time()) // 86400) * 86400
    today_count = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (today_epoch,),
    ).fetchone()[0]
    six_days_ago = today_epoch - 6 * 86400
    oldest_count = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (six_days_ago,),
    ).fetchone()[0]

    # Today landed; oldest did not
    assert today_count >= 1
    assert oldest_count == 0


def test_repeat_call_idempotent():
    """Two successive full-completion runs produce the same rows."""
    conn = _make_db()
    _seed_week(conn)
    _update_author_day(conn)
    first = conn.execute(
        "SELECT author_did, day_epoch, events FROM derived_author_day "
        "ORDER BY author_did, day_epoch"
    ).fetchall()
    _update_author_day(conn)
    second = conn.execute(
        "SELECT author_did, day_epoch, events FROM derived_author_day "
        "ORDER BY author_did, day_epoch"
    ).fetchall()
    assert [tuple(r) for r in first] == [tuple(r) for r in second]


# ---- Resume-semantics tests (Option 2: real backlog with persisted bounds) ----

# Save the truly-original yield at import time so cross-cycle test helpers can
# restore it after a defer-trigger monkeypatch is no longer wanted.
_ORIGINAL_YIELD = scan._yield_between_derive_steps


def _restore_yield(monkeypatch):
    """Restore yield to the real, non-trigger version for the next cycle."""
    monkeypatch.setattr(scan, "_yield_between_derive_steps", _ORIGINAL_YIELD)


def _stop_yield_after(monkeypatch, n_yields: int):
    """Force a budget defer after `n_yields` _yield_between_derive_steps calls.

    Each successful day commit calls _yield once at the end; the next loop
    iteration's pressure check then trips budget=0. The wrapper always calls
    through to the saved-at-import-time original so nested setups don't stack.
    """
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 600.0)
    call_count = {"n": 0}

    def stop_after():
        call_count["n"] += 1
        if call_count["n"] >= n_yields:
            monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 0.0)
        _ORIGINAL_YIELD()

    monkeypatch.setattr(scan, "_yield_between_derive_steps", stop_after)


def test_defer_persists_pending_range(monkeypatch):
    """After defer in normal mode, meta keys describe the pending range."""
    conn = _make_db()
    _seed_week(conn)
    _stop_yield_after(monkeypatch, 2)  # commit today + today-1, then defer
    _update_author_day(conn)

    assert db.get_meta(conn, "update_author_day:backlog_active") == "1"
    today_epoch = (int(time.time()) // 86400) * 86400
    oldest = int(db.get_meta(conn, "update_author_day:pending_oldest"))
    newest = int(db.get_meta(conn, "update_author_day:pending_newest"))
    # Normal mode (newest-first) processed today + today-1. Remaining = today-2..today-6.
    assert oldest == today_epoch - 6 * 86400
    assert newest == today_epoch - 2 * 86400
    # Defer reason and last-completed are persisted for visibility.
    assert db.get_meta(conn, "update_author_day:last_defer_reason").startswith("budget:")
    assert int(db.get_meta(conn, "update_author_day:last_completed_day_epoch")) == today_epoch - 86400


def test_backlog_mode_processes_only_pending_range(monkeypatch):
    """Cycle 2 in backlog mode skips already-fresh days from cycle 1."""
    conn = _make_db()
    _seed_week(conn)

    # Cycle 1: defer after 2 days (today + today-1 land).
    _stop_yield_after(monkeypatch, 2)
    _update_author_day(conn)

    # Wipe rows that cycle 1 landed so we can prove backlog mode skipped them.
    # Then cycle 2 in backlog mode should NOT touch today / today-1 (no rows
    # reappear there), and SHOULD land today-2 through today-6.
    today_epoch = (int(time.time()) // 86400) * 86400
    conn.execute(
        "DELETE FROM derived_author_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    )
    conn.commit()

    # Cycle 2: restore real yield + clear budget so backlog drains cleanly.
    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 600.0)
    _update_author_day(conn)

    # today and today-1 must remain empty — backlog mode skipped them.
    skipped = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    ).fetchone()[0]
    assert skipped == 0
    # today-2 through today-6 must all be populated by the backlog drain.
    landed = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_day WHERE day_epoch BETWEEN ? AND ?",
        (today_epoch - 6 * 86400, today_epoch - 2 * 86400),
    ).fetchone()[0]
    assert landed == 5
    # Backlog cleared.
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"


def test_backlog_drains_in_multiple_cycles(monkeypatch):
    """A backlog that itself can't complete in one cycle advances cursor each time."""
    conn = _make_db()
    _seed_week(conn)

    # Cycle 1: defer after 2 days. Pending = today-2..today-6 (5 days).
    _stop_yield_after(monkeypatch, 2)
    _update_author_day(conn)
    today_epoch = (int(time.time()) // 86400) * 86400
    assert int(db.get_meta(conn, "update_author_day:pending_oldest")) == today_epoch - 6 * 86400
    assert int(db.get_meta(conn, "update_author_day:pending_newest")) == today_epoch - 2 * 86400

    # Cycle 2: defer after 2 days again. Backlog ascending: today-6, today-5 land;
    # remaining = today-4, today-3, today-2.
    _stop_yield_after(monkeypatch, 2)
    _update_author_day(conn)
    assert db.get_meta(conn, "update_author_day:backlog_active") == "1"
    assert int(db.get_meta(conn, "update_author_day:pending_oldest")) == today_epoch - 4 * 86400
    assert int(db.get_meta(conn, "update_author_day:pending_newest")) == today_epoch - 2 * 86400

    # Cycle 3: complete the remaining 3 days. Backlog should drain.
    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 600.0)
    _update_author_day(conn)
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"


def test_backlog_clamped_when_oldest_falls_out_of_window(monkeypatch, caplog):
    """If the pending oldest is older than the 7-day window, it gets clamped."""
    conn = _make_db()
    _seed_week(conn)
    today_epoch = (int(time.time()) // 86400) * 86400

    # Forge backlog state where oldest is 10 days back — out of window.
    db.set_meta(conn, "update_author_day:backlog_active", "1")
    db.set_meta(conn, "update_author_day:pending_oldest", str(today_epoch - 10 * 86400))
    db.set_meta(conn, "update_author_day:pending_newest", str(today_epoch - 3 * 86400))
    conn.commit()

    import logging
    with caplog.at_level(logging.WARNING, logger="labelwatch.scan"):
        _update_author_day(conn)

    # Stale-cursor warning was emitted.
    assert any("backlog cursor stale" in r.message for r in caplog.records)
    # Backlog drained cleanly (days today-6..today-3 within window all landed).
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"


def test_backlog_evaporates_when_entire_range_out_of_window(monkeypatch):
    """If both bounds fell out of window, drop backlog and resume normal mode."""
    conn = _make_db()
    _seed_week(conn)
    today_epoch = (int(time.time()) // 86400) * 86400

    # Forge backlog where both bounds are 10+ days old — fully out of window.
    db.set_meta(conn, "update_author_day:backlog_active", "1")
    db.set_meta(conn, "update_author_day:pending_oldest", str(today_epoch - 12 * 86400))
    db.set_meta(conn, "update_author_day:pending_newest", str(today_epoch - 10 * 86400))
    conn.commit()

    _update_author_day(conn)

    # Backlog should have evaporated and then a full normal cycle should have run.
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"
    # All 7 days in the current window are populated (normal mode took over).
    landed = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_day "
        "WHERE day_epoch BETWEEN ? AND ?",
        (today_epoch - 6 * 86400, today_epoch),
    ).fetchone()[0]
    assert landed == 7


def test_full_completion_clears_state_and_prunes(monkeypatch):
    """Clean cycle clears backlog meta AND runs retention prune."""
    conn = _make_db()
    _seed_week(conn)

    # Pre-stage stale backlog meta (simulating a leftover from a prior incident)
    # and a stale retention row that the prune should remove.
    today_epoch = (int(time.time()) // 86400) * 86400
    db.set_meta(conn, "update_author_day:backlog_active", "1")
    db.set_meta(conn, "update_author_day:pending_oldest", str(today_epoch - 4 * 86400))
    db.set_meta(conn, "update_author_day:pending_newest", str(today_epoch - 2 * 86400))
    stale_day = today_epoch - 90 * 86400
    conn.execute(
        "INSERT INTO derived_author_day(author_did, day_epoch, events, applies, removes, labelers, targets, vals) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
        (TARGET_A, stale_day, 1, 1, 0, 1, 1, 1),
    )
    conn.commit()

    _update_author_day(conn)

    # Backlog drained
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"
    # Retention pruned the stale row
    stale_remaining = conn.execute(
        "SELECT COUNT(*) FROM derived_author_day WHERE day_epoch = ?",
        (stale_day,),
    ).fetchone()[0]
    assert stale_remaining == 0


def test_normal_mode_after_drain_returns_to_newest_first(monkeypatch):
    """After backlog drains, the next cycle is normal mode (newest-first)."""
    conn = _make_db()
    _seed_week(conn)

    # Cycle 1: defer after 1 day (today only lands).
    _stop_yield_after(monkeypatch, 1)
    _update_author_day(conn)
    assert db.get_meta(conn, "update_author_day:backlog_active") == "1"

    # Cycle 2: clear budget AND restore yield, drain the backlog.
    _restore_yield(monkeypatch)
    monkeypatch.setattr(scan, "_UPDATE_AUTHOR_DAY_BUDGET_S", 600.0)
    _update_author_day(conn)
    assert db.get_meta(conn, "update_author_day:backlog_active") == "0"

    # Cycle 3: defer after 2 days. Should be normal mode (newest-first),
    # so today + today-1 land, today-2..today-6 are deferred.
    today_epoch = (int(time.time()) // 86400) * 86400
    conn.execute(
        "DELETE FROM derived_author_day WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    )
    conn.commit()
    _stop_yield_after(monkeypatch, 2)
    _update_author_day(conn)

    # today and today-1 landed (newest-first proved by repopulating exactly
    # the rows we deleted).
    landed_recent = conn.execute(
        "SELECT COUNT(DISTINCT day_epoch) FROM derived_author_day "
        "WHERE day_epoch IN (?, ?)",
        (today_epoch, today_epoch - 86400),
    ).fetchone()[0]
    assert landed_recent == 2
    # Pending range = today-6..today-2 (the older days that did NOT land).
    assert int(db.get_meta(conn, "update_author_day:pending_oldest")) == today_epoch - 6 * 86400
    assert int(db.get_meta(conn, "update_author_day:pending_newest")) == today_epoch - 2 * 86400
