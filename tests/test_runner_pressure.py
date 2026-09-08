from __future__ import annotations

import sqlite3

from labelwatch import db, runner


def test_derive_pressure_gate_is_open_without_bounded_cgroup() -> None:
    assert runner._derive_memory_pressure_reason(None, None) is None
    assert runner._derive_memory_pressure_reason(
        {"current": 1, "high": 0, "high_events": 0}, 0
    ) is None


def test_derive_pressure_gate_detects_current_soft_limit(monkeypatch) -> None:
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_RATIO", 0.90)
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_EVENT_DELTA", 1000)
    reason = runner._derive_memory_pressure_reason(
        {"current": 900, "high": 1000, "high_events": 12}, 12
    )
    assert reason == "current_ratio:0.900>=0.900"


def test_derive_pressure_gate_detects_new_throttling(monkeypatch) -> None:
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_RATIO", 0.90)
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_EVENT_DELTA", 1000)
    reason = runner._derive_memory_pressure_reason(
        {"current": 500, "high": 1000, "high_events": 1300}, 200
    )
    assert reason == "high_events_delta:1100>=1000"


def test_derive_pressure_gate_allows_recovery_after_baseline_moves(monkeypatch) -> None:
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_RATIO", 0.90)
    monkeypatch.setattr(runner, "_DERIVE_MEMORY_HIGH_EVENT_DELTA", 1000)
    snapshot = {"current": 500, "high": 1000, "high_events": 1300}
    assert runner._derive_memory_pressure_reason(snapshot, 1300) is None


def test_record_derive_deferred_is_durable_and_counted() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db.init_db(conn)

    runner._record_derive_deferred(conn, "high_events_delta:1100>=1000")
    runner._record_derive_deferred(conn, "current_ratio:0.950>=0.900")

    assert db.get_meta(conn, "ops:derive:deferred_count") == "2"
    assert db.get_meta(conn, "ops:derive:deferred_reason") == "current_ratio:0.950>=0.900"
    assert db.get_meta(conn, "ops:derive:deferred_at")
