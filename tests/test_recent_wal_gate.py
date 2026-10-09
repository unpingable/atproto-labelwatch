"""C4-full fix (2026-10-09): public readers never stop ingestion; WAL pressure is bounded.

Fast checks of the writer-preference gate. The full-size qualification (8 readers x
60 s, >= 1M events, >= 150 ev/s) runs recent_concurrent.py with LABELWATCH_WAL_TRACE.
"""
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import storage  # noqa: E402
from storage import connect, custody, db, lock  # noqa: E402
from recent_storage import RecentStore  # noqa: E402

NOW = datetime(2026, 10, 9, 1, tzinfo=timezone.utc)
SRC = 'did:plc:labeler'


@pytest.fixture
def store(tmp_path, monkeypatch):
    root = tmp_path / 'runtime'; root.mkdir()
    monkeypatch.setattr(storage, 'ROOT', root)
    s = RecentStore.create(root / 'store', NOW.isoformat())
    s.remember_sources([{'did': SRC, 'endpoint': 'https://labeler.example'}])
    with connect(s.state) as c:
        db.set_meta(c, 'q:wal_budget_bytes', str(1 << 20)); c.commit()      # 1 MiB budget
    return s


def page(n, rows=100):
    out = []
    for i in range(rows):
        raw = {'src': SRC, 'uri': f'did:plc:t{(n*rows+i) % 50}', 'val': f'v{n}-{i}', 'cts': '2026-10-09T00:30:00Z'}
        out.append((SRC, SRC, raw['uri'], None, raw['val'], 0, None, None, raw['cts'],
                    custody.canonical_identity(raw), raw['uri']))
    return out


def ingest(store, n, cursor):
    stamp = (NOW + timedelta(milliseconds=10*n)).isoformat()
    token = store.begin_source_attempt(SRC, stamp)
    store.accept_page(page(n), source=SRC, request_cursor=cursor, next_cursor=str(n+1), observed_at=stamp,
                      attempt_token=token)
    return str(n+1)


def wal_size(store):
    try:
        return (store.root / 'state.sqlite-wal').stat().st_size
    except FileNotFoundError:
        return 0


def test_lease_honoring_readers_cannot_grow_the_wal_past_budget(store):
    stop, reads = threading.Event(), [0]
    def reader():
        while not stop.is_set():
            with lock(store.root, 'reader.lock', shared=True, timeout=5):
                c = sqlite3.connect(f'file:{store.state}?mode=ro', uri=True)
                c.execute('BEGIN'); c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()
                time.sleep(.02)
                c.close()
            reads[0] += 1
    threads = [threading.Thread(target=reader) for _ in range(8)]
    for t in threads: t.start()
    try:
        cursor, peak, gates = None, 0, []
        for n in range(150):
            cursor = ingest(store, n, cursor)
            peak = max(peak, wal_size(store))
            figures = store.wal_gate()
            gates.append(figures['gate'])
    finally:
        stop.set()
        for t in threads: t.join()
    budget = 1 << 20
    assert reads[0] > 100
    assert peak <= budget + (1 << 20), peak                    # budget + one batch
    assert any(g and g['result'] == 'truncate' for g in gates)
    assert store.collector_bootstrap_summary()['wal_pinned'] is None


def test_pathological_reader_cannot_stop_ingest_beyond_the_bounded_wait(store):
    # Holds reader.lock and a snapshot for the whole test (never releases).
    held, release = threading.Event(), threading.Event()
    def forever():
        with lock(store.root, 'reader.lock', shared=True):
            c = sqlite3.connect(f'file:{store.state}?mode=ro', uri=True)
            c.execute('BEGIN'); c.execute('SELECT COUNT(*) FROM meta').fetchone()
            held.set(); release.wait(120); c.close()
    t = threading.Thread(target=forever); t.start()
    assert held.wait(10)
    try:
        cursor, slowest, gated = None, 0.0, 0
        for n in range(40):
            t0 = time.monotonic()
            cursor = ingest(store, n, cursor)          # never refused
            took = time.monotonic() - t0
            slowest, gated = max(slowest, took), gated + (took > 1)
        pinned = store.collector_bootstrap_summary()['wal_pinned']
    finally:
        release.set(); t.join()
    assert slowest <= storage.CHECKPOINT_READER_WAIT + 1, slowest
    assert gated <= 2                                   # one bounded wait per backoff interval
    assert pinned and pinned['wal_file_bytes'] > pinned['budget_bytes']   # disclosed, store-level
    store._wal_gate_retry = 0.0
    store.wal_gate()
    assert store.collector_bootstrap_summary()['wal_pinned'] is None     # cleared once readers let go
    assert wal_size(store) <= 1 << 20


def test_new_readers_wait_bounded_for_a_pending_checkpoint(store):
    flag = store.root / storage.CHECKPOINT_FLAG
    flag.touch()
    threading.Timer(.3, flag.unlink).start()
    t0 = time.monotonic()
    with lock(store.root, 'reader.lock', shared=True, timeout=5):
        waited = time.monotonic() - t0
    assert .25 <= waited < 1.5
    flag.touch()                                      # writer never clears it: bounded anyway
    t0 = time.monotonic()
    with lock(store.root, 'reader.lock', shared=True, timeout=5):
        waited = time.monotonic() - t0
    assert storage.CHECKPOINT_READER_WAIT - .1 <= waited < storage.CHECKPOINT_READER_WAIT + 1
    import os
    os.utime(flag, (time.time()-60, time.time()-60))  # stale flag (crashed writer) is ignored
    t0 = time.monotonic()
    with lock(store.root, 'reader.lock', shared=True, timeout=5):
        assert time.monotonic() - t0 < .2


def test_fully_checkpointed_large_wal_is_not_backlog(store):
    cursor = None
    for n in range(30):
        cursor = ingest(store, n, cursor)
    with connect(store.state) as c:
        busy, used, backlog = store.wal_state(c)
    assert busy == 0 and backlog == 0
