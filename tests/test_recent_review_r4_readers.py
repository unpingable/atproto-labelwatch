"""Review r4: lease-free readers while daily maintenance retires/expires a pinned owner."""
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
import recent_provider  # noqa: E402
from recent_provider import RecentProvider  # noqa: E402
from labelwatch.recent_observations import Limits, RecentObservations, Refused  # noqa: E402
from test_recent_overview import store, row, A, B, SUBJECT  # noqa: E402,F401

LO, HI = '2026-09-01T00:00:00Z', '2026-09-02T12:00:00.000001Z'


def read(store, now, lo=LO, hi=HI):
    p = RecentObservations(RecentProvider(store), Limits(), now=lambda: now)
    try:
        return sorted(r['value'] for r in p.account(p.create(SUBJECT, lo, hi)['cursor'])['timeline'])
    except Refused as exc:
        return 'refused:' + str(exc)


def slow_hot_reads(monkeypatch, pause, entered):
    real = recent_provider.connect
    def connect(path, readonly=False):
        c = real(path, readonly)
        if readonly and Path(path).name != 'state.sqlite':
            entered.set(); time.sleep(pause)      # reader mid-snapshot, segment pinned
        return c
    monkeypatch.setattr(recent_provider, 'connect', connect)


def test_retirement_waits_for_a_reader_pinning_the_owner(store, monkeypatch):
    now = datetime(2026, 9, 3, 0, 0, 1, tzinfo=timezone.utc)
    quiet = read(store, now)
    assert isinstance(quiet, list) and quiet
    entered = threading.Event(); slow_hot_reads(monkeypatch, 1.5, entered)
    out, errors = {}, []
    def reader():
        try: out['rows'] = read(store, now)
        except Exception as exc: errors.append(repr(exc))
    t = threading.Thread(target=reader); t.start()
    assert entered.wait(10)
    t0 = time.monotonic()
    result = store.advance_day('2026-09-03T00:00:00Z')     # seals, archives, retires 2026-09-02
    waited = time.monotonic() - t0
    t.join()
    monkeypatch.undo()
    assert errors == [] and out['rows'] == quiet, (errors, out)
    assert '2026-09-02' in result['retired'] and not (store.root / '2026-09-02.sqlite').exists()
    assert read(store, now) == quiet                      # now served from the archive
    assert waited < 12


def test_expiry_while_reader_pins_the_expiring_owner(store, monkeypatch):
    # Advance until 2026-09-01 is expired by maintain while a reader holds it.
    now = datetime(2026, 9, 30, 23, 59, tzinfo=timezone.utc)
    store.advance_day('2026-09-30T00:00:00Z')
    quiet = read(store, now, '2026-09-01T00:00:01Z', '2026-09-30T00:00:00Z')
    assert isinstance(quiet, list)
    entered = threading.Event()
    import duckdb
    real = recent_provider.duckdb.connect
    class Slow:
        def __init__(self): self.inner = real()
        def __getattr__(self, name): return getattr(self.inner, name)
        def from_parquet(self, *a, **k):
            entered.set(); time.sleep(1.5)
            return self.inner.from_parquet(*a, **k)
    monkeypatch.setattr(recent_provider.duckdb, 'connect', lambda *a, **k: Slow())
    out, errors = {}, []
    def reader():
        try: out['rows'] = read(store, now, '2026-09-01T00:00:01Z', '2026-09-30T00:00:00Z')
        except Exception as exc: errors.append(repr(exc))
    t = threading.Thread(target=reader); t.start()
    assert entered.wait(10)
    result = store.advance_day('2026-10-02T00:00:00Z')
    t.join(); monkeypatch.undo()
    assert errors == [], errors
    print('EXPIRY_READER', out, quiet)
    assert out['rows'] == quiet or str(out['rows']).startswith('refused:'), out
    assert '2026-09-01' in result['maintenance']['expired_owners'], result


def test_vanished_wal_skip_still_checks_an_over_budget_state_wal(store):
    from storage import connect, db
    with connect(store.state) as c:
        db.set_meta(c, 'q:wal_budget_bytes', '4096'); c.commit()
    pin = sqlite3.connect(f'file:{store.state}?mode=ro', uri=True)
    pin.execute('BEGIN'); pin.execute('SELECT COUNT(*) FROM meta').fetchone()   # a lease-free reader snapshot
    try:
        with connect(store.state) as c:
            c.execute('PRAGMA wal_autocheckpoint=0')
            for i in range(50):
                db.set_meta(c, 'q:r4_filler_%d' % i, 'x' * 2000)
            c.commit()
        assert (store.root / 'state.sqlite-wal').stat().st_size > 4096
        with pytest.raises(RuntimeError, match='wal_budget_pinned'):
            store.check_local()
    finally:
        pin.close()
    store.check_local()


class Counted:
    """Fake provider whose fixed-frontier total is an account with n in-window rows."""
    def __init__(self, n):
        self.n = n

    from contextlib import contextmanager

    @contextmanager
    def snapshot(self, did, start, end, max_rows, *, after_id=None, upper_id=None, **kwargs):
        upper = self.n if upper_id is None else upper_id
        ids = list(range(after_id+1, min(upper, after_id+max_rows)+1))
        rows = [{'id': i, 'labeler_did': 'did:plc:source', 'uri': did, 'val': str(i), 'ts': '2020-01-01T00:00:00Z',
                 'observed_at': '2026-10-05T00:00:00Z', 'neg': 0} for i in ids]
        yield {'rows': rows, 'upper_id': upper, 'next_after_id': ids[-1] if ids else after_id,
               'has_more': upper > (ids[-1] if ids else after_id), 'total': (upper-after_id) if after_id == 0 else None,
               'coverage': {'status': 'unknown', 'gaps': [], 'acquisition': {'schema': 'labelwatch.acquisition.v1',
                            'generation_id': '12345678-1234-4234-9234-123456789012', 'started_at': '2026-09-10T00:00:00Z',
                            'basis': 'local_acceptance_clock'}},
               'frontier': 'fixed'}


@pytest.mark.parametrize('rows,allowed', [(60_000, True), (114_000, True), (114_101, False), (120_000, False)])
def test_export_sizing_cap(rows, allowed):
    now = datetime(2026, 10, 6, tzinfo=timezone.utc)
    p = RecentObservations(Counted(rows), Limits(), now=lambda: now)
    if not allowed:
        with pytest.raises(Refused, match='export_too_large'):
            p.create_export('did:plc:subject', '2026-10-05T00:00:00Z', '2026-10-06T00:00:00Z')
        return
    made = p.create_export('did:plc:subject', '2026-10-05T00:00:00Z', '2026-10-06T00:00:00Z')
    d = made['manifest']['deadline']
    assert d['cap_seconds'] == 600 and d['rows_estimate'] == rows and d['seconds'] == 30 + 0.5*-(-rows//100)
    assert 'capped' in d['rule']
