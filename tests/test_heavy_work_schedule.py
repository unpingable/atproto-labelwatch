import sqlite3
from types import SimpleNamespace

import pytest

from labelwatch import db, runner


def connection():
    c = sqlite3.connect(':memory:')
    c.row_factory = sqlite3.Row
    db.init_db(c)
    return c


def test_report_declines_due_derive_without_taking_lock():
    heavy = runner._HeavyWork()
    heavy.derive_due.set()
    assert heavy.acquire_report() == 'derive_due'
    assert heavy.lock.acquire(blocking=False)
    heavy.lock.release()


def test_report_and_derive_never_wait_on_report(monkeypatch):
    c = connection()
    heavy = runner._HeavyWork()
    assert heavy.acquire_report() is None
    assert heavy.acquire_report() == 'heavy_work_busy'
    assert runner._admit_derive(c, None, None, heavy, 17) == (False, 17)
    assert db.get_meta(c, 'ops:derive:deferred_reason') == 'report_busy'
    assert heavy.derive_due.is_set()
    heavy.lock.release()


@pytest.mark.parametrize('state', ['COMPLETE', 'INCOMPLETE'])
def test_completed_attempt_releases_priority_until_next_due(monkeypatch, state):
    c = connection()
    heavy = runner._HeavyWork()
    monkeypatch.setattr(runner, '_read_cgroup_memory_snapshot', lambda: None)
    monkeypatch.setattr(runner.scan, 'run_derive', lambda *a, **k: {'state': state, 'steps': {}})
    assert runner._admit_derive(c, None, None, heavy, 17) == (True, 17)
    assert not heavy.derive_due.is_set()
    assert bool(db.get_meta(c, 'last_derive_ok_ts')) == (state == 'COMPLETE')
    assert heavy.acquire_report() is None
    heavy.lock.release()


def test_derive_error_releases_lock_but_preserves_pending(monkeypatch):
    heavy = runner._HeavyWork()
    monkeypatch.setattr(runner, '_read_cgroup_memory_snapshot', lambda: None)
    def broken(*a, **k):
        raise RuntimeError('fixture')
    monkeypatch.setattr(runner.scan, 'run_derive', broken)
    with pytest.raises(RuntimeError):
        runner._admit_derive(connection(), None, None, heavy, None)
    assert heavy.derive_due.is_set()
    assert heavy.lock.acquire(blocking=False)
    heavy.lock.release()


def test_memory_guard_remains_effective_and_nonblocking(monkeypatch):
    c = connection()
    heavy = runner._HeavyWork()
    monkeypatch.setattr(runner, '_read_cgroup_memory_snapshot', lambda: {'current': 99, 'high':100, 'high_events':20})
    assert runner._admit_derive(c, None, None, heavy, 10) == (False, 20)
    assert heavy.derive_due.is_set()
    assert heavy.lock.acquire(blocking=False)
    heavy.lock.release()


class StopFixture(Exception):
    pass


@pytest.mark.parametrize('duration,expected_sleep', [(2400,1200),(4000,0)])
@pytest.mark.parametrize('fails', [False,True])
def test_report_start_to_start_and_finally_release(monkeypatch,duration,expected_sleep,fails):
    heavy=runner._HeavyWork()
    times=iter([100,100+duration])
    monkeypatch.setattr(runner.time,'monotonic',lambda:next(times))
    monkeypatch.setattr(runner,'_wal_size_mb',lambda _:0)
    monkeypatch.setattr(runner.db,'connect',lambda *a,**k:SimpleNamespace(close=lambda:None))
    monkeypatch.setattr(runner,'_heartbeat',lambda *a:None)
    def generate(*a,**k):
        assert heavy.lock.locked()
        if fails:raise RuntimeError('fixture')
    monkeypatch.setattr(runner.report_mod,'generate_report',generate)
    def sleep(delay):
        assert delay==expected_sleep
        assert not heavy.lock.locked()
        raise StopFixture
    monkeypatch.setattr(runner.time,'sleep',sleep)
    with pytest.raises(StopFixture):runner._report_loop('fixture','output',3600,heavy=heavy)


@pytest.mark.parametrize('kind',['priority','stat_failure'])
def test_report_deferral_retries_60s_without_opening_db(monkeypatch,kind):
    heavy=runner._HeavyWork()
    if kind=='priority':heavy.derive_due.set()
    def stat(_):
        if kind=='stat_failure':raise PermissionError('fixture')
        return 0
    monkeypatch.setattr(runner,'_wal_size_mb',stat)
    def unexpected(*a,**k):raise AssertionError('must not open DB')
    monkeypatch.setattr(runner.db,'connect',unexpected)
    def sleep(delay):
        assert delay==60
        assert not heavy.lock.locked()
        raise StopFixture
    monkeypatch.setattr(runner.time,'sleep',sleep)
    with pytest.raises(StopFixture):runner._report_loop('fixture','output',3600,heavy=heavy)


def test_busy_report_does_not_prevent_ingest_or_scan(monkeypatch,tmp_path):
    from labelwatch.config import Config
    heavy=runner._HeavyWork();heavy.lock.acquire()
    monkeypatch.setattr(runner,'_HeavyWork',lambda:heavy)
    monkeypatch.setattr(runner,'_read_cgroup_memory_snapshot',lambda:None)
    monkeypatch.setattr(runner,'_release_memory',lambda *a:None)
    calls=[]
    monkeypatch.setattr(runner.ingest,'ingest_from_service',lambda *a:calls.append('ingest'))
    monkeypatch.setattr(runner.resolve,'resolve_handles_for_labelers',lambda *a:None)
    monkeypatch.setattr(runner.scan,'run_scan',lambda *a,**k:calls.append('scan'))
    def stop(*a):raise StopFixture
    monkeypatch.setattr(runner,'_sleep_until',stop)
    cfg=Config(db_path=str(tmp_path/'state.db'),labeler_dids=['did:fixture'])
    with pytest.raises(StopFixture):runner.run_loop(cfg,120,300)
    assert calls==['ingest','scan']
    heavy.lock.release()
