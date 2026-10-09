"""Review r5 (2026-10-08): closure verification of collector r4 fixes (cc03b68..0faa136)."""
import sqlite3

import httpx
import pytest

from test_recent_collector_bootstrap import (  # noqa: F401  (fixture import)
    A, B, NOW, Clock, Labeler, Transport, enroll, run, status, store, gaps)
from test_recent_review_r4 import _Mix
from labelwatch.recent_collector import Collector, Limits


def _round(store, t, clock, trace, watch):
    t0, before = clock.t, len(t.calls)
    Collector(store, t, limits=Limits(seconds=30), monotonic=clock.monotonic, now=clock.now, sleep=clock.sleep).tick()
    calls = t.calls[before:]
    trace.append({'elapsed': round(clock.t-t0, 2), 'requests': len(calls),
                  'busy_probes': sum(1 for c in calls if c[0] in watch),
                  'established_pages': sum(1 for c in calls if c[2] == 100 and c[0] not in watch),
                  'busy': [status(store, d)['bootstrap'] for d in watch]})
    clock.t = t0+120


# B(i): 400 dead at 1 s timeout + 100 growing small sources.
def test_b1_busy_establishes_with_slow_dead_and_growing_small(store):
    dead = [f'did:plc:d{i:03d}' for i in range(400)]
    small = [Labeler(f'did:plc:s{i:03d}', 30, recent=30) for i in range(83)]
    growing = [Labeler(f'did:plc:g{i:03d}', 10, recent=10, growth=3) for i in range(100)]
    big = Labeler(A, 1_000_000, recent=30, growth=1)
    clock = Clock(); t = _Mix(clock, [big, *small, *growing], set(dead), 1.0)
    enroll(store, A, *dead, *(l.did for l in small), *(l.did for l in growing))
    trace = []
    for _ in range(10):
        _round(store, t, clock, trace, [A])
        if trace[-1]['busy'] == ['established']:
            break
    print(trace)
    assert all(r['elapsed'] <= 30+1.3 for r in trace), trace
    assert trace[-1]['busy'] == ['established'], [(i+1, r['busy_probes']) for i, r in enumerate(trace)]


def _establish(store, labelers):
    with sqlite3.connect(store.state) as c:
        for lab in labelers:
            c.execute("UPDATE q_recent_sources SET provider_cursor=?, boot_phase='established', boot_cursor=? WHERE did=?",
                      (str(lab.size), str(lab.size), lab.did))


# B(ii): busy source discovered on day 2 while 580 established sources rotate.
def test_b2_busy_discovered_day_two_establishes_without_starving_established(store):
    est = [Labeler(f'did:plc:e{i:03d}', 30, recent=30, growth=1) for i in range(580)]
    big = Labeler(A, 1_000_000, recent=30, growth=1)
    clock = Clock(); t = _Mix(clock, [big, *est], set(), 0)
    enroll(store, *(l.did for l in est)); _establish(store, est)
    baseline = []
    for _ in range(3):
        t0, before = clock.t, len(t.calls)
        Collector(store, t, limits=Limits(seconds=30), monotonic=clock.monotonic, now=clock.now, sleep=clock.sleep).tick()
        baseline.append(sum(1 for c in t.calls[before:] if c[2] == 100)); clock.t = t0+120
    store.remember_sources([{'did': A, 'endpoint': 'https://fixture.invalid'}], request_cursor=None, next_cursor=None)
    trace = []
    for _ in range(10):
        _round(store, t, clock, trace, [A])
        if trace[-1]['busy'] == ['established']:
            break
    print(baseline, trace)
    assert trace[-1]['busy'] == ['established'], trace
    base = min(baseline)
    assert all(r['established_pages'] >= (2*base)//3 for r in trace), (baseline, trace)


# B livelock: two busy sources never alternate forever.
def test_b3_two_busy_sources_both_establish(store):
    a, b = Labeler(A, 1_000_000, recent=30, growth=1), Labeler(B, 2_000_000, recent=30, growth=1)
    small = [Labeler(f'did:plc:s{i:03d}', 30, recent=30, growth=1) for i in range(150)]
    clock = Clock(); t = _Mix(clock, [a, b, *small], set(), 0)
    enroll(store, A, B, *(l.did for l in small))
    trace = []
    for _ in range(20):
        _round(store, t, clock, trace, [A, B])
        if trace[-1]['busy'] == ['established', 'established']:
            break
    print(trace)
    assert trace[-1]['busy'] == ['established', 'established'], trace


# B: a source failing inside its sticky run goes to backoff and frees the share.
def test_b4_failure_mid_search_frees_share(store):
    class Flaky(_Mix):
        fail_at = 3
        def request(self, operation, params, max_bytes, timeout_seconds):
            if params['did'] == A and sum(1 for c in self.calls if c[0] == A) == self.fail_at:
                self.calls.append((A, params['cursor'], params['limit'])); self.clock.t += .21
                raise httpx.ConnectError('flap')
            return super().request(operation, params, max_bytes, timeout_seconds)
    a, b = Labeler(A, 1_000_000, recent=30, growth=1), Labeler(B, 1_000_000, recent=30, growth=1)
    clock = Clock(); t = Flaky(clock, [a, b], set(), 0)
    enroll(store, A, B)
    Collector(store, t, limits=Limits(seconds=30), monotonic=clock.monotonic, now=clock.now, sleep=clock.sleep).tick()
    calls = [c[0] for c in t.calls]
    after = calls[[i for i, d in enumerate(calls) if d == A][3]+1:]
    assert A not in after, calls                 # backoff: never re-claimed this tick
    assert B in after, calls                     # share freed for the other source
    row = status(store, A)
    assert row['failures'] == 1


# C: SSRF ---------------------------------------------------------------------------
from labelwatch.recent_collector import HTTPTransport, Refused, public_host


@pytest.mark.parametrize('host', ['::ffff:127.0.0.1', '[::ffff:127.0.0.1]', '0.0.0.0', '[::]', 'LOCALHOST.',
                                  'x.LOCAL.', '[fe80::1%25eth0]', '64:ff9b::7f00:1'])
def test_c_literal_layer_refuses(host):
    assert public_host(host) is False


@pytest.mark.parametrize('host', ['2130706433', '0177.0.0.1', '127.1', '0x7f.1', '127.0.0.1.nip.io', 'metadata'])
def test_c_numeric_and_rebinding_names_refused_on_real_resolution_path(host):
    # Literal layer admits these (not ipaddress literals); the real-network resolution
    # check must refuse them because getaddrinfo maps them to loopback/link-local.
    import asyncio
    from urllib.parse import urlsplit
    answers = {'127.0.0.1.nip.io': '127.0.0.1', 'metadata': '169.254.169.254'}
    loop = asyncio.new_event_loop()
    async def fake(host, port, *a, **k):
        if host in answers:
            return [(2, 1, 6, '', (answers[host], port))]
        return await asyncio.get_running_loop().run_in_executor(None, __import__('socket').getaddrinfo, host, port)
    loop.getaddrinfo = fake
    with pytest.raises(Refused):
        loop.run_until_complete(HTTPTransport()._public_resolution(urlsplit(f'https://{host}/x')))
    loop.close()


def test_c_mixed_public_private_answer_refused():
    import asyncio
    from urllib.parse import urlsplit
    loop = asyncio.new_event_loop()
    async def fake(host, port, *a, **k):
        return [(2, 1, 6, '', ('93.184.216.34', port)), (10, 1, 6, '', ('::1', port, 0, 0))]
    loop.getaddrinfo = fake
    with pytest.raises(Refused, match='non-public address'):
        loop.run_until_complete(HTTPTransport()._public_resolution(urlsplit('https://labeler.example/x')))
    loop.close()


def test_c_cgnat_answer_refused():
    # 100.64.0.0/10 is not is_private in Python 3.12; the unit denies it but the
    # in-process check admits it (finding, defence in depth only).
    import asyncio
    from urllib.parse import urlsplit
    loop = asyncio.new_event_loop()
    async def fake(host, port, *a, **k):
        return [(2, 1, 6, '', ('100.64.0.1', port))]
    loop.getaddrinfo = fake
    with pytest.raises(Refused):
        loop.run_until_complete(HTTPTransport()._public_resolution(urlsplit('https://labeler.example/x')))
    loop.close()


@pytest.mark.parametrize('did,endpoint,first', [('did:plc:aaaa', None, 'plc.directory'),
                                                ('did:plc:aaaa', 'https://labeler.example', 'labeler.example'),
                                                ('did:web:labeler.example', None, 'labeler.example')])
def test_c_every_request_host_is_resolution_checked(did, endpoint, first):
    seen = []
    t = HTTPTransport()
    async def record(url):
        seen.append(url.hostname); raise Refused('stop')
    t._public_resolution = record
    with pytest.raises(Refused, match='stop'):
        t.request('labels', {'did': did, 'endpoint': endpoint, 'cursor': None, 'limit': 5}, max_bytes=1000, timeout_seconds=5)
    with pytest.raises(Refused, match='stop'):
        t.request('discover', {'cursor': None, 'limit': 5}, max_bytes=1000, timeout_seconds=5)
    assert seen == [first, 'bsky.network']


def test_c_did_web_numeric_loopback_authority_refused_before_any_request():
    t = HTTPTransport(); seen = []
    async def record(url):
        seen.append(url.hostname); raise Refused('stop')
    t._public_resolution = record
    with pytest.raises(Refused):
        t.request('labels', {'did': 'did:web:127.1', 'endpoint': None, 'cursor': None, 'limit': 5},
                  max_bytes=1000, timeout_seconds=5)
    assert seen == ['127.1']     # literal layer admits; reaches the resolution check (refused there)


# E: short-page confirmation --------------------------------------------------------
from test_recent_review_r4 import Capped20
from test_recent_collector_bootstrap import until_decided, probes


class Head20(Labeler):
    """Cap 20; size 325 so the probe at 320 serves a genuine 5-row head page."""
    arrivals = 0

    def query(self, cursor, limit):
        body = super().query(cursor, min(limit, 20))
        if 0 < len(body['labels']) < 20 and self.arrivals:
            self.size += self.arrivals; self.arrivals = 0     # new rows land before the confirmation
        return body


def test_e1_mid_history_short_page_then_full_is_not_head(store):
    clock = Clock(); lab = Capped20(A, 200_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A); lab.short_at = '320'
    row = until_decided(store, t, clock, A)
    print('E1 probes', row['bootstrap_probes'], row['bootstrap'], row['bootstrap_cursor'])
    assert row['bootstrap'] == 'established' and int(row['bootstrap_cursor']) > 100_000


@pytest.mark.parametrize('arrivals', [0, 3])
def test_e2_genuine_head_short_page_confirmed(store, arrivals):
    clock = Clock(); lab = Head20(A, 325, recent=325); lab.arrivals = arrivals
    t = Transport(clock, [lab]); enroll(store, A)
    row = until_decided(store, t, clock, A)
    print('E2', arrivals, [c[1] for c in probes(t)], row['bootstrap_probes'], row['bootstrap_cursor'])
    assert row['bootstrap'] == 'established' and row['bootstrap_cursor'] == '320'
    assert row['bootstrap_probes'] == 4


# G: WAL under lease-free readers ---------------------------------------------------
import threading
import time


def test_g_eight_lease_free_readers_sixty_seconds_wal_bounded(store):
    from storage import connect, db
    budget = 1 << 20
    with connect(store.state) as c:
        db.set_meta(c, 'q:wal_budget_bytes', str(budget)); c.commit()
    labs = [Labeler(f'did:plc:w{i:02d}', 10, recent=10, growth=40) for i in range(20)]
    clock = Clock(); t = Transport(clock, labs); enroll(store, *(l.did for l in labs))
    stop, sizes, reads, errors = threading.Event(), [], [0], []
    wal = store.root/'state.sqlite-wal'
    def reader():
        while not stop.is_set():
            try:
                c = sqlite3.connect(f'file:{store.state}?mode=ro', uri=True, timeout=5)
                c.execute('BEGIN'); c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()
                c.execute('SELECT COUNT(*) FROM q_recent_sources').fetchone(); time.sleep(0.02)
                c.execute('COMMIT'); c.close(); reads[0] += 1
            except Exception as exc:
                errors.append(repr(exc))
    threads = [threading.Thread(target=reader) for _ in range(8)]
    for th in threads: th.start()
    end, ticks, held = time.monotonic()+60, 0, []
    try:
        while time.monotonic() < end:
            out = Collector(store, t, limits=Limits(seconds=30), monotonic=clock.monotonic, now=clock.now,
                            sleep=clock.sleep).tick()
            held.append(out['round'].get('store_hold')); ticks += 1; clock.t += 31
            try: sizes.append(wal.stat().st_size)
            except FileNotFoundError: sizes.append(0)
    finally:
        stop.set()
        for th in threads: th.join()
    accepted = sum(status(store, l.did)['accepted_rows'] for l in labs)
    print('G1 ticks', ticks, 'reads', reads[0], 'max_wal', max(sizes), 'last', sizes[-1], 'accepted', accepted,
          'holds', {h: held.count(h) for h in set(held)}, 'errors', errors[:3])
    assert errors == []
    assert max(sizes) <= budget + (1 << 20), max(sizes)
    assert accepted > 1000


def test_g_pinned_wal_records_one_maintenance_gap_no_source_failure(store):
    from storage import connect, db
    with connect(store.state) as c:
        db.set_meta(c, 'q:wal_budget_bytes', '4096'); c.commit()
    clock = Clock(); t = Transport(clock, [Labeler(A, 3, recent=3), Labeler(B, 3, recent=3)]); enroll(store, A, B)
    pin = sqlite3.connect(f'file:{store.state}?mode=ro', uri=True)
    pin.execute('BEGIN'); pin.execute('SELECT COUNT(*) FROM meta').fetchone()
    try:
        with connect(store.state) as c:
            c.execute('PRAGMA wal_autocheckpoint=0')
            for i in range(50): db.set_meta(c, 'q:r5_filler_%d' % i, 'x'*2000)
            c.commit()
        out = run(store, t, clock)[0]
    finally:
        pin.close()
    print('G2', out['round'], gaps(store))
    # C4-full ruling (2026-10-09) supersedes the r4 round stop: ingest has priority,
    # the pinned WAL is one store-level maintenance gap plus a wal_pinned disclosure.
    assert out['round']['status'] == 'ran' and out['round']['store_hold'] is None
    assert [g[2] for g in gaps(store)] == ['maintenance']
    assert store.collector_bootstrap_summary()['wal_pinned'] is not None
    for d in (A, B):
        row = status(store, d)
        assert row['failures'] == 0 and row['unavailable_since'] is None, row


# F: v8 refusal ---------------------------------------------------------------------
import importlib.util
from pathlib import Path

MSG = 'recent schema v8 required; no implicit conversion; old evidence preserved'


def test_f_store_created_by_cc03b68_code_is_refused_exactly(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'tools'/'segmented_qualification'))
    import storage
    monkeypatch.setattr(storage, 'ROOT', tmp_path)
    spec = importlib.util.spec_from_file_location('recent_storage_cc03b68',
                                                  Path(__file__).parent/'fixtures_r5'/'recent_storage_cc03b68.py')
    old = importlib.util.module_from_spec(spec); spec.loader.exec_module(old)
    old.RecentStore.create(tmp_path/'store', NOW.isoformat())
    from recent_storage import RecentStore
    s = RecentStore(tmp_path/'store')
    with sqlite3.connect(s.state) as c:
        assert c.execute("SELECT value FROM meta WHERE key='q:recent_version'").fetchone()[0] == '7'
    for call in (lambda: s.collector_sources(10), lambda: s.collector_bootstrap_summary(), lambda: s.frontier(),
                 lambda: s.collector_claim_source(10, NOW.isoformat(), probing=True)):
        with pytest.raises(RuntimeError) as err:
            call()
        assert str(err.value) == MSG


@pytest.mark.parametrize('col', ['origin', 'last_request', 'failures', 'hot', 'next_due', 'boot_lo'])
def test_f_v8_store_missing_column_is_refused_not_crashed(store, col):
    with sqlite3.connect(store.state) as c:
        c.execute(f'ALTER TABLE q_recent_sources DROP COLUMN {col}')
    for call in (lambda: store.collector_sources(10), lambda: store.collector_bootstrap_summary(),
                 lambda: store.collector_claim_source(10, NOW.isoformat()),
                 lambda: store.collector_claim_source(10, NOW.isoformat(), probing=True)):
        with pytest.raises(RuntimeError) as err:
            call()
        assert str(err.value) == MSG, (col, repr(err.value))
