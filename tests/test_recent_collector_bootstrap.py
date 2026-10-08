"""Bounded head establishment for cursorless sources (BOOTSTRAP.md).

The fixture labeler reproduces the provider behaviour verified on 2026-10-08
(evidence protocol-bootstrap-55f079ff): decimal id cursors, rows with id >
cursor in ascending order, next cursor = last id, empty page beyond the head
(no cursor on Ozone, cursor "0" on skyware-style servers), limit honoured.
"""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from labelwatch.recent_collector import Collector, Limits, TOTAL_PROBES

NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
A, B = 'did:plc:aaaa', 'did:plc:bbbb'


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'tools'/'segmented_qualification'))
    import storage
    monkeypatch.setattr(storage, 'ROOT', tmp_path)
    from recent_storage import RecentStore
    return RecentStore.create(tmp_path/'store', NOW.isoformat())


class Clock:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def now(self):
        return NOW+timedelta(seconds=self.t)


class Labeler:
    """History of `size` labels; ids 1..size; old ids are source-dated long before the epoch."""

    def __init__(self, did, size, recent=0, growth=0, skyware=False, cts=None, opaque=False, regress=False):
        self.did, self.size, self.recent, self.growth = did, size, recent, growth
        self.skyware, self.cts, self.opaque, self.regress = skyware, cts, opaque, regress
        self.recent_from = size-recent        # ids above this (and all new ids) are recent

    def label_cts(self, i):
        if self.cts:
            return self.cts(i)
        if i > self.recent_from:
            return '2026-10-07T00:00:00Z'      # inside the 48 h allowance
        return '2024-01-01T00:00:00Z'          # pre-epoch history

    def query(self, cursor, limit):
        start = int(cursor or 0)
        ids = range(start+1, min(self.size, start+limit)+1)
        rows = [{'src': self.did, 'uri': f'at://did:plc:t{i % 97}/app.bsky.feed.post/{i}', 'val': 'v',
                 'cts': self.label_cts(i)} for i in ids]
        self.size += self.growth             # the head moves while we probe
        body = {'labels': rows}
        if rows:
            last = ids[-1]
            body['cursor'] = (f'opaque:{last}' if self.opaque else
                              str(max(0, start-5)) if self.regress and cursor is not None else str(last))
        elif self.skyware:
            body['cursor'] = '0'
        return body


class Transport:
    def __init__(self, clock, labelers):
        self.clock, self.labelers, self.calls = clock, {l.did: l for l in labelers}, []

    def request(self, operation, params, max_bytes, timeout_seconds):
        assert operation == 'labels' and params['limit'] <= 100
        self.calls.append((params['did'], params['cursor'], params['limit']))
        self.clock.t += .01
        return json.dumps(self.labelers[params['did']].query(params['cursor'], params['limit'])).encode()


def probes(transport):
    # Probe requests use limit 50; ordinary collection uses Limits.page_rows (100).
    return [c for c in transport.calls if c[2] == 50]


def enroll(store, *dids):
    store.remember_sources([{'did': d, 'endpoint': 'https://fixture.invalid'} for d in dids])


def run(store, transport, clock, ticks=1, **limits):
    out = []
    for _ in range(ticks):
        out.append(Collector(store, transport, limits=Limits(**limits), monotonic=clock.monotonic, now=clock.now).tick())
        clock.t += 31        # past the quiet interval
    return out


def status(store, did):
    return next(r for r in store.collector_source_status(2048) if r['did'] == did)


def until_decided(store, transport, clock, did, ticks=40, **limits):
    for _ in range(ticks):
        run(store, transport, clock, **limits)
        if status(store, did)['bootstrap'] in ('established', 'refused'):
            return status(store, did)
    raise AssertionError('bootstrap did not decide')


def test_million_label_history_reaches_moving_head_within_bound(store):
    clock = Clock(); lab = Labeler(A, 1_000_000, recent=30, growth=3, skyware=True)
    t = Transport(clock, [lab]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'established' and final['bootstrap_probes'] <= TOTAL_PROBES
    assert len(probes(t)) == final['bootstrap_probes'] <= 68
    established = int(final['bootstrap_cursor'])
    assert 1_000_000-50 <= established <= 1_000_000+3*len(probes(t))   # near the head, never the beginning
    # Continue collecting: catches up to the moving head and keeps every new label.
    lab.growth = 0
    for _ in range(3):
        run(store, t, clock)
    assert int(store.collector_cursor(A)) == lab.size
    # An empty skyware page answers cursor "0": the stored position must not reset.
    run(store, t, clock)
    assert int(store.collector_cursor(A)) == lab.size
    with sqlite3.connect(store.state) as c:
        accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
    skipped = status(store, A)['pre_epoch_skipped']
    assert accepted+skipped == lab.size-established and accepted >= 30 and skipped < 50


def test_opaque_cursor_refused_with_unknown_coverage_and_never_traversed(store):
    clock = Clock(); t = Transport(clock, [Labeler(A, 10_000, opaque=True)]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'refused' and final['bootstrap_reason'] == 'bootstrap_refused:opaque_cursor'
    calls = len(t.calls)
    run(store, t, clock, ticks=3)
    assert len(t.calls) == calls == 1              # refused source is never claimed again
    summary = store.collector_bootstrap_summary()
    assert summary['refused'] == 1 and summary['coverage'] == 'unknown'
    assert summary['refused_sources'] == [{'did': A, 'reason': 'bootstrap_refused:opaque_cursor'}]
    assert store.collector_cursor(A) is None


def test_small_source_with_opaque_cursor_is_established_from_one_short_page(store):
    clock = Clock(); t = Transport(clock, [Labeler(A, 3, recent=3, opaque=True)]); enroll(store, A)
    result = run(store, t, clock)[0]['results'][0]
    assert result['bootstrap'] == 'established' and result['result']['inserted'] == 3
    assert store.collector_cursor(A) == 'opaque:3'


def test_cursor_regression_on_probe_refused(store):
    clock = Clock(); t = Transport(clock, [Labeler(A, 10_000, regress=True)]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'refused' and final['bootstrap_reason'] == 'bootstrap_refused:cursor_regression'


def test_nonmonotone_source_time_is_not_a_refusal_but_rows_are_filtered(store):
    # Verified on Ozone (fran.pw, urbanism.plus): queryLabels renders current state
    # for old ids, so cts is not monotone in id order. Only the id cursor is required.
    cts = lambda i: '2026-10-07T00:00:00Z' if i % 2 else '2023-01-01T00:00:00Z'
    clock = Clock(); t = Transport(clock, [Labeler(A, 5_000, cts=cts)]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'established'
    run(store, t, clock, ticks=3)
    row = status(store, A)
    with sqlite3.connect(store.state) as c:
        accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
    assert accepted and row['pre_epoch_skipped'] and abs(accepted-row['pre_epoch_skipped']) <= 1


def test_restart_mid_probing_resumes_from_persisted_bracket(store):
    from recent_storage import RecentStore
    clock = Clock(); lab = Labeler(A, 1_000_000)
    t = Transport(clock, [lab]); enroll(store, A)
    run(store, t, clock, ticks=4, pages=1)
    state = store.collector_source_state(A)
    assert state['phase'] == 'probing' and state['probes'] == 4 and state['lo'] is not None
    lo, hi = int(state['lo']), state['hi'] and int(state['hi'])
    expected = (lo+hi)//2 if hi else min(lo*16, 2**53)
    # New process: new store handle, new collector, new transport object.
    t2 = Transport(clock, [lab])
    run(RecentStore(store.root), t2, clock, pages=1)
    assert t2.calls[0][1] == str(expected)
    assert store.collector_source_state(A)['probes'] == 5


def test_lost_process_between_request_and_persist_repeats_only_that_probe(store):
    clock = Clock(); lab = Labeler(A, 1_000_000)
    t = Transport(clock, [lab]); enroll(store, A)
    run(store, t, clock, ticks=2, pages=1)
    before = store.collector_source_state(A)
    class Lost(BaseException):
        pass
    def lost(*args, **kwargs):
        raise Lost()
    store.bootstrap_step = lost
    with pytest.raises(Lost):
        run(store, t, clock, pages=1)
    del store.bootstrap_step
    assert store.collector_source_state(A) == before      # nothing advanced
    repeated = t.calls[-1][1]
    run(store, t, clock, pages=1)
    assert t.calls[-1][1] == repeated                     # same probe, not from scratch
    with sqlite3.connect(store.state) as c:
        assert c.execute("SELECT COUNT(*) FROM q_recent_gaps WHERE reason='unknown'").fetchone()[0] == 1


def test_per_tick_probe_share_defers_without_looping(store):
    clock = Clock(); t = Transport(clock, [Labeler(A, 1_000_000)]); enroll(store, A)
    result = run(store, t, clock, pages=128, bootstrap_probes=3)[0]
    assert len(t.calls) == 3 and result['attempted'] == 3
    assert store.collector_source_state(A)['phase'] == 'probing'
    run(store, t, clock, pages=128, bootstrap_probes=3)
    assert len(t.calls) == 6


def test_total_probe_budget_exhaustion_refuses_deterministically(store, monkeypatch):
    import recent_storage
    monkeypatch.setitem(recent_storage.BOOTSTRAP_POLICY, 'total_probes', 5)
    clock = Clock(); t = Transport(clock, [Labeler(A, 1_000_000)]); enroll(store, A)
    run(store, t, clock, ticks=4)
    row = status(store, A)
    assert row['bootstrap'] == 'refused' and row['bootstrap_reason'] == 'bootstrap_refused:probe_budget_exhausted'
    assert len(t.calls) == 5


def test_pre_epoch_rows_skipped_counted_and_observed_time_is_acceptance(store):
    from recent_storage import micros
    cts = {1: '2026-10-01T00:00:00Z', 2: '2026-10-06T13:00:00Z', 3: 'not-a-time', 4: '2026-10-08T11:00:00Z',
           5: '2026-10-06T11:59:59Z'}
    clock = Clock(); t = Transport(clock, [Labeler(A, 5, cts=cts.get)]); enroll(store, A)
    result = run(store, t, clock)[0]['results'][0]
    assert result['bootstrap'] == 'established'
    assert (result['rows'], result['pre_epoch_skipped'], result['undated_skipped']) == (2, 2, 1)
    row = status(store, A)
    assert (row['pre_epoch_skipped'], row['undated_skipped']) == (2, 1)
    with sqlite3.connect(store.state) as c:
        observed = {r[0] for r in c.execute('SELECT observed_us FROM q_recent_seen')}
    assert len(observed) == 1
    stamp = observed.pop()
    assert micros(NOW) < stamp <= micros(NOW+timedelta(seconds=1))   # acceptance clock, not cts
    assert store.collector_bootstrap_summary()['pre_epoch_skipped'] == 2


def test_ready_sources_are_not_starved_by_probing(store):
    clock = Clock()
    big = Labeler(A, 1_000_000); small = Labeler(B, 10, recent=10, growth=2)
    t = Transport(clock, [big, small]); enroll(store, A, B)
    run(store, t, clock, pages=2)       # B establishes from one short page
    assert store.collector_source_state(B)['phase'] == 'established'
    for _ in range(3):
        before = len([c for c in t.calls if c[0] == B])
        run(store, t, clock, pages=6)
        assert len([c for c in t.calls if c[0] == B]) > before
    assert store.collector_source_state(A)['phase'] in ('probing', 'established')


@pytest.mark.parametrize('size', [0, 1, 49, 50, 51, 99, 4_097, 1_000_000, 2**40, 2**52])
def test_bootstrap_requests_per_source_bounded(store, size):
    clock = Clock(); t = Transport(clock, [Labeler(A, size, recent=min(size, 5))]); enroll(store, A)
    final = until_decided(store, t, clock, A, ticks=20, bootstrap_probes=16)
    assert final['bootstrap'] == 'established'
    assert len(probes(t)) == final['bootstrap_probes'] <= 68
    cursor = final['bootstrap_cursor']
    assert cursor is None if size < 50 else size-50 <= int(cursor) <= size


def test_full_terminal_history_at_cursor_max_refused(store):
    clock = Clock(); t = Transport(clock, [Labeler(A, 2**53+1000)]); enroll(store, A)
    final = until_decided(store, t, clock, A, ticks=20, bootstrap_probes=16)
    assert final['bootstrap'] == 'refused' and final['bootstrap_reason'] == 'bootstrap_refused:cursor_out_of_range'
    assert len(t.calls) <= 68


# Review r1 gap policy: one source's failure is per-source state, not a store-wide gap.

class Dead:
    def __init__(self, clock, alive):
        self.clock, self.alive, self.calls = clock, alive, []

    def request(self, operation, params, max_bytes, timeout_seconds):
        self.calls.append(params['did'])
        self.clock.t += .01
        if params['did'] not in self.alive:
            raise TimeoutError('dead labeler')
        return json.dumps({'labels': []}).encode()


def gaps(store):
    with sqlite3.connect(store.state) as c:
        return list(c.execute('SELECT start,end,reason FROM q_recent_gaps ORDER BY start'))


def test_dead_source_is_per_source_unavailability_not_store_gaps(store):
    clock = Clock(); t = Dead(clock, {B}); enroll(store, A, B)
    for _ in range(40):                      # ~10 h, stays before the midnight owner hold
        run(store, t, clock)
        clock.t += 900                       # past the maximum failure backoff
    assert t.calls.count(A) >= 30 and gaps(store) == []
    row = status(store, A)
    assert row['unavailable_since'] == '2026-10-08T12:00:00.000000Z'
    assert row['last_failure_reason'] == 'TimeoutError' and row['failures'] == 6
    assert status(store, B)['unavailable_since'] is None
    summary = store.collector_bootstrap_summary()
    assert summary['unavailable'] == 1 and summary['coverage'] == 'unknown'
    assert summary['unavailable_sources'][0]['did'] == A
    t.alive.add(A)
    run(store, t, clock); clock.t += 900; run(store, t, clock)
    assert status(store, A)['unavailable_since'] is None
    assert store.collector_bootstrap_summary()['unavailable'] == 0


def test_daily_owner_hold_is_one_merged_maintenance_gap_and_stops_the_round(store):
    clock = Clock(); t = Dead(clock, {A, B}); enroll(store, A, B)
    clock.t = 12*3600+60                    # 2026-10-09T00:01Z, owner not rotated
    for _ in range(4):
        result = Collector(store, t, monotonic=clock.monotonic, now=clock.now).tick()
        assert result['round']['status'] == 'refused' and result['attempted'] == 0
        clock.t += 60
    assert t.calls == []                     # no source claimed or contacted
    assert gaps(store) == [('2026-10-09T00:00:00.000000Z', '2026-10-09T00:04:00.000000Z', 'maintenance')]


def test_gap_overflow_released_as_one_in_window_unknown_gap(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/'tools'/'segmented_qualification'))
    import storage
    root = tmp_path/'runtime'; root.mkdir(); (tmp_path/'evidence').mkdir()
    (tmp_path/'evidence'/'ARCHIVE-DESTINATION.json').write_text(json.dumps({'campaign_archive': str(root/'store/archive')}))
    monkeypatch.setattr(storage, 'ROOT', root)
    from recent_storage import RecentStore
    s = RecentStore.create(root/'store', '2026-09-01T00:00:00Z')
    base = datetime(2026, 9, 1, 1, tzinfo=timezone.utc)
    for n in range(128):
        lo = base+timedelta(seconds=10*n)
        s.record_gap(lo.isoformat(), (lo+timedelta(seconds=1)).isoformat(), 'ingest_paused')
    s.record_gap('2026-09-20T01:00:00Z', '2026-09-20T02:00:00Z', 'ingest_paused')   # dropped: overflow
    s.advance_day('2026-10-05T00:00:00Z')
    assert gaps(s) == [('2026-09-20T01:00:00.000000Z', '2026-09-20T02:00:00.000000Z', 'unknown')]
    with sqlite3.connect(s.state) as c:
        assert c.execute("SELECT value FROM meta WHERE key='q:recent_gap_overflow'").fetchone()[0] == '0'


def test_skipped_counts_are_per_source_and_summed(store):
    clock = Clock()
    labs = [Labeler(A, 2, cts={1: 'nope', 2: '2026-10-07T00:00:00Z'}.get),
            Labeler(B, 3, cts={1: '2020-01-01T00:00:00Z', 2: '2020-01-01T00:00:00Z', 3: 'garbage'}.get)]
    t = Transport(clock, labs); enroll(store, A, B)
    run(store, t, clock)
    assert (status(store, A)['pre_epoch_skipped'], status(store, A)['undated_skipped']) == (0, 1)
    assert (status(store, B)['pre_epoch_skipped'], status(store, B)['undated_skipped']) == (2, 1)
    summary = store.collector_bootstrap_summary()
    assert (summary['pre_epoch_skipped'], summary['undated_skipped']) == (2, 2)


def test_collect_command_exit_reflects_whole_round_only(store, monkeypatch, capsys):
    import recent_collect
    clock = Clock(); enroll(store, A, B)
    class Fixed:
        def __init__(self): self.inner = Dead(clock, {B})
        def __enter__(self): return self.inner
        def __exit__(self, *exc): return False
    monkeypatch.setattr(recent_collect, 'HTTPTransport', Fixed)
    monkeypatch.setattr(recent_collect, 'Collector', lambda s, tr: Collector(s, tr, monotonic=clock.monotonic, now=clock.now))
    assert recent_collect.main(['--store', str(store.root), '--network']) == 0
    out = json.loads(capsys.readouterr().out)
    assert out['collection']['refused_sources'] == [A]
    clock.t = 12*3600+60
    assert recent_collect.main(['--store', str(store.root), '--network']) == 1
    assert json.loads(capsys.readouterr().out)['collection']['round']['status'] == 'refused'


# Review r2 rulings.

def test_late_rows_are_bounded_at_acceptance_time_and_counted_apart_from_pre_epoch(store):
    # Day 10 of the epoch: a row 3 days old is inside the epoch but late; a row
    # older than the epoch floor is pre-epoch; a 1-day-old row is accepted.
    cts = {1: '2026-10-01T00:00:00Z', 2: '2026-10-15T12:00:00Z', 3: '2026-10-17T12:00:00Z'}
    clock = Clock(); clock.t = 9*86400
    t = Transport(clock, [Labeler(A, 3, cts=cts.get)]); enroll(store, A)
    store.rotate('2026-10-17')
    result = run(store, t, clock)[0]['results'][0]
    assert (result['rows'], result['pre_epoch_skipped'], result['late_skipped']) == (1, 1, 1)
    row = status(store, A)
    assert (row['pre_epoch_skipped'], row['late_skipped'], row['accepted_rows']) == (1, 1, 1)
    assert store.collector_bootstrap_summary()['late_skipped'] == 1


class Rejecting:
    def __init__(self, clock, status):
        self.clock, self.status, self.calls = clock, status, 0

    def request(self, operation, params, max_bytes, timeout_seconds):
        from labelwatch.recent_collector import Refused
        self.calls += 1; self.clock.t += .01
        raise Refused(f'provider status {self.status}')


@pytest.mark.parametrize('code,refused', [(404, True), (401, True), (503, False)])
def test_deterministic_provider_rejections_are_count_bounded(store, code, refused):
    clock = Clock(); t = Rejecting(clock, code); enroll(store, A)
    for _ in range(12):
        run(store, t, clock); clock.t += 900
    row = status(store, A)
    if refused:
        assert t.calls == 6 and row['bootstrap'] == 'refused'
        assert row['bootstrap_reason'] == f'bootstrap_refused:provider_rejects:{code}'
    else:
        assert t.calls == 12 and row['bootstrap'] == 'unstarted' and row['provider_rejects'] == 0


def test_resolved_endpoint_is_persisted_so_each_request_is_one_get(store):
    import httpx
    from labelwatch.recent_collector import HTTPTransport
    hosts = []
    def handler(request):
        hosts.append(request.url.host)
        if request.url.host == 'plc.directory':
            return httpx.Response(200, json={'id': A, 'service': [{'id': '#atproto_labeler', 'serviceEndpoint': 'https://labeler.test'}]})
        return httpx.Response(200, json={'labels': []})
    store.remember_sources([{'did': A, 'endpoint': None}])
    clock = Clock()
    with HTTPTransport(httpx.MockTransport(handler)) as transport:
        for _ in range(3):
            Collector(store, transport, monotonic=clock.monotonic, now=clock.now).tick(); clock.t += 31
    assert hosts == ['plc.directory', 'labeler.test', 'labeler.test', 'labeler.test']
    assert store.collector_sources(10) == [{'did': A, 'endpoint': 'https://labeler.test'}]
    clock.t += 86400          # daily re-resolution
    store.rotate('2026-10-09')
    with HTTPTransport(httpx.MockTransport(handler)) as transport:
        Collector(store, transport, monotonic=clock.monotonic, now=clock.now).tick()
    assert hosts[-2:] == ['plc.directory', 'labeler.test']


def test_overlapping_cursor_pages_mark_order_unverified_with_evidence(store):
    class Overlap(Labeler):
        def query(self, cursor, limit):
            # fran.pw-like: the page after a returned cursor repeats rows already served.
            body = super().query(None if cursor is None else str(max(0, int(cursor)-3)), limit)
            return body if cursor is None or int(body.get('cursor') or 0) > int(cursor) else {'labels': []}
    clock = Clock(); t = Transport(clock, [Overlap(A, 200, recent=200, growth=5)]); enroll(store, A)
    run(store, t, clock, ticks=3)
    row = status(store, A)
    assert row['cursor_order_unverified'] == 1 and json.loads(row['cursor_order_evidence'])['overlap'] == 3
    summary = store.collector_bootstrap_summary()
    assert summary['cursor_order_unverified'] == 1 and summary['coverage'] == 'unknown'
    assert row['accepted_rows'] >= 200 and row['bootstrap'] == 'established'   # rows still accepted, never refused


def test_v6_store_refused_without_conversion(store):
    from storage import connect as sconnect, db as sdb
    with sconnect(store.state) as c:
        sdb.set_meta(c, 'q:recent_version', '6')
    with pytest.raises(RuntimeError, match='recent schema v7 required; no implicit conversion'):
        store.collector_sources(10)
