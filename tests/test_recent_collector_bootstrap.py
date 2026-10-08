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
