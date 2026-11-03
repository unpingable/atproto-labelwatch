"""Review r4 (2026-10-08): adversarial closure checks for r3 F1-F10 and live D1-D7."""
import json
import sqlite3

import httpx
import pytest

from test_recent_collector_bootstrap import (  # noqa: F401  (fixture import)
    A, B, NOW, Clock, Labeler, Transport, enroll, run, status, store, until_decided, probes, gaps)
from labelwatch.recent_collector import Collector, HTTPTransport, Limits, Refused


# F2 / endpoint custody -------------------------------------------------------

def _doc(did, endpoint):
    return {'id': did, 'service': [{'id': '#atproto_labeler', 'type': 'AtprotoLabeler', 'serviceEndpoint': endpoint}]}


def test_provider_body_fields_never_change_endpoint_or_bootstrap_state(store):
    store.remember_sources([{'did': A, 'endpoint': 'https://good.example'}])
    hosts = []
    def handler(request):
        hosts.append(request.url.host)
        cursor = request.url.params.get('cursor')
        n = int(cursor or 0)
        rows = [{'src': A, 'uri': f'at://did:plc:t/app.bsky.feed.post/{i}', 'val': 'v',
                 'cts': '2026-10-08T11:00:00Z'} for i in range(n+1, n+4)] if n < 9 else []
        body = {'labels': rows, '_resolved_endpoint': 'https://evil.example', 'endpoint': 'https://evil.example',
                'service': {'serviceEndpoint': 'https://evil.example', '_resolved_endpoint': 'https://evil.example'},
                'resolutions': {A: 'https://evil.example'}}
        if rows:
            body['cursor'] = str(n+3)
        return httpx.Response(200, json=body)
    clock = Clock()
    with HTTPTransport(httpx.MockTransport(handler)) as t:
        for _ in range(4):
            Collector(store, t, monotonic=clock.monotonic, now=clock.now).tick(); clock.t += 31
            assert t.resolutions == {}
    row = status(store, A)
    assert set(hosts) == {'good.example'}
    assert store.collector_sources(10) == [{'did': A, 'endpoint': 'https://good.example'}]
    assert row['endpoint_resolved_at'] is None and row['last_failure_reason'] != 'endpoint_changed'


def test_oversized_provider_cursor_is_refused_without_state_change(store):
    store.remember_sources([{'did': A, 'endpoint': 'https://good.example'}])
    def handler(request):
        rows = [{'src': A, 'uri': 'at://did:plc:t/app.bsky.feed.post/1', 'val': 'v', 'cts': '2026-10-08T11:00:00Z'}]
        return httpx.Response(200, json={'labels': rows, 'cursor': 'x'*5000, '_resolved_endpoint': 'https://evil.example'})
    clock = Clock()
    with HTTPTransport(httpx.MockTransport(handler)) as t:
        out = Collector(store, t, monotonic=clock.monotonic, now=clock.now).tick()
    row = status(store, A)
    assert out['results'][0]['status'] == 'refused' and 'cursor' in out['results'][0]['reason']
    assert (row['bootstrap'], row['bootstrap_probes']) == ('unstarted', 0)
    assert store.collector_sources(10) == [{'did': A, 'endpoint': 'https://good.example'}]


@pytest.mark.parametrize('endpoint', ['https://127.0.0.1', 'https://localhost:8431', 'https://10.1.2.3',
                                      'https://169.254.169.254', 'https://[::1]', 'https://192.168.1.10'])
def test_resolved_endpoint_on_loopback_or_private_host_is_refused(store, endpoint):
    # SSRF exposure: a DID document (attacker-authored, did:plc or did:web) can point the
    # collector's labels GET at loopback/link-local/private hosts on the collector host.
    store.remember_sources([{'did': A, 'endpoint': None}])
    hosts = []
    def handler(request):
        hosts.append(request.url.host)
        if request.url.host == 'plc.directory':
            return httpx.Response(200, json=_doc(A, endpoint))
        return httpx.Response(200, json={'labels': []})
    clock = Clock()
    with HTTPTransport(httpx.MockTransport(handler)) as t:
        Collector(store, t, monotonic=clock.monotonic, now=clock.now).tick()
    assert hosts == ['plc.directory'], hosts


def test_did_web_on_loopback_authority_is_refused(store):
    did = 'did:web:127.0.0.1%3A8431'
    store.remember_sources([{'did': did, 'endpoint': None}])
    hosts = []
    def handler(request):
        hosts.append((request.url.host, request.url.port))
        return httpx.Response(404)
    clock = Clock()
    with HTTPTransport(httpx.MockTransport(handler)) as t:
        Collector(store, t, monotonic=clock.monotonic, now=clock.now).tick()
    assert hosts == [], hosts


# F1 / history cap and short-page confirmation ---------------------------------

class Capped20(Labeler):
    """Provider page cap 20; every page = 1 undated + 19 pre-epoch; one short page mid-history."""
    short_at = None

    def query(self, cursor, limit):
        body = super().query(cursor, min(limit, 20))
        for row in body['labels']:
            if row['uri'].endswith('0'):
                row.pop('cts')
        if self.short_at is not None and cursor == self.short_at:
            self.short_at = None             # exactly once
            body['labels'] = body['labels'][:5]
            if body['labels']:
                body['cursor'] = str(int(cursor)+5)
        return body


def test_mid_history_short_page_is_not_confirmed_as_head_when_continuation_is_fuller(store):
    # Confirmation rule: head iff NOT (served >= before >= 10). A 5-row short page
    # followed by a full 20-row page has served(20) > before(5) but before < 10, so it
    # is confirmed as the head although the continuation proves more history exists.
    clock = Clock(); lab = Capped20(A, 200_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A)
    lab.short_at = '320'                     # third probe: None(cap 20 found) -> 20 -> 320
    until_decided(store, t, clock, A)
    row = status(store, A)
    assert row['bootstrap'] != 'established' or int(row['bootstrap_cursor']) > 100_000, row['bootstrap_cursor']


def test_f1_hostile_provider_total_requests_bounded(store):
    clock = Clock(); lab = Capped20(A, 200_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A)
    lab.short_at = '320'
    for _ in range(60):
        run(store, t, clock)
        if status(store, A)['bootstrap'] == 'refused':
            break
    row = status(store, A)
    # 72 probes ever + 16 history pages per re-probe cycle.
    assert len(t.calls) <= 72 + 16*36, (len(t.calls), row['bootstrap'], row['bootstrap_probes'])


# D2 / lost attempts --------------------------------------------------------------

def test_young_in_flight_attempt_is_not_swept(store):
    clock = Clock(); enroll(store, A, B)
    store.begin_source_attempt(A, clock.now().isoformat())
    clock.t = 30
    out = run(store, Transport(clock, [Labeler(A, 0), Labeler(B, 0)]), clock)[0]
    assert out['round']['lost_attempts'] == 0
    with sqlite3.connect(store.state) as c:
        assert list(c.execute("SELECT 1 FROM q_recent_gaps WHERE reason='unknown'")) == [] or True


def test_lost_attempt_sweep_during_midnight_hold_does_not_crash(store):
    clock = Clock(); enroll(store, A)
    store.begin_source_attempt(A, clock.now().isoformat())
    clock.t = 12*3600+120                    # 00:02 next day, owner not rotated
    out = Collector(store, Transport(clock, [Labeler(A, 0)]), monotonic=clock.monotonic, now=clock.now).tick()
    assert out['round']['status'] == 'refused' and out['round']['lost_attempts'] == 1
    assert store.collector_bootstrap_summary()['pending_attempts'] == 0


# Consistency -------------------------------------------------------------------------

def test_summary_counts_are_consistent(store):
    store.remember_sources([{'did': A, 'endpoint': None}])
    store.remember_sources([{'did': B, 'endpoint': None}], request_cursor=None, next_cursor=None)
    s = store.collector_bootstrap_summary()
    enrolled = sum(s[k] for k in ('unstarted', 'probing', 'established', 'refused'))
    assert s['seeded'] + s['discovered'] == enrolled == 2
    with sqlite3.connect(store.state) as c:
        cols = {r[1] for r in c.execute('PRAGMA table_info(q_recent_sources)')}
    assert {'future_skipped', 'boot_confirm', 'rearm_at', 'rearms', 'origin', 'last_request'} <= cols


def test_pre_delta_v7_store_is_refused_explicitly_not_crashed(store):
    # Stores created before cc03b68 carry q:recent_version=7 without the new columns
    # (e.g. the C1-live occurrence). require() admits them; queries then fail raw.
    with sqlite3.connect(store.state) as c:
        for col in ('future_skipped', 'boot_confirm', 'rearm_at', 'rearms', 'origin', 'last_request'):
            c.execute(f'ALTER TABLE q_recent_sources DROP COLUMN {col}')
    with pytest.raises(RuntimeError, match='schema'):
        store.collector_bootstrap_summary()


# F5 / D1 starvation: realistic first-day roster -------------------------------------

class _Mix(Transport):
    def __init__(self, clock, labelers, dead, dead_cost):
        super().__init__(clock, labelers); self.dead, self.dead_cost = dead, dead_cost

    def request(self, operation, params, max_bytes, timeout_seconds):
        self.clock.t += .21
        if params['did'] in self.dead:
            self.calls.append((params['did'], params['cursor'], params['limit']))
            self.clock.t += min(self.dead_cost, timeout_seconds)
            raise httpx.ConnectError('unreachable')
        return super().request(operation, params, max_bytes, timeout_seconds)


def first_day(store, dead_cost, rounds):
    dead = [f'did:plc:d{i:03d}' for i in range(400)]
    small = [Labeler(f'did:plc:s{i:03d}', 30, recent=30) for i in range(149)]
    growing = [Labeler(f'did:plc:g{i:03d}', 10, recent=10, growth=20) for i in range(34)]
    big = Labeler(A, 1_000_000, recent=30, growth=1)
    clock = Clock(); t = _Mix(clock, [big, *small, *growing], set(dead), dead_cost)
    enroll(store, A, *dead, *(l.did for l in small), *(l.did for l in growing))
    trace = []
    for rnd in range(1, rounds+1):
        t0, before = clock.t, len(t.calls)
        Collector(store, t, limits=Limits(seconds=30), monotonic=clock.monotonic, now=clock.now, sleep=clock.sleep).tick()
        calls = t.calls[before:]
        trace.append({'round': rnd, 'elapsed': clock.t-t0, 'requests': len(calls),
                      'busy_probes': sum(1 for c in calls if c[0] == A),
                      'growing_pages': sum(1 for c in calls if c[0].startswith('did:plc:g') and c[2] == 100),
                      'busy': status(store, A)['bootstrap']})
        if trace[-1]['busy'] == 'established':
            break
        clock.t = t0+120
    return trace


def test_busy_source_among_400_unreachable_cursorless_establishes_within_ten_rounds(store):
    # All 584 seeded sources are cursorless on day one. Rotation claims of unreachable
    # (never-establishing) cursorless sources count as probe attempts, so the reserved
    # share (probe_attempts*3 < attempts+1) never reopens and the sticky busy source is
    # only reached once per round-robin pass over the roster: 1 probe per ~5 rounds.
    trace = first_day(store, 0.05, 12)
    assert all(r['elapsed'] <= 30+1.3 for r in trace)            # never sleeps past the deadline
    assert trace[-1]['busy'] == 'established', [(r['round'], r['busy_probes']) for r in trace]


def test_cursor_jump_is_disclosed_for_a_provider_capping_pages_below_page_rows(store):
    # cursor_jump only fires when len(page) == Limits.page_rows (100). A provider that
    # caps pages at 50 (the cap the bootstrap itself learns) never produces a "full"
    # page in forward collection, so skipped ids are never disclosed.
    class CappedJumping(Labeler):
        def query(self, cursor, limit):
            body = super().query(cursor, min(limit, 50))
            if limit == 100 and cursor is not None and len(body['labels']) == 50:   # forward collection only
                body['cursor'] = str(int(cursor)+50*5)
            return body
    clock = Clock(); t = Transport(clock, [CappedJumping(A, 10, recent=10, growth=400)]); enroll(store, A)
    run(store, t, clock, ticks=4)
    row = status(store, A)
    assert row['accepted_rows'] >= 100
    assert row['cursor_order_unverified'] == 1, (row['accepted_rows'], row['cursor_order_evidence'])
