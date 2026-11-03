"""Discovery failure is bounded metadata, not loss of enrolled-source observations."""
import json
import sqlite3
from datetime import timedelta

import pytest

from labelwatch.recent_collector import Collector, Limits, Refused
from test_recent_collector_bootstrap import A, NOW, Clock, Labeler, Transport, store  # noqa: F401
from test_recent_collector_capacity import ready


class DiscoveryTransport(Transport):
    def __init__(self, clock, failure=True, terminal=True):
        super().__init__(clock, [Labeler(A, 1, recent=1)])
        self.failure, self.terminal = failure, terminal

    def request(self, operation, params, **limits):
        if operation == 'discover':
            self.clock.t += .01
            if self.failure:
                raise TimeoutError('discovery down')
            return json.dumps({'sources': [], 'cursor': None if self.terminal else 'next-page'}).encode()
        return super().request(operation, params, **limits)


def run(store, clock, transport):
    return Collector(store, transport, limits=Limits(pages=1), now=clock.now,
                     monotonic=clock.monotonic).run_round(discover=True)


def test_repeated_discovery_outage_does_not_overflow_collection_gaps(store):
    ready(store, A)
    clock = Clock()
    transport = DiscoveryTransport(clock)
    for _ in range(129):
        result = run(store, clock, transport)
        assert result['collection']['round']['status'] == 'ran'
        clock.t += 120
    with sqlite3.connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0] == 0
        assert c.execute("SELECT value FROM meta WHERE key='q:recent_gap_overflow'").fetchone()[0] == '0'
        assert c.execute('SELECT accepted_rows FROM q_recent_sources').fetchone()[0] == 1
        assert c.execute('SELECT last_observed FROM q_recent_sources').fetchone()[0] is not None
        assert c.execute("SELECT COUNT(*) FROM meta WHERE key='q:recent_discovery_status'").fetchone()[0] == 1
    assert store.collector_discovery_cursor() is None


def test_discovery_failure_is_persistent_bounded_and_recovers_without_rewriting_gaps(store):
    from recent_storage import RecentStore
    from recent_publish import status_snapshot
    ready(store, A)
    clock = Clock()
    transport = DiscoveryTransport(clock, failure=False)
    run(store, clock, transport)
    success = store.collector_discovery_status()
    assert success['status'] == 'accepted' and success['terminal'] is True
    clock.t += 120
    transport.failure = True
    run(store, clock, transport)
    failed = RecentStore(store.root).collector_discovery_status()
    assert failed['status'] == 'refused' and failed['reason'] == 'TimeoutError'
    assert failed['last_success_at'] == success['attempted_at']
    since = failed['unavailable_since']
    store.record_gap('2026-10-08T12:01:00Z', '2026-10-08T12:01:01Z', 'unknown')
    clock.t += 120
    run(store, clock, transport)
    assert store.collector_discovery_status()['unavailable_since'] == since
    snapshot = status_snapshot(store, clock.now())
    assert snapshot['discovery']['status'] == 'refused'
    assert snapshot['coverage']['status'] == 'unknown'
    clock.t += 120
    transport.failure = False
    run(store, clock, transport)
    recovered = store.collector_discovery_status()
    assert recovered['status'] == 'accepted' and recovered['unavailable_since'] is None
    with sqlite3.connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0] == 1


def test_missing_stale_or_incomplete_discovery_keeps_roster_coverage_unknown(store):
    from recent_publish import status_snapshot
    ready(store, A)
    clock = Clock()
    assert status_snapshot(store, clock.now())['coverage']['status'] == 'unknown'
    transport = DiscoveryTransport(clock, failure=False, terminal=False)
    run(store, clock, transport)
    assert status_snapshot(store, clock.now())['coverage']['status'] == 'unknown'
    transport.terminal = True
    clock.t += 120
    run(store, clock, transport)
    assert status_snapshot(store, clock.now())['discovery']['current'] is True
    assert status_snapshot(store, clock.now()+timedelta(seconds=601))['discovery']['current'] is False
    assert status_snapshot(store, clock.now()+timedelta(seconds=601))['coverage']['status'] == 'unknown'


def test_discovery_outcome_capacity_refusal_remains_a_store_refusal(store, monkeypatch):
    ready(store, A)
    clock = Clock()
    def refused(*args, **kwargs):
        raise RuntimeError('shared_reserve_unavailable')
    monkeypatch.setattr(store, 'record_discovery_outcome', refused)
    result = run(store, clock, DiscoveryTransport(clock))
    assert result['discovery']['reason'] == 'TimeoutError'
    assert result['collection']['round']['reason'] == 'shared_reserve_unavailable'
    with sqlite3.connect(store.state) as c:
        assert c.execute('SELECT last_observed FROM q_recent_sources').fetchone()[0] is None


def test_recent_complete_discovery_remains_current_during_next_scan(store):
    from recent_publish import status_snapshot
    ready(store, A)
    clock = Clock()
    transport = DiscoveryTransport(clock, failure=False, terminal=True)
    run(store, clock, transport)
    completed = store.collector_discovery_status()['last_complete_at']
    clock.t += 120
    transport.terminal = False
    run(store, clock, transport)
    discovery = status_snapshot(store, clock.now())['discovery']
    assert discovery['last_complete_at'] == completed
    assert discovery['terminal'] is False and discovery['current'] is True
    clock.t += 120
    transport.failure = True
    run(store, clock, transport)
    assert status_snapshot(store, clock.now())['discovery']['current'] is False

def test_existing_overflow_and_gap_history_are_preserved(store):
    ready(store, A)
    clock = Clock()
    with sqlite3.connect(store.state) as c:
        for n in range(128):
            start = NOW + timedelta(seconds=10*n)
            c.execute('INSERT INTO q_recent_gaps VALUES (?,?,?)',
                      (start.isoformat(), (start+timedelta(seconds=1)).isoformat(), 'source_unavailable'))
        c.execute("UPDATE meta SET value='1' WHERE key='q:recent_gap_overflow'")
        c.execute("INSERT INTO meta VALUES ('q:recent_gap_overflow_start','2026-10-08T12:30:00.000000Z')")
        c.execute("INSERT INTO meta VALUES ('q:recent_gap_overflow_end','2026-10-08T12:31:00.000000Z')")
        before = list(c.execute('SELECT start,end,reason FROM q_recent_gaps ORDER BY start'))
        meta_before = list(c.execute("SELECT key,value FROM meta WHERE key LIKE 'q:recent_gap_overflow%' ORDER BY key"))
    run(store, clock, DiscoveryTransport(clock))
    with sqlite3.connect(store.state) as c:
        assert list(c.execute('SELECT start,end,reason FROM q_recent_gaps ORDER BY start')) == before
        assert list(c.execute("SELECT key,value FROM meta WHERE key LIKE 'q:recent_gap_overflow%' ORDER BY key")) == meta_before


def test_recovery_predecessor_and_discovery_share_status_without_losing_api_presentation(store):
    from recent_storage import RecentStore
    from recent_publish import status_snapshot, write_status
    from recent_provider import RecentProvider
    from labelwatch.recent_observations import RecentObservations
    from test_recent_site import module, get
    predecessor = {'schema': 'labelwatch.predecessor.v1', 'recorded': True,
                   'generation_id': '12345678-1234-4567-9234-567812345678', 'started_at': '2026-10-06T12:00:00Z',
                   'observed_through': '2026-10-07T12:00:00Z', 'ended_on': None,
                   'end_reason': 'store_lost', 'basis': 'predecessor_receipt'}
    successor = RecentStore.create(store.root.parent/'after-loss', NOW.isoformat(), predecessor=predecessor)
    successor.record_discovery_outcome({'status': 'accepted', 'terminal': True}, NOW.isoformat())
    snapshot = status_snapshot(successor, NOW)
    assert snapshot['predecessor']['generation_id'] == '12345678-1234-4567-9234-567812345678'
    assert snapshot['predecessor']['end_reason'] == 'store_lost'
    assert snapshot['discovery']['current'] is True
    assert any(g['reason'] == 'observed_then_lost' for g in snapshot['coverage']['gaps'])
    write_status(successor, now=NOW)
    mod = module()
    product = RecentObservations(RecentProvider(successor), now=lambda: NOW)
    site = mod.SiteStatus(product)
    handler = mod.handler(product, status=site)
    assert site.health()[1]['discovery']['current'] is True
    assert 'Use the observations' in get(handler, '/api')[2].decode()
    assert 'The previous epoch was lost' in get(handler, '/previous-epoch')[2].decode()
    landing = get(handler, '/')[2].decode()
    assert 'Continuity break' in landing and 'API &amp; data' in landing
