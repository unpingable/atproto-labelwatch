"""Exact local reserve refusal remains a whole-round collector condition."""
import json
import sqlite3

import pytest

from labelwatch.recent_collector import Collector, Refused
from test_recent_collector_bootstrap import A, B, NOW, Clock, Labeler, Transport, enroll, status, store  # noqa: F401

REASON = 'shared_reserve_unavailable'


def ready(store, *dids):
    enroll(store, *dids)
    with sqlite3.connect(store.state) as c:
        c.execute("UPDATE q_recent_sources SET provider_cursor='0', boot_cursor='0', boot_phase='established'")


def positions(store):
    with sqlite3.connect(store.state) as c:
        return list(c.execute('SELECT did,provider_cursor,attempt_token,failures,unavailable_since FROM q_recent_sources ORDER BY did'))


def test_cli_impossible_real_policy_refuses_before_transport_and_writes_precise_status(store, monkeypatch, capsys):
    import recent_collect
    ready(store, A)
    before = positions(store)
    with sqlite3.connect(store.state) as c:
        recorded = json.loads(c.execute("SELECT value FROM meta WHERE key='q:recent_reserves'").fetchone()[0])
    monkeypatch.setenv('LABELWATCH_RECENT_RESERVES', json.dumps([
        {'mount': row['mount'], 'bytes': 2**62} for row in recorded['reserves']]))
    calls = []
    def forbidden():
        calls.append('transport')
        raise AssertionError('transport must not be constructed before capacity admission')
    monkeypatch.setattr(recent_collect, 'HTTPTransport', forbidden)
    assert recent_collect.main(['--store', str(store.root), '--network', '--discover']) == 1
    output = json.loads(capsys.readouterr().out.splitlines()[0])
    assert output['round'] == {'status': 'refused', 'reason': REASON}
    assert not calls and positions(store) == before
    status_doc = json.loads((store.root / 'STATUS.json').read_text())
    assert status_doc['round'] == {'status': 'refused', 'reason': REASON}


@pytest.mark.parametrize('after_success', [False, True])
def test_writer_reserve_race_is_store_level_and_preserves_earlier_acceptance(store, monkeypatch, after_success):
    ready(store, A, B)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1), Labeler(B, 1, recent=1)])
    original = store.accept_page
    accepted = []
    def race(*args, **kwargs):
        if not after_success or accepted:
            raise RuntimeError(REASON)
        result = original(*args, **kwargs)
        accepted.append(kwargs['source'])
        return result
    monkeypatch.setattr(store, 'accept_page', race)
    out = Collector(store, transport, now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep).tick()
    assert out['round']['status'] == 'refused' and out['round']['reason'] == REASON
    assert out['round']['store_hold'] == REASON and out['refused_sources'] == []
    assert sum(row['status'] == 'accepted' for row in out['results']) == int(after_success)
    for did, cursor, token, failures, unavailable in positions(store):
        assert cursor == ('1' if did in accepted else '0')
        assert token is None and failures == 0 and unavailable is None
    with sqlite3.connect(store.state) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0] == int(after_success)
        assert [r[0] for r in c.execute('SELECT reason FROM q_recent_gaps')] == ['ingest_paused']


@pytest.mark.parametrize('operation', ['collector_sweep_attempts', 'collector_claim_source', 'begin_source_attempt'])
def test_capacity_at_store_operations_outside_request_try_keeps_precise_reason(store, monkeypatch, operation):
    ready(store, A)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1)])
    def refused(*args, **kwargs): raise RuntimeError(REASON)
    monkeypatch.setattr(store, operation, refused)
    out = Collector(store, transport, now=clock.now, monotonic=clock.monotonic).tick()
    assert out['round']['status'] == 'refused' and out['round']['reason'] == REASON
    assert not transport.calls and out['refused_sources'] == []
    assert status(store, A)['unavailable_since'] is None


def test_claim_capacity_after_accepted_page_keeps_accepted_result(store, monkeypatch):
    ready(store, A)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1)])
    claim = store.collector_claim_source
    def race(*args, **kwargs):
        with sqlite3.connect(store.state) as c:
            accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
        if accepted: raise RuntimeError(REASON)
        return claim(*args, **kwargs)
    monkeypatch.setattr(store, 'collector_claim_source', race)
    out = Collector(store, transport, now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep).tick()
    assert out['round']['status'] == 'refused' and out['round']['reason'] == REASON
    assert [r['status'] for r in out['results']] == ['accepted', 'refused']
    assert positions(store)[0][1] == '1'


def test_gap_failure_does_not_mask_capacity_reason(store, monkeypatch):
    ready(store, A)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1)])
    def capacity(*args, **kwargs): raise RuntimeError(REASON)
    def gap_failure(*args, **kwargs): raise RuntimeError('gap recording unavailable')
    monkeypatch.setattr(store, 'accept_page', capacity)
    monkeypatch.setattr(store, 'record_gap', gap_failure)
    out = Collector(store, transport, now=clock.now, monotonic=clock.monotonic).tick()
    assert out['round']['reason'] == REASON
    assert out['round']['cleanup_errors'] == [{'operation': 'record_gap', 'reason': 'gap recording unavailable'}]
    assert positions(store)[0][1:]==('0', None, 0, None)


@pytest.mark.parametrize('error', [RuntimeError(REASON), Refused(REASON), RuntimeError('other failure mentions '+REASON)])
def test_transport_capacity_text_remains_source_failure(store, error):
    ready(store, A)
    clock = Clock()
    class Provider:
        def request(self, *args, **kwargs): raise error
    out = Collector(store, Provider(), now=clock.now, monotonic=clock.monotonic).tick()
    assert out['round']['status'] == 'ran' and out['round']['store_hold'] is None
    assert out['refused_sources'] == [A]
    assert status(store, A)['unavailable_since'] is not None


def test_store_unrelated_runtime_error_with_capacity_substring_remains_source_failure(store, monkeypatch):
    ready(store, A)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1)])
    def other(*args, **kwargs): raise RuntimeError('other failure mentions '+REASON)
    monkeypatch.setattr(store, 'accept_page', other)
    out = Collector(store, transport, now=clock.now, monotonic=clock.monotonic).tick()
    assert out['round']['status'] == 'ran' and out['round']['store_hold'] is None
    assert out['refused_sources'] == [A]


def test_discovery_store_capacity_is_not_source_unavailable(store, monkeypatch):
    ready(store, A)
    clock = Clock()
    class Discovery:
        def request(self, *args, **kwargs): return b'{"sources":[]}'
    def refused(*args, **kwargs): raise RuntimeError(REASON)
    monkeypatch.setattr(store, 'remember_sources', refused)
    out = Collector(store, Discovery(), now=clock.now, monotonic=clock.monotonic).run_round(discover=True)
    assert out['discovery']['scope'] == 'store' and out['discovery']['reason'] == REASON
    assert out['collection']['round']['status'] == 'refused'
    with sqlite3.connect(store.state) as c:
        assert [r[0] for r in c.execute('SELECT reason FROM q_recent_gaps')] == ['ingest_paused']


@pytest.mark.parametrize('after_success', [False, True])
def test_cli_writer_race_exit_stdout_and_status_preserve_precise_reason(store, monkeypatch, capsys, after_success):
    import recent_collect
    ready(store, A, B)
    clock = Clock(); transport = Transport(clock, [Labeler(A, 1, recent=1), Labeler(B, 1, recent=1)])
    class Context:
        def __enter__(self): return transport
        def __exit__(self, *args): return False
    monkeypatch.setattr(recent_collect, 'HTTPTransport', Context)
    monkeypatch.setattr(recent_collect, 'Collector', lambda s, t, **kw: Collector(s, t, now=clock.now, monotonic=clock.monotonic, sleep=clock.sleep))
    accept = store.accept_page; calls = []
    def race(*args, **kwargs):
        if not after_success or calls: raise RuntimeError(REASON)
        calls.append(kwargs['source']); return accept(*args, **kwargs)
    monkeypatch.setattr(store, 'accept_page', race)
    monkeypatch.setattr(recent_collect, 'RecentStore', lambda path: store)
    assert recent_collect.main(['--store', str(store.root), '--network']) == 1
    output = json.loads(capsys.readouterr().out.splitlines()[0])
    assert output['collection']['round']['reason'] == REASON
    assert sum(r['status']=='accepted' for r in output['collection']['results']) == int(after_success)
    doc = json.loads((store.root/'STATUS.json').read_text())
    assert doc['round'] == {'status':'refused','reason':REASON,'store_hold':REASON}


def test_discovery_outage_gap_capacity_preserves_both_causes(store, monkeypatch):
    ready(store, A)
    clock = Clock()
    class Discovery:
        def request(self, *args, **kwargs): raise Refused('discovery unavailable')
    def refused(*args, **kwargs): raise RuntimeError(REASON)
    monkeypatch.setattr(store, 'record_gap', refused)
    out = Collector(store, Discovery(), now=clock.now, monotonic=clock.monotonic).run_round(discover=True)
    assert out['discovery'] == {'status': 'refused', 'reason': 'discovery unavailable'}
    assert out['collection']['round']['status'] == 'refused'
    assert out['collection']['round']['reason'] == REASON
    assert out['collection']['round']['cleanup_errors'] == [{'operation': 'record_gap', 'reason': REASON}]
    assert positions(store)[0][1:] == ('0', None, 0, None)
