"""Bounded adapter controls; fixture transport never opens a socket."""
import json
from datetime import datetime, timezone
import pytest
from labelwatch.recent_collector import Collector, Limits, Refused

DID = 'did:plc:one'
NOW = datetime(2026, 10, 6, tzinfo=timezone.utc)


def label(value='x'):
    return {'src': DID, 'uri': 'did:plc:subject', 'val': value, 'cts': '2020-01-01T00:00:00Z'}


class Store:
    def __init__(self, count=1):
        self.sources = [{'did': DID if n == 0 else f'did:plc:s{n}', 'endpoint': 'https://example.test'} for n in range(count)]
        self.cursors, self.pages, self.gaps = {}, [], []

    def collector_sources(self, limit):
        return self.sources[:limit]

    def collector_cursor(self, source):
        return self.cursors.get(source)

    def remember_sources(self, rows, limit):
        merged = {r['did']: r for r in self.sources}
        merged.update({r['did']: r for r in rows})
        if len(merged) > limit:
            raise Refused('roster ceiling')
        self.sources = list(merged.values())

    def accept_page(self, rows, source, request_cursor, next_cursor, observed_at):
        assert self.cursors.get(source) == request_cursor
        self.pages.append(rows)
        if next_cursor is not None:
            self.cursors[source] = next_cursor
        return {'inserted': len(rows)}

    def record_gap(self, *args):
        self.gaps.append(args)


class Transport:
    def __init__(self, responses):
        self.responses, self.calls = iter(responses), []

    def request(self, operation, params, **limits):
        self.calls.append((operation, params, limits))
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, bytes) else json.dumps(value).encode()


def collector(store, responses, **kwargs):
    transport = Transport(responses)
    return Collector(store, transport, now=lambda: NOW, **kwargs), transport


def test_terminal_nonempty_page_preserves_opaque_input_and_accepts_changed_tail():
    s = Store()
    s.cursors[DID] = 'opaque:Z/2'
    c, t = collector(s, [{'labels': [label()]}, {'labels': [label(), label('later')]}])
    assert c.tick()['results'][0]['status'] == 'accepted'
    assert c.tick()['results'][0]['rows'] == 2
    assert [call[1]['cursor'] for call in t.calls] == ['opaque:Z/2']*2
    assert s.cursors[DID] == 'opaque:Z/2'
    assert s.pages[0][0][8] == '2020-01-01T00:00:00Z'


def test_empty_absent_cursor_remains_cursorless():
    s = Store()
    c, _ = collector(s, [{'labels': []}])
    assert c.tick()['results'][0]['terminal']
    assert s.cursors == {}


@pytest.mark.parametrize('page', [
    {'labels': [label()], 'cursor': 'same'},
    {'labels': [dict(label(), src='did:plc:other')]},
    {'labels': [dict(label(), neg='false')]},
    {'labels': [dict(label(), cts=None)]},
    {'labels': [label()]*101}, {'other': []}, b'not-json', b' '*1048577,
])
def test_refusal_preserves_cursor_and_records_gap(page):
    s = Store(); s.cursors[DID] = 'same'
    c, _ = collector(s, [page])
    assert c.tick()['results'][0]['status'] == 'refused'
    assert s.cursors[DID] == 'same' and not s.pages and len(s.gaps) == 1


def test_all_or_nothing_normalization():
    s = Store(); c, _ = collector(s, [{'labels': [label(), {'src': DID}]}])
    assert c.tick()['results'][0]['status'] == 'refused'
    assert not s.pages


def test_fairness_under_page_budget_and_failed_source():
    s = Store(3)
    c, t = collector(s, [TimeoutError(), {'labels': []}, {'labels': []}], limits=Limits(pages=1))
    for _ in range(3):
        assert c.tick()['attempted'] == 1
    assert len({call[1]['did'] for call in t.calls}) == 3
    assert len(s.gaps) == 1


def test_transport_budget_and_late_return_refused_before_commit():
    s = Store(); times = iter([0, 0, 0, 11])
    c, t = collector(s, [{'labels': [label()]}], monotonic=lambda: next(times))
    assert c.tick()['results'][0]['status'] == 'refused'
    assert not s.pages
    assert t.calls[0][2] == {'max_bytes': 1048576, 'timeout_seconds': 10}


def test_discovery_atomic_sticky_roster_and_terminal_page():
    s = Store()
    c, _ = collector(s, [{'sources': [{'did': 'did:plc:new', 'endpoint': 'https://new.test'}]}])
    assert c.discover()['terminal']
    assert {row['did'] for row in s.sources} == {DID, 'did:plc:new'}


def test_discovery_roster_ceiling_does_not_partially_enroll():
    s = Store()
    c, _ = collector(s, [{'sources': [{'did': 'did:plc:new', 'endpoint': 'https://new.test'}]}], limits=Limits(sources=1))
    with pytest.raises(Refused): c.discover()
    assert len(s.sources) == 1


@pytest.mark.parametrize('endpoint', ['http://x.test', 'https://user:pass@x.test', 'https://x.test/?q=1'])
def test_invalid_discovery_endpoint_refused(endpoint):
    s = Store(); c, _ = collector(s, [{'sources': [{'did': 'did:plc:new', 'endpoint': endpoint}]}])
    with pytest.raises(Refused): c.discover()
    assert len(s.sources) == 1


def test_gap_write_failure_is_not_hidden():
    s = Store()
    def fail(*args): raise RuntimeError('gap persistence failed')
    s.record_gap = fail
    c, _ = collector(s, [TimeoutError()])
    with pytest.raises(RuntimeError, match='gap persistence'): c.tick()
