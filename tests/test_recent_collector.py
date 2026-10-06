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
        self.schedule = 0
        self.discovery_cursor = None

    def collector_sources(self, limit):
        return self.sources[:limit]

    def collector_claim_source(self, limit,observed_at=None):
        rows = sorted(self.sources, key=lambda row: row['did'])
        row = rows[self.schedule % len(rows)]
        self.schedule += 1
        return row

    def collector_cursor(self, source):
        return self.cursors.get(source)

    def collector_discovery_cursor(self):
        return self.discovery_cursor

    def remember_sources(self, rows, limit, request_cursor=None, next_cursor=None):
        merged = {r['did']: r for r in self.sources}
        merged.update({r['did']: r for r in rows})
        if len(merged) > limit:
            raise Refused('roster ceiling')
        assert self.discovery_cursor == request_cursor
        self.sources = list(merged.values())
        self.discovery_cursor = next_cursor

    def begin_source_attempt(self, did, observed_at):
        return did

    def finish_source_attempt(self, did, attempt_token,observed_at=None):
        assert did == attempt_token

    def accept_page(self, rows, source, request_cursor, next_cursor, observed_at, attempt_token=None):
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
    kwargs.setdefault("limits",Limits(pages=1))
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
    assert t.calls[0][2] == {'max_bytes': 1048576, 'timeout_seconds': 1}


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


def http_transport(handler):
    import httpx
    from labelwatch.recent_collector import HTTPTransport
    return HTTPTransport(httpx.MockTransport(handler))


def test_http_discovery_preserves_unresolved_source_and_opaque_cursor():
    import httpx
    def handler(request):
        assert request.url.params['cursor'] == 'opaque/with?symbols'
        assert request.url.params['collection'] == 'app.bsky.labeler.service'
        return httpx.Response(200, json={'repos': [{'did': DID}], 'cursor': 'next'})
    t = http_transport(handler)
    result = json.loads(t.request('discover', {'cursor': 'opaque/with?symbols', 'limit': 10}, max_bytes=4096, timeout_seconds=1))
    assert result == {'sources': [{'did': DID, 'endpoint': None}], 'cursor': 'next'}


def test_http_resolution_matches_did_and_streams_labels():
    import httpx
    requests = []
    def handler(request):
        requests.append(request)
        if request.url.host == 'plc.directory':
            return httpx.Response(200, json={'id': DID, 'service': [{'id': '#atproto_labeler', 'serviceEndpoint': 'https://labeler.test'}]})
        assert request.url.params['sources'] == DID
        assert request.url.params['cursor'] == 'z?x'
        return httpx.Response(200, json={'labels': [label()]})
    t = http_transport(handler)
    result = json.loads(t.request('labels', {'did': DID, 'endpoint': None, 'cursor': 'z?x', 'limit': 10}, max_bytes=4096, timeout_seconds=1))
    assert result['labels'] == [label()] and len(requests) == 2


def test_http_resolution_identity_refusal_prevents_label_query():
    import httpx
    calls = []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={'id': 'did:plc:other'})
    with pytest.raises(Refused, match='identity'):
        http_transport(handler).request('labels', {'did': DID, 'endpoint': None, 'cursor': None, 'limit': 10}, max_bytes=4096, timeout_seconds=1)
    assert len(calls) == 1


def test_http_total_deadline_and_stream_ceiling():
    import asyncio
    import httpx
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{'
            await asyncio.sleep(0.05)
            yield b'}'
    async def handler(request):
        return httpx.Response(200, stream=Slow())
    t = http_transport(handler)
    with pytest.raises(TimeoutError):
        t.request('discover', {'cursor': None, 'limit': 10}, max_bytes=4096, timeout_seconds=.005)
    def large(request): return httpx.Response(200, content=b' '*4097)
    with pytest.raises(Refused, match='byte ceiling'):
        http_transport(large).request('discover', {'cursor': None, 'limit': 10}, max_bytes=4096, timeout_seconds=1)


def test_store_claim_resumes_fairness_across_adapter_restart():
    s = Store(3)
    c, first = collector(s, [{'labels': []}], limits=Limits(pages=1))
    c.tick()
    successor, second = collector(s, [{'labels': []}], limits=Limits(pages=1))
    successor.tick()
    assert first.calls[0][1]['did'] != second.calls[0][1]['did']


def test_discovery_keeps_unresolved_sources_for_collection_retry():
    s = Store(0)
    c, _ = collector(s, [{'sources': [{'did': DID, 'endpoint': None}]}, TimeoutError()])
    c.discover()
    assert s.sources == [{'did': DID, 'endpoint': None}]
    assert c.tick()['results'][0]['status'] == 'refused'
    assert s.sources and s.gaps


def test_real_store_changed_terminal_page_and_restart_claim(tmp_path, monkeypatch):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/"tools"/"segmented_qualification"))
    import storage
    monkeypatch.setattr(storage, "ROOT", tmp_path)
    from recent_storage import RecentStore
    s = RecentStore.create(tmp_path/'store', NOW.isoformat())
    s.remember_sources([{'did': DID, 'endpoint': None}])
    c, _ = collector(s, [{'labels': [label()]}, {'labels': [label(), label('later')]}, {'labels': [label(), label('later')]}])
    from datetime import timedelta
    assert c.tick()['results'][0]['result']['inserted'] == 1
    c.now=lambda:NOW+timedelta(seconds=31)
    assert c.tick()['results'][0]['result']['inserted'] == 1
    c.now=lambda:NOW+timedelta(seconds=62)
    assert c.tick()['results'][0]['result']['inserted'] == 0
    assert s.collector_cursor(DID) is None
    s.remember_sources([{'did': 'did:plc:two', 'endpoint': None}])
    stamp=(NOW+timedelta(seconds=100)).isoformat()
    first = s.collector_claim_source(observed_at=stamp)['did']
    assert RecentStore(tmp_path/'store').collector_claim_source(observed_at=stamp)['did'] != first


def test_real_store_mock_http_discovery_and_collection(tmp_path, monkeypatch):
    import httpx
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1]/"tools"/"segmented_qualification"))
    import storage
    monkeypatch.setattr(storage, "ROOT", tmp_path)
    from recent_storage import RecentStore
    s = RecentStore.create(tmp_path/'store', NOW.isoformat())
    def handler(request):
        if request.url.host == 'bsky.network':
            return httpx.Response(200, json={'repos': [{'did': DID}]})
        if request.url.host == 'plc.directory':
            return httpx.Response(200, json={'id': DID, 'service': [{'id': '#atproto_labeler', 'serviceEndpoint': 'https://labeler.test'}]})
        return httpx.Response(200, json={'labels': [label()], 'cursor': 'opaque/position'})
    c = Collector(s, http_transport(handler), now=lambda: NOW)
    assert c.discover()['discovered'] == 1
    assert c.tick()['results'][0]['result']['inserted'] == 1
    assert s.collector_cursor(DID) == 'opaque/position'


def test_failed_attempt_seals_gap_before_clearing_exact_token():
    s = Store(); steps = []
    s.begin_source_attempt = lambda did, observed_at: 'exact-attempt'
    s.record_gap = lambda *args: steps.append('gap')
    def finish(did, token,observed_at=None):
        assert did == DID and token == 'exact-attempt'
        steps.append('finished')
    s.finish_source_attempt = finish
    c, _ = collector(s, [TimeoutError()])
    assert c.tick()['results'][0]['status'] == 'refused'
    assert steps == ['gap', 'finished']


def test_discovery_continuation_survives_adapter_restart_and_terminal_rescan():
    s = Store(0)
    first, _ = collector(s, [{'sources': [{'did': DID, 'endpoint': None}], 'cursor': 'page-two'}])
    first.discover()
    successor, t = collector(s, [{'sources': [{'did': 'did:plc:two', 'endpoint': None}]}])
    assert successor.discover()['terminal']
    assert t.calls[0][1]['cursor'] == 'page-two'
    assert s.collector_discovery_cursor() is None and len(s.sources) == 2


def test_discovery_refusal_still_collects_enrolled_source():
    s = Store()
    c, _ = collector(s, [TimeoutError(), {'labels': [label()], 'cursor': 'next'}])
    result = c.run_round(discover=True)
    assert result['discovery']['status'] == 'refused'
    assert len(s.pages) == 1 and s.gaps


def test_request_timeout_does_not_join_resolver_before_next_request():
    import asyncio
    import threading
    from labelwatch.recent_collector import HTTPTransport
    release = threading.Event()
    transport = HTTPTransport()
    async def fixture(operation, params, max_bytes, timeout_seconds):
        if operation == 'blocked':
            await asyncio.get_running_loop().run_in_executor(None, release.wait)
        return b'{}'
    transport._request = fixture
    with transport:
        try:
            with pytest.raises(TimeoutError):
                transport.request('blocked', {}, max_bytes=100, timeout_seconds=.02)
            assert transport.request('ready', {}, max_bytes=100, timeout_seconds=.1) == b'{}'
            assert not release.is_set()
        finally:
            release.set()
