"""Readers never take the writer lease; contention is an explicit store_busy refusal."""
import io
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
from storage import lock  # noqa: E402
from recent_storage import RecentStore  # noqa: E402
from recent_provider import RecentProvider  # noqa: E402
from labelwatch.recent_observations import Limits, RecentObservations, Refused  # noqa: E402
from test_recent_overview import store, row, A, B, SUBJECT  # noqa: E402,F401
from test_recent_site import module, get  # noqa: E402

LATER = datetime(2026, 9, 2, 13, tzinfo=timezone.utc)
LO, HI = '2026-09-01T00:00:00Z', '2026-09-02T12:00:00.000001Z'


def product(store, **limits):
    return RecentObservations(RecentProvider(store), Limits(**limits), now=lambda: LATER)


def test_account_and_export_read_while_ingest_holds_the_writer_lease(store):
    p = product(store, page_rows=2)
    with lock(store.root):  # a collector round in progress
        account = p.account(p.create(SUBJECT, LO, HI)['cursor'])
        export = p.create_export(SUBJECT, LO, HI)
        pages = [p.page(export['cursor'])]
        while not pages[-1]['terminal']:
            pages.append(p.page(pages[-1]['next_cursor']))
    assert account['summary']['observed_events'] == 5
    assert sum(len(x['rows']) for x in pages) == 5 and pages[-1]['count'] == 5


def test_rows_pending_a_flush_are_read_from_the_state_once(store, monkeypatch):
    monkeypatch.setattr(RecentStore, 'flush', lambda self, c, death=None: None)
    store.ingest([row(A, 'pending')], 'fixture', 'c4', '2026-09-02T12:10:00Z')
    monkeypatch.undo()
    p = product(store)
    rows = p.account(p.create(SUBJECT, LO, '2026-09-02T12:10:00.000001Z')['cursor'])['timeline']
    assert sorted(r['value'] for r in rows).count('pending') == 1 and len(rows) == 6


def test_export_deadline_is_sized_from_pages_and_stated(store):
    export = product(store, page_rows=1).create_export(SUBJECT, LO, HI)
    deadline = export['manifest']['deadline']
    assert deadline['pages_estimate'] == 5 and deadline['rows_estimate'] == 5
    assert deadline['seconds'] == 120 and deadline['cap_seconds'] == 600  # max(ttl, 30 + 0.5*5)
    big = product(store, page_rows=1, export_base_seconds=30, export_page_seconds=200, export_cap_seconds=600)
    with pytest.raises(Refused, match='export_too_large'):
        big.create_export(SUBJECT, LO, HI)


class BusyProvider:
    def __init__(self, inner):
        self.inner, self.busy = inner, False

    @contextmanager
    def snapshot(self, *args, **kwargs):
        if self.busy:
            raise TimeoutError('store lease deadline exceeded')
        with self.inner.snapshot(*args, **kwargs) as source:
            yield source


def test_lease_timeout_is_store_busy_and_does_not_consume_an_export_page(store):
    provider = BusyProvider(RecentProvider(store))
    p = RecentObservations(provider, Limits(page_rows=2), now=lambda: LATER)
    export = p.create_export(SUBJECT, LO, HI)
    first = p.page(export['cursor'])
    provider.busy = True
    with pytest.raises(Refused, match='store_busy'):
        p.create(SUBJECT, LO, HI)
    with pytest.raises(Refused, match='store_busy'):
        p.page(first['next_cursor'])
    provider.busy = False
    rest = [p.page(first['next_cursor'])]
    while not rest[-1]['terminal']:
        rest.append(p.page(rest[-1]['next_cursor']))
    assert len(first['rows']) + sum(len(x['rows']) for x in rest) == 5


def test_http_store_busy_is_503_with_retry_after(store):
    mod = module()
    provider = BusyProvider(RecentProvider(store)); provider.busy = True
    h = mod.handler(RecentObservations(provider, now=lambda: LATER))
    status, headers, body = get(h, '/account?did=' + SUBJECT + '&start=' + LO + '&end=' + HI)
    assert status == 503 and headers['Retry-After'] == '5' and json.loads(body)['error'] == 'store_busy'
    status, headers, body = get(h, '/account?did=' + SUBJECT + '&start=' + LO + '&end=' + HI, accept='text/html')
    assert status == 503 and b'busy' in body
