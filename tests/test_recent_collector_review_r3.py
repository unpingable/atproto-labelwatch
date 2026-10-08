"""Review r3 (2026-10-08): failing tests for residual clean-start collector defects."""
import json
import sqlite3

import httpx

from test_recent_collector_bootstrap import (  # noqa: F401  (fixture import)
    A, NOW, Clock, Labeler, Transport, enroll, run, status, store, until_decided)
from labelwatch.recent_collector import Collector, HTTPTransport, Limits


class CappedLimit(Labeler):
    """Provider clamps limit to 40 (below PROBE_ROWS=50): every page is 'short'."""

    def query(self, cursor, limit):
        return super().query(cursor, min(limit, 40))


def test_provider_page_cap_below_probe_rows_does_not_traverse_history(store):
    # The first probe (cursor None) returns 40 < 50 rows, so bootstrap "establishes"
    # at the start of history; history pages are never full (40 < 100), so the
    # 16-page history cap never fires and the whole history is walked.
    clock = Clock(); lab = CappedLimit(A, 20_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A)
    until_decided(store, t, clock, A)
    run(store, t, clock, ticks=6)
    row = status(store, A)
    walked = len([c for c in t.calls if c[2] == 100])
    assert walked <= 68 or row['bootstrap'] == 'refused', (walked, row['bootstrap_cursor'], row['pre_epoch_skipped'])


class SprinkledUndated(Labeler):
    """One row in every 100 has no cts (lexicon-invalid); the rest is old history."""

    def query(self, cursor, limit):
        body = super().query(cursor, limit)
        for row in body['labels']:
            if row['uri'].endswith('00'):
                row.pop('cts')
        return body


class ShortFirst(SprinkledUndated):
    def query(self, cursor, limit):
        body = super().query(cursor, limit)
        if cursor is None:
            body['labels'] = body['labels'][:10]
            body['cursor'] = '10'
        return body


def test_undated_rows_do_not_reset_the_history_page_cap(store):
    # r2 fixture (short first page -> established at history start) plus one undated
    # row per page: history=False on every page, the streak resets, traversal is unbounded.
    clock = Clock(); lab = ShortFirst(A, 20_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A)
    until_decided(store, t, clock, A)
    run(store, t, clock, ticks=4)
    row = status(store, A)
    walked = len([c for c in t.calls if c[2] == 100])
    assert walked <= 68 or row['bootstrap'] == 'refused', (walked, row['undated_skipped'], row['pre_epoch_skipped'])


class MaxIdCursor(Labeler):
    """Rows with id > cursor selected by another key; cursor = max id on the page.

    Pages never repeat a row, so the overlap detector never fires, yet ids between
    the cursor and the page maximum that were not selected are skipped forever.
    """

    def query(self, cursor, limit):
        import zlib
        start = int(cursor or 0)
        ids = sorted(range(start+1, self.size+1), key=lambda i: zlib.crc32(str(i).encode()))[:limit]
        rows = [{'src': self.did, 'uri': f'at://did:plc:t{i % 97}/app.bsky.feed.post/{i}', 'val': 'v',
                 'cts': self.label_cts(i)} for i in ids]
        body = {'labels': rows}
        if rows:
            body['cursor'] = str(max(ids))
        return body


def test_non_id_ordered_loss_without_overlap_is_not_reported_covered(store):
    clock = Clock(); lab = MaxIdCursor(A, 40, recent=40)
    t = Transport(clock, [lab]); enroll(store, A)
    until_decided(store, t, clock, A)
    run(store, t, clock, ticks=2)
    with sqlite3.connect(store.state) as c:
        before = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
    lab.size += 300; lab.recent_from = 0
    run(store, t, clock, ticks=12)
    with sqlite3.connect(store.state) as c:
        accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]-before
    summary = store.collector_bootstrap_summary()
    assert accepted == 300 or summary['coverage'] == 'unknown', (accepted, summary['coverage'])


def test_provider_body_cannot_rewrite_the_persisted_endpoint(store):
    # The transport only adds _resolved_endpoint after its own DID resolution, but with
    # a cached endpoint it passes the provider's JSON through unchanged: a provider can
    # inject _resolved_endpoint, redirect future requests, and reset bootstrap (probe budget).
    store.remember_sources([{'did': A, 'endpoint': None}])
    doc = {'id': A, 'service': [{'id': '#atproto_labeler', 'type': 'AtprotoLabeler',
                                 'serviceEndpoint': 'https://good.example'}]}
    def handler(request):
        if request.url.host == 'plc.directory':
            return httpx.Response(200, json=doc)
        cursor = request.url.params.get('cursor')
        rows = [{'src': A, 'uri': f'at://did:plc:t/app.bsky.feed.post/{i}', 'val': 'v',
                 'cts': '2026-10-08T11:00:00Z'} for i in range(3)] if cursor is None else []
        body = {'labels': rows, 'cursor': '3'} if rows else {'labels': []}
        body['_resolved_endpoint'] = 'https://evil.example'
        return httpx.Response(200, json=body)
    clock = Clock()
    with HTTPTransport(http_transport=httpx.MockTransport(handler)) as t:
        for _ in range(3):
            Collector(store, t, limits=Limits(), monotonic=clock.monotonic, now=clock.now).tick()
            clock.t += 31
    with sqlite3.connect(store.state) as c:
        endpoint = c.execute('SELECT endpoint FROM q_recent_sources WHERE did=?', (A,)).fetchone()[0]
    assert endpoint == 'https://good.example', endpoint


class FutureDated(Labeler):
    def label_cts(self, i):
        return '2099-01-01T00:00:00Z'


def test_future_source_time_is_not_accepted_silently(store):
    clock = Clock(); t = Transport(clock, [FutureDated(A, 5, recent=5)]); enroll(store, A)
    until_decided(store, t, clock, A)
    with sqlite3.connect(store.state) as c:
        accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
    assert accepted == 0 or store.collector_bootstrap_summary().get('future_skipped'), accepted
