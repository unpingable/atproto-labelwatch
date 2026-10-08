"""Review r2 (2026-10-08): failing tests demonstrating clean-start collector defects.

Live evidence (review r2 probe log): on ozone.fran.pw and labeler.urbanism.plus the
queryLabels cursor returned for a request is NOT monotone in the request cursor
(fran.pw: 1200 -> 8321 but 2000 -> 5089; urbanism.plus: 5700 -> 7428 but
6500 -> 6541), pages at 1200 and 2000 share 43 of 50 rows, and following the chain
from 1200 to the head never returned 6 rows that the page at 2000 contains.
"""
import sqlite3
import zlib

from test_recent_collector_bootstrap import (  # noqa: F401  (fixture import)
    A, Clock, Labeler, Transport, enroll, run, status, store, until_decided)


class ShortFirstPage(Labeler):
    """Provider post-filters rows: the first page is short although history is long."""

    def query(self, cursor, limit):
        body = super().query(cursor, limit)
        if cursor is None:
            body['labels'] = body['labels'][:10]
            body['cursor'] = '10'
        return body


def test_post_establishment_history_traversal_is_bounded_or_disclosed(store):
    # A short page mid-history makes the source "established" at the beginning; the
    # collector then walks the whole pre-epoch history page by page with no bound.
    clock = Clock(); lab = ShortFirstPage(A, 20_000, recent=5)
    t = Transport(clock, [lab]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'established'
    run(store, t, clock, ticks=4)
    pages_after = len([c for c in t.calls if c[2] == 100])
    row = status(store, A)
    # Requirement: no unbounded traversal of history after a (mis)established head.
    assert pages_after <= 68 or row['bootstrap'] == 'refused', (pages_after, row['pre_epoch_skipped'])


class NonIdOrdered(Labeler):
    """Rows with id > cursor, but selected/ordered by another key; cursor = last row id (fran.pw-like)."""

    def query(self, cursor, limit):
        start = int(cursor or 0)
        ids = sorted(range(start+1, self.size+1), key=lambda i: zlib.crc32(str(i).encode()))[:limit]
        rows = [{'src': self.did, 'uri': f'at://did:plc:t{i % 97}/app.bsky.feed.post/{i}', 'val': 'v',
                 'cts': self.label_cts(i)} for i in ids]
        body = {'labels': rows}
        if rows:
            body['cursor'] = str(ids[-1])
        return body


def test_non_id_ordered_pages_do_not_silently_lose_recent_rows(store):
    clock = Clock(); lab = NonIdOrdered(A, 40, recent=40)
    t = Transport(clock, [lab]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'established'
    run(store, t, clock, ticks=2)
    with sqlite3.connect(store.state) as c:
        before = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]
    lab.size += 300; lab.recent_from = 0        # a burst of 300 new, in-allowance labels
    run(store, t, clock, ticks=12)
    with sqlite3.connect(store.state) as c:
        accepted = c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0]-before
        gaps = c.execute('SELECT COUNT(*) FROM q_recent_gaps').fetchone()[0]
    # Either every new row is observed, or the loss is disclosed as a gap/unknown coverage.
    assert accepted == 300 or gaps > 0, (accepted, gaps)


class Undated(Labeler):
    """cts present but not zone-qualified: counted as undated_skipped."""
    def query(self, cursor, limit):
        body = super().query(cursor, limit)
        for row in body['labels']:
            row['cts'] = '2026-10-07T00:00:00'
        return body


def test_source_with_only_undated_rows_is_not_reported_as_covered(store):
    clock = Clock(); t = Transport(clock, [Undated(A, 30, recent=30)]); enroll(store, A)
    final = until_decided(store, t, clock, A)
    assert final['bootstrap'] == 'established' and status(store, A)['undated_skipped'] == 30
    # Every row from this source is dropped; coverage must not read as established.
    assert store.collector_bootstrap_summary()['coverage'] == 'unknown'


class MissingCts(Labeler):
    """One lexicon-invalid row (no cts) poisons every page that contains it."""
    def query(self, cursor, limit):
        body = super().query(cursor, limit)
        for row in body['labels']:
            if row['uri'].endswith('/7'):
                row.pop('cts')
        return body


def test_row_without_cts_is_decided_not_retried_forever(store):
    clock = Clock(); t = Transport(clock, [MissingCts(A, 30, recent=30)]); enroll(store, A)
    for _ in range(12):
        run(store, t, clock)
        clock.t += 900
    row = status(store, A)
    # Deterministic outcome required: counted (undated) or refused, not perpetual retry.
    assert row['bootstrap'] in ('established', 'refused'), (row, len(t.calls))
