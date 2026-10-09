"""Oversized accounts stay fully exportable through server-suggested narrower intervals."""
import bisect
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / 'tools/segmented_qualification'))
from test_recent_overview import store, SUBJECT  # noqa: E402,F401
from labelwatch.recent_observations import (Limits, RecentObservations, Refused, iso, timestamp,  # noqa: E402
                                            suggest_intervals, refusal_html)

DID = 'did:plc:denseaccount'
NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
ACQ = {'schema': 'labelwatch.acquisition.v1', 'generation_id': '12345678-1234-4234-9234-123456789012',
       'started_at': '2026-09-01T00:00:00Z', 'basis': 'local_acceptance_clock'}


class MemoryProvider:
    """Keyset provider over monotonic (id, observed_us) rows, with an hour histogram."""

    def __init__(self, stamps):
        self.stamps = sorted(stamps)
        self.micros = [int(t.timestamp() * 1e6) for t in self.stamps]

    def _range(self, start, end):
        lo = bisect.bisect_left(self.micros, int(timestamp(start).timestamp() * 1e6))
        hi = bisect.bisect_left(self.micros, int(timestamp(end).timestamp() * 1e6))
        return lo, hi  # ids are index + 1

    def histogram(self, did, start, end, upper_id, bucket_seconds=3600, timeout_seconds=10):
        lo, hi = self._range(start, end)
        out = {}
        for i in range(lo, min(hi, upper_id)):
            t = self.stamps[i].replace(minute=0, second=0, microsecond=0)
            out[iso(t)] = out.get(iso(t), 0) + 1
        return out

    @contextmanager
    def snapshot(self, did, start, end, max_rows, *, after_id=None, upper_id=None, **budgets):
        lo, hi = self._range(start, end)
        if upper_id is None:
            upper_id = len(self.stamps)
        first = max(lo, after_id or 0)
        last = min(hi, upper_id)
        ids = list(range(first + 1, last + 1))
        page = ids[:max_rows]
        rows = [{'id': i, 'labeler_did': 'did:plc:labeler', 'uri': DID, 'val': 'v%d' % i,
                 'ts': '2026-01-01T00:00:00Z', 'observed_at': iso(self.stamps[i - 1]), 'neg': 0} for i in page]
        yield {'rows': rows, 'frontier': 'memory-frontier', 'upper_id': upper_id,
               'coverage': {'status': 'unknown', 'gaps': [], 'acquisition': ACQ},
               'next_after_id': page[-1] if page else (after_id or 0), 'has_more': len(ids) > max_rows,
               'total': max(0, last - lo) if after_id == 0 else None}


def export_all(product, start, end):
    created = product.create_export(DID, start, end)
    pages = [product.page(created['cursor'])]
    while not pages[-1]['terminal']:
        pages.append(product.page(pages[-1]['next_cursor']))
    return created['manifest'], [r for p in pages for r in p['rows']], pages[-1]


def tiles(intervals, lo, hi):
    assert intervals[0]['start'] == iso(lo) and intervals[-1]['end_exclusive'] == iso(hi)
    for a, b in zip(intervals, intervals[1:]):
        assert a['end_exclusive'] == b['start']  # end-exclusive: no gap, no overlap


def test_120k_rows_refused_with_suggestions_whose_union_is_the_full_row_set():
    lo = NOW - timedelta(days=30)
    stamps = [lo + timedelta(seconds=21.6 * n + 1) for n in range(120000)]  # ~4,000 rows/day over 30 days
    product = RecentObservations(MemoryProvider(stamps), now=lambda: NOW)
    with pytest.raises(Refused, match='export_too_large') as refusal:
        product.create_export(DID, iso(lo), iso(NOW))
    details = refusal.value.details
    assert details['rows_in_window'] == 120000 and details['max_rows_per_export'] == 114000
    intervals = details['suggested_intervals']
    assert len(intervals) >= 2 and all(i['rows'] <= 114000 and i['split'] == 'day' for i in intervals)
    assert sum(i['rows'] for i in intervals) == 120000
    tiles(intervals, lo, NOW)
    seen, frontiers = [], set()
    for item in intervals:
        manifest, rows, terminal = export_all(product, item['start'], item['end_exclusive'])
        assert manifest['interval'] == {'start': item['start'], 'end': item['end_exclusive'], 'end_exclusive': True}
        assert manifest['coverage']['acquisition']['generation_id'] == ACQ['generation_id']
        assert terminal['count'] == item['rows'] == len(rows) and terminal['content_sha256']
        frontiers.add(manifest['deadline']['rows_estimate'])
        seen.extend(r['value'] for r in rows)
    assert len(seen) == len(set(seen)) == 120000  # no duplicates, nothing missing


def test_a_dense_day_is_split_by_hour_and_still_tiles():
    day = datetime(2026, 10, 1, tzinfo=timezone.utc)
    stamps = [day + timedelta(minutes=3 * n) for n in range(480)] + [day + timedelta(days=2, hours=5)]
    product = RecentObservations(MemoryProvider(stamps), Limits(page_rows=10, export_cap_seconds=40), now=lambda: NOW)
    assert product.max_export_rows() == 200
    lo, hi = NOW - timedelta(days=10), NOW
    with pytest.raises(Refused) as refusal:
        product.create_export(DID, iso(lo), iso(hi))
    intervals = refusal.value.details['suggested_intervals']
    assert any(i['split'] == 'hour' for i in intervals) and 'split by hour' in refusal.value.details['note']
    assert not any(i.get('status') for i in intervals)
    tiles(intervals, lo, hi)
    values = []
    for item in intervals:
        values += [r['value'] for r in export_all(product, item['start'], item['end_exclusive'])[1]]
    assert len(values) == len(set(values)) == 481


def test_a_dense_hour_is_reported_never_truncated():
    hour = datetime(2026, 10, 3, 7, tzinfo=timezone.utc)
    stamps = [hour + timedelta(seconds=10 * n) for n in range(300)] + [hour + timedelta(hours=3)]
    product = RecentObservations(MemoryProvider(stamps), Limits(page_rows=10, export_cap_seconds=40), now=lambda: NOW)
    lo, hi = NOW - timedelta(days=10), NOW
    with pytest.raises(Refused) as refusal:
        product.create_export(DID, iso(lo), iso(hi))
    details = refusal.value.details
    dense = details['interval_too_dense']
    assert dense == [{'start': iso(hour), 'end_exclusive': iso(hour + timedelta(hours=1)), 'rows': 300,
                      'split': 'hour', 'status': 'interval_too_dense'}]
    intervals = details['suggested_intervals']
    tiles(intervals, lo, hi)
    assert sum(i['rows'] for i in intervals) == 301
    html = refusal_html('export_too_large', details)
    assert 'interval_too_dense' in html and '300' in html


def test_suggestions_with_mid_hour_bounds_tile_exactly():
    lo, hi = datetime(2026, 10, 1, 0, 30, tzinfo=timezone.utc), datetime(2026, 10, 3, 6, 45, tzinfo=timezone.utc)
    hourly = {iso(datetime(2026, 10, 1, h, tzinfo=timezone.utc)): 50 for h in range(24)}
    out = suggest_intervals(lo, hi, hourly, 100)
    tiles(out, lo, hi)
    assert sum(i['rows'] for i in out) == 1200 and all(i['rows'] <= 100 for i in out)


def test_real_store_refusal_suggestions_export_fully_and_http_carries_export_urls(store):
    from recent_provider import RecentProvider
    later = datetime(2026, 9, 2, 13, tzinfo=timezone.utc)
    product = RecentObservations(RecentProvider(store), Limits(page_rows=1, export_cap_seconds=31), now=lambda: later)
    assert product.max_export_rows() == 2
    lo, hi = '2026-09-01T00:00:00Z', '2026-09-02T12:00:00.000001Z'
    with pytest.raises(Refused) as refusal:
        product.create_export(SUBJECT, lo, hi)
    details = refusal.value.details
    assert details['rows_in_window'] == 5
    intervals = details['suggested_intervals']
    tiles(intervals, timestamp(lo), timestamp(hi))
    assert any(i['split'] == 'hour' for i in intervals)  # 2026-09-01 holds 3 rows > 2
    values = []
    for item in intervals:
        assert item['rows'] <= 2 and not item.get('status')
        rows = export_all_for(product, SUBJECT, item['start'], item['end_exclusive'])
        assert len(rows) == item['rows']
        values += [(r['labeler_did'], r['value'], r['observed_at']) for r in rows]
    assert len(values) == len(set(values)) == 5
    from test_recent_site import module, get
    mod = module()
    h = mod.handler(RecentObservations(RecentProvider(store), Limits(page_rows=1, export_cap_seconds=31), now=lambda: later))
    status, _, body = get(h, '/exports?did=' + SUBJECT + '&start=' + lo + '&end=' + hi)
    body = json.loads(body)
    assert status == 409 and body['error'] == 'export_too_large' and body['max_rows_per_export'] == 2
    assert all(i['export_url'].startswith('/exports?did=') for i in body['suggested_intervals'])
    status, _, page = get(h, '/exports?did=' + SUBJECT + '&start=' + lo + '&end=' + hi, accept='text/html')
    assert status == 409 and b'Export these intervals instead' in page and b'/exports?did=' in page


def export_all_for(product, did, start, end):
    created = product.create_export(did, start, end)
    pages = [product.page(created['cursor'])]
    while not pages[-1]['terminal']:
        pages.append(product.page(pages[-1]['next_cursor']))
    return [r for p in pages for r in p['rows']]
