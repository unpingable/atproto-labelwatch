#!/usr/bin/env python3
"""Publish the aggregate estate overview (labelwatch.recent-overview.v1).

  PYTHONPATH=src:. python tools/segmented_qualification/recent_publish.py --store PATH --pubdir DIR
  PYTHONPATH=src:. python tools/segmented_qualification/recent_publish.py --pubdir DIR --hold
  ... --hold-marker /etc/labelwatch-operator-safe-hold --activation-marker /etc/labelwatch-recent-qualified-activation

Reads one admitted RecentStore under the same writer/reader lease order as the
account provider, releases the writer lease after the state snapshot, and keeps
a shared reader lease (which fences retirement and maintenance) while counting
retained event payloads. Publication is an atomic replace of <pubdir>/overview.json
and refuses, with a non-zero exit and no replacement, when the directory or file
would not be readable by another service account.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import ExitStack, closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from recent_storage import RecentStore, clock, iso, micros, day_number, day_name  # noqa: E402
from storage import connect, lock, db, custody  # noqa: E402
from labelwatch import recent_overview  # noqa: E402


def _sources(c):
    """Source roster counts from the sticky enrollment table.

    seeded: enrolled sources; established: sources with at least one accepted page.
    probing/refused/pre_epoch_skipped are read only if the collector exposes them.
    """
    columns = {row[1] for row in c.execute('PRAGMA table_info(q_recent_sources)')}
    seeded = c.execute('SELECT COUNT(*) FROM q_recent_sources').fetchone()[0]
    established = c.execute('SELECT COUNT(*) FROM q_recent_sources WHERE last_observed IS NOT NULL').fetchone()[0]
    result = {'seeded': seeded, 'established': established, 'probing': None, 'refused': None,
              'pre_epoch_skipped': None}
    # TODO(collector): expose per-source roster state as q_recent_sources.roster_state
    # in {'established','probing','refused'}; until then probing/refused are unknown.
    if 'roster_state' in columns:
        states = dict(c.execute('SELECT roster_state,COUNT(*) FROM q_recent_sources GROUP BY roster_state').fetchall())
        result.update(probing=states.get('probing', 0), refused=states.get('refused', 0))
    # TODO(collector): expose skipped pre-epoch rows as q_recent_sources.pre_epoch_skipped
    # (per-source integer); until then the total is unknown.
    if 'pre_epoch_skipped' in columns:
        result['pre_epoch_skipped'] = c.execute('SELECT COALESCE(SUM(pre_epoch_skipped),0) FROM q_recent_sources').fetchone()[0]
    return result


def _count_since(c, cut, upper):
    """Exact count of observations at or after cut: whole owner days by index, boundary day by clock."""
    first = day_number(cut.date().isoformat())
    whole = c.execute('SELECT COUNT(*) FROM q_recent_seen WHERE owner_day>? AND event_id<=?', (first, upper)).fetchone()[0]
    edge = c.execute('SELECT COUNT(*) FROM q_recent_seen WHERE owner_day=? AND observed_us>=? AND event_id<=?',
                     (first, micros(cut), upper)).fetchone()[0]
    return whole + edge


def snapshot_state(store, now, *, timeout=10.0, labelers=True):
    """Read every state-owned figure in one read transaction under the writer lease."""
    c = connect(store.state, readonly=True)
    try:
        c.execute('BEGIN')
        store.require(c)
        if c.execute('SELECT 1 FROM q_recent_transition').fetchone():
            raise RuntimeError('recent maintenance recovery pending')
        meta = {key: db.get_meta(c, 'q:recent_' + key) for key in ('start', 'end', 'clock', 'generation', 'gap_overflow')}
        acquisition = store.acquisition(c)
        upper = c.execute('SELECT COALESCE(MAX(event_id),0) FROM q_recent_seen').fetchone()[0]
        cuts = {'events_24h': now - timedelta(hours=24), 'events_7d': now - timedelta(days=7),
                'events_window': now - timedelta(days=recent_overview.WINDOW_DAYS)}
        counts = {key: _count_since(c, cut, upper) for key, cut in cuts.items()}
        for key, cut in (('labelers_active_7d', cuts['events_7d']), ('labelers_active_window', cuts['events_window'])):
            counts[key] = c.execute('SELECT COUNT(*) FROM labelers WHERE last_seen>=?', (iso(cut),)).fetchone()[0]
        window_start = iso(cuts['events_window'])
        gaps = [dict(start=row[0], end=row[1], reason=row[2]) for row in
                c.execute('SELECT start,end,reason FROM q_recent_gaps WHERE end>? ORDER BY start,end,reason', (window_start,))]
        plan = None
        if labelers:
            seven = cuts['events_7d']
            first = day_number(seven.date().isoformat())
            last = day_number(now.date().isoformat())
            segments = dict(c.execute('SELECT identity,status FROM q_segments').fetchall())
            receipts = {owner: receipt for owner, _, receipt in custody.committed(c)}
            plan = []
            for day in range(first, last + 1):
                owner = day_name(day)
                if day == first:
                    low, n = c.execute('SELECT MIN(event_id),COUNT(*) FROM q_recent_seen WHERE owner_day=? AND observed_us>=? AND event_id<=?',
                                       (day, micros(seven), upper)).fetchone()
                else:
                    low, n = c.execute('SELECT MIN(event_id),COUNT(*) FROM q_recent_seen WHERE owner_day=? AND event_id<=?',
                                       (day, upper)).fetchone()
                if not n:
                    continue
                pending = dict(c.execute('SELECT e.labeler_did,COUNT(*) FROM label_events e JOIN q_pending p ON p.id=e.id '
                                         'WHERE p.segment=? AND e.id>=? AND e.id<=? GROUP BY e.labeler_did', (owner, low, upper)).fetchall())
                plan.append({'owner': owner, 'low': low, 'expected': n, 'status': segments.get(owner),
                             'receipt': receipts.get(owner), 'pending': pending})
        c.rollback()
    finally:
        c.close()
    return {'acquisition': acquisition, 'meta': meta, 'upper': upper, 'counts': counts, 'gaps': gaps,
            'gap_overflow': meta['gap_overflow'] == '1', 'observed_through': meta['clock'],
            'sources': None, 'plan': plan}


def _labeler_counts(store, plan, upper, deadline):
    """Per-labeler counts from retained payloads, cross-checked against the observation index."""
    import duckdb
    totals = {}
    duck = duckdb.connect()
    try:
        duck.execute("SET memory_limit='128MB'"); duck.execute('SET threads=1'); duck.execute("SET max_temp_directory_size='0B'")
        for item in plan:
            if time.monotonic() > deadline:
                raise TimeoutError('overview deadline exceeded')
            owner, low = item['owner'], item['low']
            found = dict(item['pending'])
            source = store.root / (owner + '.sqlite')
            if source.exists():
                seg = connect(source, readonly=True)
                try:
                    seg.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                    rows = seg.execute('SELECT labeler_did,COUNT(*) FROM label_events WHERE id>=? AND id<=? GROUP BY labeler_did',
                                       (low, upper)).fetchall()
                finally:
                    seg.close()
            elif item['receipt'] is not None:
                path = Path(item['receipt']['archive_root']) / (owner + '.parquet')
                rows = duck.execute('SELECT labeler_did,COUNT(*) FROM read_parquet(?) WHERE id>=? AND id<=? GROUP BY labeler_did',
                                    [str(path), low, upper]).fetchall()
            else:
                raise RuntimeError('overview payload owner unavailable: ' + owner)
            for did, n in rows:
                found[did] = found.get(did, 0) + n
            if sum(found.values()) != item['expected']:
                raise RuntimeError('overview payload coverage mismatch: ' + owner)
            for did, n in found.items():
                totals[did] = totals.get(did, 0) + n
    finally:
        duck.close()
    return totals


def read_store(store, now, *, timeout=10.0, budget=120.0, labelers=True):
    """Aggregate store statistics for the overview and the landing page.

    labelers=False skips payload reads (top labelers) for a cheap landing summary.
    """
    store = store if isinstance(store, RecentStore) else RecentStore(store)
    now = clock(now.isoformat() if isinstance(now, datetime) else now)
    deadline = time.monotonic() + budget
    with ExitStack() as reader:
        writer = ExitStack()
        try:
            writer.enter_context(lock(store.root, timeout=timeout))
            reader.enter_context(lock(store.root, 'reader.lock', shared=True, timeout=timeout))
            stats = snapshot_state(store, now, labelers=labelers)
            with closing(connect(store.state, readonly=True)) as c:
                stats['sources'] = _sources(c)
        finally:
            writer.close()  # The shared reader lease alone fences retirement/maintenance.
        top = []
        if labelers:
            totals = _labeler_counts(store, stats['plan'], stats['upper'], deadline)
            if len(totals) != stats['counts']['labelers_active_7d']:
                raise RuntimeError('overview labeler activity mismatch')
            top = [{'labeler_did': did, 'count': n} for did, n in totals.items()]
    stats['top_labelers_7d'] = top
    generation = stats['meta']['generation']
    stats['last_maintenance'] = {
        'generation': int(generation),
        'at': iso(clock(stats['meta']['start']) + timedelta(days=recent_overview.WINDOW_DAYS)) if generation != '0' else None,
    }
    return stats


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--store', type=Path)
    parser.add_argument('--pubdir', type=Path, required=True)
    parser.add_argument('--hold', action='store_true', help='publish an explicit held artifact; the store is not read')
    parser.add_argument('--hold-marker', type=Path, action='append', default=[],
                        help='publish the held artifact if this path exists (repeatable)')
    parser.add_argument('--activation-marker', type=Path, default=None,
                        help='publish the held artifact unless this path exists')
    parser.add_argument('--now', default=None, help='UTC acceptance clock; defaults to current UTC')
    parser.add_argument('--budget-seconds', type=float, default=120.0)
    args = parser.parse_args(argv)
    now = clock(args.now) if args.now else datetime.now(timezone.utc)
    try:
        hold = args.hold or any(marker.exists() for marker in args.hold_marker) or (
            args.activation_marker is not None and not args.activation_marker.exists())
        if hold:
            envelope = recent_overview.held(now)
        else:
            if args.store is None:
                parser.error('--store is required unless --hold')
            stats = read_store(args.store, now, budget=args.budget_seconds)
            envelope = recent_overview.overview(stats, now)
        target = recent_overview.publish(args.pubdir, envelope)
    except (recent_overview.OverviewRefused, RuntimeError, TimeoutError, OSError, ValueError) as exc:
        print(json.dumps({'published': False, 'error': type(exc).__name__, 'reason': str(exc)[:240]}, sort_keys=True))
        return 2
    print(json.dumps({'published': True, 'path': str(target), 'hold': envelope['hold'],
                      'generated_at': envelope['generated_at']}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
