"""Measured off-production reuse of the retained production-shaped specimen.

No corpus is generated; rows are streamed into a new campaign-owned store.
This runner never selects or opens production state. Dispatch supplies exact
source identity, occurrence ceiling and independent host reserves.
"""
import argparse
from contextlib import closing
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import resource
import time

import pyarrow.parquet as pq
from storage import atomic, connect, FIELDS, owned, sha
from recent_storage import RecentStore, iso


def allocated(root):
    return sum(p.stat().st_blocks * 512 for p in root.rglob('*') if p.is_file())


def main(args):
    out = owned(args.occurrence)
    out.mkdir()
    ceiling = args.ceiling_bytes
    started = time.monotonic()
    peak = 0

    def progress(phase, **values):
        nonlocal peak
        free = {p: os.statvfs(p).f_bavail * os.statvfs(p).f_frsize for p in ('/', '/data')}
        if min(free.values()) < 64424509440:
            raise RuntimeError('shared host reserve lost; stop before further phase')
        used = allocated(out)
        peak = max(peak, used)
        if used > ceiling:
            raise RuntimeError('admitted occurrence storage ceiling exceeded')
        result = dict(phase=phase, accepted=done, allocated_bytes=used, sampled_peak_bytes=peak,
                      free=free, elapsed=time.monotonic()-started,
                      rss_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                      **values)
        atomic(out/'PROGRESS.json', result)
        print(json.dumps(result), flush=True)
        return result

    def admit_transition():
        # Conservative temporary allowance for archive generation plus global
        # rebuild; no credit is taken for files the transition may reclaim.
        local = list(store.root.glob('*.sqlite'))
        extra = 2 * sum(p.stat().st_size for p in local)
        if allocated(out) + extra > ceiling:
            raise RuntimeError('transition peak exceeds admitted occurrence budget')
        for path in ('/', '/data'):
            fs=os.statvfs(path)
            if fs.f_bavail*fs.f_frsize-extra < 64424509440:
                raise RuntimeError('transition peak would consume shared reserve')

    def state_measurement():
        with closing(connect(store.state, readonly=True)) as c:
            tables = {name:c.execute('SELECT COUNT(*) FROM "'+name+'"').fetchone()[0]
                      for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            physical = dict(page_count=c.execute('PRAGMA page_count').fetchone()[0],
                            freelist_count=c.execute('PRAGMA freelist_count').fetchone()[0],
                            page_size=c.execute('PRAGMA page_size').fetchone()[0],
                            objects={r[0]:r[1] for r in c.execute('SELECT name,sum(pgsize) FROM dbstat GROUP BY name')})
        return tables, physical

    done = 0
    progress('VERIFY_RETAINED_SPECIMEN')
    receipt = json.loads(args.specimen.with_name('SPECIMEN.json').read_text())
    expected = receipt['parquet_sha256']
    if sha(args.specimen) != expected:
        raise RuntimeError('retained specimen identity mismatch')
    start = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
    store = RecentStore.create(out/'store', iso(start))
    active = start.date().isoformat()
    epochs = []
    # Retained12M corpus spans seven synthetic days. Repetition, if requested,
    # adds an explicitly synthetic identity and source-time namespace.
    while done < args.events:
        cycle = done // 12000000
        for batch in pq.ParquetFile(args.specimen).iter_batches(batch_size=1000):
            if done >= args.events:
                break
            rows = batch.to_pylist()[:args.events-done]
            observed = start + dt.timedelta(microseconds=done*604800000000//12000000)
            day = observed.date().isoformat()
            if day != active:
                admit_transition()
                store.rotate(day)
                store.archive(active)
                store.retire(active)
                store.maintain(iso(observed))
                epochs.append(progress('SEALED_AND_RETIRED_HOT_OWNER', owner=active))
                active = day
            tuples = []
            for row in rows:
                if cycle:
                    row['ts'] = iso(dt.datetime.fromisoformat(row['ts'].replace('Z','+00:00')) + dt.timedelta(days=7*cycle))
                    row['event_hash'] = hashlib.sha256((row['event_hash']+':cycle:'+str(cycle)).encode()).hexdigest()
                tuples.append(tuple(row[k] for k in FIELDS[1:]))
            store.ingest(tuples, 'qualified-fixture-source', str(done+len(rows)), iso(observed))
            done += len(rows)
            if done % 100000 == 0:
                progress('INGEST')
        if done < args.events and cycle == done//12000000:
            raise RuntimeError('unexpected specimen row count')
    before = progress('WINDOW_RETAINED')
    counts, physical = state_measurement()
    atomic(out/'RETAINED.json', dict(counts=counts, physical=physical, measurement=before))
    # Exercise the actual account lookup against the retained measured store,
    # not an independently generated small database. The last accepted batch
    # supplies an exact short-interval oracle; the full-window query is bounded
    # and establishes retrieval latency only, not complete dense-account export.
    from recent_provider import RecentProvider
    provider = RecentProvider(store)
    subject = rows[-1]['target_did']
    expected_ids = {row['event_hash'] for row in rows if row['target_did'] == subject}
    query_started = time.monotonic()
    query_end = iso(observed + dt.timedelta(microseconds=1))
    with provider.snapshot(subject, iso(observed), query_end, 10001,
                           timeout_seconds=30, max_bytes=16*1024**2) as answer:
        actual_rows = answer['rows']
        assert len(actual_rows) == len(expected_ids), 'last-batch account correspondence'
        assert all(row['observed_at'] == iso(observed) for row in actual_rows)
        assert {row['uri'] for row in actual_rows} == {row['uri'] for row in rows if row['target_did'] == subject}
        exact_query = dict(rows=len(actual_rows), elapsed=time.monotonic()-query_started,
                           frontier=answer['frontier'], coverage=answer['coverage'])
    query_started = time.monotonic()
    with provider.snapshot(subject, store.frontier()['start'], query_end, 101,
                           timeout_seconds=30, max_bytes=1024**2) as answer:
        bounded_query = dict(rows=len(answer['rows']), elapsed=time.monotonic()-query_started,
                             maximum_rows=101, complete=False)
    atomic(out/'ACCOUNT-QUERIES.json', dict(exact_last_batch=exact_query,
                                           bounded_full_window=bounded_query))
    # Qualify actual block release after a full-window advance, not freelist reuse.
    after_time = start + dt.timedelta(days=61)
    admit_transition()
    store.rotate(after_time.date().isoformat())
    store.archive(active)
    store.retire(active)
    pre_expire = progress('BEFORE_EXPIRE')
    sealed_counts, sealed_physical = state_measurement()
    atomic(out/'SEALED.json', dict(counts=sealed_counts, physical=sealed_physical,
                                 measurement=pre_expire))
    store.maintain(iso(after_time))
    after = progress('AFTER_EXPIRE')
    with closing(connect(store.state, readonly=True)) as c:
        assert c.execute('SELECT COUNT(*) FROM q_recent_seen').fetchone()[0] == 0
        assert c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0] == 0
        assert c.execute('SELECT COUNT(*) FROM custody_archives').fetchone()[0] == 0
        assert c.execute('SELECT COUNT(*) FROM q_segments').fetchone()[0] == 1
        assert c.execute('SELECT COUNT(*) FROM q_recent_transition').fetchone()[0] == 0
        assert c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0] == 0
        from storage import db
        assert db.get_cursor(c, 'qualified-fixture-source') == str(done)
    assert not list((store.root/'archive').glob('*.parquet'))
    assert not list((store.root/'archive').glob('*.identities.sqlite'))
    assert pre_expire['allocated_bytes'] > after['allocated_bytes']
    result = dict(result='PASS', events=done, specimen_sha256=expected,
                  input_scope='Retained production-shaped pseudonymized fixture; 1000-row batches share synthetic observation time (~50.4s/batch). Repeated cycles have synthetic hashes/times; no valid-signature claim',
                  retained=before, retained_table_counts=counts, epochs=epochs,
                  account_queries=dict(exact_last_batch=exact_query,bounded_full_window=bounded_query),
                  pre_expire=pre_expire, post_expire=after,
                  reclaimed_allocated_bytes=pre_expire['allocated_bytes']-after['allocated_bytes'],
                  limits='Sampled allocation peaks only. Does not establish continuous all-source coverage, full30day capacity unless actually run, global production-table population, migration, or activation.',
                  production_mutations=[])
    atomic(out/'RESULT.json', result)


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--occurrence',type=Path,required=True)
    p.add_argument('--specimen',type=Path,required=True)
    p.add_argument('--events',type=int,default=12000000)
    p.add_argument('--ceiling-bytes',type=int,default=34359738368)
    args=p.parse_args()
    try:
        main(args)
    except BaseException as exc:
        if args.occurrence.exists():atomic(args.occurrence/'TERMINAL.json',dict(result='FAILED',error=repr(exc)))
        raise
    else:
        atomic(args.occurrence/'TERMINAL.json',dict(result='PASS'))
