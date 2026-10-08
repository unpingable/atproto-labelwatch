"""C4 concurrent reads under ingest; off-production qualification only.

A new campaign-owned RecentStore is populated (constructed bulk pages or the
accept path, same page/oracle plan as recent_replenish.py). Then one ingest
process keeps accepting pages through begin_source_attempt/accept_page at a
configured rate while one reader process runs K threads of product reads:
account snapshots + timelines for random target DIDs, dense exports paginated
to completion, and /health status reads (recent_product.SiteStatus). Each
reader thread owns a RecentObservations instance (stronger than the deployed
single-threaded HTTPServer, which serializes requests).

Torn-read oracle: every read uses an interval whose end is the population
frontier E0, so later acceptance can only add rows observed after it. Each
concurrent result (count, ordered row digest, refusal) must equal a quiescent
re-read after ingest stops and the generator's own per-target count. Export
manifests must be identical on every page. Ingest latency is measured with
and without readers; the stated bound is the provider's per-read writer-lock
timeout. Finally a day is retired mid-export and the next page must refuse.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import hashlib
import json
import multiprocessing as mp
import os
import random
import signal
import threading
import time
import traceback
import resource
from contextlib import closing
from pathlib import Path

import storage
from storage import atomic, connect, owned, sha
from recent_storage import RecentStore, clock, iso, micros
from recent_replenish import (PAGE_ROWS, DEFAULT_T0, Plan, Specimen, Sampler, allocated, free_bytes, du,
                              open_unlinked, store_census, ingest_pages, bulk_pages_range, percentiles,
                              page_cursors, next_stamp)


def subject(row):
    uri = row[2]
    return uri[5:].split('/')[0] if uri.startswith('at://') else uri


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def compare(concurrent, quiescent, expected_count, row_limit=None):
    """Pure verdict for one target: concurrent equals quiescent equals generator."""
    problems = []
    if concurrent.get('refusal') != quiescent.get('refusal'):
        return ['refusal differs from quiescent read']
    if concurrent.get('refusal') is None:
        if concurrent['count'] != quiescent['count'] or concurrent['digest'] != quiescent['digest']:
            problems.append('torn or unstable snapshot')
        if concurrent['count'] != expected_count:
            problems.append('count differs from generator oracle')
        if not concurrent.get('manifest_stable', True):
            problems.append('manifest changed across pages')
    elif row_limit is not None and concurrent['refusal'] == 'snapshot_row_limit' and expected_count <= row_limit:
        problems.append('row-limit refusal below the limit')
    return problems


def _product(root, storage_root):
    storage.ROOT = Path(storage_root)
    from recent_provider import RecentProvider
    from labelwatch.recent_observations import RecentObservations, Limits
    store = RecentStore(root)
    provider = RecentProvider(store)
    return RecentObservations(provider, Limits(), now=lambda: clock(store.frontier()['end']))


def account_read(product, did, start, end):
    from labelwatch.recent_observations import Refused
    t = time.monotonic()
    out = {'role': 'account', 'did': did}
    try:
        made = product.create(did, start, end)
        summary = product.account(made['cursor'])
        manifest = made['manifest']
        rows, cursor, pages, stable = [], made['cursor'], 0, True
        while cursor:
            page = product.page(cursor)
            pages += 1
            stable &= page['manifest'] == manifest
            rows.extend(page['rows'])
            cursor = page['next_cursor']
        digest = hashlib.sha256(b'\n'.join(encoded(r) for r in rows)).hexdigest()
        stable &= len(rows) == manifest['count'] == summary['summary']['observed_events']
        out.update(count=len(rows), digest=digest, pages=pages, manifest_stable=stable,
                   content_sha256=manifest['content_sha256'], refusal=None)
    except Refused as exc:
        out.update(refusal=str(exc))
    out['seconds'] = time.monotonic() - t
    return out


def export_read(product, did, start, end, pause=0.0):
    from labelwatch.recent_observations import Refused
    t = time.monotonic()
    out = {'role': 'export', 'did': did}
    pages = 0
    try:
        made = product.create_export(did, start, end)
        manifest = made['manifest']
        cursor, digest, count, stable, seen, slowest = made['cursor'], hashlib.sha256(), 0, True, set(), 0
        lo, hi = clock(start), clock(end)
        while cursor:
            p0 = time.monotonic()
            page = product.page(cursor)
            slowest = max(slowest, time.monotonic() - p0)
            pages += 1
            stable &= page['manifest'] == manifest
            for row in page['rows']:
                blob = encoded(row)
                if blob in seen or not lo <= clock(row['observed_at']) < hi:
                    stable = False
                seen.add(blob)
                digest.update(b'\n' + blob)
            count += len(page['rows'])
            if page['terminal']:
                stable &= page.get('count') == count
            cursor = page['next_cursor']
            if pause:
                time.sleep(pause)
        out.update(count=count, digest=digest.hexdigest(), pages=pages, manifest_stable=stable, refusal=None,
                   slowest_page_seconds=slowest)
    except Refused as exc:
        out.update(refusal=str(exc), pages=pages)
    out['seconds'] = time.monotonic() - t
    return out


def health_read(site):
    t = time.monotonic()
    code, body = site.health()
    return {'role': 'health', 'code': code, 'status': body.get('status'), 'seconds': time.monotonic() - t}


def _reader_main(conf):
    """K threads; first pass over every assigned target is mandatory."""
    from recent_product import SiteStatus
    out = Path(conf['out'])
    lock = threading.Lock()
    deadline = time.monotonic() + conf['seconds']
    first_pass = threading.Barrier(conf['readers'])
    workers = []
    log = (out / ('READS-%d.jsonl' % os.getpid())).open('a')

    def emit(record):
        with lock:
            log.write(json.dumps(record) + '\n')
            log.flush()

    def worker(index):
        product = _product(conf['store'], conf['storage_root'])
        role = conf['roles'][index]
        targets = conf['assign'][index]
        repeat = 0
        try:
            if role == 'health':
                site = SiteStatus(product, ttl=0)
                while True:
                    emit({**health_read(site), 'thread': index, 'repeat': repeat, 'at': time.monotonic()})
                    repeat += 1
                    if time.monotonic() > deadline and all(not w.is_alive() for i, w in enumerate(workers)
                                                           if conf['roles'][i] != 'health'):
                        break
                    time.sleep(.1)
                return
            while True:
                for did in targets:
                    # One product instance per read: its 4-snapshot/120 s admission
                    # window would otherwise refuse a reader's own repeats.
                    product = _product(conf['store'], conf['storage_root'])
                    if role == 'account':
                        record = account_read(product, did, conf['start'], conf['end'])
                    else:
                        record = export_read(product, did, conf['start'], conf['end'])
                    emit({**record, 'thread': index, 'repeat': repeat, 'at': time.monotonic()})
                repeat += 1
                if time.monotonic() > deadline or not targets:
                    break
        except Exception as exc:
            emit({'role': role, 'thread': index, 'error': repr(exc), 'traceback': traceback.format_exc()[-2000:]})

    for i in range(conf['readers']):
        workers.append(threading.Thread(target=worker, args=(i,), daemon=True))
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    log.close()


def _ingest_main(conf, stop):
    """Paced acceptance; latencies (monotonic, seconds) written at the end."""
    storage.ROOT = Path(conf['storage_root'])
    store = RecentStore(conf['store'])
    plan = Plan(Specimen(conf['specimen']), conf['sources'], conf['events_per_day'], clock(conf['t0']))
    g, last = conf['first'], conf['last']
    stamp = clock(conf['floor'])
    interval = PAGE_ROWS / conf['rate']
    samples = []
    began = time.monotonic()
    next_at = began
    exhausted = False
    while not stop.is_set():
        if g >= last:
            exhausted = True
            stop.wait(.05)
            continue
        now = time.monotonic()
        if now < next_at:
            stop.wait(next_at - now)
            continue
        next_at += interval
        source, rows = plan.page(g)
        request, following = page_cursors(g, len(plan.sources))
        stamp = next_stamp(plan, g, stamp)
        t = time.monotonic()
        token = store.begin_source_attempt(source, iso(stamp))
        t1 = time.monotonic()
        result = store.accept_page(rows, source=source, request_cursor=request, next_cursor=following,
                                   observed_at=iso(stamp), attempt_token=token)
        t2 = time.monotonic()
        if result['inserted'] != PAGE_ROWS:
            raise RuntimeError('concurrent ingest page not fully accepted')
        samples.append((t - began, t2 - t, t2 - t1))
        g += 1
    atomic(Path(conf['out']) / 'INGEST.json', {'samples': samples, 'pages': g - conf['first'],
                                               'next_page': g, 'day_exhausted': exhausted,
                                               'last_stamp': iso(stamp), 'began_monotonic': began})


def run(args):
    out = owned(args.occurrence)
    out.mkdir()
    (out / 'tmp').mkdir()
    os.environ['TMPDIR'] = str(out / 'tmp')
    began = time.monotonic()
    receipt = json.loads(args.specimen.with_name('SPECIMEN.json').read_text())
    if not args.skip_specimen_hash and sha(args.specimen) != receipt['parquet_sha256']:
        raise RuntimeError('retained specimen identity mismatch')
    if args.kill_points:
        bad = [k for k in args.kill_points if not k.startswith('readers:')]
        if bad:
            raise ValueError('C4 kill points: readers:SECONDS only')
    before = {'free_bytes': free_bytes(), 'occurrence_allocated_bytes': allocated(out)}
    plan = Plan(Specimen(args.specimen), args.sources, args.events_per_day, DEFAULT_T0)
    root = out / 'store'
    sampler = Sampler(out, root, args.ceiling_bytes)
    sampler.start()
    state = {'phase': 'POPULATE'}
    last_progress = [0.0]

    def progress(**values):
        atomic(out / 'PROGRESS.json', {'phase': state['phase'], 'elapsed_seconds': time.monotonic() - began,
                                       'allocated_bytes': allocated(out), 'sampled_peak_bytes': sampler.peak,
                                       'free_bytes': free_bytes(), **values})
        last_progress[0] = time.monotonic()

    def guard():
        if sampler.violation:
            return sampler.violation
        if time.monotonic() - began > args.budget_seconds:
            return 'budget seconds exceeded'
        if time.monotonic() - last_progress[0] >= 30:
            progress()
        return None

    progress()
    store = RecentStore.create(root, iso(plan.t0))
    store.remember_sources([{'did': d, 'endpoint': 'https://%s/' % d[8:]} for d in plan.sources])
    oracle_start = plan.day_start(2)
    counts = collections.Counter()
    floor = plan.t0
    next_id = 1
    population = []
    for day in range(1, args.days + 1):
        first, last = plan.day_pages(day)
        t = time.monotonic()
        def observe(g, stamp, rows):
            if stamp >= oracle_start:
                counts.update(subject(row) for row in rows)
        if args.populate == 'bulk':
            _, floor, next_id = bulk_pages_range(store, plan, first, last, floor, next_id, stop=guard, observe=observe)
        else:
            floor = ingest_pages(store, plan, first, last, floor, stop=guard, observe=observe)[3]
        seconds = time.monotonic() - t
        t = time.monotonic()
        store.advance_day(iso(plan.day_start(day + 1)))
        population.append({'day': day, 'seconds': seconds, 'advance_seconds': time.monotonic() - t,
                           'allocated_bytes': allocated(root)})
        floor = plan.day_start(day + 1)
        progress(day=day)
        if why := guard():
            raise RuntimeError(why)
    frontier = store.frontier()
    e0 = clock(frontier['end'])
    if e0 != plan.day_start(args.days + 1):
        raise AssertionError('population frontier is not the day boundary')
    interval = (iso(oracle_start), iso(e0))
    rng = random.Random(args.seed)
    population_events = args.days * args.events_per_day
    eligible = sorted(counts)
    accounts = rng.sample(eligible, min(args.accounts, len(eligible)))
    dense = [did for did, _ in counts.most_common(args.exports)]
    # Concurrent phase.
    state['phase'] = 'BASELINE'
    progress()
    first, last = plan.day_pages(args.days + 1)
    ctx = mp.get_context('spawn')
    stop = ctx.Event()
    ingest = ctx.Process(target=_ingest_main, args=({
        'store': str(root), 'storage_root': str(storage.ROOT), 'specimen': str(args.specimen),
        'sources': args.sources, 'events_per_day': args.events_per_day, 't0': iso(plan.t0),
        'first': first, 'last': last, 'floor': iso(e0), 'rate': args.ingest_events_per_second,
        'out': str(out)}, stop))
    ingest.start()
    sampler.children.add(ingest.pid)
    t_base = time.monotonic()
    while time.monotonic() - t_base < args.baseline_seconds:
        time.sleep(.5)
        if why := guard():
            raise RuntimeError(why)
    roles = []
    for i in range(args.readers):
        roles.append(('account', 'account', 'export', 'health')[i % 4])
    assign = {i: [] for i in range(args.readers)}
    for role, targets in (('account', accounts), ('export', dense)):
        idx = [i for i, r in enumerate(roles) if r == role] or [0]
        for n, did in enumerate(targets):
            assign[idx[n % len(idx)]].append(did)
    conf = {'store': str(root), 'storage_root': str(storage.ROOT), 'out': str(out), 'readers': args.readers,
            'roles': roles, 'assign': assign, 'start': interval[0], 'end': interval[1], 'seconds': args.read_seconds}
    state['phase'] = 'CONCURRENT'
    progress()
    reader_runs = []
    t_read = time.monotonic()
    pending_kills = sorted(float(k.split(':')[1]) for k in args.kill_points or ())
    reader = ctx.Process(target=_reader_main, args=(conf,))
    reader.start()
    sampler.children.add(reader.pid)
    while reader.is_alive() or pending_kills:
        reader.join(.5)
        if pending_kills and time.monotonic() - t_read >= pending_kills[0]:
            pending_kills.pop(0)
            if reader.is_alive():
                os.kill(reader.pid, signal.SIGKILL)
                reader.join()
                reader_runs.append({'killed_at_seconds': time.monotonic() - t_read, 'exit': reader.exitcode})
                remaining = max(1.0, args.read_seconds - (time.monotonic() - t_read))
                reader = ctx.Process(target=_reader_main, args=({**conf, 'seconds': remaining},))
                reader.start()
                sampler.children.add(reader.pid)
        if why := guard():
            os.kill(reader.pid, signal.SIGKILL)
            stop.set()
            raise RuntimeError(why)
        if not reader.is_alive() and not pending_kills:
            break
    reader_runs.append({'exit': reader.exitcode})
    t_read_end = time.monotonic()
    # A short post-reader tail of ingest alone, then stop.
    time.sleep(min(10.0, args.baseline_seconds))
    stop.set()
    ingest.join(60)
    if ingest.exitcode != 0:
        raise RuntimeError('ingest process failed: %r' % ingest.exitcode)
    sampler.children.discard(ingest.pid)
    ingest_log = json.loads((out / 'INGEST.json').read_text())
    base0 = ingest_log['began_monotonic']
    samples = ingest_log['samples']
    window = lambda lo, hi: [s[1] for s in samples if lo <= s[0] + base0 < hi]
    baseline = window(t_base, t_read)
    concurrent = window(t_read, t_read_end)
    # Quiescent oracle.
    state['phase'] = 'QUIESCENT_ORACLE'
    progress()
    reads = []
    for path in sorted(out.glob('READS-*.jsonl')):
        reads.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    errors = [r for r in reads if 'error' in r]
    quiet = {}
    for did in accounts:
        quiet[('account', did)] = account_read(_product(root, storage.ROOT), did, *interval)
    for did in dense:
        quiet[('export', did)] = export_read(_product(root, storage.ROOT), did, *interval)
    product = _product(root, storage.ROOT)
    verdicts = []
    for record in reads:
        if record.get('role') not in ('account', 'export') or 'error' in record:
            continue
        problems = compare(record, quiet[(record['role'], record['did'])], counts[record['did']],
                           row_limit=1000 if record['role'] == 'account' else None)
        if problems:
            verdicts.append({'did': record['did'], 'role': record['role'], 'problems': problems,
                             'concurrent': record, 'quiescent': quiet[(record['role'], record['did'])]})
    covered_accounts = {r['did'] for r in reads if r.get('role') == 'account' and 'error' not in r}
    covered_exports = {r['did'] for r in reads if r.get('role') == 'export' and 'error' not in r}
    # Retirement mid-export: the next page must refuse; no lease pins the expired day.
    state['phase'] = 'RETIRE_MID_EXPORT'
    progress()
    from labelwatch.recent_observations import Refused
    now = clock(store.frontier()['end'])
    expiry = {'did': dense[0]}
    old_start = iso(now - dt.timedelta(days=30) + dt.timedelta(seconds=1))
    made = product.create_export(dense[0], old_start, iso(e0))
    first_page = product.page(made['cursor'])
    expiry['first_page_rows'] = len(first_page['rows'])
    expiring_owner = plan.day_start(1).date().isoformat()
    expired_files = sorted(p.name for p in (root / 'archive').glob(expiring_owner + '.*'))
    expired_bytes = sum(p.stat().st_blocks * 512 for p in (root / 'archive').glob(expiring_owner + '.*'))
    before_alloc = allocated(root)
    t = time.monotonic()
    advance = store.advance_day(iso(plan.day_start(args.days + 2)))
    expiry['advance_seconds'] = time.monotonic() - t
    expiry['expired_owners'] = (advance.get('maintenance') or {}).get('expired_owners')
    try:
        product.page(first_page['next_cursor']) if first_page['next_cursor'] else None
        expiry['next_page'] = 'served' if first_page['next_cursor'] else 'terminal_before_retirement'
    except Refused as exc:
        expiry['next_page'] = 'refused:' + str(exc)
    try:
        _product(root, storage.ROOT).create(dense[0], old_start, iso(e0))
        expiry['new_snapshot_on_retired_range'] = 'served'
    except Refused as exc:
        expiry['new_snapshot_on_retired_range'] = 'refused:' + str(exc)
    expiry['expired_files_before'] = expired_files
    expiry['expired_files_after'] = sorted(p.name for p in (root / 'archive').glob(expiring_owner + '.*'))
    expiry['expired_bytes'] = expired_bytes
    expiry['store_allocated_before'] = before_alloc
    expiry['store_allocated_after'] = allocated(root)
    expiry['open_unlinked'] = open_unlinked(root)
    expiry['pass'] = (expiry['next_page'].startswith('refused:') and expiry['new_snapshot_on_retired_range'].startswith('refused:')
                      and expiring_owner in (expiry['expired_owners'] or []) and not expiry['expired_files_after']
                      and not expiry['open_unlinked'])
    # Verdicts.
    bound = args.ingest_block_bound_seconds
    lat_base, lat_conc = percentiles(baseline), percentiles(concurrent)
    blocking_ok = bool(lat_conc) and lat_conc['max'] <= bound + (lat_base['max'] if lat_base else 0)
    role_stats = {}
    for role in ('account', 'export', 'health'):
        rs = [r for r in reads if r.get('role') == role and 'error' not in r]
        role_stats[role] = {'reads': len(rs), 'latency_seconds': percentiles([r['seconds'] for r in rs]),
                            'refusals': dict(collections.Counter(r.get('refusal') for r in rs if r.get('refusal'))),
                            'health_status': dict(collections.Counter(r.get('status') for r in rs)) if role == 'health' else None}
    passed = (not verdicts and not errors and covered_accounts == set(accounts) and covered_exports == set(dense)
              and blocking_ok and expiry['pass'] and len(baseline) > 0 and len(concurrent) > 0)
    result = {
        'result': 'PASS' if passed else 'FAIL', 'control': 'C4', 'source_revision': args.source_revision,
        'specimen_sha256': receipt['parquet_sha256'],
        'args': {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        'population': {'events': population_events, 'days': args.days, 'mode': args.populate,
                       'seconds': sum(p['seconds'] + p['advance_seconds'] for p in population),
                       'store_allocated_bytes': allocated(root)},
        'interval': interval, 'accounts': len(accounts), 'dense_exports': dense,
        'dense_expected_rows': {d: counts[d] for d in dense},
        'reader_threads': args.readers, 'roles': roles, 'reader_runs': reader_runs,
        'reads': role_stats, 'reader_errors': errors[:10], 'torn_or_mismatched': verdicts[:20],
        'torn_or_mismatched_count': len(verdicts),
        'ingest': {'rate_target_events_per_second': args.ingest_events_per_second,
                   'pages': ingest_log['pages'], 'day_exhausted': ingest_log['day_exhausted'],
                   'accept_latency_baseline_seconds': lat_base, 'accept_latency_with_readers_seconds': lat_conc,
                   'achieved_events_per_second_with_readers': len(concurrent) * PAGE_ROWS / max(1e-9, t_read_end - t_read),
                   'stated_block_bound_seconds': bound, 'block_bound_held': blocking_ok},
        'retire_mid_export': expiry, 'sampled_peak_bytes': sampler.peak, 'before': before,
        'after': {'free_bytes': free_bytes(), 'occurrence_allocated_bytes': allocated(out)},
        'du_store': du(root), 'elapsed_seconds': time.monotonic() - began,
        'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        'child_rss_peak_bytes': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024,
        'scope': ('Reader threads in one process, each with its own RecentObservations; product clock = store '
                  'frontier end. Interval end fixed at the population frontier so concurrent acceptance cannot '
                  'change the answer; equality with a quiescent re-read and the generator count is the torn-read '
                  'oracle. Accept latency = begin_source_attempt + accept_page wall time. Population via '
                  + ('constructed bulk pages (no acceptance lookups).' if args.populate == 'bulk' else 'the accept path.')),
        'production_mutations': [],
    }
    sampler.stop.set()
    sampler.join()
    atomic(out / 'RESULT.json', result)
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--occurrence', type=Path, required=True)
    p.add_argument('--specimen', type=Path, required=True)
    p.add_argument('--days', type=int, default=30, help='population days before the concurrent day')
    p.add_argument('--events-per-day', type=int, required=True)
    p.add_argument('--populate', choices=('bulk', 'accept'), default='bulk')
    p.add_argument('--sources', type=int, default=16)
    p.add_argument('--readers', type=int, default=8)
    p.add_argument('--accounts', type=int, default=100)
    p.add_argument('--exports', type=int, default=5)
    p.add_argument('--read-seconds', type=float, default=60.0)
    p.add_argument('--baseline-seconds', type=float, default=20.0)
    p.add_argument('--ingest-events-per-second', type=float, default=500.0)
    p.add_argument('--ingest-block-bound-seconds', type=float, default=10.0,
                   help="stated bound: the product's provider timeout on one read's writer-lock hold")
    p.add_argument('--kill-points', nargs='*', default=[], help='readers:SECONDS SIGKILLs the reader process')
    p.add_argument('--ceiling-bytes', type=int, required=True)
    p.add_argument('--budget-seconds', type=float, required=True)
    p.add_argument('--seed', type=int, default=20261008)
    p.add_argument('--source-revision', default=None)
    p.add_argument('--skip-specimen-hash', action='store_true', help='tests only')
    args = p.parse_args(argv)
    owned(args.occurrence)
    try:
        result = run(args)
    except BaseException as exc:
        if args.occurrence.exists():
            atomic(args.occurrence / 'TERMINAL.json', {'result': 'FAIL', 'error': repr(exc),
                                                      'traceback': traceback.format_exc()[-4000:]})
        raise
    atomic(args.occurrence / 'TERMINAL.json', {'result': result['result']})
    print(json.dumps({'result': result['result'], 'torn': result['torn_or_mismatched_count']}), flush=True)
    if result['result'] != 'PASS':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
