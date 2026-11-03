"""C3 sustained replenishment + recovery burst; off-production qualification only.

Drives a new campaign-owned RecentStore through its public collector API
(remember_sources, begin_source_attempt, accept_page, record_gap,
advance_day, frontier). Interruptions are real process deaths: a spawned
child runs the day's ingest or the day-boundary advance and is SIGKILLed,
either by the parent at a progress threshold or by itself at a named death
phase of the existing store hooks. The parent then reopens the store and
continues as a restarted collector would (cursor taken from the store).

Oracle: every page is a pure function of its global page index g (specimen
row g*100+slot, synthetic source g%S, cursor chain, nominal authored time),
so the expected identity digest of each day is computed independently of the
store and compared with the vessel and, after the advance, the archive.
Source-authored time is derived from the nominal schedule, never from the
acceptance clock, and stays inside the collector's late allowance.

No production state is selected, opened or enrolled. Not an adoption claim.
"""
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import multiprocessing as mp
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
import traceback
import resource
from contextlib import closing
from pathlib import Path

import storage
from storage import FIELDS, atomic, connect, owned, sha
from recent_storage import RecentStore, clock, iso, micros, day_number

UTC = dt.timezone.utc
PAGE_ROWS = 100
HOST_RESERVE = 64424509440  # crow 60 GiB on each of / and /data
try:  # read-only use of the collector's declared allowance
    from labelwatch.recent_collector import LATE_ALLOWANCE
except Exception:  # pragma: no cover - collector module mid-change elsewhere
    LATE_ALLOWANCE = dt.timedelta(hours=48)
LAG_NORMAL = dt.timedelta(seconds=5)
LAG_LATE = dt.timedelta(hours=47)
LATE_EVERY = 100           # one page in 100 is authored 47 h before nominal
DEFAULT_T0 = dt.datetime(2026, 11, 1, tzinfo=UTC)
MAINT_PHASES = ('after_recent_plan', 'after_recent_authority', 'during_recent_unlink')
ARCHIVE_PHASES = ('after_parquet_before_checkpoint', 'after_archive_commit_before_receipt',
                  'after_receipt_before_ack', 'after_checkpoint_before_retirement')
RETIRE_PHASES = ('after_unlink_before_retired',)
INGEST_PHASES = ('before_recent_ingest_commit', 'after_recent_ingest_commit', 'after_effect_before_identity')


# ---------------------------------------------------------------- pure oracle

class Digest:
    """Order-independent identity multiset digest: count, sum and xor mod 2^256."""
    MOD = 1 << 256

    def __init__(self, count=0, total=0, xor=0):
        self.count, self.total, self.xor = count, total, xor

    def add(self, identity):
        value = int(identity, 16)
        self.count += 1
        self.total = (self.total + value) % self.MOD
        self.xor ^= value

    def update(self, identities):
        for identity in identities:
            self.add(identity)
        return self

    def merge(self, other):
        return Digest(self.count + other.count, (self.total + other.total) % self.MOD, self.xor ^ other.xor)

    def as_dict(self):
        return {'count': self.count, 'sum': format(self.total, '064x'), 'xor': format(self.xor, '064x')}

    def __eq__(self, other):
        return isinstance(other, Digest) and (self.count, self.total, self.xor) == (other.count, other.total, other.xor)

    def __repr__(self):
        return 'Digest(%r)' % self.as_dict()


def source_names(n):
    if not 0 < n <= 2048:
        raise ValueError('source roster bound')
    return ['did:web:c3-source-%04d.invalid' % k for k in range(n)]


def page_cursors(g, sources):
    """Per-source opaque chain: page g follows page g-S of the same source."""
    return (None if g < sources else str(g - sources + 1)), str(g + 1)


def committed_through(cursor):
    """Global index of the last committed page of a source, -1 when none."""
    return -1 if cursor is None else int(cursor) - 1


def resume_point(cursors, first, last, sources):
    """First uncommitted page of the day [first,last) from stored cursors.

    Pages are offered strictly in g order with one attempt in flight, so the
    committed set must be a prefix. A hole (lost page) or a cursor beyond the
    plan (manufactured progress) is refused.
    """
    done = {s: committed_through(cursors[s]) for s in range(sources)}
    resume = last
    for g in range(first, last):
        if done[g % sources] < g:
            resume = g
            break
    for s, through in done.items():
        if through >= resume and through >= first:
            raise AssertionError('committed pages are not a prefix: source %d through %d, resume %d' % (s, through, resume))
        if through >= last:
            raise AssertionError('cursor beyond planned pages')
    return resume


def nominal(t0, pages_per_day, g):
    """Nominal observation time of page g; authored time derives from it."""
    day, j = divmod(g, pages_per_day)
    step = 86400 * 1000000 // (pages_per_day + 2)
    return t0 + dt.timedelta(days=day, microseconds=(j + 1) * step)


def authored(t0, pages_per_day, g, slot):
    lag = LAG_LATE if g % LATE_EVERY == LATE_EVERY - 1 else LAG_NORMAL
    return nominal(t0, pages_per_day, g) - lag - dt.timedelta(microseconds=slot)


def within_allowance(observed, cts, epoch):
    return dt.timedelta(0) <= observed - cts < LATE_ALLOWANCE and cts >= epoch - LATE_ALLOWANCE


def plateau(values, tolerance):
    """Steady state: (max-min)/median of end-of-day allocation within tolerance."""
    if len(values) < 2:
        return {'pass': False, 'reason': 'fewer than two samples'}
    spread = (max(values) - min(values)) / statistics.median(values)
    return {'pass': spread <= tolerance, 'spread': spread, 'tolerance': tolerance,
            'min': min(values), 'max': max(values), 'samples': len(values)}


def covered(gaps, start, end, reason=None):
    """True when [start,end) lies inside one recorded gap (optionally of reason)."""
    lo, hi = clock(start), clock(end)
    return any(clock(a) <= lo and hi <= clock(b) and (reason is None or r == reason) for a, b, r in gaps)


def parse_kill_points(values):
    """ingest:DAY:FRACTION[:PHASE] | maintain:DAY:PHASE | maintain:DAY:timer:SECONDS."""
    points = []
    for text in values or ():
        parts = text.split(':')
        kind = parts[0]
        if kind == 'ingest' and len(parts) in (3, 4):
            fraction = float(parts[2])
            if not 0 <= fraction < 1:
                raise ValueError('ingest kill fraction must be in [0,1)')
            phase = parts[3] if len(parts) == 4 else None
            if phase is not None and phase not in INGEST_PHASES:
                raise ValueError('unknown ingest death phase ' + phase)
            points.append({'kind': 'ingest', 'day': int(parts[1]), 'fraction': fraction,
                           'mode': 'phase' if phase else 'parent_sigkill', 'phase': phase, 'spec': text})
        elif kind == 'maintain' and len(parts) == 3:
            phase = parts[2]
            if phase not in MAINT_PHASES + ARCHIVE_PHASES + RETIRE_PHASES:
                raise ValueError('unknown maintenance death phase ' + phase)
            points.append({'kind': 'maintain', 'day': int(parts[1]), 'mode': 'phase', 'phase': phase, 'spec': text})
        elif kind == 'maintain' and len(parts) == 4 and parts[2] == 'timer':
            points.append({'kind': 'maintain', 'day': int(parts[1]), 'mode': 'parent_sigkill',
                           'seconds': float(parts[3]), 'phase': None, 'spec': text})
        else:
            raise ValueError('kill point syntax: ' + text)
    keys = [(p['kind'], p['day']) for p in points]
    if len(set(keys)) != len(keys):
        raise ValueError('one kill point per kind and day')
    return points


def percentiles(values):
    if not values:
        return None
    ordered = sorted(values)
    rank = lambda q: ordered[min(len(ordered) - 1, max(0, int(-(-q * len(ordered) // 1)) - 1))]
    return {'n': len(ordered), 'p50': rank(.5), 'p99': rank(.99), 'max': ordered[-1], 'mean': sum(ordered) / len(ordered)}


# ---------------------------------------------------------------- generator

class Specimen:
    """Row-group-indexed access to the retained production-shaped specimen."""

    def __init__(self, path):
        import pyarrow.parquet as pq
        self.path = Path(path)
        self.file = pq.ParquetFile(self.path)
        meta = self.file.metadata
        self.rows = meta.num_rows
        self.starts = []
        n = 0
        for i in range(meta.num_row_groups):
            self.starts.append(n)
            n += meta.row_group(i).num_rows
        self._group, self._cache = None, None

    def row(self, index):
        i = index % self.rows
        group = bisect.bisect_right(self.starts, i) - 1
        if group != self._group:
            self._cache = self.file.read_row_group(group).to_pylist()
            self._group = group
        return self._cache[i - self.starts[group]]


class Plan:
    def __init__(self, specimen, sources, events_per_day, t0=DEFAULT_T0):
        if events_per_day % PAGE_ROWS or events_per_day <= 0:
            raise ValueError('events per day must be a positive multiple of 100')
        self.specimen = specimen
        self.sources = source_names(sources)
        self.pages_per_day = events_per_day // PAGE_ROWS
        self.t0 = t0

    def day_pages(self, day):
        """Global page range [first,last) of 1-based simulated day."""
        return (day - 1) * self.pages_per_day, day * self.pages_per_day

    def day_start(self, day):
        return self.t0 + dt.timedelta(days=day - 1)

    def page(self, g):
        from labelwatch import db as lw_db, ingest as lw_ingest
        source = self.sources[g % len(self.sources)]
        rows = []
        for slot in range(PAGE_ROWS):
            raw = self.specimen.row(g * PAGE_ROWS + slot)
            label = {k: raw.get(k) for k in ('uri', 'cid', 'val', 'exp', 'sig')}
            label.update(src=source, labeler_did=source, neg=bool(raw.get('neg')),
                         cts=iso(authored(self.t0, self.pages_per_day, g, slot)))
            n = lw_ingest.normalize_label(label, strict_identity=True)
            rows.append(tuple(getattr(n, k) for k in FIELDS[1:-1]) + (lw_db.parse_target_did(n.uri),))
        return source, rows

    def digest(self, first, last):
        d = Digest()
        for g in range(first, last):
            d.update(row[9] for row in self.page(g)[1])
        return d


# ---------------------------------------------------------------- measurement

def allocated(root):
    total = 0
    for base, dirs, files in os.walk(root):
        for name in files:
            try:
                total += os.lstat(os.path.join(base, name)).st_blocks * 512
            except FileNotFoundError:
                pass
    return total


def free_bytes():
    out = {}
    for p in ('/', '/data'):
        fs = os.statvfs(p)
        out[p] = fs.f_bavail * fs.f_frsize
    return out


def du(root):
    result = {}
    for flag, key in (('-sb', 'apparent_bytes'), ('-sB1', 'allocated_bytes')):
        out = subprocess.run(['du', flag, str(root)], capture_output=True, text=True)
        result[key] = int(out.stdout.split()[0]) if out.returncode == 0 and out.stdout else None
    return result


def open_unlinked(root, pid='self'):
    """Descriptors of a process that still point at deleted files under root."""
    found = []
    base = Path('/proc') / str(pid) / 'fd'
    try:
        entries = list(base.iterdir())
    except FileNotFoundError:
        return found
    for fd in entries:
        try:
            link = os.readlink(fd)
        except OSError:
            continue
        if link.startswith(str(root)) and link.endswith(' (deleted)'):
            found.append(link)
    return found


class Sampler(threading.Thread):
    """Allocation/WAL/reserve sampler; enforces the occurrence ceiling."""

    def __init__(self, occurrence, store_root, ceiling, period=.2):
        super().__init__(daemon=True)
        self.occurrence, self.store_root, self.ceiling, self.period = occurrence, store_root, ceiling, period
        self.peak = 0
        self.window = {'peak': 0, 'wal': 0}
        self.violation = None
        self.children = set()
        self.started = time.monotonic()
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.last_write = 0

    def open_window(self):
        with self.lock:
            used = allocated(self.occurrence)
            self.window = {'peak': used, 'wal': self.wal()}
            return used

    def close_window(self):
        with self.lock:
            used = allocated(self.occurrence)
            self.window['peak'] = max(self.window['peak'], used)
            self.window['wal'] = max(self.window['wal'], self.wal())
            return dict(self.window)

    def wal(self):
        size = 0
        try:
            for entry in os.scandir(self.store_root):
                if entry.name.endswith('-wal'):
                    try:
                        size = max(size, entry.stat().st_size)
                    except FileNotFoundError:
                        pass
        except FileNotFoundError:
            pass
        return size

    def run(self):
        while not self.stop.wait(self.period):
            try:
                self.sample()
            except Exception as exc:  # sampling must not mask the run
                self.violation = self.violation or ('sampler error: ' + repr(exc))

    def sample(self):
        used = allocated(self.occurrence)
        wal = self.wal()
        free = free_bytes()
        with self.lock:
            self.peak = max(self.peak, used)
            self.window['peak'] = max(self.window['peak'], used)
            self.window['wal'] = max(self.window['wal'], wal)
        if used > self.ceiling:
            self.violation = 'occurrence allocation ceiling exceeded'
        if min(free.values()) < HOST_RESERVE:
            self.violation = 'shared host reserve would be lost'
        if self.violation:
            for pid in list(self.children):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        now = time.monotonic()
        if now - self.last_write >= 1:
            self.last_write = now
            atomic(self.occurrence / 'ALLOCATION.json', {
                'allocated_bytes': used, 'sampled_peak_bytes': self.peak, 'wal_bytes': wal, 'free': free,
                'elapsed': now - self.started, 'sample_period_seconds': self.period,
                'ceiling_bytes': self.ceiling, 'reserve_each_bytes': HOST_RESERVE,
                'limits': 'Point samples of visible allocated files; a sampled lower bound of the true peak.'})


# ---------------------------------------------------------------- store driving

def store_census(store):
    """Read-only verification projection (never used to drive the store)."""
    with closing(connect(store.state, readonly=True)) as c:
        q = lambda sql, *a: c.execute(sql, a).fetchone()[0]
        return {
            'pending': q('SELECT COUNT(*) FROM q_pending'),
            'rollover_transition': q('SELECT COUNT(*) FROM q_transition'),
            'recent_transition': q('SELECT COUNT(*) FROM q_recent_transition'),
            'open_attempts': [dict(r) for r in c.execute(
                'SELECT did,attempt_start FROM q_recent_sources WHERE attempt_token IS NOT NULL ORDER BY did')],
            'segments': [tuple(r) for r in c.execute('SELECT identity,status FROM q_segments ORDER BY identity')],
            'seen': q('SELECT COUNT(*) FROM q_recent_seen'),
            'gaps': [tuple(r) for r in c.execute('SELECT start,end,reason FROM q_recent_gaps ORDER BY start')],
        }


def stored_cursors(store, plan):
    return {s: store.collector_cursor(did) for s, did in enumerate(plan.sources)}


def vessel_digest(path):
    with closing(connect(path, readonly=True)) as c:
        rows = [r[0] for r in c.execute('SELECT event_hash FROM label_events')]
    return Digest().update(rows), len(set(rows))


def archive_digest(path):
    import pyarrow.parquet as pq
    values = pq.read_table(path, columns=['event_hash']).column('event_hash').to_pylist()
    return Digest().update(values), len(set(values))


def next_stamp(plan, g, previous):
    """Acceptance clock of page g: nominal, never before the restart floor."""
    stamp = max(nominal(plan.t0, plan.pages_per_day, g), previous + dt.timedelta(microseconds=1))
    if stamp.date() != nominal(plan.t0, plan.pages_per_day, g).date():
        raise RuntimeError('recovery pushed page %d past its observation day' % g)
    return stamp


def planned_stamps(plan, first, last, floor):
    out, stamp = [], floor
    for g in range(first, last):
        stamp = next_stamp(plan, g, stamp)
        out.append(stamp)
    return out


def ingest_pages(store, plan, first, last, floor, death=None, report=None, latencies=None, stop=None, stamps=None,
                 observe=None):
    """Offer pages [first,last) in order; returns (accepted, duplicates, replayed, last stamp)."""
    accepted = duplicates = replayed = 0
    epoch = plan.t0
    stamp = floor
    for g in range(first, last):
        if stop is not None and (why := stop()):
            raise RuntimeError(why)
        source, rows = plan.page(g)
        request, following = page_cursors(g, len(plan.sources))
        stamp = next_stamp(plan, g, stamp)
        if stamps is not None:
            stamps.append(micros(stamp))
        if observe is not None:
            observe(g, stamp, rows)
        for slot, row in enumerate(rows):
            if not within_allowance(stamp, clock(row[8]), epoch):
                raise RuntimeError('generated authored time outside late allowance')
        t = time.monotonic()
        token = store.begin_source_attempt(source, iso(stamp))
        result = store.accept_page(rows, source=source, request_cursor=request, next_cursor=following,
                                   observed_at=iso(stamp), attempt_token=token,
                                   death=death(g) if death else None)
        if latencies is not None:
            latencies.append(time.monotonic() - t)
        accepted += result['inserted']
        duplicates += len(result.get('duplicate_ids') or ())
        replayed += bool(result.get('replayed_page'))
        if report is not None:
            report(g)
    return accepted, duplicates, replayed, stamp


def bulk_pages_range(store, plan, first, last, floor, next_id, stamps=None, stop=None, observe=None):
    """Constructed (offline) prefill: constructed_window.bulk_pages, <=10 pages/commit.

    Same pages, cursors and clock as the accept path, but no acceptance lookups;
    used only to fill the window before the measured days.
    """
    from constructed_window import bulk_pages
    stamp = floor
    batch = []
    for g in range(first, last):
        if stop is not None and (why := stop()):
            raise RuntimeError(why)
        source, rows = plan.page(g)
        request, following = page_cursors(g, len(plan.sources))
        stamp = next_stamp(plan, g, stamp)
        if stamps is not None:
            stamps.append(micros(stamp))
        if observe is not None:
            observe(g, stamp, rows)
        batch.append(dict(rows=rows, source=source, request_cursor=request, next_cursor=following, observed_at=iso(stamp)))
        if len(batch) == 10 or g == last - 1:
            next_id = bulk_pages(store, batch, next_id)
            batch = []
    return (last - first) * PAGE_ROWS, stamp, next_id


def _self_kill(phase):
    def death(name):
        if name == phase:
            os.kill(os.getpid(), signal.SIGKILL)
    return death


def _ingest_child(conf, pipe):
    """Spawned process: same ingest code; parent or a death phase kills it."""
    storage.ROOT = Path(conf['storage_root'])
    store = RecentStore(conf['store'])
    plan = Plan(Specimen(conf['specimen']), conf['sources'], conf['events_per_day'], clock(conf['t0']))
    target = conf.get('phase_page')
    death = (lambda g: _self_kill(conf['phase']) if g == target else None) if conf.get('phase') else None
    ingest_pages(store, plan, conf['first'], conf['last'], clock(conf['floor']), death=death,
                 report=lambda g: pipe.send(g))
    pipe.send('done')


def _advance_child(conf):
    storage.ROOT = Path(conf['storage_root'])
    store = RecentStore(conf['store'])
    phase = conf.get('phase')
    if phase in MAINT_PHASES:
        original = store.maintain
        store.maintain = lambda now, archive=None, death=None: original(now, archive, death=_self_kill(phase))
    elif phase in ARCHIVE_PHASES:  # existing cut(): os._exit(73) at the named phase
        original_archive = store.archive
        store.archive = lambda identity, destination=None, **kw: original_archive(identity, destination, death=phase)
    elif phase in RETIRE_PHASES:
        original_retire = store.retire
        store.retire = lambda identity, **kw: original_retire(identity, death=phase)
    store.advance_day(conf['now'])


# ---------------------------------------------------------------- C3 runner

class Run:
    def __init__(self, args):
        self.args = args
        self.out = owned(args.occurrence)
        self.began = time.monotonic()
        self.last_progress = 0
        self.days = []
        self.kills = []
        self.state = 'START'

    def progress(self, **values):
        atomic(self.out / 'PROGRESS.json', {'phase': self.state, 'elapsed_seconds': time.monotonic() - self.began,
                                            'allocated_bytes': allocated(self.out),
                                            'sampled_peak_bytes': self.sampler.peak, 'free_bytes': free_bytes(),
                                            'days_completed': len(self.days), **values})
        self.last_progress = time.monotonic()

    def guard(self):
        if self.sampler.violation:
            return self.sampler.violation
        if time.monotonic() - self.began > self.args.budget_seconds:
            return 'budget seconds exceeded'
        if time.monotonic() - self.last_progress >= 30:
            self.progress()
        return None

    def spawn(self, target, *a):
        ctx = mp.get_context('spawn')
        proc = ctx.Process(target=target, args=a, daemon=False)
        proc.start()
        self.sampler.children.add(proc.pid)
        return proc

    def reap(self, proc):
        proc.join()
        self.sampler.children.discard(proc.pid)
        return proc.exitcode


def run(args):
    r = Run(args)
    out = r.out
    out.mkdir()
    (out / 'tmp').mkdir()
    os.environ['TMPDIR'] = str(out / 'tmp')  # SQLite FILE temp stays on this filesystem
    points = parse_kill_points(args.kill_points)
    import random
    jitter = random.Random(args.seed)
    receipt = json.loads(args.specimen.with_name('SPECIMEN.json').read_text())
    if not args.skip_specimen_hash and sha(args.specimen) != receipt['parquet_sha256']:
        raise RuntimeError('retained specimen identity mismatch')
    before = {'free_bytes': free_bytes(), 'occurrence_allocated_bytes': allocated(out)}
    plan = Plan(Specimen(args.specimen), args.sources, args.events_per_day, DEFAULT_T0)
    total_days = args.prefill_days + args.days
    root = out / 'store'
    r.sampler = Sampler(out, root, args.ceiling_bytes)
    r.sampler.start()
    r.progress(state='CREATE')
    store = RecentStore.create(root, iso(plan.t0))
    store.remember_sources([{'did': d, 'endpoint': 'https://%s/' % d[8:]} for d in plan.sources])
    expected_by_day = {}
    floor = plan.t0
    next_id = 1
    gaps_expected = []
    for day in range(1, total_days + 1):
        measured = day > args.prefill_days
        k = day - args.prefill_days
        r.state = 'INGEST' if measured else 'PREFILL'
        first, last = plan.day_pages(day)
        owner = plan.day_start(day).date().isoformat()
        record = {'day': day, 'measured_day': k if measured else None, 'owner': owner,
                  'planned_events': (last - first) * PAGE_ROWS, 'accepted': 0, 'duplicates': 0,
                  'replayed_pages': 0, 'kills': []}
        latencies = []
        day_stamps = []
        t_ingest = time.monotonic()
        r.sampler.open_window()
        kill = next((p for p in points if measured and p['kind'] == 'ingest' and p['day'] == k), None)
        start = first
        expected = None
        if kill:
            expected = plan.digest(first, last)  # independent of what the child reports
            threshold = first + int(kill['fraction'] * (last - first))
            parent_end, child_end = mp.get_context('spawn').Pipe(duplex=False)
            conf = {'store': str(root), 'specimen': str(args.specimen), 'sources': args.sources,
                    'events_per_day': args.events_per_day, 't0': iso(plan.t0), 'first': first, 'last': last,
                    'floor': iso(floor), 'phase': kill['phase'], 'phase_page': threshold,
                    'storage_root': str(storage.ROOT)}
            proc = r.spawn(_ingest_child, conf, child_end)
            child_end.close()
            reported = first - 1
            killed_at = None
            while True:
                if parent_end.poll(.05):
                    try:
                        msg = parent_end.recv()
                    except EOFError:
                        break
                    if msg == 'done':
                        break
                    reported = msg
                    if kill['mode'] == 'parent_sigkill' and reported + 1 >= threshold and killed_at is None:
                        # Seeded jitter so the kill lands anywhere in the next page's work.
                        time.sleep(jitter.uniform(0, args.kill_jitter_seconds))
                        os.kill(proc.pid, signal.SIGKILL)
                        killed_at = time.monotonic()
                elif not proc.is_alive():
                    break
                why = r.guard()
                if why:
                    os.kill(proc.pid, signal.SIGKILL)
                    raise RuntimeError(why)
            code = r.reap(proc)
            parent_end.close()
            if code != -signal.SIGKILL:
                raise RuntimeError('ingest kill point not exercised (exit %r)' % code)
            inspection = store_census(store)
            store = RecentStore(root)  # reopen
            cursors = stored_cursors(store, plan)
            start = resume_point(cursors, first, last, len(plan.sources))
            in_flight = nominal(plan.t0, plan.pages_per_day, start)
            last_attempt = max([clock(a['attempt_start']) for a in inspection['open_attempts']] or [in_flight])
            resume_floor = max(last_attempt, in_flight) + dt.timedelta(seconds=args.recovery_seconds)
            event = {'spec': kill['spec'], 'mode': kill['mode'], 'phase': kill['phase'], 'exit': code,
                     'child_reported_through': reported, 'store_resume_page': start, 'pages_committed_before_kill': start - first,
                     'pending_rows_at_reopen': inspection['pending'], 'open_attempts_at_reopen': inspection['open_attempts'],
                     'rollover_transition': inspection['rollover_transition'], 'recent_transition': inspection['recent_transition'],
                     'resume_observed_at': iso(resume_floor),
                     'report_lag_pages': start - 1 - reported}
            # Deliberate redelivery of the last committed page (provider replay) must not duplicate.
            if start > first:
                g = start - 1
                source, rows = plan.page(g)
                request, following = page_cursors(g, len(plan.sources))
                if store.collector_cursor(source) == following:
                    token = store.begin_source_attempt(source, iso(resume_floor))
                    replay = store.accept_page(rows, source=source, request_cursor=request, next_cursor=following,
                                               observed_at=iso(resume_floor), attempt_token=token)
                    event['redelivered_last_page'] = {'g': g, 'inserted': replay['inserted'],
                                                      'replayed_page': bool(replay.get('replayed_page'))}
                    if replay['inserted']:
                        raise AssertionError('redelivered page inserted rows')
            day_stamps.extend(micros(x) for x in planned_stamps(plan, first, start, floor))
            floor = resume_floor
            a, d, rp, floor = ingest_pages(store, plan, start, min(start + 1, last), floor, latencies=latencies,
                                           stop=r.guard, stamps=day_stamps)
            census = store_census(store)
            event['pending_after_first_write'] = census['pending']
            if census['pending']:
                raise AssertionError('q_pending not drained by recovery')
            if inspection['open_attempts']:
                attempt = inspection['open_attempts'][0]
                event['unknown_gap_expected'] = [attempt['attempt_start'], iso(resume_floor)]
                event['unknown_gap_recorded'] = covered(census['gaps'], attempt['attempt_start'], iso(resume_floor), 'unknown')
                if not event['unknown_gap_recorded']:
                    raise AssertionError('killed in-flight attempt left no unknown gap')
                gaps_expected.append((attempt['attempt_start'], iso(resume_floor), 'unknown'))
            else:
                event['unknown_gap_expected'] = None
                event['note'] = 'no attempt in flight at kill: cursor continuity, no store-recorded gap'
            # Pages before the resume point were committed by the killed child
            # (store cursor authority); the digest oracle below checks them.
            record['accepted'] += a + (start - first) * PAGE_ROWS
            record['duplicates'] += d
            record['kills'].append(event)
            start = min(start + 1, last)
        if start < last and not measured and args.prefill_mode == 'bulk':
            a, floor, next_id = bulk_pages_range(store, plan, start, last, floor, next_id, stamps=day_stamps, stop=r.guard)
            record['accepted'] += a
            record['prefill_mode'] = 'bulk'
        elif start < last:
            a, d, rp, floor = ingest_pages(store, plan, start, last, floor, latencies=latencies, stop=r.guard,
                                           stamps=day_stamps)
            record['accepted'] += a
            record['duplicates'] += d
            record['replayed_pages'] += rp
        record['ingest_seconds'] = time.monotonic() - t_ingest
        window = r.sampler.close_window()
        record['ingest_wal_max_bytes'] = window['wal']
        record['ingest_sampled_peak_bytes'] = window['peak']
        record['accept_latency_seconds'] = percentiles(latencies)
        # Oracle at the vessel, before the boundary.
        if expected is None:
            expected = plan.digest(first, last)
        census = store_census(store)
        if census['pending']:
            raise AssertionError('pending rows at end of day')
        got, distinct = vessel_digest(root / (owner + '.sqlite'))
        cursors = stored_cursors(store, plan)
        cursor_ok = all(committed_through(cursors[s]) == max(g for g in range(first, last) if g % len(plan.sources) == s)
                        for s in range(len(plan.sources)) if any(g % len(plan.sources) == s for g in range(first, last)))
        record['oracle'] = {'expected': expected.as_dict(), 'vessel': got.as_dict(), 'vessel_distinct': distinct,
                            'lost': max(0, expected.count - distinct), 'duplicated': got.count - distinct,
                            'match': got == expected, 'cursors_at_plan_end': cursor_ok}
        if not (got == expected and distinct == got.count and cursor_ok and record['accepted'] == expected.count):
            raise AssertionError('day %d oracle mismatch: %r' % (day, record['oracle']))
        if len(day_stamps) != last - first:
            raise AssertionError('stamp oracle incomplete')
        expected_by_day[owner] = day_stamps
        # Day boundary.
        r.state = 'ADVANCE'
        now = plan.day_start(day + 1)
        horizon = (now - dt.timedelta(days=30)).date().isoformat()
        expiring = [o for o in expected_by_day if o < horizon and (root / 'archive' / (o + '.parquet')).exists()]
        expiring_bytes = sum(p.stat().st_blocks * 512 for o in expiring for p in (root / 'archive').glob(o + '.*'))
        vessel_bytes = (root / (owner + '.sqlite')).stat().st_blocks * 512
        before_alloc = allocated(root)
        r.sampler.open_window()
        t_adv = time.monotonic()
        mkill = next((p for p in points if measured and p['kind'] == 'maintain' and p['day'] == k), None)
        if mkill:
            proc = r.spawn(_advance_child, {'store': str(root), 'now': iso(now), 'phase': mkill['phase'],
                                                'storage_root': str(storage.ROOT)})
            if mkill['mode'] == 'parent_sigkill':
                proc.join(mkill['seconds'])
                if proc.is_alive():
                    os.kill(proc.pid, signal.SIGKILL)
            code = r.reap(proc)
            killed = code in (-signal.SIGKILL, 73)
            if not killed:
                raise RuntimeError('maintenance kill point not exercised (exit %r)' % code)
            inspection = store_census(store)
            store = RecentStore(root)
            refused = None
            try:
                store.frontier()
            except RuntimeError as exc:
                refused = str(exc)
            retry = now + dt.timedelta(seconds=args.recovery_seconds)
            maint = store.advance_day(iso(retry))
            census = store_census(store)
            event = {'spec': mkill['spec'], 'mode': mkill['mode'], 'phase': mkill['phase'], 'exit': code,
                     'recent_transition_at_reopen': inspection['recent_transition'],
                     'rollover_transition_at_reopen': inspection['rollover_transition'],
                     'segments_at_reopen': inspection['segments'], 'reads_refused_before_recovery': refused,
                     'recovered_with': iso(retry), 'recent_transition_after': census['recent_transition']}
            if inspection['recent_transition'] and not refused:
                raise AssertionError('pending maintenance did not refuse reads')
            if census['recent_transition'] or census['rollover_transition']:
                raise AssertionError('maintenance recovery incomplete')
            record['kills'].append(event)
            boundary = retry
        else:
            maint = store.advance_day(iso(now))
            boundary = now
        seconds = time.monotonic() - t_adv
        window = r.sampler.close_window()
        after_alloc = allocated(root)
        census = store_census(store)
        # Physical retirement and authority checks.
        leftovers = [str(p.name) for o in expiring for p in (root / 'archive').glob(o + '.*')]
        if leftovers:
            raise AssertionError('expired owner files remain: %r' % leftovers)
        if any(Path(str(root / (owner + '.sqlite')) + x).exists() for x in ('', '-wal', '-shm')):
            raise AssertionError('archived day vessel or sidecar not retired')
        segments = {o for o, _ in census['segments']}
        if any(o < horizon for o in segments):
            raise AssertionError('expired owner authority remains')
        archived, adistinct = archive_digest(root / 'archive' / (owner + '.parquet'))
        if archived != expected or adistinct != expected.count:
            raise AssertionError('archive digest mismatch for ' + owner)
        cutoff = micros(boundary - dt.timedelta(days=30))
        retained = PAGE_ROWS * sum(1 for o, xs in expected_by_day.items() if o >= horizon for x in xs if x >= cutoff)
        if census['seen'] != retained:
            raise AssertionError('retained observation rows %d != oracle %d' % (census['seen'], retained))
        unlinked = open_unlinked(root)
        if unlinked:
            raise AssertionError('open descriptors on deleted files: %r' % unlinked)
        maintenance_end = boundary + dt.timedelta(seconds=max(1.0, seconds))
        if args.record_maintenance_gaps:
            store.record_gap(iso(plan.day_start(day + 1)), iso(maintenance_end), 'maintenance')
            gaps_expected.append((iso(plan.day_start(day + 1)), iso(maintenance_end), 'maintenance'))
        floor = maintenance_end
        record.update({
            'maintenance_seconds': seconds, 'maintenance_sampled_peak_bytes': window['peak'],
            'maintenance_wal_max_bytes': window['wal'], 'store_allocated_before_advance': before_alloc,
            'store_allocated_after_advance': after_alloc, 'expired_owners': expiring,
            'expired_owner_bytes_released': expiring_bytes, 'retired_vessel_bytes': vessel_bytes,
            'archive_oracle_match': True, 'retained_events': census['seen'],
            'retained_owners': len(census['segments']), 'du_store': du(root),
            'open_unlinked_descriptors': 0, 'maintenance_result_expired': (maint.get('maintenance') or {}).get('expired_owners'),
            'window_full': day >= 30})
        r.days.append(record)
        with (out / 'DAILY.jsonl').open('a') as f:
            f.write(json.dumps(record, sort_keys=True) + '\n')
        r.progress(state=r.state, day=day, accepted=record['accepted'], maintenance_seconds=seconds)
        why = r.guard()
        if why:
            raise RuntimeError(why)
    # Gap disclosure via the public provider for one target over each expected gap.
    from recent_provider import RecentProvider
    provider = RecentProvider(store)
    frontier = store.frontier()
    gap_checks = []
    for lo, hi, reason in gaps_expected:
        if clock(hi) <= clock(frontier['start']):
            continue
        lo2 = max(clock(lo), clock(frontier['start']))
        hi2 = min(clock(hi), clock(frontier['end']))
        if lo2 >= hi2:
            continue  # the final hold extends beyond the last accepted observation
        with provider.snapshot('did:plc:gap-probe', iso(lo2), iso(hi2), 1) as answer:
            gaps = [(g['start'], g['end'], g['reason']) for g in answer['coverage']['gaps']]
        gap_checks.append({'start': iso(lo2), 'end': iso(hi2), 'reason': reason,
                           'disclosed': covered(gaps, iso(lo2), iso(hi2), reason)})
    if not all(x['disclosed'] for x in gap_checks):
        raise AssertionError('expected gap not disclosed by provider')
    measured_days = [d for d in r.days if d['measured_day']]
    tail = measured_days[-8:]
    steady = plateau([d['store_allocated_after_advance'] for d in tail], args.plateau_tolerance)
    steady['window_full_all'] = all(d['window_full'] for d in tail)
    steady['days'] = [d['day'] for d in tail]
    steady['criterion'] = ('days N-7..N (last 8 measured) all at a full 30-day window and (max-min)/median of '
                           'post-advance store allocation <= tolerance')
    steady['pass'] = steady['pass'] and steady['window_full_all']
    if args.require_plateau and not steady['pass']:
        raise AssertionError('no steady-state plateau: %r' % steady)
    total = lambda key: sum(d[key] for d in r.days)
    result = {
        'result': 'PASS', 'control': 'C3', 'source_revision': args.source_revision,
        'specimen_sha256': receipt['parquet_sha256'],
        'args': {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()},
        'days_total': total_days, 'prefill_days': args.prefill_days, 'measured_days': args.days,
        'events_per_day': args.events_per_day, 'sources': args.sources,
        'accepted': total('accepted'), 'store_reported_duplicates': total('duplicates'),
        'lost': sum(d['oracle']['lost'] for d in r.days), 'duplicated': sum(d['oracle']['duplicated'] for d in r.days),
        'kills': [k for d in r.days for k in d['kills']], 'gap_checks': gap_checks,
        'steady_state': steady,
        'measured': {
            'maintenance_seconds': percentiles([d['maintenance_seconds'] for d in measured_days]),
            'maintenance_sampled_peak_bytes_max': max(d['maintenance_sampled_peak_bytes'] for d in measured_days),
            'wal_max_bytes': max(max(d['maintenance_wal_max_bytes'], d['ingest_wal_max_bytes']) for d in measured_days),
            'ingest_events_per_second': sum(d['accepted'] for d in measured_days) / sum(d['ingest_seconds'] for d in measured_days),
            'expired_owner_bytes_released': sum(d['expired_owner_bytes_released'] for d in measured_days),
            'retired_vessel_bytes': sum(d['retired_vessel_bytes'] for d in measured_days),
        },
        'sampled_peak_bytes': r.sampler.peak, 'before': before,
        'after': {'free_bytes': free_bytes(), 'occurrence_allocated_bytes': allocated(out)},
        'elapsed_seconds': time.monotonic() - r.began,
        'rss_peak_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        'child_rss_peak_bytes': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024,
        'scope': ('Synthetic sources (src=labeler=did:web:c3-source-N.invalid) carrying specimen uri/val/cid/sig; '
                  'strict canonical identities; authored time nominal-5s (1% nominal-47h), inside the 48h allowance; '
                  'simulated observation clock 1:1 with measured maintenance seconds; harness acts as collector and '
                  'records the maintenance-hold gap like RecentCollector._maintenance_gap. Sampled peaks only.'),
        'production_mutations': [],
    }
    r.sampler.stop.set()
    r.sampler.join()
    atomic(out / 'RESULT.json', result)
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--occurrence', type=Path, required=True)
    p.add_argument('--specimen', type=Path, required=True)
    p.add_argument('--days', type=int, required=True, help='measured replenishment days')
    p.add_argument('--events-per-day', type=int, required=True)
    p.add_argument('--prefill-days', type=int, default=0,
                   help='days populated through the same accept path before the measured days (window fill)')
    p.add_argument('--prefill-mode', choices=('accept', 'bulk'), default='bulk',
                   help='bulk = constructed_window.bulk_pages (offline construction) for prefill days only')
    p.add_argument('--sources', type=int, default=16)
    p.add_argument('--kill-points', nargs='*', default=[])
    p.add_argument('--ceiling-bytes', type=int, required=True)
    p.add_argument('--budget-seconds', type=float, required=True)
    p.add_argument('--recovery-seconds', type=float, default=120.0, help='simulated restart delay after a kill')
    p.add_argument('--kill-jitter-seconds', type=float, default=.5)
    p.add_argument('--seed', type=int, default=20261008)
    p.add_argument('--plateau-tolerance', type=float, default=.15)
    p.add_argument('--require-plateau', type=int, choices=(0, 1), default=1)
    p.add_argument('--record-maintenance-gaps', type=int, choices=(0, 1), default=1)
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
    atomic(args.occurrence / 'TERMINAL.json', {'result': 'PASS', 'accepted': result['accepted']})
    print(json.dumps({k: result[k] for k in ('result', 'accepted', 'lost', 'duplicated')}), flush=True)


if __name__ == '__main__':
    main()
