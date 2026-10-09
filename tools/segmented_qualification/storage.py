"""Isolated qualification adapter. No production path is accepted.

Global SQLite owns accepted events until a durable flush, state and cursors.
Event vessels own immutable payload after flush. ACTIVE.json is a projection.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).parents[2]
sys.path.insert(0, str(REPO / 'src'))
sys.path.insert(0, str(REPO))
from labelwatch import db, ingest, retention, custody
from tools.label_events_cold_archive import ARROW_SCHEMA

FIELDS = ('id', 'labeler_did', 'src', 'uri', 'cid', 'val', 'neg', 'exp', 'sig', 'ts', 'event_hash', 'target_did')
COLS = ','.join(FIELDS)
ROOT = Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-segmented-storage-qualification-20261005/runtime')
RESERVE = 64424509440


ADMITTED_ENV = 'LABELWATCH_RECENT_ADMITTED_ROOT'
RESERVES_ENV = 'LABELWATCH_RECENT_RESERVES'
RESERVES_FILE_ENV = 'LABELWATCH_RECENT_RESERVES_FILE'
INODES_ENV = 'LABELWATCH_RECENT_MIN_FREE_INODES'
OVERRIDE_ENV = 'LABELWATCH_RECENT_RESERVES_OVERRIDE_ACK'
RESERVES_KEY = 'q:recent_reserves'
RESERVES_SCHEMA = 'labelwatch.recent-reserves.v1'
MIN_FREE_INODES = 1000000


def campaign_roots():
    base = ROOT.parent.parent
    return (ROOT, base / 'labelwatch-segmented-horizon-capacity-20261005' / 'runtime',
            base / 'labelwatch-global-lifetime-custody-20261005' / 'runtime')


def _unlinked(path):
    """Absolute path with no symlinked component (realpath is a no-op)."""
    path = str(path)
    if not os.path.isabs(path): raise ValueError('admitted root must be absolute')
    if os.path.realpath(path) != os.path.normpath(path): raise ValueError('admitted path has a symlinked component')
    return Path(os.path.normpath(path))


SYSTEM_PREFIXES = tuple(Path(p) for p in (
    '/etc', '/usr', '/bin', '/sbin', '/boot', '/proc', '/sys', '/dev', '/run', '/root', '/srv', '/var/www',
    '/var/lib/labelwatch/generations', '/var/lib/labelwatch/cold'))
ADMITTED_LOOSE = {'units.lock'}


def _system(root):
    parts = root.parts
    if len(parts) > 1 and parts[1].startswith('lib'): return True  # /lib, /lib32, /lib64, /libx32
    if len(parts) > 3 and parts[1] == 'home' and parts[3] == '.config': return True
    return any(root == p or p in root.parents for p in SYSTEM_PREFIXES)


def admitted_root():
    """One operator-configured root, disjoint from every campaign root.

    It holds only epoch stores (directories with ACTIVE.json or state.sqlite),
    empty directories (an interrupted or pending init) and the unit lock.
    """
    value = os.environ.get(ADMITTED_ENV)
    if not value: return None
    root = _unlinked(value)
    if root == Path('/') or not root.is_dir(): raise ValueError('admitted root must be an existing non-/ directory')
    if _system(root): raise ValueError('admitted root is under a system prefix')
    for campaign in campaign_roots():
        campaign = campaign.resolve()
        if root == campaign or root in campaign.parents or campaign in root.parents:
            raise ValueError('admitted root overlaps a campaign root')
    for entry in root.iterdir():
        if entry.name in ADMITTED_LOOSE and entry.is_file() and not entry.is_symlink(): continue
        if entry.is_dir() and not entry.is_symlink() and (
                (entry / 'ACTIVE.json').exists() or (entry / 'state.sqlite').exists() or not any(entry.iterdir())):
            continue
        raise ValueError('admitted root holds a foreign entry: ' + entry.name)
    return root


def in_campaign(root):
    root = Path(root).resolve()
    return any(c in root.parents for c in campaign_roots())


def owned(root):
    if in_campaign(root): return Path(root).resolve()
    admitted = admitted_root()
    if admitted is not None:
        root = _unlinked(os.path.abspath(root))
        if admitted in root.parents: return root
    raise ValueError('only new campaign-owned child fixtures are admitted')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def reserve_policy(root):
    """Running per-filesystem reserve policy; never assumes /data exists.

    Unconfigured is a refusal except for campaign fixtures, which keep the
    qualified crow pair (/ and /data at 60 GiB each).
    """
    raw = os.environ.get(RESERVES_ENV)
    if not raw and os.environ.get(RESERVES_FILE_ENV):
        raw = Path(os.environ[RESERVES_FILE_ENV]).read_text()
    if raw:
        try: entries = json.loads(raw)
        except ValueError as exc: raise RuntimeError('reserve_policy_malformed') from exc
    elif in_campaign(root):
        entries = [{'mount': '/', 'bytes': RESERVE}, {'mount': '/data', 'bytes': RESERVE}]
    else:
        raise RuntimeError('reserve_policy_unconfigured')
    if not isinstance(entries, list) or not entries: raise RuntimeError('reserve_policy_malformed')
    seen = {}
    for e in entries:
        if not isinstance(e, dict) or set(e) != {'mount', 'bytes'} or not isinstance(e['mount'], str) \
                or type(e['bytes']) is not int or e['bytes'] <= 0 or not os.path.isabs(e['mount']) \
                or os.path.normpath(e['mount']) != e['mount'] or e['mount'] in seen:
            raise RuntimeError('reserve_policy_malformed')
        if not os.path.isdir(e['mount']): raise RuntimeError('reserve_policy_mount_missing: ' + e['mount'])
        seen[e['mount']] = e['bytes']
    try: inodes = int(os.environ.get(INODES_ENV) or MIN_FREE_INODES)
    except ValueError as exc: raise RuntimeError('reserve_policy_malformed') from exc
    if inodes < 0: raise RuntimeError('reserve_policy_malformed')
    return {'schema': RESERVES_SCHEMA, 'min_free_inodes': inodes,
            'reserves': [{'mount': m, 'bytes': seen[m]} for m in sorted(seen)]}


def override_ack(recorded_canonical, running):
    """Binds one reviewed weakening: any later change to the running policy voids it."""
    return hashlib.sha256((recorded_canonical + '\n' + canonical(running)).encode()).hexdigest()


def weaker(running, recorded):
    have = {e['mount']: e['bytes'] for e in running['reserves']}
    return running['min_free_inodes'] < recorded['min_free_inodes'] or any(
        have.get(e['mount'], -1) < e['bytes'] for e in recorded['reserves'])


def admit_reserves(policy, need=0, paths=(), message='shared_reserve_unavailable'):
    """Every reserve holds after `need` bytes land on the filesystems of `paths`."""
    devices = {os.stat(p).st_dev for p in paths}
    covered = set()
    for e in policy['reserves']:
        device = os.stat(e['mount']).st_dev; covered.add(device)
        s = os.statvfs(e['mount'])
        if s.f_bavail * s.f_frsize - (need if device in devices else 0) < e['bytes'] \
                or s.f_favail < policy['min_free_inodes']:
            raise RuntimeError(message)
    if devices - covered: raise RuntimeError(message + ': filesystem without configured reserve')


def atomic(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.incomplete')
    with tmp.open('w') as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path); syncdir(path.parent)


def syncdir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(8 * 1024 * 1024), b''): h.update(b)
    return h.hexdigest()


def digest(rows):
    h = hashlib.sha256(); n = 0
    for r in rows:
        h.update((json.dumps(list(r), ensure_ascii=False, separators=(',', ':')) + '\n').encode()); n += 1
    return {'rows': n, 'ordered_row_sha256': h.hexdigest()}


def cut(requested, phase):
    if requested == phase: os._exit(73)


# Writer preference for WAL checkpoints. While this file is fresh, NEW shared
# reader.lock holders wait (at most CHECKPOINT_READER_WAIT seconds, never past
# their own timeout) so in-flight reads drain and the writer can checkpoint.
# Readers never block the writer; the writer never waits on readers beyond its
# own bound (see RecentStore.check_local). A stale file (crashed writer) is ignored.
CHECKPOINT_FLAG = 'checkpoint.pending'
CHECKPOINT_READER_WAIT = 2.0
CHECKPOINT_FLAG_STALE = 10.0


def _checkpoint_pending(root):
    try:
        return time.time() - (root / CHECKPOINT_FLAG).stat().st_mtime < CHECKPOINT_FLAG_STALE
    except FileNotFoundError:
        return False


@contextlib.contextmanager
def lock(root, name='writer.lock', shared=False, timeout=None):
    started = time.monotonic()
    if name == 'reader.lock' and shared and _checkpoint_pending(root):
        until = started + min(CHECKPOINT_READER_WAIT, CHECKPOINT_READER_WAIT if timeout is None else timeout)
        while _checkpoint_pending(root) and time.monotonic() < until:
            time.sleep(.01)
    with (root / name).open('a') as f:
        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        if timeout is None:
            fcntl.flock(f, mode)
        else:
            deadline = started + timeout     # the checkpoint wait counts against it
            while True:
                try:
                    fcntl.flock(f, mode | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('store lease deadline exceeded')
                    time.sleep(min(.01, max(0, deadline-time.monotonic())))
        yield


def connect(path, readonly=False):
    c = sqlite3.connect(f'file:{path}?mode=ro' if readonly else str(path), uri=readonly, timeout=2)
    c.row_factory = sqlite3.Row
    if not readonly:
        # Required by opt-in bounded-state counters when existing writers use
        # INSERT OR REPLACE: implicit replacement deletion must fire triggers.
        c.execute('PRAGMA recursive_triggers=ON')
        c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=FULL')
        c.execute('PRAGMA wal_autocheckpoint=1000')
        # A fully checkpointed WAL is reset and truncated to this size instead of
        # being kept at its high-water mark.
        c.execute('PRAGMA journal_size_limit=67108864')
        # max_page_count is connection-local. Read the persisted owned limit
        # and apply it to every writer, including recovery and metadata writers.
        cap = 2097152 if Path(path).name == 'state.sqlite' else 4194304
        if Path(path).name == 'state.sqlite' and c.execute("SELECT 1 FROM sqlite_master WHERE name='meta'").fetchone():
            row = c.execute("SELECT value FROM meta WHERE key='q:state_page_cap'").fetchone()
            if row: cap = int(row[0])
        elif c.execute("SELECT 1 FROM sqlite_master WHERE name='segment_meta'").fetchone():
            row = c.execute("SELECT value FROM segment_meta WHERE key='page_cap'").fetchone()
            if row: cap = int(row[0])
        if cap <= 0 or cap > 4294967294:
            c.close(); raise RuntimeError('invalid owned page ceiling')
        if c.execute('PRAGMA page_size').fetchone()[0] != 4096:
            c.close(); raise RuntimeError('unqualified SQLite page size')
        applied = c.execute('PRAGMA max_page_count=' + str(cap)).fetchone()[0]
        if applied > cap:
            c.close(); raise RuntimeError('existing file exceeds admitted page ceiling')

    c.execute('PRAGMA cache_size=-16384'); c.execute('PRAGMA temp_store=FILE')
    return c


def vessel(path, identity):
    c = connect(path)
    # Same actual event DDL/indexes, not an approximation of their constraints.
    import ast
    tree = ast.parse((REPO / 'src/labelwatch/db.py').read_text())
    v = {n.targets[0].id: ast.literal_eval(n.value) for n in tree.body
         if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
         and n.targets[0].id in ('SCHEMA_TABLES', 'SCHEMA_INDEXES')}
    table = next(x for x in v['SCHEMA_TABLES'].split(';') if 'CREATE TABLE IF NOT EXISTS label_events (' in x)
    indexes = ';'.join(x for x in v['SCHEMA_INDEXES'].split(';') if ' ON label_events(' in x)
    c.executescript(table + ';' + indexes + ';CREATE TABLE segment_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);')
    c.executemany('INSERT INTO segment_meta VALUES (?,?)', [('identity', identity), ('schema_generation', '23'), ('sealed', '0'), ('page_cap', '4194304')])
    c.execute('PRAGMA max_page_count=4194304')  # Finite event-file ceiling 16GiB.
    c.commit(); c.execute('PRAGMA wal_checkpoint(TRUNCATE)'); c.close(); syncdir(path.parent)


class Store:
    def __init__(self, root):
        self.root = owned(root)
        self.state = self.root / 'state.sqlite'
        if self.state.exists(): self.reserve_policy()

    def reserve_policy(self):
        """Running policy, refused when weaker than the one recorded at init."""
        policy = reserve_policy(self.root)
        with contextlib.closing(sqlite3.connect(f'file:{self.state}?mode=ro', uri=True, timeout=2)) as c:
            row = c.execute('SELECT value FROM meta WHERE key=?', (RESERVES_KEY,)).fetchone()
        if row is None:
            # Pre-existing campaign fixtures predate the record; nothing else does.
            if in_campaign(self.root): return policy
            raise RuntimeError('reserve_policy_record_missing')
        recorded = json.loads(row[0])
        if weaker(policy, recorded) and os.environ.get(OVERRIDE_ENV) != override_ack(row[0], policy):
            raise RuntimeError('reserve_policy_weaker_than_recorded')
        return policy

    def admit_reserves(self, need=0, paths=(), message='shared_reserve_unavailable'):
        admit_reserves(self.reserve_policy(), need, paths, message)

    @classmethod
    def create(cls, root, period='2026-09-28', floor='2026-08-15T00:00:00Z'):
        root = owned(root); policy = canonical(reserve_policy(root)); root.mkdir()
        c = connect(root / 'state.sqlite'); db.init_db(c)
        c.executescript('''
        CREATE TABLE q_segments(identity TEXT PRIMARY KEY, status TEXT NOT NULL, schema_generation INTEGER NOT NULL);
        CREATE UNIQUE INDEX q_one_active ON q_segments(status) WHERE status='ACTIVE';
        CREATE TABLE q_pending(id INTEGER PRIMARY KEY, segment TEXT NOT NULL);
        CREATE TABLE q_transition(singleton INTEGER PRIMARY KEY CHECK(singleton=1), old TEXT NOT NULL, new TEXT NOT NULL);
        CREATE TABLE q_archive(identity TEXT PRIMARY KEY, receipt TEXT NOT NULL);
        ''')
        custody.initialize(c, str(root))
        db.set_meta(c, retention.RETENTION_FLOOR_KEY, floor)
        db.set_meta(c, RESERVES_KEY, policy)
        db.set_meta(c, 'q:max_pending', '10000'); db.set_meta(c, 'q:max_local_segments', '2')
        db.set_meta(c, 'q:active', period)
        c.execute('INSERT INTO q_segments VALUES (?, ?, 23)', (period, 'ACTIVE'))
        c.commit(); c.close(); vessel(root / (period + '.sqlite'), period)
        atomic(root / 'ACTIVE.json', {'identity': period, 'authority': 'state.sqlite:q_segments'})
        return cls(root)

    def check_local(self, injected=None):
        self.check_capacity(injected)
        self.check_wal()

    def check_capacity(self, injected=None):
        """Filesystem reserve and page ceiling: the only ingestion refusals that remain."""
        if injected == 'root_low': raise RuntimeError('local_capacity_below_required_bound')
        self.admit_reserves(paths=(self.root,), message='shared_reserve_unavailable')
        # Finite fixture limits. Adoption limits must be derived from the
        # measured envelope; none of these changes a production gate.
        if self.state.exists():
            c = connect(self.state)
            cap = int(db.get_meta(c, 'q:state_page_cap') or '2097152')
            c.execute('PRAGMA max_page_count=' + str(cap)); c.close()

    def check_wal(self):
        """Legacy file-size WAL budget (refuses); RecentStore replaces it."""
        c = connect(self.state, readonly=True)
        try: wal_budget = int(db.get_meta(c, 'q:wal_budget_bytes') or '268435456')
        finally: c.close()
        if wal_budget <= 0: raise RuntimeError('invalid owned WAL budget')
        for path in self.root.glob('*.sqlite-wal'):
            # Lease-free readers (reader catalogs) open and close WAL files
            # concurrently; one that vanished after the listing has no budget.
            try: size = path.stat().st_size
            except FileNotFoundError: continue
            if size > wal_budget:
                c = connect(Path(str(path)[:-4])); r = c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone(); c.close()
                if r[0] != 0: raise RuntimeError('wal_budget_pinned; ingestion refused before acceptance')

    def flush(self, c, death=None):
        pending = c.execute('SELECT segment FROM q_pending GROUP BY segment').fetchall()
        for (identity,) in pending:
            rows = c.execute('SELECT ' + ','.join('e.' + k for k in FIELDS) + ' FROM label_events e JOIN q_pending p ON p.id=e.id WHERE p.segment=? ORDER BY e.id', (identity,)).fetchall()
            target = connect(self.root / (identity + '.sqlite'))
            if target.execute("SELECT value FROM segment_meta WHERE key='identity'").fetchone()[0] != identity:
                target.close(); raise RuntimeError('pending target identity mismatch')
            if target.execute("SELECT value FROM segment_meta WHERE key='sealed'").fetchone()[0] != '0':
                target.close(); raise RuntimeError('pending writes reference sealed segment')
            target.executemany('INSERT OR IGNORE INTO label_events VALUES (' + ','.join('?' for _ in FIELDS) + ')', rows)
            # Exact id/hash content, not merely INSERT OR IGNORE success.
            for row in rows:
                found = target.execute('SELECT ' + COLS + ' FROM label_events WHERE id=?', (row[0],)).fetchone()
                if tuple(found or ()) != tuple(row): raise RuntimeError('flush identity/content mismatch')
            target.commit(); target.close(); cut(death, 'after_vessel_commit')
            c.executemany('DELETE FROM label_events WHERE id=?', [(r[0],) for r in rows])
            c.executemany('DELETE FROM q_pending WHERE id=?', [(r[0],) for r in rows])
            c.commit(); cut(death, 'after_journal_drain')

    def recover(self, c, death=None):
        transition = c.execute('SELECT old,new FROM q_transition').fetchone()
        if transition:
            old, new = transition
            self.flush(c)
            path = self.root / (old + '.sqlite'); source = connect(path)
            if source.execute("SELECT value FROM segment_meta WHERE key='identity'").fetchone()[0] != old:
                source.close(); raise RuntimeError('rollover source identity mismatch')
            source.execute("UPDATE segment_meta SET value='1' WHERE key='sealed'"); source.commit()
            result = source.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
            if result[0] != 0: source.close(); raise RuntimeError('seal_checkpoint_pinned')
            source.close(); cut(death, 'after_seal')
            successor = self.root / (new + '.sqlite')
            if not successor.exists(): vessel(successor, new)
            target = connect(successor, readonly=True)
            if target.execute('PRAGMA quick_check').fetchone()[0] != 'ok': raise RuntimeError('successor_invalid')
            if target.execute("SELECT value FROM segment_meta WHERE key='identity'").fetchone()[0] != new: raise RuntimeError('successor_identity_mismatch')
            target.close(); cut(death, 'after_successor')
            c.execute("UPDATE q_segments SET status='SEALED' WHERE identity=?", (old,))
            c.execute('INSERT INTO q_segments VALUES (?, ?, 23)', (new, 'ACTIVE'))
            db.set_meta(c, 'q:active', new); c.execute('DELETE FROM q_transition'); c.commit()
            cut(death, 'after_active_before_projection')
        self.flush(c, death)
        for row in c.execute("SELECT identity FROM q_segments WHERE status='ARCHIVED'").fetchall():
            identity = row[0]
            if not (self.root / (identity + '.sqlite')).exists():
                receipt_row = c.execute('SELECT receipt FROM q_archive WHERE identity=?', (identity,)).fetchone()
                if receipt_row is None: raise RuntimeError('missing source without custody receipt')
                receipt = json.loads(Path(receipt_row[0]).read_text())
                archive = Path(receipt['archive_root']) / (identity + '.parquet')
                if sha(archive) != receipt['parquet_sha256']: raise RuntimeError('missing source and invalid archive custody')
                c.execute("UPDATE q_segments SET status='RETIRED' WHERE identity=?", (identity,)); c.commit()
        active = c.execute("SELECT identity FROM q_segments WHERE status='ACTIVE'").fetchall()
        if len(active) != 1: raise RuntimeError('active authority is ambiguous')
        atomic(self.root / 'ACTIVE.json', {'identity': active[0][0], 'authority': 'state.sqlite:q_segments'})

    def ingest(self, rows, source, cursor, death=None, injected=None):
        if len(rows) > 10000: raise RuntimeError('bounded ingest page exceeded')
        self.check_local(injected)
        with lock(self.root):
            c = connect(self.state)
            try:
                self.recover(c); c.execute('BEGIN IMMEDIATE')
                identity = db.get_meta(c, 'q:active')
                accepted, duplicate_ids = custody.stage(c, rows, identity, lambda phase: cut(death, phase))
                for event_id, row in accepted:
                    c.execute('INSERT INTO q_pending VALUES (?,?)', (event_id, identity))
                evidence_seen = set()
                for event_id, row in accepted:
                    db.upsert_labeler(c, row[0], row[8])
                    ingest._track_observed_src(c, row[1] or row[0], row[8], evidence_seen)
                n = len(accepted)
                cut(death, 'before_global_commit')
                db.set_cursor(c, source, cursor)
                cut(death, 'after_global_commit')
                self.flush(c, death)
                return {'inserted': n, 'cursor': db.get_cursor(c, source), 'accepted_ids': [r[0] for r in accepted], 'duplicate_ids': duplicate_ids}
            finally: c.close()

    def rotate(self, period, death=None):
        dt.date.fromisoformat(period); self.check_local()
        with lock(self.root):
            c = connect(self.state)
            try:
                self.recover(c)
                old = db.get_meta(c, 'q:active')
                if period == old: return  # duplicate retry
                if period < old: raise RuntimeError('backward clock refused')
                count = c.execute("SELECT COUNT(*) FROM q_segments WHERE status!='RETIRED'").fetchone()[0]
                if count >= int(db.get_meta(c, 'q:max_local_segments')): raise RuntimeError('local_segment_queue_full')
                cut(death, 'before_rollover')
                c.execute('INSERT INTO q_transition VALUES (1,?,?)', (old, period)); c.commit()
                cut(death, 'after_transition')
                self.recover(c, death)
            finally: c.close()

    def snapshot(self):
        with lock(self.root):
            c = connect(self.state)
            try:
                self.recover(c)
                return {'cursor': {r[0]: r[1] for r in c.execute("SELECT key,value FROM meta WHERE key LIKE 'ingest_cursor:%'")},
                        'active': db.get_meta(c, 'q:active'), 'segments': [tuple(r) for r in c.execute('SELECT * FROM q_segments ORDER BY identity')],
                        'pending': c.execute('SELECT COUNT(*) FROM q_pending').fetchone()[0], 'keys': c.execute('SELECT COUNT(*) FROM q_hot_keys').fetchone()[0],
                        'state_tables': {r[0]: digest(c.execute('SELECT * FROM "' + r[0] + '" ORDER BY rowid')) for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'q_%' AND name NOT IN ('meta','label_events','sqlite_sequence')").fetchall()}}
            finally: c.close()

    def archive(self, identity, destination, death=None, injected=None, lock_name=None):
        dt.date.fromisoformat(identity)
        # Serialize retries; retain a source reader lease until checkpoint.
        # Ingestion keeps its independent writer fence and continues.
        with lock(self.root, lock_name or ('archive-' + identity + '.lock')), lock(self.root, 'reader.lock', shared=True):
            return self._archive(identity, destination, death, injected)

    def _archive(self, identity, destination, death=None, injected=None):
        dt.date.fromisoformat(identity)
        self.check_local(injected)
        destination = Path(destination)
        # Only this campaign's verified NFS root or its exact local negative fixtures.
        archive_root = Path(json.loads((ROOT.parent / 'evidence/ARCHIVE-DESTINATION.json').read_text())['campaign_archive'])
        horizon = ROOT.parent.parent / 'labelwatch-segmented-horizon-capacity-20261005' / 'runtime'
        repair = ROOT.parent.parent / 'labelwatch-global-lifetime-custody-20261005' / 'runtime'
        if destination.resolve() != archive_root.resolve() and archive_root.resolve() not in destination.resolve().parents and ROOT not in destination.resolve().parents and horizon not in destination.resolve().parents and repair not in destination.resolve().parents:
            raise ValueError('unowned archive path')
        if injected in ('archive_unavailable', 'archive_read_only', 'archive_full'):
            raise OSError(injected)
        destination.mkdir(exist_ok=True)
        source = self.root / (identity + '.sqlite')
        c = connect(self.state); record = c.execute('SELECT status FROM q_segments WHERE identity=?', (identity,)).fetchone(); c.close()
        if record is None or record[0] == 'ACTIVE': raise RuntimeError('active source cannot archive')
        receipt = destination / (identity + '.receipt.json')
        output = destination / (identity + '.parquet')
        if not source.exists():
            if record[0] != 'RETIRED' or not receipt.exists() or sha(output) != json.loads(receipt.read_text())['parquet_sha256']:
                raise RuntimeError('retired custody cannot be established')
            from tier import VerifiedCatalog
            authority=connect(self.state)
            try:VerifiedCatalog(destination,store=self).admit(authority,identity)
            finally:authority.close()
            return json.loads(receipt.read_text())
        # Preflight capacity and actual bounded I/O before expensive conversion.
        need = source.stat().st_size * 2 + 1048576
        fs = os.statvfs(destination)
        if fs.f_bavail * fs.f_frsize < need: raise RuntimeError('archive_capacity_unavailable')
        probe = destination / (identity + '.probe')
        with probe.open('wb') as f: f.write(b'probe'); f.flush(); os.fsync(f.fileno())
        probe.rename(probe.with_suffix('.probe-renamed')); probe.with_suffix('.probe-renamed').unlink(); syncdir(destination)
        sql = connect(source, readonly=True)
        if sql.execute("SELECT value FROM segment_meta WHERE key='sealed'").fetchone()[0] != '1': raise RuntimeError('source not sealed')
        if sql.execute("SELECT value FROM segment_meta WHERE key='identity'").fetchone()[0] != identity:
            sql.close(); raise RuntimeError('archive source identity mismatch')
        generation = sql.execute("SELECT value FROM segment_meta WHERE key='schema_generation'").fetchone()[0]
        physical_columns = tuple(r[1] for r in sql.execute('PRAGMA table_info(label_events)'))
        if generation != '23' or physical_columns != FIELDS:
            sql.close(); raise RuntimeError('unqualified writer schema refused before conversion; use separately qualified generation adapter')
        source_hash = sha(source); started = time.perf_counter(); temp = output.with_suffix('.parquet.incomplete')
        if not output.exists():
            schema = ARROW_SCHEMA.with_metadata({b'labelwatch.schema_generation': b'23', b'labelwatch.segment_identity': identity.encode()})
            with pq.ParquetWriter(temp, schema, compression='zstd') as writer:
                cursor = sql.execute('SELECT ' + COLS + ' FROM label_events ORDER BY id')
                while rows := cursor.fetchmany(10000):
                    writer.write_table(pa.Table.from_pylist([dict(zip(FIELDS, r)) for r in rows], schema=schema))
                    cut(death, 'conversion_killed')
            with temp.open('rb') as f: os.fsync(f.fileno())
            os.replace(temp, output); syncdir(destination)
        cut(death, 'after_parquet_before_checkpoint')
        conversion_seconds = time.perf_counter() - started; verify_started = time.perf_counter()
        expected = digest(sql.execute('SELECT ' + COLS + ' FROM label_events ORDER BY id'))
        identity_index = custody.build_index(sql, destination / (identity + '.identities.sqlite'), identity)
        sql.close()
        def parquet_rows():
            pf = pq.ParquetFile(output)
            if pf.schema_arrow.metadata.get(b'labelwatch.schema_generation') != b'23' or not pf.schema_arrow.remove_metadata().equals(ARROW_SCHEMA.remove_metadata()):
                raise RuntimeError('archive schema identity/type mismatch')
            if pf.schema_arrow.metadata.get(b'labelwatch.segment_identity') != identity.encode(): raise RuntimeError('archive identity mismatch')
            for batch in pf.iter_batches(batch_size=10000):
                cut(death, 'verification_killed')
                for r in batch.to_pylist(): yield tuple(r[k] for k in FIELDS)
        actual = digest(parquet_rows())
        if actual != expected: raise RuntimeError('archive content mismatch')
        result = {'identity': identity, 'schema_generation': 23, 'source_sha256': source_hash, 'parquet_sha256': sha(output),
                  'content': actual, 'conversion_seconds': conversion_seconds, 'verification_seconds': time.perf_counter() - verify_started,
                  'source_bytes': source.stat().st_size, 'source_allocated_bytes': source.stat().st_blocks * 512, 'parquet_bytes': output.stat().st_size,
                  'archive_admitted_bytes': need, 'archive_root': str(destination), 'identity_index': identity_index}
        c = connect(self.state)
        try:
            previous = c.execute('SELECT receipt_json FROM custody_archives WHERE identity=?', (identity,)).fetchone()
            if previous:
                committed = json.loads(previous[0])
                for field in ('identity','schema_generation','source_sha256','parquet_sha256','content','archive_root','identity_index'):
                    if committed[field] != result[field]: raise RuntimeError('committed receipt provenance mismatch')
                result = committed
            if receipt.exists() and json.loads(receipt.read_text()) != result:
                raise RuntimeError('receipt projection conflict; original evidence preserved')
            c.execute('BEGIN IMMEDIATE')
            custody.commit_archive(c, receipt, result, lambda phase: cut(death, phase))
            c.execute('INSERT OR REPLACE INTO q_archive VALUES (?,?)', (identity, str(receipt)))
            c.execute("UPDATE q_segments SET status='ARCHIVED' WHERE identity=?", (identity,))
            c.commit()
            from tier import VerifiedCatalog
            VerifiedCatalog(destination,store=self).admit(c,identity,require_published=False)
            cut(death, 'after_archive_commit_before_receipt')
            if not receipt.exists(): atomic(receipt, result)
            cut(death, 'after_receipt_before_ack')
            from tier import VerifiedCatalog
            VerifiedCatalog(destination,store=self).admit(c,identity)
        finally: c.close()
        cut(death, 'after_checkpoint_before_retirement')
        return result

    def retire(self, identity, injected=None, death=None):
        dt.date.fromisoformat(identity)
        with lock(self.root), lock(self.root, 'reader.lock'):
            c = connect(self.state)
            try:
                self.recover(c)
                record = c.execute('SELECT status FROM q_segments WHERE identity=?', (identity,)).fetchone()
                if not record or record[0] not in ('ARCHIVED', 'RETIRED'): raise RuntimeError('unverified retirement refused')
                authority = c.execute('SELECT receipt,receipt_json FROM custody_archives WHERE identity=?',(identity,)).fetchone()
                if authority is None: raise RuntimeError('retirement requires committed custody authority')
                receipt_path, receipt = Path(authority[0]), json.loads(authority[1])
                if json.loads(Path(receipt_path).read_text()) != receipt: raise RuntimeError('retirement receipt projection conflict')
                custody.verify_index(receipt)
                dest = Path(receipt['archive_root']) / (identity + '.parquet')
                if sha(dest) != receipt['parquet_sha256']: raise RuntimeError('archive corrupted; source preserved')
                source = self.root / (identity + '.sqlite')
                if injected == 'retirement_failed': raise OSError('retirement blocked')
                allocated = 0
                if source.exists():
                    if sha(source) != receipt['source_sha256']: raise RuntimeError('sealed source changed')
                    allocated = source.stat().st_blocks * 512
                    source.unlink(); syncdir(self.root)
                cut(death, 'after_unlink_before_retired')
                c.execute("UPDATE q_segments SET status='RETIRED' WHERE identity=?", (identity,)); c.commit()
                atomic(Path(receipt['archive_root']) / (identity + '.retired.json'),
                       {'identity': identity, 'receipt_sha256': sha(receipt_path), 'parquet_sha256': receipt['parquet_sha256'], 'source_retired': True})
                return allocated
            finally: c.close()

    def advance_floor(self, floor, death=None, observer=None):
        # Floor commit is fenced; each retryable expiry page releases the
        # writer so present ingestion never waits for the whole historical GC.
        retention._validate_floor(floor)
        with lock(self.root):
            c = connect(self.state)
            try:
                self.recover(c)
                previous = retention.live_floor(c)
                if previous and floor < previous: raise RuntimeError('backward retention floor refused')
                for identity, status in c.execute('SELECT identity,status FROM q_segments').fetchall():
                    if status in ('ARCHIVED', 'RETIRED'):
                        row = c.execute('SELECT receipt FROM q_archive WHERE identity=?', (identity,)).fetchone()
                        if not row: raise RuntimeError('archive custody missing')
                        receipt = json.loads(Path(row[0]).read_text())
                        if sha(Path(receipt['archive_root']) / (identity + '.parquet')) != receipt['parquet_sha256']:
                            raise RuntimeError('archive custody invalid; floor cannot advance')
                    else:
                        s = connect(self.root / (identity + '.sqlite'), readonly=True)
                        older = s.execute('SELECT 1 FROM label_events WHERE ts<? LIMIT 1', (floor,)).fetchone(); s.close()
                        if older: raise RuntimeError('unarchived accepted data below proposed floor')
                db.set_meta(c, retention.RETENTION_FLOOR_KEY, floor); c.commit(); cut(death, 'after_floor_commit')
            finally: c.close()
        deleted = 0
        while True:
            with lock(self.root):
                self.check_local()
                c = connect(self.state)
                try:
                    self.recover(c)
                    if retention.live_floor(c) != floor:
                        raise RuntimeError('key_expiry_occurrence_superseded; extra keys retained')
                    owners = [r[0] for r in c.execute("SELECT DISTINCT k.segment FROM q_hot_keys k JOIN q_segments s ON s.identity=k.segment WHERE s.status='RETIRED'")]
                    removed = sum(custody.expire_owner(c, owner) for owner in owners)
                    c.commit(); deleted += removed; cut(death, 'during_key_gc')
                    c.execute('PRAGMA wal_checkpoint(PASSIVE)')
                finally: c.close()
            if observer: observer(deleted)
            if removed == 0: break
            time.sleep(0)  # Yield after a durable page; no accepted-data effect.
        with lock(self.root):
            c = connect(self.state)
            try:
                self.recover(c)
                if retention.live_floor(c) != floor:
                    raise RuntimeError('key_expiry_occurrence_superseded; retry custody retained')
                # Keep only cache-relevant history and a bounded retry ring locally.
                keep = [r[0] for r in c.execute("SELECT identity FROM q_segments WHERE status='RETIRED' ORDER BY identity DESC LIMIT 8")]
                retired = [r[0] for r in c.execute("SELECT identity FROM q_segments WHERE status='RETIRED' AND NOT EXISTS(SELECT 1 FROM q_hot_keys WHERE segment=q_segments.identity)")]
                # Expected historical coverage cannot disappear with a local
                # retry-ring record. Publish an immutable archive-side catalog
                # generation, then commit its compact global identity together
                # with pruning. A process death before commit leaves an orphan
                # generation; the prior committed catalog remains authoritative.
                pruning = [identity for identity in retired if identity not in keep]
                coverage_path = db.get_meta(c, 'q:archive_coverage_path')
                coverage_hash = db.get_meta(c, 'q:archive_coverage_sha256')
                entries = {}
                namespace = None
                if coverage_path:
                    prior = Path(coverage_path)
                    if sha(prior) != coverage_hash: raise RuntimeError('expected archive coverage corrupted; pruning refused')
                    manifest = json.loads(prior.read_text())
                    if manifest.get('schema') != 'labelwatch.archive-coverage.v1': raise RuntimeError('unknown coverage schema')
                    entries = manifest['entries']; namespace = prior.parent
                for identity in pruning:
                    receipt_path = Path(c.execute('SELECT receipt FROM q_archive WHERE identity=?', (identity,)).fetchone()[0])
                    receipt = json.loads(receipt_path.read_text()); destination = Path(receipt['archive_root'])
                    if namespace is not None and destination.resolve() != namespace.resolve(): raise RuntimeError('coverage archive namespace mismatch')
                    namespace = destination
                    marker = namespace / (identity + '.retired.json')
                    if not marker.exists() or json.loads(marker.read_text())['receipt_sha256'] != sha(receipt_path):
                        raise RuntimeError('external retirement custody not durable; local retry record retained')
                    entry = {'receipt_sha256': sha(receipt_path), 'parquet_sha256': receipt['parquet_sha256']}
                    if identity in entries and entries[identity] != entry: raise RuntimeError('expected coverage identity conflict')
                    entries[identity] = entry
                if pruning:
                    manifest = {'schema':'labelwatch.archive-coverage.v1','entries':entries}
                    encoded = (json.dumps(manifest, sort_keys=True, indent=2) + '\n').encode()
                    generation = hashlib.sha256(encoded).hexdigest()
                    published = namespace / ('coverage-' + generation + '.json')
                    if published.exists():
                        if sha(published) != generation: raise RuntimeError('immutable coverage generation changed')
                    else:
                        atomic(published, manifest)
                    db.set_meta(c, 'q:archive_coverage_path', str(published))
                    db.set_meta(c, 'q:archive_coverage_sha256', sha(published))
                    cut(death, 'after_coverage_before_prune_commit')
                for identity in retired:
                    if identity in keep: continue
                    receipt_path = c.execute('SELECT receipt FROM q_archive WHERE identity=?', (identity,)).fetchone()[0]
                    receipt = json.loads(Path(receipt_path).read_text()); marker = Path(receipt['archive_root']) / (identity + '.retired.json')
                    if not marker.exists() or json.loads(marker.read_text())['receipt_sha256'] != sha(receipt_path):
                        raise RuntimeError('external retirement custody not durable; local retry record retained')
                    c.execute('DELETE FROM q_archive WHERE identity=?', (identity,)); c.execute('DELETE FROM q_segments WHERE identity=?', (identity,))
                c.commit(); return deleted
            finally: c.close()

    @contextlib.contextmanager
    def queries(self):
        lease = lock(self.root, 'reader.lock', shared=True)
        with lock(self.root):
            c = connect(self.state)
            self.recover(c)
            lease.__enter__()
            c.execute('PRAGMA query_only=OFF')
            files = c.execute("SELECT identity FROM q_segments WHERE status!='RETIRED' ORDER BY identity").fetchall()
            pieces = []
            for i, (identity,) in enumerate(files):
                path = self.root / (identity + '.sqlite')
                c.execute(f'ATTACH DATABASE ? AS s{i}', (f'file:{path}?mode=ro',))
                pieces.append(f'SELECT {COLS} FROM s{i}.label_events')
            c.execute('CREATE TEMP VIEW label_events AS ' + ' UNION ALL '.join(pieces))
            c.execute('PRAGMA query_only=ON')
            c.execute('BEGIN')
            # Establish every attached snapshot while the writer is fenced.
            c.execute('SELECT 1 FROM main.meta LIMIT 1').fetchone()
            for i in range(len(files)): c.execute(f'SELECT id FROM s{i}.label_events LIMIT 1').fetchone()
        try: yield c
        finally: c.close(); lease.__exit__(None, None, None)


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(); p.add_argument('action'); p.add_argument('root'); p.add_argument('--period', default='2026-10-05'); p.add_argument('--death'); p.add_argument('--archive'); p.add_argument('--rows-json'); p.add_argument('--cursor', default='2')
    a = p.parse_args(); s = Store(a.root)
    if a.action == 'rotate': s.rotate(a.period, a.death)
    elif a.action == 'ingest': print(s.ingest(json.loads(Path(a.rows_json).read_text()), 'fixture-provider', a.cursor, a.death))
    elif a.action == 'archive': print(json.dumps(s.archive(a.period, a.archive, a.death)))
    elif a.action == 'retire': print(s.retire(a.period, death=a.death))
    elif a.action == 'floor': print(s.advance_floor(a.period, death=a.death))
