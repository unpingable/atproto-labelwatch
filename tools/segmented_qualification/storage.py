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
from labelwatch import db, ingest, retention
from tools.label_events_cold_archive import ARROW_SCHEMA

FIELDS = ('id', 'labeler_did', 'src', 'uri', 'cid', 'val', 'neg', 'exp', 'sig', 'ts', 'event_hash', 'target_did')
COLS = ','.join(FIELDS)
ROOT = Path('/data/git/atproto-nutrition/portfolio-private/campaigns/labelwatch-segmented-storage-qualification-20261005/runtime')
RESERVE = 64424509440


def owned(root):
    root = Path(root).resolve()
    if ROOT not in root.parents:
        raise ValueError('only new campaign-owned child fixtures are admitted')
    return root


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


@contextlib.contextmanager
def lock(root, name='writer.lock', shared=False):
    with (root / name).open('a') as f:
        fcntl.flock(f, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        yield


def connect(path, readonly=False):
    c = sqlite3.connect(f'file:{path}?mode=ro' if readonly else str(path), uri=readonly, timeout=2)
    c.row_factory = sqlite3.Row
    if not readonly:
        c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=FULL')
        c.execute('PRAGMA wal_autocheckpoint=1000')
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

    @classmethod
    def create(cls, root, period='2026-09-28', floor='2026-08-15T00:00:00Z'):
        root = owned(root); root.mkdir()
        c = connect(root / 'state.sqlite'); db.init_db(c)
        c.executescript('''
        CREATE TABLE q_segments(identity TEXT PRIMARY KEY, status TEXT NOT NULL, schema_generation INTEGER NOT NULL);
        CREATE UNIQUE INDEX q_one_active ON q_segments(status) WHERE status='ACTIVE';
        CREATE TABLE q_pending(id INTEGER PRIMARY KEY, segment TEXT NOT NULL);
        CREATE TABLE q_hot_keys(event_hash TEXT PRIMARY KEY, ts TEXT NOT NULL, event_id INTEGER NOT NULL, segment TEXT NOT NULL);
        CREATE INDEX q_hot_keys_ts ON q_hot_keys(ts);
        CREATE INDEX q_hot_keys_segment ON q_hot_keys(segment);
        CREATE TABLE q_transition(singleton INTEGER PRIMARY KEY CHECK(singleton=1), old TEXT NOT NULL, new TEXT NOT NULL);
        CREATE TABLE q_archive(identity TEXT PRIMARY KEY, receipt TEXT NOT NULL);
        ''')
        db.set_meta(c, retention.RETENTION_FLOOR_KEY, floor)
        db.set_meta(c, 'q:max_pending', '10000'); db.set_meta(c, 'q:max_local_segments', '2')
        db.set_meta(c, 'q:active', period)
        c.execute('INSERT INTO q_segments VALUES (?, ?, 23)', (period, 'ACTIVE'))
        c.commit(); c.close(); vessel(root / (period + '.sqlite'), period)
        atomic(root / 'ACTIVE.json', {'identity': period, 'authority': 'state.sqlite:q_segments'})
        return cls(root)

    def check_local(self, injected=None):
        if injected == 'root_low': raise RuntimeError('local_capacity_below_required_bound')
        for fs in ('/', '/data'):
            s = os.statvfs(fs)
            if s.f_bavail * s.f_frsize < RESERVE: raise RuntimeError('shared_reserve_unavailable')
        # Finite fixture limits. Adoption limits must be derived from the
        # measured envelope; none of these changes a production gate.
        if self.state.exists():
            c = connect(self.state)
            cap = int(db.get_meta(c, 'q:state_page_cap') or '2097152')
            c.execute('PRAGMA max_page_count=' + str(cap)); c.close()
        c = connect(self.state, readonly=True)
        try: wal_budget = int(db.get_meta(c, 'q:wal_budget_bytes') or '268435456')
        finally: c.close()
        if wal_budget <= 0: raise RuntimeError('invalid owned WAL budget')
        for path in self.root.glob('*.sqlite-wal'):
            if path.stat().st_size > wal_budget:
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
                fresh = []
                for row in rows:
                    # Same below-floor lexical comparison as current ingest.
                    floor = retention.live_floor(c)
                    if floor and row[8] < floor:
                        fresh.append(row); continue
                    if c.execute('SELECT 1 FROM q_hot_keys WHERE event_hash=?', (row[9],)).fetchone() is None:
                        fresh.append(row)
                n = ingest.store_label_events(c, fresh)
                for row in c.execute('SELECT id,event_hash,ts FROM label_events').fetchall():
                    c.execute('INSERT OR IGNORE INTO q_hot_keys VALUES (?,?,?,?)', (row[1], row[2], row[0], identity))
                    c.execute('INSERT OR IGNORE INTO q_pending VALUES (?,?)', (row[0], identity))
                evidence_seen = set()
                for row in rows:
                    db.upsert_labeler(c, row[0], row[8])
                    ingest._track_observed_src(c, row[1] or row[0], row[8], evidence_seen)
                cut(death, 'before_global_commit')
                db.set_cursor(c, source, cursor)  # Actual runtime helper commits this transaction.
                cut(death, 'after_global_commit')
                self.flush(c, death)
                return {'inserted': n, 'cursor': db.get_cursor(c, source)}
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

    def archive(self, identity, destination, death=None, injected=None):
        dt.date.fromisoformat(identity)
        # Serialize retries; retain a source reader lease until checkpoint.
        # Ingestion keeps its independent writer fence and continues.
        with lock(self.root, 'archive-' + identity + '.lock'), lock(self.root, 'reader.lock', shared=True):
            return self._archive(identity, destination, death, injected)

    def _archive(self, identity, destination, death=None, injected=None):
        dt.date.fromisoformat(identity)
        self.check_local(injected)
        destination = Path(destination)
        # Only this campaign's verified NFS root or its exact local negative fixtures.
        archive_root = Path(json.loads((ROOT.parent / 'evidence/ARCHIVE-DESTINATION.json').read_text())['campaign_archive'])
        if destination.resolve() != archive_root.resolve() and archive_root.resolve() not in destination.resolve().parents and ROOT not in destination.resolve().parents:
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
        expected = digest(sql.execute('SELECT ' + COLS + ' FROM label_events ORDER BY id')); sql.close()
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
                  'archive_admitted_bytes': need, 'archive_root': str(destination)}
        if receipt.exists():
            previous = json.loads(receipt.read_text())
            for field in ('identity', 'schema_generation', 'source_sha256', 'parquet_sha256', 'content', 'archive_root'):
                if previous[field] != result[field]: raise RuntimeError('existing receipt provenance mismatch; original evidence preserved')
            result = previous  # Preserve the original verified occurrence, including timing.
        else: atomic(receipt, result)
        c = connect(self.state); c.execute('INSERT OR REPLACE INTO q_archive VALUES (?,?)', (identity, str(receipt)))
        c.execute("UPDATE q_segments SET status='ARCHIVED' WHERE identity=?", (identity,)); c.commit(); c.close()
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
                receipt_path = c.execute('SELECT receipt FROM q_archive WHERE identity=?', (identity,)).fetchone()[0]
                receipt = json.loads(Path(receipt_path).read_text()); dest = Path(receipt['archive_root']) / (identity + '.parquet')
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

    def advance_floor(self, floor, death=None):
        # Qualification clock/floor, not a production retention policy change.
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
                deleted = 0
                while True:
                    self.check_local()  # Refuse another GC page while a reader pins oversized WAL.
                    result = c.execute('DELETE FROM q_hot_keys WHERE event_hash IN (SELECT event_hash FROM q_hot_keys WHERE ts<? ORDER BY ts LIMIT 10000)', (floor,))
                    c.commit(); deleted += result.rowcount; cut(death, 'during_key_gc')
                    c.execute('PRAGMA wal_checkpoint(PASSIVE)')
                    if result.rowcount == 0: break
                # Keep only cache-relevant history and a bounded retry ring locally.
                keep = [r[0] for r in c.execute("SELECT identity FROM q_segments WHERE status='RETIRED' ORDER BY identity DESC LIMIT 8")]
                retired = [r[0] for r in c.execute("SELECT identity FROM q_segments WHERE status='RETIRED' AND NOT EXISTS(SELECT 1 FROM q_hot_keys WHERE segment=q_segments.identity)")]
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
