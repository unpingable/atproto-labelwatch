"""Opt-in event custody contract. No automatic enrollment of existing databases.

SQLite commits acceptance; immutable indexes discharge hot-cache obligations.
Receipt files are projections, never acceptance or archive admission authority.
"""
from __future__ import annotations
import hashlib
import json
import os
import sqlite3
from functools import lru_cache
from pathlib import Path
from . import db, retention
from .utils import stable_json, format_ts, now_utc


class Refused(RuntimeError):
    """The caller must preserve the page/checkpoint and reconcile this condition."""


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''): h.update(chunk)
    return h.hexdigest()


def enabled(conn):
    return db.get_meta(conn, 'custody:contract') == 'source-record-v1'


def initialize(conn, lineage):
    if conn.in_transaction: raise Refused('initialize outside a transaction')
    if enabled(conn):
        if db.get_meta(conn, 'custody:lineage') != lineage: raise Refused('lineage conflict')
        return
    if conn.execute('SELECT 1 FROM label_events LIMIT 1').fetchone():
        raise Refused('legacy identity enrollment required; populated DB preserved')
    conn.execute('PRAGMA synchronous=FULL')
    conn.executescript('''
    CREATE TABLE q_hot_keys(event_hash TEXT PRIMARY KEY, ts TEXT NOT NULL,
        event_id INTEGER NOT NULL, segment TEXT NOT NULL, payload_sha256 TEXT NOT NULL);
    CREATE INDEX q_hot_keys_ts ON q_hot_keys(ts);
    CREATE INDEX q_hot_keys_segment ON q_hot_keys(segment);
    CREATE TABLE custody_archives(identity TEXT PRIMARY KEY, receipt TEXT NOT NULL, receipt_json TEXT NOT NULL);
    ''')
    db.set_meta(conn, 'custody:contract', 'source-record-v1')
    db.set_meta(conn, 'custody:lineage', lineage)
    db.set_meta(conn, 'custody:generation', '0')
    conn.commit()


def canonical_identity(raw):
    """Digest immutable observable record; transport cursor never participates."""
    authored = raw.get('cts') or raw.get('ts')
    if not authored or not isinstance(authored, str): raise Refused('source authored time absent; identity unavailable')
    if raw.get('cts') and raw.get('ts') and raw['cts'] != raw['ts']:
        raise Refused('conflicting authored time aliases')
    signature = raw.get('sig')
    if isinstance(signature, dict):
        if set(signature) != {'$bytes'}: raise Refused('unsupported signature encoding')
        signature = signature['$bytes']
    if signature is not None and not isinstance(signature, str): raise Refused('unsupported signature encoding')
    src = raw.get('src') or raw.get('labeler_did')
    if not src or not raw.get('uri') or not raw.get('val'): raise Refused('source record identity incomplete')
    record = {k: raw.get(k) for k in ('ver', 'uri', 'cid', 'val', 'exp')}
    record.update(src=src, neg=bool(raw.get('neg', False)), cts=authored, sig=signature)
    return hashlib.sha256(('labelwatch:source-record-v1\n' + stable_json(record)).encode()).hexdigest()


def payload_digest(row):
    return hashlib.sha256(stable_json(list(row)).encode()).hexdigest()


def committed(conn):
    if not enabled(conn): raise Refused('custody lineage is not enrolled')
    return [(r[0], Path(r[1]), json.loads(r[2])) for r in conn.execute('SELECT identity,receipt,receipt_json FROM custody_archives ORDER BY identity')]


def index_path(receipt):
    return Path(receipt['archive_root']) / receipt['identity_index']['file']


def fingerprint(path):
    st = Path(path).stat()
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


@lru_cache(maxsize=256)
def _admit_index(path, expected, stamp):
    # Same immutable fingerprint admission pattern as the existing cold catalog.
    # Not a bit-rot or concurrent nonconforming-writer guarantee.
    if file_sha(path) != expected or fingerprint(path) != stamp:
        raise Refused('labelwatch.custody.indeterminate: archive identity index unavailable/corrupt')


def verify_index(receipt):
    path = index_path(receipt)
    _admit_index(str(path), receipt['identity_index']['sha256'], fingerprint(path))
    return path


def lookup(conn, row):
    found = conn.execute('SELECT event_id,payload_sha256 FROM q_hot_keys WHERE event_hash=?', (row[9],)).fetchone()
    if found:
        if found[1] != payload_digest(row): raise Refused('record identity content conflict')
        return found[0]
    answer = None
    for owner, receipt_path, receipt in committed(conn):
        path = verify_index(receipt)
        c = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
        try: result = c.execute('SELECT event_id,payload_sha256 FROM identities WHERE event_hash=?', (row[9],)).fetchone()
        finally: c.close()
        if result:
            if result[1] != payload_digest(row): raise Refused('archived record identity content conflict')
            if answer is not None: raise Refused('multiple committed custody owners for one record identity')
            answer = result[0]
    return answer


def stage(conn, rows, owner, death=None):
    """Caller holds BEGIN IMMEDIATE; effects, identities and cursor commit together."""
    if not conn.in_transaction or not enabled(conn): raise Refused('custody acceptance transaction required')
    if conn.execute('PRAGMA synchronous').fetchone()[0] != 2: raise Refused('FULL durability required before acceptance')
    if len(rows) > 10000: raise Refused('bounded source page exceeded')
    accepted, duplicates = [], []
    for row in rows:
        if len(row) != 11 or len(row[9]) != 64: raise Refused('invalid normalized source record')
        previous = lookup(conn, row)
        if previous is not None:
            duplicates.append(previous); continue
        floor = retention.live_floor(conn)
        if floor and row[8] < floor:
            raise Refused('labelwatch.replay.identity_unavailable: unaccepted below-floor record; cursor preserved')
        if conn.execute('SELECT 1 FROM custody_archives WHERE identity=?', (owner,)).fetchone():
            raise Refused('sealed custody owner cannot accept a new event; advance active owner explicitly')
        if db.insert_label_events(conn, [row]) != 1:
            raise Refused('payload exists without its required acceptance identity; reconciliation required')
        event_id = conn.execute('SELECT id FROM label_events WHERE event_hash=?', (row[9],)).fetchone()[0]
        if death: death('after_effect_before_identity')
        conn.execute('INSERT INTO q_hot_keys VALUES (?,?,?,?,?)', (row[9], format_ts(now_utc()), event_id, owner, payload_digest(row)))
        accepted.append((event_id, row))
    db.set_meta(conn, 'custody:generation', str(int(db.get_meta(conn, 'custody:generation')) + 1))
    return accepted, duplicates


def accept(conn, rows, source, cursor, owner=None, death=None):
    """Actual single-file event insert/cursor path, opt-in only."""
    conn.execute('PRAGMA synchronous=FULL')
    conn.execute('BEGIN IMMEDIATE')
    owner = owner or db.get_meta(conn, 'custody:active_owner') or 'single-file'
    try:
        fresh, duplicates = stage(conn, rows, owner, death)
        from .ingest import _track_observed_src
        observed = set()
        for _, row in fresh:
            db.upsert_labeler(conn, row[0], row[8])
            _track_observed_src(conn, row[1] or row[0], row[8], observed)
        if death: death('before_global_commit')
        if cursor is not None: db.set_cursor(conn, source, cursor)
        else: conn.commit()
        if death: death('after_global_commit')
        return {'inserted': len(fresh), 'accepted_ids': [r[0] for r in fresh], 'duplicate_ids': duplicates, 'cursor': db.get_cursor(conn, source)}
    except BaseException:
        conn.rollback(); raise


def build_index(source, destination, owner):
    """Immutable derived exact membership, streamed from the sealed event source."""
    destination = Path(destination); temp = destination.with_suffix('.incomplete')
    if temp.exists(): temp.unlink()  # Exact owned interrupted index, not evidence.
    out = sqlite3.connect(temp)
    out.execute('CREATE TABLE identities(event_hash TEXT PRIMARY KEY,event_id INTEGER NOT NULL,payload_sha256 TEXT NOT NULL) WITHOUT ROWID')
    cursor = source.execute('SELECT id,labeler_did,src,uri,cid,val,neg,exp,sig,ts,event_hash,target_did FROM label_events ORDER BY id')
    count = 0
    while batch := cursor.fetchmany(10000):
        out.executemany('INSERT INTO identities VALUES (?,?,?)', [(r[10], r[0], payload_digest(tuple(r)[1:])) for r in batch]); count += len(batch)
    out.commit()
    assert out.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    out.close()
    with temp.open('rb') as f: os.fsync(f.fileno())
    if destination.exists():
        # Logical equivalence, not accidental SQLite byte-layout equivalence.
        a = sqlite3.connect(f'file:{destination}?mode=ro&immutable=1', uri=True)
        b = sqlite3.connect(f'file:{temp}?mode=ro&immutable=1', uri=True)
        try:
            ca, cb = a.execute('SELECT * FROM identities ORDER BY event_hash'), b.execute('SELECT * FROM identities ORDER BY event_hash')
            while True:
                ra, rb = ca.fetchmany(10000), cb.fetchmany(10000)
                if ra != rb: raise Refused('immutable identity index conflict')
                if not ra: break
        finally: a.close(); b.close()
        temp.unlink()
    else: os.replace(temp, destination)
    fd = os.open(destination.parent, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)
    return {'file': destination.name, 'sha256': file_sha(destination), 'rows': count}


def commit_archive(conn, receipt_path, receipt, death=None):
    """Commit owner/index binding; receipt projection must be published afterward."""
    previous = conn.execute('SELECT receipt_json FROM custody_archives WHERE identity=?', (receipt['identity'],)).fetchone()
    path = verify_index(receipt)
    idx = sqlite3.connect(f'file:{path}?mode=ro&immutable=1', uri=True)
    try:
        count = idx.execute('SELECT COUNT(*) FROM identities').fetchone()[0]
        if count != receipt['content']['rows'] or count != receipt['identity_index']['rows']:
            raise Refused('archive identity coverage count mismatch')
        if not previous:
            for key, event_id, payload in idx.execute('SELECT event_hash,event_id,payload_sha256 FROM identities'):
                binding = conn.execute('SELECT event_id,segment,payload_sha256 FROM q_hot_keys WHERE event_hash=?', (key,)).fetchone()
                if binding is None or tuple(binding) != (event_id, receipt['identity'], payload):
                    raise Refused('archive source includes an identity not exclusively owned by this custody object')
        for key, event_id, payload in conn.execute('SELECT event_hash,event_id,payload_sha256 FROM q_hot_keys WHERE segment=?', (receipt['identity'],)):
            if idx.execute('SELECT event_id,payload_sha256 FROM identities WHERE event_hash=?', (key,)).fetchone() != (event_id, payload):
                raise Refused('archive identity coverage incomplete')
    finally: idx.close()
    body = stable_json(receipt)
    previous = conn.execute('SELECT receipt_json FROM custody_archives WHERE identity=?', (receipt['identity'],)).fetchone()
    if previous and previous[0] != body: raise Refused('committed custody cannot be rewritten')
    conn.execute('INSERT OR IGNORE INTO custody_archives VALUES (?,?,?)', (receipt['identity'], str(receipt_path), body))
    # Caller commits this together with layout ownership and continuation metadata.
    if death: death('before_archive_commit')


def expire_owner(conn, owner):
    entry = conn.execute('SELECT receipt_json FROM custody_archives WHERE identity=?', (owner,)).fetchone()
    if not entry: raise Refused('identity lifetime obligation not discharged')
    stored = json.loads(entry[0])
    # Recheck against the current cache: no event accepted after the transfer
    # may disappear simply because its owner name was reused.
    commit_archive(conn, conn.execute('SELECT receipt FROM custody_archives WHERE identity=?', (owner,)).fetchone()[0], stored)
    return conn.execute('DELETE FROM q_hot_keys WHERE segment=?', (owner,)).rowcount


def replay(conn, rows, mode='verification', expected_checkpoint=None):
    if mode not in ('verification', 'recovery'): raise Refused('reprocessing requires a separate projection target')
    if mode == 'recovery':
        if expected_checkpoint is None: raise Refused('explicit authoritative recovery checkpoint required')
        validate_resume(conn, expected_checkpoint)
    # No current mutation authority: recovery validates already accepted identities.
    # Payload restoration uses existing reconstruction tools into an inactive target.
    answer = []
    for row in rows:
        identity = lookup(conn, row)
        if identity is None: raise Refused('prior acceptance unavailable; no recovery reacceptance')
        answer.append(identity)
    return answer


def checkpoint(conn):
    return {'lineage': db.get_meta(conn, 'custody:lineage'), 'generation': db.get_meta(conn, 'custody:generation'),
            'archives': [[owner, hashlib.sha256(stable_json(receipt).encode()).hexdigest()] for owner, _, receipt in committed(conn)],
            'cursors': dict(conn.execute("SELECT key,value FROM meta WHERE key LIKE 'ingest_cursor:%' ORDER BY key"))}


def validate_resume(conn, expected):
    if checkpoint(conn) != expected: raise Refused('labelwatch.custody.indeterminate: restored checkpoint is not authoritative')
    for _, _, receipt in committed(conn): verify_index(receipt)


def receipt(conn, rows, path, publisher, death=None):
    if conn.in_transaction:
        raise Refused('success publication requires committed acceptance outside the staging transaction')
    ids = sorted(set(replay(conn, rows, 'verification')))
    value = {'lineage': db.get_meta(conn, 'custody:lineage'), 'accepted_ids': ids, 'status': 'ACCEPTED_ONCE'}
    path = Path(path)
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise Refused('receipt identity conflict; previous publication preserved')
        return ids
    if death: death('before_receipt')
    publisher(path, value)
    if death: death('after_receipt_before_ack')
    return ids
