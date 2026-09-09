"""Exact logical preservation evidence for the finite SQLite relief procedure.

This is a read-only application verifier, not an authorization or custody engine.
The caller owns the quiescence cut and transaction. Every ordinary table is
included, even derived/cache tables; virtual tables fail closed rather than being
silently excluded. Physical layout and implicit rowids are not logical contents.
"""
from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import struct

from . import db


class VerificationRefused(ValueError):
    pass


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _frame(value: bytes) -> bytes:
    return struct.pack('>Q', len(value)) + value


def _cell(kind: str, value) -> bytes:
    if kind == 'null':
        encoded = b''
    elif kind == 'integer':
        encoded = str(value).encode('ascii')
    elif kind == 'real':
        if not math.isfinite(value):
            raise VerificationRefused('nonfinite real requires a declared comparison contract')
        encoded = struct.pack('>d', value)
    elif kind == 'text':
        # SELECT CAST(column AS BLOB) preserves SQLite UTF-8 text bytes, including
        # embedded NUL. Do not decode/re-encode through a permissive text codec.
        encoded = value
    elif kind == 'blob':
        encoded = struct.pack('>Q', len(value)) + hashlib.sha256(value).digest()
    else:
        raise VerificationRefused('unknown SQLite storage class')
    return _frame(kind.encode('ascii')) + _frame(encoded)


def logical_manifest(conn: sqlite3.Connection, *, application_revision: str) -> dict:
    """Stream all declared logical rows within the caller's read transaction.

    No schema bootstrap, migration, cursor advancement or PRAGMA write occurs.
    Sort by every declared column with BINARY collation. This declares a total
    logical ordering (identical duplicate rows are interchangeable); no hidden
    rowid is assumed stable across VACUUM.
    """
    if len(application_revision) != 40 or any(c not in '0123456789abcdef' for c in application_revision):
        raise VerificationRefused('exact application revision required')
    schema = [tuple(row) for row in conn.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name COLLATE BINARY"
    )]
    if any(row[3] and 'CREATE VIRTUAL TABLE' in row[3].upper() for row in schema):
        raise VerificationRefused('virtual table has no declared preservation/rebuild contract')
    version = db.get_schema_version(conn)
    if version != db.SCHEMA_VERSION:
        raise VerificationRefused('application schema version differs')
    schema_bytes = json.dumps(schema, ensure_ascii=True, separators=(',', ':')).encode()
    tables = []
    for _, name, _, _ in (row for row in schema if row[0] == 'table'):
        columns = [row[1] for row in conn.execute(f'PRAGMA table_xinfo({_quote(name)})')]
        if not columns:
            raise VerificationRefused('table has no declared columns')
        digest = hashlib.sha256(b'labelwatch.logical-table/v1\0' + _frame(name.encode()))
        expressions = []
        for column in columns:
            quoted = _quote(column)
            digest.update(_frame(column.encode()))
            expressions.extend([f'typeof({quoted})',
                f'CASE WHEN typeof({quoted}) = \'text\' THEN CAST({quoted} AS BLOB) ELSE {quoted} END'])
        ordering = ','.join(f'typeof({_quote(column)}) COLLATE BINARY,{_quote(column)} COLLATE BINARY'
                            for column in columns)
        count = 0
        for row in conn.execute(f'SELECT {",".join(expressions)} FROM {_quote(name)} ORDER BY {ordering}'):
            digest.update(b'row\0')
            for i in range(0, len(row), 2):
                digest.update(_cell(row[i], row[i + 1]))
            count += 1
        tables.append({'table': name, 'columns': columns, 'ordering': 'all-columns/storage-class-then-BINARY',
                       'row_count': count, 'rows_sha256': digest.hexdigest()})
    return {'schema': 'labelwatch.logical-preservation/v1',
            'application_revision': application_revision, 'application_schema': version,
            'schema_sha256': hashlib.sha256(schema_bytes).hexdigest(),
            'tables': tables, 'exclusions': [],
            'claim': 'all declared logical rows including meta/cursors; not physical layout or external state'}


def offline_application_verify(conn: sqlite3.Connection, *, application_revision: str) -> dict:
    """Reopen application schema/progress and traverse real rule evaluation.

    The rules' full read surface executes at a fixed comparison time with a
    fixed config; no claim of operational health follows from successful reads.
    """
    from datetime import datetime, timezone
    from .config import Config
    from .rules import run_rules

    integrity = [tuple(row) for row in conn.execute('PRAGMA integrity_check')]
    if integrity != [('ok',)]:
        raise VerificationRefused('SQLite integrity check failed')
    conn.row_factory = sqlite3.Row
    manifest = logical_manifest(conn, application_revision=application_revision)
    rules = run_rules(conn, Config(warmup_enabled=False),
                      datetime(2024, 1, 2, tzinfo=timezone.utc))
    return {'schema': 'labelwatch.offline-application-verification/v1',
            'integrity': 'ok', 'logical_manifest': manifest,
            'rule_ids': sorted({item['rule_id'] for item in rules}),
            'claim': 'application read/rule compatibility at declared fixed time, not current health'}
