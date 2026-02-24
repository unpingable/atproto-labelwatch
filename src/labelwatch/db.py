from __future__ import annotations

import sqlite3
from typing import Iterable, Optional

from .utils import get_git_commit

SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS label_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    src TEXT,
    uri TEXT NOT NULL,
    cid TEXT,
    val TEXT NOT NULL,
    neg INTEGER DEFAULT 0,
    exp TEXT,
    sig TEXT,
    ts TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS labelers (
    labeler_did TEXT PRIMARY KEY,
    handle TEXT,
    description TEXT,
    first_seen TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT NOT NULL,
    labeler_did TEXT NOT NULL,
    ts TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    evidence_hashes_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    receipt_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_label_events_labeler_ts ON label_events(labeler_did, ts);
CREATE INDEX IF NOT EXISTS idx_label_events_uri_ts ON label_events(uri, ts);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_ts ON alerts(rule_id, ts);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    current = get_schema_version(conn)
    if current is None:
        conn.executescript(SCHEMA)
        set_schema_version(conn, SCHEMA_VERSION)
        conn.commit()
        current = SCHEMA_VERSION
    if current > SCHEMA_VERSION:
        raise RuntimeError(f"DB schema version {current} is newer than code {SCHEMA_VERSION}")
    if current < SCHEMA_VERSION:
        migrate(conn, current, SCHEMA_VERSION)
        conn.commit()

    set_meta(conn, "code_schema_version_seen", str(SCHEMA_VERSION))
    git_commit = get_git_commit()
    if git_commit:
        set_meta(conn, "code_build_seen", git_commit)
    conn.commit()


def get_schema_version(conn: sqlite3.Connection) -> int | None:
    value = get_meta(conn, "schema_version")
    if value is None:
        return None
    return int(value)


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return row["value"]


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO meta(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )


def set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    set_meta(conn, "schema_version", str(version))


def migrate(conn: sqlite3.Connection, current: int, target: int) -> None:
    if current == 0 and target >= 1:
        conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.executescript(SCHEMA)
        set_schema_version(conn, 1)
        current = 1
    if current == 1 and target >= 2:
        # Add handle column to labelers
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        if "handle" not in cols:
            conn.execute("ALTER TABLE labelers ADD COLUMN handle TEXT")
        set_schema_version(conn, 2)
        current = 2
    if current != target:
        raise RuntimeError(f"Unsupported schema migration {current} -> {target}")


def upsert_labeler(conn: sqlite3.Connection, labeler_did: str, seen_ts: str, description: Optional[str] = None) -> None:
    conn.execute(
        """
        INSERT INTO labelers(labeler_did, description, first_seen, last_seen)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(labeler_did) DO UPDATE SET
            last_seen=excluded.last_seen,
            description=COALESCE(excluded.description, labelers.description)
        """,
        (labeler_did, description, seen_ts, seen_ts),
    )


def insert_label_events(conn: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    cur = conn.executemany(
        """
        INSERT OR IGNORE INTO label_events(
            labeler_did, src, uri, cid, val, neg, exp, sig, ts, event_hash
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return cur.rowcount


def get_cursor(conn: sqlite3.Connection, source: str) -> str | None:
    return get_meta(conn, f"ingest_cursor:{source}")


def set_cursor(conn: sqlite3.Connection, source: str, cursor: str) -> None:
    set_meta(conn, f"ingest_cursor:{source}", cursor)
    conn.commit()


def get_handle(conn: sqlite3.Connection, labeler_did: str) -> Optional[str]:
    row = conn.execute("SELECT handle FROM labelers WHERE labeler_did=?", (labeler_did,)).fetchone()
    if row and row["handle"]:
        return row["handle"]
    return None


def insert_alert(conn: sqlite3.Connection, row: tuple) -> None:
    conn.execute(
        """
        INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )
