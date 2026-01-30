import sqlite3

import pytest

from labelwatch import db


def _create_v0_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE label_events (
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

        CREATE TABLE labelers (
            labeler_did TEXT PRIMARY KEY,
            description TEXT,
            first_seen TEXT,
            last_seen TEXT
        );

        CREATE TABLE alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id TEXT NOT NULL,
            labeler_did TEXT NOT NULL,
            ts TEXT NOT NULL,
            inputs_json TEXT NOT NULL,
            evidence_hashes_json TEXT NOT NULL,
            config_hash TEXT NOT NULL,
            receipt_hash TEXT NOT NULL
        );
        """
    )
    conn.commit()


def test_init_db_sets_schema_version_meta():
    conn = db.connect(":memory:")
    db.init_db(conn)

    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    assert row is not None
    assert int(row["value"]) == db.SCHEMA_VERSION
    code_row = conn.execute("SELECT value FROM meta WHERE key='code_schema_version_seen'").fetchone()
    assert code_row is not None
    assert int(code_row["value"]) == db.SCHEMA_VERSION


def test_init_db_rejects_newer_schema():
    conn = db.connect(":memory:")
    db.init_db(conn)
    conn.execute("UPDATE meta SET value=? WHERE key='schema_version'", (str(db.SCHEMA_VERSION + 1),))
    conn.commit()

    with pytest.raises(RuntimeError):
        db.init_db(conn)


def test_migrate_v0_to_v1():
    conn = db.connect(":memory:")
    _create_v0_schema(conn)
    conn.execute(
        """
        INSERT INTO label_events(labeler_did, uri, val, ts, event_hash)
        VALUES('did:plc:labeler', 'at://did:plc:user/post/1', 'test', '2024-01-01T00:00:00Z', 'hash')
        """
    )
    conn.commit()

    db.init_db(conn)

    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    assert row is not None
    assert int(row["value"]) == db.SCHEMA_VERSION

    tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "label_events" in tables
    assert "labelers" in tables
    assert "alerts" in tables
    assert "meta" in tables

    count = conn.execute("SELECT COUNT(*) AS c FROM label_events").fetchone()["c"]
    assert count == 1
