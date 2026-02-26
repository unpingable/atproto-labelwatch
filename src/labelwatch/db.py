from __future__ import annotations

import sqlite3
from typing import Iterable, List, Optional

from .utils import get_git_commit

SCHEMA_VERSION = 9

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
    display_name TEXT,
    description TEXT,
    service_endpoint TEXT,
    labeler_class TEXT DEFAULT 'third_party',
    is_reference INTEGER DEFAULT 0,
    endpoint_status TEXT DEFAULT 'unknown',
    last_probed TEXT,
    first_seen TEXT,
    last_seen TEXT,
    visibility_class TEXT DEFAULT 'unresolved',
    reachability_state TEXT DEFAULT 'unknown',
    classification_confidence TEXT DEFAULT 'low',
    classification_reason TEXT,
    classification_version TEXT DEFAULT 'v1',
    classified_at TEXT,
    auditability TEXT DEFAULT 'low',
    observed_as_src INTEGER DEFAULT 0,
    has_labeler_service INTEGER DEFAULT 0,
    has_label_key INTEGER DEFAULT 0,
    declared_record INTEGER DEFAULT 0,
    likely_test_dev INTEGER DEFAULT 0,
    scan_count INTEGER DEFAULT 0,
    regime_state TEXT,
    regime_reason_codes TEXT,
    auditability_risk INTEGER,
    auditability_risk_band TEXT,
    auditability_risk_reasons TEXT,
    inference_risk INTEGER,
    inference_risk_band TEXT,
    inference_risk_reasons TEXT,
    temporal_coherence INTEGER,
    temporal_coherence_band TEXT,
    temporal_coherence_reasons TEXT,
    derive_version TEXT,
    derived_at TEXT,
    regime_pending TEXT,
    regime_pending_count INTEGER DEFAULT 0,
    auditability_risk_prev INTEGER,
    inference_risk_prev INTEGER,
    temporal_coherence_prev INTEGER,
    coverage_ratio REAL,
    coverage_window_successes INTEGER DEFAULT 0,
    coverage_window_attempts INTEGER DEFAULT 0,
    last_ingest_success_ts TEXT,
    last_ingest_attempt_ts TEXT
);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id TEXT NOT NULL,
    labeler_did TEXT NOT NULL,
    ts TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    evidence_hashes_json TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    receipt_hash TEXT NOT NULL,
    warmup_alert INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS labeler_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    evidence_value TEXT,
    ts TEXT NOT NULL,
    source TEXT
);

CREATE TABLE IF NOT EXISTS labeler_probe_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    ts TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    http_status INTEGER,
    normalized_status TEXT NOT NULL,
    latency_ms INTEGER,
    failure_type TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS derived_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    receipt_type TEXT NOT NULL,
    derivation_version TEXT NOT NULL,
    trigger TEXT NOT NULL,
    ts TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    previous_value_json TEXT NOT NULL,
    new_value_json TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_derived_receipts_did_type ON derived_receipts(labeler_did, receipt_type, ts);

CREATE TABLE IF NOT EXISTS ingest_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    ts TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    events_fetched INTEGER,
    http_status INTEGER,
    latency_ms INTEGER,
    error_type TEXT,
    error_summary TEXT,
    source TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_outcomes_did_ts ON ingest_outcomes(labeler_did, ts);

CREATE INDEX IF NOT EXISTS idx_label_events_labeler_ts ON label_events(labeler_did, ts);
CREATE INDEX IF NOT EXISTS idx_label_events_uri_ts ON label_events(uri, ts);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_ts ON alerts(rule_id, ts);
CREATE INDEX IF NOT EXISTS idx_labeler_evidence_did ON labeler_evidence(labeler_did, evidence_type);
CREATE INDEX IF NOT EXISTS idx_probe_history_did_ts ON labeler_probe_history(labeler_did, ts);
"""


def connect(db_path: str, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # WAL mode: readers don't block writers
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Cap SQLite page cache to ~50MB so aggregates don't eat all RAM
    conn.execute("PRAGMA cache_size=-50000")
    # Force temp tables (GROUP BY, ORDER BY) to disk instead of RAM
    conn.execute("PRAGMA temp_store=FILE")
    if readonly:
        conn.execute("PRAGMA query_only=ON")
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

    # Update query planner statistics so grouped queries use indexes
    conn.execute("ANALYZE")

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
    if current == 2 and target >= 3:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("display_name", "TEXT"),
            ("service_endpoint", "TEXT"),
            ("labeler_class", "TEXT DEFAULT 'third_party'"),
            ("is_reference", "INTEGER DEFAULT 0"),
            ("endpoint_status", "TEXT DEFAULT 'unknown'"),
            ("last_probed", "TEXT"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")
        set_schema_version(conn, 3)
        current = 3
    if current == 3 and target >= 4:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("visibility_class", "TEXT DEFAULT 'unresolved'"),
            ("reachability_state", "TEXT DEFAULT 'unknown'"),
            ("classification_confidence", "TEXT DEFAULT 'low'"),
            ("classification_reason", "TEXT"),
            ("classification_version", "TEXT DEFAULT 'v1'"),
            ("classified_at", "TEXT"),
            ("auditability", "TEXT DEFAULT 'low'"),
            ("observed_as_src", "INTEGER DEFAULT 0"),
            ("has_labeler_service", "INTEGER DEFAULT 0"),
            ("has_label_key", "INTEGER DEFAULT 0"),
            ("declared_record", "INTEGER DEFAULT 0"),
            ("likely_test_dev", "INTEGER DEFAULT 0"),
            ("scan_count", "INTEGER DEFAULT 0"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")

        # Create new tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS labeler_evidence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labeler_did TEXT NOT NULL,
                evidence_type TEXT NOT NULL,
                evidence_value TEXT,
                ts TEXT NOT NULL,
                source TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_labeler_evidence_did
            ON labeler_evidence(labeler_did, evidence_type)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS labeler_probe_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labeler_did TEXT NOT NULL,
                ts TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                http_status INTEGER,
                normalized_status TEXT NOT NULL,
                latency_ms INTEGER,
                failure_type TEXT,
                error TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_probe_history_did_ts
            ON labeler_probe_history(labeler_did, ts)
        """)

        # Backfill: all existing labelers came from listReposByCollection
        conn.execute("UPDATE labelers SET declared_record = 1")
        conn.execute("""
            UPDATE labelers SET has_labeler_service = 1
            WHERE service_endpoint IS NOT NULL
        """)
        conn.execute("UPDATE labelers SET visibility_class = 'declared'")
        conn.execute("""
            UPDATE labelers SET reachability_state = endpoint_status
            WHERE endpoint_status IS NOT NULL
        """)
        conn.execute("UPDATE labelers SET classification_reason = 'migrated_from_v3'")

        set_schema_version(conn, 4)
        current = 4
    if current == 4 and target >= 5:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("regime_state", "TEXT"),
            ("regime_reason_codes", "TEXT"),
            ("auditability_risk", "INTEGER"),
            ("auditability_risk_band", "TEXT"),
            ("auditability_risk_reasons", "TEXT"),
            ("inference_risk", "INTEGER"),
            ("inference_risk_band", "TEXT"),
            ("inference_risk_reasons", "TEXT"),
            ("temporal_coherence", "INTEGER"),
            ("temporal_coherence_band", "TEXT"),
            ("temporal_coherence_reasons", "TEXT"),
            ("derive_version", "TEXT"),
            ("derived_at", "TEXT"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_receipts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labeler_did TEXT NOT NULL,
                receipt_type TEXT NOT NULL,
                derivation_version TEXT NOT NULL,
                trigger TEXT NOT NULL,
                ts TEXT NOT NULL,
                input_hash TEXT NOT NULL,
                previous_value_json TEXT NOT NULL,
                new_value_json TEXT NOT NULL,
                reason_codes_json TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_derived_receipts_did_type
            ON derived_receipts(labeler_did, receipt_type, ts)
        """)

        set_schema_version(conn, 5)
        current = 5
    if current == 5 and target >= 6:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("regime_pending", "TEXT"),
            ("regime_pending_count", "INTEGER DEFAULT 0"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")
        set_schema_version(conn, 6)
        current = 6
    if current == 6 and target >= 7:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("auditability_risk_prev", "INTEGER"),
            ("inference_risk_prev", "INTEGER"),
            ("temporal_coherence_prev", "INTEGER"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")
        set_schema_version(conn, 7)
        current = 7
    if current == 7 and target >= 8:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()]
        if "warmup_alert" not in cols:
            conn.execute("ALTER TABLE alerts ADD COLUMN warmup_alert INTEGER DEFAULT 0")
            # Mark all existing alerts as warmup (system was in warmup when they were created)
            conn.execute("UPDATE alerts SET warmup_alert = 1")
        set_schema_version(conn, 8)
        current = 8
    if current == 8 and target >= 9:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingest_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labeler_did TEXT NOT NULL,
                ts TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                events_fetched INTEGER,
                http_status INTEGER,
                latency_ms INTEGER,
                error_type TEXT,
                error_summary TEXT,
                source TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ingest_outcomes_did_ts
            ON ingest_outcomes(labeler_did, ts)
        """)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(labelers)").fetchall()]
        for col, typedef in [
            ("coverage_ratio", "REAL"),
            ("coverage_window_successes", "INTEGER DEFAULT 0"),
            ("coverage_window_attempts", "INTEGER DEFAULT 0"),
            ("last_ingest_success_ts", "TEXT"),
            ("last_ingest_attempt_ts", "TEXT"),
        ]:
            if col not in cols:
                conn.execute(f"ALTER TABLE labelers ADD COLUMN {col} {typedef}")
        set_schema_version(conn, 9)
        current = 9
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


def insert_evidence(conn: sqlite3.Connection, labeler_did: str, evidence_type: str,
                    evidence_value: Optional[str], ts: str, source: Optional[str] = None) -> None:
    conn.execute(
        """
        INSERT INTO labeler_evidence(labeler_did, evidence_type, evidence_value, ts, source)
        VALUES(?, ?, ?, ?, ?)
        """,
        (labeler_did, evidence_type, evidence_value, ts, source),
    )


def insert_probe_history(conn: sqlite3.Connection, labeler_did: str, ts: str,
                         endpoint: str, http_status: Optional[int],
                         normalized_status: str, latency_ms: Optional[int],
                         failure_type: Optional[str] = None,
                         error: Optional[str] = None) -> None:
    conn.execute(
        """
        INSERT INTO labeler_probe_history(
            labeler_did, ts, endpoint, http_status, normalized_status,
            latency_ms, failure_type, error
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (labeler_did, ts, endpoint, http_status, normalized_status,
         latency_ms, failure_type, error),
    )


def get_evidence(conn: sqlite3.Connection, labeler_did: str) -> List[dict]:
    rows = conn.execute(
        "SELECT * FROM labeler_evidence WHERE labeler_did=? ORDER BY ts DESC",
        (labeler_did,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_probe_history(conn: sqlite3.Connection, labeler_did: str,
                      limit: int = 50) -> List[dict]:
    rows = conn.execute(
        "SELECT * FROM labeler_probe_history WHERE labeler_did=? ORDER BY ts DESC LIMIT ?",
        (labeler_did, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def insert_derived_receipt(conn: sqlite3.Connection, labeler_did: str,
                           receipt_type: str, derivation_version: str,
                           trigger: str, ts: str, input_hash: str,
                           previous_value_json: str, new_value_json: str,
                           reason_codes_json: str) -> None:
    conn.execute(
        """
        INSERT INTO derived_receipts(
            labeler_did, receipt_type, derivation_version, trigger, ts,
            input_hash, previous_value_json, new_value_json, reason_codes_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (labeler_did, receipt_type, derivation_version, trigger, ts,
         input_hash, previous_value_json, new_value_json, reason_codes_json),
    )


def get_latest_derived(conn: sqlite3.Connection, labeler_did: str,
                       receipt_type: str) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT * FROM derived_receipts
        WHERE labeler_did=? AND receipt_type=?
        ORDER BY ts DESC LIMIT 1
        """,
        (labeler_did, receipt_type),
    ).fetchone()
    return dict(row) if row else None


def update_labeler_derived(conn: sqlite3.Connection, labeler_did: str,
                           regime_state: str, regime_reason_codes: str,
                           auditability_risk: int, auditability_risk_band: str,
                           auditability_risk_reasons: str,
                           inference_risk: int, inference_risk_band: str,
                           inference_risk_reasons: str,
                           temporal_coherence: int, temporal_coherence_band: str,
                           temporal_coherence_reasons: str,
                           derive_version: str, derived_at: str,
                           regime_pending: Optional[str] = None,
                           regime_pending_count: int = 0,
                           auditability_risk_prev: Optional[int] = None,
                           inference_risk_prev: Optional[int] = None,
                           temporal_coherence_prev: Optional[int] = None) -> None:
    conn.execute(
        """
        UPDATE labelers SET
            regime_state=?, regime_reason_codes=?,
            auditability_risk=?, auditability_risk_band=?, auditability_risk_reasons=?,
            inference_risk=?, inference_risk_band=?, inference_risk_reasons=?,
            temporal_coherence=?, temporal_coherence_band=?, temporal_coherence_reasons=?,
            derive_version=?, derived_at=?,
            regime_pending=?, regime_pending_count=?,
            auditability_risk_prev=?, inference_risk_prev=?, temporal_coherence_prev=?
        WHERE labeler_did=?
        """,
        (regime_state, regime_reason_codes,
         auditability_risk, auditability_risk_band, auditability_risk_reasons,
         inference_risk, inference_risk_band, inference_risk_reasons,
         temporal_coherence, temporal_coherence_band, temporal_coherence_reasons,
         derive_version, derived_at,
         regime_pending, regime_pending_count,
         auditability_risk_prev, inference_risk_prev, temporal_coherence_prev,
         labeler_did),
    )


def increment_scan_count(conn: sqlite3.Connection, labeler_did: str) -> None:
    conn.execute(
        "UPDATE labelers SET scan_count = scan_count + 1 WHERE labeler_did = ?",
        (labeler_did,),
    )


def insert_ingest_outcome(conn: sqlite3.Connection, labeler_did: str, ts: str,
                           attempt_id: str, outcome: str, events_fetched: int,
                           http_status: Optional[int], latency_ms: Optional[int],
                           error_type: Optional[str], error_summary: Optional[str],
                           source: str) -> None:
    conn.execute(
        """
        INSERT INTO ingest_outcomes(
            labeler_did, ts, attempt_id, outcome, events_fetched,
            http_status, latency_ms, error_type, error_summary, source
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (labeler_did, ts, attempt_id, outcome, events_fetched,
         http_status, latency_ms, error_type, error_summary, source),
    )
