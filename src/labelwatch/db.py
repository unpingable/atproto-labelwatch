from __future__ import annotations

import logging
import sqlite3
from typing import Iterable, List, Optional

from .utils import get_git_commit

_log = logging.getLogger(__name__)

SCHEMA_VERSION = 21

# SCHEMA_TABLES: all CREATE TABLE statements. Safe to run against pre-existing
# tables (IF NOT EXISTS is a no-op). Used by v0→v1 bootstrap where the table
# may already exist with fewer columns — later migrations add columns via ALTER.
SCHEMA_TABLES = """
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
    event_hash TEXT NOT NULL UNIQUE,
    target_did TEXT
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
    last_ingest_attempt_ts TEXT,
    events_7d INTEGER DEFAULT 0,
    events_30d INTEGER DEFAULT 0,
    unique_targets_7d INTEGER DEFAULT 0,
    unique_targets_30d INTEGER DEFAULT 0,
    unique_subjects_7d INTEGER DEFAULT 0,
    unique_subjects_30d INTEGER DEFAULT 0
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

CREATE TABLE IF NOT EXISTS derived_label_fp (
    label_event_id    INTEGER PRIMARY KEY,
    labeler_did       TEXT NOT NULL,
    uri               TEXT NOT NULL,
    label_ts          TEXT NOT NULL,
    claim_fingerprint TEXT,
    post_created_ts   TEXT,
    lag_sec_claimed   INTEGER
);

CREATE TABLE IF NOT EXISTS derived_labeler_lag_7d (
    labeler_did    TEXT PRIMARY KEY,
    n_total        INTEGER NOT NULL,
    null_rate      REAL NOT NULL,
    p50_lag        INTEGER,
    p90_lag        INTEGER,
    p95_lag        INTEGER,
    p99_lag        INTEGER,
    p90_p50_ratio  REAL,
    neg_rate       REAL NOT NULL,
    updated_epoch  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_labeler_reversal_7d (
    labeler_did      TEXT PRIMARY KEY,
    n_apply_events   INTEGER NOT NULL,
    n_apply_groups   INTEGER NOT NULL,
    n_reversals      INTEGER NOT NULL,
    pct_reversed     REAL NOT NULL,
    p50_dwell        INTEGER,
    p90_dwell        INTEGER,
    p95_dwell        INTEGER,
    p99_dwell        INTEGER,
    top_val          TEXT,
    top_val_pct      REAL,
    truncated        INTEGER NOT NULL DEFAULT 0,
    updated_epoch    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_labeler_boundary_load_7d (
    labeler_did      TEXT PRIMARY KEY,
    n_matched        INTEGER NOT NULL,
    n_negative       INTEGER NOT NULL,
    n_sub_1s         INTEGER NOT NULL,
    n_sub_5s         INTEGER NOT NULL,
    n_sub_30s        INTEGER NOT NULL,
    n_sub_60s        INTEGER NOT NULL,
    p5_lag           INTEGER,
    p10_lag          INTEGER,
    updated_epoch    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_val_dist_day (
    labeler_did  TEXT NOT NULL,
    day_epoch    INTEGER NOT NULL,
    val          TEXT NOT NULL,
    n            INTEGER NOT NULL,
    PRIMARY KEY (labeler_did, day_epoch, val)
);

CREATE TABLE IF NOT EXISTS derived_labeler_entropy_7d (
    labeler_did    TEXT PRIMARY KEY,
    n_events_7d    INTEGER NOT NULL,
    k_vals_7d      INTEGER NOT NULL,
    entropy_7d     REAL,
    h_norm_7d      REAL,
    n_eff_7d       REAL,
    top1_val       TEXT,
    top1_share     REAL,
    top2_share     REAL,
    n_events_30d   INTEGER NOT NULL,
    k_vals_30d     INTEGER NOT NULL,
    entropy_30d    REAL,
    h_norm_30d     REAL,
    n_eff_30d      REAL,
    delta_h_norm   REAL,
    updated_epoch  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_author_day (
    author_did   TEXT NOT NULL,
    day_epoch    INTEGER NOT NULL,
    events       INTEGER NOT NULL,
    applies      INTEGER NOT NULL,
    removes      INTEGER NOT NULL,
    labelers     INTEGER NOT NULL,
    targets      INTEGER NOT NULL,
    vals         INTEGER NOT NULL,
    PRIMARY KEY (author_did, day_epoch)
);

CREATE TABLE IF NOT EXISTS derived_author_labeler_day (
    author_did   TEXT NOT NULL,
    day_epoch    INTEGER NOT NULL,
    labeler_did  TEXT NOT NULL,
    events       INTEGER NOT NULL,
    applies      INTEGER NOT NULL,
    removes      INTEGER NOT NULL,
    targets      INTEGER NOT NULL,
    PRIMARY KEY (author_did, day_epoch, labeler_did)
);

CREATE TABLE IF NOT EXISTS discovery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    labeler_did TEXT NOT NULL,
    operation TEXT NOT NULL,
    source TEXT NOT NULL,
    time_us INTEGER,
    commit_cid TEXT,
    commit_rev TEXT,
    record_json TEXT,
    record_sha256 TEXT,
    resolved_endpoint TEXT,
    discovered_at TEXT NOT NULL,
    UNIQUE(labeler_did, commit_rev, operation)
);

CREATE TABLE IF NOT EXISTS boundary_edges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    edge_type TEXT NOT NULL,
    target_uri TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    labeler_a TEXT NOT NULL,
    labeler_b TEXT NOT NULL,
    jsd REAL,
    top_family_a TEXT,
    top_share_a REAL,
    top_family_b TEXT,
    top_share_b REAL,
    delta_s REAL,
    overlap REAL,
    leader_did TEXT,
    n_events_a INTEGER,
    n_events_b INTEGER,
    family_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    UNIQUE(edge_type, target_uri, window_start, labeler_a, labeler_b, family_version)
);

CREATE TABLE IF NOT EXISTS boundary_targets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_uri TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    n_labelers INTEGER NOT NULL,
    n_events INTEGER NOT NULL,
    mean_jsd_to_centroid REAL,
    max_jsd_pair REAL,
    top_families_json TEXT,
    family_version TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    UNIQUE(target_uri, window_start, family_version)
);

CREATE TABLE IF NOT EXISTS posted_findings (
    dedupe_key TEXT PRIMARY KEY,
    finding_type TEXT NOT NULL,
    post_uri TEXT,
    posted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_registry (
    host_pattern TEXT PRIMARY KEY,
    match_type TEXT NOT NULL,        -- 'exact' | 'suffix'
    provider_group TEXT NOT NULL,    -- bluesky | known_alt | one_off | unknown
    provider_label TEXT NOT NULL,    -- human-readable
    is_major_provider INTEGER NOT NULL DEFAULT 0
);
"""

# SCHEMA_INDEXES: all CREATE INDEX statements. Separated from tables because
# the v0→v1 bootstrap may encounter pre-existing tables missing columns added
# by later migrations. Indexes referencing those columns would fail. Each
# migration creates its own indexes at the right time; fresh DBs run both.
SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_derived_receipts_did_type ON derived_receipts(labeler_did, receipt_type, ts);
CREATE INDEX IF NOT EXISTS idx_ingest_outcomes_did_ts ON ingest_outcomes(labeler_did, ts);
CREATE INDEX IF NOT EXISTS idx_derived_label_fp_labeler ON derived_label_fp(labeler_did);
CREATE INDEX IF NOT EXISTS idx_derived_label_fp_fp ON derived_label_fp(claim_fingerprint);
CREATE INDEX IF NOT EXISTS idx_derived_author_labeler_day_author ON derived_author_labeler_day(author_did);
CREATE INDEX IF NOT EXISTS idx_label_events_labeler_ts ON label_events(labeler_did, ts);
CREATE INDEX IF NOT EXISTS idx_label_events_uri_ts ON label_events(uri, ts);
CREATE INDEX IF NOT EXISTS idx_label_events_target_did_ts ON label_events(target_did, ts);
CREATE INDEX IF NOT EXISTS idx_label_events_ts ON label_events(ts);
CREATE INDEX IF NOT EXISTS idx_alerts_rule_ts ON alerts(rule_id, ts);
CREATE INDEX IF NOT EXISTS idx_labeler_evidence_did ON labeler_evidence(labeler_did, evidence_type);
CREATE INDEX IF NOT EXISTS idx_probe_history_did_ts ON labeler_probe_history(labeler_did, ts);
CREATE INDEX IF NOT EXISTS idx_discovery_events_did ON discovery_events(labeler_did);
CREATE INDEX IF NOT EXISTS idx_discovery_events_ts ON discovery_events(discovered_at);
CREATE INDEX IF NOT EXISTS idx_boundary_edges_target ON boundary_edges(target_uri);
CREATE INDEX IF NOT EXISTS idx_boundary_edges_computed ON boundary_edges(computed_at);
CREATE INDEX IF NOT EXISTS idx_boundary_targets_computed ON boundary_targets(computed_at);
CREATE INDEX IF NOT EXISTS idx_label_events_hide ON label_events(src, ts, uri) WHERE val = '!hide' AND neg = 0;
"""

# Full schema = tables + indexes. Used for fresh DB init.
SCHEMA = SCHEMA_TABLES + SCHEMA_INDEXES


def _ensure_columns(conn: sqlite3.Connection, table: str,
                    columns: list[tuple[str, str]]) -> None:
    """Add columns to a table if they don't already exist.

    Each entry in columns is (column_name, type_and_default), e.g.
    ("handle", "TEXT") or ("scan_count", "INTEGER DEFAULT 0").
    Uses PRAGMA table_info guard so it's safe to call on tables that
    already have the columns (idempotent across fresh-init and migration).
    """
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, typedef in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")


def parse_target_did(uri: str | None) -> str | None:
    """Extract the target DID from an AT URI (at://did:plc:xxx/...).

    Returns None for non-AT URIs, bare DIDs, or malformed input.
    """
    if not uri:
        return None
    uri = uri.strip()
    if not uri.startswith("at://"):
        return None
    parts = uri[5:].split("/", 1)
    did = parts[0]
    if did.startswith("did:plc:") or did.startswith("did:web:"):
        return did
    return None


def _backfill_target_did(conn: sqlite3.Connection) -> None:
    """Batch-update target_did from existing URIs. Called during v15→v16 migration."""
    total = 0
    while True:
        rows = conn.execute(
            "SELECT id, uri FROM label_events "
            "WHERE target_did IS NULL AND uri LIKE 'at://%' "
            "LIMIT 10000"
        ).fetchall()
        if not rows:
            break
        for r in rows:
            did = parse_target_did(r["uri"])
            if did:
                conn.execute(
                    "UPDATE label_events SET target_did = ? WHERE id = ?",
                    (did, r["id"]),
                )
        conn.commit()
        total += len(rows)
        _log.info("backfill_target_did: %d rows updated (running total: %d)", len(rows), total)
    if total > 0:
        _log.info("backfill_target_did: complete, %d rows total", total)


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
        # Check whether core tables already exist (v0 DB: tables present, no meta).
        # Check multiple tables to avoid misclassifying a partial/unrelated DB.
        has_tables = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name IN ('label_events', 'labelers')"
        ).fetchone()
        if has_tables:
            # v0 DB — create meta, set version 0, let migrate() walk the chain.
            conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            set_schema_version(conn, 0)
            conn.commit()
            current = 0
        else:
            # Truly fresh DB — run full schema (tables + indexes).
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


def optimize_db(conn: sqlite3.Connection) -> dict:
    """Run query planner optimization. Maintenance, not boot.

    Returns summary of what was done.
    """
    result: dict = {}
    # PRAGMA optimize: lightweight, lets SQLite decide which tables need ANALYZE
    conn.execute("PRAGMA optimize")
    result["pragma_optimize"] = True

    # Targeted ANALYZE on tables that change frequently
    for table in ("label_events", "labelers", "alerts", "discovery_events",
                  "labeler_evidence", "ingest_outcomes"):
        try:
            conn.execute(f"ANALYZE {table}")
        except sqlite3.OperationalError:
            pass  # table may not exist on older schemas
    result["targeted_analyze"] = True

    conn.commit()
    _log.info("optimize_db: done")
    return result


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
        # Tables only — indexes may reference columns added by later migrations.
        # Each migration creates its own indexes; the full chain will cover them.
        conn.executescript(SCHEMA_TABLES)
        set_schema_version(conn, 1)
        current = 1
    if current == 1 and target >= 2:
        _ensure_columns(conn, "labelers", [("handle", "TEXT")])
        set_schema_version(conn, 2)
        current = 2
    if current == 2 and target >= 3:
        _ensure_columns(conn, "labelers", [
            ("display_name", "TEXT"),
            ("service_endpoint", "TEXT"),
            ("labeler_class", "TEXT DEFAULT 'third_party'"),
            ("is_reference", "INTEGER DEFAULT 0"),
            ("endpoint_status", "TEXT DEFAULT 'unknown'"),
            ("last_probed", "TEXT"),
        ])
        set_schema_version(conn, 3)
        current = 3
    if current == 3 and target >= 4:
        _ensure_columns(conn, "labelers", [
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
        ])

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
        _ensure_columns(conn, "labelers", [
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
        ])

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
        _ensure_columns(conn, "labelers", [
            ("regime_pending", "TEXT"),
            ("regime_pending_count", "INTEGER DEFAULT 0"),
        ])
        set_schema_version(conn, 6)
        current = 6
    if current == 6 and target >= 7:
        _ensure_columns(conn, "labelers", [
            ("auditability_risk_prev", "INTEGER"),
            ("inference_risk_prev", "INTEGER"),
            ("temporal_coherence_prev", "INTEGER"),
        ])
        set_schema_version(conn, 7)
        current = 7
    if current == 7 and target >= 8:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}
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
        _ensure_columns(conn, "labelers", [
            ("coverage_ratio", "REAL"),
            ("coverage_window_successes", "INTEGER DEFAULT 0"),
            ("coverage_window_attempts", "INTEGER DEFAULT 0"),
            ("last_ingest_success_ts", "TEXT"),
            ("last_ingest_attempt_ts", "TEXT"),
        ])
        set_schema_version(conn, 9)
        current = 9
    if current == 9 and target >= 10:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_label_fp (
                label_event_id    INTEGER PRIMARY KEY,
                labeler_did       TEXT NOT NULL,
                uri               TEXT NOT NULL,
                label_ts          TEXT NOT NULL,
                claim_fingerprint TEXT,
                post_created_ts   TEXT,
                lag_sec_claimed   INTEGER
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_derived_label_fp_labeler
            ON derived_label_fp(labeler_did)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_derived_label_fp_fp
            ON derived_label_fp(claim_fingerprint)
        """)
        set_schema_version(conn, 10)
        current = 10
    if current == 10 and target >= 11:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_labeler_lag_7d (
                labeler_did    TEXT PRIMARY KEY,
                n_total        INTEGER NOT NULL,
                null_rate      REAL NOT NULL,
                p50_lag        INTEGER,
                p90_lag        INTEGER,
                neg_rate       REAL NOT NULL,
                updated_epoch  INTEGER NOT NULL
            )
        """)
        set_schema_version(conn, 11)
        current = 11
    if current == 11 and target >= 12:
        _ensure_columns(conn, "derived_labeler_lag_7d", [
            ("p95_lag", "INTEGER"),
            ("p99_lag", "INTEGER"),
            ("p90_p50_ratio", "REAL"),
        ])
        set_schema_version(conn, 12)
        current = 12
    if current == 12 and target >= 13:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_labeler_reversal_7d (
                labeler_did      TEXT PRIMARY KEY,
                n_apply_events   INTEGER NOT NULL,
                n_apply_groups   INTEGER NOT NULL,
                n_reversals      INTEGER NOT NULL,
                pct_reversed     REAL NOT NULL,
                p50_dwell        INTEGER,
                p90_dwell        INTEGER,
                p95_dwell        INTEGER,
                p99_dwell        INTEGER,
                top_val          TEXT,
                top_val_pct      REAL,
                truncated        INTEGER NOT NULL DEFAULT 0,
                updated_epoch    INTEGER NOT NULL
            )
        """)
        set_schema_version(conn, 13)
        current = 13
    if current == 13 and target >= 14:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_labeler_boundary_load_7d (
                labeler_did      TEXT PRIMARY KEY,
                n_matched        INTEGER NOT NULL,
                n_negative       INTEGER NOT NULL,
                n_sub_1s         INTEGER NOT NULL,
                n_sub_5s         INTEGER NOT NULL,
                n_sub_30s        INTEGER NOT NULL,
                n_sub_60s        INTEGER NOT NULL,
                p5_lag           INTEGER,
                p10_lag          INTEGER,
                updated_epoch    INTEGER NOT NULL
            )
        """)
        set_schema_version(conn, 14)
        current = 14
    if current == 14 and target >= 15:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_val_dist_day (
                labeler_did  TEXT NOT NULL,
                day_epoch    INTEGER NOT NULL,
                val          TEXT NOT NULL,
                n            INTEGER NOT NULL,
                PRIMARY KEY (labeler_did, day_epoch, val)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_labeler_entropy_7d (
                labeler_did    TEXT PRIMARY KEY,
                n_events_7d    INTEGER NOT NULL,
                k_vals_7d      INTEGER NOT NULL,
                entropy_7d     REAL,
                h_norm_7d      REAL,
                n_eff_7d       REAL,
                top1_val       TEXT,
                top1_share     REAL,
                top2_share     REAL,
                n_events_30d   INTEGER NOT NULL,
                k_vals_30d     INTEGER NOT NULL,
                entropy_30d    REAL,
                h_norm_30d     REAL,
                n_eff_30d      REAL,
                delta_h_norm   REAL,
                updated_epoch  INTEGER NOT NULL
            )
        """)
        set_schema_version(conn, 15)
        current = 15
    if current == 15 and target >= 16:
        _ensure_columns(conn, "label_events", [("target_did", "TEXT")])
        conn.commit()  # Release DDL lock before long DML

        # Backfill target_did from existing URIs
        _backfill_target_did(conn)

        # Indexes (after backfill to avoid index maintenance during bulk update)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_label_events_target_did_ts
            ON label_events(target_did, ts)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_label_events_ts
            ON label_events(ts)
        """)

        # Rollup tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_author_day (
                author_did   TEXT NOT NULL,
                day_epoch    INTEGER NOT NULL,
                events       INTEGER NOT NULL,
                applies      INTEGER NOT NULL,
                removes      INTEGER NOT NULL,
                labelers     INTEGER NOT NULL,
                targets      INTEGER NOT NULL,
                vals         INTEGER NOT NULL,
                PRIMARY KEY (author_did, day_epoch)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS derived_author_labeler_day (
                author_did   TEXT NOT NULL,
                day_epoch    INTEGER NOT NULL,
                labeler_did  TEXT NOT NULL,
                events       INTEGER NOT NULL,
                applies      INTEGER NOT NULL,
                removes      INTEGER NOT NULL,
                targets      INTEGER NOT NULL,
                PRIMARY KEY (author_did, day_epoch, labeler_did)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_derived_author_labeler_day_author
            ON derived_author_labeler_day(author_did)
        """)

        set_schema_version(conn, 16)
        current = 16
    if current == 16 and target >= 17:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS discovery_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                labeler_did TEXT NOT NULL,
                operation TEXT NOT NULL,
                source TEXT NOT NULL,
                time_us INTEGER,
                commit_cid TEXT,
                commit_rev TEXT,
                record_json TEXT,
                record_sha256 TEXT,
                resolved_endpoint TEXT,
                discovered_at TEXT NOT NULL,
                UNIQUE(labeler_did, commit_rev, operation)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_discovery_events_did
            ON discovery_events(labeler_did)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_discovery_events_ts
            ON discovery_events(discovered_at)
        """)
        set_schema_version(conn, 17)
        current = 17
    if current == 17 and target >= 18:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boundary_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                edge_type TEXT NOT NULL,
                target_uri TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                labeler_a TEXT NOT NULL,
                labeler_b TEXT NOT NULL,
                jsd REAL,
                top_family_a TEXT,
                top_share_a REAL,
                top_family_b TEXT,
                top_share_b REAL,
                delta_s REAL,
                overlap REAL,
                leader_did TEXT,
                n_events_a INTEGER,
                n_events_b INTEGER,
                family_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                UNIQUE(edge_type, target_uri, window_start, labeler_a, labeler_b, family_version)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS boundary_targets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_uri TEXT NOT NULL,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                n_labelers INTEGER NOT NULL,
                n_events INTEGER NOT NULL,
                mean_jsd_to_centroid REAL,
                max_jsd_pair REAL,
                top_families_json TEXT,
                family_version TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                UNIQUE(target_uri, window_start, family_version)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_boundary_edges_target
            ON boundary_edges(target_uri)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_boundary_edges_computed
            ON boundary_edges(computed_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_boundary_targets_computed
            ON boundary_targets(computed_at)
        """)
        set_schema_version(conn, 18)
        current = 18
    if current == 18 and target >= 19:
        _ensure_columns(conn, "labelers", [
            ("events_7d", "INTEGER DEFAULT 0"),
            ("events_30d", "INTEGER DEFAULT 0"),
            ("unique_targets_7d", "INTEGER DEFAULT 0"),
            ("unique_targets_30d", "INTEGER DEFAULT 0"),
            ("unique_subjects_7d", "INTEGER DEFAULT 0"),
            ("unique_subjects_30d", "INTEGER DEFAULT 0"),
        ])
        set_schema_version(conn, 19)
        current = 19
    if current == 19 and target >= 20:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS posted_findings (
                dedupe_key TEXT PRIMARY KEY,
                finding_type TEXT NOT NULL,
                post_uri TEXT,
                posted_at TEXT NOT NULL
            );
        """)
        set_schema_version(conn, 20)
        current = 20
    if current == 20 and target >= 21:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS provider_registry (
                host_pattern TEXT PRIMARY KEY,
                match_type TEXT NOT NULL,
                provider_group TEXT NOT NULL,
                provider_label TEXT NOT NULL,
                is_major_provider INTEGER NOT NULL DEFAULT 0
            );
        """)
        # Seed known providers
        conn.executemany(
            "INSERT OR IGNORE INTO provider_registry VALUES (?, ?, ?, ?, ?)",
            [
                ("host.bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
                ("bsky.social", "exact", "bluesky", "Bluesky-hosted", 1),
                ("bsky.network", "suffix", "bluesky", "Bluesky-hosted", 1),
                ("blacksky.app", "suffix", "known_alt", "Blacksky", 1),
                ("atproto.brid.gy", "exact", "known_alt", "Bridgy Fed", 1),
                ("pds.rip", "suffix", "known_alt", "pds.rip", 0),
            ],
        )
        set_schema_version(conn, 21)
        current = 21
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
            labeler_did, src, uri, cid, val, neg, exp, sig, ts, event_hash, target_did
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def insert_discovery_event(conn: sqlite3.Connection, labeler_did: str,
                           operation: str, source: str,
                           discovered_at: str,
                           time_us: Optional[int] = None,
                           commit_cid: Optional[str] = None,
                           commit_rev: Optional[str] = None,
                           record_json: Optional[str] = None,
                           record_sha256: Optional[str] = None,
                           resolved_endpoint: Optional[str] = None) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO discovery_events(
            labeler_did, operation, source, time_us, commit_cid, commit_rev,
            record_json, record_sha256, resolved_endpoint, discovered_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (labeler_did, operation, source, time_us, commit_cid, commit_rev,
         record_json, record_sha256, resolved_endpoint, discovered_at),
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
                           temporal_coherence_prev: Optional[int] = None,
                           events_7d: int = 0,
                           events_30d: int = 0,
                           unique_targets_7d: int = 0,
                           unique_targets_30d: int = 0,
                           unique_subjects_7d: int = 0,
                           unique_subjects_30d: int = 0) -> None:
    conn.execute(
        """
        UPDATE labelers SET
            regime_state=?, regime_reason_codes=?,
            auditability_risk=?, auditability_risk_band=?, auditability_risk_reasons=?,
            inference_risk=?, inference_risk_band=?, inference_risk_reasons=?,
            temporal_coherence=?, temporal_coherence_band=?, temporal_coherence_reasons=?,
            derive_version=?, derived_at=?,
            regime_pending=?, regime_pending_count=?,
            auditability_risk_prev=?, inference_risk_prev=?, temporal_coherence_prev=?,
            events_7d=?, events_30d=?,
            unique_targets_7d=?, unique_targets_30d=?,
            unique_subjects_7d=?, unique_subjects_30d=?
        WHERE labeler_did=?
        """,
        (regime_state, regime_reason_codes,
         auditability_risk, auditability_risk_band, auditability_risk_reasons,
         inference_risk, inference_risk_band, inference_risk_reasons,
         temporal_coherence, temporal_coherence_band, temporal_coherence_reasons,
         derive_version, derived_at,
         regime_pending, regime_pending_count,
         auditability_risk_prev, inference_risk_prev, temporal_coherence_prev,
         events_7d, events_30d,
         unique_targets_7d, unique_targets_30d,
         unique_subjects_7d, unique_subjects_30d,
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


def has_been_posted(conn: sqlite3.Connection, dedupe_key: str,
                    cooldown_days: int = 0) -> bool:
    """Check if a finding with this dedupe_key has been posted (recently).

    Args:
        dedupe_key: The finding's identity key.
        cooldown_days: If >0, only returns True if posted within this many
            days. A fight posted 10 days ago with cooldown_days=7 returns
            False (eligible for repost). If 0, any prior post = True.
    """
    if cooldown_days > 0:
        from .utils import format_ts, now_utc
        from datetime import timedelta
        cutoff = format_ts(now_utc() - timedelta(days=cooldown_days))
        row = conn.execute(
            "SELECT 1 FROM posted_findings WHERE dedupe_key = ? AND posted_at >= ?",
            (dedupe_key, cutoff),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM posted_findings WHERE dedupe_key = ?", (dedupe_key,)
        ).fetchone()
    return row is not None


def record_posted(conn: sqlite3.Connection, dedupe_key: str,
                  finding_type: str, post_uri: str | None = None) -> None:
    """Record that a finding has been posted."""
    from .utils import format_ts, now_utc
    conn.execute(
        """
        INSERT OR REPLACE INTO posted_findings (dedupe_key, finding_type, post_uri, posted_at)
        VALUES (?, ?, ?, ?)
        """,
        (dedupe_key, finding_type, post_uri, format_ts(now_utc())),
    )
