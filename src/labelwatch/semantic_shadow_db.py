"""Sidecar store for the semantic shadow (LW-JEV-SHADOW-v0, DESIGN §5.7).

`labelwatch_semantic_shadow.db` lives next to the main DB, following the
state.py precedent: own schema version, WAL + synchronous=NORMAL, meta
with build_status, an is_admissible() gate, and a disk-floor probe. The
main DB's label-record schema is not mutated and gains no tables.

Hard boundary (DESIGN §8): no post text is ever stored here — only
sha256 digests of URIs and texts. No account fields anywhere.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .utils import format_ts, stable_json

_log = logging.getLogger(__name__)

SIDECAR_SCHEMA_VERSION = 1
SIDECAR_FILENAME = "labelwatch_semantic_shadow.db"
RETENTION_DAYS = 30  # DESIGN §5.7, mirroring boundary.py retention

SIDECAR_SCHEMA = """
CREATE TABLE IF NOT EXISTS edge (
    edge_id TEXT PRIMARY KEY,          -- sha256(relation_type|uri_a|uri_b)
    relation_type TEXT NOT NULL,       -- reply | quote | repost
    uri_a_sha256 TEXT NOT NULL,
    uri_b_sha256 TEXT NOT NULL,
    text_a_sha256 TEXT,                -- digests only, never text (§5.3)
    text_b_sha256 TEXT,
    day TEXT NOT NULL,                 -- UTC day of B (sampling + retention)
    dt_seconds INTEGER,
    lexical_jaccard REAL,
    channel_structural INTEGER NOT NULL DEFAULT 1,
    channel_lexical INTEGER,           -- NULL until adjudicated
    channel_semantic INTEGER,          -- NULL when no semantic receipt (§5.8)
    channel_temporal INTEGER,
    shadow_qualified INTEGER,
    fragility_json TEXT,               -- leave-one-channel-out flags (§5.5)
    disposition TEXT NOT NULL,         -- sampled_pending | adjudicated |
                                       -- semantic_unavailable | budget_refused
    gate_state TEXT,                   -- §5.6 gate state for this edge
    semantic_argmax TEXT,              -- replicate-median argmax (§5.4 vocab)
    ambiguity_set_json TEXT,           -- §2.6 ambiguity set
    first_seen_ts TEXT NOT NULL,
    updated_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_semantic_shadow_edge_day ON edge(day);
CREATE INDEX IF NOT EXISTS idx_semantic_shadow_edge_a ON edge(uri_a_sha256, day);

CREATE TABLE IF NOT EXISTS calls (
    call_id TEXT PRIMARY KEY,
    ts TEXT NOT NULL,
    campaign TEXT NOT NULL,
    component TEXT NOT NULL,
    decision_family TEXT NOT NULL,
    question_id TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    model_requested TEXT NOT NULL,
    model_resolved TEXT,
    probabilities_json TEXT,
    argmax_recomputed TEXT,
    choice_returned TEXT,
    choice_argmax_discrepancy INTEGER,
    confidence_observed REAL,
    latency_s REAL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    est_cost_usd REAL,
    replicate_set_id TEXT,
    replicate_index INTEGER,
    status TEXT NOT NULL,
    http_status INTEGER,
    typesafe_request_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_semantic_shadow_calls_ts ON calls(ts);
CREATE INDEX IF NOT EXISTS idx_semantic_shadow_calls_question ON calls(question_id);

CREATE TABLE IF NOT EXISTS episode_summary (
    episode_id TEXT PRIMARY KEY,       -- sha256(episode|day|uri_a_sha256)
    day TEXT NOT NULL,
    root_uri_sha256 TEXT NOT NULL,     -- the shared A, digest only
    edge_count INTEGER NOT NULL,
    channel_pattern_counts_json TEXT NOT NULL,
    origin_digests_json TEXT NOT NULL, -- set-valued B-side uri digests
    updated_ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

META_BUILD_STATUS = "build_status"  # empty | active
META_SIDECAR_SCHEMA = "sidecar_schema_version"
# Own cursor key in the sidecar meta — never jetstream_discovery_cursor,
# which belongs to the discovery stream and the main DB (§5.2).
META_CURSOR = "jetstream_semantic_shadow_cursor"
META_LAST_MSG_AT = "jetstream_semantic_shadow_last_msg_at"


def default_sidecar_path(main_db_path: str) -> str:
    """The sidecar lives next to the main DB."""
    return os.path.join(
        os.path.dirname(os.path.abspath(main_db_path)), SIDECAR_FILENAME
    )


def init_sidecar(path: str) -> sqlite3.Connection:
    """Open or create the sidecar DB; ensure schema. Returns a writable conn."""
    new = not os.path.exists(path)
    resolved = os.path.realpath(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SIDECAR_SCHEMA)
    conn.commit()
    if new:
        _log.info("Created semantic shadow sidecar at %s (resolved=%s)", path, resolved)
        meta_set(conn, META_BUILD_STATUS, "active")
        meta_set(conn, META_SIDECAR_SCHEMA, SIDECAR_SCHEMA_VERSION)
        conn.commit()
        return conn
    prior = meta_get(conn, META_SIDECAR_SCHEMA)
    if prior is None or int(prior) != SIDECAR_SCHEMA_VERSION:
        conn.close()
        raise RuntimeError(
            f"semantic shadow sidecar at {resolved} has schema v{prior}; "
            f"code expects v{SIDECAR_SCHEMA_VERSION}. Drop the sidecar to "
            "rebuild (it is disposable shadow state, not testimony)."
        )
    _log.info("Opened semantic shadow sidecar at %s (resolved=%s, schema v%s)",
              path, resolved, prior)
    return conn


def meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def meta_set(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def is_admissible(conn: sqlite3.Connection) -> Tuple[bool, str]:
    """Reader gate (state.py precedent): a half-built or version-mismatched
    sidecar is not testimony."""
    version = meta_get(conn, META_SIDECAR_SCHEMA)
    if version is None or int(version) != SIDECAR_SCHEMA_VERSION:
        return False, f"sidecar_schema_version={version!r} (need {SIDECAR_SCHEMA_VERSION})"
    status = meta_get(conn, META_BUILD_STATUS)
    if status != "active":
        return False, f"build_status={status!r} (need 'active')"
    return True, "ok"


# --- safety probes ----------------------------------------------------------

def free_disk_gb(path: str) -> float:
    s = os.statvfs(os.path.dirname(os.path.abspath(path)))
    return (s.f_bavail * s.f_frsize) / (1024 ** 3)


# --- edge records -------------------------------------------------------------

def insert_edge(conn: sqlite3.Connection, fields: Dict[str, Any]) -> None:
    """Insert a sampled candidate edge (disposition sampled_pending)."""
    conn.execute(
        """
        INSERT INTO edge(
            edge_id, relation_type, uri_a_sha256, uri_b_sha256,
            text_a_sha256, text_b_sha256, day, dt_seconds, lexical_jaccard,
            channel_structural, disposition, first_seen_ts, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        """,
        (
            fields["edge_id"], fields["relation_type"],
            fields["uri_a_sha256"], fields["uri_b_sha256"],
            fields.get("text_a_sha256"), fields.get("text_b_sha256"),
            fields["day"], fields.get("dt_seconds"),
            fields.get("lexical_jaccard"),
            fields.get("disposition", "sampled_pending"),
            fields["first_seen_ts"], fields["updated_ts"],
        ),
    )


def finalize_edge(
    conn: sqlite3.Connection,
    edge_id: str,
    *,
    disposition: str,
    channels: Dict[str, Optional[bool]],
    shadow_qualified: Optional[bool],
    fragility: Optional[Dict[str, bool]],
    gate_state: Optional[str],
    semantic_argmax: Optional[str],
    ambiguity: Optional[List[str]],
    updated_ts: str,
) -> None:
    """Record the adjudication outcome for a sampled edge. On any Jev
    failure there is no semantic receipt: channel_semantic stays NULL and
    disposition is semantic_unavailable (§5.8)."""
    conn.execute(
        """
        UPDATE edge SET
            disposition = ?,
            channel_lexical = ?, channel_semantic = ?, channel_temporal = ?,
            shadow_qualified = ?, fragility_json = ?, gate_state = ?,
            semantic_argmax = ?, ambiguity_set_json = ?, updated_ts = ?
        WHERE edge_id = ?
        """,
        (
            disposition,
            _bool_or_none(channels.get("lexical")),
            _bool_or_none(channels.get("semantic")),
            _bool_or_none(channels.get("temporal")),
            _bool_or_none(shadow_qualified),
            stable_json(fragility) if fragility is not None else None,
            gate_state,
            semantic_argmax,
            stable_json(ambiguity) if ambiguity is not None else None,
            updated_ts,
            edge_id,
        ),
    )


def _bool_or_none(v: Optional[bool]) -> Optional[int]:
    if v is None:
        return None
    return 1 if v else 0


def get_edge(conn: sqlite3.Connection, edge_id: str) -> Optional[dict]:
    row = conn.execute("SELECT * FROM edge WHERE edge_id = ?", (edge_id,)).fetchone()
    return dict(row) if row else None


# --- per-call records (§2.5) ---------------------------------------------------

_CALL_COLUMNS = (
    "call_id", "ts", "campaign", "component", "decision_family",
    "question_id", "request_digest", "model_requested", "model_resolved",
    "probabilities_json", "argmax_recomputed", "choice_returned",
    "choice_argmax_discrepancy", "confidence_observed", "latency_s",
    "input_tokens", "output_tokens", "est_cost_usd", "replicate_set_id",
    "replicate_index", "status", "http_status", "typesafe_request_id",
)


def insert_call(conn: sqlite3.Connection, record: Dict[str, Any]) -> None:
    probs = record.get("probabilities")
    row = {
        **record,
        "probabilities_json": stable_json(probs) if probs is not None else None,
        "choice_argmax_discrepancy": _bool_or_none(
            record.get("choice_argmax_discrepancy")
        ),
    }
    conn.execute(
        f"INSERT INTO calls({', '.join(_CALL_COLUMNS)}) "
        f"VALUES ({', '.join('?' for _ in _CALL_COLUMNS)})",
        tuple(row.get(c) for c in _CALL_COLUMNS),
    )


def iter_calls(
    conn: sqlite3.Connection, day: Optional[str] = None
) -> Iterator[dict]:
    """Per-call records as §2.5-shaped dicts (probabilities unpacked)."""
    sql = "SELECT * FROM calls"
    params: tuple = ()
    if day:
        sql += " WHERE substr(ts, 1, 10) = ?"
        params = (day,)
    sql += " ORDER BY ts, call_id"
    for row in conn.execute(sql, params):
        rec = dict(row)
        probs = rec.pop("probabilities_json")
        rec["probabilities"] = json.loads(probs) if probs is not None else None
        rec["choice_argmax_discrepancy"] = bool(rec["choice_argmax_discrepancy"])
        yield rec


# --- client state (durable ClientState for semantic_shadow_jev) -----------------

class SidecarClientState:
    """Durable client state over sidecar meta/calls: daily call counter,
    daily model anchor + stop (§2.7), per-call record sink (§2.5)."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def calls_today(self, day: str) -> int:
        return int(meta_get(self.conn, f"calls:{day}") or 0)

    def set_calls_today(self, day: str, n: int) -> None:
        meta_set(self.conn, f"calls:{day}", n)
        self.conn.commit()

    def model_anchor(self, day: str) -> Optional[str]:
        return meta_get(self.conn, f"model_anchor:{day}")

    def set_model_anchor(self, day: str, model: str) -> None:
        meta_set(self.conn, f"model_anchor:{day}", model)
        self.conn.commit()

    def is_model_stopped(self, day: str) -> bool:
        return meta_get(self.conn, f"model_stopped:{day}") == "1"

    def set_model_stopped(self, day: str) -> None:
        meta_set(self.conn, f"model_stopped:{day}", "1")
        self.conn.commit()

    def record_call(self, record: dict) -> None:
        insert_call(self.conn, record)
        self.conn.commit()


# --- episode summaries (§5.5) ----------------------------------------------------

def upsert_episode_summary(
    conn: sqlite3.Connection,
    *,
    episode_id: str,
    day: str,
    root_uri_sha256: str,
    edge_count: int,
    pattern_counts: Dict[str, int],
    origin_digests: List[str],
    updated_ts: str,
) -> None:
    conn.execute(
        """
        INSERT INTO episode_summary(
            episode_id, day, root_uri_sha256, edge_count,
            channel_pattern_counts_json, origin_digests_json, updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(episode_id) DO UPDATE SET
            edge_count = excluded.edge_count,
            channel_pattern_counts_json = excluded.channel_pattern_counts_json,
            origin_digests_json = excluded.origin_digests_json,
            updated_ts = excluded.updated_ts
        """,
        (
            episode_id, day, root_uri_sha256, edge_count,
            stable_json(pattern_counts),
            stable_json(sorted(set(origin_digests))),
            updated_ts,
        ),
    )


# --- retention (§5.7; mirrors boundary.py:703-707) --------------------------------

def prune_old(
    conn: sqlite3.Connection, now: datetime, retention_days: int = RETENTION_DAYS
) -> Dict[str, int]:
    """Delete sidecar rows older than the 30-day retention period."""
    cutoff = format_ts(now - timedelta(days=retention_days))
    deleted = {}
    cur = conn.execute(
        "DELETE FROM edge WHERE first_seen_ts < ?", (cutoff,)
    )
    deleted["edge"] = cur.rowcount
    cur = conn.execute("DELETE FROM calls WHERE ts < ?", (cutoff,))
    deleted["calls"] = cur.rowcount
    cur = conn.execute(
        "DELETE FROM episode_summary WHERE updated_ts < ?", (cutoff,)
    )
    deleted["episode_summary"] = cur.rowcount
    conn.commit()
    return deleted


# --- status --------------------------------------------------------------------

def status_summary(conn: sqlite3.Connection, day: str) -> Dict[str, Any]:
    """Counts for the CLI status surface. Callers should consult
    is_admissible() before treating these as testimony."""
    ok, reason = is_admissible(conn)
    edges = conn.execute(
        "SELECT disposition, COUNT(*) AS c FROM edge GROUP BY disposition"
    ).fetchall()
    calls_today = conn.execute(
        "SELECT COUNT(*) AS c, COALESCE(SUM(input_tokens), 0) AS in_tok, "
        "COALESCE(SUM(output_tokens), 0) AS out_tok, "
        "COALESCE(SUM(est_cost_usd), 0) AS cost "
        "FROM calls WHERE substr(ts, 1, 10) = ?",
        (day,),
    ).fetchone()
    qualified = conn.execute(
        "SELECT COUNT(*) AS c FROM edge WHERE shadow_qualified = 1"
    ).fetchone()["c"]
    return {
        "admissible": ok,
        "admissible_reason": reason,
        "sidecar_schema_version": meta_get(conn, META_SIDECAR_SCHEMA),
        "build_status": meta_get(conn, META_BUILD_STATUS),
        "cursor": meta_get(conn, META_CURSOR),
        "last_msg_at": meta_get(conn, META_LAST_MSG_AT),
        "edges_by_disposition": {r["disposition"]: r["c"] for r in edges},
        "edges_shadow_qualified": qualified,
        "calls_today": calls_today["c"],
        "input_tokens_today": calls_today["in_tok"],
        "output_tokens_today": calls_today["out_tok"],
        "est_cost_usd_today": round(calls_today["cost"], 6),
        "model_anchor_today": meta_get(conn, f"model_anchor:{day}"),
        "model_stopped_today": meta_get(conn, f"model_stopped:{day}") == "1",
    }
