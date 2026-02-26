from __future__ import annotations

import json
import logging
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional
from uuid import uuid4

from . import db
from .config import Config
from .utils import format_ts, hash_sha256, now_utc, sqlite_safe_text, stable_json

log = logging.getLogger(__name__)

_VALID_DID_RE = re.compile(r"^did:(plc|web):[a-zA-Z0-9._:%-]{1,256}$")


def _classify_exception(exc: Exception) -> tuple[str, int | None]:
    """Classify an exception as 'timeout' or 'error', and extract http_status if available."""
    if isinstance(exc, socket.timeout):
        return "timeout", None
    # HTTPError is a subclass of URLError, so check it first
    if isinstance(exc, urllib.error.HTTPError):
        return "error", getattr(exc, "code", None)
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return "timeout", None
        return "error", None
    if isinstance(exc, TimeoutError):
        return "timeout", None
    return "error", None


def _is_valid_did(did: str) -> bool:
    """Validate DID shape: must be did:plc: or did:web: with reasonable length."""
    return bool(_VALID_DID_RE.match(did))


@dataclass
class LabelEvent:
    labeler_did: str
    src: Optional[str]
    uri: str
    cid: Optional[str]
    val: str
    neg: int
    exp: Optional[str]
    sig: Optional[str]
    ts: str
    event_hash: str


def normalize_label(raw: Dict) -> LabelEvent:
    labeler_did = raw.get("labeler_did") or raw.get("src")
    if not labeler_did:
        raise ValueError("labeler_did or src required")
    src = raw.get("src")
    uri = raw.get("uri")
    val = raw.get("val")
    if not uri or not val:
        raise ValueError("uri and val required")
    cid = sqlite_safe_text(raw.get("cid"))
    neg = 1 if raw.get("neg") else 0
    exp = sqlite_safe_text(raw.get("exp"))
    sig_raw = raw.get("sig")
    if isinstance(sig_raw, dict):
        sig = sig_raw.get("$bytes")
    else:
        sig = sig_raw
    sig = sqlite_safe_text(sig)
    if sig_raw is not None and not isinstance(sig_raw, (str, dict, type(None))):
        log.info("Coerced sig type=%s for %s: %.80r", type(sig_raw).__name__, labeler_did, sig_raw)
    ts = raw.get("ts") or format_ts(now_utc())
    canonical = {
        "labeler_did": labeler_did,
        "src": src,
        "uri": uri,
        "cid": cid,
        "val": val,
        "neg": neg,
        "exp": exp,
        "sig": sig,
        "ts": ts,
    }
    event_hash = hash_sha256(stable_json(canonical))
    return LabelEvent(
        labeler_did=labeler_did,
        src=src,
        uri=uri,
        cid=cid,
        val=val,
        neg=neg,
        exp=exp,
        sig=sig,
        ts=ts,
        event_hash=event_hash,
    )


def fetch_labels(service_url: str, sources: List[str], cursor: Optional[str] = None, limit: int = 100,
                  uri_patterns: Optional[List[str]] = None) -> Dict:
    params = [("limit", str(limit))]
    for pat in (uri_patterns or ["*"]):
        params.append(("uriPatterns", pat))
    for src in sources:
        params.append(("sources", src))
    if cursor:
        params.append(("cursor", cursor))
    query = urllib.parse.urlencode(params)
    url = f"{service_url.rstrip('/')}/xrpc/com.atproto.label.queryLabels?{query}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data


def _cursor_key(config: Config) -> str:
    return config.service_url.rstrip("/")


def _track_observed_src(conn, src_did: str, ts: str, evidence_seen: set) -> None:
    """Track observed label src DID: create observed_only labeler or update sticky flag."""
    if not src_did or not _is_valid_did(src_did):
        return

    existing = conn.execute(
        "SELECT observed_as_src, visibility_class FROM labelers WHERE labeler_did=?", (src_did,)
    ).fetchone()

    if existing is None:
        # Unknown DID — create as observed_only
        conn.execute(
            """
            INSERT INTO labelers(labeler_did, first_seen, last_seen,
                                 visibility_class, reachability_state, observed_as_src,
                                 classification_reason)
            VALUES(?, ?, ?, 'observed_only', 'unknown', 1, 'observed_label_src')
            """,
            (src_did, ts, ts),
        )
    else:
        # Known DID — update sticky observed_as_src, and upgrade visibility if unresolved
        updates = ["observed_as_src = 1"]
        if existing["visibility_class"] == "unresolved":
            updates.append("visibility_class = 'observed_only'")
            updates.append("classification_reason = 'observed_label_src'")
        conn.execute(
            f"UPDATE labelers SET {', '.join(updates)} WHERE labeler_did = ?",
            (src_did,),
        )

    # Insert evidence (dedupe within this ingest run)
    ev_key = (src_did, "observed_label_src")
    if ev_key not in evidence_seen:
        db.insert_evidence(conn, src_did, "observed_label_src", "true", ts, "ingest")
        evidence_seen.add(ev_key)


def ingest_from_service(conn, config: Config, limit: int = 100, max_pages: int = 10) -> int:
    total = 0
    source = _cursor_key(config)
    cursor = db.get_cursor(conn, source)
    evidence_seen: set = set()
    attempt_id = uuid4().hex
    ts_now = format_ts(now_utc())
    seen_dids: set = set()
    t0 = time.monotonic()

    try:
        for _ in range(max_pages):
            payload = fetch_labels(config.service_url, config.labeler_dids, cursor=cursor, limit=limit)
            labels = payload.get("labels", [])
            if not labels:
                break
            rows = []
            for raw in labels:
                event = normalize_label(raw)
                rows.append(
                    (
                        event.labeler_did,
                        event.src,
                        event.uri,
                        event.cid,
                        event.val,
                        event.neg,
                        event.exp,
                        event.sig,
                        event.ts,
                        event.event_hash,
                    )
                )
                seen_dids.add(event.labeler_did)
                db.upsert_labeler(conn, event.labeler_did, event.ts)
                # Track observed src DID
                src_did = event.src or event.labeler_did
                _track_observed_src(conn, src_did, event.ts, evidence_seen)
            total += db.insert_label_events(conn, rows)
            cursor = payload.get("cursor")
            # Persist cursor only after events are committed
            if cursor:
                db.set_cursor(conn, source, cursor)
            conn.commit()
            if not cursor:
                break
    except Exception as exc:
        latency_ms = int((time.monotonic() - t0) * 1000)
        outcome, http_status = _classify_exception(exc)
        error_type = type(exc).__name__
        error_summary = str(exc)[:200]
        for did in config.labeler_dids:
            db.insert_ingest_outcome(
                conn, did, ts_now, attempt_id, outcome, 0,
                http_status, latency_ms, error_type, error_summary, "service",
            )
        conn.commit()
        raise

    latency_ms = int((time.monotonic() - t0) * 1000)
    # Record outcomes per configured DID
    for did in config.labeler_dids:
        if did in seen_dids:
            db.insert_ingest_outcome(
                conn, did, ts_now, attempt_id, "success", total,
                None, latency_ms, None, None, "service",
            )
        else:
            db.insert_ingest_outcome(
                conn, did, ts_now, attempt_id, "partial", 0,
                None, latency_ms, None, None, "service",
            )
    conn.commit()
    return total


def ingest_from_iter(conn, items: Iterable[Dict]) -> int:
    rows = []
    total = 0
    evidence_seen: set = set()
    for raw in items:
        event = normalize_label(raw)
        rows.append(
            (
                event.labeler_did,
                event.src,
                event.uri,
                event.cid,
                event.val,
                event.neg,
                event.exp,
                event.sig,
                event.ts,
                event.event_hash,
            )
        )
        db.upsert_labeler(conn, event.labeler_did, event.ts)
        src_did = event.src or event.labeler_did
        _track_observed_src(conn, src_did, event.ts, evidence_seen)
    if rows:
        total = db.insert_label_events(conn, rows)
        conn.commit()
    return total


def ingest_multi(conn, config: Config, timeout: int | None = None,
                  budget: int | None = None, max_pages: int | None = None) -> Dict[str, int]:
    """Ingest from all accessible labeler endpoints.

    Each labeler gets its own cursor keyed by DID. Failures are logged
    and skipped. Respects a time budget to avoid blocking the main loop.

    Returns {did: count_inserted, ...}.
    """
    if timeout is None:
        timeout = config.multi_ingest_timeout
    if budget is None:
        budget = config.multi_ingest_budget
    if max_pages is None:
        max_pages = config.multi_ingest_max_pages

    rows = conn.execute(
        "SELECT labeler_did, service_endpoint FROM labelers WHERE endpoint_status='accessible'"
    ).fetchall()

    results: Dict[str, int] = {}
    start_time = time.monotonic()
    attempt_id = uuid4().hex
    ts_now = format_ts(now_utc())

    for row in rows:
        if time.monotonic() - start_time > budget:
            log.info("Multi-ingest budget exhausted after %ds", budget)
            break

        did = row["labeler_did"]
        endpoint = row["service_endpoint"]
        if not endpoint:
            continue

        cursor_key = did
        cursor = db.get_cursor(conn, cursor_key)
        total = 0
        evidence_seen: set = set()
        t0 = time.monotonic()

        try:
            for _ in range(max_pages):
                payload = fetch_labels(endpoint, [did], cursor=cursor, limit=100)
                labels = payload.get("labels", [])
                if not labels:
                    break
                event_rows = []
                for raw in labels:
                    event = normalize_label(raw)
                    event_rows.append((
                        event.labeler_did, event.src, event.uri, event.cid,
                        event.val, event.neg, event.exp, event.sig,
                        event.ts, event.event_hash,
                    ))
                    db.upsert_labeler(conn, event.labeler_did, event.ts)
                    src_did = event.src or event.labeler_did
                    _track_observed_src(conn, src_did, event.ts, evidence_seen)
                total += db.insert_label_events(conn, event_rows)
                cursor = payload.get("cursor")
                if cursor:
                    db.set_cursor(conn, cursor_key, cursor)
                conn.commit()
                if not cursor:
                    break

            latency_ms = int((time.monotonic() - t0) * 1000)
            outcome = "success" if total > 0 else "empty"
            db.insert_ingest_outcome(
                conn, did, ts_now, attempt_id, outcome, total,
                None, latency_ms, None, None, "multi",
            )
            conn.commit()
        except Exception as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            outcome, http_status = _classify_exception(exc)
            error_type = type(exc).__name__
            error_summary = str(exc)[:200]
            db.insert_ingest_outcome(
                conn, did, ts_now, attempt_id, outcome, 0,
                http_status, latency_ms, error_type, error_summary, "multi",
            )
            conn.commit()
            log.warning("Multi-ingest failed for %s at %s", did, endpoint, exc_info=True)

        results[did] = total
        if total > 0:
            log.info("Multi-ingest %s: %d events", did, total)

    return results


def ingest_from_fixture(conn, path: str) -> int:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "label" in obj:
                obj = obj["label"]
            items.append(obj)
    return ingest_from_iter(conn, items)
