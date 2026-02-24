from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from . import db
from .config import Config
from .utils import format_ts, hash_sha256, now_utc, stable_json


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
    cid = raw.get("cid")
    neg = 1 if raw.get("neg") else 0
    exp = raw.get("exp")
    sig = raw.get("sig")
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


def fetch_labels(service_url: str, sources: List[str], cursor: Optional[str] = None, limit: int = 100) -> Dict:
    params = [("limit", str(limit))]
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


def ingest_from_service(conn, config: Config, limit: int = 100, max_pages: int = 10) -> int:
    total = 0
    source = _cursor_key(config)
    cursor = db.get_cursor(conn, source)
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
            db.upsert_labeler(conn, event.labeler_did, event.ts)
        total += db.insert_label_events(conn, rows)
        cursor = payload.get("cursor")
        # Persist cursor only after events are committed
        if cursor:
            db.set_cursor(conn, source, cursor)
        conn.commit()
        if not cursor:
            break
    return total


def ingest_from_iter(conn, items: Iterable[Dict]) -> int:
    rows = []
    total = 0
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
    if rows:
        total = db.insert_label_events(conn, rows)
        conn.commit()
    return total


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
