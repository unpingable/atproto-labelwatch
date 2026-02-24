"""Resolve ATProto DIDs to human-readable handles via the PLC directory."""
from __future__ import annotations

import json
import logging
import urllib.request
from typing import Optional

log = logging.getLogger(__name__)

PLC_DIRECTORY = "https://plc.directory"


def resolve_handle(did: str, timeout: int = 10) -> Optional[str]:
    """Resolve a DID to its handle via plc.directory.

    Returns the handle (e.g. 'moderation.bsky.app') or None on failure.
    """
    url = f"{PLC_DIRECTORY}/{did}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        log.debug("Failed to resolve %s", did, exc_info=True)
        return None
    also_known_as = data.get("alsoKnownAs", [])
    for aka in also_known_as:
        if aka.startswith("at://"):
            return aka[len("at://"):]
    return None


def resolve_handles_for_labelers(conn, timeout: int = 10) -> int:
    """Resolve handles for all labelers that don't have one yet.

    Returns the number of newly resolved handles.
    """
    rows = conn.execute(
        "SELECT labeler_did FROM labelers WHERE handle IS NULL OR handle = ''"
    ).fetchall()
    resolved = 0
    for row in rows:
        did = row["labeler_did"]
        handle = resolve_handle(did, timeout=timeout)
        if handle:
            conn.execute("UPDATE labelers SET handle=? WHERE labeler_did=?", (handle, did))
            resolved += 1
            log.info("Resolved %s -> %s", did, handle)
    if resolved:
        conn.commit()
    return resolved
