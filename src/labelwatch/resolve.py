"""Resolve ATProto DIDs to human-readable handles via the PLC directory."""
from __future__ import annotations

import json
import logging
import urllib.request
from typing import Optional

log = logging.getLogger(__name__)

PLC_DIRECTORY = "https://plc.directory"


def fetch_did_doc(did: str, timeout: int = 10) -> dict | None:
    """Fetch the full DID document from plc.directory.

    Returns the parsed JSON dict or None on failure.
    """
    url = f"{PLC_DIRECTORY}/{did}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        log.debug("Failed to fetch DID doc for %s", did, exc_info=True)
        return None


def resolve_handle(did: str, timeout: int = 10) -> Optional[str]:
    """Resolve a DID to its handle via plc.directory.

    Returns the handle (e.g. 'moderation.bsky.app') or None on failure.
    """
    data = fetch_did_doc(did, timeout=timeout)
    if data is None:
        return None
    also_known_as = data.get("alsoKnownAs", [])
    for aka in also_known_as:
        if aka.startswith("at://"):
            return aka[len("at://"):]
    return None


def resolve_service_endpoint(did_doc: dict) -> str | None:
    """Extract #atproto_labeler serviceEndpoint from DID document."""
    services = did_doc.get("service", [])
    for svc in services:
        if svc.get("id") == "#atproto_labeler" or svc.get("type") == "AtprotoLabeler":
            return svc.get("serviceEndpoint")
    return None


def resolve_label_key(did_doc: dict) -> bool:
    """Check for #atproto_label verification method in DID document."""
    methods = did_doc.get("verificationMethod", [])
    for method in methods:
        if method.get("id") == "#atproto_label":
            return True
        if method.get("type") == "Multikey" and "atproto_label" in str(method.get("id", "")):
            return True
    return False


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
