"""Labeler discovery: enumerate, hydrate, and probe ATProto labelers."""
from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from . import db
from .classify import EvidenceDict, classify_labeler, detect_test_dev
from .config import Config
from .resolve import fetch_did_doc, resolve_label_key, resolve_service_endpoint
from .utils import format_ts, now_utc

log = logging.getLogger(__name__)

REPO_LIST_URL = "https://bsky.network/xrpc/com.atproto.sync.listReposByCollection"
LABELER_SERVICES_URL = "https://public.api.bsky.app/xrpc/app.bsky.labeler.getServices"


@dataclass
class ProbeResult:
    normalized_status: str  # accessible / auth_required / down
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None
    failure_type: Optional[str] = None
    error: Optional[str] = None


def list_labeler_dids(max_pages: int = 50, timeout: int = 30) -> List[str]:
    """Enumerate all labeler DIDs via listReposByCollection.

    Paginates through com.atproto.sync.listReposByCollection with
    collection=app.bsky.labeler.service.
    """
    dids: List[str] = []
    cursor: Optional[str] = None
    for _ in range(max_pages):
        params = [("collection", "app.bsky.labeler.service"), ("limit", "500")]
        if cursor:
            params.append(("cursor", cursor))
        query = urllib.parse.urlencode(params)
        url = f"{REPO_LIST_URL}?{query}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            log.warning("Failed to fetch labeler repos page", exc_info=True)
            break
        repos = data.get("repos", [])
        for repo in repos:
            did = repo.get("did")
            if did:
                dids.append(did)
        cursor = data.get("cursor")
        if not cursor or not repos:
            break
    return dids


def _fetch_did_info(did: str, timeout: int = 10) -> Tuple[str, Optional[dict], Optional[str], Optional[str], bool]:
    """Fetch DID doc and extract handle + service endpoint + label key. Thread-safe (no DB)."""
    doc = fetch_did_doc(did, timeout=timeout)
    if doc is None:
        return did, None, None, None, False
    ep = resolve_service_endpoint(doc)
    has_label_key = resolve_label_key(doc)
    handle = None
    for aka in doc.get("alsoKnownAs", []):
        if aka.startswith("at://"):
            handle = aka[len("at://"):]
            break
    return did, doc, handle, ep, has_label_key


def hydrate_labelers(dids: List[str], timeout: int = 15) -> Dict[str, dict]:
    """Batch-fetch display names via app.bsky.labeler.getServices.

    Returns {did: {"display_name": str|None}} for each DID.
    """
    result: Dict[str, dict] = {}
    batch_size = 25
    for i in range(0, len(dids), batch_size):
        batch = dids[i : i + batch_size]
        params = [("detailed", "true")]
        for did in batch:
            params.append(("dids", did))
        query = urllib.parse.urlencode(params)
        url = f"{LABELER_SERVICES_URL}?{query}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            log.warning("Failed to hydrate labeler batch starting at %d", i, exc_info=True)
            for did in batch:
                result.setdefault(did, {"display_name": None})
            continue
        views = data.get("views", [])
        seen = set()
        for view in views:
            did = view.get("creator", {}).get("did")
            if did:
                seen.add(did)
                result[did] = {"display_name": view.get("creator", {}).get("displayName")}
        for did in batch:
            if did not in seen:
                result.setdefault(did, {"display_name": None})
    return result


def probe_endpoint(endpoint_url: str, did: str, timeout: int = 10) -> ProbeResult:
    """Probe a labeler's queryLabels endpoint.

    Returns a ProbeResult with normalized_status, http_status, latency, and failure info.
    """
    params = urllib.parse.urlencode([
        ("uriPatterns", "*"),
        ("sources", did),
        ("limit", "1"),
    ])
    url = f"{endpoint_url.rstrip('/')}/xrpc/com.atproto.label.queryLabels?{params}"
    t0 = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency = int((time.monotonic() - t0) * 1000)
            status = resp.status
            if 200 <= status < 300:
                return ProbeResult("accessible", http_status=status, latency_ms=latency)
            return ProbeResult("down", http_status=status, latency_ms=latency)
    except urllib.error.HTTPError as e:
        latency = int((time.monotonic() - t0) * 1000)
        if e.code in (401, 403):
            return ProbeResult("auth_required", http_status=e.code, latency_ms=latency, failure_type=f"http_{e.code // 100}xx")
        failure_type = f"http_{e.code // 100}xx" if e.code else None
        return ProbeResult("down", http_status=e.code, latency_ms=latency, failure_type=failure_type, error=str(e))
    except OSError as e:
        latency = int((time.monotonic() - t0) * 1000)
        error_str = str(e).lower()
        if "timed out" in error_str or "timeout" in error_str:
            failure_type = "timeout"
        elif "name or service not known" in error_str or "getaddrinfo" in error_str:
            failure_type = "dns_error"
        elif "ssl" in error_str or "certificate" in error_str:
            failure_type = "tls_error"
        elif "connection refused" in error_str:
            failure_type = "connection_refused"
        else:
            failure_type = "connection_refused"
        return ProbeResult("down", latency_ms=latency, failure_type=failure_type, error=str(e))
    except Exception as e:
        latency = int((time.monotonic() - t0) * 1000)
        return ProbeResult("down", latency_ms=latency, failure_type="connection_refused", error=str(e))


def _probe_with_host_limit(did: str, endpoint: str, host_slots: Dict[str, int],
                           max_per_host: int, timeout: int) -> Tuple[str, ProbeResult]:
    """Probe an endpoint, respecting per-host concurrency by brief sleep if busy.

    Thread-safe (no DB). Returns (did, ProbeResult).
    """
    host = urlparse(endpoint).hostname or ""
    # Simple per-host backoff: if this host already has active probes, sleep briefly
    current = host_slots.get(host, 0)
    if current >= max_per_host:
        time.sleep(0.5)
    host_slots[host] = host_slots.get(host, 0) + 1
    try:
        return did, probe_endpoint(endpoint, did, timeout=timeout)
    finally:
        host_slots[host] = max(0, host_slots.get(host, 0) - 1)


def _classify_labeler(did: str, config: Config) -> tuple[str, int]:
    """Return (labeler_class, is_reference) for a DID."""
    if did in config.reference_dids:
        return "official_platform", 1
    return "third_party", 0


def run_discovery(conn, config: Config, did_workers: int = 10,
                  probe_workers: int = 5, probe_timeout: int = 8,
                  max_per_host: int = 2) -> dict:
    """Full discovery pipeline: enumerate, hydrate, probe, classify, upsert.

    Phases:
      1. Enumerate DIDs (serial, paginated)
      2. Resolve DID docs (parallel, did_workers threads)
      3. Hydrate display names (serial, already batched 25/request)
      4. Probe endpoints (parallel, probe_workers threads, per-host limited)
      5. Upsert to DB with classification (serial, main thread)

    Returns summary dict with counts.
    """
    log.info("Starting labeler discovery")
    t0 = time.monotonic()

    # Phase 1: Enumerate
    dids = list_labeler_dids()
    log.info("Discovered %d labeler DIDs (%.1fs)", len(dids), time.monotonic() - t0)
    if not dids:
        return {"discovered": 0, "accessible": 0, "auth_required": 0, "down": 0, "no_endpoint": 0}

    # Phase 2: Resolve DID docs in parallel
    t1 = time.monotonic()
    did_docs: Dict[str, dict] = {}
    endpoints: Dict[str, str] = {}
    handles: Dict[str, str] = {}
    label_keys: Dict[str, bool] = {}

    with ThreadPoolExecutor(max_workers=did_workers) as pool:
        futures = {pool.submit(_fetch_did_info, did): did for did in dids}
        for future in as_completed(futures):
            try:
                did, doc, handle, ep, has_label_key = future.result()
            except Exception:
                log.debug("DID doc fetch failed for %s", futures[future], exc_info=True)
                continue
            if doc is not None:
                did_docs[did] = doc
            if handle:
                handles[did] = handle
            if ep:
                endpoints[did] = ep
            label_keys[did] = has_label_key

    log.info("Resolved %d DID docs, %d endpoints (%.1fs)",
             len(did_docs), len(endpoints), time.monotonic() - t1)

    # Phase 3: Hydrate display names (already batched, keep serial)
    t2 = time.monotonic()
    hydration = hydrate_labelers(dids)
    log.info("Hydrated display names (%.1fs)", time.monotonic() - t2)

    # Phase 4: Probe endpoints in parallel with per-host limiting
    t3 = time.monotonic()
    probe_results: Dict[str, ProbeResult] = {}
    host_slots: Dict[str, int] = defaultdict(int)

    with ThreadPoolExecutor(max_workers=probe_workers) as pool:
        futures = {}
        for did, ep in endpoints.items():
            f = pool.submit(_probe_with_host_limit, did, ep, host_slots,
                            max_per_host, probe_timeout)
            futures[f] = did
        for future in as_completed(futures):
            try:
                did, result = future.result()
                probe_results[did] = result
            except Exception:
                probe_results[futures[future]] = ProbeResult("down", failure_type="connection_refused")
                log.debug("Probe failed for %s", futures[future], exc_info=True)

    log.info("Probed %d endpoints (%.1fs)", len(probe_results), time.monotonic() - t3)

    # Phase 5: Upsert into DB with classification (main thread only)
    seen_ts = format_ts(now_utc())
    summary = {"discovered": len(dids), "accessible": 0, "auth_required": 0, "down": 0, "no_endpoint": 0}

    # Track evidence already inserted this run for dedupe
    evidence_seen: set = set()

    for did in dids:
        labeler_class, is_reference = _classify_labeler(did, config)
        display_name = hydration.get(did, {}).get("display_name")
        handle = handles.get(did)
        endpoint = endpoints.get(did)
        probe = probe_results.get(did)
        status = probe.normalized_status if probe else "unknown"
        has_service = did in endpoints
        has_lk = label_keys.get(did, False)

        if did not in endpoints:
            status = "unknown"
            summary["no_endpoint"] += 1
        elif status == "accessible":
            summary["accessible"] += 1
        elif status == "auth_required":
            summary["auth_required"] += 1
        else:
            summary["down"] += 1

        upsert_discovered_labeler(
            conn, did,
            handle=handle,
            display_name=display_name,
            endpoint=endpoint,
            has_service=has_service,
            has_label_key=has_lk,
            declared_record=True,
            probe=probe,
            labeler_class=labeler_class,
            is_reference=is_reference,
            test_dev=detect_test_dev(handle, display_name) if config.noise_policy_enabled else False,
            seen_ts=seen_ts,
            evidence_source="discovery",
            evidence_seen=evidence_seen,
        )
    conn.commit()

    # Mark reference DIDs even if not discovered via enumeration
    for ref_did in config.reference_dids:
        conn.execute(
            """
            UPDATE labelers SET is_reference=1, labeler_class='official_platform'
            WHERE labeler_did=?
            """,
            (ref_did,),
        )
    conn.commit()

    db.set_meta(conn, "last_discovery_ts", format_ts(now_utc()))
    conn.commit()

    elapsed = time.monotonic() - t0
    summary["elapsed_seconds"] = round(elapsed, 1)
    log.info("Discovery complete in %.1fs: %s", elapsed, summary)
    return summary


def upsert_discovered_labeler(
    conn,
    did: str,
    *,
    handle: Optional[str] = None,
    display_name: Optional[str] = None,
    endpoint: Optional[str] = None,
    has_service: bool = False,
    has_label_key: bool = False,
    declared_record: bool = False,
    probe: Optional[ProbeResult] = None,
    labeler_class: str = "third_party",
    is_reference: int = 0,
    test_dev: bool = False,
    seen_ts: Optional[str] = None,
    evidence_source: str = "discovery",
    evidence_seen: Optional[set] = None,
) -> None:
    """Shared labeler upsert with sticky fields and evidence logging.

    Used by batch discovery, Jetstream stream discovery, and backstop.
    """
    if seen_ts is None:
        seen_ts = format_ts(now_utc())
    if evidence_seen is None:
        evidence_seen = set()

    status = probe.normalized_status if probe else "unknown"

    # Check existing row for sticky fields
    existing = conn.execute(
        "SELECT observed_as_src, has_labeler_service, has_label_key, declared_record FROM labelers WHERE labeler_did=?",
        (did,),
    ).fetchone()
    existing_observed_src = existing["observed_as_src"] if existing else 0
    existing_has_service = existing["has_labeler_service"] if existing else 0
    existing_has_lk = existing["has_label_key"] if existing else 0

    # Build evidence for classifier
    evidence = EvidenceDict(
        declared_record_present=declared_record,
        did_doc_labeler_service_present=has_service or bool(existing_has_service),
        did_doc_label_key_present=has_label_key or bool(existing_has_lk),
        observed_label_src=bool(existing_observed_src),
        probe_result=status if status != "unknown" else None,
    )

    classification = classify_labeler(evidence)

    # Write evidence records (dedupe within this run)
    if declared_record:
        ev_key = (did, "declared_record", "true")
        if ev_key not in evidence_seen:
            db.insert_evidence(conn, did, "declared_record", "true", seen_ts, evidence_source)
            evidence_seen.add(ev_key)

    if has_service:
        ev_key = (did, "did_doc_labeler_service", endpoint)
        if ev_key not in evidence_seen:
            db.insert_evidence(conn, did, "did_doc_labeler_service", endpoint, seen_ts, evidence_source)
            evidence_seen.add(ev_key)

    if has_label_key:
        ev_key = (did, "did_doc_label_key", "true")
        if ev_key not in evidence_seen:
            db.insert_evidence(conn, did, "did_doc_label_key", "true", seen_ts, evidence_source)
            evidence_seen.add(ev_key)

    if probe:
        ev_key = (did, "probe_result", probe.normalized_status)
        if ev_key not in evidence_seen:
            db.insert_evidence(conn, did, "probe_result", probe.normalized_status, seen_ts, evidence_source)
            evidence_seen.add(ev_key)
        db.insert_probe_history(
            conn, did, seen_ts, endpoint or "",
            probe.http_status, probe.normalized_status,
            probe.latency_ms, probe.failure_type, probe.error,
        )

    # Sticky fields: only upgrade, never downgrade
    sticky_has_service = max(int(has_service), int(existing_has_service))
    sticky_has_lk = max(int(has_label_key), int(existing_has_lk))
    sticky_observed_src = int(existing_observed_src)

    conn.execute(
        """
        INSERT INTO labelers(labeler_did, handle, display_name, service_endpoint,
                             labeler_class, is_reference, endpoint_status, last_probed,
                             first_seen, last_seen,
                             visibility_class, reachability_state,
                             classification_confidence, classification_reason,
                             classification_version, classified_at, auditability,
                             observed_as_src, has_labeler_service, has_label_key,
                             declared_record, likely_test_dev)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(labeler_did) DO UPDATE SET
            handle=COALESCE(excluded.handle, labelers.handle),
            display_name=COALESCE(excluded.display_name, labelers.display_name),
            service_endpoint=COALESCE(excluded.service_endpoint, labelers.service_endpoint),
            labeler_class=excluded.labeler_class,
            is_reference=excluded.is_reference,
            endpoint_status=excluded.endpoint_status,
            last_probed=excluded.last_probed,
            last_seen=excluded.last_seen,
            visibility_class=excluded.visibility_class,
            reachability_state=excluded.reachability_state,
            classification_confidence=excluded.classification_confidence,
            classification_reason=excluded.classification_reason,
            classification_version=excluded.classification_version,
            classified_at=excluded.classified_at,
            auditability=excluded.auditability,
            observed_as_src=MAX(labelers.observed_as_src, excluded.observed_as_src),
            has_labeler_service=MAX(labelers.has_labeler_service, excluded.has_labeler_service),
            has_label_key=MAX(labelers.has_label_key, excluded.has_label_key),
            declared_record=MAX(labelers.declared_record, excluded.declared_record),
            likely_test_dev=excluded.likely_test_dev
        """,
        (did, handle, display_name, endpoint,
         labeler_class, is_reference, status, seen_ts,
         seen_ts, seen_ts,
         classification.visibility_class, classification.reachability_state,
         classification.classification_confidence, classification.reason,
         classification.version, seen_ts, classification.auditability,
         sticky_observed_src, sticky_has_service, sticky_has_lk,
         int(declared_record), int(test_dev)),
    )


LABELER_LISTS_ACTOR = "labeler-lists.bsky.social"
BSKY_PUBLIC_API = "https://public.api.bsky.app/xrpc"


def backstop_from_lists(conn, timeout: int = 10, max_members: int = 500) -> dict:
    """Scrape labeler-lists.bsky.social as belt-and-suspenders discovery.

    Returns summary dict with counts.
    """
    seen_ts = format_ts(now_utc())
    summary = {"lists_fetched": 0, "members_seen": 0, "new_labelers": 0, "errors": 0}
    deadline = time.monotonic() + 600  # 10 minute budget

    # Fetch lists for the actor
    try:
        url = f"{BSKY_PUBLIC_API}/app.bsky.graph.getLists?actor={LABELER_LISTS_ACTOR}&limit=50"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        log.warning("Failed to fetch lists for %s", LABELER_LISTS_ACTOR, exc_info=True)
        summary["errors"] += 1
        return summary

    lists = data.get("lists", [])
    known_dids = {r["labeler_did"] for r in conn.execute("SELECT labeler_did FROM labelers").fetchall()}
    evidence_seen: set = set()
    total_members = 0

    for lst in lists:
        if time.monotonic() > deadline:
            log.warning("Backstop hit 10min budget, stopping")
            break

        list_uri = lst.get("uri")
        if not list_uri:
            continue

        summary["lists_fetched"] += 1
        cursor = None

        while total_members < max_members:
            if time.monotonic() > deadline:
                break
            try:
                list_url = (
                    f"{BSKY_PUBLIC_API}/app.bsky.graph.getList"
                    f"?list={urllib.parse.quote(list_uri, safe='')}&limit=100"
                )
                if cursor:
                    list_url += f"&cursor={urllib.parse.quote(cursor, safe='')}"
                req = urllib.request.Request(list_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    list_data = json.loads(resp.read().decode("utf-8"))
            except Exception:
                log.debug("Failed to fetch list %s", list_uri, exc_info=True)
                summary["errors"] += 1
                break

            items = list_data.get("items", [])
            if not items:
                break

            for item in items:
                total_members += 1
                subject = item.get("subject", {})
                member_did = subject.get("did")
                if not member_did:
                    continue
                summary["members_seen"] += 1

                if member_did in known_dids:
                    continue

                # New DID — resolve and upsert
                did_doc = fetch_did_doc(member_did, timeout=timeout)
                if did_doc is None:
                    continue

                endpoint = resolve_service_endpoint(did_doc)
                has_lk = resolve_label_key(did_doc)
                handle = None
                for aka in did_doc.get("alsoKnownAs", []):
                    if aka.startswith("at://"):
                        handle = aka[len("at://"):]
                        break

                upsert_discovered_labeler(
                    conn, member_did,
                    handle=handle,
                    endpoint=endpoint,
                    has_service=endpoint is not None,
                    has_label_key=has_lk,
                    declared_record=False,
                    seen_ts=seen_ts,
                    evidence_source="backstop",
                    evidence_seen=evidence_seen,
                )
                db.insert_discovery_event(
                    conn, member_did, "create", "backstop",
                    discovered_at=seen_ts,
                    resolved_endpoint=endpoint,
                )
                conn.commit()
                known_dids.add(member_did)
                summary["new_labelers"] += 1
                log.info("BACKSTOP discovered %s handle=%s", member_did, handle)

            cursor = list_data.get("cursor")
            if not cursor:
                break

    log.info("Backstop complete: %s", summary)
    return summary
