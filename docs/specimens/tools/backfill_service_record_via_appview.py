"""Bundle F: discover repair / provenance normalization.

Fetches a labeler's app.bsky.labeler.service record via the public
appview and inserts it into labelwatch.db's discovery_events table —
the normal discovery path — so the deriver's primary lookup
(discovery_events) resolves it instead of falling through to the
snapshot fallback.

This is a NARROW fix. It does not modify labelwatch's discover.py /
discovery_stream.py modules; it just gets the bytes into the right
table once. Auto-refresh / scheduled backstop is future work.

Idempotent: skips if an appview-backfill row already exists for the
given DID. To force a fresh backfill, delete the old row first.

Usage:
    python3 backfill_service_record_via_appview.py \\
        --db /var/lib/labelwatch/labelwatch.db \\
        --did did:plc:ar7c4by46qjdydhdevvrndac

Discipline:
  - The backfill row carries source='appview_backfill' so it's
    distinguishable from Jetstream-discovered rows
    (source='jetstream') or batch-discover rows.
  - commit_rev is synthetic ('appview-backfill-<utc-ts>'); we don't
    have the underlying PDS commit revision via appview.
  - No freshness_horizon is asserted; the appview view is "current as
    of fetch" without any validity guarantee. The deriver continues
    to classify this as unknown_basis per Bundle E discipline.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone


APPVIEW_URL = (
    "https://public.api.bsky.app/xrpc/app.bsky.labeler.getServices"
    "?dids={did}&detailed=true"
)


def fetch_appview_view(labeler_did: str) -> dict:
    url = APPVIEW_URL.format(did=labeler_did)
    with urllib.request.urlopen(url, timeout=30) as r:
        body = r.read()
    data = json.loads(body)
    views = data.get("views") or []
    if not views:
        raise SystemExit(
            f"appview returned no views for {labeler_did!r}; either the DID "
            "is not a labeler, the appview doesn't have it, or the network "
            "request failed silently."
        )
    return views[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="path to labelwatch.db")
    ap.add_argument("--did", required=True, help="labeler DID to backfill")
    ap.add_argument(
        "--force",
        action="store_true",
        help="re-insert even if an appview-backfill row already exists",
    )
    args = ap.parse_args()

    view = fetch_appview_view(args.did)
    policies = view.get("policies") or {}
    if not policies.get("labelValueDefinitions"):
        raise SystemExit(
            f"labeler {args.did!r} has no labelValueDefinitions in its "
            "appview policies block; nothing to ingest."
        )

    # Build the record_json so the deriver's existing query finds it:
    #   json_extract(record_json, '$.policies.labelValueDefinitions') IS NOT NULL
    record = {"policies": policies}
    record_json = json.dumps(record, separators=(",", ":"), sort_keys=True)
    record_sha256 = hashlib.sha256(record_json.encode("utf-8")).hexdigest()

    now = datetime.now(timezone.utc)
    commit_rev = f"appview-backfill-{now.strftime('%Y%m%dT%H%M%SZ')}"
    discovered_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    conn = sqlite3.connect(args.db)
    try:
        existing = conn.execute(
            "SELECT id, commit_rev, discovered_at FROM discovery_events "
            "WHERE labeler_did=? AND source='appview_backfill' "
            "ORDER BY discovered_at DESC",
            (args.did,),
        ).fetchall()
        if existing and not args.force:
            print(
                f"already backfilled: {len(existing)} appview row(s) for "
                f"{args.did}; most recent commit_rev={existing[0][1]} "
                f"discovered_at={existing[0][2]}. Use --force to re-insert.",
                file=sys.stderr,
            )
            return 0

        conn.execute(
            """INSERT INTO discovery_events
               (labeler_did, operation, source, commit_cid, commit_rev,
                record_json, record_sha256, resolved_endpoint, discovered_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                args.did,
                "create",
                "appview_backfill",
                None,
                commit_rev,
                record_json,
                record_sha256,
                None,
                discovered_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    print(
        f"backfilled {args.did}: commit_rev={commit_rev} "
        f"discovered_at={discovered_at} "
        f"labelValueDefinitions_count={len(policies['labelValueDefinitions'])}",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
