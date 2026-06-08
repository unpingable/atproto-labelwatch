"""Operator-maturity scan (analysis A1 — labeler-operator-maturity-001).

Treats labelers like services. For each labeler with either recent
events or a published service record, produces one row:

  labeler_did | handle | display_name | labeler_class | active_recently
  declares_scope | explains_labels | has_contact_or_appeal_path
  has_stable_service_record | label_count_30d | distinct_targets_30d
  user_visible_consequence_known | maturity_class | notes

This is NOT admissibility apparatus. It is SRE-style service-readiness
profiling. "Would I trust this thing enough to expose it to users?"

Heuristic flag definitions:

  active_recently               events_30d > 0
  declares_scope                latest service record has >= 1 labelValueDefinition
  explains_labels               at least one labelValueDefinition has a non-empty
                                locale name/description
  has_contact_or_appeal_path    UNKNOWN in v1 (requires appview profile fetch +
                                  manual classification of description text)
  has_stable_service_record     1 <= service_record_revisions <= 5 in scan window
                                  (more = churning; 0 = no record)
  label_count_30d               events_30d
  distinct_targets_30d          unique_targets_30d
  user_visible_consequence_known
                                latest_label_def_count > 0 AND at least one
                                  declared label has a defaultSetting in
                                  {hide, warn, ignore} (consumer knows what to do)

  maturity_class — heuristic, NOT measurement:
    platform-root             — did:plc:ar7c4by46qjdydhdevvrndac (mod.bsky)
    abandoned                 — had service record, events_30d = 0
    experimental              — sparse activity (events_30d in [1, 10))
                                  or likely_test_dev=1
    personal/reputational     — events_30d in [10, 100); declares some scope
    community-service         — events_30d in [100, 10000); declares scope
                                  with explanations
    moderation-infrastructure — events_30d >= 10000 with declared scope
    unknown                   — insufficient signal (no record, no events,
                                  or contradictory signals)

Usage:
    sudo -u labelwatch python3 operator_maturity_scan.py \\
        --db /var/lib/labelwatch/labelwatch.db \\
        --out /tmp/operator_maturity.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone

PLATFORM_ROOT_DID = "did:plc:ar7c4by46qjdydhdevvrndac"  # moderation.bsky.app

SCAN_SQL = """
WITH latest_record AS (
  SELECT
    de.labeler_did,
    de.record_json,
    de.discovered_at,
    ROW_NUMBER() OVER (
      PARTITION BY de.labeler_did
      ORDER BY de.discovered_at DESC
    ) AS rn
  FROM discovery_events de
  WHERE de.operation IN ('create','update')
    AND json_extract(de.record_json,'$.policies.labelValueDefinitions') IS NOT NULL
),
service_record_stats AS (
  SELECT
    labeler_did,
    COUNT(*) AS service_record_revisions,
    MIN(discovered_at) AS first_record_at,
    MAX(discovered_at) AS last_record_at
  FROM discovery_events
  WHERE operation IN ('create','update')
    AND json_extract(record_json,'$.policies.labelValueDefinitions') IS NOT NULL
  GROUP BY labeler_did
)
SELECT
  l.labeler_did,
  COALESCE(l.handle, '') AS handle,
  COALESCE(l.display_name, '') AS display_name,
  COALESCE(l.labeler_class, 'third_party') AS labeler_class,
  l.is_reference,
  COALESCE(l.events_7d, 0) AS events_7d,
  COALESCE(l.events_30d, 0) AS events_30d,
  COALESCE(l.unique_targets_30d, 0) AS unique_targets_30d,
  l.last_seen,
  l.coverage_ratio,
  l.regime_state,
  l.endpoint_status,
  l.visibility_class,
  COALESCE(l.likely_test_dev, 0) AS likely_test_dev,
  COALESCE(l.has_labeler_service, 0) AS has_labeler_service,
  COALESCE(l.auditability, 'low') AS auditability,
  COALESCE(srs.service_record_revisions, 0) AS service_record_revisions,
  srs.first_record_at,
  srs.last_record_at,
  lr.record_json AS latest_record_json
FROM labelers l
LEFT JOIN service_record_stats srs ON l.labeler_did = srs.labeler_did
LEFT JOIN latest_record lr ON l.labeler_did = lr.labeler_did AND lr.rn = 1
WHERE COALESCE(l.events_30d, 0) > 0
   OR srs.service_record_revisions > 0
ORDER BY l.events_30d DESC NULLS LAST
"""


def classify_maturity(enriched: dict) -> str:
    """Operates on the enriched dict (post derive_flags), so uses
    enriched key names (label_count_30d etc.), not the raw SQL row."""
    did = enriched["labeler_did"]
    if did == PLATFORM_ROOT_DID:
        return "platform-root"
    if enriched["likely_test_dev"]:
        return "experimental"
    events_30d = enriched["label_count_30d"]
    def_count = enriched["latest_label_def_count"]
    explains = enriched["explains_labels"]
    if events_30d == 0 and enriched["service_record_revisions"] > 0:
        return "abandoned"
    if events_30d == 0:
        return "unknown"
    if events_30d >= 10000 and def_count >= 1:
        return "moderation-infrastructure"
    if events_30d >= 100 and def_count >= 1 and explains:
        return "community-service"
    if events_30d >= 10 and def_count >= 1:
        return "personal/reputational"
    if events_30d < 10:
        return "experimental"
    return "unknown"


def derive_flags(row: dict) -> dict:
    """Compute the heuristic boolean flags + classify maturity."""
    record_json = row.get("latest_record_json")
    latest_label_def_count = 0
    explains_labels = False
    user_visible_consequence_known = False
    definitions_with_setting = 0
    if record_json:
        try:
            rec = json.loads(record_json)
            defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
            latest_label_def_count = len(defs)
            for d in defs:
                locales = d.get("locales") or []
                if any(
                    (l.get("name") or "").strip() or (l.get("description") or "").strip()
                    for l in locales
                ):
                    explains_labels = True
                if d.get("defaultSetting") in ("hide", "warn", "ignore"):
                    definitions_with_setting += 1
            user_visible_consequence_known = (
                latest_label_def_count > 0 and definitions_with_setting > 0
            )
        except (json.JSONDecodeError, TypeError):
            pass

    has_stable_service_record = (
        1 <= row["service_record_revisions"] <= 5
    )

    enriched = {
        "labeler_did": row["labeler_did"],
        "handle": row["handle"] or "<unknown>",
        "display_name": row["display_name"] or "",
        "labeler_class": row["labeler_class"],
        "is_reference": bool(row["is_reference"]),
        "active_recently": row["events_30d"] > 0,
        "declares_scope": latest_label_def_count > 0,
        "explains_labels": explains_labels,
        "has_contact_or_appeal_path": "unknown_v1",
        "has_stable_service_record": has_stable_service_record,
        "service_record_revisions": row["service_record_revisions"],
        "first_record_at": row["first_record_at"],
        "last_record_at": row["last_record_at"],
        "label_count_7d": row["events_7d"],
        "label_count_30d": row["events_30d"],
        "distinct_targets_30d": row["unique_targets_30d"],
        "latest_label_def_count": latest_label_def_count,
        "user_visible_consequence_known": user_visible_consequence_known,
        "last_seen": row["last_seen"],
        "regime_state": row["regime_state"],
        "endpoint_status": row["endpoint_status"],
        "visibility_class": row["visibility_class"],
        "auditability": row["auditability"],
        "likely_test_dev": bool(row["likely_test_dev"]),
    }
    enriched["maturity_class"] = classify_maturity(enriched)
    return enriched


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True, help="output JSON path")
    args = ap.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        rows = [dict(r) for r in conn.execute(SCAN_SQL).fetchall()]
    finally:
        conn.close()

    enriched = [derive_flags(r) for r in rows]

    # Class histogram
    histogram: dict = {}
    for r in enriched:
        histogram[r["maturity_class"]] = histogram.get(r["maturity_class"], 0) + 1

    out = {
        "scan_meta": {
            "scanned_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "scanner_version": "operator_maturity_scan.py v1",
            "db_path": args.db,
            "platform_root_did": PLATFORM_ROOT_DID,
            "rows_returned": len(enriched),
            "class_histogram": histogram,
        },
        "rows": enriched,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=2, default=str)
        f.write("\n")
    print(
        f"wrote {len(enriched)} rows to {args.out}; "
        f"class histogram: {histogram}",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
