"""Detection-lane evidence deriver.

Pulls one or more REAL label events from labelwatch.db and composes an
evidence packet in the same schema as the hand-authored fixtures
(specimen-00X-*.evidence.json). The output is NOT a fixture — it is a
detection-lane artifact for the classifier to run over without
operator pre-judgment.

Hard guard: the output evidence packet contains NO derived fields.
No expected_gap. No admissible_claims. No inadmissible_claims. No
"execution_gap" prose. The deriver only fills the evidence-shape
fields from observable sources:

  - LabelObservation         from labelwatch.label_events + labelers
  - PolicyDocumentation      from @atproto/api global LABELS map presence
  - PolicyWitness            architectural status (always
                             "partial_documentary_not_receipted" when
                             policy documented; "not_applicable" when
                             policy absent for consumer)
  - RenderObservation        always "absent" — atproto publishes no
                             per-render receipts

Usage:
  python3 derive_evidence.py \
      --db /var/lib/labelwatch/labelwatch.db \
      --labeler-did did:plc:ar7c4by46qjdydhdevvrndac \
      --label-value porn \
      --out derived/

Outputs:
  derived/derived-<timestamp>-<labeler_slug>-<label_value>-<id>.evidence.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Pinned policy artifact (matches fixtures 001/002 at session time).
# When the deriver loses calibration with the live ecosystem, these
# fields should be updated together and DISAGREEMENTS.md should record
# the bump.
DEFAULT_POLICY_REPO = "bluesky-social/atproto"
DEFAULT_POLICY_PACKAGE = "@atproto/api"
DEFAULT_POLICY_PACKAGE_VERSION = "0.19.17"
DEFAULT_POLICY_FILE_PATH = "packages/api/src/moderation/const/labels.ts"
DEFAULT_POLICY_HEAD = "7b8c5d60a"

# Atproto's global LABELS map (from packages/api/src/moderation/const/labels.ts
# at HEAD 7b8c5d60a). Hardcoded fallback for environments without the cloned
# atproto repo. Used to determine whether the default bsky.app-default-client
# has a documented policy for a given label_value.
GLOBAL_LABELS = {
    "!hide", "!warn", "!no-unauthenticated", "!takedown",
    "porn", "sexual", "nudity", "graphic-media",
    "intolerant", "self-harm", "sensitive", "threat", "spam",
    "rude", "sexual-figurative", "impersonation", "illicit",
    "security", "misleading", "unsafe-link", "inauthentic",
}


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:40]


def _load_label_event(
    conn: sqlite3.Connection,
    labeler_did: str,
    label_value: str,
    pick: str = "latest",
) -> Optional[sqlite3.Row]:
    if pick == "latest":
        order = "ORDER BY ts DESC"
    elif pick == "earliest":
        order = "ORDER BY ts ASC"
    else:
        raise ValueError(f"unknown pick mode: {pick!r}")
    return conn.execute(
        f"""
        SELECT id, labeler_did, val, uri, ts, neg, target_did
        FROM label_events
        WHERE labeler_did = ? AND val = ? AND neg = 0
        {order}
        LIMIT 1
        """,
        (labeler_did, label_value),
    ).fetchone()


def _load_labeler_metadata(
    conn: sqlite3.Connection, labeler_did: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT labeler_did, handle, display_name, labeler_class,
               is_reference, endpoint_status, visibility_class
        FROM labelers
        WHERE labeler_did = ?
        """,
        (labeler_did,),
    ).fetchone()


def _classify_target(uri: str) -> str:
    if uri.startswith("at://") and "/app.bsky.feed.post/" in uri:
        return "post_record"
    if uri.startswith("did:"):
        return "account"
    if uri.startswith("at://"):
        return "atproto_record"
    return "unknown"


def derive(
    db_path: str,
    labeler_did: str,
    label_value: str,
    pick: str = "latest",
) -> Dict[str, Any]:
    """Build an evidence packet from a real DB row. Returns the dict."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        row = _load_label_event(conn, labeler_did, label_value, pick)
        if row is None:
            raise SystemExit(
                f"no label event found for labeler_did={labeler_did} "
                f"label_value={label_value}"
            )
        labeler = _load_labeler_metadata(conn, labeler_did)
    finally:
        conn.close()

    policy_in_global = label_value in GLOBAL_LABELS

    # Compose evidence packet. NO derived fields below this line.
    packet: Dict[str, Any] = {
        "specimen_id": f"derived-{row['id']}",
        "title": (
            f"{(labeler['handle'] if labeler else labeler_did)} / "
            f"{label_value} / {row['uri']}"
        ),
        "summary": (
            "Detection-lane evidence packet auto-derived from "
            "labelwatch.label_events. No operator pre-judgment of gap or "
            "admissible claims."
        ),
        "schema_version": 1,
        "schema_kind": "evidence",
        "lane": "detection",
        "provenance": {
            "source": "labelwatch.label_events (auto-derived)",
            "deriver": "derive_evidence.py",
            "derived_at": datetime.now(timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_row_id": int(row["id"]),
            "source_db_path": db_path,
        },
        "LabelObservation": {
            "labeler_did": row["labeler_did"],
            "labeler_handle": (labeler["handle"] if labeler else None),
            "labeler_class": (
                labeler["labeler_class"] if labeler else "unknown"
            ),
            "is_reference_labeler": bool(
                labeler["is_reference"] if labeler else 0
            ),
            "target_uri": row["uri"],
            "target_kind": _classify_target(row["uri"]),
            "label_value": row["val"],
            "neg": int(row["neg"] or 0),
            "ts": row["ts"],
            "ingest_witness": "labelwatch label_events stream",
            "labelwatch_authority_effect_classification": None,
        },
        "PolicyDocumentation": _policy_documentation(label_value, policy_in_global),
        "PolicyWitness": _policy_witness(policy_in_global),
        "RenderObservation": {
            "status": "absent",
            "reason": (
                "ATProto publishes no per-render receipts in the wire "
                "protocol. Whether any specific user, at any specific "
                "moment, saw this content rendered with any specific "
                "action is invisible to Labelwatch."
            ),
            "what_would_be_required_for_observation": [
                "consumer-side per-render receipts in a published stream",
                "or external probes",
            ],
        },
    }
    return packet


def _policy_documentation(label_value: str, in_global: bool) -> Dict[str, Any]:
    consumer = {
        "consumer_id": "bsky.app-default-client",
        "description": (
            "Bluesky official appview + client running default moderation "
            "pipeline; no third-party labeler services subscribed beyond "
            "the platform defaults"
        ),
        "render_context": {
            "client_family": "bsky.app",
            "viewer_state": "logged_out_or_fresh_logged_in_default",
            "adult_content_enabled": False,
            "per_label_setting_override": None,
            "policy_pipeline_version_assumed": (
                f"client running {DEFAULT_POLICY_PACKAGE} at or near "
                f"v{DEFAULT_POLICY_PACKAGE_VERSION}"
            ),
        },
    }
    artifact_base = {
        "repo": DEFAULT_POLICY_REPO,
        "package": DEFAULT_POLICY_PACKAGE,
        "package_version": DEFAULT_POLICY_PACKAGE_VERSION,
        "file_path": DEFAULT_POLICY_FILE_PATH,
        "head_or_commit": DEFAULT_POLICY_HEAD,
    }
    if in_global:
        return {
            "consumer": consumer,
            "policy_artifact": {
                **artifact_base,
                "extracted_rule": {
                    "label_value": label_value,
                    "note": (
                        "label_value present in global LABELS map; "
                        "deriver records artifact presence only — full "
                        "extracted_rule body is not pulled by the deriver "
                        "in v1. Hand-authored fixtures may include the "
                        "full rule body when richer context matters."
                    ),
                },
            },
            "documented_expected_action": {
                "action_for_post_render_under_render_context": (
                    "see extracted_rule in pinned artifact"
                ),
                "preconditions": [
                    "viewer has not opted into adult content",
                    "viewer has not overridden per-label setting",
                    "live client is using a policy pipeline that includes this rule",
                ],
            },
            "status": "documented",
        }
    return {
        "consumer": consumer,
        "policy_artifact_searched": {
            **artifact_base,
            "search_result": "no_entry",
            "search_evidence": (
                f"Global LABELS map at the pinned artifact does not "
                f"contain an entry for {label_value!r}."
            ),
        },
        "status": "absent_for_consumer",
        "scoping_note": (
            "Policy-documentation status FOR THE NAMED CONSUMER under "
            "the stated render_context. Evidence is silent over other "
            "consumer configurations (e.g., viewers who have opted into "
            "a third-party labeler service whose definitions cover this "
            "label_value)."
        ),
    }


def _policy_witness(in_global: bool) -> Dict[str, Any]:
    if in_global:
        return {
            "status": "partial_documentary_not_receipted",
            "reason": (
                "Public source code declares the rule, but no live "
                "receipt binds the consumer to that specific policy "
                "version at render time."
            ),
            "what_would_be_required_for_full_witness": [
                "a per-client-version reporting stream",
                "or render-side receipts citing the policy artifact hash used at decision time",
            ],
        }
    return {
        "status": "not_applicable",
        "reason": (
            "No policy is documented for this label_value in the named "
            "consumer's pipeline; there is no rule whose live "
            "application could be witnessed."
        ),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", required=True, help="Path to labelwatch.db")
    p.add_argument("--labeler-did", required=True)
    p.add_argument("--label-value", required=True)
    p.add_argument("--pick", default="latest", choices=("latest", "earliest"))
    p.add_argument("--out", default=".", help="Output directory")
    args = p.parse_args()

    packet = derive(args.db, args.labeler_did, args.label_value, args.pick)
    os.makedirs(args.out, exist_ok=True)
    out_name = (
        f"derived-{packet['provenance']['source_row_id']}"
        f"-{_slug(args.labeler_did)}"
        f"-{_slug(args.label_value)}.evidence.json"
    )
    out_path = os.path.join(args.out, out_name)
    with open(out_path, "w") as f:
        json.dump(packet, f, indent=2)
        f.write("\n")
    print(out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
