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

# Atproto's global LABELS map keys, AUTHORITATIVE per the upstream
# @atproto/api package at HEAD 7b8c5d60a. The previous Bundle A list had
# 21 entries; upstream actually has only these 8. The other 13 (intolerant,
# self-harm, sensitive, threat, spam, rude, sexual-figurative,
# impersonation, illicit, security, misleading, unsafe-link, inauthentic)
# were operator inventions — they do not exist in the default-client's
# policy map and the default consumer has NO documented behavior for them.
# Bundle B finding F-002 records the correction.
GLOBAL_LABELS = {
    "!hide", "!warn", "!no-unauthenticated",
    "porn", "sexual", "nudity", "graphic-media", "gore",
}

# Labels documented at the atproto PROTOCOL level (not in @atproto/api's
# global LABELS map) whose default-client behavior is well-defined by
# protocol/PDS implementation. Treated as documented for policy
# resolution, but the policy_artifact citation is different.
#
# !takedown: PDS removes the record; client never sees it; behavior is
# upstream of any client-library rule. Bundle B finding F-003 records
# the distinction between LABELS-const artifacts and protocol-spec
# artifacts.
PROTOCOL_DOCUMENTED_LABELS = {
    "!takedown",
}

# execution_surface mapping for known global labels. Each entry carries
# audit metadata so future readers can trace why the surface was assigned
# this value, what evidence backed the assignment, and when it was last
# reviewed against upstream.
#
# Values:
#   client_render  — effect applied by client at render time (blur, warn,
#                    hide-from-default-feed); record remains hosted unchanged.
#   pds_hosting    — effect at PDS / hosting layer (record removed, account
#                    suspended, withheld); happens server-side independent
#                    of client.
#   mixed          — both surfaces involved. DO NOT INVENT — only add when
#                    upstream behavior is documentably split.
#   unknown        — label is global but its surface is not in this table;
#                    classifier MUST emit a specific gap (never default to
#                    render or hosting).
#
# When this table loses calibration with upstream, the deriver's
# DEFAULT_POLICY_HEAD constant should bump and the reviewed_at field on
# each affected row should refresh.
KNOWN_LABEL_SURFACE = {
    # ----- PDS / hosting layer -----
    "!takedown": {
        "surface": "pds_hosting",
        "source": "atproto protocol behavior (Ozone takedown removes the record from the PDS). NOT in @atproto/api LABELS const v0.19.17 — F-003 records this distinction. Documented at the protocol/PDS-implementation level, not the client-library level.",
        "rationale": "Takedown is an administrative action at the PDS layer; clients see absence or tombstone; behavior is upstream of any client-library rule. There is no need for @atproto/api to define a render action because the record never reaches the client to be rendered.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B, post-F-003)",
    },
    "!no-unauthenticated": {
        "surface": "pds_hosting",
        "source": "atproto @atproto/api LABELS const + protocol behavior: gates who can READ the record from the PDS",
        "rationale": "Access control at the hosting layer; unauthenticated readers (logged-out, scrapers) are refused service. Render-side behavior is downstream of and dependent on the hosting-side gate.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },

    # ----- Client render layer -----
    "!hide": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const",
        "rationale": "Default client hides content with blur and content-warning placeholder; record remains hosted unchanged. flags=['no-override','no-self'] means the consumer's policy is fixed at hide, not that the action is hosting-side.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },
    "!warn": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const",
        "rationale": "Default client shows warning overlay; record remains hosted unchanged.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },
    "porn": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const v0.19.17, packages/api/src/moderation/const/labels.ts at HEAD 7b8c5d60a",
        "rationale": "Default behavior is blur(media) under adult-content gate; rendered by client at viewing time.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (D-001 patch)",
    },
    "sexual": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const v0.19.17",
        "rationale": "Default behavior is warn(media) under adult-content gate; rendered by client.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },
    "nudity": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const v0.19.17",
        "rationale": "Default behavior is ignore (configurable); rendered by client when user enables.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },
    "graphic-media": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const v0.19.17",
        "rationale": "Default behavior is warn(media); rendered by client.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B)",
    },
    "gore": {
        "surface": "client_render",
        "source": "atproto @atproto/api LABELS const v0.19.17",
        "rationale": "Default behavior rendered by client. Added during Bundle B after F-002 audit found gore was missing from prior GLOBAL_LABELS list.",
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle B, F-002)",
    },
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
        emitter_definition = _lookup_emitter_service_record(
            conn, labeler_did, label_value
        )
        labeler_record_exists = _labeler_has_service_record(conn, labeler_did)
    finally:
        conn.close()

    policy_documented = _policy_documented(label_value)

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
        "PolicyDocumentation": _policy_documentation(label_value, policy_documented),
        "LabelerEmitterDocumentation": _labeler_emitter_documentation(
            labeler_did, label_value, emitter_definition, labeler_record_exists,
        ),
        "PolicyWitness": _policy_witness(policy_documented),
        "RenderObservation": _render_observation(label_value, policy_documented),
        "HostingObservation": _hosting_observation(label_value, policy_documented),
    }
    return packet


def _labeler_emitter_documentation(
    labeler_did: str,
    label_value: str,
    found: Optional[Dict[str, Any]],
    labeler_record_exists: bool = False,
) -> Dict[str, Any]:
    """Distinct from PolicyDocumentation: this records what the LABELER
    has declared via its app.bsky.labeler.service record. Provenance of
    rule, NOT global consumer authority.

    Bundle C invariant: service-record declaration is provenance, not
    global authority. consumer_scope here is always 'emitter_declared'
    when found — separately upgrading to 'opt_in_consumer_observed'
    requires direct consumer evidence which v1 does not model.

    D.5 adds `labeler_record_exists` to distinguish two absence cases:
      - service record exists but doesn't declare this label
        (genuine emitter undeclared)
      - no service record at all (ingestion gap)
    """
    if found is None:
        if labeler_record_exists:
            return {
                "status": "service_record_found_label_not_declared",
                "artifact_kind": None,
                "consumer_scope": "unknown",
                "labeler_service_record_present": True,
                "reason": (
                    f"A labeler service record for {labeler_did!r} IS "
                    "available (in discovery_events or in "
                    "service_record_snapshots/), but it does not declare "
                    f"a labelValueDefinition for {label_value!r}. This is "
                    "genuine emitter-side undeclared, NOT an ingestion gap."
                ),
            }
        return {
            "status": "absent",
            "artifact_kind": None,
            "consumer_scope": "unknown",
            "labeler_service_record_present": False,
            "reason": (
                f"No app.bsky.labeler.service record for {labeler_did!r} "
                "was found in discovery_events OR in "
                "service_record_snapshots/. Likely an ingestion gap "
                "(F-005 shape) rather than confirmed absence."
            ),
        }
    return {
        "status": "documented_via_service_record",
        "artifact_kind": "service_record",
        "consumer_scope": "emitter_declared",
        "labeler_service_record_present": True,
        "scoping_note": (
            "consumer_scope='emitter_declared' means the LABELER has "
            "published a rule for this label_value in its service "
            "record. It does NOT mean any consumer honors this rule. "
            "Service-record declaration is provenance, not global "
            "authority. Upgrading to 'opt_in_consumer_observed' requires "
            "direct evidence of consumer honor (deferred to Bundle D+)."
        ),
        "execution_surface": _surface_from_emitter_definition(
            found["definition"]
        ),
        "extracted_definition": found["definition"],
        "service_record_provenance": {
            "labeler_did": labeler_did,
            "commit_cid": found["service_record_commit_cid"],
            "commit_rev": found["service_record_commit_rev"],
            "discovered_at": found["service_record_discovered_at"],
            "source_table": "labelwatch.db / discovery_events",
        },
        "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "reviewed_at": "2026-06-08",
        "reviewer": "labelwatch-claude (Bundle C)",
    }


def _policy_documented(label_value: str) -> bool:
    """True if this label has a documented policy for the default consumer
    via either the @atproto/api global LABELS const or atproto protocol
    behavior. Bundle C distinguishes this from emitter-declared status
    (which is provenance, not consumer authority — see Bundle C invariant)."""
    return (
        label_value in GLOBAL_LABELS or label_value in PROTOCOL_DOCUMENTED_LABELS
    )


def _policy_artifact_kind(label_value: str) -> Optional[str]:
    """Which artifact backs the documented policy. Bundle C vocabulary:
       upstream_const | protocol_doc | service_record | empirical_observation

    Service-record + empirical_observation appear in the
    LabelerEmitterDocumentation block, not PolicyDocumentation —
    consumer scope is what differs. Service-record declaration is
    provenance, not global authority.
    """
    if label_value in GLOBAL_LABELS:
        return "upstream_const"
    if label_value in PROTOCOL_DOCUMENTED_LABELS:
        return "protocol_doc"
    return None


def _lookup_emitter_service_record(
    conn: sqlite3.Connection,
    labeler_did: str,
    label_value: str,
) -> Optional[Dict[str, Any]]:
    """Find the labeler's labelValueDefinition for `label_value` from
    EITHER labelwatch.db/discovery_events OR a static snapshot in
    service_record_snapshots/. Returns None when no source has a
    matching definition.

    D.5 / E-prep adds the snapshot fallback to work around F-005
    (mod.bsky's service record is not in labelwatch's discovery
    pipeline). Snapshots are emitter declarations, NOT global authority
    — the consumer_scope assigned downstream is still emitter_declared.
    """
    # 1. discovery_events (primary; live as labelwatch ingestion catches up)
    row = conn.execute(
        """
        SELECT record_json, commit_cid, commit_rev, discovered_at
        FROM discovery_events
        WHERE labeler_did = ?
          AND operation IN ('create','update')
          AND json_extract(record_json, '$.policies.labelValueDefinitions') IS NOT NULL
        ORDER BY discovered_at DESC
        LIMIT 1
        """,
        (labeler_did,),
    ).fetchone()
    if row is not None:
        try:
            rec = json.loads(row["record_json"])
        except (json.JSONDecodeError, TypeError):
            rec = None
        if rec is not None:
            defs = (rec.get("policies") or {}).get("labelValueDefinitions") or []
            for d in defs:
                if d.get("identifier") == label_value:
                    return {
                        "definition": d,
                        "service_record_commit_cid": row["commit_cid"],
                        "service_record_commit_rev": row["commit_rev"],
                        "service_record_discovered_at": row["discovered_at"],
                        "source": "labelwatch.db / discovery_events",
                    }

    # 2. service_record_snapshots/ fallback (F-005 workaround for labelers
    # whose service records did not land in discovery_events).
    snap = _load_service_record_snapshot(labeler_did)
    if snap is not None:
        defs = (snap.get("policies") or {}).get("labelValueDefinitions") or []
        for d in defs:
            if d.get("identifier") == label_value:
                meta = snap.get("_snapshot_meta") or {}
                return {
                    "definition": d,
                    "service_record_commit_cid": None,
                    "service_record_commit_rev": None,
                    "service_record_discovered_at": meta.get("snapshotted_at"),
                    "source": (
                        "service_record_snapshots/ (F-005 fallback; "
                        f"snapshotted_at={meta.get('snapshotted_at')}; "
                        f"source_url={meta.get('source_url')})"
                    ),
                }
    return None


def _load_service_record_snapshot(labeler_did: str) -> Optional[Dict[str, Any]]:
    """Look in docs/specimens/service_record_snapshots/<sanitized-did>.json
    for a pre-fetched service record. Returns the parsed JSON or None."""
    here = os.path.dirname(os.path.abspath(__file__))
    fname = labeler_did.replace(":", "-") + ".json"
    path = os.path.join(here, "service_record_snapshots", fname)
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _labeler_has_service_record(
    conn: sqlite3.Connection, labeler_did: str
) -> bool:
    """True if EITHER labelwatch.discovery_events OR the local snapshot
    directory has a service record for this labeler — regardless of
    whether the record declares any specific label_value.

    Used to distinguish:
      - 'we have no service record at all for this labeler' (ingestion gap)
      - 'we have the service record but it doesn't declare this label'
        (genuine emitter undeclared)
    """
    row = conn.execute(
        """
        SELECT 1 FROM discovery_events
        WHERE labeler_did = ?
          AND operation IN ('create','update')
          AND json_extract(record_json, '$.policies.labelValueDefinitions') IS NOT NULL
        LIMIT 1
        """,
        (labeler_did,),
    ).fetchone()
    if row is not None:
        return True
    return _load_service_record_snapshot(labeler_did) is not None


def _surface_from_emitter_definition(definition: Dict[str, Any]) -> str:
    """Derive execution_surface from a service-record labelValueDefinition.

    Service-record definitions describe CONSUMER-SIDE render behavior
    (blurs, severity, defaultSetting). The surface is therefore always
    client_render at this layer. PDS-level effects are not declared via
    service records — they are protocol/admin actions (see
    PROTOCOL_DOCUMENTED_LABELS).
    """
    return "client_render"


def _surface_for(label_value: str, documented: bool) -> Optional[str]:
    """Look up the surface assignment for a label_value. Returns the
    surface string, or 'unknown' if the label is documented but has no
    KNOWN_LABEL_SURFACE entry, or None if not documented at all."""
    if not documented:
        return None
    entry = KNOWN_LABEL_SURFACE.get(label_value)
    if entry is None:
        return "unknown"
    return entry["surface"]


def _render_observation(label_value: str, documented: bool) -> Dict[str, Any]:
    surface = _surface_for(label_value, documented)
    # not_applicable when the documented policy does not act on the render
    # surface; otherwise absent because atproto has no render receipts.
    if surface == "pds_hosting":
        return {
            "status": "not_applicable",
            "reason": (
                "Documented policy execution_surface is pds_hosting; the "
                "render surface is not where this conversion acts."
            ),
        }
    return {
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
    }


def _hosting_observation(label_value: str, documented: bool) -> Dict[str, Any]:
    surface = _surface_for(label_value, documented)
    # not_applicable when documented surface is client_render; absent when
    # surface is pds_hosting or mixed (we have no hosting-side probe yet).
    if surface in ("pds_hosting", "mixed"):
        return {
            "status": "absent",
            "reason": (
                "Documented policy execution_surface includes pds_hosting; "
                "the hosting state of the target (whether the PDS has "
                "removed/withheld the record) is not directly observed by "
                "Labelwatch in v1."
            ),
            "what_would_be_required_for_observation": [
                "hosting-side probe (resolve target_uri against its PDS; "
                "compare to pre-takedown snapshot)",
                "or appview-published receipts of hosting actions",
            ],
        }
    if surface == "client_render":
        return {
            "status": "not_applicable",
            "reason": (
                "Documented policy execution_surface is client_render; the "
                "hosting layer is not where this conversion acts."
            ),
        }
    # surface unknown or no documented policy: stay agnostic
    return {
        "status": "not_applicable",
        "reason": (
            "Documented policy execution_surface is unknown or absent; "
            "hosting observation not framed for this packet."
        ),
    }


def _policy_documentation(label_value: str, documented: bool) -> Dict[str, Any]:
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
    if documented:
        entry = KNOWN_LABEL_SURFACE.get(label_value)
        surface = entry["surface"] if entry else "unknown"
        # Audit metadata for the surface assignment, surfaced into the
        # evidence packet so future readers can trace the rationale.
        if entry:
            surface_audit = {
                "source": entry.get("source"),
                "rationale": entry.get("rationale"),
                "reviewed_at": entry.get("reviewed_at"),
                "reviewer": entry.get("reviewer"),
            }
        else:
            surface_audit = {
                "source": "label_value is in GLOBAL_LABELS but has no KNOWN_LABEL_SURFACE entry",
                "rationale": "deriver records 'unknown' rather than guessing render or hosting; classifier must emit a surface-uncertain gap (must not default to either surface)",
                "reviewed_at": None,
                "reviewer": None,
            }
        artifact_kind = _policy_artifact_kind(label_value)
        if artifact_kind == "upstream_const":
            policy_artifact = {
                **artifact_base,
                "artifact_kind": "upstream_const",
                "extracted_rule": {
                    "label_value": label_value,
                    "note": (
                        "label_value present in @atproto/api global LABELS "
                        "map. Deriver records artifact presence only — "
                        "full extracted_rule body is not pulled by the "
                        "deriver in v1."
                    ),
                },
            }
        else:  # protocol_doc
            policy_artifact = {
                "artifact_kind": "protocol_doc",
                "artifact_description": (
                    f"atproto protocol behavior for {label_value!r}; "
                    "documented by protocol/PDS implementation rather than "
                    "the @atproto/api client library's LABELS const. F-003 "
                    "records this distinction."
                ),
                "extracted_rule": {
                    "label_value": label_value,
                    "note": (
                        "Behavior is upstream of any client-library rule. "
                        "Hand-authored fixture may cite the relevant "
                        "protocol-spec section when needed."
                    ),
                },
            }
        return {
            "consumer": consumer,
            "policy_artifact": policy_artifact,
            "consumer_scope": "global_platform",
            "execution_surface": surface,
            "execution_surface_source": (
                "KNOWN_LABEL_SURFACE table in derive_evidence.py (sourced "
                "from policy artifact + known atproto label semantics). "
                "Describes WHERE the documented conversion acts; does not "
                "encode whether the conversion gap exists."
            ),
            "execution_surface_audit": surface_audit,
            "documented_expected_action": {
                "action_summary": (
                    "see extracted_rule in pinned artifact; surface is "
                    f"{surface!r}"
                ),
                "preconditions": [
                    "viewer has not opted into adult content (where adult flag applies)",
                    "viewer has not overridden per-label setting",
                    "live client is using a policy pipeline that includes this rule",
                    "for pds_hosting surface: PDS honors the takedown/withhold action",
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
        "execution_surface": None,
        "execution_surface_source": (
            "no documented policy for this label_value in the named "
            "consumer's pipeline; execution_surface is undefined for an "
            "absent policy."
        ),
        "status": "absent_for_consumer",
        "consumer_scope": "unknown",
        "scoping_note": (
            "Policy-documentation status FOR THE NAMED CONSUMER under "
            "the stated render_context. Evidence is silent over other "
            "consumer configurations (e.g., viewers who have opted into "
            "a third-party labeler service whose definitions cover this "
            "label_value)."
        ),
    }


def _policy_witness(documented: bool) -> Dict[str, Any]:
    if documented:
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
