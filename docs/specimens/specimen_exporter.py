"""Specimen exporter skeleton (Bundle D).

Takes an evidence packet + its classifier output and decides whether
the (labeler, label, target) instance is exportable as a candidate
specimen, OR is blocked because provenance is insufficient.

Hard discipline (Bundle D invariant):

    Unknown is not a specimen. It is either a real surface absence or
    an instrumentation failure; the exporter must preserve that
    ambiguity instead of laundering it.

Lanes:
  authority_surface  — provenance/conversion-shape specimens (Bundle D
                       primary lane; mostly skeleton plumbing here).
  freshness          — placeholder; substantive semantics arrive in
                       Bundle E. Always blocked at this layer unless
                       state-basis fields are present in the evidence.

Output contracts:
  exported candidates carry both policy_provenance AND emitter_provenance
  fields. consumer_scope_effective is the precedence-resolved scope used
  by the gap struct (matches classifier.ConversionGap.consumer_scope).
  Caveats list non-fatal qualifications, e.g. "non_global_provenance"
  for emitter_declared sources.

  blocked candidates carry a typed `blocker` string and a `reason`
  explaining what would need to change to unblock. Blocked candidates
  are NOT specimens; downstream consumers (Lean, fixtures, exporters)
  MUST refuse to admit them.

Refusal vocabulary (Bundle D v1 + D.5 + E):
  no_label_observation             — gap.name = observability_gap
  unknown_surface_not_specimen     — surface=unknown OR gap.name=execution_gap_surface_unknown
  ingestion_gap_surface_unresolved — first-party labeler + consumer_scope=unknown +
                                     no service record found at all (true F-005 shape)
  emitter_does_not_declare_label   — first-party labeler + consumer_scope=unknown +
                                     service record exists but doesn't declare this label
                                     (D.5: genuine emitter-side undeclared, NOT an
                                     ingestion gap)
  provenance_unresolved            — consumer_scope=unknown for a non-first-party labeler
  missing_required_basis           — lane=freshness AND no StateBasis in evidence
                                     (Bundle E rename: previously "requires_state_basis";
                                     fires only when basis is genuinely absent,
                                     not when basis is present-with-unknown-horizon)

Bundle E state_basis_status vocabulary (carried on exported freshness candidates):
  current_basis        — StateBasis.freshness_horizon is an ISO deadline still
                         in the future as of now_iso
  stale_basis          — StateBasis.freshness_horizon is an ISO deadline already
                         passed as of now_iso (caveat 'stale_basis' added)
  unknown_basis        — StateBasis.freshness_horizon is 'unknown' or unparsable
                         (caveat 'unknown_basis' added)
  missing              — StateBasis is absent entirely (this is what produces the
                         missing_required_basis blocker on the freshness lane;
                         on authority_surface, the candidate exports informationally
                         with state_basis_status='missing' but no caveat — basis
                         is not the freshness lane there)

Bundle E invariant:
  Freshness is not authority. It may preserve, caveat, stale, or block. It must
  not promote. stale_basis and unknown_basis never silently export as
  current_basis. A fresh undeclared label is still undeclared; a stale global
  policy is still not current.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


EXPORTER_VERSION = "specimen_exporter.py v2 (Bundle E state-basis gate)"
SCHEMA_VERSION = 2


# --- main entry point ----------------------------------------------------

def export_candidate(
    evidence: Dict[str, Any],
    classifier_output: Dict[str, Any],
    lane: str = "authority_surface",
    evidence_source: Optional[str] = None,
    now_iso: Optional[str] = None,
) -> Dict[str, Any]:
    """Pure function. Returns either an exported candidate or a blocked
    candidate. Never raises on schema mismatch — produces an
    'unclassified_evidence' blocker instead so the runner sees the
    problem.

    `lane` is one of {'authority_surface', 'freshness'} for v1.
    `evidence_source` is an optional pointer to where the evidence came
    from (filename or url-like) used in the candidate's provenance block.
    `now_iso` is a clock injection point for deterministic tests.
    """
    if now_iso is None:
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    label_obs = evidence.get("LabelObservation") or {}
    policy_doc = evidence.get("PolicyDocumentation") or {}
    emitter_doc = evidence.get("LabelerEmitterDocumentation") or {}
    gap = (classifier_output or {}).get("ConversionGap") or {}

    base = {
        "schema_kind": None,  # set below
        "schema_version": SCHEMA_VERSION,
        "lane": lane,
        "exporter": EXPORTER_VERSION,
        "exported_at": now_iso,
        "evidence_source": evidence_source,
        "labeler": _labeler_block(label_obs),
        "label": _label_block(label_obs),
        "target": _target_block(label_obs),
    }

    # --- HARD GATES (refusal first) ---

    blocker = _hard_gates(gap, label_obs, lane, evidence)
    if blocker is not None:
        return {
            **base,
            "schema_kind": "blocked_candidate",
            "blocker": blocker["blocker"],
            "reason": blocker["reason"],
            "what_would_unblock": blocker.get("what_would_unblock"),
            "conversion_gap": gap or None,
        }

    # --- EXPORTABLE CANDIDATE ---

    state_basis = evidence.get("StateBasis")
    state_basis_status = _classify_state_basis_status(state_basis, now_iso)

    candidate = {
        **base,
        "schema_kind": "specimen_candidate",
        "observation": _observation_block(label_obs, evidence),
        "policy_provenance": _policy_provenance_block(policy_doc),
        "emitter_provenance": _emitter_provenance_block(emitter_doc),
        "consumer_scope_effective": gap.get("consumer_scope"),
        "conversion_gap": gap,
        "state_basis": state_basis,
        "state_basis_status": state_basis_status,
        "export_caveats": _caveats(
            policy_doc, emitter_doc, gap, lane, evidence, state_basis_status,
        ),
    }
    return candidate


def _classify_state_basis_status(
    state_basis: Optional[Dict[str, Any]],
    now_iso: str,
) -> str:
    """Bundle E: classify the freshness state of the evidence's basis.
    Returns one of: missing | unknown_basis | current_basis | stale_basis.

    Vocabulary:
      - missing       : no StateBasis at all
      - unknown_basis : StateBasis present but freshness_horizon is 'unknown'
                        / None / unparsable. Caller decides whether to export
                        with caveat or block (freshness lane: export with
                        caveat per Bundle E invariant)
      - current_basis : freshness_horizon is an ISO timestamp still in the
                        future as of now_iso
      - stale_basis   : freshness_horizon is an ISO timestamp already passed

    ISO timestamp comparison is lexical — relies on Z-suffixed UTC. v1
    does not parse duration strings ("24h", "7d"); those classify as
    unknown_basis until a future refinement.
    """
    if not state_basis:
        return "missing"
    horizon = state_basis.get("freshness_horizon")
    if horizon == "unknown" or horizon is None:
        return "unknown_basis"
    if isinstance(horizon, str) and len(horizon) >= 10 and horizon[0:4].isdigit():
        # Treat as ISO timestamp (lexical comparison works for Z-suffixed UTC)
        if horizon >= now_iso:
            return "current_basis"
        return "stale_basis"
    # Anything else (e.g., duration strings, malformed) -> honest unknown
    return "unknown_basis"


# --- hard refusal gates --------------------------------------------------

def _hard_gates(
    gap: Dict[str, Any],
    label_obs: Dict[str, Any],
    lane: str,
    evidence: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Apply refusal gates in priority order. Returns None when nothing
    blocks; otherwise a dict with blocker + reason + (optional)
    what_would_unblock."""

    if not gap:
        return {
            "blocker": "unclassified_evidence",
            "reason": "No classifier_output / ConversionGap was provided. Exporter requires both evidence AND classifier output.",
        }

    name = gap.get("name")
    surface = gap.get("surface")
    consumer_scope = gap.get("consumer_scope")
    labeler_class = (label_obs or {}).get("labeler_class")

    # 1. No label observation — exporter has nothing to ship.
    if name == "observability_gap" or not label_obs.get("label_value"):
        return {
            "blocker": "no_label_observation",
            "reason": "LabelObservation is absent or missing label_value. Without a witnessed label event there is nothing to export.",
        }

    # 2. Unknown surface MUST NOT export as a specimen. Per Bundle D
    # invariant: unknown is not a specimen; preserve ambiguity instead
    # of laundering it.
    if name == "execution_gap_surface_unknown" or surface == "unknown":
        return {
            "blocker": "unknown_surface_not_specimen",
            "reason": "ConversionGap surface is 'unknown' — the deriver records the label is documented but has no KNOWN_LABEL_SURFACE entry. Exporting would falsely treat surface uncertainty as a substantive moderation finding.",
            "what_would_unblock": "Add an entry to KNOWN_LABEL_SURFACE for this label_value with surface assignment + source/rationale/reviewed_at audit metadata.",
        }

    # 3. First-party labeler with consumer_scope=unknown — refine the
    # blocker name based on whether the labeler's service record was
    # found at all. D.5 distinguishes ingestion-gap-shaped absence from
    # genuine emitter-undeclared absence.
    if consumer_scope == "unknown" and labeler_class == "official_platform":
        emitter_doc = evidence.get("LabelerEmitterDocumentation") or {}
        record_present = emitter_doc.get("labeler_service_record_present")
        emitter_status = emitter_doc.get("status")
        if record_present and emitter_status == "service_record_found_label_not_declared":
            return {
                "blocker": "emitter_does_not_declare_label",
                "reason": (
                    "First-party labeler IS publishing a service record "
                    "(found in discovery_events or service_record_snapshots), "
                    "but the record does not declare a labelValueDefinition "
                    "for this label_value. The label is being emitted "
                    "operationally without an emitter-side rule — not an "
                    "ingestion gap, a genuine emitter-side undeclared label."
                ),
                "what_would_unblock": (
                    "Either the labeler adds a labelValueDefinition for "
                    "this label_value to its service record, OR the label "
                    "becomes documented via upstream_const/protocol_doc."
                ),
            }
        return {
            "blocker": "ingestion_gap_surface_unresolved",
            "reason": "Labeler is official_platform but consumer_scope is 'unknown' AND no service record was found in discovery_events OR service_record_snapshots/. Most likely the labeler's app.bsky.labeler.service record has not been ingested (F-005 ingestion gap).",
            "what_would_unblock": "Ingest the labeler's service record into labelwatch.discovery_events, OR add a snapshot to docs/specimens/service_record_snapshots/<did>.json. F-005 patch.",
        }

    # 4. Other consumer_scope=unknown cases — third-party labelers
    # without a service record (or a service record without this
    # label_value). Real surface absence is possible here, but exporting
    # would still launder uncertainty.
    if consumer_scope == "unknown":
        return {
            "blocker": "provenance_unresolved",
            "reason": "consumer_scope is 'unknown' and no LabelerEmitterDocumentation was found. This may be a real conversion absence OR a missing service record. The ambiguity must be preserved, not exported.",
            "what_would_unblock": "Either confirm the labeler publishes no service record (real absence) or ingest the record and re-derive (likely emitter_declared on re-derive).",
        }

    # 5. Lane-specific freshness gate. Bundle E:
    # freshness blocks only when StateBasis is genuinely absent. With
    # StateBasis present (even with freshness_horizon='unknown'), the
    # candidate exports with a state_basis_status caveat — unknown_basis,
    # stale_basis, or current_basis. Per the invariant: freshness can
    # preserve, caveat, stale, or block. It must not promote.
    if lane == "freshness" and not _has_state_basis(evidence):
        return {
            "blocker": "missing_required_basis",
            "reason": (
                "Lane 'freshness' requires evidence.StateBasis. The packet "
                "has no StateBasis field — exporter cannot reason about "
                "current/stale/unknown without a basis to evaluate."
            ),
            "what_would_unblock": (
                "Populate evidence.StateBasis with source_kind, captured_at, "
                "artifact_identity, freshness_horizon, derivation_source. "
                "The deriver does this for db_row evidence; hand-authored "
                "fixtures must declare their own basis."
            ),
        }

    return None


# --- block builders ------------------------------------------------------

def _labeler_block(label_obs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "did": label_obs.get("labeler_did"),
        "handle": label_obs.get("labeler_handle"),
        "display_name": label_obs.get("labeler_display_name"),
        "class": label_obs.get("labeler_class"),
        "is_reference": label_obs.get("is_reference_labeler"),
    }


def _label_block(label_obs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "value": label_obs.get("label_value"),
        "neg": label_obs.get("neg"),
        "labelwatch_authority_effect": label_obs.get(
            "labelwatch_authority_effect_classification"
        ),
    }


def _target_block(label_obs: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "uri": label_obs.get("target_uri"),
        "kind": label_obs.get("target_kind"),
        "target_did": label_obs.get("target_did"),
    }


def _observation_block(
    label_obs: Dict[str, Any], evidence: Dict[str, Any]
) -> Dict[str, Any]:
    """v1: single-event proxy. state_basis / first_seen / last_seen /
    reassertion_count are stubs until Bundle E wires the sidecar."""
    return {
        "ts": label_obs.get("ts"),
        "first_seen": None,
        "last_seen": None,
        "reassertion_count": None,
        "state_basis": None,
        "current_state": None,
        "state_lookup_note": (
            "v1: single-event proxy only. Bundle E will populate from "
            "label_state sidecar; until then freshness-lane exports are "
            "gated on this absence."
        ),
    }


def _policy_provenance_block(policy_doc: Dict[str, Any]) -> Dict[str, Any]:
    artifact = policy_doc.get("policy_artifact") or {}
    return {
        "artifact_kind": artifact.get("artifact_kind"),
        "consumer_scope": policy_doc.get("consumer_scope"),
        "status": policy_doc.get("status"),
        "execution_surface": policy_doc.get("execution_surface"),
    }


def _emitter_provenance_block(emitter_doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "artifact_kind": emitter_doc.get("artifact_kind"),
        "consumer_scope": emitter_doc.get("consumer_scope"),
        "status": emitter_doc.get("status"),
        "execution_surface": emitter_doc.get("execution_surface"),
    }


def _caveats(
    policy_doc: Dict[str, Any],
    emitter_doc: Dict[str, Any],
    gap: Dict[str, Any],
    lane: str,
    evidence: Dict[str, Any],
    state_basis_status: str,
) -> List[str]:
    caveats: List[str] = []
    consumer_scope = gap.get("consumer_scope")
    if consumer_scope == "emitter_declared":
        caveats.append("non_global_provenance")
    if (evidence.get("RenderObservation") or {}).get("status") == "absent" and \
            gap.get("surface") == "client_render":
        caveats.append("render_execution_unwitnessed")
    if (evidence.get("HostingObservation") or {}).get("status") == "absent" and \
            gap.get("surface") == "pds_hosting":
        caveats.append("hosting_execution_unwitnessed")
    if (evidence.get("PolicyWitness") or {}).get("status") == \
            "partial_documentary_not_receipted":
        caveats.append("policy_witnessed_documentary_only")
    # Bundle E: state-basis caveats. Apply on freshness lane so the
    # candidate cannot be mistaken for current_basis. Authority_surface
    # lane gets the info too (state_basis_status field is present), but
    # we don't add it as a caveat there — basis isn't the authority lane's
    # concern.
    if lane == "freshness":
        if state_basis_status == "unknown_basis":
            caveats.append("unknown_basis")
        elif state_basis_status == "stale_basis":
            caveats.append("stale_basis")
    return caveats


def _has_state_basis(evidence: Dict[str, Any]) -> bool:
    """Bundle E: True iff the evidence packet declares a StateBasis block.
    A missing block means the freshness lane cannot reason about
    current/stale/unknown — that's the missing_required_basis blocker.
    A present block with freshness_horizon='unknown' still counts as
    present; the exporter classifies it as unknown_basis and exports
    with caveat per Bundle E invariant."""
    return bool(evidence.get("StateBasis"))
