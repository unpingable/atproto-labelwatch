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

Refusal vocabulary (Bundle D v1):
  no_label_observation             — gap.name = observability_gap
  unknown_surface_not_specimen     — surface=unknown OR gap.name=execution_gap_surface_unknown
  ingestion_gap_surface_unresolved — labeler is first-party / official_platform AND consumer_scope=unknown
                                     (likely the service-record-discovery gap of F-005)
  provenance_unresolved            — consumer_scope=unknown for a non-first-party labeler
  requires_state_basis             — lane=freshness AND no state_basis/first_seen data in evidence
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


EXPORTER_VERSION = "specimen_exporter.py v1 (Bundle D skeleton)"
SCHEMA_VERSION = 1


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

    candidate = {
        **base,
        "schema_kind": "specimen_candidate",
        "observation": _observation_block(label_obs, evidence),
        "policy_provenance": _policy_provenance_block(policy_doc),
        "emitter_provenance": _emitter_provenance_block(emitter_doc),
        "consumer_scope_effective": gap.get("consumer_scope"),
        "conversion_gap": gap,
        "export_caveats": _caveats(policy_doc, emitter_doc, gap, lane, evidence),
    }
    return candidate


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

    # 3. First-party labeler with consumer_scope=unknown is most likely
    # the F-005 service-record-discovery ingestion gap, NOT a real
    # absence of consumer policy.
    if consumer_scope == "unknown" and labeler_class == "official_platform":
        return {
            "blocker": "ingestion_gap_surface_unresolved",
            "reason": "Labeler is official_platform but consumer_scope is 'unknown'. Most likely the labeler's app.bsky.labeler.service record is not in labelwatch's discovery_events (F-005 ingestion gap). The default client operationally honors first-party labels via auto-subscription, so this 'unknown' is almost certainly instrumentation failure, not real surface absence.",
            "what_would_unblock": "Ingest the labeler's service record into labelwatch.discovery_events so LabelerEmitterDocumentation can populate. F-005 patch.",
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

    # 5. Lane-specific freshness requirements. v1 always blocks freshness
    # because state_basis / first_seen aren't yet populated by the
    # deriver (Bundle E will wire that path).
    if lane == "freshness" and not _has_state_basis(evidence):
        return {
            "blocker": "requires_state_basis",
            "reason": "Lane 'freshness' requires state_basis / first_seen / reassertion_count from the label_state sidecar. The deriver does not yet populate these fields; Bundle E adds the sidecar lookup.",
            "what_would_unblock": "Bundle E: wire derive_evidence.py to label_state sidecar lookups; populate observation.first_seen, observation.state_basis, observation.reassertion_count.",
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
    return caveats


def _has_state_basis(evidence: Dict[str, Any]) -> bool:
    """v1: always False because the deriver doesn't populate state fields
    yet. Bundle E flips this to a real check."""
    return False
