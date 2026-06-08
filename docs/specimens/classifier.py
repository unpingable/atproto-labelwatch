"""Deterministic specimen classifier.

Reads a specimen evidence bundle (see *.evidence.json) and derives:
  - ConversionGap
  - admissible_claims
  - inadmissible_claims

This is the REFERENCE implementation of the testimony->constraint
admissibility rules. The formal-layer Lean rules should derive the same
conclusions from the same evidence. If they don't, one of them is wrong.

Inputs are pure evidence: what was observed/documented, what consumer,
what render_context, what artifact refs, what timestamps, what
provenance. Inputs MUST NOT contain a pre-baked gap classification or
hand-authored admissible/inadmissible lists — those are this module's
output.

Success conditions (per the spec):
  - Changing the golden's expected gap should only fail comparison
    (this classifier doesn't see the golden).
  - Changing evidence presence/absence should change the derived gap
    (this classifier walks the evidence shape).
  - The gap must not be hand-authored into the Lean input.

Derivation frame (accepted by the formal layer):
  Evidence bundles derive ADMISSIBLE CLAIMS. They do NOT assert
  factual non-occurrence. Negative findings are SCOPED to a named
  consumer/context — never global.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# --- gap classification --------------------------------------------------

# Status vocabularies. These are the evidence-layer enums the classifier
# discriminates on. The formal layer should match these strings exactly.
POLICY_STATUS_ABSENT = {"absent", "absent_for_consumer"}
POLICY_STATUS_DOCUMENTED = {"documented"}
POLICY_STATUS_WITNESSED = {"witnessed_live"}
RENDER_STATUS_ABSENT = {"absent"}
RENDER_STATUS_OBSERVED = {"observed"}
HOSTING_STATUS_ABSENT = {"absent"}
HOSTING_STATUS_OBSERVED = {"observed"}

# execution_surface vocabulary, sourced from PolicyDocumentation. Describes
# WHERE the documented conversion acts — not whether the gap exists.
EXECUTION_SURFACE_CLIENT_RENDER = "client_render"
EXECUTION_SURFACE_PDS_HOSTING = "pds_hosting"
EXECUTION_SURFACE_MIXED = "mixed"
EXECUTION_SURFACE_UNKNOWN = "unknown"


def classify_evidence(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Run the full derivation on one evidence bundle. Pure function."""
    label = evidence.get("LabelObservation")
    policy_doc = evidence.get("PolicyDocumentation") or {}
    policy_wit = evidence.get("PolicyWitness") or {}
    render = evidence.get("RenderObservation") or {}
    hosting = evidence.get("HostingObservation") or {}

    gap = _classify_gap(label, policy_doc, policy_wit, render, hosting)
    admissible = _derive_admissible(evidence, gap)
    inadmissible = _derive_inadmissible(evidence, gap)
    return {
        "ConversionGap": gap,
        "admissible_claims": admissible,
        "inadmissible_claims": inadmissible,
    }


def _classify_gap(
    label: Optional[Dict[str, Any]],
    policy_doc: Dict[str, Any],
    policy_wit: Dict[str, Any],
    render: Dict[str, Any],
    hosting: Dict[str, Any],
) -> Dict[str, Any]:
    """Discriminator over (A, B, C) presence + B's documentary/witnessed split
    + execution_surface from PolicyDocumentation.

    Returns a struct {name: str, surface: Optional[str]}. Possible names:
      observability_gap                  -- no label witnessed (A absent)
      conversion_witness_gap_no_consumer -- A present; B absent for consumer
      execution_gap_policy_present       -- A + B(documented); execution
                                            observation absent on the surface
                                            the policy acts on
      complete_path                      -- A + B(witnessed live) +
                                            execution observed on the policy
                                            surface
      execution_observed_without_policy_witness
      unclassified                       -- evidence shape doesn't match any
                                            of the above (treat as bug / new case)

    `surface` is populated from PolicyDocumentation.execution_surface when
    PolicyDocumentation is present; absent (None) otherwise.
    """
    if not label:
        return {"name": "observability_gap", "surface": None}

    policy_status = policy_doc.get("status")
    surface = policy_doc.get("execution_surface")
    render_status = render.get("status")
    hosting_status = hosting.get("status")

    if policy_status in POLICY_STATUS_ABSENT:
        return {"name": "conversion_witness_gap_no_consumer", "surface": None}

    if policy_status in POLICY_STATUS_DOCUMENTED:
        # Surface-unknown is its own gap: we have a documented policy but
        # the deriver could not (or chose not to) say which surface the
        # conversion acts on. The classifier MUST NOT silently default to
        # render or hosting — that would let an upstream change quietly
        # become a render-shaped claim about a label that's actually
        # hosting-shaped (or vice versa). The discipline: emit
        # execution_gap_surface_unknown so the formal layer sees the
        # uncertainty as a typed object.
        if surface == EXECUTION_SURFACE_UNKNOWN:
            return {
                "name": "execution_gap_surface_unknown",
                "surface": EXECUTION_SURFACE_UNKNOWN,
            }
        # Legacy / pre-migration packets without a surface field — emit a
        # distinct gap value so they can be found and migrated, rather
        # than silently classifying as render-side.
        if surface is None:
            return {
                "name": "execution_gap_surface_unspecified",
                "surface": None,
            }

        execution_witnessed = _execution_witnessed_on_surface(
            surface, render_status, hosting_status
        )
        if not execution_witnessed:
            return {"name": "execution_gap_policy_present", "surface": surface}
        if policy_status in POLICY_STATUS_WITNESSED:
            return {"name": "complete_path", "surface": surface}
        return {"name": "execution_observed_without_policy_witness", "surface": surface}

    return {"name": "unclassified", "surface": None}


def _execution_witnessed_on_surface(
    surface: str,
    render_status: Optional[str],
    hosting_status: Optional[str],
) -> bool:
    """Surface-aware predicate for whether an execution observation exists.
    Caller is responsible for handling surface in {unknown, None} BEFORE
    calling this — this predicate is only well-defined for the three known
    surfaces (client_render, pds_hosting, mixed)."""
    if surface == EXECUTION_SURFACE_CLIENT_RENDER:
        return render_status in RENDER_STATUS_OBSERVED
    if surface == EXECUTION_SURFACE_PDS_HOSTING:
        return hosting_status in HOSTING_STATUS_OBSERVED
    if surface == EXECUTION_SURFACE_MIXED:
        return (
            render_status in RENDER_STATUS_OBSERVED
            or hosting_status in HOSTING_STATUS_OBSERVED
        )
    raise ValueError(
        f"_execution_witnessed_on_surface called with surface={surface!r}; "
        f"only client_render / pds_hosting / mixed are valid. Caller must "
        f"branch on unknown / None before calling."
    )


# --- admissible claim derivation -----------------------------------------

def _derive_admissible(
    evidence: Dict[str, Any], gap: str
) -> List[Dict[str, Any]]:
    """Template-driven admissible claim generation from evidence shape."""
    claims: List[Dict[str, Any]] = []
    label = evidence.get("LabelObservation") or {}
    policy = evidence.get("PolicyDocumentation") or {}
    consumer = (policy.get("consumer") or {}).get("consumer_id", "unnamed-consumer")

    if label:
        claims.append({
            "id": "label_exists",
            "scope": "this specimen",
            "claim": (
                f"A label record with label_value='{label.get('label_value')}' "
                f"exists on {label.get('target_uri')}, issued by "
                f"{label.get('labeler_handle') or label.get('labeler_did')} "
                f"at {label.get('ts')}."
            ),
            "derivation_basis": ["LabelObservation"],
        })

    if policy.get("status") in POLICY_STATUS_DOCUMENTED:
        action = (
            (policy.get("documented_expected_action") or {})
            .get("action_for_post_render_under_render_context", "<unspecified action>")
        )
        claims.append({
            "id": "policy_rule_documented",
            "scope": f"PolicyDocumentation.consumer ({consumer})",
            "claim": (
                "The published policy at the pinned artifact maps the "
                f"label_value to action: {action}, under the documented render_context."
            ),
            "derivation_basis": ["PolicyDocumentation.policy_artifact"],
        })
        claims.append({
            "id": "conditional_render_under_policy",
            "scope": f"consumer={consumer} + render_context",
            "claim": (
                "Under the named consumer with the stated render_context, the "
                "documented policy would apply the documented action to this target."
            ),
            "derivation_basis": ["LabelObservation", "PolicyDocumentation"],
            "qualifier": (
                "Conditional derivation over the documented rule; not over actual "
                "render execution."
            ),
        })

    if policy.get("status") in POLICY_STATUS_ABSENT:
        claims.append({
            "id": "no_rule_for_consumer",
            "scope": f"PolicyDocumentation.consumer ({consumer})",
            "claim": (
                "Under the named consumer's published policy pipeline at the pinned "
                "artifact, no rule exists that maps this label_value to any render action."
            ),
            "derivation_basis": ["PolicyDocumentation.policy_artifact_searched"],
        })
        claims.append({
            "id": "no_constraint_derivable_for_consumer",
            "scope": f"consumer={consumer} + EvidenceBundle",
            "claim": (
                "From this evidence bundle, no constraint may be derived for the "
                "named consumer from this label record."
            ),
            "derivation_basis": ["LabelObservation", "PolicyDocumentation"],
            "qualifier": (
                "Epistemic non-derivation, not a factual assertion of non-occurrence. "
                "Scoped to the named consumer."
            ),
        })
    return claims


# --- inadmissible claim derivation ---------------------------------------

def _derive_inadmissible(
    evidence: Dict[str, Any], gap: str
) -> List[Dict[str, Any]]:
    """Template-driven inadmissible claim generation. Some entries are
    universal (the no-laundering theorem); others depend on what's absent.
    """
    claims: List[Dict[str, Any]] = []

    # Always: the no-laundering rule. Universally inadmissible regardless of
    # which evidence is present.
    claims.append({
        "id": "no_laundering",
        "claim_form": "label_observed entails render_observed",
        "why_inadmissible": (
            "Laundering: collapses producer/consumer/enforcer distinction. "
            "label_observed alone does not derive render_observed. The "
            "no-laundering theorem forbids this derivation."
        ),
    })

    surface = (evidence.get("PolicyDocumentation") or {}).get("execution_surface")
    render_status = (evidence.get("RenderObservation") or {}).get("status")
    hosting_status = (evidence.get("HostingObservation") or {}).get("status")

    # Surface=unknown gets a specific inadmissible: no execution claim of
    # any kind is admissible because the deriver could not (or chose not
    # to) say which observation type would close the gap.
    if surface == EXECUTION_SURFACE_UNKNOWN:
        claims.append({
            "id": "no_execution_claim_surface_unknown",
            "claim_form": (
                "Any claim that this label was applied (rendered, removed, "
                "warned, blurred, etc.) on this target"
            ),
            "why_inadmissible": (
                "PolicyDocumentation.execution_surface is 'unknown' — the "
                "deriver records the label is in the global LABELS set but "
                "has not been assigned a surface in KNOWN_LABEL_SURFACE. "
                "Classifier must not default to render or hosting. The "
                "label requires manual surface assignment before any "
                "execution claim is admissible."
            ),
        })
        # Skip the surface-specific render/hosting claims entirely below.
        # Fall through to the policy-status checks for the negative-side
        # inadmissibles (which don't apply since policy IS documented).
        return claims

    # Render-side inadmissible claims fire when the policy's surface
    # involves client_render and the render observation is absent.
    render_relevant = surface in (
        EXECUTION_SURFACE_CLIENT_RENDER, EXECUTION_SURFACE_MIXED, None,
    )
    if render_relevant and render_status in RENDER_STATUS_ABSENT:
        claims.append({
            "id": "no_individual_render_claim",
            "claim_form": "This post was rendered with action X for user U at time T",
            "why_inadmissible": (
                "Requires RenderObservation, which is absent. Cannot be derived from "
                "LabelObservation + PolicyDocumentation alone."
            ),
        })
        claims.append({
            "id": "no_population_render_claim",
            "claim_form": "All renders of this post produce the documented action",
            "why_inadmissible": (
                "Population claim. Policy may be configurable; viewer may override; "
                "live client may run a different policy version. Requires probe "
                "campaign with defined sampling frame, not derivation from this "
                "evidence bundle."
            ),
        })

    # Hosting-side inadmissible claims fire when the policy's surface
    # involves pds_hosting and the hosting observation is absent.
    hosting_relevant = surface in (
        EXECUTION_SURFACE_PDS_HOSTING, EXECUTION_SURFACE_MIXED,
    )
    if hosting_relevant and hosting_status in HOSTING_STATUS_ABSENT:
        claims.append({
            "id": "no_individual_hosting_claim",
            "claim_form": "This subject was removed/withheld at the PDS at time T",
            "why_inadmissible": (
                "Requires HostingObservation (a hosting-side probe or PDS state "
                "receipt), which is absent. The documented policy declares a "
                "hosting-layer effect, but the effect's application has not been "
                "directly observed."
            ),
        })
        claims.append({
            "id": "no_population_hosting_claim",
            "claim_form": "This subject is unavailable across all hosting consumers",
            "why_inadmissible": (
                "Population claim. PDS state may vary by mirror, by appview, by "
                "cache; hosting takedowns can be partially propagated. Requires "
                "probe across the relevant consumer surface, not derivation from "
                "this evidence bundle."
            ),
        })

    policy_status = (evidence.get("PolicyDocumentation") or {}).get("status")
    if policy_status in POLICY_STATUS_ABSENT:
        claims.append({
            "id": "no_global_non_occurrence",
            "claim_form": "This label produced no constraint anywhere",
            "why_inadmissible": (
                "Factual non-occurrence claim. The evidence bundle is silent over "
                "other consumer configurations; inferring a global negative from a "
                "scoped derivation is the laundering shape in reverse. Per the "
                "derivation frame: evidence bundles derive admissible claims, they "
                "do not assert factual non-occurrence."
            ),
        })
        claims.append({
            "id": "no_meaning_judgment",
            "claim_form": "The label is meaningless / unimportant",
            "why_inadmissible": (
                "Out of scope. Labelwatch observes; whether testimony is meaningful "
                "is a different judgment requiring different inputs. The calculus "
                "must remain silent on importance."
            ),
        })
        claims.append({
            "id": "no_labeler_effect_claim",
            "claim_form": "The labeler's labeling activity has no operational effect",
            "why_inadmissible": (
                "Adoption is a free variable. The evidence bundle documents one "
                "consumer's zero-conversion case; it does not measure the labeler's "
                "reach across all possible consumer configurations."
            ),
        })

    return claims
