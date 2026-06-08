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


def classify_evidence(evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Run the full derivation on one evidence bundle. Pure function."""
    label = evidence.get("LabelObservation")
    policy_doc = evidence.get("PolicyDocumentation") or {}
    policy_wit = evidence.get("PolicyWitness") or {}
    render = evidence.get("RenderObservation") or {}

    gap = _classify_gap(label, policy_doc, policy_wit, render)
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
) -> str:
    """Discriminator over (A, B, C) presence + B's documentary/witnessed split.

    Returns one of:
      observability_gap                  -- no label witnessed (A absent)
      conversion_witness_gap_no_consumer -- A present; B absent for consumer
      execution_gap_policy_present       -- A + B(documented); C absent
      complete_path                      -- A + B(witnessed live) + C(observed)
      unclassified                       -- evidence shape doesn't match any
                                            of the above (treat as bug / new case)
    """
    if not label:
        return "observability_gap"

    policy_status = policy_doc.get("status")
    render_status = render.get("status")

    if policy_status in POLICY_STATUS_ABSENT:
        return "conversion_witness_gap_no_consumer"

    if policy_status in POLICY_STATUS_DOCUMENTED and render_status in RENDER_STATUS_ABSENT:
        return "execution_gap_policy_present"

    if render_status in RENDER_STATUS_OBSERVED:
        # complete_path requires B witnessed too, otherwise we'd be saying
        # "rendered" without a verifiable policy chain.
        if policy_status in POLICY_STATUS_WITNESSED:
            return "complete_path"
        return "execution_observed_without_policy_witness"

    return "unclassified"


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

    render_status = (evidence.get("RenderObservation") or {}).get("status")
    if render_status in RENDER_STATUS_ABSENT:
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
