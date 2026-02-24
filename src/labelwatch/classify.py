"""Pure classifier for labeler visibility, reachability, and auditability.

No network. No DB. Pure function. Deterministic. Testable with fixtures.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

CLASSIFIER_VERSION = "v1"


@dataclass
class EvidenceDict:
    declared_record_present: bool = False
    did_doc_labeler_service_present: bool = False
    did_doc_label_key_present: bool = False
    observed_label_src: bool = False
    probe_result: Optional[str] = None  # accessible / auth_required / down / None


@dataclass
class Classification:
    visibility_class: str           # declared / protocol_public / observed_only / unresolved
    reachability_state: str         # accessible / auth_required / down / unknown
    auditability: str               # high / medium / low
    classification_confidence: str  # high / medium / low
    reason: str                     # compact explanation
    version: str = CLASSIFIER_VERSION


def classify_labeler(evidence: EvidenceDict) -> Classification:
    """Classify a labeler based on structured evidence.

    Decision tree:
    1. declared_record → visibility = declared
    2. NOT declared + did_doc_service → visibility = protocol_public
    3. observed_src + NOT declared + NOT did_doc → visibility = observed_only
    4. Else → unresolved
    """
    # Determine reachability from probe
    if evidence.probe_result == "accessible":
        reachability = "accessible"
    elif evidence.probe_result == "auth_required":
        reachability = "auth_required"
    elif evidence.probe_result == "down":
        reachability = "down"
    else:
        reachability = "unknown"

    # Build reason parts
    reason_parts = []

    # Visibility class
    if evidence.declared_record_present:
        visibility = "declared"
        reason_parts.append("declared")

        if evidence.did_doc_labeler_service_present:
            reason_parts.append("did_service")
        if evidence.did_doc_label_key_present:
            reason_parts.append("did_label_key")

        if reachability == "accessible":
            auditability = "high"
            reason_parts.append("probe_accessible")
        elif reachability == "auth_required":
            auditability = "medium"
            reason_parts.append("probe_auth_required")
        else:
            auditability = "medium"
            if reachability == "down":
                reason_parts.append("probe_down")
            elif reachability == "unknown":
                reason_parts.append("not_probed")

    elif evidence.did_doc_labeler_service_present:
        visibility = "protocol_public"
        reason_parts.append("protocol_public")
        if evidence.did_doc_label_key_present:
            reason_parts.append("did_label_key")

        if reachability == "accessible":
            auditability = "medium"
            reason_parts.append("probe_accessible")
        elif reachability == "auth_required":
            auditability = "medium"
            reason_parts.append("probe_auth_required")
        else:
            auditability = "medium"
            if reachability == "down":
                reason_parts.append("probe_down")

    elif evidence.observed_label_src:
        visibility = "observed_only"
        reason_parts.append("observed_only_no_declaration")
        auditability = "low"

    else:
        visibility = "unresolved"
        reason_parts.append("unresolved")
        auditability = "low"

    if evidence.observed_label_src and visibility != "observed_only":
        reason_parts.append("observed_src")

    reason = "+".join(reason_parts)

    # Confidence scoring
    confidence = _compute_confidence(evidence)

    return Classification(
        visibility_class=visibility,
        reachability_state=reachability,
        auditability=auditability,
        classification_confidence=confidence,
        reason=reason,
        version=CLASSIFIER_VERSION,
    )


def _compute_confidence(evidence: EvidenceDict) -> str:
    """Compute classification confidence from evidence independence.

    Strong evidence (independent observations):
      - probe_result (accessible/auth_required/down)
      - observed_label_src

    Medium evidence (protocol/registry declarations):
      - declared_record_present
      - did_doc_labeler_service_present
      - did_doc_label_key_present

    Rules:
      - Two strong or one strong + two medium = high
      - One strong + one medium, or two medium = medium
      - Single surface or all weak = low
    """
    strong = 0
    medium = 0

    if evidence.probe_result in ("accessible", "auth_required", "down"):
        strong += 1
    if evidence.observed_label_src:
        strong += 1
    if evidence.declared_record_present:
        medium += 1
    if evidence.did_doc_labeler_service_present:
        medium += 1
    if evidence.did_doc_label_key_present:
        medium += 1

    if strong >= 2 or (strong >= 1 and medium >= 2):
        return "high"
    if (strong >= 1 and medium >= 1) or medium >= 2:
        return "medium"
    return "low"


# Noise detection patterns
_TEST_DEV_PATTERNS = [
    re.compile(r"\btest\b", re.IGNORECASE),
    re.compile(r"\bdev\b", re.IGNORECASE),
    re.compile(r"\bdemo\b", re.IGNORECASE),
    re.compile(r"\bexample\b", re.IGNORECASE),
    re.compile(r"\bsandbox\b", re.IGNORECASE),
    re.compile(r"\btmp\b", re.IGNORECASE),
    re.compile(r"\bfoo\b", re.IGNORECASE),
    re.compile(r"\bbar\b", re.IGNORECASE),
    re.compile(r"^test[-.]", re.IGNORECASE),
    re.compile(r"[-.]test$", re.IGNORECASE),
    re.compile(r"^dev[-.]", re.IGNORECASE),
    re.compile(r"[-.]dev$", re.IGNORECASE),
]


def detect_test_dev(handle: str | None, display_name: str | None) -> bool:
    """Heuristic: is this labeler likely a test/dev instance?

    Checks handle and display_name for test/dev patterns.
    """
    for text in [handle, display_name]:
        if not text:
            continue
        for pat in _TEST_DEV_PATTERNS:
            if pat.search(text):
                return True
    return False
