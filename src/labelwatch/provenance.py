"""Labeler provenance scorecard — is this operator legible, declared, operational?

Does NOT answer whether a labeler is *right*. Answers whether it is:
- legible (can you tell who runs it?)
- declared (does it say what it does?)
- operational (does the infrastructure work?)
- behaviorally coherent (do outputs match declarations?)

Five scoring axes (0-100 total):
  Identity (0-25), Policy (0-20), Infrastructure (0-20),
  Behavior (0-25), Accountability (0-10)
"""
from __future__ import annotations

import json
import logging
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable

log = logging.getLogger(__name__)

APPVIEW_BASE = "https://public.api.bsky.app"
PLC_DIRECTORY_BASE = "https://plc.directory"


class EvidenceClass(str, Enum):
    DECLARED = "declared"
    VERIFIED = "verified"
    BEHAVIOR_INFERRED = "behavior-inferred"
    HUMAN_CONFIRMED = "human-confirmed"


class OperatorLegibility(str, Enum):
    OPAQUE = "opaque"
    NOMINAL = "nominal"
    LEGIBLE = "legible"


class GovernanceMode(str, Enum):
    MODERATION = "moderation"
    CURATION = "curation"
    BADGE_STATUS = "badge-status"
    NOVELTY = "novelty"
    MIXED = "mixed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ScoreBreakdown:
    identity: int
    policy: int
    infrastructure: int
    behavior: int
    accountability: int

    @property
    def total(self) -> int:
        return (self.identity + self.policy + self.infrastructure
                + self.behavior + self.accountability)


@dataclass(frozen=True)
class EvidenceNote:
    code: str
    detail: str
    kind: str  # hard, soft, behavioral, manual


@dataclass(frozen=True)
class ObservedMetrics:
    """Feed from local ingest / SQLite, not from AppView."""
    total_labels_emitted: int = 0
    active_days: int = 0
    novelty_ratio: float = 0.0
    badge_ratio: float = 0.0
    negation_rate: float = 0.0
    scope_adherence: float = 0.0
    ontology_drift: float = 0.0
    account_label_ratio: float = 0.0
    record_label_ratio: float = 0.0
    empty_definition_usage_ratio: float = 0.0
    contradictory_pair_rate: float = 0.0
    docs_url_present: bool = False
    appeals_or_contact_present: bool = False


@dataclass(frozen=True)
class LabelValueDefinition:
    identifier: str
    severity: str | None
    blurs: str | None
    default_setting: str | None
    locales: list[dict[str, Any]] = field(default_factory=list)

    @property
    def has_readable_locale(self) -> bool:
        for locale in self.locales:
            name = (locale.get("name") or "").strip()
            desc = (locale.get("description") or "").strip()
            if name or desc:
                return True
        return False


@dataclass(frozen=True)
class LabelerPolicy:
    label_values: list[str]
    label_value_definitions: list[LabelValueDefinition]


@dataclass(frozen=True)
class CreatorProfile:
    did: str
    handle: str | None
    display_name: str | None
    description: str | None
    avatar: str | None


@dataclass(frozen=True)
class LabelerServiceRecord:
    did: str
    uri: str
    cid: str
    creator: CreatorProfile
    policies: LabelerPolicy
    indexed_at: str | None
    reason_types: list[str]
    subject_types: list[str]
    subject_collections: list[str]


@dataclass(frozen=True)
class DidServiceInfo:
    endpoint: str | None
    signing_key: str | None
    raw_doc: dict[str, Any] | None
    fetch_error: str | None = None


@dataclass(frozen=True)
class ProvenanceSnapshot:
    did: str
    handle: str | None
    display_name: str | None
    operator_legibility: OperatorLegibility
    governance_mode: GovernanceMode
    confidence_class: EvidenceClass
    scores: ScoreBreakdown
    red_flags: list[str]
    evidence: list[EvidenceNote]
    declared_summary: dict[str, Any]
    observed_summary: dict[str, Any]
    mismatch_summary: list[str]
    did_service_endpoint: str | None
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["operator_legibility"] = self.operator_legibility.value
        data["governance_mode"] = self.governance_mode.value
        data["confidence_class"] = self.confidence_class.value
        return data


# ---------------------------------------------------------------------------
# Network client
# ---------------------------------------------------------------------------

class BskyClient:
    def __init__(
        self,
        appview_base: str = APPVIEW_BASE,
        plc_base: str = PLC_DIRECTORY_BASE,
        timeout: float = 10.0,
        user_agent: str = "labelwatch-provenance/0.1",
    ) -> None:
        self.appview_base = appview_base.rstrip("/")
        self.plc_base = plc_base.rstrip("/")
        self.timeout = timeout
        self.user_agent = user_agent
        self._ssl_context = ssl.create_default_context()

    def _get_json(self, url: str) -> dict[str, Any]:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
        )
        try:
            with urllib.request.urlopen(
                req, timeout=self.timeout, context=self._ssl_context,
            ) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Request failed for {url}: {exc}") from exc

    def get_labeler_service(self, did: str) -> LabelerServiceRecord:
        qs = urllib.parse.urlencode(
            [("dids", did), ("detailed", "true")], doseq=True,
        )
        url = f"{self.appview_base}/xrpc/app.bsky.labeler.getServices?{qs}"
        data = self._get_json(url)
        views = data.get("views") or []
        if not views:
            raise RuntimeError(f"No labeler service returned for DID {did}")

        view = views[0]
        creator_raw = view.get("creator") or {}
        creator = CreatorProfile(
            did=creator_raw.get("did", did),
            handle=creator_raw.get("handle"),
            display_name=creator_raw.get("displayName"),
            description=creator_raw.get("description"),
            avatar=creator_raw.get("avatar"),
        )

        policies_raw = view.get("policies") or {}
        defs = []
        for item in policies_raw.get("labelValueDefinitions") or []:
            defs.append(LabelValueDefinition(
                identifier=item.get("identifier", ""),
                severity=item.get("severity"),
                blurs=item.get("blurs"),
                default_setting=item.get("defaultSetting"),
                locales=item.get("locales") or [],
            ))

        policies = LabelerPolicy(
            label_values=list(policies_raw.get("labelValues") or []),
            label_value_definitions=defs,
        )

        return LabelerServiceRecord(
            did=creator.did, uri=view["uri"], cid=view["cid"],
            creator=creator, policies=policies,
            indexed_at=view.get("indexedAt"),
            reason_types=list(view.get("reasonTypes") or []),
            subject_types=list(view.get("subjectTypes") or []),
            subject_collections=list(view.get("subjectCollections") or []),
        )

    def fetch_did_service_info(self, did: str) -> DidServiceInfo:
        try:
            doc = self._fetch_did_doc(did)
            services = doc.get("service") or []
            endpoint = None
            for svc in services:
                svc_id = svc.get("id", "")
                svc_type = svc.get("type", "")
                if svc_id.endswith("#atproto_labeler") or svc_type == "AtprotoLabeler":
                    endpoint = svc.get("serviceEndpoint")
                    break

            signing_key = None
            for method in doc.get("verificationMethod") or []:
                key = method.get("publicKeyMultibase") or method.get("publicKeyJwk")
                if key:
                    signing_key = str(key)
                    break

            return DidServiceInfo(
                endpoint=endpoint, signing_key=signing_key, raw_doc=doc,
            )
        except Exception as exc:
            return DidServiceInfo(
                endpoint=None, signing_key=None, raw_doc=None,
                fetch_error=str(exc),
            )

    def _fetch_did_doc(self, did: str) -> dict[str, Any]:
        if did.startswith("did:plc:"):
            url = f"{self.plc_base}/{urllib.parse.quote(did, safe=':')}"
            return self._get_json(url)
        if did.startswith("did:web:"):
            host = did.removeprefix("did:web:").replace(":", "/")
            if "/" in host:
                url = f"https://{host}/did.json"
            else:
                url = f"https://{host}/.well-known/did.json"
            return self._get_json(url)
        raise RuntimeError(f"Unsupported DID method: {did}")


# ---------------------------------------------------------------------------
# Derivation helpers
# ---------------------------------------------------------------------------

def derive_governance_mode(
    service: LabelerServiceRecord, observed: ObservedMetrics,
) -> GovernanceMode:
    values = {v.lower() for v in service.policies.label_values}

    if observed.badge_ratio >= 0.70:
        return GovernanceMode.BADGE_STATUS
    if observed.novelty_ratio >= 0.80 and observed.scope_adherence < 0.50:
        return GovernanceMode.NOVELTY

    moderation_tokens = {
        "porn", "sexual", "nudity", "graphic-media", "self-harm", "spam",
        "scam", "impersonation", "hate", "harassment", "violence",
    }
    if values & moderation_tokens:
        return GovernanceMode.MODERATION

    if (service.policies.label_value_definitions
            and observed.badge_ratio < 0.40
            and observed.novelty_ratio < 0.50):
        return GovernanceMode.CURATION

    if not values:
        return GovernanceMode.UNKNOWN
    return GovernanceMode.MIXED


def derive_operator_legibility(
    service: LabelerServiceRecord,
    did_info: DidServiceInfo,
    observed: ObservedMetrics,
) -> OperatorLegibility:
    score = 0
    if service.creator.handle:
        score += 1
    if service.creator.display_name:
        score += 1
    if service.creator.description:
        score += 1
    if did_info.endpoint:
        score += 1
    if observed.docs_url_present:
        score += 1
    if observed.appeals_or_contact_present:
        score += 1

    if score <= 1:
        return OperatorLegibility.OPAQUE
    if score <= 3:
        return OperatorLegibility.NOMINAL
    return OperatorLegibility.LEGIBLE


def derive_confidence_class(
    did_info: DidServiceInfo,
    observed: ObservedMetrics,
    human_confirmed: bool = False,
) -> EvidenceClass:
    if human_confirmed:
        return EvidenceClass.HUMAN_CONFIRMED
    if observed.total_labels_emitted > 0:
        return EvidenceClass.BEHAVIOR_INFERRED
    if did_info.endpoint:
        return EvidenceClass.VERIFIED
    return EvidenceClass.DECLARED


# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------

def score_identity(
    service: LabelerServiceRecord, did_info: DidServiceInfo,
    evidence: list[EvidenceNote], red_flags: list[str],
) -> int:
    score = 0
    if service.creator.handle:
        score += 5
        evidence.append(EvidenceNote("handle-present", service.creator.handle, "hard"))
    else:
        red_flags.append("no-handle")

    if service.creator.display_name:
        score += 5
        evidence.append(EvidenceNote("display-name", service.creator.display_name, "soft"))
    else:
        red_flags.append("no-display-name")

    if service.creator.description:
        score += 4
        evidence.append(EvidenceNote("profile-description", "present", "soft"))
    else:
        red_flags.append("no-profile-description")

    if service.did.startswith(("did:plc:", "did:web:")):
        score += 5
        evidence.append(EvidenceNote("supported-did-method", service.did.split(":")[1], "hard"))

    if did_info.endpoint:
        score += 6
        evidence.append(EvidenceNote("did-labeler-endpoint", did_info.endpoint, "hard"))
    else:
        red_flags.append("no-labeler-endpoint-in-did")

    return min(score, 25)


def score_policy(
    service: LabelerServiceRecord,
    evidence: list[EvidenceNote], red_flags: list[str],
) -> int:
    score = 0
    policies = service.policies

    if policies.label_values:
        score += 6
        evidence.append(EvidenceNote(
            "label-values", f"{len(policies.label_values)} declared", "hard"))
    else:
        red_flags.append("no-declared-label-values")

    if policies.label_value_definitions:
        score += 5
        evidence.append(EvidenceNote(
            "custom-label-definitions",
            f"{len(policies.label_value_definitions)} custom", "hard"))

    readable = sum(1 for d in policies.label_value_definitions if d.has_readable_locale)
    if readable:
        score += min(5, readable)
        evidence.append(EvidenceNote(
            "readable-definitions", f"{readable} with locale text", "hard"))
    elif policies.label_value_definitions:
        red_flags.append("custom-labels-without-readable-definitions")

    scoped = sum(1 for x in [
        service.reason_types, service.subject_types, service.subject_collections
    ] if x)
    score += min(4, scoped * 2)
    if scoped == 0:
        red_flags.append("no-explicit-scope-fields")
    else:
        evidence.append(EvidenceNote(
            "scope-fields",
            f"reason={len(service.reason_types)} subject={len(service.subject_types)} "
            f"collections={len(service.subject_collections)}", "hard"))

    return min(score, 20)


def score_infrastructure(
    did_info: DidServiceInfo,
    evidence: list[EvidenceNote], red_flags: list[str],
) -> int:
    score = 0
    if did_info.endpoint:
        score += 12
        evidence.append(EvidenceNote("endpoint-reachable", did_info.endpoint, "hard"))
    else:
        red_flags.append("did-endpoint-missing-or-unreachable")

    if did_info.signing_key:
        score += 4
        evidence.append(EvidenceNote("signing-key", "present", "hard"))
    else:
        red_flags.append("no-signing-key-observed")

    if did_info.raw_doc:
        score += 4
        evidence.append(EvidenceNote("did-doc", "fetched", "hard"))

    return min(score, 20)


def score_behavior(
    observed: ObservedMetrics,
    evidence: list[EvidenceNote], red_flags: list[str],
) -> int:
    score = 0

    if observed.total_labels_emitted > 0:
        score += 4
        evidence.append(EvidenceNote(
            "observed-labels",
            f"{observed.total_labels_emitted} over {observed.active_days}d", "behavioral"))
    else:
        red_flags.append("no-observed-label-history")

    score += max(0, min(8, int(round(observed.scope_adherence * 8))))
    evidence.append(EvidenceNote(
        "scope-adherence", f"{observed.scope_adherence:.2f}", "behavioral"))
    if observed.scope_adherence < 0.50:
        red_flags.append("low-scope-adherence")

    score += max(0, 4 - int(round(observed.novelty_ratio * 4)))
    if observed.novelty_ratio > 0.70:
        red_flags.append("high-novelty-ratio")

    score += max(0, 4 - int(round(observed.ontology_drift * 4)))
    if observed.ontology_drift > 0.60:
        red_flags.append("high-ontology-drift")

    score += max(0, 3 - int(round(observed.contradictory_pair_rate * 3)))
    if observed.contradictory_pair_rate > 0.20:
        red_flags.append("high-contradictory-pair-rate")

    score += max(0, 2 - int(round(observed.negation_rate * 2)))
    if observed.negation_rate > 0.40:
        red_flags.append("high-negation-rate")

    return min(score, 25)


def score_accountability(
    service: LabelerServiceRecord, observed: ObservedMetrics,
    evidence: list[EvidenceNote], red_flags: list[str],
) -> int:
    score = 0
    readable = sum(1 for d in service.policies.label_value_definitions
                   if d.has_readable_locale)
    if readable > 0:
        score += 3
        evidence.append(EvidenceNote("human-readable-label-docs", str(readable), "hard"))
    else:
        red_flags.append("no-human-readable-label-docs")

    if observed.docs_url_present:
        score += 3
        evidence.append(EvidenceNote("docs-url", "present", "soft"))
    else:
        red_flags.append("no-docs-url")

    if observed.appeals_or_contact_present:
        score += 4
        evidence.append(EvidenceNote("appeal-contact", "present", "soft"))
    else:
        red_flags.append("no-appeal-or-contact-path")

    return min(score, 10)


# ---------------------------------------------------------------------------
# Mismatch detection
# ---------------------------------------------------------------------------

def derive_mismatches(
    service: LabelerServiceRecord, observed: ObservedMetrics,
) -> list[str]:
    mismatches: list[str] = []

    if (service.reason_types or service.subject_types or service.subject_collections):
        if observed.scope_adherence < 0.50:
            mismatches.append("declared scope is substantially looser in practice")

    if (service.policies.label_value_definitions
            and observed.empty_definition_usage_ratio > 0.30):
        mismatches.append("custom labels used beyond their definitions suggest")

    if observed.badge_ratio > 0.70:
        mod_tokens = {"spam", "scam", "harassment", "porn", "hate"}
        if mod_tokens & {v.lower() for v in service.policies.label_values}:
            mismatches.append("moderation vocabulary declared but behavior is badge-heavy")

    if observed.contradictory_pair_rate > 0.20:
        mismatches.append("contradictions exceed expected moderation churn")

    if observed.ontology_drift > 0.60:
        mismatches.append("label ontology appears unstable over time")

    return mismatches


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------

def summarize_declared(service: LabelerServiceRecord) -> dict[str, Any]:
    return {
        "label_values": service.policies.label_values,
        "custom_label_definitions": [
            {
                "identifier": d.identifier,
                "has_readable_locale": d.has_readable_locale,
                "severity": d.severity,
                "blurs": d.blurs,
                "default_setting": d.default_setting,
            }
            for d in service.policies.label_value_definitions
        ],
        "reason_types": service.reason_types,
        "subject_types": service.subject_types,
        "subject_collections": service.subject_collections,
    }


def build_provenance_snapshot(
    service: LabelerServiceRecord,
    did_info: DidServiceInfo,
    observed: ObservedMetrics,
    *,
    human_confirmed: bool = False,
) -> ProvenanceSnapshot:
    evidence: list[EvidenceNote] = []
    red_flags: list[str] = []

    scores = ScoreBreakdown(
        identity=score_identity(service, did_info, evidence, red_flags),
        policy=score_policy(service, evidence, red_flags),
        infrastructure=score_infrastructure(did_info, evidence, red_flags),
        behavior=score_behavior(observed, evidence, red_flags),
        accountability=score_accountability(service, observed, evidence, red_flags),
    )

    return ProvenanceSnapshot(
        did=service.did,
        handle=service.creator.handle,
        display_name=service.creator.display_name,
        operator_legibility=derive_operator_legibility(service, did_info, observed),
        governance_mode=derive_governance_mode(service, observed),
        confidence_class=derive_confidence_class(did_info, observed, human_confirmed),
        scores=scores,
        red_flags=sorted(set(red_flags)),
        evidence=evidence,
        declared_summary=summarize_declared(service),
        observed_summary=asdict(observed),
        mismatch_summary=derive_mismatches(service, observed),
        did_service_endpoint=did_info.endpoint,
        generated_at=datetime.now(tz=timezone.utc).isoformat(),
    )


def snapshot_for_did(
    did: str, observed: ObservedMetrics,
    *, client: BskyClient | None = None,
    human_confirmed: bool = False,
) -> ProvenanceSnapshot:
    client = client or BskyClient()
    service = client.get_labeler_service(did)
    did_info = client.fetch_did_service_info(did)
    return build_provenance_snapshot(
        service, did_info, observed, human_confirmed=human_confirmed,
    )


# ---------------------------------------------------------------------------
# SQLite adapter — derive ObservedMetrics from labelwatch DB
# ---------------------------------------------------------------------------

def derive_observed_metrics(conn, labeler_did: str) -> ObservedMetrics:
    """Derive ObservedMetrics from labelwatch's existing tables."""
    # Total labels emitted
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()
    total = row["c"] if row else 0

    if total == 0:
        return ObservedMetrics()

    # Active days
    row = conn.execute(
        "SELECT COUNT(DISTINCT DATE(ts)) AS d FROM label_events WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()
    active_days = row["d"] if row else 0

    # Negation rate
    row = conn.execute(
        "SELECT COALESCE(SUM(CASE WHEN neg = 1 THEN 1 ELSE 0 END), 0) AS negs "
        "FROM label_events WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()
    negation_rate = row["negs"] / total if total else 0.0

    # Account vs record label ratio
    row = conn.execute(
        "SELECT "
        "  COALESCE(SUM(CASE WHEN uri LIKE 'did:%' THEN 1 ELSE 0 END), 0) AS acct, "
        "  COALESCE(SUM(CASE WHEN uri LIKE 'at://%' THEN 1 ELSE 0 END), 0) AS rec "
        "FROM label_events WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()
    acct = row["acct"]
    rec = row["rec"]
    denom = acct + rec or 1
    account_label_ratio = acct / denom
    record_label_ratio = rec / denom

    # Distinct label values (for novelty/badge estimation)
    vals_rows = conn.execute(
        "SELECT val, COUNT(*) AS c FROM label_events "
        "WHERE labeler_did = ? GROUP BY val ORDER BY c DESC",
        (labeler_did,),
    ).fetchall()

    # Check profile for docs/contact hints
    labeler_row = conn.execute(
        "SELECT handle, display_name FROM labelers WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()

    # Flip-flop / contradiction rate (same uri+val, both apply and negate)
    row = conn.execute(
        "SELECT COUNT(DISTINCT uri || '|' || val) AS pairs "
        "FROM label_events WHERE labeler_did = ? AND neg = 1",
        (labeler_did,),
    ).fetchone()
    negated_pairs = row["pairs"] if row else 0
    row = conn.execute(
        "SELECT COUNT(DISTINCT uri || '|' || val) AS pairs "
        "FROM label_events WHERE labeler_did = ?",
        (labeler_did,),
    ).fetchone()
    total_pairs = row["pairs"] if row else 1
    contradictory_pair_rate = negated_pairs / total_pairs if total_pairs else 0.0

    return ObservedMetrics(
        total_labels_emitted=total,
        active_days=active_days,
        novelty_ratio=0.0,  # requires domain classification (Phase 2)
        badge_ratio=0.0,    # requires domain classification (Phase 2)
        negation_rate=negation_rate,
        scope_adherence=1.0,  # requires declared vs observed comparison
        ontology_drift=0.0,   # requires temporal analysis
        account_label_ratio=account_label_ratio,
        record_label_ratio=record_label_ratio,
        empty_definition_usage_ratio=0.0,
        contradictory_pair_rate=contradictory_pair_rate,
        docs_url_present=False,
        appeals_or_contact_present=False,
    )
