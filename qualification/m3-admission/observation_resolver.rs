//! Application-owned factual prerequisite adapter, not an authority issuer.
//!
//! Compile against the enrolled AG-ng and NQ-ng revisions. The enrollment owner
//! binds this exact cleanup step, source, scope and native request; arbitrary
//! caller-selected favorable receipts are not an enrollment mechanism.
use std::collections::BTreeSet;
use std::path::PathBuf;

use ag_campaign::governed::*;
use ag_primitives::Digest;
use nq_core::labelwatch_cleanup::{self, CleanupSource, Request};
use nq_core::labelwatch_relief::Phase;
use serde_json::{Value, json};

pub const RESOLVER_ID: &str = "labelwatch.m3-cleanup-native-prerequisite/v1";

pub struct EnrolledCleanup {
    pub receipt: PathBuf,
    pub expected_receipt_id: String,
    pub expected_request: Request,
    pub cleanup_step_sha256: String,
    pub subject: Digest,
    pub scope: Digest,
}

/// Expected observation identity commits the exact authority-side subject and
/// scope to the exact factual receipt/request and immutable cleanup input.
/// Receipt possession itself does not authorize cleanup.
pub fn observation_identity(enrolled: &EnrolledCleanup) -> Result<ObservationRefV1, String> {
    let binding = json!({"schema":"labelwatch.m3-cleanup-binding/v1",
        "receipt_id":enrolled.expected_receipt_id,
        "request":enrolled.expected_request,
        "cleanup_step_sha256":enrolled.cleanup_step_sha256,
        "subject":enrolled.subject.as_str(), "scope":enrolled.scope.as_str()});
    let canonical = nq_protocol::canonical_json_bytes(&binding).map_err(|e| e.to_string())?;
    Ok(ObservationRefV1::from_digest(Digest::hash_domain(
        "labelwatch/m3-cleanup-observation/v1",
        &canonical,
    )))
}

pub struct NativeCleanupObservation {
    pub enrolled: EnrolledCleanup,
}

fn refused() -> ExternalBoundaryErrorV1 {
    ExternalBoundaryErrorV1::Refused {
        code: "labelwatch-native-cleanup-prerequisite".into(),
        evidence: None,
    }
}

impl NativeCleanupObservation {
    fn resolve(
        &self,
        request: &ObservationResolutionRequestV1<'_>,
    ) -> Result<ObservationResolutionV2, String> {
        let enrolled = &self.enrolled;
        if enrolled.expected_request.held_request.phase != Phase::PreIngest
            || enrolled.cleanup_step_sha256.len() != 64
            || !enrolled
                .cleanup_step_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || request.subject != &enrolled.subject
            || request.observation != &observation_identity(enrolled)?
        {
            return Err("enrolled cleanup subject/scope/observation differs".into());
        }
        // Existing NQ bounded intake captures the physical regular file before
        // reading. It refuses links, devices, FIFOs and oversized input.
        let raw = nq_app::bounded_input::read(&enrolled.receipt, 2 * 1024 * 1024)
            .map_err(|e| e.to_string())?;
        let receipt: Value =
            nq_protocol::decode_json_document(&raw, 2 * 1024 * 1024).map_err(|e| e.to_string())?;
        labelwatch_cleanup::replay(&receipt)?;
        if receipt["schema"] != "nq.labelwatch-cleanup-qualification/v2"
            || receipt["receipt_id"] != enrolled.expected_receipt_id
            || receipt["request"]
                != serde_json::to_value(&enrolled.expected_request).map_err(|e| e.to_string())?
            || receipt["claim"] != "held_cut_and_recoverable_copy_prerequisites"
            || receipt["disposition"] != "ESTABLISHED"
            || receipt["refuted"] != json!([])
            || receipt["unknown"] != json!([])
        {
            return Err("exact established native prerequisite absent".into());
        }
        let source_raw = receipt["source_utf8"].as_str().ok_or("source absent")?;
        let source: CleanupSource = serde_json::from_value(
            nq_protocol::decode_json_document(source_raw.as_bytes(), 2 * 1024 * 1024)
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let started = u64::try_from(source.currentness_started_at.timestamp_millis())
            .map_err(|e| e.to_string())?;
        let completed = u64::try_from(source.currentness_completed_at.timestamp_millis())
            .map_err(|e| e.to_string())?;
        let evaluated = u64::try_from(enrolled.expected_request.evaluated_at.timestamp_millis())
            .map_err(|e| e.to_string())?;
        let fresh_until = started
            .checked_add(
                u64::from(enrolled.expected_request.maximum_currentness_age_seconds) * 1000,
            )
            .ok_or("freshness overflow")?;
        if request.now_unix_ms < completed
            || request.now_unix_ms < evaluated
            || request.now_unix_ms >= fresh_until
        {
            return Err("native prerequisite no longer applicable at AG clock cut".into());
        }
        let basis = DecisionBasisV1 {
            schema: DECISION_BASIS_SCHEMA_V1.into(),
            rule: DecisionBasisRuleV1 {
                id: DECISION_BASIS_RULE_ID_V1.into(),
                version: DECISION_BASIS_RULE_VERSION_V1.into(),
                digest: decision_basis_rule_digest_v1().as_str().into(),
            },
            atoms: BTreeSet::from(["condition.clean".into(), "delivery.not_required".into()]),
        };
        Ok(ObservationResolutionV2 {
            schema: OBSERVATION_RESOLUTION_SCHEMA_V2.into(),
            key: request.key.clone(),
            observation: request.observation.clone(),
            currentness: ObservationCurrentnessRefV1::from_digest(Digest::hash_domain(
                "labelwatch/m3-native-receipt-currentness/v1",
                &raw,
            )),
            normalized_preconditions: PreconditionBasisRefV1::from_digest(
                basis.decision_basis_digest().map_err(|e| e.to_string())?,
            ),
            basis,
            resolver_id: RESOLVER_ID.into(),
            subject: request.subject.clone(),
            status: ObservationStatusV1::Current,
            resolved_at_unix_ms: request.now_unix_ms,
            fresh_until_unix_ms: fresh_until,
        })
    }
}

impl ObservationResolverV1 for NativeCleanupObservation {
    fn resolve_observation(
        &mut self,
        request: &ObservationResolutionRequestV1<'_>,
    ) -> Result<VersionedObservationResolutionV1, ExternalBoundaryErrorV1> {
        self.resolve(request).map(Into::into).map_err(|_| refused())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ag_campaign::CampaignId;
    use std::fs;
    use uuid::Uuid;

    #[test]
    fn actual_native_receipt_and_closed_negative_admission_controls() {
        let directory = PathBuf::from(
            std::env::var("M3_NATIVE_FIXTURE").expect("retained real observer fixture"),
        );
        let source = fs::read(directory.join("cleanup-source.json")).unwrap();
        let policy: Request = nq_protocol::decode_json_document(
            &fs::read(directory.join("cleanup-request.json")).unwrap(),
            2 * 1024 * 1024,
        )
        .unwrap();
        let receipt = labelwatch_cleanup::qualify(&source, &policy).unwrap();
        assert_eq!(receipt["disposition"], "ESTABLISHED");
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("receipt.json");
        fs::write(&path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let subject = Digest::hash_domain("m3-test", b"subject");
        let scope = Digest::hash_domain("m3-test", b"cleanup-scope");
        let mut resolver = NativeCleanupObservation {
            enrolled: EnrolledCleanup {
                receipt: path.clone(),
                expected_receipt_id: receipt["receipt_id"].as_str().unwrap().into(),
                expected_request: policy.clone(),
                cleanup_step_sha256: "a".repeat(64),
                subject: subject.clone(),
                scope,
            },
        };
        let observation = observation_identity(&resolver.enrolled).unwrap();
        let key = OccurrenceKeyV1 {
            campaign: CampaignId::from_digest(subject.clone()),
            occurrence: OccurrenceId::from_uuid(Uuid::from_u128(1)),
        };
        let now = u64::try_from(policy.evaluated_at.timestamp_millis()).unwrap();
        let request = ObservationResolutionRequestV1 {
            key: &key,
            observation: &observation,
            subject: &subject,
            now_unix_ms: now,
        };
        assert!(resolver.resolve_observation(&request).is_ok());
        let stale = ObservationResolutionRequestV1 {
            now_unix_ms: now + 31_000,
            ..request.clone()
        };
        assert!(resolver.resolve_observation(&stale).is_err());
        resolver.enrolled.scope = Digest::hash_domain("m3-test", b"other-scope");
        assert!(resolver.resolve_observation(&request).is_err());
        resolver.enrolled.scope = Digest::hash_domain("m3-test", b"cleanup-scope");
        resolver
            .enrolled
            .expected_request
            .held_request
            .source
            .push_str("-substituted");
        assert!(resolver.resolve_observation(&request).is_err());
        resolver.enrolled.expected_request = policy.clone();
        let mut altered = receipt.clone();
        altered["disposition"] = json!("REFUTED");
        fs::write(&path, serde_json::to_vec(&altered).unwrap()).unwrap();
        assert!(resolver.resolve_observation(&request).is_err());
        // A genuinely replayable unknown receipt also cannot supply clean.
        let mut unknown_source: Value =
            nq_protocol::decode_json_document(&source, 2 * 1024 * 1024).unwrap();
        unknown_source["backup"] = json!({"state":"NOT_OBSERVABLE","value":null});
        unknown_source["unknowns"] = json!([{"slot":"backup","reason":"fixture unavailable"}]);
        let unknown =
            labelwatch_cleanup::qualify(&serde_json::to_vec(&unknown_source).unwrap(), &policy)
                .unwrap();
        assert_eq!(unknown["disposition"], "NOT_OBSERVABLE");
        resolver.enrolled.expected_receipt_id = unknown["receipt_id"].as_str().unwrap().into();
        let unknown_observation = observation_identity(&resolver.enrolled).unwrap();
        let unknown_request = ObservationResolutionRequestV1 {
            observation: &unknown_observation,
            ..request.clone()
        };
        fs::write(&path, serde_json::to_vec(&unknown).unwrap()).unwrap();
        assert!(resolver.resolve_observation(&unknown_request).is_err());
        fs::remove_file(&path).unwrap();
        assert!(resolver.resolve_observation(&unknown_request).is_err());
    }
}
