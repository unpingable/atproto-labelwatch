"""Bundle D minimal tests + corpus run.

Two halves:
  1. Synthetic invariants — assertions over crafted evidence packets
     that exercise each refusal gate and each successful export path.
  2. Corpus run — walks docs/specimens/derived/ + the fixture pair,
     runs classifier + exporter on each, prints a one-line summary,
     and writes the result JSON to docs/specimens/export_out/.

Exit 0 if all invariants pass; nonzero on any failure. The corpus run
is informational — it shows the exported-vs-blocked split but does
not assert specific outcomes (those drift as data changes).

Run:
    python3 test_export.py
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict

from classifier import classify_evidence
from specimen_exporter import export_candidate


def _synth(
    label_value: str,
    *,
    labeler_class: str = "official_platform",
    policy_status: str = "absent_for_consumer",
    policy_artifact_kind: str = None,
    policy_consumer_scope: str = "unknown",
    policy_surface: str = None,
    emitter_status: str = "absent",
    emitter_artifact_kind: str = None,
    emitter_consumer_scope: str = "unknown",
    render_status: str = "absent",
    hosting_status: str = "not_applicable",
) -> Dict[str, Any]:
    """Build a synthetic evidence packet for a single test case."""
    packet = {
        "specimen_id": f"synth-{label_value}",
        "schema_kind": "evidence",
        "lane": "test",
        "provenance": {"source": "synthetic Bundle D test"},
        "LabelObservation": {
            "labeler_did": "did:plc:synth",
            "labeler_handle": "synth.test",
            "labeler_class": labeler_class,
            "is_reference_labeler": False,
            "target_uri": "at://did:plc:x/app.bsky.feed.post/y",
            "target_kind": "post_record",
            "label_value": label_value,
            "neg": 0,
            "ts": "2026-06-08T00:00:00Z",
        },
        "PolicyDocumentation": {
            "consumer": {"consumer_id": "bsky.app-default-client"},
            "status": policy_status,
            "consumer_scope": policy_consumer_scope,
            "execution_surface": policy_surface,
        },
        "LabelerEmitterDocumentation": {
            "status": emitter_status,
            "artifact_kind": emitter_artifact_kind,
            "consumer_scope": emitter_consumer_scope,
        },
        "PolicyWitness": {"status": "not_applicable"},
        "RenderObservation": {"status": render_status},
        "HostingObservation": {"status": hosting_status},
    }
    if policy_status == "documented":
        packet["PolicyDocumentation"]["policy_artifact"] = {
            "artifact_kind": policy_artifact_kind,
        }
    return packet


def _run(
    name: str,
    evidence: Dict[str, Any],
    lane: str = "authority_surface",
    now_iso_override: str = None,
) -> Dict[str, Any]:
    c = classify_evidence(evidence)
    return export_candidate(
        evidence, c, lane=lane, evidence_source=name, now_iso=now_iso_override,
    )


# --- synthetic invariants -----------------------------------------------

def test_unknown_surface_blocked() -> None:
    """surface=unknown MUST block. Per Bundle D invariant."""
    ev = _synth(
        "hypothetical-new-global",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="unknown",
    )
    out = _run("test_unknown_surface", ev)
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "unknown_surface_not_specimen", out
    print("  PASS unknown_surface_not_specimen")


def test_first_party_unknown_consumer_scope_blocked() -> None:
    """official_platform + consumer_scope=unknown + no service record at all
    -> ingestion gap blocker (true F-005 shape)."""
    ev = _synth(
        "imaginary-first-party-label",
        labeler_class="official_platform",
        policy_status="absent_for_consumer",
        policy_consumer_scope="unknown",
        emitter_status="absent",
        emitter_consumer_scope="unknown",
    )
    # synth() doesn't set labeler_service_record_present; default for status=absent is False
    ev["LabelerEmitterDocumentation"]["labeler_service_record_present"] = False
    out = _run("test_first_party_no_record", ev)
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "ingestion_gap_surface_unresolved", out
    print("  PASS ingestion_gap_surface_unresolved (no service record at all)")


def test_first_party_emitter_does_not_declare() -> None:
    """D.5: first-party + service record FOUND + label NOT in it
    -> emitter_does_not_declare_label blocker (NOT ingestion gap)."""
    ev = _synth(
        "needs-review",
        labeler_class="official_platform",
        policy_status="absent_for_consumer",
        policy_consumer_scope="unknown",
        emitter_status="service_record_found_label_not_declared",
        emitter_consumer_scope="unknown",
    )
    ev["LabelerEmitterDocumentation"]["labeler_service_record_present"] = True
    out = _run("test_first_party_emitter_undeclared", ev)
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "emitter_does_not_declare_label", out
    print("  PASS emitter_does_not_declare_label (service record found, label undeclared)")


def test_third_party_unknown_consumer_scope_blocked() -> None:
    """non-first-party + consumer_scope=unknown -> generic provenance blocker."""
    ev = _synth(
        "obscure-label",
        labeler_class="third_party",
        policy_status="absent_for_consumer",
        policy_consumer_scope="unknown",
        emitter_status="absent",
        emitter_consumer_scope="unknown",
    )
    out = _run("test_third_party_unknown", ev)
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "provenance_unresolved", out
    print("  PASS provenance_unresolved")


def test_emitter_declared_exports_with_caveat() -> None:
    """service_record / emitter_declared -> EXPORTS, with non_global_provenance caveat."""
    ev = _synth(
        "fringe-media",
        labeler_class="third_party",
        policy_status="absent_for_consumer",
        policy_consumer_scope="unknown",
        emitter_status="documented_via_service_record",
        emitter_artifact_kind="service_record",
        emitter_consumer_scope="emitter_declared",
    )
    out = _run("test_emitter_declared", ev)
    assert out["schema_kind"] == "specimen_candidate", out
    assert "non_global_provenance" in out["export_caveats"], out
    assert out["consumer_scope_effective"] == "emitter_declared", out
    assert out["emitter_provenance"]["artifact_kind"] == "service_record", out
    print("  PASS emitter_declared exports with non_global_provenance caveat")


def test_protocol_doc_exports() -> None:
    """!takedown-shape: protocol_doc / pds_hosting -> exports, surface preserved."""
    ev = _synth(
        "!takedown",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="protocol_doc",
        policy_consumer_scope="global_platform",
        policy_surface="pds_hosting",
        hosting_status="absent",
    )
    out = _run("test_protocol_doc", ev)
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["policy_provenance"]["artifact_kind"] == "protocol_doc", out
    assert out["consumer_scope_effective"] == "global_platform", out
    assert "hosting_execution_unwitnessed" in out["export_caveats"], out
    print("  PASS protocol_doc exports with hosting_execution_unwitnessed caveat")


def test_upstream_const_exports() -> None:
    """porn-shape: upstream_const / client_render -> exports, surface preserved."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
        render_status="absent",
    )
    out = _run("test_upstream_const", ev)
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["policy_provenance"]["artifact_kind"] == "upstream_const", out
    assert out["consumer_scope_effective"] == "global_platform", out
    assert "render_execution_unwitnessed" in out["export_caveats"], out
    print("  PASS upstream_const exports with render_execution_unwitnessed caveat")


def test_freshness_missing_basis_blocks() -> None:
    """Freshness lane: StateBasis absent -> missing_required_basis blocker
    (Bundle E rename from requires_state_basis)."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    # _synth does not populate StateBasis
    out = _run("test_freshness_missing_basis", ev, lane="freshness")
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "missing_required_basis", out
    print("  PASS missing_required_basis (freshness lane, no StateBasis)")


def test_freshness_unknown_basis_exports_with_caveat() -> None:
    """Freshness lane: StateBasis present with freshness_horizon='unknown'
    -> EXPORTS with state_basis_status=unknown_basis + caveat. Bundle E
    invariant: unknown basis never silently exports as current."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    ev["StateBasis"] = {
        "source_kind": "db_row",
        "captured_at": "2026-06-08T00:00:00Z",
        "artifact_identity": "test",
        "freshness_horizon": "unknown",
        "derivation_source": "test",
    }
    out = _run("test_freshness_unknown", ev, lane="freshness", )
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["state_basis_status"] == "unknown_basis", out
    assert "unknown_basis" in out["export_caveats"], out
    # Confirm no current_basis claim
    assert "current_basis" not in str(out), "unknown_basis must not claim current"
    print("  PASS unknown_basis exports with caveat, never current")


def test_freshness_stale_basis_exports_with_caveat() -> None:
    """Freshness lane: StateBasis with concrete horizon already passed
    -> EXPORTS with state_basis_status=stale_basis + caveat."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    ev["StateBasis"] = {
        "source_kind": "snapshot",
        "captured_at": "2025-01-01T00:00:00Z",
        "artifact_identity": "test-snapshot",
        "freshness_horizon": "2025-06-01T00:00:00Z",  # past
        "derivation_source": "test snapshot deadline already passed",
    }
    out = _run("test_freshness_stale", ev, lane="freshness", now_iso_override="2026-06-08T00:00:00Z")
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["state_basis_status"] == "stale_basis", out
    assert "stale_basis" in out["export_caveats"], out
    print("  PASS stale_basis exports with caveat, never current")


def test_freshness_current_basis_exports_clean() -> None:
    """Freshness lane: StateBasis with concrete horizon in the future
    -> EXPORTS with state_basis_status=current_basis + no basis caveat."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    ev["StateBasis"] = {
        "source_kind": "live_fetch",
        "captured_at": "2026-06-08T00:00:00Z",
        "artifact_identity": "test-live",
        "freshness_horizon": "2099-01-01T00:00:00Z",  # far future
        "derivation_source": "test live fetch with explicit horizon",
    }
    out = _run("test_freshness_current", ev, lane="freshness", now_iso_override="2026-06-08T00:00:00Z")
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["state_basis_status"] == "current_basis", out
    assert "unknown_basis" not in out["export_caveats"], out
    assert "stale_basis" not in out["export_caveats"], out
    print("  PASS current_basis exports clean (basis horizon honored)")


def test_authority_surface_ignores_state_basis_absence() -> None:
    """Bundle E invariant: authority_surface lane does NOT gate on
    StateBasis. D.5 behavior unchanged for global_platform/emitter_declared/
    undeclared/unresolved-surface even when StateBasis is missing."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    # no StateBasis
    out = _run("test_authority_no_basis", ev, lane="authority_surface")
    assert out["schema_kind"] == "specimen_candidate", out
    assert out["state_basis_status"] == "missing", out
    # No basis-related caveat on authority_surface lane:
    assert "unknown_basis" not in out["export_caveats"], out
    assert "stale_basis" not in out["export_caveats"], out
    print("  PASS authority_surface ignores StateBasis (D.5 behavior preserved)")


def test_no_label_observation_blocked() -> None:
    """Missing LabelObservation -> no_label_observation."""
    ev = {"LabelObservation": {}, "PolicyDocumentation": {}}
    c = {"ConversionGap": {"name": "observability_gap", "surface": None, "consumer_scope": "unknown"}}
    out = export_candidate(ev, c, lane="authority_surface", evidence_source="test_no_label")
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "no_label_observation", out
    print("  PASS no_label_observation")


# --- corpus run ---------------------------------------------------------

def run_corpus(out_dir: str = "export_out") -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(here, out_dir)
    os.makedirs(out_path, exist_ok=True)

    print()
    print(f"--- corpus run: walking docs/specimens/ for BOTH lanes ---")
    print(f"{'evidence_file':<70} {'lane':<18} {'verdict':<22} {'blocker_or_scope'}")
    print("-" * 145)

    for lane in ("authority_surface", "freshness"):
        # Fixtures
        for fname in sorted(os.listdir(here)):
            if not fname.endswith(".evidence.json"):
                continue
            _process_one(here, fname, out_path, lane)

        # Derived packets
        derived_dir = os.path.join(here, "derived")
        if os.path.isdir(derived_dir):
            for fname in sorted(os.listdir(derived_dir)):
                if not fname.endswith(".evidence.json"):
                    continue
                _process_one(derived_dir, fname, out_path, lane, subdir="derived")
        print()

    print(f"Output JSONs written to {out_path}/")


def _process_one(base_dir: str, fname: str, out_path: str, lane: str, subdir: str = "") -> None:
    src = os.path.join(base_dir, fname)
    with open(src) as f:
        ev = json.load(f)
    c = classify_evidence(ev)
    rel_src = os.path.join(subdir, fname) if subdir else fname
    out = export_candidate(ev, c, lane=lane, evidence_source=rel_src)
    if out["schema_kind"] == "specimen_candidate":
        verdict = "EXPORTED"
        scope = out.get("consumer_scope_effective", "?")
        basis = out.get("state_basis_status", "?")
        tail = f"{scope}/{basis}  caveats={out['export_caveats']}"
    else:
        verdict = "blocked"
        tail = f"{out['blocker']}"
    print(f"{rel_src:<70} {lane:<18} {verdict:<22} {tail}")
    out_name = fname.replace(".evidence.json", f".export.{lane}.json")
    out_subdir = os.path.join(out_path, subdir) if subdir else out_path
    os.makedirs(out_subdir, exist_ok=True)
    with open(os.path.join(out_subdir, out_name), "w") as f:
        json.dump(out, f, indent=2)
        f.write("\n")


# --- main --------------------------------------------------------------

def main() -> int:
    print("--- synthetic invariants ---")
    failures = 0
    for t in [
        test_unknown_surface_blocked,
        test_first_party_unknown_consumer_scope_blocked,
        test_first_party_emitter_does_not_declare,
        test_third_party_unknown_consumer_scope_blocked,
        test_emitter_declared_exports_with_caveat,
        test_protocol_doc_exports,
        test_upstream_const_exports,
        test_freshness_missing_basis_blocks,
        test_freshness_unknown_basis_exports_with_caveat,
        test_freshness_stale_basis_exports_with_caveat,
        test_freshness_current_basis_exports_clean,
        test_authority_surface_ignores_state_basis_absence,
        test_no_label_observation_blocked,
    ]:
        try:
            t()
        except AssertionError as e:
            print(f"  FAIL {t.__name__}: {e}")
            failures += 1
    if failures:
        print(f"\n{failures} synthetic invariant(s) failed.")
        return 1
    print(f"\nAll synthetic invariants passed.")

    run_corpus()
    return 0


if __name__ == "__main__":
    sys.exit(main())
