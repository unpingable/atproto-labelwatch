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


def _run(name: str, evidence: Dict[str, Any], lane: str = "authority_surface") -> Dict[str, Any]:
    c = classify_evidence(evidence)
    return export_candidate(evidence, c, lane=lane, evidence_source=name)


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
    """official_platform + consumer_scope=unknown -> ingestion gap blocker
    (F-005 territory)."""
    ev = _synth(
        "needs-review",
        labeler_class="official_platform",
        policy_status="absent_for_consumer",
        policy_consumer_scope="unknown",
        emitter_status="absent",
        emitter_consumer_scope="unknown",
    )
    out = _run("test_first_party_unknown", ev)
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "ingestion_gap_surface_unresolved", out
    print("  PASS ingestion_gap_surface_unresolved (F-005-shaped)")


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


def test_freshness_lane_always_blocks() -> None:
    """Even a fully-exportable authority_surface candidate is blocked
    on the freshness lane in v1 (no state_basis populated yet)."""
    ev = _synth(
        "porn",
        labeler_class="official_platform",
        policy_status="documented",
        policy_artifact_kind="upstream_const",
        policy_consumer_scope="global_platform",
        policy_surface="client_render",
    )
    out = _run("test_freshness_lane", ev, lane="freshness")
    assert out["schema_kind"] == "blocked_candidate", out
    assert out["blocker"] == "requires_state_basis", out
    print("  PASS freshness lane blocks on missing state_basis")


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
    print(f"--- corpus run: walking docs/specimens/ ---")
    print(f"{'evidence_file':<70} {'lane':<18} {'verdict':<22} {'blocker_or_scope'}")
    print("-" * 145)

    # Fixtures
    for fname in sorted(os.listdir(here)):
        if not fname.endswith(".evidence.json"):
            continue
        _process_one(here, fname, out_path, "authority_surface")

    # Derived packets
    derived_dir = os.path.join(here, "derived")
    if os.path.isdir(derived_dir):
        for fname in sorted(os.listdir(derived_dir)):
            if not fname.endswith(".evidence.json"):
                continue
            _process_one(derived_dir, fname, out_path, "authority_surface", subdir="derived")

    print()
    print(f"Output JSONs written to {out_path}/")


def _process_one(base_dir: str, fname: str, out_path: str, lane: str, subdir: str = "") -> None:
    src = os.path.join(base_dir, fname)
    with open(src) as f:
        ev = json.load(f)
    c = classify_evidence(ev)
    rel_src = os.path.join(subdir, fname) if subdir else fname
    out = export_candidate(ev, c, lane=lane, evidence_source=rel_src)
    label = (ev.get("LabelObservation") or {}).get("label_value", "?")
    if out["schema_kind"] == "specimen_candidate":
        verdict = "EXPORTED"
        tail = f"{out['consumer_scope_effective']}  caveats={out['export_caveats']}"
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
        test_third_party_unknown_consumer_scope_blocked,
        test_emitter_declared_exports_with_caveat,
        test_protocol_doc_exports,
        test_upstream_const_exports,
        test_freshness_lane_always_blocks,
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
