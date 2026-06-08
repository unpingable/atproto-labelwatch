"""Verify classifier output matches golden expected files.

Walks *.evidence.json in this directory, runs classifier.classify_evidence
on each, loads the matching *.expected.json, and reports any divergence
on:
  - ConversionGap          vs expected_gap
  - admissible_claims       vs expected_admissible_claims
  - inadmissible_claims     vs expected_inadmissible_claims

Exit 0 on full match; nonzero on any divergence.

Run:
    python3 verify.py

Success conditions (per the manifest split spec):
  - Changing the golden's expected_gap should fail ONLY the comparison
    (this script).
  - Changing evidence presence/absence should change the derived gap
    (classifier reads evidence; this script then sees mismatch).
  - The gap MUST NOT be hand-authored into the evidence input.
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List, Tuple

from classifier import classify_evidence


def _load(path: str) -> Dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def _compare_claims(
    derived: List[Dict[str, Any]],
    expected: List[Dict[str, Any]],
    kind: str,
) -> List[str]:
    """Compare two claim lists; return list of human-readable diff lines."""
    diffs: List[str] = []
    derived_by_id = {c.get("id"): c for c in derived}
    expected_by_id = {c.get("id"): c for c in expected}
    missing = set(expected_by_id) - set(derived_by_id)
    extra = set(derived_by_id) - set(expected_by_id)
    for cid in sorted(missing):
        diffs.append(f"  {kind}: missing claim id {cid!r} (in golden, not derived)")
    for cid in sorted(extra):
        diffs.append(f"  {kind}: extra claim id {cid!r} (derived, not in golden)")
    for cid in sorted(set(derived_by_id) & set(expected_by_id)):
        d = derived_by_id[cid]
        e = expected_by_id[cid]
        for field in ("claim", "claim_form", "scope", "why_inadmissible", "qualifier"):
            if d.get(field) != e.get(field):
                diffs.append(
                    f"  {kind}.{cid}.{field}: derived={d.get(field)!r} "
                    f"expected={e.get(field)!r}"
                )
    return diffs


def verify_one(evidence_path: str, expected_path: str) -> Tuple[bool, List[str]]:
    evidence = _load(evidence_path)
    expected = _load(expected_path)
    derived = classify_evidence(evidence)

    diffs: List[str] = []
    if derived["ConversionGap"] != expected.get("expected_gap"):
        diffs.append(
            f"  ConversionGap: derived={derived['ConversionGap']!r} "
            f"expected={expected.get('expected_gap')!r}"
        )
    diffs.extend(_compare_claims(
        derived["admissible_claims"],
        expected.get("expected_admissible_claims", []),
        "admissible",
    ))
    diffs.extend(_compare_claims(
        derived["inadmissible_claims"],
        expected.get("expected_inadmissible_claims", []),
        "inadmissible",
    ))
    return (not diffs), diffs


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    evidence_files = sorted(
        f for f in os.listdir(here) if f.endswith(".evidence.json")
    )
    if not evidence_files:
        print("verify.py: no *.evidence.json files found", file=sys.stderr)
        return 2

    failures = 0
    for ev_file in evidence_files:
        stem = ev_file[: -len(".evidence.json")]
        exp_file = stem + ".expected.json"
        ev_path = os.path.join(here, ev_file)
        exp_path = os.path.join(here, exp_file)
        if not os.path.exists(exp_path):
            print(f"FAIL: {ev_file} has no matching {exp_file}", file=sys.stderr)
            failures += 1
            continue
        ok, diffs = verify_one(ev_path, exp_path)
        if ok:
            print(f"PASS: {ev_file}")
        else:
            print(f"FAIL: {ev_file}")
            for d in diffs:
                print(d)
            failures += 1
    if failures:
        print(f"\n{failures} specimen(s) failed verification.", file=sys.stderr)
        return 1
    print(f"\nAll {len(evidence_files)} specimens verify.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
