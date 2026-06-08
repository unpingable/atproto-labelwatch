"""Regression checks for the operator-maturity findings page.

Asserts the headline numbers cited in docs/findings/operator-maturity/
match the published snapshot artifact. The point is NOT that the
numbers must remain stable forever — the ecosystem changes; abandoned
labelers may re-emit, undeclared labelers may publish service
records, etc. The point is that the SCANNER must explain when they
change.

Re-run after any new operator-maturity scan and confirm:
  - F-007 cohort size (high-volume zero-definition emitters)
  - F-008 cohort size (abandoned labelers)
  - F-008 substantial-declared-scope subset
  - vocalabeller churn specimen remains identifiable
  - T-001 mis-flags (xblock + recordcollector) remain visible

If any assertion fails: investigate WHY before updating the page.
The page cites specific numbers (14, 65, 28, 106000); if those drift,
the page wording drifts with them, and the reader gets a clear
delta rather than silent fudging.

Run:
    cd docs/findings/operator-maturity/
    python3 regression/test_findings_regression.py

Exit 0 if all checks pass; nonzero on any failure.
"""
from __future__ import annotations

import json
import os
import sys

# Snapshot the page cites. To regress against a different snapshot,
# pass --snapshot <path>.
DEFAULT_SNAPSHOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "artifacts",
    "operator-maturity-scan-2026-06-08.json",
)

# Headline numbers cited on the findings page. Bump these (and the
# page) only if the scanner re-runs and the new snapshot produces
# different counts — with a one-line note explaining what changed.
EXPECTED = {
    "total_rows": 150,
    "f007_high_volume_undeclared": 14,    # events_30d > 1000 AND latest_label_def_count == 0
    "f008_abandoned_total": 65,           # maturity_class == "abandoned"
    "f008_abandoned_substantial_scope": 28,  # abandoned AND latest_label_def_count >= 6
    "vocalabeller_revisions": 106000,
    "vocalabeller_defs": 1,
    "vocalabeller_events_30d": 0,
    "t001_mis_flags": {
        "xblock.aendra.dev": True,        # likely_test_dev == 1
        "recordcollector.edavis.dev": True,
    },
    "class_histogram": {
        "abandoned": 65,
        "unknown": 26,
        "experimental": 24,
        "community-service": 13,
        "personal/reputational": 11,
        "moderation-infrastructure": 10,
        "platform-root": 1,
    },
}


def _load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def _by_handle(rows: list, handle: str) -> dict | None:
    for r in rows:
        if r.get("handle") == handle:
            return r
    return None


def main() -> int:
    snapshot_path = DEFAULT_SNAPSHOT
    if len(sys.argv) > 2 and sys.argv[1] == "--snapshot":
        snapshot_path = sys.argv[2]

    data = _load(snapshot_path)
    rows = data["rows"]
    failures: list[str] = []

    # 1. total rows
    if len(rows) != EXPECTED["total_rows"]:
        failures.append(
            f"total_rows: expected {EXPECTED['total_rows']}, got {len(rows)}"
        )

    # 2. F-007 cohort
    high_vol_undeclared = [
        r for r in rows
        if r.get("label_count_30d", 0) > 1000
        and r.get("latest_label_def_count", 0) == 0
    ]
    if len(high_vol_undeclared) != EXPECTED["f007_high_volume_undeclared"]:
        failures.append(
            f"F-007 cohort: expected {EXPECTED['f007_high_volume_undeclared']}, "
            f"got {len(high_vol_undeclared)}"
        )

    # 3. F-008 abandoned total
    abandoned = [r for r in rows if r.get("maturity_class") == "abandoned"]
    if len(abandoned) != EXPECTED["f008_abandoned_total"]:
        failures.append(
            f"F-008 abandoned total: expected {EXPECTED['f008_abandoned_total']}, "
            f"got {len(abandoned)}"
        )

    # 4. F-008 abandoned-with-substantial-declared-scope subset
    abandoned_substantial = [
        r for r in abandoned if r.get("latest_label_def_count", 0) >= 6
    ]
    if len(abandoned_substantial) != EXPECTED["f008_abandoned_substantial_scope"]:
        failures.append(
            f"F-008 abandoned w/ substantial scope: "
            f"expected {EXPECTED['f008_abandoned_substantial_scope']}, "
            f"got {len(abandoned_substantial)}"
        )

    # 5. vocalabeller churn specimen
    voca = _by_handle(rows, "vocalabeller.kanshen.click")
    if voca is None:
        failures.append("vocalabeller.kanshen.click: not present in snapshot")
    else:
        if voca["service_record_revisions"] != EXPECTED["vocalabeller_revisions"]:
            failures.append(
                f"vocalabeller revisions: expected "
                f"{EXPECTED['vocalabeller_revisions']}, "
                f"got {voca['service_record_revisions']}"
            )
        if voca["latest_label_def_count"] != EXPECTED["vocalabeller_defs"]:
            failures.append(
                f"vocalabeller defs: expected {EXPECTED['vocalabeller_defs']}, "
                f"got {voca['latest_label_def_count']}"
            )
        if voca["label_count_30d"] != EXPECTED["vocalabeller_events_30d"]:
            failures.append(
                f"vocalabeller events_30d: expected "
                f"{EXPECTED['vocalabeller_events_30d']}, "
                f"got {voca['label_count_30d']}"
            )

    # 6. T-001 mis-flags
    for handle, expected_flag in EXPECTED["t001_mis_flags"].items():
        r = _by_handle(rows, handle)
        if r is None:
            failures.append(f"T-001: {handle} not present in snapshot")
            continue
        if bool(r.get("likely_test_dev")) != expected_flag:
            failures.append(
                f"T-001 {handle}: expected likely_test_dev={expected_flag}, "
                f"got {bool(r.get('likely_test_dev'))}"
            )

    # 7. class histogram (sanity-check D-002 cohort sizes)
    actual_hist: dict[str, int] = {}
    for r in rows:
        c = r.get("maturity_class", "unknown")
        actual_hist[c] = actual_hist.get(c, 0) + 1
    for cls, expected_count in EXPECTED["class_histogram"].items():
        got = actual_hist.get(cls, 0)
        if got != expected_count:
            failures.append(
                f"class histogram[{cls!r}]: expected {expected_count}, got {got}"
            )

    if failures:
        print(f"FAIL ({len(failures)} regressions):")
        for f in failures:
            print(f"  - {f}")
        print()
        print(
            "If the underlying ecosystem changed, re-run the scanner, "
            "update EXPECTED in this file + the cited numbers in "
            "docs/findings/operator-maturity/index.md, and commit "
            "BOTH together so the page is never out of sync with its "
            "snapshot."
        )
        return 1

    print(f"PASS ({len(rows)} rows; all 7 regression checks green)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
