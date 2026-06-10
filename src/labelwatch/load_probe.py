"""labelwatch.load_probe.v1 — real-subject load characterization.

The shape audit (labelwatch.index_audit.v1) verdicts whether the planner
picks the right index, using a synthetic probe DID that matches zero
rows. That's a FLOOR: it shows the lookup is structurally supportable,
not how it behaves under real load.

The load probe sweeps the top-N labeled subjects (the actually-loaded
end of the workload curve) and measures end-to-end lookup_subject()
wall time. Emits a receipt with p50 / p90 / p99 and a verdict:

    admissible_for_publication   p99 < 500ms, no unbounded path
    admissible_with_debt         p99 500ms - 5s
    refused_unbounded            p99 > 5s OR any subject hung

This module is OFFLINE / ONE-SHOT by design. It does NOT plug into the
live frontdoor. The live surface's gate is the SHAPE audit; load is
a separate axis. See subject-lookup-load-probe-001.md.

The sampling query itself (top-N by event count) is an unbounded SCAN
of label_events — the same shape the audit refuses for live use. That's
expected: identifying which subjects are most-labeled is workload
characterization, not a per-request operation.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

from . import db as db_mod
from . import frontdoor
from .utils import format_ts, get_git_commit, hash_sha256, now_utc, stable_json

log = logging.getLogger(__name__)


RECEIPT_KIND = "labelwatch.load_probe.v1"
RECEIPT_SCHEMA_VERSION = 1
DEFAULT_CONSUMER_SURFACE = "whatsonme.frontdoor.v0"
DEFAULT_SAMPLE_SIZE = 100

# Verdict thresholds (chatty subject-lookup-load-probe-001 draft).
ADMISSIBLE_P99_MS = 500.0
DEBT_P99_MS = 5000.0


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

# This query is intentionally unbounded — it's the workload-discovery step,
# run once per probe invocation. NOT wired into the live surface.
_SAMPLE_TOP_LABELED_SQL = (
    "SELECT target_did, COUNT(*) AS n "
    "FROM label_events "
    "WHERE target_did IS NOT NULL "
    "GROUP BY target_did "
    "ORDER BY n DESC "
    "LIMIT ?"
)


def sample_top_labeled_subjects(conn: sqlite3.Connection, n: int) -> list[dict]:
    """Return top-N labeled subjects by event count.

    Each row: {"did": ..., "event_count": ...}. Sorted descending by count.
    This query SCANS label_events — slow on a large DB but one-shot.
    """
    rows = conn.execute(_SAMPLE_TOP_LABELED_SQL, (n,)).fetchall()
    return [{"did": r["target_did"], "event_count": int(r["n"])} for r in rows]


# ---------------------------------------------------------------------------
# Per-subject probe
# ---------------------------------------------------------------------------

@dataclass
class SubjectProbe:
    did: str
    sampled_event_count: int       # from the sampling query
    observed_event_count: int      # from lookup_subject result (should match)
    labelers: int                  # number of labelers touching subject
    distinct_label_values: int     # sum of distinct vals across labelers
    wall_ms: float                 # end-to-end lookup_subject time
    refusal: Optional[str]         # None on success; e.g. "no_observed_labels"


def probe_subject(
    conn: sqlite3.Connection,
    did: str,
    sampled_event_count: int,
    audit_receipt: Optional[dict],
) -> SubjectProbe:
    """Run lookup_subject() against one DID, time it, return metrics."""
    t0 = time.perf_counter()
    result = frontdoor.lookup_subject(
        conn,
        did,
        audit_receipt=audit_receipt,
    )
    wall_ms = (time.perf_counter() - t0) * 1000.0

    if result.refusal is not None:
        return SubjectProbe(
            did=did,
            sampled_event_count=sampled_event_count,
            observed_event_count=0,
            labelers=0,
            distinct_label_values=0,
            wall_ms=round(wall_ms, 3),
            refusal=result.refusal,
        )

    observed = sum(c.event_count for c in result.labelers)
    vals = sum(len(c.label_values) for c in result.labelers)
    return SubjectProbe(
        did=did,
        sampled_event_count=sampled_event_count,
        observed_event_count=observed,
        labelers=len(result.labelers),
        distinct_label_values=vals,
        wall_ms=round(wall_ms, 3),
        refusal=None,
    )


# ---------------------------------------------------------------------------
# Percentile + verdict
# ---------------------------------------------------------------------------

def percentile(sorted_values: list[float], p: float) -> Optional[float]:
    """Linear-interpolated percentile. p in [0, 100]. None for empty input."""
    if not sorted_values:
        return None
    if p <= 0:
        return sorted_values[0]
    if p >= 100:
        return sorted_values[-1]
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return sorted_values[f]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def _summary(values: list[float]) -> dict:
    """p50 / p90 / p99 / min / max / count. None when empty."""
    if not values:
        return {"p50": None, "p90": None, "p99": None, "min": None, "max": None, "n": 0}
    s = sorted(values)
    return {
        "p50": round(percentile(s, 50), 3),
        "p90": round(percentile(s, 90), 3),
        "p99": round(percentile(s, 99), 3),
        "min": round(s[0], 3),
        "max": round(s[-1], 3),
        "n": len(s),
    }


def _verdict(probes: list[SubjectProbe]) -> tuple[str, str]:
    """Apply the verdict rule to the probe set. Returns (verdict, rationale)."""
    successful = [p for p in probes if p.refusal is None]
    if not successful:
        return ("refused_no_data",
                "no successful lookups in sample — the surface is not characterizable")

    wall_times = sorted(p.wall_ms for p in successful)
    p99 = percentile(wall_times, 99) or 0.0

    if p99 < ADMISSIBLE_P99_MS:
        return ("admissible_for_publication",
                f"p99 wall time {p99:.1f}ms < {ADMISSIBLE_P99_MS:.0f}ms threshold")
    if p99 < DEBT_P99_MS:
        return ("admissible_with_debt",
                f"p99 wall time {p99:.1f}ms in [{ADMISSIBLE_P99_MS:.0f}ms, "
                f"{DEBT_P99_MS:.0f}ms] — slow tail, surface ships but with debt")
    return ("refused_unbounded",
            f"p99 wall time {p99:.1f}ms exceeds {DEBT_P99_MS:.0f}ms — gate "
            f"the surface until remediation lands")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_probe(
    db_path: str,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    consumer_surface: str = DEFAULT_CONSUMER_SURFACE,
    audit_receipts_dir: Optional[str] = None,
) -> dict:
    """Run the full load probe and return the receipt."""
    audit_receipt = frontdoor.find_latest_audit_receipt(audit_receipts_dir)
    if audit_receipt is None:
        log.warning(
            "load_probe: no labelwatch.index_audit.v1 receipt found; "
            "the probe will refuse every lookup (gate misuse). Run the shape "
            "audit first."
        )

    conn = db_mod.connect(db_path, readonly=True)
    try:
        t_sample_start = time.perf_counter()
        sampled = sample_top_labeled_subjects(conn, sample_size)
        sample_ms = (time.perf_counter() - t_sample_start) * 1000.0

        probes: list[SubjectProbe] = []
        for entry in sampled:
            p = probe_subject(
                conn,
                entry["did"],
                entry["event_count"],
                audit_receipt=audit_receipt,
            )
            probes.append(p)
    finally:
        conn.close()

    verdict, rationale = _verdict(probes)
    successful = [p for p in probes if p.refusal is None]

    receipt = {
        "receipt_kind": RECEIPT_KIND,
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "consumer_surface": consumer_surface,
        "generated_at": format_ts(now_utc()),
        "git_commit": get_git_commit(),
        "db_path": db_path,
        "sample_size_requested": sample_size,
        "sample_size_observed": len(probes),
        "sample_method": "top_N_by_target_did_event_count",
        "sample_query_ms": round(sample_ms, 1),
        "audit_verdict": (audit_receipt or {}).get("overall_verdict"),
        "audit_receipt_path": (audit_receipt or {}).get("_receipt_path"),
        "audit_generated_at": (audit_receipt or {}).get("generated_at"),
        "successful_probes": len(successful),
        "refusals": [p.refusal for p in probes if p.refusal],
        "percentiles": {
            "wall_ms":            _summary([p.wall_ms for p in successful]),
            "labelers_per_subj":  _summary([p.labelers for p in successful]),
            "events_per_subj":    _summary([p.observed_event_count for p in successful]),
            "vals_per_subj":      _summary([p.distinct_label_values for p in successful]),
        },
        "subjects": [
            {
                "did": p.did,
                "sampled_event_count": p.sampled_event_count,
                "observed_event_count": p.observed_event_count,
                "labelers": p.labelers,
                "distinct_label_values": p.distinct_label_values,
                "wall_ms": p.wall_ms,
                "refusal": p.refusal,
            }
            for p in probes
        ],
        "verdict": verdict,
        "rationale": rationale,
    }
    receipt_for_hash = {k: v for k, v in receipt.items() if k != "receipt_hash"}
    receipt["receipt_hash"] = hash_sha256(stable_json(receipt_for_hash))
    return receipt


# ---------------------------------------------------------------------------
# Human-readable rendering
# ---------------------------------------------------------------------------

def render_text(receipt: dict) -> str:
    lines: list[str] = []
    lines.append(f"=== {receipt['receipt_kind']} ===")
    lines.append(f"consumer_surface : {receipt['consumer_surface']}")
    lines.append(f"generated_at     : {receipt['generated_at']}")
    git = receipt.get("git_commit")
    lines.append(f"git_commit       : {git[:12] if git else 'unknown'}")
    lines.append(f"db_path          : {receipt['db_path']}")
    lines.append(
        f"sample           : {receipt['sample_size_observed']} subjects "
        f"({receipt['sample_method']}, query={receipt['sample_query_ms']:.0f}ms)"
    )
    lines.append(f"audit_verdict    : {receipt.get('audit_verdict')}")
    lines.append(f"successful_probes: {receipt['successful_probes']} / {receipt['sample_size_observed']}")
    lines.append("")
    pcts = receipt["percentiles"]
    lines.append(f"{'metric':<22} {'p50':>10} {'p90':>10} {'p99':>10} {'max':>10}")
    lines.append(f"{'-'*22} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
    for name, summary in pcts.items():
        if summary["n"] == 0:
            lines.append(f"{name:<22} {'-':>10} {'-':>10} {'-':>10} {'-':>10}")
            continue
        lines.append(
            f"{name:<22} "
            f"{summary['p50']:>10.2f} {summary['p90']:>10.2f} "
            f"{summary['p99']:>10.2f} {summary['max']:>10.2f}"
        )
    lines.append("")
    lines.append(f"verdict          : {receipt['verdict']}")
    lines.append(f"rationale        : {receipt['rationale']}")

    # Slowest 10 subjects — useful when verdict ≠ admissible_for_publication.
    successful = [s for s in receipt["subjects"] if s["refusal"] is None]
    slow = sorted(successful, key=lambda s: -s["wall_ms"])[:10]
    if slow:
        lines.append("")
        lines.append("slowest subjects:")
        lines.append(
            f"  {'wall_ms':>10} {'labelers':>9} {'events':>10} {'vals':>6}  did"
        )
        for s in slow:
            lines.append(
                f"  {s['wall_ms']:>10.2f} {s['labelers']:>9} "
                f"{s['observed_event_count']:>10} {s['distinct_label_values']:>6}  "
                f"{s['did']}"
            )

    if receipt["refusals"]:
        from collections import Counter
        rf_counts = Counter(receipt["refusals"])
        lines.append("")
        lines.append(f"refusal kinds:")
        for k, v in sorted(rf_counts.items(), key=lambda kv: -kv[1]):
            lines.append(f"  {k:30s} {v}")

    return "\n".join(lines)
