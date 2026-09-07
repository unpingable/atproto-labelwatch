#!/usr/bin/env python3
"""Ingest the synthetic stream and assert a real Labelwatch finding."""
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from labelwatch import db, ingest, ops_status, report
from labelwatch.config import Config
from labelwatch.rules import run_rules

NOW = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    owner = None
    if args.output is None:
        owner = tempfile.TemporaryDirectory(prefix="labelwatch-demo-")
        root = Path(owner.name)
    else:
        root = args.output.resolve()
        root.mkdir(parents=True, exist_ok=True)
    path = root / "labelwatch.sqlite"
    conn = db.connect(str(path))
    db.init_db(conn)
    ingested = ingest.ingest_from_fixture(conn, Path("tests/fixtures/synthetic_streams.jsonl"))
    for did in ("did:plc:labelerA", "did:plc:labelerB"):
        db.insert_ingest_outcome(conn, did, NOW.isoformat(), "offline-demo",
                                 "success", ingested, None, 1, None, None,
                                 "fixture")
    conn.commit()
    config = Config(
        warmup_enabled=False, min_current_count=1,
        spike_min_count_default=1, spike_min_count_reference=1, spike_k=0.1,
        concentration_min_labels=1, concentration_threshold=0.1,
        churn_min_targets=1, churn_threshold=0.1, coverage_threshold=0.5,
    )
    findings = run_rules(conn, config, NOW)
    keys = {(item["rule_id"], item["labeler_did"]) for item in findings}
    expected = ("flip_flop", "did:plc:labelerB")
    if ingested != 37 or expected not in keys:
        raise SystemExit(f"unexpected result: ingested={ingested}, findings={sorted(keys)}")
    rendered = root / "report"
    report.generate_report(conn, str(rendered), now=NOW, config=config)
    status = ops_status.build_status(path, now=NOW)
    result = {
        "schema": "labelwatch.offline-demo/v1",
        "ingested": ingested,
        "finding_count": len(findings),
        "expected_finding": list(expected),
        "report": str(rendered / "index.html"),
        "status_schema": status["schema"],
    }
    print(json.dumps(result, sort_keys=True))
    conn.close()
    if owner is not None:
        owner.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
