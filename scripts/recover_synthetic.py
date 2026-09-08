#!/usr/bin/env python3
"""Rehearse Labelwatch application recovery using the fixed synthetic fixture."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from labelwatch import db, ingest, report
from labelwatch.config import Config
from labelwatch.rules import run_rules

FIXTURE = Path(__file__).resolve().parents[1] / "tests/fixtures/synthetic_streams.jsonl"
FIXTURE_SHA256 = "97c90fb0e8bc9dbf2c7da53682ebc3315400d97533ac81ffffefb2b952d2b152"
NOW = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)
CURSOR_KEY = "offline-recovery"


def fail(message: str) -> None:
    raise SystemExit(f"recovery refused: {message}")


def new_output(path: Path) -> Path:
    resolved = path.resolve()
    forbidden = [Path(p) for p in ("/etc", "/opt", "/root", "/srv", "/usr", "/var")]
    if resolved.exists():
        fail(f"output already exists: {resolved}")
    if resolved == Path("/") or any(resolved == item or item in resolved.parents for item in forbidden):
        fail(f"output is under a protected live-path prefix: {resolved}")
    if not resolved.parent.is_dir():
        fail(f"output parent does not exist: {resolved.parent}")
    resolved.mkdir()
    return resolved


def demo_config() -> Config:
    return Config(
        warmup_enabled=False, min_current_count=1,
        spike_min_count_default=1, spike_min_count_reference=1, spike_k=0.1,
        concentration_min_labels=1, concentration_threshold=0.1,
        churn_min_targets=1, churn_threshold=0.1, coverage_threshold=0.5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True,
                        help="new directory for campaign-owned recovery artifacts")
    args = parser.parse_args()
    output = new_output(args.output)
    if hashlib.sha256(FIXTURE.read_bytes()).hexdigest() != FIXTURE_SHA256:
        fail("fixed fixture digest does not match the reviewed fixture")

    with tempfile.TemporaryDirectory(prefix="working-", dir=output) as temporary:
        work = Path(temporary)
        primary = work / "primary.sqlite"
        conn = db.connect(str(primary))
        db.init_db(conn)
        ingested = ingest.ingest_from_fixture(conn, FIXTURE)
        db.set_cursor(conn, CURSOR_KEY, "cursor-37")
        for did in ("did:plc:labelerA", "did:plc:labelerB"):
            db.insert_ingest_outcome(conn, did, NOW.isoformat(), "offline-recovery",
                                     "success", ingested, None, 1, None, None, "fixture")
        conn.commit()
        findings = run_rules(conn, demo_config(), NOW)
        report.generate_report(conn, str(work / "primary-report"), now=NOW,
                               config=demo_config())
        backup_path = output / "backup.sqlite"
        backup = sqlite3.connect(backup_path)
        conn.backup(backup)
        backup.close()
        conn.close()
        restored_path = output / "restored.sqlite"
        source = sqlite3.connect(backup_path)
        restored = sqlite3.connect(restored_path)
        source.backup(restored)
        source.close()
        integrity = restored.execute("PRAGMA integrity_check").fetchone()[0]
        restored.close()
        restored_conn = db.connect(str(restored_path))
        persisted_cursor = db.get_cursor(restored_conn, CURSOR_KEY)
        event_count = restored_conn.execute("SELECT COUNT(*) FROM label_events").fetchone()[0]
        restored_findings = run_rules(restored_conn, demo_config(), NOW)
        restored_report = output / "report"
        report.generate_report(restored_conn, str(restored_report), now=NOW,
                               config=demo_config())
        restored_conn.close()

    startup = subprocess.run(
        [sys.executable, "-m", "labelwatch.cli", "--db", str(restored_path),
         "ops-status", "--format", "json", "--now", NOW.isoformat()],
        check=False, text=True, capture_output=True)
    try:
        status = json.loads(startup.stdout)
    except json.JSONDecodeError as exc:
        fail(f"installed CLI did not emit JSON status: {exc}; stderr={startup.stderr!r}")
    expected = ("flip_flop", "did:plc:labelerB")
    keys = {(item["rule_id"], item["labeler_did"]) for item in restored_findings}
    assertions = {
        "backup_restore_integrity": integrity == "ok",
        "cursor_continuity": persisted_cursor == "cursor-37",
        "event_count": event_count == 37 and ingested == 37,
        "meaningful_flip_flop_finding": expected in keys,
        "pre_backup_finding": expected in {(item["rule_id"], item["labeler_did"])
                                           for item in findings},
        "startup_exit_code": startup.returncode == 0,
        "status_schema": status.get("schema") == "project.ops.status/v1",
    }
    receipt = {
        "schema": "labelwatch.synthetic-application-recovery/v1",
        "fixture": str(FIXTURE.relative_to(FIXTURE.parents[1])),
        "fixture_sha256": FIXTURE_SHA256,
        "interpreter": sys.version.split()[0],
        "assertions": assertions,
        "observed": {"cursor": persisted_cursor, "event_count": event_count,
                     "finding_count": len(restored_findings), "integrity_check": integrity,
                     "status_state": status.get("state")},
        "passed": all(assertions.values()),
        "scope": "synthetic application/data recovery; not source reconstruction or production rollback",
    }
    (output / "result.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt, sort_keys=True))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
