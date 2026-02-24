from __future__ import annotations

import json
from datetime import datetime

from . import db
from .config import Config
from .receipts import config_hash, receipt_hash
from .rules import run_rules
from .utils import now_utc, stable_json


def run_scan(conn, config: Config, now: datetime | None = None) -> int:
    if now is None:
        now = now_utc()
    alerts = run_rules(conn, config, now)
    cfg_hash = config_hash(config.to_receipt_dict())

    # Track which labelers were evaluated (appeared in alerts)
    evaluated_labelers = set()

    for alert in alerts:
        evaluated_labelers.add(alert["labeler_did"])
        inputs_json = stable_json(alert["inputs"])
        evidence_json = json.dumps(alert["evidence_hashes"], sort_keys=True)
        receipt = receipt_hash(
            alert["rule_id"],
            alert["labeler_did"],
            alert["ts"],
            alert["inputs"],
            alert["evidence_hashes"],
            cfg_hash,
        )
        conn.execute(
            """
            INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert["rule_id"],
                alert["labeler_did"],
                alert["ts"],
                inputs_json,
                evidence_json,
                cfg_hash,
                receipt,
            ),
        )

    # Increment scan_count for all labelers that were actually evaluated by the rule pipeline
    # This includes all labelers that had rules run against them (not just those that triggered alerts)
    # Since rules iterate all labelers, increment for all labelers in the DB
    labeler_rows = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labeler_rows:
        db.increment_scan_count(conn, row["labeler_did"])

    conn.commit()
    return len(alerts)
