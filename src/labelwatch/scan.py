from __future__ import annotations

import json
from datetime import datetime

from .config import Config
from .receipts import config_hash, receipt_hash
from .rules import run_rules
from .utils import now_utc, stable_json


def run_scan(conn, config: Config, now: datetime | None = None) -> int:
    if now is None:
        now = now_utc()
    alerts = run_rules(conn, config, now)
    cfg_hash = config_hash(config.to_receipt_dict())

    for alert in alerts:
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
    conn.commit()
    return len(alerts)
