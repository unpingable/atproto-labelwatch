from __future__ import annotations

import json
from datetime import datetime, timedelta

from . import db
from .config import Config
from .derive import (
    LabelerSignals,
    classify_regime_state,
    score_auditability_risk,
    score_inference_risk,
    score_temporal_coherence,
)
from .receipts import config_hash, receipt_hash
from .rules import run_rules
from .utils import format_ts, now_utc, parse_ts, stable_json

DERIVE_VERSION = "derive_v1"


def _build_signals(conn, row, config: Config, now: datetime) -> LabelerSignals:
    """Build LabelerSignals from a labeler DB row + aggregated queries."""
    did = row["labeler_did"]

    # Event counts
    ts_24h = format_ts(now - timedelta(hours=24))
    ts_7d = format_ts(now - timedelta(days=7))
    ts_30d = format_ts(now - timedelta(days=30))

    event_count_24h = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=?",
        (did, ts_24h),
    ).fetchone()["c"]
    event_count_7d = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=?",
        (did, ts_7d),
    ).fetchone()["c"]
    event_count_30d = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=? AND ts>=?",
        (did, ts_30d),
    ).fetchone()["c"]
    event_count_total = conn.execute(
        "SELECT COUNT(*) AS c FROM label_events WHERE labeler_did=?",
        (did,),
    ).fetchone()["c"]

    # Hourly counts for 7d (for burstiness)
    hourly_counts: list[int] = []
    rows_h = conn.execute(
        """SELECT strftime('%Y-%m-%d %H', ts) AS hr, COUNT(*) AS c
           FROM label_events WHERE labeler_did=? AND ts>=?
           GROUP BY hr ORDER BY hr""",
        (did, ts_7d),
    ).fetchall()
    hour_map = {r["hr"]: r["c"] for r in rows_h}
    # Fill 168 hours
    for i in range(168):
        hr_dt = now - timedelta(hours=167 - i)
        hr_key = hr_dt.strftime("%Y-%m-%d %H")
        hourly_counts.append(hour_map.get(hr_key, 0))

    # Dormancy
    last_event = conn.execute(
        "SELECT MAX(ts) AS ts FROM label_events WHERE labeler_did=?", (did,)
    ).fetchone()["ts"]
    if last_event:
        dormancy_days = (now - parse_ts(last_event)).total_seconds() / 86400
    else:
        first_seen = row["first_seen"]
        dormancy_days = (now - parse_ts(first_seen)).total_seconds() / 86400 if first_seen else 999.0

    # Age
    first_seen_hours = 999.0
    if row["first_seen"]:
        first_seen_hours = (now - parse_ts(row["first_seen"])).total_seconds() / 3600

    # Probe stats from probe_history
    probe_rows = conn.execute(
        "SELECT normalized_status FROM labeler_probe_history WHERE labeler_did=? AND ts>=? ORDER BY ts",
        (did, ts_30d),
    ).fetchall()
    probe_count_30d = len(probe_rows)
    probe_successes = sum(1 for r in probe_rows if r["normalized_status"] == "accessible")
    probe_success_ratio = probe_successes / probe_count_30d if probe_count_30d else 0.0

    # Probe transitions
    probe_statuses_30d = [r["normalized_status"] for r in probe_rows]
    transitions = 0
    for i in range(1, len(probe_statuses_30d)):
        if probe_statuses_30d[i] != probe_statuses_30d[i - 1]:
            transitions += 1

    # Recent probe statuses (7d)
    probe_rows_7d = conn.execute(
        "SELECT normalized_status FROM labeler_probe_history WHERE labeler_did=? AND ts>=? ORDER BY ts",
        (did, ts_7d),
    ).fetchall()
    probe_statuses_7d = [r["normalized_status"] for r in probe_rows_7d]

    # Fail streak
    fail_streak = 0
    for status in reversed(probe_statuses_30d):
        if status != "accessible":
            fail_streak += 1
        else:
            break

    # Class/confidence churn from derived_receipts
    class_transitions = conn.execute(
        "SELECT COUNT(*) AS c FROM derived_receipts WHERE labeler_did=? AND receipt_type='regime' AND ts>=?",
        (did, ts_30d),
    ).fetchone()["c"]
    conf_transitions = conn.execute(
        "SELECT COUNT(*) AS c FROM derived_receipts WHERE labeler_did=? AND receipt_type='inference_risk' AND ts>=?",
        (did, ts_30d),
    ).fetchone()["c"]

    # Recent class change
    last_regime_change = conn.execute(
        "SELECT ts FROM derived_receipts WHERE labeler_did=? AND receipt_type='regime' ORDER BY ts DESC LIMIT 1",
        (did,),
    ).fetchone()
    recent_class_change_hours = None
    if last_regime_change:
        recent_class_change_hours = (now - parse_ts(last_regime_change["ts"])).total_seconds() / 3600

    return LabelerSignals(
        labeler_did=did,
        visibility_class=row["visibility_class"] or "unresolved",
        auditability=row["auditability"] or "low",
        classification_confidence=row["classification_confidence"] or "low",
        likely_test_dev=bool(row["likely_test_dev"]),
        first_seen_hours_ago=first_seen_hours,
        scan_count=row["scan_count"] or 0,
        event_count_total=event_count_total,
        warmup_enabled=config.warmup_enabled,
        warmup_min_age_hours=config.warmup_min_age_hours,
        warmup_min_events=config.warmup_min_events,
        warmup_min_scans=config.warmup_min_scans,
        event_count_24h=event_count_24h,
        event_count_7d=event_count_7d,
        event_count_30d=event_count_30d,
        hourly_counts_7d=hourly_counts,
        interarrival_secs_7d=[],  # expensive to compute; empty triggers neutral cadence score
        dormancy_days=dormancy_days,
        probe_count_30d=probe_count_30d,
        probe_success_ratio_30d=probe_success_ratio,
        probe_transition_count_30d=transitions,
        probe_last_status=row["endpoint_status"],
        probe_statuses_7d=probe_statuses_7d,
        probe_recent_fail_streak=fail_streak,
        class_transition_count_30d=class_transitions,
        confidence_transition_count_30d=conf_transitions,
        recent_class_change_hours_ago=recent_class_change_hours,
        declared_record=bool(row["declared_record"]),
        has_labeler_service=bool(row["has_labeler_service"]),
        has_label_key=bool(row["has_label_key"]),
        observed_as_src=bool(row["observed_as_src"]),
    )


def _emit_receipt_if_changed(conn, did: str, receipt_type: str,
                              prev_value: str, new_value: str,
                              reason_codes: list[str], input_hash: str,
                              ts: str) -> bool:
    """Insert a derived receipt if the value changed. Returns True if emitted."""
    if prev_value == new_value:
        return False
    reason_json = json.dumps(reason_codes, separators=(",", ":"))
    db.insert_derived_receipt(
        conn, did, receipt_type, DERIVE_VERSION, "scan",
        ts, input_hash, prev_value, new_value, reason_json,
    )
    return True


def _run_derive_pass(conn, config: Config, now: datetime) -> None:
    """Run regime/risk/coherence derivation for all labelers."""
    ts = format_ts(now)
    labelers = conn.execute("SELECT * FROM labelers").fetchall()

    for row in labelers:
        did = row["labeler_did"]
        signals = _build_signals(conn, row, config, now)

        # Classify
        regime = classify_regime_state(signals)
        audit_risk = score_auditability_risk(signals)
        inf_risk = score_inference_risk(signals, regime)
        coherence = score_temporal_coherence(signals, regime)

        # Build input hash for receipts
        input_hash = stable_json({
            "visibility_class": signals.visibility_class,
            "event_count_30d": signals.event_count_30d,
            "probe_count_30d": signals.probe_count_30d,
            "probe_success_ratio_30d": round(signals.probe_success_ratio_30d, 3),
            "probe_transition_count_30d": signals.probe_transition_count_30d,
            "dormancy_days": round(signals.dormancy_days, 1),
            "scan_count": signals.scan_count,
        })

        # Emit receipts on change
        prev_regime = row["regime_state"] or ""
        prev_audit = str(row["auditability_risk"] or "")
        prev_inf = str(row["inference_risk"] or "")

        _emit_receipt_if_changed(
            conn, did, "regime", prev_regime, regime.regime_state,
            regime.reason_codes, input_hash, ts,
        )
        _emit_receipt_if_changed(
            conn, did, "auditability_risk", prev_audit, str(audit_risk.score),
            audit_risk.reason_codes, input_hash, ts,
        )
        _emit_receipt_if_changed(
            conn, did, "inference_risk", prev_inf, str(inf_risk.score),
            inf_risk.reason_codes, input_hash, ts,
        )

        # Update labeler row
        db.update_labeler_derived(
            conn, did,
            regime_state=regime.regime_state,
            regime_reason_codes=json.dumps(regime.reason_codes, separators=(",", ":")),
            auditability_risk=audit_risk.score,
            auditability_risk_band=audit_risk.band,
            auditability_risk_reasons=json.dumps(audit_risk.reason_codes, separators=(",", ":")),
            inference_risk=inf_risk.score,
            inference_risk_band=inf_risk.band,
            inference_risk_reasons=json.dumps(inf_risk.reason_codes, separators=(",", ":")),
            temporal_coherence=coherence.score,
            temporal_coherence_band=coherence.band,
            temporal_coherence_reasons=json.dumps(coherence.reason_codes, separators=(",", ":")),
            derive_version=DERIVE_VERSION,
            derived_at=ts,
        )


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

    # Increment scan_count for all labelers
    labeler_rows = conn.execute("SELECT labeler_did FROM labelers").fetchall()
    for row in labeler_rows:
        db.increment_scan_count(conn, row["labeler_did"])

    # Run derive pass (regime, risk scores, coherence)
    _run_derive_pass(conn, config, now)

    conn.commit()
    return len(alerts)
