from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta

from . import db
from .config import Config
from .derive import (
    LabelerSignals,
    RegimeResult,
    classify_regime_state,
    score_auditability_risk,
    score_inference_risk,
    score_temporal_coherence,
)
from .receipts import config_hash, receipt_hash
from .rules import run_rules
from .utils import format_ts, now_utc, parse_ts, stable_json

DERIVE_VERSION = "derive_v1"


def _fetch_event_stats(conn, ts_24h: str, ts_7d: str, ts_30d: str) -> dict:
    """One query: per-labeler event counts (24h/7d/30d/total) + last event ts."""
    rows = conn.execute(
        """SELECT labeler_did,
                  SUM(CASE WHEN ts >= ? THEN 1 ELSE 0 END) AS cnt_24h,
                  SUM(CASE WHEN ts >= ? THEN 1 ELSE 0 END) AS cnt_7d,
                  SUM(CASE WHEN ts >= ? THEN 1 ELSE 0 END) AS cnt_30d,
                  COUNT(*) AS cnt_total,
                  MAX(ts) AS last_event_ts
           FROM label_events
           GROUP BY labeler_did""",
        (ts_24h, ts_7d, ts_30d),
    ).fetchall()
    return {r["labeler_did"]: dict(r) for r in rows}


def _fetch_hourly_counts(conn, ts_7d: str) -> dict:
    """One query: per-labeler hourly event counts for burstiness."""
    rows = conn.execute(
        """SELECT labeler_did, strftime('%Y-%m-%d %H', ts) AS hr, COUNT(*) AS c
           FROM label_events
           WHERE ts >= ?
           GROUP BY labeler_did, hr""",
        (ts_7d,),
    ).fetchall()
    result: dict[str, dict[str, int]] = defaultdict(dict)
    for r in rows:
        result[r["labeler_did"]][r["hr"]] = r["c"]
    return result


def _fetch_probe_history(conn, ts_7d: str, ts_30d: str) -> dict:
    """One query: per-labeler probe statuses (30d), split into 30d/7d in memory."""
    rows = conn.execute(
        """SELECT labeler_did, ts, normalized_status
           FROM labeler_probe_history
           WHERE ts >= ?
           ORDER BY labeler_did, ts""",
        (ts_30d,),
    ).fetchall()
    result: dict[str, dict] = {}
    current_did = None
    statuses_30d: list[str] = []
    statuses_7d: list[str] = []

    def _flush(did):
        nonlocal statuses_30d, statuses_7d
        count = len(statuses_30d)
        successes = sum(1 for s in statuses_30d if s == "accessible")
        # Transitions
        transitions = 0
        for i in range(1, len(statuses_30d)):
            if statuses_30d[i] != statuses_30d[i - 1]:
                transitions += 1
        # Fail streak (from end)
        fail_streak = 0
        for s in reversed(statuses_30d):
            if s != "accessible":
                fail_streak += 1
            else:
                break
        result[did] = {
            "probe_count_30d": count,
            "probe_success_ratio_30d": successes / count if count else 0.0,
            "probe_transition_count_30d": transitions,
            "probe_recent_fail_streak": fail_streak,
            "probe_statuses_7d": list(statuses_7d),
        }

    for r in rows:
        did = r["labeler_did"]
        if did != current_did:
            if current_did is not None:
                _flush(current_did)
            current_did = did
            statuses_30d = []
            statuses_7d = []
        statuses_30d.append(r["normalized_status"])
        if r["ts"] >= ts_7d:
            statuses_7d.append(r["normalized_status"])
    if current_did is not None:
        _flush(current_did)
    return result


def _fetch_receipt_stats(conn, ts_30d: str) -> dict:
    """One query: per-labeler derived receipt counts by type (30d)."""
    rows = conn.execute(
        """SELECT labeler_did, receipt_type, COUNT(*) AS c
           FROM derived_receipts
           WHERE ts >= ?
           GROUP BY labeler_did, receipt_type""",
        (ts_30d,),
    ).fetchall()
    result: dict[str, dict[str, int]] = defaultdict(lambda: {"regime": 0, "inference_risk": 0})
    for r in rows:
        result[r["labeler_did"]][r["receipt_type"]] = r["c"]
    return result


def _fetch_last_regime_change(conn) -> dict:
    """One query: per-labeler most recent regime change timestamp."""
    rows = conn.execute(
        """SELECT labeler_did, MAX(ts) AS ts
           FROM derived_receipts
           WHERE receipt_type = 'regime'
           GROUP BY labeler_did""",
    ).fetchall()
    return {r["labeler_did"]: r["ts"] for r in rows}


def _build_all_signals(conn, config: Config, now: datetime) -> dict[str, LabelerSignals]:
    """Build LabelerSignals for all labelers using batched queries.

    ~6 grouped queries instead of ~10 per labeler.
    """
    ts_24h = format_ts(now - timedelta(hours=24))
    ts_7d = format_ts(now - timedelta(days=7))
    ts_30d = format_ts(now - timedelta(days=30))

    # Batch queries (6 total)
    event_stats = _fetch_event_stats(conn, ts_24h, ts_7d, ts_30d)
    hourly_map = _fetch_hourly_counts(conn, ts_7d)
    probe_stats = _fetch_probe_history(conn, ts_7d, ts_30d)
    receipt_stats = _fetch_receipt_stats(conn, ts_30d)
    last_regime = _fetch_last_regime_change(conn)

    labelers = conn.execute("SELECT * FROM labelers").fetchall()

    # Pre-compute hour keys for 168-slot array
    hour_keys = []
    for i in range(168):
        hr_dt = now - timedelta(hours=167 - i)
        hour_keys.append(hr_dt.strftime("%Y-%m-%d %H"))

    signals_map: dict[str, LabelerSignals] = {}
    empty_event_stats = {"cnt_24h": 0, "cnt_7d": 0, "cnt_30d": 0, "cnt_total": 0, "last_event_ts": None}
    empty_probe_stats = {
        "probe_count_30d": 0, "probe_success_ratio_30d": 0.0,
        "probe_transition_count_30d": 0, "probe_recent_fail_streak": 0,
        "probe_statuses_7d": [],
    }

    for row in labelers:
        did = row["labeler_did"]

        # Event data
        ev = event_stats.get(did, empty_event_stats)

        # Hourly counts (fill 168 slots)
        did_hourly = hourly_map.get(did, {})
        hourly_counts = [did_hourly.get(hk, 0) for hk in hour_keys]

        # Dormancy
        last_event_ts = ev["last_event_ts"]
        if last_event_ts:
            dormancy_days = (now - parse_ts(last_event_ts)).total_seconds() / 86400
        else:
            first_seen = row["first_seen"]
            dormancy_days = (now - parse_ts(first_seen)).total_seconds() / 86400 if first_seen else 999.0

        # Age
        first_seen_hours = 999.0
        if row["first_seen"]:
            first_seen_hours = (now - parse_ts(row["first_seen"])).total_seconds() / 3600

        # Probe data
        pr = probe_stats.get(did, empty_probe_stats)

        # Receipt data
        rc = receipt_stats.get(did, {"regime": 0, "inference_risk": 0})

        # Recent regime change
        recent_class_change_hours = None
        regime_ts = last_regime.get(did)
        if regime_ts:
            recent_class_change_hours = (now - parse_ts(regime_ts)).total_seconds() / 3600

        signals_map[did] = LabelerSignals(
            labeler_did=did,
            visibility_class=row["visibility_class"] or "unresolved",
            auditability=row["auditability"] or "low",
            classification_confidence=row["classification_confidence"] or "low",
            likely_test_dev=bool(row["likely_test_dev"]),
            first_seen_hours_ago=first_seen_hours,
            scan_count=row["scan_count"] or 0,
            event_count_total=ev["cnt_total"],
            warmup_enabled=config.warmup_enabled,
            warmup_min_age_hours=config.warmup_min_age_hours,
            warmup_min_events=config.warmup_min_events,
            warmup_min_scans=config.warmup_min_scans,
            event_count_24h=ev["cnt_24h"],
            event_count_7d=ev["cnt_7d"],
            event_count_30d=ev["cnt_30d"],
            hourly_counts_7d=hourly_counts,
            interarrival_secs_7d=[],
            dormancy_days=dormancy_days,
            probe_count_30d=pr["probe_count_30d"],
            probe_success_ratio_30d=pr["probe_success_ratio_30d"],
            probe_transition_count_30d=pr["probe_transition_count_30d"],
            probe_last_status=row["endpoint_status"],
            probe_statuses_7d=pr["probe_statuses_7d"],
            probe_recent_fail_streak=pr["probe_recent_fail_streak"],
            class_transition_count_30d=rc["regime"],
            confidence_transition_count_30d=rc["inference_risk"],
            recent_class_change_hours_ago=recent_class_change_hours,
            declared_record=bool(row["declared_record"]),
            has_labeler_service=bool(row["has_labeler_service"]),
            has_label_key=bool(row["has_label_key"]),
            observed_as_src=bool(row["observed_as_src"]),
        )

    return signals_map


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
    """Run regime/risk/coherence derivation for all labelers.

    Uses batched queries (~6 total) instead of per-labeler queries.
    """
    ts = format_ts(now)

    # Build all signals in one pass (6 grouped queries)
    signals_map = _build_all_signals(conn, config, now)

    # Fetch labeler rows for previous derived values
    labelers = conn.execute("SELECT * FROM labelers").fetchall()

    threshold = config.regime_hysteresis_scans

    for row in labelers:
        did = row["labeler_did"]
        signals = signals_map.get(did)
        if signals is None:
            continue

        # Classify (computed proposal)
        regime = classify_regime_state(signals)
        computed = regime.regime_state

        # Hysteresis: determine effective regime
        current = row["regime_state"] or ""
        pending = row["regime_pending"]
        pending_count = row["regime_pending_count"] or 0

        if current == "":
            # First derive — accept immediately, no hysteresis
            effective = computed
            pending = None
            pending_count = 0
        elif computed == current:
            # Steady state — clear any pending
            effective = current
            pending = None
            pending_count = 0
        elif computed == pending:
            # Same proposal as last pass — increment
            pending_count += 1
            if pending_count >= threshold:
                effective = computed
                pending = None
                pending_count = 0
            else:
                effective = current
        else:
            # New/different proposal — reset counter
            effective = current
            pending = computed
            pending_count = 1

        # Build effective RegimeResult for scoring and receipts
        if effective == computed:
            effective_regime = regime
        else:
            effective_regime = RegimeResult(effective, regime.reason_codes)

        audit_risk = score_auditability_risk(signals)
        inf_risk = score_inference_risk(signals, effective_regime)
        coherence = score_temporal_coherence(signals, effective_regime)

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

        # Emit receipts on change (using effective regime)
        prev_regime = row["regime_state"] or ""
        prev_audit = str(row["auditability_risk"] or "")
        prev_inf = str(row["inference_risk"] or "")

        _emit_receipt_if_changed(
            conn, did, "regime", prev_regime, effective_regime.regime_state,
            effective_regime.reason_codes, input_hash, ts,
        )
        _emit_receipt_if_changed(
            conn, did, "auditability_risk", prev_audit, str(audit_risk.score),
            audit_risk.reason_codes, input_hash, ts,
        )
        _emit_receipt_if_changed(
            conn, did, "inference_risk", prev_inf, str(inf_risk.score),
            inf_risk.reason_codes, input_hash, ts,
        )

        # Shift current scores to prev (only if current is not NULL)
        audit_prev = row["auditability_risk"] if row["auditability_risk"] is not None else None
        inf_prev = row["inference_risk"] if row["inference_risk"] is not None else None
        coh_prev = row["temporal_coherence"] if row["temporal_coherence"] is not None else None

        # Update labeler row (with effective regime + pending state + prev scores)
        db.update_labeler_derived(
            conn, did,
            regime_state=effective_regime.regime_state,
            regime_reason_codes=json.dumps(effective_regime.reason_codes, separators=(",", ":")),
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
            regime_pending=pending,
            regime_pending_count=pending_count,
            auditability_risk_prev=audit_prev,
            inference_risk_prev=inf_prev,
            temporal_coherence_prev=coh_prev,
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

    # Batch increment scan_count for all labelers (1 query instead of N)
    conn.execute("UPDATE labelers SET scan_count = scan_count + 1")

    conn.commit()
    return len(alerts)


def run_derive(conn, config: Config, now: datetime | None = None) -> None:
    """Run regime/risk/coherence derivation (expensive — call less often than scan)."""
    if now is None:
        now = now_utc()
    _run_derive_pass(conn, config, now)
    conn.commit()
