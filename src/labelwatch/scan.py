from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import time
from bisect import bisect_left
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
REVERSAL_CAP_PER_LABELER = 50_000


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


def _fetch_interarrival_secs(conn, ts_7d: str) -> dict[str, list[float]]:
    """One query: per-labeler inter-arrival times (seconds) from 7d event timestamps.

    Streams rows via cursor (never fetchall) to avoid loading millions of rows
    into memory. Computes deltas inline since data is ordered by (labeler_did, ts).
    Capped at 5000 events per labeler to bound memory.
    """
    cursor = conn.execute(
        """SELECT labeler_did, ts FROM label_events
           WHERE ts >= ?
           ORDER BY labeler_did, ts""",
        (ts_7d,),
    )

    result: dict[str, list[float]] = {}
    cap = 5000
    current_did: str | None = None
    prev_ts = None
    count = 0
    deltas: list[float] = []

    for r in cursor:
        did = r["labeler_did"]
        if did != current_did:
            # Flush previous labeler
            if current_did is not None:
                result[current_did] = deltas
            current_did = did
            prev_ts = parse_ts(r["ts"])
            count = 1
            deltas = []
            continue

        count += 1
        if count > cap:
            continue

        cur = parse_ts(r["ts"])
        delta = (cur - prev_ts).total_seconds()
        if delta >= 0:
            deltas.append(delta)
        prev_ts = cur

    # Flush last labeler
    if current_did is not None:
        result[current_did] = deltas

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

    # Batch queries (7 total)
    event_stats = _fetch_event_stats(conn, ts_24h, ts_7d, ts_30d)
    hourly_map = _fetch_hourly_counts(conn, ts_7d)
    interarrival_map = _fetch_interarrival_secs(conn, ts_7d)
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
            interarrival_secs_7d=interarrival_map.get(did, []),
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
        is_warmup = 1 if alert["inputs"].get("warmup") else 0
        conn.execute(
            """
            INSERT INTO alerts(rule_id, labeler_did, ts, inputs_json, evidence_hashes_json, config_hash, receipt_hash, warmup_alert)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert["rule_id"],
                alert["labeler_did"],
                alert["ts"],
                inputs_json,
                evidence_json,
                cfg_hash,
                receipt,
                is_warmup,
            ),
        )

    # Batch increment scan_count for all labelers (1 query instead of N)
    conn.execute("UPDATE labelers SET scan_count = scan_count + 1")

    conn.commit()
    return len(alerts)


def _update_coverage_columns(conn, config: Config, now: datetime) -> None:
    """Batch-update labelers.coverage_* columns from ingest_outcomes."""
    window_start = format_ts(now - timedelta(minutes=config.coverage_window_minutes))
    try:
        rows = conn.execute(
            """SELECT labeler_did,
                      COUNT(*) AS attempts,
                      SUM(CASE WHEN outcome IN ('success','empty') THEN 1 ELSE 0 END) AS successes
               FROM ingest_outcomes WHERE ts >= ? GROUP BY labeler_did""",
            (window_start,),
        ).fetchall()
    except Exception:
        return

    for r in rows:
        attempts = r["attempts"]
        successes = r["successes"]
        ratio = successes / attempts if attempts > 0 else 0.0
        conn.execute(
            """UPDATE labelers SET
                coverage_ratio=?, coverage_window_successes=?, coverage_window_attempts=?
               WHERE labeler_did=?""",
            (ratio, successes, attempts, r["labeler_did"]),
        )

    # Update last_ingest_success_ts and last_ingest_attempt_ts
    try:
        success_rows = conn.execute(
            """SELECT labeler_did, MAX(ts) AS ts
               FROM ingest_outcomes WHERE outcome IN ('success','empty')
               GROUP BY labeler_did"""
        ).fetchall()
        for r in success_rows:
            conn.execute(
                "UPDATE labelers SET last_ingest_success_ts=? WHERE labeler_did=?",
                (r["ts"], r["labeler_did"]),
            )

        attempt_rows = conn.execute(
            "SELECT labeler_did, MAX(ts) AS ts FROM ingest_outcomes GROUP BY labeler_did"
        ).fetchall()
        for r in attempt_rows:
            conn.execute(
                "UPDATE labelers SET last_ingest_attempt_ts=? WHERE labeler_did=?",
                (r["ts"], r["labeler_did"]),
            )
    except Exception:
        pass


def _cleanup_ingest_outcomes(conn, now: datetime) -> None:
    """Remove ingest_outcomes older than 7 days."""
    cutoff = format_ts(now - timedelta(days=7))
    try:
        conn.execute("DELETE FROM ingest_outcomes WHERE ts < ?", (cutoff,))
    except Exception:
        pass


_log = logging.getLogger("labelwatch.scan")


def _sync_driftwatch_facts(conn, config: Config) -> None:
    """Join label_events with driftwatch facts sidecar to compute lag_sec_claimed."""
    path = config.driftwatch_facts_path
    if not path or not os.path.exists(path):
        return

    # Validate path (ATTACH doesn't support parameter binding)
    if "'" in path or ";" in path:
        _log.warning("driftwatch_facts_path contains unsafe characters, skipping")
        return

    # Retry once on ATTACH failure (rename race window)
    for attempt in range(2):
        try:
            conn.execute(f"ATTACH DATABASE 'file:{path}?mode=ro' AS drift")
            break
        except sqlite3.OperationalError:
            if attempt == 0:
                time.sleep(1)
                continue
            raise

    try:
        hwm_row = conn.execute(
            "SELECT COALESCE(MAX(label_event_id), 0) FROM derived_label_fp"
        ).fetchone()
        hwm = hwm_row[0] if hwm_row else 0

        # 72h overlap floor as epoch
        overlap_epoch = int(time.time()) - (72 * 3600)

        conn.execute("""
            INSERT OR REPLACE INTO derived_label_fp
                (label_event_id, labeler_did, uri, label_ts,
                 claim_fingerprint, post_created_ts, lag_sec_claimed)
            SELECT
                le.id, le.labeler_did, le.uri, le.ts,
                uf.fingerprint,
                datetime(uf.created_epoch, 'unixepoch'),
                CAST(strftime('%s', le.ts) AS INTEGER) - uf.created_epoch
            FROM label_events le
            JOIN drift.uri_fingerprint uf ON le.uri = uf.post_uri
            WHERE (le.id > ?
                   OR CAST(strftime('%s', le.ts) AS INTEGER) >= ?)
              AND le.uri LIKE 'at://%/app.bsky.feed.post/%'
        """, (hwm, overlap_epoch))
        conn.commit()
    finally:
        conn.execute("DETACH DATABASE drift")


def _compute_labeler_lag_7d(conn) -> None:
    """Aggregate per-labeler lag stats from derived_label_fp (last 7 days)."""
    cutoff_epoch = int(time.time()) - (7 * 86400)

    rows = conn.execute("""
        SELECT labeler_did, lag_sec_claimed
        FROM derived_label_fp
        WHERE CAST(strftime('%s', label_ts) AS INTEGER) >= ?
    """, (cutoff_epoch,)).fetchall()

    per_labeler: dict[str, list] = defaultdict(list)
    for r in rows:
        per_labeler[r["labeler_did"]].append(r["lag_sec_claimed"])

    now_epoch = int(time.time())
    conn.execute("DELETE FROM derived_labeler_lag_7d")

    for did, lags in per_labeler.items():
        n_total = len(lags)
        null_count = sum(1 for l in lags if l is None)
        neg_count = sum(1 for l in lags if l is not None and l < 0)
        non_null = sorted(l for l in lags if l is not None)

        null_rate = null_count / n_total if n_total > 0 else 0.0
        neg_rate = neg_count / n_total if n_total > 0 else 0.0

        if non_null:
            p50 = non_null[len(non_null) // 2]
            p90_idx = min(int(len(non_null) * 0.9), len(non_null) - 1)
            p90 = non_null[p90_idx]
            p95_idx = min(int(len(non_null) * 0.95), len(non_null) - 1)
            p95 = non_null[p95_idx]
            p99_idx = min(int(len(non_null) * 0.99), len(non_null) - 1)
            p99 = non_null[p99_idx]
            p90_p50_ratio = round(p90 / p50, 1) if p50 > 0 else None
        else:
            p50 = None
            p90 = None
            p95 = None
            p99 = None
            p90_p50_ratio = None

        conn.execute(
            """INSERT INTO derived_labeler_lag_7d
               (labeler_did, n_total, null_rate, p50_lag, p90_lag,
                p95_lag, p99_lag, p90_p50_ratio, neg_rate, updated_epoch)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (did, n_total, null_rate, p50, p90, p95, p99, p90_p50_ratio, neg_rate, now_epoch),
        )


def _compute_reversal_stats_7d(conn) -> None:
    """Compute per-labeler reversal (apply→negate) stats from label_events (last 7 days)."""
    cutoff_epoch = int(time.time()) - (7 * 86400)
    # ISO cutoff lets SQLite prune rows before expensive epoch conversion + sort
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_epoch))

    cursor = conn.execute("""
        WITH e AS (
            SELECT labeler_did, uri, val, neg,
                   CAST(strftime('%s', ts) AS INTEGER) AS ts_epoch
            FROM label_events
            WHERE uri LIKE 'at://%/app.bsky.feed.post/%'
              AND ts >= ?
        )
        SELECT * FROM e
        WHERE ts_epoch IS NOT NULL AND ts_epoch >= ?
        ORDER BY labeler_did, uri, val, ts_epoch
    """, (cutoff_iso, cutoff_epoch))

    events_by_labeler: dict[str, int] = defaultdict(int)
    truncated_labelers: set[str] = set()
    per_labeler: dict[str, dict] = defaultdict(
        lambda: {"apply_events": 0, "apply_groups": 0, "dwells": [], "val_counts": defaultdict(int)}
    )

    current_group = None
    last_apply_epoch = None
    pair_found = False

    for r in cursor:
        did = r["labeler_did"]
        events_by_labeler[did] += 1
        if events_by_labeler[did] > REVERSAL_CAP_PER_LABELER:
            truncated_labelers.add(did)
            continue

        group_key = (did, r["uri"], r["val"])
        if group_key != current_group:
            # Count the previous group if it had an apply
            if current_group is not None and last_apply_epoch is not None:
                per_labeler[current_group[0]]["apply_groups"] += 1
            current_group = group_key
            last_apply_epoch = None
            pair_found = False

        neg = r["neg"]
        ts_epoch = r["ts_epoch"]

        if neg == 0:
            per_labeler[did]["apply_events"] += 1
            last_apply_epoch = ts_epoch
        elif neg == 1 and last_apply_epoch is not None and not pair_found:
            dwell = ts_epoch - last_apply_epoch
            per_labeler[did]["dwells"].append(dwell)
            val_key = r["val"] if r["val"] is not None else "<null>"
            per_labeler[did]["val_counts"][val_key] += 1
            pair_found = True

    # Don't forget the last group
    if current_group is not None and last_apply_epoch is not None:
        per_labeler[current_group[0]]["apply_groups"] += 1

    now_epoch = int(time.time())
    conn.execute("DELETE FROM derived_labeler_reversal_7d")

    for did, stats in per_labeler.items():
        n_apply_events = stats["apply_events"]
        n_apply_groups = stats["apply_groups"]
        dwells = stats["dwells"]
        n_reversals = len(dwells)
        pct_reversed = round(n_reversals / n_apply_groups, 4) if n_apply_groups > 0 else 0.0

        if dwells:
            dwells.sort()
            p50 = dwells[len(dwells) // 2]
            p90 = dwells[min(int(len(dwells) * 0.9), len(dwells) - 1)]
            p95 = dwells[min(int(len(dwells) * 0.95), len(dwells) - 1)]
            p99 = dwells[min(int(len(dwells) * 0.99), len(dwells) - 1)]
        else:
            p50 = p90 = p95 = p99 = None

        val_counts = stats["val_counts"]
        if val_counts:
            top_val = max(val_counts, key=val_counts.get)
            top_val_pct = round(val_counts[top_val] / n_reversals, 4) if n_reversals > 0 else None
        else:
            top_val = None
            top_val_pct = None

        truncated = 1 if did in truncated_labelers else 0

        conn.execute(
            "INSERT INTO derived_labeler_reversal_7d VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (did, n_apply_events, n_apply_groups, n_reversals, pct_reversed,
             p50, p90, p95, p99, top_val, top_val_pct, truncated, now_epoch),
        )


def _nearest_rank(sorted_vals, p):
    """Nearest-rank percentile on a pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return None
    idx = max(0, math.ceil(p * n) - 1)
    return sorted_vals[idx]


def _shannon_entropy(counts: list[int]) -> tuple[float, float | None, float]:
    """Return (H_bits, H_norm, N_eff) from a list of value counts."""
    total = sum(counts)
    if total == 0:
        return (0.0, None, 0.0)
    k = len(counts)
    h = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            h -= p * math.log2(p)
    h_norm = h / math.log2(k) if k >= 2 else None
    n_eff = 2 ** h
    return (h, h_norm, n_eff)


def _update_val_dist_day(conn) -> None:
    """Incrementally update derived_val_dist_day from label_events.

    Recomputes the last 7 days (covers late-arriving self-reported timestamps).
    Prunes rows older than 60 days.
    """
    now_epoch = int(time.time())
    start_day_epoch = ((now_epoch // 86400) - 6) * 86400  # 7 days back
    cutoff_iso = time.strftime("%Y-%m-%dT00:00:00.000000Z", time.gmtime(start_day_epoch))
    retention_cutoff_day_epoch = ((now_epoch // 86400) - 60) * 86400

    # Delete recompute window
    conn.execute(
        "DELETE FROM derived_val_dist_day WHERE day_epoch >= ?",
        (start_day_epoch,),
    )

    # Reinsert from label_events
    conn.execute("""
        INSERT OR REPLACE INTO derived_val_dist_day (labeler_did, day_epoch, val, n)
        SELECT  le.labeler_did,
                (CAST(strftime('%s', le.ts) AS INTEGER) / 86400) * 86400 AS day_epoch,
                COALESCE(le.val, '<null>') AS val,
                COUNT(*) AS n
        FROM label_events le
        WHERE le.neg = 0
          AND le.uri LIKE 'at://%/app.bsky.feed.post/%'
          AND le.ts >= :cutoff_iso
        GROUP BY le.labeler_did, day_epoch, COALESCE(le.val, '<null>')
    """, {"cutoff_iso": cutoff_iso})

    # Prune old rows
    conn.execute(
        "DELETE FROM derived_val_dist_day WHERE day_epoch < ?",
        (retention_cutoff_day_epoch,),
    )


def _compute_entropy_7d(conn) -> None:
    """Compute per-labeler entropy summary from derived_val_dist_day."""
    now_epoch = int(time.time())
    cutoff_7d = ((now_epoch // 86400) - 6) * 86400
    cutoff_30d = ((now_epoch // 86400) - 29) * 86400

    # 7d window
    rows_7d = conn.execute("""
        SELECT labeler_did, val, SUM(n) AS n
        FROM derived_val_dist_day
        WHERE day_epoch >= ?
        GROUP BY labeler_did, val
    """, (cutoff_7d,)).fetchall()

    # 30d window
    rows_30d = conn.execute("""
        SELECT labeler_did, val, SUM(n) AS n
        FROM derived_val_dist_day
        WHERE day_epoch >= ?
        GROUP BY labeler_did, val
    """, (cutoff_30d,)).fetchall()

    # Build per-labeler dicts
    data_7d: dict[str, dict[str, int]] = defaultdict(dict)
    for r in rows_7d:
        data_7d[r["labeler_did"]][r["val"]] = r["n"]

    data_30d: dict[str, dict[str, int]] = defaultdict(dict)
    for r in rows_30d:
        data_30d[r["labeler_did"]][r["val"]] = r["n"]

    # All labelers seen in either window
    all_dids = set(data_7d.keys()) | set(data_30d.keys())

    # Atomic: temp table + swap
    conn.execute("DROP TABLE IF EXISTS tmp_entropy")
    conn.execute("""CREATE TEMP TABLE tmp_entropy (
        labeler_did    TEXT PRIMARY KEY,
        n_events_7d    INTEGER NOT NULL,
        k_vals_7d      INTEGER NOT NULL,
        entropy_7d     REAL,
        h_norm_7d      REAL,
        n_eff_7d       REAL,
        top1_val       TEXT,
        top1_share     REAL,
        top2_share     REAL,
        n_events_30d   INTEGER NOT NULL,
        k_vals_30d     INTEGER NOT NULL,
        entropy_30d    REAL,
        h_norm_30d     REAL,
        n_eff_30d      REAL,
        delta_h_norm   REAL,
        updated_epoch  INTEGER NOT NULL
    )""")

    for did in all_dids:
        vc_7d = data_7d.get(did, {})
        vc_30d = data_30d.get(did, {})

        counts_7d = list(vc_7d.values())
        counts_30d = list(vc_30d.values())

        n_events_7d = sum(counts_7d)
        k_vals_7d = len(counts_7d)
        n_events_30d = sum(counts_30d)
        k_vals_30d = len(counts_30d)

        h_7d, h_norm_7d, n_eff_7d = _shannon_entropy(counts_7d)
        h_30d, h_norm_30d, n_eff_30d = _shannon_entropy(counts_30d)

        # Top1 and top2 share (from 7d)
        if counts_7d:
            sorted_vals = sorted(vc_7d.items(), key=lambda x: x[1], reverse=True)
            top1_val = sorted_vals[0][0]
            top1_share = sorted_vals[0][1] / n_events_7d
            top2_n = sorted_vals[0][1] + (sorted_vals[1][1] if len(sorted_vals) > 1 else 0)
            top2_share = top2_n / n_events_7d
        else:
            top1_val = None
            top1_share = None
            top2_share = None

        # Delta: only if 30d has enough data
        if h_norm_7d is not None and h_norm_30d is not None and n_events_30d >= 200:
            delta_h_norm = h_norm_7d - h_norm_30d
        else:
            delta_h_norm = None

        conn.execute(
            """INSERT INTO tmp_entropy
               (labeler_did, n_events_7d, k_vals_7d, entropy_7d, h_norm_7d, n_eff_7d,
                top1_val, top1_share, top2_share,
                n_events_30d, k_vals_30d, entropy_30d, h_norm_30d, n_eff_30d,
                delta_h_norm, updated_epoch)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (did, n_events_7d, k_vals_7d, h_7d, h_norm_7d, n_eff_7d,
             top1_val, top1_share, top2_share,
             n_events_30d, k_vals_30d, h_30d, h_norm_30d, n_eff_30d,
             delta_h_norm, now_epoch),
        )

    # Swap
    conn.execute("DELETE FROM derived_labeler_entropy_7d")
    conn.execute("INSERT INTO derived_labeler_entropy_7d SELECT * FROM tmp_entropy")
    conn.execute("DROP TABLE IF EXISTS tmp_entropy")


def _compute_boundary_load_7d(conn) -> None:
    """Compute per-labeler boundary load stats from derived_label_fp (last 7 days).

    Boundary load σ counts labels applied faster than verification thresholds.
    Uses bisect on sorted non-negative lags for cumulative bucket counts.
    """
    cutoff_epoch = int(time.time()) - (7 * 86400)

    rows = conn.execute("""
        SELECT labeler_did, lag_sec_claimed
        FROM derived_label_fp
        WHERE lag_sec_claimed IS NOT NULL
          AND CAST(strftime('%s', label_ts) AS INTEGER) >= ?
    """, (cutoff_epoch,)).fetchall()
    # fetchall() fine — derived_label_fp is ~25K rows

    per_labeler: dict[str, list[int]] = defaultdict(list)
    for r in rows:
        per_labeler[r["labeler_did"]].append(r["lag_sec_claimed"])

    now_epoch = int(time.time())

    # Atomic: temp table + swap inside transaction
    conn.execute("DROP TABLE IF EXISTS tmp_boundary_load")
    conn.execute("""CREATE TEMP TABLE tmp_boundary_load (
        labeler_did TEXT PRIMARY KEY, n_matched INTEGER NOT NULL,
        n_negative INTEGER NOT NULL, n_sub_1s INTEGER NOT NULL,
        n_sub_5s INTEGER NOT NULL, n_sub_30s INTEGER NOT NULL,
        n_sub_60s INTEGER NOT NULL, p5_lag INTEGER, p10_lag INTEGER,
        updated_epoch INTEGER NOT NULL
    )""")

    for did, lags in per_labeler.items():
        n_matched = len(lags)
        n_negative = sum(1 for l in lags if l < 0)
        non_negative = sorted(l for l in lags if l >= 0)

        # Bucket counts via bisect (cumulative: sub_1s <= sub_5s <= sub_30s <= sub_60s)
        n_sub_1s = bisect_left(non_negative, 1)
        n_sub_5s = bisect_left(non_negative, 5)
        n_sub_30s = bisect_left(non_negative, 30)
        n_sub_60s = bisect_left(non_negative, 60)

        # Fast-tail percentiles on non-negative lags only
        p5 = _nearest_rank(non_negative, 0.05)
        p10 = _nearest_rank(non_negative, 0.10)

        conn.execute(
            """INSERT INTO tmp_boundary_load
               (labeler_did, n_matched, n_negative, n_sub_1s, n_sub_5s,
                n_sub_30s, n_sub_60s, p5_lag, p10_lag, updated_epoch)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (did, n_matched, n_negative, n_sub_1s, n_sub_5s,
             n_sub_30s, n_sub_60s, p5, p10, now_epoch),
        )

    # Swap: old data survives if we crashed before this point
    conn.execute("DELETE FROM derived_labeler_boundary_load_7d")
    conn.execute("""INSERT INTO derived_labeler_boundary_load_7d
                    SELECT * FROM tmp_boundary_load""")
    conn.execute("DROP TABLE IF EXISTS tmp_boundary_load")


def run_derive(conn, config: Config, now: datetime | None = None) -> None:
    """Run regime/risk/coherence derivation (expensive — call less often than scan)."""
    if now is None:
        now = now_utc()
    _run_derive_pass(conn, config, now)
    _update_coverage_columns(conn, config, now)
    _cleanup_ingest_outcomes(conn, now)

    if config.driftwatch_facts_path:
        try:
            _sync_driftwatch_facts(conn, config)
        except Exception as exc:
            _log.warning("driftwatch facts sync failed: %s", exc)

    try:
        _compute_labeler_lag_7d(conn)
    except Exception as exc:
        _log.warning("labeler lag 7d compute failed: %s", exc)

    try:
        _compute_reversal_stats_7d(conn)
    except Exception as exc:
        _log.warning("reversal stats 7d compute failed: %s", exc)

    try:
        _compute_boundary_load_7d(conn)
    except Exception as exc:
        _log.warning("boundary load 7d compute failed: %s", exc)

    try:
        _update_val_dist_day(conn)
    except Exception as exc:
        _log.warning("val dist day update failed: %s", exc)

    try:
        _compute_entropy_7d(conn)
    except Exception as exc:
        _log.warning("entropy 7d compute failed: %s", exc)

    conn.commit()
